"""The FastAPI service: ingestion, serving, and the three health semantics.

This module is deliberately thin. Orchestration lives in ``pipeline.py`` and
readiness lives in ``readiness.py``; what is here is the HTTP contract — status
codes, error bodies, and the boundary where a Python exception becomes a
response a client can branch on.

Three ideas are worth reading the code for.

**Ingestion returns 202, never 200.** The API's only job on ``/feedback`` and
``/documents`` is to durably hand the event to Kafka. Delta, Feast and Qdrant
are updated later by Airflow and Spark, so claiming 201 Created would be a lie
about work that has not happened yet. 202 plus the event id is the honest
answer, and the id is what a student greps for to follow one record across the
whole platform.

**An omitted idempotency key is derived, not skipped.** Replaying the same
feedback must collapse to one Delta row, one feature update and one vector
point. Deriving the key from the content hash means that holds even for a
client that never thought about retries.

**Liveness, startup and readiness are three different questions.** ``/health``
answers "is this process alive", ``/startup`` answers "did configuration and
clients construct", ``/ready`` answers "can this pod serve a request right
now". Only the last one touches a dependency, and only the last one can take a
pod out of the gateway's rotation.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from lab28_platform import metrics, readiness
from lab28_platform.contracts import (
    ERROR_STATUS,
    AskRequest,
    AskResponse,
    DocumentPayload,
    DocumentSubmission,
    ErrorCategory,
    ErrorResponse,
    FeedbackPayload,
    FeedbackSubmission,
    IngestionAccepted,
    IngestionEvent,
    ReadinessResponse,
    content_hash,
)
from lab28_platform.event_bus import BrokerUnavailable, EventPublisher
from lab28_platform.feature_store import FeatureClient
from lab28_platform.llm_client import VLLMClient
from lab28_platform.model_registry import ReleaseRegistry
from lab28_platform.pipeline import AskPipeline, ServingError
from lab28_platform.settings import Settings
from lab28_platform.telemetry import (
    SPAN_API_INGEST,
    configure_telemetry,
    current_trace_id,
    current_traceparent,
    instrument_fastapi,
    instrument_httpx,
    span,
)
from lab28_platform.vector_store import VectorStore

#: Categories a client can usefully retry. Anything else is a client-side bug
#: or a policy decision, and retrying it only adds load.
RETRYABLE = {
    ErrorCategory.RATE_LIMITED,
    ErrorCategory.DEPENDENCY_UNAVAILABLE,
    ErrorCategory.DEPENDENCY_TIMEOUT,
    ErrorCategory.NOT_READY,
}


@dataclass
class Runtime:
    """Long-lived clients, built once per process.

    Construction is not connection: every client here is cheap to build and
    fails at call time instead. That is deliberate — a pod that refuses to start
    because MLflow is briefly down can never report *why* through ``/ready``.
    """

    settings: Settings
    publisher: EventPublisher | None
    registry: ReleaseRegistry
    features: FeatureClient
    vectors: VectorStore
    llm: VLLMClient
    pipeline: AskPipeline

    @classmethod
    def build(cls, settings: Settings) -> Runtime:
        registry = ReleaseRegistry(settings.mlflow)
        features = FeatureClient(settings.feast)
        vectors = VectorStore(settings.qdrant)
        llm = VLLMClient(settings.vllm)
        try:
            publisher: EventPublisher | None = EventPublisher(settings.kafka)
        except Exception:
            # A misconfigured broker must not stop the serving path from
            # starting; ingestion will report it as dependency_unavailable.
            publisher = None
        return cls(
            settings=settings,
            publisher=publisher,
            registry=registry,
            features=features,
            vectors=vectors,
            llm=llm,
            pipeline=AskPipeline(
                registry=registry,
                features=features,
                vectors=vectors,
                llm=llm,
                settings=settings.serving,
                embedding_model_id=settings.qdrant.embedding_model_id,
            ),
        )

    def close(self) -> None:
        self.pipeline.close()
        if self.publisher is not None:
            self.publisher.close()


def _runtime(request: Request) -> Runtime:
    return request.app.state.runtime  # type: ignore[no-any-return]


def _error(category: ErrorCategory, message: str, service: str) -> JSONResponse:
    """Render the one error body the platform returns."""
    body = ErrorResponse(
        category=category,
        message=message,
        trace_id=current_trace_id(),
        service=service,
        retryable=category in RETRYABLE,
    )
    return JSONResponse(status_code=ERROR_STATUS[category], content=body.model_dump(mode="json"))


def _route_of(request: Request) -> str:
    """The path template, so ``/api/v1/ask`` is one series and not many."""
    route = request.scope.get("route")
    return getattr(route, "path", request.url.path)


def _derive_key(kind: str, entity_id: str, text: str) -> str:
    """Content-derived de-duplication key for a client that supplied none.

    Two submissions of the same text by the same entity are the same fact, so
    they must produce the same key and collapse downstream.
    """
    return f"{kind}:{entity_id}:{content_hash(text)[:32]}"


def _publish(runtime: Runtime, topic: str, event: IngestionEvent) -> IngestionAccepted:
    """Publish one ingestion event and return the 202 body.

    The metric is incremented on both paths on purpose: an ingestion endpoint
    that only counts successes cannot show a broker outage on the dashboard.
    """
    if runtime.publisher is None:
        raise ServingError(
            ErrorCategory.DEPENDENCY_UNAVAILABLE,
            "the event publisher could not be constructed; check LAB28_KAFKA_BOOTSTRAP_SERVERS",
        )

    with span(
        SPAN_API_INGEST,
        attributes={
            "lab28.ingest.kind": event.kind,
            "lab28.ingest.event_id": event.event_id,
            "lab28.ingest.idempotency_key": event.idempotency_key,
        },
    ):
        started = time.perf_counter()
        try:
            runtime.publisher.publish(topic, event.idempotency_key, event)
        except BrokerUnavailable as error:
            metrics.INGESTION_EVENTS.labels(
                kind=event.kind, topic=topic, outcome="rejected"
            ).inc()
            raise ServingError(
                ErrorCategory.DEPENDENCY_UNAVAILABLE, f"could not publish event: {error}"
            ) from error

        metrics.INGESTION_PUBLISH_SECONDS.labels(topic=topic).observe(
            time.perf_counter() - started
        )
        metrics.INGESTION_EVENTS.labels(kind=event.kind, topic=topic, outcome="accepted").inc()

    return IngestionAccepted(
        event_id=event.event_id,
        idempotency_key=event.idempotency_key,
        entity_id=event.entity_id,
        topic=topic,
        trace_id=current_trace_id(),
    )


def create_app(settings: Settings | None = None, *, runtime: Runtime | None = None) -> FastAPI:
    """Build the application.

    A factory rather than a module-level ``app`` so that settings and the
    dependency bundle are both injectable: the fast test suite passes a
    ``Runtime`` of fakes and exercises the real routing, validation and error
    mapping without a broker, a registry or a GPU anywhere in sight.
    """
    resolved = settings or Settings.from_env()
    configure_telemetry(resolved.telemetry)
    instrument_httpx()

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> Any:
        application.state.runtime = runtime or Runtime.build(resolved)
        try:
            yield
        finally:
            application.state.runtime.close()

    app = FastAPI(
        title="lab28 platform API",
        version="3.0.0",
        summary="Ingestion and grounded question answering across ten integration points.",
        lifespan=lifespan,
    )

    _register_middleware(app)
    _register_error_handlers(app, resolved)
    _register_routes(app)
    instrument_fastapi(app)
    return app


def _register_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def observe(request: Request, call_next: Any) -> Response:
        """Time every request and stamp the trace id on the response.

        Returning the trace id is what lets a student paste one header value
        into Grafana or LangSmith and see the same request end to end.
        """
        started = time.perf_counter()
        response = await call_next(request)
        route = _route_of(request)
        status = str(response.status_code)
        metrics.REQUEST_SECONDS.labels(route=route, status=status).observe(
            time.perf_counter() - started
        )
        metrics.REQUESTS.labels(route=route, status=status).inc()

        trace_id = current_trace_id()
        if trace_id:
            response.headers["x-lab28-trace-id"] = trace_id
        traceparent = current_traceparent()
        if traceparent:
            response.headers["traceparent"] = traceparent
        return response


def _register_error_handlers(app: FastAPI, settings: Settings) -> None:
    service = settings.telemetry.service_name

    @app.exception_handler(ServingError)
    async def serving_error(request: Request, error: ServingError) -> JSONResponse:
        metrics.ERRORS.labels(route=_route_of(request), category=error.category.value).inc()
        return _error(error.category, str(error), service)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request: Request, error: RequestValidationError) -> JSONResponse:
        """Collapse pydantic's detail list into one message.

        The full list is useful in a test and noisy in a log line, so the body
        keeps the field paths and drops the input echo — which could otherwise
        replay user text into an error store.
        """
        fields = "; ".join(
            f"{'.'.join(str(part) for part in item['loc'][1:])}: {item['msg']}"
            for item in error.errors()
        )
        metrics.ERRORS.labels(
            route=_route_of(request), category=ErrorCategory.VALIDATION.value
        ).inc()
        return _error(ErrorCategory.VALIDATION, fields or "invalid request body", service)

    @app.exception_handler(Exception)
    async def unexpected(request: Request, error: Exception) -> JSONResponse:
        metrics.ERRORS.labels(
            route=_route_of(request), category=ErrorCategory.INTERNAL.value
        ).inc()
        return _error(
            ErrorCategory.INTERNAL, f"unhandled {type(error).__name__}", service
        )


def _register_routes(app: FastAPI) -> None:
    @app.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        """Liveness. Deliberately touches nothing."""
        return {"status": "alive", "service": "lab28-api"}

    @app.get("/startup", tags=["health"])
    def startup(request: Request) -> dict[str, Any]:
        """Startup. Configuration parsed and clients constructed."""
        runtime = _runtime(request)
        return {
            "status": "started",
            "model_name": runtime.settings.mlflow.model_name,
            "collection": runtime.settings.qdrant.collection,
            "publisher": runtime.publisher is not None,
        }

    @app.get("/ready", tags=["health"], response_model=ReadinessResponse)
    def ready(request: Request, response: Response) -> ReadinessResponse:
        """Readiness. 503 removes this pod from the gateway's rotation.

        ``degraded`` still answers 200: a cold feature store makes an answer
        worse, not wrong, and draining every pod over it would turn a partial
        outage into a total one.
        """
        runtime = _runtime(request)
        report = readiness.serving_readiness(
            runtime.settings,
            features=runtime.features,
            vectors=runtime.vectors,
            registry=runtime.registry,
        )
        if report.status == "not_ready":
            response.status_code = 503
        return report

    @app.get("/metrics", tags=["health"])
    def prometheus() -> Response:
        return Response(content=metrics.render(), media_type=metrics.CONTENT_TYPE)

    @app.post("/api/v1/feedback", status_code=202, tags=["ingestion"])
    def submit_feedback(request: Request, body: FeedbackSubmission) -> IngestionAccepted:
        runtime = _runtime(request)
        event = IngestionEvent(
            idempotency_key=body.idempotency_key or _derive_key("fb", body.asker_id, body.text),
            entity_id=body.asker_id,
            traceparent=current_traceparent(),
            payload=FeedbackPayload(
                asker_id=body.asker_id,
                text=body.text,
                rating=body.rating,
                locale=body.locale,
                label=body.label,
            ),
        )
        return _publish(runtime, runtime.settings.kafka.topic_raw, event)

    @app.post("/api/v1/documents", status_code=202, tags=["ingestion"])
    def submit_document(request: Request, body: DocumentSubmission) -> IngestionAccepted:
        runtime = _runtime(request)
        event = IngestionEvent(
            idempotency_key=body.idempotency_key or _derive_key("doc", body.doc_id, body.text),
            entity_id=body.doc_id,
            traceparent=current_traceparent(),
            payload=DocumentPayload(
                doc_id=body.doc_id,
                title=body.title,
                text=body.text,
                locale=body.locale,
                tags=body.tags,
            ),
        )
        return _publish(runtime, runtime.settings.kafka.topic_raw, event)

    @app.post("/api/v1/ask", tags=["serving"], response_model=AskResponse)
    def ask(request: Request, body: AskRequest) -> AskResponse:
        """The whole platform in one call: features, retrieval, model, evidence."""
        return _runtime(request).pipeline.answer(body)


def iter_routes(app: FastAPI) -> Iterator[str]:
    """Route paths, used by the readiness report and the CLI's ``inspect``."""
    for route in app.routes:
        path = getattr(route, "path", None)
        if path:
            yield path


# No module-level ``app``: importing this module must not configure telemetry or
# patch httpx as a side effect. Serve it with ``lab28 serve``, or with
# ``uvicorn lab28_platform.api:create_app --factory``.
