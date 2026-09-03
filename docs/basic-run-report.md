# Basic-system run report

## Configuration

- Mode: individual, basic Docker system, no GPU/vLLM.
- Python dependencies: repository-local `.venv` created by `uv`.
- Runtime state: `.lab28/` and `evidence/`, both ignored by Git.
- Docker services observed running: API, gateway, Kafka, Qdrant, Feast, MLflow,
  OTEL collector, Jaeger, Prometheus, Grafana, and Pushgateway.

## Checkpoint results

| Checkpoint | Result | Evidence |
|---|---|---|
| Code contracts | Pass | 87 unit/contract tests plus Ruff and repository validators. |
| Kafka topics and IP01 key propagation | Pass | Four declared topics created; a consumed event proves Kafka key/header/payload use the idempotency key. |
| Vector index | Pass | 13 documents upserted into `lab28_documents`. |
| Model release | Pass | `lab28-rag-release` version 5 is the `champion`. |
| Gateway ingestion | Pass | Seed accepted 13 documents and 12 feedback events. |
| Serving readiness | Scoped pass | HTTP 200 / `degraded`; only vLLM is unavailable. |
| Load profile | Completed with finding | 200/200 HTTP 200; P50 974.93 ms, P95 1696.47 ms, P99 2762.14 ms. |

## Evidence inventory

The generated runtime evidence directory contains:

- `ip01-kafka-consume.json`
- `ip05-qdrant-search.json`
- `ip06-mlflow-release.json`
- `ip07-vllm-identity.json` — records the real unavailable endpoint; it is not
  evidence of vLLM success.
- `integration-report.json`

The following items are intentionally absent for this scope:

| Evidence | Status | Reason |
|---|---|---|
| IP02 Airflow run | NOT RUN | Airflow/Spark is only in `--profile full`. |
| IP03 Delta history | NOT RUN | No full profile or Delta table exists. |
| IP04 materialized Feast entity | NOT RUN | Requires Delta output from the full pipeline. |
| IP07 vLLM identity | UNVERIFIED | The 4 GiB GPU cannot meet the real vLLM gate. |
| IP08 429 proof | NOT RUN | Live integration fixture intentionally requires the complete stack. |
| IP09 full targets/dashboard proof | NOT RUN | Full monitoring fixture requires Airflow target. |
| IP10 required-span trace | NOT RUN | Requires the full HTTP → Kafka → Airflow → Delta journey. |

## Incident/recovery narrative for the scoped demo

Qdrant was stopped deliberately. `/ready` changed from HTTP 200 / `degraded`
to HTTP 503 / `not_ready`, with `qdrant.ready=false`. After restarting Qdrant,
the collection still had 13 points and readiness recovered to HTTP 200 /
`degraded`; vLLM remained the only optional unavailable dependency. See
`evidence/incident-qdrant-recovery.json`.
