# Evidence index — basic, no-GPU run

Các link dưới đây là link cục bộ được dùng trong lúc stack đang chạy. JSON trong
`evidence/` là artefact có thể đính kèm khi nộp; không có ảnh mô phỏng.

| Nội dung | Artefact / link | Trạng thái |
|---|---|---|
| Fast suite | `evidence/fast-suite.txt` | 87 passed |
| Integration matrix | `evidence/matrix-validation.txt` | pass |
| IP01 Kafka | `evidence/ip01-kafka-consume.json` | verified |
| IP05 Qdrant | `evidence/ip05-qdrant-search.json`, <http://localhost:6333/dashboard> | verified |
| IP06 MLflow | `evidence/ip06-mlflow-release.json`, <http://localhost:5000> | verified |
| Readiness | <http://localhost:8000/ready>, <http://localhost:8080/health> | degraded, expected for no GPU |
| Metrics | <http://localhost:9090/targets>, <http://localhost:3000> | UI link; full IP09 proof NOT RUN |
| Trace | <http://localhost:16686/trace/d8ac39e213e743f9843ea12e57965a47> | readiness trace only; full IP10 proof NOT RUN |
| Controlled incident | `evidence/incident-qdrant-recovery.json` | verified recovery |
| Load profile | `evidence/load-profile.json` | 200/200 HTTP 200 |

## Scope boundary

Airflow/Spark/Delta không chạy trong basic profile, vì vậy IP02, IP03 và
materialization end-to-end của IP04 là `NOT RUN`. IP07 là `UNVERIFIED` do không
có GPU/vLLM thật. IP08–IP10 full journey không được suy diễn từ các UI link này.
