# Reflection — Lab 28

## Bối cảnh và phạm vi

Em làm bài cá nhân theo nhánh hệ thống cơ bản: Docker chạy Kafka, API, gateway,
Qdrant, Feast, MLflow và stack quan sát, không chạy Airflow/Spark/Delta của
`--profile full`. Máy có 12 logical CPU, còn GPU RTX 2050 chỉ có 4 GiB VRAM.
Vì không đáp ứng điều kiện của một endpoint vLLM thật, IP07 được ghi
`UNVERIFIED`. Em không thay nó bằng mock, CPU classifier hay server chỉ mô phỏng
OpenAI API.

Phạm vi này giúp em tập trung kiểm tra đúng những boundary có thể tái lập trên
máy cá nhân. Đổi lại, em không nhận IP02, IP03, phần materialization của IP04
và trace end-to-end là đã hoàn tất. Các mục đó đều được ghi rõ `NOT RUN` trong
evidence index và báo cáo chạy.

## Điều khó nhất

Điều khó nhất là giữ ranh giới giữa một kết quả nhìn có vẻ đúng và một kết
quả thực sự có đủ bằng chứng. Lúc đầu, Feast trả health check tốt và Grafana,
Jaeger đều mở được. Nhưng những tín hiệu ấy chưa chứng minh rằng dữ liệu đã đi
từ Kafka qua Airflow, Delta rồi materialize sang Feast. Nếu gọi đó là happy path
thì bài làm sẽ dễ nhìn hơn, nhưng không trung thực.

Một khó khăn khác là môi trường Windows. `uv` phải tạo virtual environment tại
repo, model embedding phải có cache runtime riêng, còn MLflow từng dừng ở bước
đóng run vì console CP1252 không in được ký tự emoji. Chạy release với
`PYTHONUTF8=1` giải quyết được lỗi này. Sự cố đó nhắc em rằng portability không
chỉ là chạy được unit test; encoding, cache path và hành vi Docker cũng là một
phần của vận hành.

Khó khăn đáng chú ý nhất về integration là evidence IP01. Consume Kafka cho
thấy message key và header ban đầu lấy theo `entity_id`, trong khi payload có
`idempotency_key` khác. Em không dừng ở việc test helper pass mà sửa publish
path, recreate API, gửi event có `entity_id` khác key và consume lại. Khi đó
Kafka key, header `idempotency-key` và payload mới khớp nhau. Đây là ví dụ rõ
nhất cho việc unit test xanh chưa đủ để khẳng định contract đang đúng ở runtime.

## Các lựa chọn kỹ thuật và trade-off

### Idempotency và trace qua Kafka

`event_headers` luôn trả `idempotency-key` dạng bytes; `traceparent` chỉ được
thêm khi có trace hợp lệ. Việc bỏ header rỗng quan trọng vì một traceparent rỗng
có thể làm consumer tưởng context vẫn tồn tại. API publish dùng
`event.idempotency_key` làm Kafka key để retry/replay đi qua cùng khóa với Delta
merge. Trade-off là message không còn được partition theo entity một cách mặc
định; trong bài này, tính tái lập và contract replay quan trọng hơn việc tối ưu
phân bố theo entity.

### Khử trùng trước Delta MERGE

`dedupe_latest` giữ đúng một event cho mỗi `idempotency_key`, chọn bản có
`(occurred_at, event_id)` lớn nhất và sắp xếp kết quả theo key. Dùng `event_id`
làm tie-breaker làm kết quả không phụ thuộc thứ tự Kafka giao lại message. Đổi
lại, đây là logic xử lý batch trong bộ nhớ; ở production cần kiểm soát kích cỡ
batch, watermark và chiến lược late-arriving event thay vì giả định mọi replay
đều nằm trong một iterable nhỏ.

### Contract Feast là nguồn chân lý duy nhất

Request online features lấy `FEATURE_REFS` từ `contracts.py`, không viết lại
danh sách feature tại API. Cách này giảm nguy cơ API gọi nhầm feature sau khi
registry thay đổi. Trade-off là thay đổi feature contract cần được review kỹ hơn
vì nhiều thành phần cùng phụ thuộc vào một nguồn chung. Với hệ thống dữ liệu,
đó là trade-off hợp lý: thay đổi có kiểm soát tốt hơn drift âm thầm.

### Readiness phân biệt lỗi bắt buộc và lỗi optional

Kafka, Qdrant, MLflow và Feast là dependency bắt buộc của serving path;
vLLM là optional ở nhánh không GPU. Vì thế lỗi vLLM cho `/ready=degraded`, còn
lỗi Qdrant phải cho `/ready=not_ready`. Điều này cho phép gateway tiếp tục phục
vụ những đường không cần LLM, nhưng không che giấu tình trạng retrieval hỏng.
Trade-off là client và vận hành phải hiểu đúng `degraded`, không coi HTTP 200 là
tất cả chức năng đều khả dụng.

### Phạm vi basic/no-GPU

Em chọn dừng ở basic profile thay vì cố ép full stack trên máy cá nhân. Quyết
định này tránh biến lỗi thiếu tài nguyên thành một demo không ổn định, nhưng
đồng nghĩa thiếu Delta history, replay end-to-end, Airflow evidence và vLLM
identity. Các file evidence chỉ ghi điều đã chạy; phần còn lại để `NOT RUN` hoặc
`UNVERIFIED`, không điền dữ liệu thay thế.

## Kết quả đã quan sát

- Fast suite có 87 test pass. Ruff, integration matrix, portability và
  manifest validation đều pass; output được lưu trong evidence bundle.
- Kafka có đủ bốn topic khai báo. IP01 có consume record thực tế, trong đó key,
  header và payload cùng dùng `ip01-evidence-key-v2-20260903`.
- Qdrant có 13 points; evidence IP05 lưu kết quả hybrid search và model ID.
- MLflow có `lab28-rag-release` version 5 với alias `champion`.
- Seed qua gateway nhận 13 documents và 12 feedback; response chứa event ID,
  idempotency key và trace ID.
- Load profile `/ready` với 200 request, 8 worker có 200 HTTP 200; P50 974.93
  ms, P95 1696.47 ms, P99 2762.14 ms.

Kết quả load profile không phải SLO cho `POST /api/v1/ask`. `/ready` đang kiểm
tra nhiều dependency nên độ trễ khoảng một giây ở P50 cho thấy readiness sâu
không nên được gọi với tần suất cao như liveness endpoint.

## Sự cố đã tạo, dấu hiệu và khôi phục

Em chủ động dừng container Qdrant để kiểm tra cơ chế readiness và recovery.
Trước sự cố, `/ready` trả HTTP 200 với `degraded` vì chỉ thiếu vLLM. Khi Qdrant
dừng, readiness chuyển sang HTTP 503, `status=not_ready` và
`qdrant.ready=false`; dấu hiệu chi tiết là `ResponseHandlingException` do tên
service không còn được resolve. Đây là hành vi mong muốn vì Qdrant là dependency
bắt buộc của serving path.

Nguyên nhân là container Qdrant bị dừng có chủ đích. Khôi phục bằng cách start
lại đúng service, chờ probe hoàn tất rồi gọi lại `/ready`. Sau recovery, Qdrant
có lại 13 points, `/ready` trở về HTTP 200 / `degraded` và chỉ vLLM còn
unavailable. Không có điểm dữ liệu bị mất vì storage của Qdrant nằm trong named
Docker volume. Dữ liệu trước/trong/sau sự cố và trace ID được lưu tại
`evidence/incident-qdrant-recovery.json`.

## Những việc em sẽ cải tiến

1. Ưu tiên chạy `--profile full` trên máy chung để tạo Delta history, Airflow
   run ID, replay-safe proof và Feast materialization thật. Đây là khoảng trống
   lớn nhất của nhánh basic.
2. Dùng endpoint vLLM được cấp để kiểm tra `/version`, `/v1/models` và metric
   `vllm:`. Chỉ khi ba tín hiệu này có mặt mới đổi IP07 từ `UNVERIFIED` sang
   verified.
3. Tách liveness rẻ khỏi readiness sâu, thêm cache ngắn và giới hạn concurrency
   cho readiness. Sau đó đo riêng latency của endpoint hỏi đáp thay vì dùng
   `/ready` làm đại diện cho trải nghiệm người dùng.
4. Đặt smoke test live cho Kafka key/header/payload. Unit test helper là cần
   thiết nhưng đã không phát hiện publish path dùng sai key.
5. Thu metrics, alert và trace từ cùng một full journey, rồi đối chiếu required
   spans trước demo. Link UI chỉ hữu ích khi đi kèm ID và truy vấn tái lập được.
