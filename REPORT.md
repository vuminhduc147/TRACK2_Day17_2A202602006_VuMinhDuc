# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Vũ Minh Đức  
**Lớp:** E403  
**Ngày:** 2026-08-17

## 0 · Kết quả `make verify`

### Kiểm tra số hàng và tính ổn định

| Bảng                 | Ổn định | Số hàng | Kỳ vọng | Ghi chú |
| -------------------- | ------: | ------: | ------: | ------: |
| `gold_training_set`  |    ✓ ok |  12,480 |  12,480 |       ✓ |
| `gold_feature_daily` |    ✓ ok |   9,100 |   9,100 |       ✓ |
| `gold_doc_chunks`    |    ✓ ok |  31,200 |  31,200 |       ✓ |
| `quarantine_tickets` |    ✓ ok |     312 |     312 |       ✓ |

### Checksum từng lượt

| Bảng                 | Lượt 1       | Lượt 2       | Lượt 3       | Kết quả |
| -------------------- | ------------ | ------------ | ------------ | ------: |
| `gold_training_set`  | `8dd7c98653` | `8dd7c98653` | `8dd7c98653` |       ✓ |
| `gold_feature_daily` | `3db448685c` | `3db448685c` | `3db448685c` |       ✓ |
| `gold_doc_chunks`    | `92d8e50131` | `92d8e50131` | `92d8e50131` |       ✓ |
| `quarantine_tickets` | `ebb89036fb` | `ebb89036fb` | `ebb89036fb` |       ✓ |

### Kiểm tra khác

| Kiểm tra                                       | Kết quả      |
| ---------------------------------------------- | ------------ |
| `dbt test`                                     | ✓ 11/11 pass |
| `silver_tickets.priority ∈ 1..4`, không `NULL` | ✓ sạch       |
| `quarantine_tickets` đúng số bản ghi lỗi       | ✓ 312 / 312  |
| `gold_training_set`: 1 hàng / 1 ticket         | ✓ không lặp  |


## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | `gold_training_set` tăng số hàng sau mỗi lần retry và một `ticket_id` xuất hiện nhiều lần. |
| **Nguyên nhân** | Model incremental không có `unique_key` và strategy nên dbt ghi bằng `INSERT`; retry cùng dữ liệu ghi thêm thay vì thay thế. Đây là bảng entity có cập nhật CDC, nên xóa/ghi theo partition ngày cũng không xử lý đúng ticket được cập nhật ở ngày khác. |
| **Cách khắc phục** | Trong `gold_training_set.sql`, dùng `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong DAG, tắt catchup và giới hạn `max_active_runs=1` để tránh các run ghi đồng thời. |
| **Bằng chứng kỳ vọng** | 12.480 hàng, 1 hàng/ticket, checksum giống nhau qua 3 lượt. |

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | Bảng ổn định nhưng chỉ có 8.645 thay vì 9.100 cặp `(event_date, customer_id)`; thiếu ở các ngày cũ. |
| **P99 độ trễ đo được** | **2,7235 ngày** (130.683 events). Max = 2,9447 ngày; 5,0037% events trễ hơn 1 ngày. |
| **Lookback đã chọn** | 3 ngày, vì làm tròn P99 lên và cũng bao phủ max của bộ dữ liệu hiện tại. |
| **Nguyên nhân** | Incremental filter chỉ nhận `event_date` lớn hơn ngày lớn nhất ở target. Event xảy ra ngày cũ nhưng ingest muộn không bao giờ qua filter sau khi watermark event-time đã tiến lên. |
| **Cách khắc phục** | Tính lại cửa sổ 3 ngày; dùng khóa ghép `['event_date', 'customer_id']` và `delete+insert` để kết quả tính lại thay thế hàng cũ. |
| **Bằng chứng kỳ vọng** | 9.100 hàng và checksum giống nhau qua 3 lượt. |

P99 là ngưỡng vận hành cân bằng giữa độ đầy đủ và chi phí tính lại ở mọi run. Dùng max có thể tốn tài nguyên vì một outlier; trong bộ seed này max vẫn dưới 3 ngày nên cửa sổ 3 ngày bao phủ cả hai. Trong production cần theo dõi phần đuôi ngoài P99 và có luồng backfill riêng.

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `try_cast` biến nhãn chữ hợp lệ thành NULL nhưng lại chấp nhận số ngoài miền như 0, 5, -1; pipeline vẫn chạy nên chất lượng model giảm âm thầm. |
| **Nguyên nhân** | Logic chỉ kiểm tra khả năng ép kiểu, không xử lý schema evolution và không kiểm tra miền 1..4; contract và accepted-values test chưa được bật. |
| **Ba nhóm giá trị** | `1..4`: giữ nguyên; `urgent/high/medium/low`: map thành `1/2/3/4`; `P1/unknown/0/5/-1/rỗng/NULL`: quarantine. |
| **Cách khắc phục** | Chuẩn hóa bằng macro dùng chung; lọc row lỗi trước khi xếp hạng CDC; đưa row lỗi vào quarantine; bật contract và test `not_null`, `accepted_values`. |
| **Bằng chứng kỳ vọng** | Quarantine = 312 hàng; Silver vẫn đủ 12.480 ticket; priority luôn thuộc 1..4; dbt test pass với hơn 9 test. |

Bronze nên giữ payload gốc để replay và điều tra. Việc chuẩn hóa/tách lỗi thuộc Silver. Không nên dừng toàn DAG vì 312 row hỏng sẽ chặn hơn 130.000 event và 31.200 chunks hợp lệ; quarantine vừa duy trì dịch vụ vừa tạo hàng đợi xử lý có thể kiểm toán.

## 4 · Tổng kết

| Nhiệm vụ | Kiểm tra đầu tiên khi tiếp nhận hệ thống |
|---|---|
| 1 | Grain, natural key và write strategy của mọi incremental model. |
| 2 | Watermark đang dựa trên event time hay ingestion time và phân bố độ trễ thực tế. |
| 3 | Contract gồm cả type lẫn domain, cùng đường đi của bản ghi không hợp lệ. |
