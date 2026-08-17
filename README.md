# LAB 17 — Data Pipeline Engineering

**AICB-P2T2 · Ngày 17 · Chương 4: Hạ Tầng**

> Ba tài liệu cần đọc:
> **README.md** (bạn đang ở đây — bối cảnh và yêu cầu) ·
> [**GUIDE.md**](GUIDE.md) (trình tự thao tác từng nhiệm vụ) ·
> [**RUBRIC.md**](RUBRIC.md) (thang điểm 100) ·
> [**EXTRA.md**](EXTRA.md) (bài mở rộng, không bắt buộc)
>
> Khung pseudo-code của mỗi nhiệm vụ nằm ngay trong file cần sửa, dưới dạng chú
> thích `KHUNG THỰC HIỆN`.

---

## 1. Bối cảnh

Bạn tiếp nhận bàn giao đường ống dữ liệu của **nền tảng AI hỗ trợ khách hàng** —
hệ thống được trình bày trong bài giảng hôm nay.

```
Postgres tickets (CDC)  ─┐
S3 transcripts (JSON)   ─┼─→  Bronze  ─→  Silver  ─┬─→  gold_doc_chunks    →  RAG index
Kafka events + feedback ─┘                          ├─→  gold_training_set  →  Classifier
                                                    └─→  gold_feature_daily →  Routing agent
```

Đường ống **chạy được, không phát sinh lỗi, `dbt test` pass**. Tuy vậy đội vận
hành đã gửi ba phiếu sự cố.

Yêu cầu của lab không phải xây dựng lại hệ thống, mà là **chẩn đoán và khắc
phục** — đúng với công việc của một data engineer khi nhận ca trực.

---

## 2. Mục tiêu

Sau lab, bạn phải thực hiện được:

- Viết transform **idempotent** — chạy lại N lần cho cùng một kết quả
- Xử lý **dữ liệu về muộn** bằng lookback window
- Dùng **data contract** để ràng buộc schema, và **định tuyến bản ghi lỗi**
  thay vì để pipeline dừng

## 3. Điều kiện cần

- **Python 3.11+** và `make`. Không cần Docker, không cần tài khoản cloud.
- SQL cơ bản. **Không** yêu cầu kinh nghiệm dbt trước đó.
- Đã xem slide Ngày 17, đặc biệt phần Transform và Engine.

> **Về lựa chọn công nghệ.** Kho dữ liệu là **DuckDB**, transform bằng **dbt**.
> Postgres, S3 và Kafka được thay bằng ba file seed và một commit log ghi trên
> đĩa (`ingest/log_client.py`). Các khái niệm giữ nguyên: CDC, offset, commit,
> partition, contract. Chỉ hạ tầng được đơn giản hoá, để thời gian dành cho
> việc *chẩn đoán* thay vì việc *cài đặt*.

---

## 4. Cài đặt

```bash
git clone https://github.com/VinUni-AI20k/Day17-Track2-DataPipeline.git
cd Day17-Track2-DataPipeline

make setup      # venv + thư viện + sinh 14 ngày dữ liệu + ghi baseline
make pipeline   # chạy đường ống một lượt
make verify     # chạy 3 lượt liên tiếp và in bảng đánh giá
```

`make verify` là công cụ phản hồi chính trong suốt lab. Chạy lại sau mỗi thay
đổi. Khi cần kiểm tra nhanh, dùng `make quick` (một lượt).

Trạng thái ban đầu — **ba dòng ✗ tương ứng ba nhiệm vụ**:

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────
  gold_training_set     ✗ FAIL            38,750      12,480   ✗ thừa 26,270 hàng
  gold_feature_daily    ✓ ok               8,645       9,100   ✗ thiếu 455 hàng
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                   0         312   ✗ thiếu 312 hàng

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 9/9 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✗ 6,606 hàng sai
  quarantine_tickets đúng số bản ghi lỗi      ✗ 0 / 312
  gold_training_set: 1 hàng / 1 ticket        ✗ 12,480 ticket bị lặp
  bài mở rộng (EXTRA.md)                      — chưa chạy `make seed-extra`
  DAG: catchup / max_active_runs              ✗ True / None

  TỔNG KẾT   1/4 tiêu chí đạt
```

Hai cột đầu đo hai đại lượng khác nhau:
`ỔN ĐỊNH` = chạy lại có cho cùng kết quả không · `SỐ HÀNG` = kết quả đó có đúng
không. Một bảng có thể **ổn định nhưng vẫn sai** — xem `gold_feature_daily`.

### Cấu trúc repo

```
├─ seed/generate.py            # sinh 14 ngày dữ liệu — không cần chỉnh sửa
├─ seed/tickets_cdc.jsonl      # CDC từ Postgres: op = c / u / d
├─ seed/events.jsonl           # topic `ai-events`
├─ seed/transcripts.jsonl      # file JSON trên S3
├─ ingest/load_bronze.py       # Bronze loader — đã idempotent, không cần sửa
├─ dbt/models/silver/          # silver_tickets, quarantine_tickets ← nhiệm vụ 3
├─ dbt/models/gold/            # 3 bảng Gold                       ← nhiệm vụ 1, 2
├─ dbt/macros/                 # normalize_priority.sql            ← nhiệm vụ 3
├─ dags/ai_training_pipeline.py# DAG Airflow — chỉ đọc, không chạy ← nhiệm vụ 1
├─ EXTRA.md + queries/ + tools/compact.py + ingest/consumer.py     ← bài mở rộng
├─ expected/                   # số hàng đúng, dùng để tự kiểm tra
├─ tools/verify.py             # make verify
└─ Makefile
```

---

## 5. Nhiệm vụ

> Mỗi nhiệm vụ cung cấp **triệu chứng**, không cung cấp nguyên nhân. Cần điều
> tra trước khi sửa. Trình tự thao tác chi tiết: [GUIDE.md](GUIDE.md).

### Nhiệm vụ 1 — Kích thước bảng training tăng sau mỗi lần chạy *(30 phút)*

> **Phiếu sự cố #1041.** "Đêm qua job lỗi mạng, mình vào Airflow bấm Clear Task
> cho chạy lại. Sáng nay `gold_training_set` nhiều hơn hẳn. Chạy lại lần nữa
> lại nhiều thêm. Không thấy báo lỗi gì cả."

**Cần thu thập trước khi sửa**
- Chạy `make pipeline` hai lần, đếm số hàng sau mỗi lần
- Đọc khối `KHUNG THỰC HIỆN` và `config()` trong
  `dbt/models/gold/gold_training_set.sql`
- Trong bốn kỹ thuật idempotent ở slide, kỹ thuật nào phù hợp với bảng *entity* có bản ghi bị cập nhật (`op='u'`)?
- Mở `dags/ai_training_pipeline.py` — hai tham số nào khiến thao tác Clear Task
  trở nên rủi ro?

**Tiêu chí đạt**
- `make verify` báo `ỔN ĐỊNH ✓` cho `gold_training_set`
- Số hàng bằng `expected/gold_training_set.count`
- Chạy thêm lượt thứ tư, thứ năm vẫn không đổi

---

### Nhiệm vụ 2 — Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ *(35 phút)*

> **Phiếu sự cố #1043.** "`gold_feature_daily` thiếu khoảng 5% so với đối chiếu
> thủ công. Kỳ lạ là chỉ thiếu ở những ngày đã chạy xong từ lâu, ngày mới thì đủ."

**Cần thu thập trước khi sửa**
- Đo hiệu số giữa `_ingested_at` và `event_time` trong Bronze. Phân bố ra sao?
  **P99 bằng bao nhiêu?**
- Đọc điều kiện lọc trong khối `is_incremental()` của `gold_feature_daily.sql`.
  Mốc so sánh là đại lượng nào?
- Một bản ghi *xảy ra* ngày 08-12 nhưng *tới kho* ngày 08-15 sẽ được xử lý ở
  lượt chạy nào?

**Tiêu chí đạt**
- Số hàng khớp `expected/gold_feature_daily.count`
- Báo cáo nêu rõ **giá trị P99 đo được** và lookback được chọn dựa trên giá trị đó
- Vẫn `ỔN ĐỊNH ✓` — thay đổi ở nhiệm vụ 2 không được làm hỏng nhiệm vụ 1

---

### Nhiệm vụ 3 — Kiểu dữ liệu cột `priority` thay đổi giữa chu kỳ *(30 phút)*

> **Phiếu sự cố #1047.** "Team backend đổi kiểu cột `priority` từ số sang chuỗi
> hôm 08-10, có thông báo trên Slack. Pipeline không hề dừng. Nhưng model phân
> loại từ hôm đó dự đoán kém hẳn."

**Cần thu thập trước khi sửa**
- `select priority, count(*) from silver_tickets group by 1` — bất thường nằm ở đâu?
- Đối chiếu với `select priority_raw, count(*) from bronze_tickets_cdc group by 1`
- Mở `dbt/models/silver/schema.yml` — `contract` đang ở trạng thái nào?
- **Câu hỏi thiết kế:** dữ liệu sai kiểu nên làm dừng cả DAG, hay nên được tách
  riêng để pipeline chạy tiếp? Và có phải mọi giá trị bất thường đều là dữ liệu
  lỗi không?

**Tiêu chí đạt**
- `contract` được bật, `dbt test` pass
- `quarantine_tickets` chứa **đúng** những bản ghi sai kiểu, số hàng khớp
  `expected/quarantine_tickets.count`
- `silver_tickets.priority` không còn NULL và luôn thuộc miền 1..4
- Pipeline **không** dừng khi gặp bản ghi lỗi — bản ghi được tách ra và quá
  trình xử lý tiếp tục

---

### Bài mở rộng *(không bắt buộc)*

Xong sớm thì có hai bài nữa trong [EXTRA.md](EXTRA.md), mỗi bài **+5 điểm**:
tối ưu query dashboard chậm (partition + small-file problem), và xử lý
consumer bị kill giữa batch (delivery semantics).

---

## 6. Nộp bài

1. **Repo** đã chỉnh sửa (link Git hoặc file nén — chạy `make clean` trước khi nén)
2. **Kết quả `make verify`** — dán nguyên output ba lượt chạy
3. **Báo cáo một trang** theo [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md). Mỗi
   nhiệm vụ trình bày: triệu chứng → **nguyên nhân** → cách khắc phục →
   bằng chứng. Nhiệm vụ 2 bắt buộc phải có giá trị **P99** đo được.

## 7. Đánh giá

Thang 100 điểm, chi tiết trong [RUBRIC.md](RUBRIC.md).

| Tiêu chí | Điểm |
|---|---|
| Ba lượt chạy cho checksum giống hệt nhau | 30 |
| Số hàng các bảng Gold khớp `expected/` | 30 |
| `contract` bật, `dbt test` pass, `quarantine_tickets` đúng | 20 |
| Báo cáo nêu đúng **nguyên nhân**, không chỉ mô tả cách khắc phục | 20 |
| *(thưởng)* mỗi bài trong [EXTRA.md](EXTRA.md) | +5 |

> Khắc phục đúng nhưng không giải thích được cơ chế sẽ mất 20 điểm cuối. Trong
> môi trường vận hành, phần giải thích là yếu tố ngăn lỗi tái diễn.
