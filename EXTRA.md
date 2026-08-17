# BÀI MỞ RỘNG — dành cho ai xong sớm

Ba nhiệm vụ chính trong [README.md](README.md) đã đủ để đạt 100 điểm.
Hai bài dưới đây là **điểm thưởng**, mỗi bài **+5**.

Chúng cần thêm dữ liệu, sinh bằng một lệnh riêng (khoảng 30 giây):

```bash
make seed-extra
```

Lệnh này tạo `data/gold_events/` — 5.000 file Parquet nhỏ — và ghi lại
baseline đo hiệu năng vào `expected/dashboard_baseline.json`.

> Chỉ chạy khi bạn thật sự làm bài mở rộng. Nếu đã chạy rồi mà sau đó gọi
> `make seed`, dữ liệu này bị xoá — chạy lại `make seed-extra`.

---

## Bài A — Query dashboard chậm *(25 phút, +5 điểm)*

> **Phiếu sự cố #1052.** "Dashboard của đội CSKH mất 38 giây mới load.
> Ba tháng trước chỉ 2 giây. Không ai sửa dòng code nào."

File liên quan: `queries/dashboard.sql`, `tools/compact.py`, `data/gold_events/`

### A.1 Đo trước khi sửa

```bash
make explain            # rows scanned, rows on disk, files, result hash
make plan               # in thêm cây EXPLAIN ANALYZE
ls data/gold_events | wc -l
du -sh data/gold_events
```

Ghi ba con số vào report: `rows scanned`, `files`, `rows on disk`.

> **Vì sao `rows scanned` lớn hơn `rows on disk`.** DuckDB đọc Parquet theo lô
> và làm tròn lên theo từng file: một file vài chục hàng vẫn tốn khối lượng
> đọc tương đương khoảng 1.000 hàng. Chênh lệch đó chính là small-file problem
> hiện thành con số.
>
> Bài này chấm theo `rows scanned` chứ không theo thời gian, vì thời gian phụ
> thuộc cấu hình máy và trạng thái cache của OS. `tools/explain.py` cũng ép
> `threads = 1` để mọi máy ra cùng một số.

### A.2 Đối chiếu filter với storage layout

Mở `queries/dashboard.sql`:

1. Query filter theo những cột nào? (có hai điều kiện)
2. Tên file trong `data/gold_events/` có mang thông tin của cột nào không?

Nếu path không mang thông tin filter, engine buộc phải mở toàn bộ file rồi mới
biết file nào có ích.

Ngoài ra xem dạng của điều kiện filter:

```sql
where strftime(event_time, '%Y-%m-%d') = '2026-08-09'
```

Cột bị bọc trong một function call. Engine không so được kết quả function với
tên thư mục partition, cũng không so được với min/max statistics của row group.
Cần viết lại sao cho **cột đứng một mình ở một vế** (predicate sargable).

### A.3 Sửa

Viết `tools/compact.py` — khung `COPY ... TO ...` và ba quyết định cần lý giải
đã có sẵn trong docstring của file. Sau đó sửa `queries/dashboard.sql` trỏ vào
dataset mới, bật `hive_partitioning`, viết lại điều kiện filter.

```bash
make compact
make explain
```

### ✅ Đạt khi

- `rows scanned` giảm tối thiểu **10 lần** so với baseline
- `files` giảm từ 5.000 xuống hàng chục
- `result hash` **không đổi** — nếu đổi, ngữ nghĩa query đã bị sửa và bài này
  không được tính điểm

> Lỗi sai hướng hay gặp: thêm index. Parquet trên đĩa không có index. Thứ duy
> nhất bạn điều khiển được là **file nằm ở đâu** và **hàng nằm theo thứ tự nào
> trong file**.

---

## Bài B — Consumer gặp sự cố giữa batch *(20 phút, +5 điểm)*

> `make crash-test` kill tiến trình consumer ở giữa một batch ghi rồi khởi
> động lại, sau đó đối chiếu số hàng. Bạn **mất** hàng hay bị **trùng** hàng?

File liên quan: `ingest/consumer.py`, `ingest/log_client.py`

Bài này **không cần** `make seed-extra`.

### B.1 Tái hiện

```bash
make crash-test
```

### B.2 Phân tích thứ tự thao tác

Mở `ingest/consumer.py`, đọc khối `KHUNG THỰC HIỆN` ở đầu file và khối được
đánh dấu trong hàm `consume()`:

```python
consumer.commit()                 # commit offset
maybe_crash(batch_no, crash_at)   # sự cố xảy ra tại đây
write_batch(con, batch)           # ghi dữ liệu
```

Trả lời:

- Nếu tiến trình chết tại `maybe_crash()`, batch hiện tại đã được ghi chưa?
  Offset đã dịch chưa? Lần restart sẽ đọc từ đâu?
- Nếu đảo thứ tự thành ghi trước, commit sau, lần restart sẽ đọc lại batch đó.
  Với câu `INSERT` hiện tại, hệ quả là gì?

Đây là hai delivery semantics **at-most-once** và **at-least-once**.
Exactly-once không tồn tại ở tầng transport; thứ chọn được là at-least-once
cộng với một phép ghi idempotent.

### B.3 Phép ghi idempotent

DuckDB hỗ trợ `insert ... on conflict (...) do update set ...`, nhưng chỉ khi
cột key có constraint `primary key` hoặc `unique`. Xem hằng `DDL` ở đầu file.

Câu hỏi cho report: `DO UPDATE` khác `DO NOTHING` ở điểm nào khi một message
được replay với nội dung đã đổi?

### ✅ Đạt khi

```bash
make crash-test     # NHIỆM VỤ MỞ RỘNG B: ĐẠT
make verify         # ba nhiệm vụ chính không bị ảnh hưởng
```

---

## Ghi vào report

Mỗi bài mở rộng viết đúng bốn dòng như các nhiệm vụ chính:
triệu chứng → root cause → cách fix → số liệu trước/sau.

Bài A bắt buộc có `rows scanned` trước và sau.
