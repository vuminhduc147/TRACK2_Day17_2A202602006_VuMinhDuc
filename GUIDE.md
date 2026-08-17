# HƯỚNG DẪN THỰC HIỆN — LAB 17

Tài liệu này không cho lời giải. Nó cho bạn **trình tự thao tác**: đo cái gì,
đọc file nào, và phải trả lời được câu hỏi nào trước khi sửa code.

Khung pseudo-code của từng nhiệm vụ nằm ngay trong file cần sửa, dưới dạng
comment `KHUNG THỰC HIỆN`:

| Nhiệm vụ | File chứa khung pseudo-code |
|---|---|
| 1 | `dbt/models/gold/gold_training_set.sql` |
| 2 | `dbt/models/gold/gold_feature_daily.sql` |
| 3 | `dbt/macros/normalize_priority.sql`, `dbt/models/silver/silver_tickets.sql`, `dbt/models/silver/quarantine_tickets.sql` |

Hai bài mở rộng (không bắt buộc) nằm trong [EXTRA.md](EXTRA.md).

---

## 0. Chuẩn bị môi trường

```bash
git clone https://github.com/VinUni-AI20k/Day17-Track2-DataPipeline.git
cd Day17-Track2-DataPipeline
make setup
```

`make setup` làm bốn việc: tạo `.venv`, cài `duckdb` + `dbt-duckdb`, sinh 14
ngày dữ liệu vào `seed/`, và ghi **baseline** cho nhiệm vụ 4 vào
`expected/dashboard_baseline.json`.

> Chỉ chạy `make setup` một lần. Baseline đã ghi sẽ không bị ghi đè ở các lần
> sau, nhưng `data/gold_events/` thì bị sinh lại — nếu đã làm xong nhiệm vụ 4,
> bạn phải chạy lại `make compact`.

```bash
make pipeline    # chạy pipeline một lượt
make verify      # chạy ba lượt, in bảng đánh giá
```

Trong lúc làm, `make quick` (một lượt) đủ để kiểm tra nhanh. `make verify`
(ba lượt) dùng khi cần xác nhận tính ổn định.

### Công cụ query

Mở terminal thứ hai và định nghĩa hàm sau để query warehouse:

```bash
q() { .venv/bin/python -c "
import duckdb, sys
duckdb.connect('warehouse.duckdb').sql(sys.argv[1]).show(max_rows=40)
" "$1"; }

q "select count(*) from gold_training_set"
```

### Các bảng sẽ dùng

| Bảng | Nội dung |
|---|---|
| `bronze_tickets_cdc` | CDC thô, `priority_raw` kiểu VARCHAR |
| `bronze_events` | Event thô, có `event_time` và `_ingested_at` |
| `silver_tickets` | Trạng thái mới nhất của mỗi ticket |
| `silver_events` | Event đã dedup, có `event_date` |
| `gold_training_set` | 1 row / 1 ticket |
| `gold_feature_daily` | 1 row / (ngày, customer) |
| `gold_doc_chunks` | 1 row / 1 chunk — **nhóm đối chứng**, không có lỗi |

---

## 0b. dbt trong 10 phút — đọc nếu bạn chưa từng dùng dbt

Bạn không cần học dbt để làm lab này. Chỉ cần năm khái niệm sau.

**Model = một file `.sql` = một bảng.** File `models/gold/gold_training_set.sql`
tạo ra bảng `gold_training_set`. Bạn viết đúng một câu `SELECT`; dbt tự bọc nó
thành `CREATE TABLE ...` hoặc `INSERT ...` tuỳ cấu hình.

**`{{ ref('ten_model') }}`** thay cho tên bảng khi lấy dữ liệu từ model khác.
Viết `from {{ ref('silver_tickets') }}` thay vì `from silver_tickets`. Nhờ đó
dbt biết thứ tự chạy: silver trước, gold sau.

**`{{ config(...) }}`** ở đầu file quyết định dbt sinh ra câu lệnh ghi nào:

```
materialized = 'table'         -> DROP rồi CREATE lại toàn bộ mỗi lần chạy.
                                  Luôn ổn định. Dùng cho các model silver.
materialized = 'incremental'   -> Lần đầu CREATE, các lần sau chỉ ghi thêm
                                  phần mới. Nhanh hơn nhiều — và mọi lỗi
                                  trong lab này đều nằm ở loại model này.
```

Với `incremental` còn hai tham số nữa quyết định *cách* ghi thêm:

```
unique_key            = 'cot_khoa'   -> cho dbt biết dòng nào là "cùng một dòng"
incremental_strategy  = 'merge' | 'delete+insert' | 'append'
```

**Khối `{% if is_incremental() %} ... {% endif %}`** chỉ có tác dụng từ lần
chạy thứ hai trở đi (khi bảng đã tồn tại). Bên trong thường là mệnh đề `WHERE`
giới hạn phần dữ liệu cần xử lý. Lần chạy đầu tiên, khối này bị bỏ qua và model
quét toàn bộ nguồn.

**Macro** = một đoạn SQL đặt tên, để trong `dbt/macros/`. Gọi bằng
`{{ ten_macro('tham_so') }}` và dbt chèn nội dung vào đúng chỗ đó. Dùng khi
nhiều model cần chung một logic.

### Xem dbt thực sự sinh ra SQL gì

Đây là cách gỡ rối hiệu quả nhất khi model chạy không như bạn nghĩ:

```bash
make pipeline
cat dbt/target/run/lab17/models/gold/gold_training_set.sql
```

File đó là câu lệnh SQL **thật** mà dbt gửi xuống database. Nếu bạn không chắc
`config` của mình có tác dụng gì, hãy đọc file này trước khi đoán.

### Chạy một model

```bash
cd dbt && ../.venv/bin/dbt run --select silver_tickets --profiles-dir .
```

Trong lab, `make pipeline` đã gọi dbt cho cả 14 ngày nên bạn hiếm khi cần lệnh
này — nhưng nó hữu ích khi muốn thử nhanh một model.

---

## 1. Nhiệm vụ 1 — `gold_training_set` tăng row sau mỗi lần chạy

### 1.1 Tái hiện hiện tượng

```bash
make reset
make pipeline && q "select count(*) from gold_training_set"
make pipeline && q "select count(*) from gold_training_set"
```

Ghi lại hai con số. Quan hệ giữa chúng cho biết lượt chạy thứ hai đã thực hiện
phép ghi nào lên bảng đích.

### 1.2 Khoanh vùng lỗi

```sql
-- Bảng đích có ticket nào xuất hiện nhiều hơn một lần không?
select ticket_id, count(*) as n
from gold_training_set
group by 1 having n > 1
order by n desc limit 10;

-- Đối chiếu với bảng nguồn
select count(*) as tong_row, count(distinct ticket_id) as so_ticket
from silver_tickets;
```

Nếu source giữ đúng 1 row / 1 ticket mà target thì không, lỗi nằm ở **cách
model được materialize**, không nằm ở dữ liệu đầu vào.

### 1.3 Đọc config materialization

Mở `dbt/models/gold/gold_training_set.sql`, đọc khối `KHUNG THỰC HIỆN` ở đầu
file rồi tới khối `config()`. Một incremental model của dbt được quyết định bởi
ba tham số; hiện mới khai báo một.

Cần trả lời:

1. Khi thiếu `unique_key`, dbt generate ra câu lệnh ghi nào?
2. Với câu lệnh đó, chạy lại cùng một ngày lần thứ hai thì row cũ bị **ghi đè**
   hay bị **ghi thêm**?

### 1.4 Đặc điểm của source CDC

```sql
select op, count(*) from bronze_tickets_cdc group by 1 order by 1;
```

Source có row `op = 'u'`. Nghĩa là một ticket được tạo ngày D1 rồi update ngày
D2 sẽ lọt qua điều kiện lọc theo `run_date` **hai lần trong cùng một lượt
chạy** — hai lần đó rơi vào hai partition ngày khác nhau.

Hệ quả: cách làm "xoá partition của ngày rồi ghi lại" không xử lý được trường
hợp này. Ba câu hỏi để chọn strategy:

- Grain của bảng là entity hay event?
- Natural key của grain đó là gì?
- Cần `append`, `delete+insert` theo partition ngày, hay `merge` theo key?

### 1.5 Parameter của DAG

Mở `dags/ai_training_pipeline.py`. Phiếu #1041 ghi nhận thao tác **Clear Task**
trên Airflow. Hai parameter ở phần `TODO` quyết định hệ quả của thao tác đó:

- `catchup` — Airflow có tự schedule chạy bù mọi ngày trong quá khứ không?
- `max_active_runs` — nhiều run có được phép ghi đồng thời vào cùng một bảng không?

Set lại cả hai. `make verify` đọc file này bằng AST và check giá trị.

Lưu ý khi viết report: hai parameter này chỉ **giảm tần suất kích hoạt** lỗi,
chúng không phải root cause. Sửa DAG mà không sửa model thì `make verify` vẫn đỏ.

### 1.6 Tiêu chí hoàn thành

```bash
make verify
```

- `gold_training_set`: cột `ỔN ĐỊNH` ✓ và số row bằng **12.480**
- Dòng `gold_training_set: 1 hàng / 1 ticket`: ✓
- Dòng `DAG: catchup / max_active_runs`: ✓

Sau khi xong nhiệm vụ 1, `gold_feature_daily` **vẫn** ở 8.645 row. Đó là lỗi
độc lập, thuộc nhiệm vụ 2.

---

## 2. Nhiệm vụ 2 — `gold_feature_daily` thiếu row ở các ngày cũ

### 2.1 Đo phân bố độ trễ của dữ liệu

```sql
select
    date_diff('hour', event_time, _ingested_at) as do_tre_gio,
    count(*)
from bronze_events
group by 1 order by 1;
```

Phân bố có hai cụm tách biệt. Lượng hoá bằng percentile:

```sql
select
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.50) as p50_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.95) as p95_ngay,
    quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99) as p99_ngay,
    max(date_diff('second', event_time, _ingested_at)/86400.0)                 as max_ngay,
    avg(case when _ingested_at > event_time + interval 1 day then 1.0 else 0 end)   as ty_le_late
from bronze_events;
```

**Ghi P99 vào report ngay ở bước này.** Đó là căn cứ định lượng cho lookback
window, và là một trong hai con số bắt buộc của rubric.

### 2.2 Xác định tập row bị thiếu

```sql
-- Kỳ vọng: 14 ngày × 650 customer = 9.100 cặp
select count(*) from gold_feature_daily;

-- Các cặp (ngày, customer) có trong Silver nhưng không có trong Gold
select s.event_date, count(distinct s.customer_id) as so_cap_thieu
from silver_events s
left join gold_feature_daily g
  on g.event_date = s.event_date and g.customer_id = s.customer_id
where g.customer_id is null
group by 1 order by 1;
```

Nhìn cột `event_date`: các cặp bị thiếu tập trung ở ngày mới hay ngày cũ?

Kiểm chứng giả thuyết bằng thời điểm dữ liệu tới warehouse:

```sql
select s.event_date,
       min(s.ingested_date) as toi_som_nhat,
       max(s.ingested_date) as toi_muon_nhat,
       count(*)             as so_event
from silver_events s
left join gold_feature_daily g
  on g.event_date = s.event_date and g.customer_id = s.customer_id
where g.customer_id is null
group by 1 order by 1 limit 5;
```

### 2.3 Phân tích điều kiện lọc incremental

Mở `dbt/models/gold/gold_feature_daily.sql`:

```sql
where event_date > (select max(event_date) from {{ this }})
```

Đọc thành lời: *chỉ xử lý những event_date lớn hơn event_date lớn nhất đã có
trong target*. Cần trả lời:

1. Một event có `event_date = 08-12` và `_ingested_at = 08-15`: tại lượt chạy
   ngày 08-15, `max(event_date)` trong target bằng bao nhiêu? Event đó có lọt
   qua điều kiện không? Ngày 08-16 thì sao?
2. Đổi `>` thành `>=` đã đủ chưa? Toán tử đó nới window thêm mấy ngày?
3. Lookback window cần lùi bao nhiêu ngày? Căn cứ vào **P99** hay vào **max**?
   Mỗi ngày lùi thêm tốn thêm gì, và tốn một lần hay tốn ở mọi lượt chạy sau này?

### 2.4 Ràng buộc đi kèm khi nới window

Window rộng hơn nghĩa là cùng một cặp `(event_date, customer_id)` sẽ được tính
lại ở nhiều lượt chạy. Nếu model chỉ biết `insert`, kết quả các lần tính sẽ
cộng dồn — tức là tái tạo đúng lỗi của nhiệm vụ 1 trên một bảng khác.

Grain ở đây gồm **hai cột**. `unique_key` của dbt nhận vào một list.

### 2.5 Tiêu chí hoàn thành

```bash
make verify
```

- `gold_feature_daily`: `ỔN ĐỊNH` ✓ và số row bằng **9.100**
- `gold_training_set` giữ nguyên **12.480** và `ỔN ĐỊNH` ✓

---

## 3. Nhiệm vụ 3 — Kiểu dữ liệu cột `priority` đổi giữa chu kỳ

### 3.1 Tìm điểm bất thường trong phân bố giá trị

```sql
select priority, count(*)
from silver_tickets
group by 1 order by 1 nulls last;
```

Hai bất thường cần ghi nhận: tỷ lệ `NULL` rất lớn, và sự có mặt của `0`, `5`,
`-1` — trong khi contract quy định `priority ∈ 1..4`.

Đối chiếu với source và xác định mốc thời gian:

```sql
select priority_raw, count(*)
from bronze_tickets_cdc
group by 1 order by 2 desc;

select event_time::date as ngay,
       count(*) filter (where try_cast(priority_raw as integer) is null) as khong_phai_so,
       count(*)                                                          as tong
from bronze_tickets_cdc
group by 1 order by 1;
```

### 3.2 Phân loại giá trị source

`priority_raw` có ba nhóm, và ba nhóm này phải xử lý khác nhau:

| Nhóm | Ví dụ | Bản chất | Xử lý |
|---|---|---|---|
| Số hợp lệ | `1` `2` `3` `4` | Đúng contract ban đầu | Giữ nguyên |
| Nhãn chuỗi | `urgent` `high` `medium` `low` | **Schema evolution**: source đổi cách biểu diễn, ý nghĩa không đổi | **Map** về 1..4 |
| Giá trị không hợp lệ | `P1` `unknown` `0` `5` `-1` `''` `null` | Dữ liệu lỗi thật | Đưa vào **quarantine** |

Tiêu chí phân biệt nhóm 2 và nhóm 3: *giá trị này có mang đúng thông tin của
contract cũ, chỉ khác cách biểu diễn hay không?* Có thì map, không thì quarantine.

Mapping theo tài liệu API của team backend: `urgent → 1`, `high → 2`,
`medium → 3`, `low → 4`.

> Xử lý nhóm 2 như nhóm 3 là lỗi phổ biến nhất ở nhiệm vụ này. Nếu quarantine
> toàn bộ row từ 08-10 trở đi, `quarantine_tickets` sẽ có hàng nghìn row thay
> vì con số kỳ vọng, đồng thời vứt bỏ một lượng lớn dữ liệu hợp lệ chỉ vì
> source đổi format.

### 3.3 Bốn chỗ cần sửa

Mọi file đã được nối sẵn với nhau. Bạn chỉ điền vào chỗ trống, không phải tạo
file mới.

**(a) `dbt/macros/normalize_priority.sql`** — thay `try_cast(...)` bằng một
khối `CASE` xử lý đủ ba nhóm ở mục 3.2. Trả về `NULL` cho nhóm 3.

Macro này đang được **cả hai** model dùng (`silver_tickets` để lấy giá trị,
`quarantine_tickets` để tìm bản ghi lỗi), nên sửa một chỗ là cả hai cùng đổi —
chúng không thể lệch nhau.

**(b) `dbt/models/silver/silver_tickets.sql`** — loại các bản ghi mà macro trả
về `NULL` ra khỏi Silver.

> ⚠️ Thứ tự quyết định số row của bảng. Hiện tại file đang xếp hạng
> (`row_number`) trước rồi mới chọn. Nếu bạn chỉ thêm điều kiện lọc vào cuối,
> ticket nào có bản ghi **mới nhất** bị hỏng sẽ biến mất khỏi Silver — số
> ticket tụt từ 12.480 xuống 12.168.
>
> Đúng phải là **lọc trước, xếp hạng sau**. Bạn loại *bản ghi* hỏng, không loại
> cả *ticket*: ticket đó vẫn còn trạng thái hợp lệ từ lần cập nhật trước.
> `make verify` có dòng riêng bắt lỗi này.

**(c) `dbt/models/silver/quarantine_tickets.sql`** — model đã viết sẵn, chỉ còn
mệnh đề `where false`. Thay bằng điều kiện "macro trả về NULL".

**(d) `dbt/models/silver/schema.yml`** — đổi `enforced: false` thành `true`, và
bỏ comment khối `tests:` ở cột `priority` rồi điền danh sách giá trị hợp lệ.

Contract ràng buộc **kiểu dữ liệu**; miền giá trị là việc của test. Cần cả hai —
contract một mình vẫn cho `priority = 99` đi qua, vì 99 đúng là integer.

### 3.4 Câu hỏi thiết kế

Hai câu này cần trả lời trong report:

1. Nên chặn dữ liệu lỗi ở tầng Bronze hay tầng Silver? Nếu Bronze từ chối row
   lỗi thì việc điều tra sự cố về sau gặp trở ngại gì?
2. Vì sao không để `dbt test` fail và dừng cả DAG khi gặp row lỗi? Cân nhắc quy
   mô: số row lỗi so với tổng số row hợp lệ đang chờ được phục vụ.

### 3.5 Tiêu chí hoàn thành

```bash
make verify
```

- `dbt test`: ✓, và tổng số test **lớn hơn 9** (bản gốc có 9 test)
- `silver_tickets.priority ∈ 1..4, không NULL`: ✓
- `quarantine_tickets`: đúng số row kỳ vọng và `ỔN ĐỊNH` ✓
- `gold_training_set` giữ nguyên **12.480**

---

## 4. Viết report

Dùng [REPORT_TEMPLATE.md](REPORT_TEMPLATE.md). Mỗi nhiệm vụ trình bày bốn mục:

```
Triệu chứng   : hiện tượng quan sát được từ phía vận hành
Root cause    : cơ chế gây ra hiện tượng đó — một câu, cụ thể
Cách fix      : sửa gì, ở file nào
Bằng chứng    : số liệu trước và sau
```

Mục **Root cause** chiếm toàn bộ 10 điểm cuối. "Thêm `unique_key`" là *cách fix*,
không phải root cause. Root cause là phát biểu về cơ chế: *incremental model
không khai báo key, nên dbt generate ra câu `INSERT`; chạy lại cùng một partition
sẽ ghi thêm thay vì ghi đè.*

---

## Phụ lục A — Xử lý sự cố thường gặp

| Hiện tượng | Hướng xử lý |
|---|---|
| `Can't open a connection to same database file` | Có tiến trình khác đang mở `warehouse.duckdb`; đóng shell DuckDB đang chạy |
| `make verify` lỗi lạ sau nhiều lần sửa | `make clean && make pipeline` |
| Không hiểu dbt sinh ra SQL gì | Đọc `dbt/target/run/lab17/models/.../<model>.sql` — xem mục 0b |
| Số row đúng nhưng `ỔN ĐỊNH` ✗ | Model đang `insert` thay vì `merge` / `delete+insert` |
| `ỔN ĐỊNH` ✓ nhưng thiếu row | Điều kiện lọc bỏ sót dữ liệu — xem nhiệm vụ 2 |
| `quarantine_tickets` có hàng nghìn row | Đang quarantine cả nhãn chuỗi hợp lệ — xem mục 3.2 |
| `silver_tickets` dưới 12.480 row | Đang loại cả ticket thay vì chỉ loại row CDC lỗi — xem mục 3.3(a) |
