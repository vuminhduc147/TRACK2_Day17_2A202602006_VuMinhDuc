-- Silver: sự kiện runtime đã dedup theo event_id.
-- Model này không chứa lỗi cần sửa. Tuy vậy hai cột `event_time` và
-- `_ingested_at` ở đây là dữ liệu đầu vào cho phép đo của nhiệm vụ 2.

{{ config(materialized = 'table') }}

select
    event_id,
    ticket_id,
    customer_id,
    customer_name,
    segment,
    event_type,
    model,
    latency_ms,
    tokens_in,
    tokens_out,
    is_escalated,
    event_time,
    _ingested_at,
    event_time::date                                          as event_date,
    _ingested_at::date                                        as ingested_date
from {{ source('bronze', 'bronze_events') }}
qualify row_number() over (partition by event_id order by _ingested_at) = 1
