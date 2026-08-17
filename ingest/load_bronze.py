"""Bronze loader — nạp dữ liệu THÔ của MỘT ngày vận hành vào kho.

Nguyên tắc của tầng Bronze trong lab này:

  * giữ nguyên payload gốc, không ép kiểu, không sửa giá trị
    (vì thế `priority` được giữ ở dạng VARCHAR — Bronze không có quyền
     phán xét dữ liệu nguồn);
  * chỉ nạp các bản ghi có `_ingested_at` rơi đúng vào ngày vận hành,
    giống hệt một job chạy theo lịch hằng ngày;
  * idempotent sẵn: xoá partition của ngày rồi nạp lại.

=> Tầng Bronze không chứa lỗi cần sửa. Bốn nhiệm vụ nằm ở tầng Silver, tầng
   Gold và ở storage layout.
"""

from __future__ import annotations

import sys

import duckdb

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from tools.common import SEED  # noqa: E402

RAW_TABLES = ("raw_tickets_cdc", "raw_events", "raw_transcripts")


def ensure_raw(con: duckdb.DuckDBPyConnection, force: bool = False) -> None:
    """Đọc ba file seed vào kho một lần (đóng vai trò 'landing zone')."""
    have = {
        r[0]
        for r in con.execute(
            "select table_name from information_schema.tables"
        ).fetchall()
    }
    if not force and all(t in have for t in RAW_TABLES):
        return

    con.execute(f"""
        create or replace table raw_tickets_cdc as
        select
            j ->> '$.op'                                   as op,
            j ->> '$.ticket_id'                            as ticket_id,
            (j ->> '$.cdc_seq')::int                       as cdc_seq,
            (j ->> '$.event_time')::timestamp              as event_time,
            (j ->> '$._ingested_at')::timestamp            as _ingested_at,
            j ->> '$._source'                              as _source,
            j ->> '$.customer_id'                          as customer_id,
            j ->> '$.customer_name'                        as customer_name,
            j ->> '$.segment'                              as segment,
            -- Bronze giữ nguyên kiểu chuỗi: nguồn có lúc gửi số, có lúc gửi nhãn.
            case when json_type(j, '$.priority') = 'NULL' then null
                 else j ->> '$.priority' end               as priority_raw,
            j ->> '$.category'                             as category,
            j ->> '$.channel'                              as channel,
            j ->> '$.status'                               as status,
            (j ->> '$.csat')::int                          as csat,
            (j ->> '$.first_response_sec')::int            as first_response_sec,
            j ->> '$.subject'                              as subject,
            j ->> '$.body'                                 as body
        from read_json_objects('{SEED / "tickets_cdc.jsonl"}',
                               format = 'newline_delimited') t(j);
    """)

    con.execute(f"""
        create or replace table raw_events as
        select * from read_json('{SEED / "events.jsonl"}',
                                format = 'newline_delimited',
                                columns = {{
                                  'event_id':      'VARCHAR',
                                  'ticket_id':     'VARCHAR',
                                  'customer_id':   'VARCHAR',
                                  'customer_name': 'VARCHAR',
                                  'segment':       'VARCHAR',
                                  'event_type':    'VARCHAR',
                                  'model':         'VARCHAR',
                                  'latency_ms':    'INTEGER',
                                  'tokens_in':     'INTEGER',
                                  'tokens_out':    'INTEGER',
                                  'is_escalated':  'BOOLEAN',
                                  'event_time':    'TIMESTAMP',
                                  '_ingested_at':  'TIMESTAMP',
                                  '_topic':        'VARCHAR'
                                }});
    """)

    con.execute(f"""
        create or replace table raw_transcripts as
        select * from read_json('{SEED / "transcripts.jsonl"}',
                                format = 'newline_delimited',
                                columns = {{
                                  'transcript_id': 'VARCHAR',
                                  'ticket_id':     'VARCHAR',
                                  'lang':          'VARCHAR',
                                  'turns':         'VARCHAR[]',
                                  'event_time':    'TIMESTAMP',
                                  '_ingested_at':  'TIMESTAMP',
                                  '_source':       'VARCHAR'
                                }});
    """)


BRONZE = {
    "bronze_tickets_cdc": "raw_tickets_cdc",
    "bronze_events": "raw_events",
    "bronze_transcripts": "raw_transcripts",
}


def load_day(con: duckdb.DuckDBPyConnection, run_date: str) -> dict[str, int]:
    """Nạp partition `_ingested_at::date = run_date`. Gọi lại bao nhiêu lần cũng như nhau."""
    ensure_raw(con)
    out: dict[str, int] = {}
    for bronze, raw in BRONZE.items():
        con.execute(f"""
            create table if not exists {bronze} as
            select *, DATE '1970-01-01' as _batch_date from {raw} limit 0;
        """)
        # idempotent: xoá partition cũ trước khi ghi lại
        con.execute(f"delete from {bronze} where _batch_date = DATE '{run_date}'")
        con.execute(f"""
            insert into {bronze}
            select *, DATE '{run_date}' as _batch_date
            from {raw}
            where _ingested_at >= TIMESTAMP '{run_date} 00:00:00'
              and _ingested_at <  TIMESTAMP '{run_date} 00:00:00' + interval 1 day;
        """)
        out[bronze] = con.execute(
            f"select count(*) from {bronze} where _batch_date = DATE '{run_date}'"
        ).fetchone()[0]
    return out
