#!/usr/bin/env python3
"""Sinh 14 ngày dữ liệu giả lập cho LAB 17 (2026-08-03 → 2026-08-16).

Bạn KHÔNG cần sửa file này. Nó chạy một lần trong `make setup`.

Thêm cờ --extra để sinh thêm data/gold_events/ (5.000 file Parquet) — chỉ
cần cho bài mở rộng trong EXTRA.md, không cần cho ba nhiệm vụ chính.

Ba nguồn được sinh ra:

  seed/tickets_cdc.jsonl   CDC từ Postgres  (op = c / u / d)
  seed/events.jsonl        Kafka topic `ai-events`
  seed/transcripts.jsonl   File JSON trên S3

Mọi con số đều được ép chính xác bằng ROW_NUMBER (không dựa vào xác suất),
nên chạy trên máy nào cũng ra cùng một bộ dữ liệu.
"""

from __future__ import annotations

import json
import pathlib
import sys
import time

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
ROOT = HERE.parent
EXPECTED = ROOT / "expected"

# ---------------------------------------------------------------- tham số
DAY0 = "2026-08-03"
N_DAYS = 14
SCHEMA_CHANGE_DAY = "2026-08-10"  # priority: int -> string kể từ ngày này

N_CUSTOMERS = 650
N_TICKETS_RAW = 12_735  # tổng ticket được tạo
N_DELETED = 255  # bị xoá ngay trong ngày -> không vào Gold
N_UPDATES = 998  # ~8% ticket sống bị sửa vào một ngày sau đó
N_BAD_PRIORITY = 312  # bản ghi sai kiểu -> phải vào quarantine

N_TRANSCRIPTS = 5_200
CHUNKS_PER_TRANSCRIPT = 6

ACME_EVENTS_PER_DAY = 3_500  # tạo skew ~40%
N_LATE_ONLY_COMBOS = 455  # (ngày, khách) chỉ có dữ liệu về muộn -> 5% hàng bị mất
LATE_RATE_PER_MILLE = 25  # thêm 2,5% bản ghi lẻ về muộn
LAST_LATE_DAY_IDX = 10  # 2026-08-13: sau ngày này không sinh dữ liệu muộn

N_GOLD_EVENT_FILES = 5_000  # nhiệm vụ 4: thư mục gồm 5.000 file nhỏ

PRIORITY_LABEL = {1: "urgent", 2: "high", 3: "medium", 4: "low"}
BAD_PRIORITY_VALUES = ["P1", "P2", "0", "5", "-1", "", None, "unknown"]

CATEGORIES = ["billing", "bug", "how_to", "outage", "refund", "account"]
CHANNELS = ["email", "chat", "voice", "api"]
STATUSES = ["open", "pending", "resolved", "closed"]
EVENT_TYPES = ["message_in", "message_out", "tool_call", "handoff", "resolve"]
MODELS = ["claude-haiku-4-5", "claude-sonnet-5", "claude-opus-5"]


def sql_list(values: list[str]) -> str:
    """['a', 'b'] -> chuỗi list literal của DuckDB."""
    return "[" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + "]"


def log(msg: str) -> None:
    print(f"  [seed] {msg}", flush=True)


# ---------------------------------------------------------------- dựng bảng
def build(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"""
        create or replace table dim_date as
        select (DATE '{DAY0}' + i::int) as event_date, i as day_idx
        from range({N_DAYS}) t(i);
    """)

    con.execute(f"""
        create or replace table dim_customer as
        select
            'C' || lpad((i + 1)::varchar, 4, '0')                       as customer_id,
            case when i = 0 then 'ACME'
                 else 'Cust_' || lpad((i + 1)::varchar, 4, '0') end     as customer_name,
            ['Enterprise', 'SMB', 'Startup'][(i % 3) + 1]               as segment
        from range({N_CUSTOMERS}) t(i);
    """)

    # ---- ticket gốc ------------------------------------------------------
    # ACME chiếm ~40% ticket -> skew thật, giống ví dụ trên slide.
    con.execute(f"""
        create or replace table ticket_base as
        with r as (
            select i as n, hash(i * 2654435761 + 7) as h from range({N_TICKETS_RAW}) t(i)
        )
        select
            'T' || lpad(n::varchar, 6, '0')                              as ticket_id,
            n, h,
            (h % {N_DAYS})::int                                          as create_day_idx,
            case when (h >> 8) % 100 < 40 then 'C0001'
                 else 'C' || lpad((((h >> 12) % {N_CUSTOMERS - 1}) + 2)::varchar, 4, '0')
            end                                                          as customer_id,
            (((h >> 20) % 4) + 1)::int                                   as priority_code,
            {sql_list(CATEGORIES)}[(((h >> 24) % {len(CATEGORIES)}) + 1)::bigint]  as category,
            {sql_list(CHANNELS)}[(((h >> 28) % {len(CHANNELS)}) + 1)::bigint]      as channel,
            (((h >> 32) % 5) + 1)::int                                   as csat,
            ((h >> 36) % 1400 + 60)::int                                 as first_response_sec,
            ((h >> 40) % 86400)::int                                     as sec_in_day
        from r;
    """)

    # 255 ticket bị xoá ngay trong ngày; 998 ticket bị sửa; 312 ticket nhận
    # bản ghi sai kiểu. Ba tập hoàn toàn rời nhau, chọn bằng ROW_NUMBER.
    con.execute(f"""
        create or replace table ticket as
        select *,
            (row_number() over (order by hash(h::varchar || '|del'), ticket_id)
             <= {N_DELETED})                                            as is_deleted_same_day
        from ticket_base;
    """)
    # Một thứ hạng duy nhất -> hai tập chắc chắn rời nhau.
    con.execute(f"""
        create or replace table ticket_live as
        select *,
            row_number() over (order by hash(h::varchar || '|pick'), ticket_id) as rk_pick
        from ticket
        where not is_deleted_same_day
          and create_day_idx <= {N_DAYS - 2};   -- còn ngày sau để sửa
    """)
    con.execute(f"""
        create or replace table upd_pick as
        select ticket_id from ticket_live where rk_pick <= {N_UPDATES};
    """)
    con.execute(f"""
        create or replace table bad_pick as
        select ticket_id from ticket_live
        where rk_pick > {N_UPDATES} and rk_pick <= {N_UPDATES + N_BAD_PRIORITY};
    """)

    n_live = con.execute(
        "select count(*) from ticket where not is_deleted_same_day"
    ).fetchone()[0]
    n_upd = con.execute("select count(*) from upd_pick").fetchone()[0]
    n_bad = con.execute("select count(*) from bad_pick").fetchone()[0]
    assert n_upd == N_UPDATES, n_upd
    assert n_bad == N_BAD_PRIORITY, n_bad
    log(f"ticket: {N_TICKETS_RAW} tạo / {N_DELETED} xoá / {n_live} sống "
        f"/ {n_upd} sửa / {n_bad} sai kiểu")

    # ---- CDC rows --------------------------------------------------------
    # op='c' : mọi ticket
    # op='d' : 255 ticket, cùng ngày tạo
    # op='u' : 998 ticket sửa hợp lệ + 312 ticket sửa sai kiểu, vào một ngày sau
    con.execute("""
        create or replace table upd_pick_all as
        select ticket_id from upd_pick
        union all
        select ticket_id from bad_pick;
    """)
    con.execute(f"""
        create or replace table cdc as
        with created as (
            select
                'c' as op, t.ticket_id, t.h,
                d.event_date + to_seconds(t.sec_in_day)                 as event_time,
                t.customer_id, t.priority_code, t.category, t.channel,
                'open' as status, t.csat, t.first_response_sec,
                0 as cdc_seq, false as is_bad
            from ticket t join dim_date d on d.day_idx = t.create_day_idx
        ),
        deleted as (
            select 'd' as op, t.ticket_id, t.h,
                d.event_date + to_seconds(least(t.sec_in_day + 2220, 86390))       as event_time,
                t.customer_id, t.priority_code, t.category, t.channel,
                'closed' as status, t.csat, t.first_response_sec,
                1 as cdc_seq, false as is_bad
            from ticket t join dim_date d on d.day_idx = t.create_day_idx
            where t.is_deleted_same_day
        ),
        updated as (
            select 'u' as op, t.ticket_id, t.h,
                d2.event_date + to_seconds(((t.h >> 44) % 80000)::int)  as event_time,
                t.customer_id, t.priority_code, t.category, t.channel,
                {sql_list(STATUSES)}[(((t.h >> 48) % {len(STATUSES)}) + 1)::bigint] as status,
                t.csat, t.first_response_sec,
                2 as cdc_seq,
                (b.ticket_id is not null) as is_bad
            from ticket t
            join upd_pick_all p on p.ticket_id = t.ticket_id
            left join bad_pick  b on b.ticket_id = t.ticket_id
            join dim_date d2
              on d2.day_idx = least(t.create_day_idx + 1 + (((t.h >> 52) % 3))::int,
                                    {N_DAYS - 1})
        )
        select * from created
        union all select * from deleted
        union all select * from updated;
    """)

    # ---- transcripts -----------------------------------------------------
    con.execute(f"""
        create or replace table transcript as
        with r as (
            select i as n, hash(i * 2246822519 + 3) as h from range({N_TRANSCRIPTS}) t(i)
        )
        select
            'TR' || lpad(n::varchar, 6, '0')                            as transcript_id,
            'T' || lpad(((h % {N_TICKETS_RAW}))::varchar, 6, '0')       as ticket_id,
            d.event_date + to_seconds((h % 80000)::int)                 as event_time,
            d.event_date + to_seconds((h % 80000)::int) + interval 11 minute as _ingested_at,
            case when h % 10 = 0 then 'vi' else 'en' end                as lang,
            list_transform(
                range({CHUNKS_PER_TRANSCRIPT}),
                x -> 'turn ' || x::varchar || ' of ' || 'TR' || lpad(n::varchar, 6, '0')
                     || ': ' || repeat('lorem ipsum dolor sit amet ', 6)
            )                                                           as turns
        from r join dim_date d on d.day_idx = (h >> 6) % {N_DAYS};
    """)

    # ---- events ----------------------------------------------------------
    # mỗi (ngày, khách) đều có ít nhất 15 event -> 14 x 650 = 9.100 tổ hợp.
    con.execute(f"""
        create or replace table combo as
        select d.event_date, d.day_idx, c.customer_id, c.customer_name, c.segment,
            case when c.customer_id = 'C0001' then {ACME_EVENTS_PER_DAY}
                 else 5 + (hash(d.day_idx::varchar || '|n|' || c.customer_id) % 9)::int end as n_events,
            hash(d.day_idx::varchar || '|c|' || c.customer_id) as ch
        from dim_date d cross join dim_customer c;
    """)
    con.execute(f"""
        create or replace table combo_late as
        select *,
            row_number() over (order by ch, event_date, customer_id) as rk_late
        from combo
        where day_idx <= {LAST_LATE_DAY_IDX}
          and customer_id <> 'C0001';   -- ACME quá lớn, sẽ làm lệch tỷ lệ late
    """)
    con.execute(f"""
        create or replace table combo_flag as
        select c.*,
            coalesce(l.rk_late <= {N_LATE_ONLY_COMBOS}, false) as late_only
        from combo c
        left join combo_late l
          on l.event_date = c.event_date and l.customer_id = c.customer_id;
    """)
    n_lateonly = con.execute(
        "select count(*) from combo_flag where late_only"
    ).fetchone()[0]
    assert n_lateonly == N_LATE_ONLY_COMBOS, n_lateonly

    con.execute(f"""
        create or replace table event as
        select
            'E' || lpad(row_number() over ()::varchar, 8, '0')          as event_id,
            f.event_date, f.day_idx, f.customer_id, f.customer_name, f.segment,
            'T' || lpad((hash(f.ch::varchar || '|t|' || s.i::varchar) % {N_TICKETS_RAW})::varchar, 6, '0') as ticket_id,
            {sql_list(EVENT_TYPES)}[((hash(f.ch::varchar || '|et|' || s.i::varchar) % {len(EVENT_TYPES)}) + 1)::bigint] as event_type,
            {sql_list(MODELS)}[((hash(f.ch::varchar || '|md|' || s.i::varchar) % {len(MODELS)}) + 1)::bigint]           as model,
            (hash(f.ch::varchar || '|lat|' || s.i::varchar) % 4800 + 120)::int                    as latency_ms,
            (hash(f.ch::varchar || '|ti|' || s.i::varchar) % 3000 + 200)::int                   as tokens_in,
            (hash(f.ch::varchar || '|to|' || s.i::varchar) % 900 + 40)::int                     as tokens_out,
            (hash(f.ch::varchar || '|esc|' || s.i::varchar) % 100 < 7)                           as is_escalated,
            f.late_only,
            (hash(f.ch::varchar || '|late|' || s.i::varchar) % 1000 < {LATE_RATE_PER_MILLE}
             and f.day_idx <= {LAST_LATE_DAY_IDX})                      as late_random,
            f.event_date + to_seconds((hash(f.ch::varchar || '|sec|' || s.i::varchar) % 86400)::int) as event_time,
            (hash(f.ch::varchar || '|dly|' || s.i::varchar) % 1000)                              as delay_bucket
        from combo_flag f, unnest(range(f.n_events)) as s(i);
    """)
    # _ingested_at: bình thường trễ 0–6 giờ; về muộn trễ 1,8–2,95 ngày.
    con.execute("""
        create or replace table event_final as
        select * exclude (delay_bucket, late_only, late_random),
            (late_only or late_random)                                  as is_late,
            case when late_only or late_random
                 then event_time + to_seconds((155520 + (delay_bucket * 99))::int)
                 else event_time + to_seconds((delay_bucket * 21)::int)
            end                                                         as _ingested_at
        from event;
    """)


# ---------------------------------------------------------------- ghi file
def write_cdc(con: duckdb.DuckDBPyConnection, path: pathlib.Path) -> int:
    """Ghi CDC ra JSONL. `priority` cố tình đổi kiểu int -> string từ 08-10."""
    rows = con.execute(f"""
        select op, ticket_id, cdc_seq, event_time, customer_id, priority_code,
               category, channel, status, csat, first_response_sec, is_bad,
               (hash(ticket_id) % {len(BAD_PRIORITY_VALUES)})::int as bad_idx,
               c.customer_name, c.segment
        from cdc join dim_customer c using (customer_id)
        order by event_time, ticket_id, cdc_seq
    """).fetchall()

    n = 0
    with path.open("w", encoding="utf-8") as fh:
        for (op, tid, seq, et, cust, pcode, cat, chan, status, csat, frs,
             is_bad, bad_idx, cname, seg) in rows:
            day = et.strftime("%Y-%m-%d")
            if is_bad:
                priority = BAD_PRIORITY_VALUES[bad_idx]
            elif day >= SCHEMA_CHANGE_DAY:
                priority = PRIORITY_LABEL[pcode]  # backend đổi sang nhãn chuỗi
            else:
                priority = pcode  # số nguyên như contract ban đầu
            rec = {
                "op": op,
                "ticket_id": tid,
                "cdc_seq": seq,
                "event_time": et.strftime("%Y-%m-%d %H:%M:%S"),
                "_ingested_at": et.strftime("%Y-%m-%d %H:%M:%S"),
                "_source": "postgres.public.tickets",
                "customer_id": cust,
                "customer_name": cname,
                "segment": seg,
                "priority": priority,
                "category": cat,
                "channel": chan,
                "status": status,
                "csat": csat,
                "first_response_sec": frs,
                "subject": f"[{cat}] ticket {tid}",
                "body": f"Khach hang {cname} bao loi {cat} qua {chan}. " * 3,
            }
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
    return n


def write_events(con: duckdb.DuckDBPyConnection, path: pathlib.Path) -> int:
    con.execute(f"""
        copy (
            select event_id, ticket_id, customer_id, customer_name, segment,
                   event_type, model, latency_ms, tokens_in, tokens_out,
                   is_escalated,
                   strftime(event_time,   '%Y-%m-%d %H:%M:%S') as event_time,
                   strftime(_ingested_at, '%Y-%m-%d %H:%M:%S') as _ingested_at,
                   'ai-events' as _topic
            from event_final
            order by _ingested_at, event_id
        ) to '{path}' (format json);
    """)
    return con.execute("select count(*) from event_final").fetchone()[0]


def write_transcripts(con: duckdb.DuckDBPyConnection, path: pathlib.Path) -> int:
    con.execute(f"""
        copy (
            select transcript_id, ticket_id, lang, turns,
                   strftime(event_time,   '%Y-%m-%d %H:%M:%S') as event_time,
                   strftime(_ingested_at, '%Y-%m-%d %H:%M:%S') as _ingested_at,
                   's3://ai-support/transcripts' as _source
            from transcript order by _ingested_at, transcript_id
        ) to '{path}' (format json);
    """)
    return con.execute("select count(*) from transcript").fetchone()[0]


def write_small_files(con: duckdb.DuckDBPyConnection, out_dir: pathlib.Path) -> int:
    """Nhiệm vụ 4: dataset Parquet 5.000 file nhỏ, không partition, thứ tự ngẫu nhiên."""
    import shutil

    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        create or replace table gold_events_src as
        select event_id, ticket_id, customer_id, customer_name, segment,
               event_type, model, latency_ms, tokens_in, tokens_out,
               is_escalated, event_time, event_date,
               (hash(event_id) % {N_GOLD_EVENT_FILES})::int as _fid
        from event_final;
    """)
    for i in range(N_GOLD_EVENT_FILES):
        con.execute(
            f"copy (select * exclude (_fid) from gold_events_src where _fid = {i}) "
            f"to '{out_dir / f'part-{i:05d}.parquet'}' (format parquet)"
        )
    return N_GOLD_EVENT_FILES


# ---------------------------------------------------------------- expected
def write_expected(con: duckdb.DuckDBPyConnection) -> dict:
    EXPECTED.mkdir(parents=True, exist_ok=True)

    training = con.execute("""
        select count(*) from ticket where not is_deleted_same_day
    """).fetchone()[0]
    feature = con.execute("select count(*) from combo_flag").fetchone()[0]
    chunks = N_TRANSCRIPTS * CHUNKS_PER_TRANSCRIPT
    quarantine = con.execute("select count(*) from cdc where is_bad").fetchone()[0]
    exp = {
        "gold_training_set": training,
        "gold_feature_daily": feature,
        "gold_doc_chunks": chunks,
        "quarantine_tickets": quarantine,
        "_note": "Số hàng đúng của từng bảng Gold sau khi sửa hết bốn nhiệm vụ.",
    }
    for k in ("gold_training_set", "gold_feature_daily", "gold_doc_chunks",
              "quarantine_tickets"):
        (EXPECTED / f"{k}.count").write_text(f"{exp[k]}\n", encoding="utf-8")
    (EXPECTED / "summary.json").write_text(
        json.dumps(exp, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return exp


# ---------------------------------------------------------------- main
def main() -> int:
    extra = "--extra" in sys.argv
    t0 = time.time()
    con = duckdb.connect()  # in-memory, chỉ dùng để sinh dữ liệu
    con.execute("set threads to 4")

    log("dựng bảng nguồn…")
    build(con)

    log("ghi seed/tickets_cdc.jsonl…")
    n_cdc = write_cdc(con, HERE / "tickets_cdc.jsonl")
    log("ghi seed/events.jsonl…")
    n_ev = write_events(con, HERE / "events.jsonl")
    log("ghi seed/transcripts.jsonl…")
    n_tr = write_transcripts(con, HERE / "transcripts.jsonl")
    n_files = 0
    if extra:
        log(f"ghi data/gold_events/ ({N_GOLD_EVENT_FILES} file nhỏ)…")
        n_files = write_small_files(con, ROOT / "data" / "gold_events")

    exp = write_expected(con)

    print()
    print(f"  CDC rows        : {n_cdc:>9,}")
    print(f"  events          : {n_ev:>9,}")
    print(f"  transcripts     : {n_tr:>9,}")
    if n_files:
        print(f"  parquet files   : {n_files:>9,}")
    print(f"  expected/       : {json.dumps({k: v for k, v in exp.items() if not k.startswith('_')})}")
    print(f"  xong sau {time.time() - t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
