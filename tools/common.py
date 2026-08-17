"""Tiện ích dùng chung cho mọi script trong lab.

Bạn không cần sửa file này.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib

import duckdb

ROOT = pathlib.Path(__file__).resolve().parent.parent
SEED = ROOT / "seed"
DBT_DIR = ROOT / "dbt"
DATA = ROOT / "data"
EXPECTED = ROOT / "expected"
WAREHOUSE = ROOT / "warehouse.duckdb"

DAY0 = dt.date(2026, 8, 3)
N_DAYS = 14
RUN_DATES = [(DAY0 + dt.timedelta(days=i)).isoformat() for i in range(N_DAYS)]

# Bảng Gold + khoá dùng để tính checksum. Checksum chỉ dựa trên các cột này,
# nên bạn được tự do thêm cột mới mà không làm hỏng phép so sánh.
GOLD_TABLES = {
    "gold_training_set": ["ticket_id", "priority", "category", "status"],
    "gold_feature_daily": ["event_date", "customer_id", "n_events", "p95_latency_ms"],
    "gold_doc_chunks": ["chunk_id", "transcript_id", "chunk_index"],
    "quarantine_tickets": ["ticket_id", "cdc_seq"],
}


def connect(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(WAREHOUSE), read_only=read_only)
    con.execute("set threads to 4")
    return con


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    return con.execute(
        "select count(*) from information_schema.tables where table_name = ?", [name]
    ).fetchone()[0] > 0


def columns_of(con: duckdb.DuckDBPyConnection, name: str) -> list[str]:
    return [
        r[0]
        for r in con.execute(
            "select column_name from information_schema.columns where table_name = ?",
            [name],
        ).fetchall()
    ]


def row_count(con: duckdb.DuckDBPyConnection, name: str) -> int | None:
    if not table_exists(con, name):
        return None
    return con.execute(f'select count(*) from "{name}"').fetchone()[0]


def checksum(con: duckdb.DuckDBPyConnection, name: str) -> str | None:
    """MD5 ổn định trên các cột khoá, không phụ thuộc thứ tự hàng."""
    if not table_exists(con, name):
        return None
    have = set(columns_of(con, name))
    cols = [c for c in GOLD_TABLES.get(name, []) if c in have]
    if not cols:
        return "no-key-columns"
    expr = " || '|' || ".join(f"coalesce(\"{c}\"::varchar, '~')" for c in cols)
    # Bảng rỗng vẫn phải có checksum xác định, nếu không thì một model khung
    # (0 hàng) sẽ bị báo là "không ổn định" trong khi nó hoàn toàn ổn định.
    return con.execute(
        f'select coalesce(md5(string_agg({expr}, chr(10) order by {expr})), '
        f"'empty') from \"{name}\""
    ).fetchone()[0]


def expected_counts() -> dict[str, int]:
    path = EXPECTED / "summary.json"
    data = json.loads(path.read_text(encoding="utf-8"))
    return {k: v for k, v in data.items() if not k.startswith("_")}


def fmt(n: int | None) -> str:
    return "—" if n is None else f"{n:,}"
