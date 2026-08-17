#!/usr/bin/env python3
"""Chạy toàn bộ đường ống MỘT lượt: phát lại 14 ngày vận hành theo thứ tự.

Với mỗi ngày D trong 2026-08-03 … 2026-08-16:

    1. Bronze  : nạp mọi bản ghi có _ingested_at rơi vào ngày D
    2. dbt run : dựng lại Silver, cập nhật Gold, với var run_date = D

Đây chính là cách Airflow gọi pipeline trong thực tế, chỉ khác là ở đây
14 ngày chạy liên tiếp trong vài chục giây thay vì trong hai tuần.

    python tools/run_pipeline.py            # một lượt
    python tools/run_pipeline.py --reset    # xoá kho rồi chạy lại từ đầu
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from ingest.load_bronze import load_day  # noqa: E402
from tools.common import DBT_DIR, RUN_DATES, WAREHOUSE, connect  # noqa: E402


def dbt_run(run_date: str, select: str | None = None) -> None:
    from dbt.cli.main import dbtRunner

    args = [
        "run",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--target-path", str(DBT_DIR / "target"),
        "--log-path", str(DBT_DIR / "logs"),
        "--quiet",
        "--vars", json.dumps({"run_date": run_date}),
    ]
    if select:
        args += ["--select", select]
    res = dbtRunner().invoke(args)
    if not res.success:
        msg = getattr(res, "exception", None) or "dbt run thất bại"
        raise SystemExit(f"\n  ✗ dbt run lỗi ở ngày {run_date}: {msg}")


def release_dbt() -> None:
    """Nhả kết nối DuckDB mà dbt đang giữ, để script khác mở lại được kho."""
    try:
        from dbt.adapters.factory import reset_adapters

        reset_adapters()
    except Exception:  # pragma: no cover - phòng khi API dbt đổi
        pass


def reset_warehouse() -> None:
    for suffix in ("", ".wal"):
        p = pathlib.Path(str(WAREHOUSE) + suffix)
        if p.exists():
            p.unlink()


def run_once(verbose: bool = True) -> float:
    os.environ["LAB17_DB"] = str(WAREHOUSE)
    t0 = time.time()
    for i, run_date in enumerate(RUN_DATES, start=1):
        con = connect()
        try:
            counts = load_day(con, run_date)
        finally:
            con.close()  # dbt cần mở file kho, nên phải nhả kết nối trước
        dbt_run(run_date)
        if verbose:
            print(
                f"  ngày {run_date}  ({i:>2}/{len(RUN_DATES)})  "
                f"cdc={counts['bronze_tickets_cdc']:>5,}  "
                f"events={counts['bronze_events']:>7,}  "
                f"transcripts={counts['bronze_transcripts']:>5,}",
                flush=True,
            )
    release_dbt()
    return time.time() - t0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="xoá warehouse.duckdb trước khi chạy")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    if args.reset:
        reset_warehouse()
        print("  kho dữ liệu đã được xoá sạch.")

    elapsed = run_once(verbose=not args.quiet)
    print(f"  pipeline xong sau {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
