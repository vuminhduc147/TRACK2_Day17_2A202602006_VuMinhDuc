#!/usr/bin/env python3
"""Đo truy vấn dashboard — nhiệm vụ 4.

    python tools/explain.py                 # đo và in bảng so sánh
    python tools/explain.py --plan          # in thêm cây EXPLAIN ANALYZE
    python tools/explain.py --save-baseline # ghi baseline "trước khi tối ưu"

Ba con số được in ra:

  rows scanned   công quét của node đọc Parquet
                 (DuckDB: metric OPERATOR_ROWS_SCANNED).
                 Đây là con số nhiệm vụ 4 chấm điểm — KHÔNG phải thời gian.
  rows on disk   tổng số hàng thật nằm trong các file mà đường dẫn trỏ tới.
  files          số file Parquet mà đường dẫn trong truy vấn trỏ tới.
  result hash    băm của kết quả. Tối ưu mà đổi kết quả thì không tính.

Vì sao "rows scanned" lớn hơn "rows on disk"? DuckDB đọc Parquet theo lô và
làm tròn LÊN cho từng file. Một file 88 hàng vẫn bị tính như ~1.000 hàng.
Đó chính là small-file problem, hiện nguyên hình thành một con số: 5.000 file
tí hon tốn 5.000.000 đơn vị công quét cho một tập chỉ có ~439.000 hàng.

Vì sao ép threads = 1: DuckDB cộng dồn metric này theo từng luồng, để mặc
định thì con số phụ thuộc số nhân CPU của máy. Ép 1 luồng => mọi máy ra
cùng một số, so sánh mới có nghĩa.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
import pathlib
import re
import sys
import time

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import EXPECTED, ROOT  # noqa: E402

QUERY_FILE = ROOT / "queries" / "dashboard.sql"
BASELINE_FILE = EXPECTED / "dashboard_baseline.json"
TARGET_FACTOR = 10  # phải giảm ít nhất 10 lần


def read_query() -> str:
    sql = QUERY_FILE.read_text(encoding="utf-8")
    # bỏ comment cuối file / dấu ; thừa
    return sql.strip().rstrip(";")


def matched_files(sql: str) -> list[str]:
    """Mọi file Parquet mà các read_parquet(...) trong truy vấn trỏ tới."""
    out: list[str] = []
    for pattern in re.findall(r"read_parquet\(\s*'([^']+)'", sql):
        out += [p for p in glob.glob(pattern, recursive=True) if os.path.isfile(p)]
    return out


def rows_on_disk(files: list[str]) -> int:
    if not files:
        return 0
    con = duckdb.connect()
    total = 0
    for i in range(0, len(files), 500):  # tránh câu lệnh quá dài
        chunk = files[i:i + 500]
        lst = ", ".join("'" + f.replace("'", "''") + "'" for f in chunk)
        total += con.execute(
            f"select coalesce(sum(num_rows), 0) from parquet_file_metadata([{lst}])"
        ).fetchone()[0]
    con.close()
    return int(total)


def measure(sql: str, want_plan: bool = False) -> dict:
    con = duckdb.connect(config={"threads": 1})
    con.execute("""
        set custom_profiling_settings = '{
            "OPERATOR_TYPE":"true",
            "OPERATOR_NAME":"true",
            "OPERATOR_CARDINALITY":"true",
            "OPERATOR_ROWS_SCANNED":"true"
        }'
    """)
    prof = ROOT / ".explain_profile.json"
    con.execute("pragma enable_profiling = 'json'")
    con.execute(f"pragma profiling_output = '{prof}'")

    t0 = time.time()
    rows = con.execute(sql).fetchall()
    elapsed_ms = (time.time() - t0) * 1000

    node = json.loads(prof.read_text(encoding="utf-8"))
    prof.unlink(missing_ok=True)

    scanned, emitted, scan_names = 0, 0, []

    def walk(n: dict) -> None:
        nonlocal scanned, emitted
        if n.get("operator_rows_scanned"):
            scanned += n["operator_rows_scanned"]
            emitted += n.get("operator_cardinality") or 0
            scan_names.append(n.get("operator_name") or n.get("operator_type"))
        for ch in n.get("children", []):
            walk(ch)

    walk(node)

    payload = json.dumps(rows, default=str, sort_keys=True)
    files = matched_files(sql)
    out = {
        "rows_scanned": scanned,
        "rows_emitted": emitted,
        "scan_operators": scan_names,
        "files": len(files),
        "rows_on_disk": rows_on_disk(files),
        "elapsed_ms": round(elapsed_ms, 1),
        "result_hash": hashlib.md5(payload.encode()).hexdigest()[:12],
        "result_rows": len(rows),
        "result": rows,
    }
    if want_plan:
        out["plan"] = "\n".join(
            r[1] for r in con.execute(f"explain analyze {sql}").fetchall()
        )
    con.close()
    return out


def load_baseline() -> dict | None:
    if BASELINE_FILE.exists():
        return json.loads(BASELINE_FILE.read_text(encoding="utf-8"))
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true", help="in cây EXPLAIN ANALYZE")
    ap.add_argument("--save-baseline", action="store_true",
                    help="ghi baseline 'trước khi tối ưu' (chỉ ghi lần đầu)")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    os.chdir(ROOT)  # đường dẫn trong dashboard.sql là tương đối với gốc repo

    # Kiểm tra TRƯỚC khi đo: mốc đã có thì không đo lại, vì lúc đó
    # dashboard.sql có thể đã được viết lại và trỏ vào dataset mới.
    if args.save_baseline and BASELINE_FILE.exists():
        print(f"  baseline cũ đã tồn tại, không ghi đè: {BASELINE_FILE.name}")
        return 0

    sql = read_query()
    m = measure(sql, want_plan=args.plan)

    if args.save_baseline:
        BASELINE_FILE.write_text(
            json.dumps({k: m[k] for k in
                        ("rows_scanned", "rows_on_disk", "files",
                         "result_hash", "result_rows")},
                       indent=2) + "\n", encoding="utf-8")
        print(f"  đã ghi baseline: rows_scanned={m['rows_scanned']:,} "
              f"files={m['files']:,} hash={m['result_hash']}")
        return 0

    if args.json:
        print(json.dumps({k: v for k, v in m.items() if k != "result"}, indent=2))
        return 0

    base = load_baseline()
    print()
    print("  queries/dashboard.sql")
    print("  " + "-" * 62)
    print(f"  {'':<16}{'TRƯỚC':>16}{'HIỆN TẠI':>16}{'MỤC TIÊU':>14}")
    if base:
        target = base["rows_scanned"] // TARGET_FACTOR
        ok_rows = m["rows_scanned"] <= target
        ok_hash = m["result_hash"] == base["result_hash"]
        factor = base["rows_scanned"] / max(m["rows_scanned"], 1)
        print(f"  {'rows scanned':<16}{base['rows_scanned']:>16,}"
              f"{m['rows_scanned']:>16,}{'≤ ' + format(target, ','):>14}"
              f"   {'✓' if ok_rows else '✗'}")
        print(f"  {'rows on disk':<16}{base.get('rows_on_disk', 0):>16,}"
              f"{m['rows_on_disk']:>16,}{'(tham khảo)':>14}")
        print(f"  {'files':<16}{base['files']:>16,}{m['files']:>16,}"
              f"{'ít hơn':>14}   {'✓' if m['files'] < base['files'] else '✗'}")
        print(f"  {'result hash':<16}{base['result_hash']:>16}"
              f"{m['result_hash']:>16}{'không đổi':>14}   {'✓' if ok_hash else '✗'}")
        print(f"  {'thời gian (ms)':<16}{'—':>16}{m['elapsed_ms']:>16,.1f}"
              f"{'(tham khảo)':>14}")
        print()
        print(f"  => giảm {factor:.1f}× (cần ≥ {TARGET_FACTOR}×)")
    else:
        print(f"  {'rows scanned':<16}{'—':>16}{m['rows_scanned']:>16,}")
        print(f"  {'files':<16}{'—':>16}{m['files']:>16,}")
        print(f"  {'result hash':<16}{'—':>16}{m['result_hash']:>16}")
        print("\n  (chưa có baseline — chạy `make setup` hoặc "
              "`python tools/explain.py --save-baseline`)")
    print()
    print(f"  kết quả truy vấn ({m['result_rows']} hàng):")
    for r in m["result"][:5]:
        print("   ", r)
    if args.plan:
        print("\n  EXPLAIN ANALYZE\n")
        print("\n".join("    " + ln for ln in m["plan"].splitlines()))
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())
