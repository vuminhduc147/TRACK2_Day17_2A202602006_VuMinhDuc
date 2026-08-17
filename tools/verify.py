#!/usr/bin/env python3
"""`make verify` — vòng phản hồi của bạn suốt buổi lab.

Xoá kho, chạy pipeline BA lần liên tiếp, rồi in ra:

  * checksum từng bảng Gold sau mỗi lần chạy  -> cột "ỔN ĐỊNH"
  * số hàng cuối cùng so với expected/        -> cột "SỐ HÀNG"
  * kết quả dbt test và các bất biến dữ liệu
  * rows scanned của queries/dashboard.sql
  * hai tham số của DAG

Chạy nó sau MỖI lần sửa. Bốn dòng ✗ lúc bắt đầu chính là bốn nhiệm vụ.

    python tools/verify.py            # đầy đủ (3 lượt, ~2 phút)
    python tools/verify.py --runs 1   # nhanh, chỉ kiểm tra số hàng
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from tools.common import (  # noqa: E402
    DBT_DIR, EXPECTED, GOLD_TABLES, WAREHOUSE, checksum, connect,
    expected_counts, fmt, row_count, table_exists,
)
from tools.run_pipeline import reset_warehouse, run_once  # noqa: E402

OK, BAD, DASH = "✓", "✗", "—"


def hr(ch: str = "─", n: int = 74) -> str:
    return "  " + ch * n


def snapshot() -> dict[str, dict]:
    con = connect()
    try:
        return {
            t: {"rows": row_count(con, t), "sum": checksum(con, t)}
            for t in GOLD_TABLES
        }
    finally:
        con.close()


# ------------------------------------------------------------------ checks
def dbt_test() -> dict:
    from dbt.cli.main import dbtRunner

    os.environ["LAB17_DB"] = str(WAREHOUSE)
    res = dbtRunner().invoke([
        "test",
        "--project-dir", str(DBT_DIR),
        "--profiles-dir", str(DBT_DIR),
        "--target-path", str(DBT_DIR / "target"),
        "--log-path", str(DBT_DIR / "logs"),
        "--quiet",
    ])
    results = getattr(res.result, "results", []) if res.result is not None else []
    passed = sum(1 for r in results if str(r.status) in ("TestStatus.Pass", "pass"))
    failed = [r for r in results if str(r.status) not in ("TestStatus.Pass", "pass")]
    return {
        "total": len(results),
        "passed": passed,
        "failed": [r.node.name for r in failed],
        "ok": bool(results) and not failed,
    }


def data_invariants() -> list[tuple[str, bool, str]]:
    """Bất biến dữ liệu — độc lập với việc bạn viết test dbt thế nào."""
    con = connect()
    out: list[tuple[str, bool, str]] = []
    try:
        if table_exists(con, "silver_tickets"):
            bad = con.execute("""
                select count(*) from silver_tickets
                where priority is null or priority not between 1 and 4
            """).fetchone()[0]
            out.append((
                "silver_tickets.priority ∈ 1..4, không NULL",
                bad == 0,
                f"{bad:,} hàng sai" if bad else "sạch",
            ))
        else:
            out.append(("silver_tickets tồn tại", False, "chưa có bảng"))

        exp_q = expected_counts()["quarantine_tickets"]
        if table_exists(con, "quarantine_tickets"):
            n = row_count(con, "quarantine_tickets")
            out.append((
                "quarantine_tickets đúng số bản ghi lỗi",
                n == exp_q,
                f"{n:,} / {exp_q:,}",
            ))
        else:
            out.append((
                "quarantine_tickets đúng số bản ghi lỗi", False,
                f"chưa có bảng (kỳ vọng {exp_q:,})",
            ))

        if table_exists(con, "gold_training_set"):
            dup = con.execute("""
                select count(*) from (
                    select ticket_id from gold_training_set
                    group by 1 having count(*) > 1
                )
            """).fetchone()[0]
            out.append((
                "gold_training_set: 1 hàng / 1 ticket",
                dup == 0,
                f"{dup:,} ticket bị lặp" if dup else "không lặp",
            ))
    finally:
        con.close()
    return out


def dashboard_check() -> dict:
    from tools.explain import TARGET_FACTOR, load_baseline, measure, read_query

    cwd = pathlib.Path.cwd()
    try:
        os.chdir(pathlib.Path(__file__).resolve().parent.parent)
        m = measure(read_query())
    finally:
        os.chdir(cwd)
    base = load_baseline()
    if not base:
        return {"ok": False, "note": "chưa có baseline — chạy make setup", **m}
    target = base["rows_scanned"] // TARGET_FACTOR
    return {
        "ok": m["rows_scanned"] <= target and m["result_hash"] == base["result_hash"],
        "before": base["rows_scanned"],
        "after": m["rows_scanned"],
        "target": target,
        "factor": base["rows_scanned"] / max(m["rows_scanned"], 1),
        "files_before": base["files"],
        "files_after": m["files"],
        "same_result": m["result_hash"] == base["result_hash"],
    }


# ------------------------------------------------------------------- main
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--no-reset", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--strict", action="store_true",
                    help="thoát với mã != 0 khi chưa đạt (dùng cho CI)")
    args = ap.parse_args()

    exp = expected_counts()

    print()
    print(hr("━"))
    print("  LAB 17 · make verify")
    print(hr("━"))

    if not args.no_reset:
        reset_warehouse()
    snaps: list[dict] = []
    for i in range(1, args.runs + 1):
        print(f"  run {i}/{args.runs} …", end=" ", flush=True)
        elapsed = run_once(verbose=False)
        snaps.append(snapshot())
        print(f"{elapsed:.1f}s")
    print()

    # ---- bảng chính ------------------------------------------------------
    print(f"  {'BẢNG':<22}{'ỔN ĐỊNH':<12}{'SỐ HÀNG':>12}{'KỲ VỌNG':>12}   GHI CHÚ")
    print(hr())
    table_ok: dict[str, bool] = {}
    for t in GOLD_TABLES:
        sums = [s[t]["sum"] for s in snaps]
        rows = [s[t]["rows"] for s in snaps]
        last, want = rows[-1], exp.get(t)
        exists = last is not None

        if not exists:
            stable_txt, stable = f"{DASH}", False
        elif len(set(sums)) == 1:
            stable_txt, stable = f"{OK} ok", True
        else:
            stable_txt, stable = f"{BAD} FAIL", False

        if not exists:
            note = "chưa có bảng"
        elif last == want:
            note = f"{OK}"
        elif last > want:
            note = f"{BAD} thừa {last - want:,} hàng"
        else:
            note = f"{BAD} thiếu {want - last:,} hàng"

        table_ok[t] = stable and last == want
        print(f"  {t:<22}{stable_txt:<12}{fmt(last):>12}{fmt(want):>12}   {note}")

    if args.runs > 1:
        print()
        print("  CHECKSUM từng lượt")
        print(hr())
        for t in GOLD_TABLES:
            raw = [s[t]["sum"] for s in snaps]
            sums = [(v or DASH)[:10] for v in raw]
            mark = OK if (len(set(sums)) == 1 and raw[-1]) else BAD
            print(f"  {t:<22}" + "  ".join(f"{s:<12}" for s in sums) + f" {mark}")

    # ---- kiểm tra khác ---------------------------------------------------
    print()
    print("  KIỂM TRA KHÁC")
    print(hr())

    t = dbt_test()
    print(f"  {'dbt test':<44}"
          f"{OK if t['ok'] else BAD} {t['passed']}/{t['total']} pass"
          + (f"  (fail: {', '.join(t['failed'][:3])})" if t["failed"] else ""))

    inv = data_invariants()
    for label, ok, detail in inv:
        print(f"  {label:<44}{OK if ok else BAD} {detail}")

    from tools.explain import BASELINE_FILE
    d = dashboard_check() if BASELINE_FILE.exists() else None
    if d and "note" not in d:
        print(f"  {'dashboard rows scanned':<44}"
              f"{OK if d['ok'] else BAD} {d['before']:,} → {d['after']:,} "
              f"({d['factor']:.1f}×, cần ≥ 10×)")
        print(f"  {'  số file parquet':<44}"
              f"{OK if d['files_after'] < d['files_before'] else BAD} "
              f"{d['files_before']:,} → {d['files_after']:,}")
        print(f"  {'  kết quả truy vấn không đổi':<44}"
              f"{OK if d['same_result'] else BAD}")

    if d is None:
        print(f"  {'bài mở rộng (EXTRA.md)':<44}— chưa chạy `make seed-extra`")

    from tools.check_dag import check as check_dag
    dag = check_dag()
    print(f"  {'DAG: catchup / max_active_runs':<44}"
          f"{OK if dag['ok'] else BAD} {dag['catchup']} / {dag['max_active_runs']}")

    # ---- tổng kết --------------------------------------------------------
    criteria = {
        "1 · gold_training_set idempotent & đúng số hàng": table_ok["gold_training_set"],
        "2 · gold_feature_daily đủ hàng (dữ liệu về muộn)": table_ok["gold_feature_daily"],
        "3 · contract + quarantine + dbt test": (
            t["ok"] and all(ok for _, ok, _ in inv)
        ),
        "4 · gold_doc_chunks vẫn ổn định (đối chứng)": table_ok["gold_doc_chunks"],
    }
    print()
    print("  TỔNG KẾT")
    print(hr())
    for label, ok in criteria.items():
        print(f"  {OK if ok else BAD}  {label}")
    done = sum(criteria.values())
    print(hr())
    print(f"  {done}/{len(criteria)} tiêu chí đạt")
    print()

    if args.json:
        (EXPECTED.parent / "verify_result.json").write_text(
            json.dumps(
                {
                    "runs": [{k: v["rows"] for k, v in s.items()} for s in snaps],
                    "criteria": criteria,
                    "dbt_test": t,
                    "dashboard": {k: v for k, v in d.items() if k != "result"} if d else None,
                    "dag": dag,
                },
                indent=2, default=str,
            ) + "\n", encoding="utf-8")
    # Mặc định luôn thoát 0: bảng ở trên đã nói hết, không cần make
    # in thêm "Error 1" mỗi lần bạn còn nhiệm vụ chưa xong.
    return 0 if (done == len(criteria) or not args.strict) else 1


if __name__ == "__main__":
    sys.exit(main())
