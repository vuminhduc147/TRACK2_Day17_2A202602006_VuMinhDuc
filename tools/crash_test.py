#!/usr/bin/env python3
"""Kịch bản sự cố cho bài mở rộng B (EXTRA.md) — `make crash-test`.

Ba lượt chạy, trên một kho riêng (không đụng warehouse.duckdb):

  A. chạy thẳng một mạch          -> số hàng chuẩn
  B. chạy rồi bị giết ở lô thứ K  -> tiến trình chết giữa chừng
  C. khởi động lại, chạy nốt      -> so với A

Đạt khi:  C == A  và  không có event_id nào trùng.
"""

from __future__ import annotations

import itertools
import json
import pathlib
import shutil
import subprocess
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA, ROOT, SEED  # noqa: E402

WORK = DATA / "crash"
TOPIC = WORK / "topic.jsonl"
OFFSETS = WORK / "offsets.json"
DB = WORK / "crash.duckdb"
N_MESSAGES = 20_000
BATCH_SIZE = 500
CRASH_AT_BATCH = 7


def build_topic() -> int:
    WORK.mkdir(parents=True, exist_ok=True)
    src = SEED / "events.jsonl"
    with src.open("r", encoding="utf-8") as fh, TOPIC.open("w", encoding="utf-8") as out:
        for line in itertools.islice(fh, N_MESSAGES):
            out.write(line)
    return N_MESSAGES


def fresh() -> None:
    for p in (DB, pathlib.Path(str(DB) + ".wal"), OFFSETS):
        p.unlink(missing_ok=True)


def run_consumer(crash_at: int | None = None) -> int:
    cmd = [
        sys.executable, str(ROOT / "ingest" / "consumer.py"),
        "--db", str(DB), "--topic", str(TOPIC), "--offset", str(OFFSETS),
        "--batch-size", str(BATCH_SIZE),
    ]
    if crash_at:
        cmd += ["--crash-at-batch", str(crash_at)]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    sys.stdout.write(proc.stdout)
    if proc.returncode not in (0, 137, -9):
        sys.stderr.write(proc.stderr)
        raise SystemExit(f"consumer lỗi (mã {proc.returncode})")
    return proc.returncode


def stats() -> tuple[int, int]:
    con = duckdb.connect(str(DB), read_only=True)
    total, distinct = con.execute(
        "select count(*), count(distinct event_id) from bronze_events_stream"
    ).fetchone()
    con.close()
    return total, distinct


def main(strict: bool = False) -> int:
    n = build_topic()
    print(f"\n  topic: {n:,} message · batch {BATCH_SIZE} · giết ở lô {CRASH_AT_BATCH}\n")

    print("  A. chạy một mạch, không sự cố")
    fresh()
    run_consumer()
    a_total, a_distinct = stats()
    print(f"     -> {a_total:,} hàng / {a_distinct:,} event_id khác nhau\n")

    print(f"  B. chạy và bị giết ở lô {CRASH_AT_BATCH}")
    fresh()
    rc = run_consumer(crash_at=CRASH_AT_BATCH)
    print(f"     -> tiến trình thoát với mã {rc}")
    offset = json.loads(OFFSETS.read_text())["bronze-loader"] if OFFSETS.exists() else 0
    print(f"     -> offset đã commit: {offset:,}\n")

    print("  C. khởi động lại, chạy nốt")
    run_consumer()
    c_total, c_distinct = stats()
    print(f"     -> {c_total:,} hàng / {c_distinct:,} event_id khác nhau\n")

    lost = a_total - c_total
    dup = c_total - c_distinct
    print("  " + "-" * 58)
    print(f"  {'không mất bản ghi':<34}"
          f"{'✓' if lost <= 0 else f'✗ mất {lost:,} hàng'}")
    print(f"  {'không trùng bản ghi':<34}"
          f"{'✓' if dup == 0 else f'✗ trùng {dup:,} hàng'}")
    ok = lost <= 0 and dup == 0 and c_total == a_total
    print(f"  {'C == A':<34}{'✓' if c_total == a_total else f'✗ {c_total:,} ≠ {a_total:,}'}")
    print("  " + "-" * 58)
    print(f"  BÀI MỞ RỘNG B: {'ĐẠT ✓' if ok else 'CHƯA ĐẠT ✗'}\n")
    return 0 if (ok or not strict) else 1


if __name__ == "__main__":
    sys.exit(main(strict="--strict" in sys.argv))
