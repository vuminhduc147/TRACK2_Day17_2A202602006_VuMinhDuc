"""Đọc dags/ai_training_pipeline.py bằng AST (không import Airflow).

Trả về trạng thái của hai tham số mà nhiệm vụ 1 quan tâm.
"""

from __future__ import annotations

import ast
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import ROOT  # noqa: E402

DAG_FILE = ROOT / "dags" / "ai_training_pipeline.py"


def check() -> dict[str, object]:
    out: dict[str, object] = {"catchup": None, "max_active_runs": None, "ok": False}
    if not DAG_FILE.exists():
        return out
    tree = ast.parse(DAG_FILE.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = getattr(fn, "id", None) or getattr(fn, "attr", None)
        if name != "DAG":
            continue
        for kw in node.keywords:
            if kw.arg in ("catchup", "max_active_runs"):
                try:
                    out[kw.arg] = ast.literal_eval(kw.value)
                except ValueError:
                    out[kw.arg] = "<không đọc được>"
    out["ok"] = out["catchup"] is False and out["max_active_runs"] == 1
    return out


if __name__ == "__main__":
    print(check())
