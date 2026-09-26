"""ENG-4: the DAG orders tasks by dependency and retries failures."""
from __future__ import annotations

import sys

import pytest

from orchestration.dag import Task, run_task, topo


def test_topological_order():
    ts = [Task("c", ["x"], ["b"]), Task("a", ["x"]), Task("b", ["x"], ["a"])]
    assert [t.name for t in topo(ts)] == ["a", "b", "c"]


def test_cycle_detected():
    with pytest.raises(ValueError):
        topo([Task("a", ["x"], ["b"]), Task("b", ["x"], ["a"])])


def test_retries_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr("orchestration.dag.time.sleep", lambda s: None)
    flag = tmp_path / "flag"
    code = f"import pathlib,sys; p=pathlib.Path(r'{flag}'); ok=p.exists(); p.touch(); sys.exit(0 if ok else 1)"
    rec = run_task(Task("flaky", [sys.executable, "-c", code], retries=2), tmp_path)
    assert rec["returncode"] == 0 and rec["attempts"] == 2


def test_gives_up_after_retries(tmp_path, monkeypatch):
    monkeypatch.setattr("orchestration.dag.time.sleep", lambda s: None)
    rec = run_task(Task("broken", [sys.executable, "-c", "import sys; sys.exit(3)"], retries=1), tmp_path)
    assert rec["returncode"] == 3 and rec["attempts"] == 2
