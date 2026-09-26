"""Test configuration.

All tests run against an isolated, temporary data root (EPISCOPE_DATA_ROOT) so they never touch the real
lake. The ``tiny_lake`` fixture builds a complete lake at a very small scale factor once per session
(generator -> bronze -> silver -> gold) for the integration tests.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TMP = Path(tempfile.mkdtemp(prefix="episcope_test_"))
os.environ["EPISCOPE_DATA_ROOT"] = str(TMP)
os.environ.setdefault("EPISCOPE_SALT", "unit-test-salt-0123456789abcdef")
os.environ["EPISCOPE__SPARK__DRIVER_MEMORY"] = "3g"
os.environ["EPISCOPE__SPARK__SHUFFLE_PARTITIONS"] = "8"
TINY_SF = 0.004


@pytest.fixture(scope="session")
def spark():
    from pipeline.common.spark import get_spark

    return get_spark("tests")


@pytest.fixture(scope="session")
def tiny_lake(spark):
    """Generate a tiny synthetic dataset and run the initial load (history)."""
    from generator import fetch_public, generate
    from pipeline import run_load

    pub = ROOT / "data" / "landing" / "public" / "boundaries"
    fetch_public.main(["--offline"])
    if pub.exists():  # reuse real cached boundaries if the repo has them
        dst = TMP / "landing" / "public" / "boundaries"
        dst.mkdir(parents=True, exist_ok=True)
        for f in pub.glob("*"):
            shutil.copy(f, dst / f.name)
    generate.main(["--sf", str(TINY_SF), "--workers", "2"])
    summary = run_load.run(str(TMP / "landing"))
    return {"root": TMP, "summary": summary}


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(TMP, ignore_errors=True)
