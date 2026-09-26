"""PRV-1: salted hashing, age banding, salt handling and 'no direct identifier beyond bronze'."""
from __future__ import annotations

import hashlib
import os

import pytest
from pyspark.sql import functions as F

from pipeline.common import privacy


def test_hash_is_salted_sha256_and_deterministic(spark):
    df = spark.createDataFrame([("1234567890",), ("1234567890",), ("",), (None,)], "patient_id string")
    out = [r.h for r in df.select(privacy.hash_id("patient_id", "salt-A-0123456789").alias("h")).collect()]
    expected = hashlib.sha256(b"salt-A-01234567891234567890").hexdigest()
    assert out[0] == out[1] == expected
    assert out[2] is None and out[3] is None          # blank IDs are not hashed (they are quarantined)
    other = df.select(privacy.hash_id("patient_id", "salt-B-0123456789").alias("h")).first().h
    assert other != expected                          # a different salt gives unlinkable hashes


@pytest.mark.parametrize("age, band", [(0, "0-4"), (4, "0-4"), (5, "5-14"), (14, "5-14"), (15, "15-24"), (24, "15-24"),
                                       (25, "25-44"), (44, "25-44"), (45, "45-64"), (64, "45-64"), (65, "65+"), (120, "65+")])
def test_age_band_boundaries(spark, age, band):
    got = spark.range(1).select(privacy.age_band(F.lit(age)).alias("b")).first().b
    assert got == band


def test_missing_or_short_salt_is_refused(monkeypatch):
    monkeypatch.setenv("EPISCOPE_SALT", "short")
    with pytest.raises(privacy.MissingSaltError):
        privacy.get_salt()
    monkeypatch.delenv("EPISCOPE_SALT")
    with pytest.raises(privacy.MissingSaltError):
        privacy.get_salt()


def test_salt_and_tokens_are_not_in_repository():
    """The real secrets live only in the git-ignored .env (checked against every source/doc file)."""
    from pipeline.common.config import REPO_ROOT
    env = REPO_ROOT / ".env"
    if not env.exists():
        pytest.skip("no .env (run make env)")
    secrets = [line.split("=", 1)[1].strip() for line in env.read_text().splitlines()
               if "=" in line and not line.lstrip().startswith("#") and len(line.split("=", 1)[1].strip()) >= 16]
    for p in REPO_ROOT.rglob("*"):
        if p.is_file() and not {".venv", ".git", "data", "__pycache__"} & set(p.parts) and p.name != ".env" \
                and p.suffix in {".py", ".yaml", ".yml", ".md", ".json", ".txt", ".js", ".html", ".example", ".tex", ".csv"}:
            text = p.read_text(errors="ignore")
            assert not any(sec in text for sec in secrets), f"a secret from .env found in {p}"


def test_no_direct_identifiers_beyond_bronze(spark, tiny_lake):
    from pipeline.common.config import lake
    for layer, table in (("silver", "encounters"), ("silver", "quarantine"), ("silver", "episode_links"),
                         ("gold", "weekly_counts"), ("gold", "daily_signal"), ("gold", "reporting_triangle")):
        cols = set(spark.read.format("delta").load(lake(layer, table)).columns)
        assert "patient_id" not in cols, f"{layer}.{table} exposes patient_id"
        assert "age_years" not in cols, f"{layer}.{table} exposes exact age"
    q = spark.read.format("delta").load(lake("silver", "quarantine")).select("record").limit(200).collect()
    assert all('"patient_id"' not in r.record for r in q)
    gold_cols = set(spark.read.format("delta").load(lake("gold", "weekly_counts")).columns)
    assert "patient_hash" not in gold_cols
