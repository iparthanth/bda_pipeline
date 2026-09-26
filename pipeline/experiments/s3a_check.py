"""ING-5 evidence: the lake lives on an S3-compatible object store (MinIO in docker-compose; any S3 endpoint).

Run a normal load with the lake on S3 and then this check, e.g.

    export S3_ENDPOINT=http://localhost:9000 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
    export EPISCOPE__PATHS__LAKE_ROOT=s3a://episcope/lake
    python -m pipeline.run_load && python -m pipeline.experiments.s3a_check

It reads the bronze, silver and gold tables back through the S3A connector, lists the objects the load wrote
and records the result in ``docs/experiments/s3a_minio_check.json``.
"""
from __future__ import annotations

import datetime as dt
import json
import os

from pipeline.common.config import REPO_ROOT, lake, path
from pipeline.common.spark import get_spark


def main() -> None:
    root = path("lake_root")
    if not root.startswith("s3a://"):
        raise SystemExit("set EPISCOPE__PATHS__LAKE_ROOT=s3a://<bucket>/<prefix> first")
    spark = get_spark("s3a-check")
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.Path(root).getFileSystem(conf)
    it = fs.listFiles(jvm.org.apache.hadoop.fs.Path(root), True)
    n_obj, n_bytes = 0, 0
    while it.hasNext():
        st = it.next()
        n_obj += 1
        n_bytes += st.getLen()
    tables = {}
    for layer, t in (("bronze", "encounters_raw"), ("silver", "encounters"), ("silver", "quarantine"), ("gold", "weekly_counts"),
                     ("gold", "daily_signal"), ("gold", "dq_report")):
        tables[f"{layer}.{t}"] = spark.read.format("delta").load(lake(layer, t)).count()
    res = {"checked_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"), "lake_root": root,
           "endpoint": os.environ.get("S3_ENDPOINT"), "filesystem": fs.getClass().getName(), "objects": n_obj,
           "bytes": n_bytes, "table_rows": tables,
           "catalog_location": spark.sql("DESCRIBE DETAIL episcope_gold.weekly_counts").first()["location"]}
    out = REPO_ROOT / "docs" / "experiments" / "s3a_minio_check.json"
    out.write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
