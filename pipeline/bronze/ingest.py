"""Bronze ingestion (ING-1): land S1-S6 unmodified, adding ingestion metadata.

* Files are discovered recursively under a source directory (landing zone or a drop folder) and
  classified by *sniffing their header* against the schema contract, so file names, counts and dates
  are never hard-coded (hidden test drop requirement).
* Every row keeps its raw string values and gains ``_source, _load_ts, _batch_id, _file_name,
  _file_checksum``. Public sources (boundaries, weather, population, OpenDengue) are stored as raw
  bytes via Spark's ``binaryFile`` reader.
* Idempotency: a manifest table records the SHA-256 of every file ever loaded; re-running a load (or
  receiving the same drop twice) skips files that are already in bronze.
"""
from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common.config import lake
from pipeline.common.schema import SIGNATURES, SOURCE_COLUMNS
from pipeline.common.spark import register_table, table_exists

MANIFEST = "ingest_manifest"


@dataclass
class LoadResult:
    batch_id: str
    load_ts: str
    files: dict[str, list[dict]] = field(default_factory=dict)  # source -> [{path, checksum, bytes}]
    skipped: list[str] = field(default_factory=list)

    @property
    def sources(self) -> list[str]:
        return [s for s, f in self.files.items() if f]


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


def _open_text(p: Path):
    """Open plain or gzip-compressed text."""
    if p.name.lower().endswith(".gz"):
        return gzip.open(p, "rt", encoding="utf-8", errors="replace")
    return open(p, "r", encoding="utf-8", errors="replace")


def _base_name(p: Path) -> str:
    n = p.name.lower()
    return n[:-3] if n.endswith(".gz") else n


def file_format(p: Path) -> str | None:
    """'csv', 'jsonl' (one object per line) or 'jsonarray' (a JSON array, possibly pretty-printed)."""
    name = _base_name(p)
    if name.endswith((".csv", ".txt")):
        return "csv"
    if name.endswith((".jsonl", ".ndjson", ".json")):
        with _open_text(p) as fh:
            head = fh.read(1 << 16).lstrip()
        if head.startswith("["):
            return "jsonarray"
        if head.startswith("{"):
            return "jsonl"
    return None


def _first_keys(p: Path, fmt: str) -> set[str]:
    with _open_text(p) as fh:
        if fmt == "csv":
            return {c.strip().strip('"').lower() for c in fh.readline().strip().split(",")}
        if fmt == "jsonl":
            return set(json.loads(fh.readline()).keys())
        head = fh.read(1 << 20)
    # JSON array: decode the first object only
    start = head.index("{")
    obj, _ = json.JSONDecoder().raw_decode(head[start:])
    return set(obj.keys())


def classify(p: Path) -> str | None:
    """Return the feed a file belongs to, based on its header/first record (None = not a feed file)."""
    if p.name.startswith(("_", ".")):
        return None
    try:
        fmt = file_format(p)
        if fmt is None:
            return None
        cols = _first_keys(p, fmt)
    except (OSError, ValueError, json.JSONDecodeError, EOFError):
        return None
    for source, sig in SIGNATURES.items():
        if sig <= cols:
            return source
    return None


def discover(root: Path) -> dict[str, list[Path]]:
    found: dict[str, list[Path]] = {s: [] for s in SIGNATURES}
    for p in sorted(root.rglob("*")):
        if p.is_file() and "public" not in p.relative_to(root).parts:
            src = classify(p)
            if src:
                found[src].append(p)
    return found


def discover_public(root: Path) -> dict[str, list[Path]]:
    pub = root / "public"
    out = {}
    for sub in ("boundaries", "population", "weather", "opendengue"):
        d = pub / sub
        out[f"public_{sub}"] = sorted(p for p in d.glob("*") if p.is_file()) if d.exists() else []
    return out


def _manifest(spark: SparkSession) -> DataFrame | None:
    loc = lake("bronze", MANIFEST)
    return spark.read.format("delta").load(loc) if table_exists(spark, loc) else None


def new_batch_id(checksums: list[str]) -> tuple[str, str]:
    now = dt.datetime.now(dt.timezone.utc)
    digest = hashlib.sha1("".join(sorted(checksums)).encode()).hexdigest()[:8]
    return f"B{now:%Y%m%dT%H%M%S}-{digest}", now.isoformat(timespec="seconds")


def _header(p: str) -> tuple[str, ...]:
    with _open_text(Path(p)) as fh:
        return tuple(c.strip().strip('"') for c in fh.readline().strip().split(","))


def _read_feed(spark: SparkSession, source: str, files: list[dict]) -> DataFrame:
    """Read a feed as raw strings, whatever the container format.

    * CSV (optionally .gz): columns matched *by name* (files grouped by header), so re-ordered or extra columns
      are tolerated; missing contract columns become NULL and are caught by the quality rules.
    * JSON Lines / JSON arrays: read with an all-string schema of the contract columns.
    * Stock (S6) is kept as the raw JSON text of each record (``raw_json``), parsed in silver.
    """
    cols = SOURCE_COLUMNS[source]
    by_fmt: dict[str, list[str]] = {}
    for f in files:
        by_fmt.setdefault(file_format(Path(f["path"])) or "csv", []).append(f["path"])
    frames = []
    str_schema = T.StructType([T.StructField(c, T.StringType()) for c in cols] + [T.StructField("_corrupt_record", T.StringType())])
    for fmt, paths in by_fmt.items():
        if source == "stock" and fmt == "jsonl":
            frames.append(spark.read.text(paths).withColumnRenamed("value", "raw_json").withColumn("_file_name", F.input_file_name()))
            continue
        if fmt in ("jsonl", "jsonarray"):
            df = (spark.read.schema(str_schema).option("multiLine", fmt == "jsonarray").option("mode", "PERMISSIVE")
                  .option("columnNameOfCorruptRecord", "_corrupt_record").json(paths))
            if source == "stock":
                df = df.select(F.to_json(F.struct(*cols)).alias("raw_json"), F.input_file_name().alias("_file_name"))
            else:
                df = df.select(*cols, "_corrupt_record", F.input_file_name().alias("_file_name"))
            frames.append(df)
            continue
        groups: dict[tuple[str, ...], list[str]] = {}
        for p in paths:
            groups.setdefault(_header(p), []).append(p)
        for header, group in groups.items():
            schema = T.StructType([T.StructField(c, T.StringType()) for c in header] +
                                  [T.StructField("_corrupt_record", T.StringType())])
            df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
                  .option("columnNameOfCorruptRecord", "_corrupt_record").schema(schema).csv(group))
            df = df.select(*[(F.col(c) if c in header else F.lit(None).cast("string")).alias(c) for c in cols],
                           "_corrupt_record", F.input_file_name().alias("_file_name"))
            frames.append(df)
    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f, allowMissingColumns=True)
    return out


def ingest(spark: SparkSession, root: Path, include_public: bool = True, force: bool = False) -> LoadResult:
    """Land every not-yet-seen feed file under ``root`` into bronze Delta tables."""
    found = discover(root)
    if include_public:
        found.update(discover_public(root))
    seen: set[str] = set()
    man = _manifest(spark)
    if man is not None and not force:
        seen = {r.checksum for r in man.select("checksum").distinct().collect()}

    files: dict[str, list[dict]] = {}
    skipped: list[str] = []
    all_sums: list[str] = []
    for source, paths in found.items():
        for p in paths:
            c = sha256(p)
            if c in seen:
                skipped.append(str(p))
                continue
            seen.add(c)  # identical file twice in the same drop is loaded once
            files.setdefault(source, []).append({"path": str(p.resolve()), "checksum": c, "bytes": p.stat().st_size})
            all_sums.append(c)
    batch_id, load_ts = new_batch_id(all_sums)
    result = LoadResult(batch_id, load_ts, files, skipped)
    if not all_sums:
        return result

    manifest_rows = []
    for source, fl in files.items():
        sums = spark.createDataFrame([(f["path"], f["checksum"]) for f in fl], "_path string, _file_checksum string")
        if source.startswith("public_"):
            df = spark.read.format("binaryFile").load([f["path"] for f in fl]).withColumnRenamed("path", "_file_name")
            df = df.withColumn("_path", F.regexp_replace("_file_name", "^file:(//)?", ""))
        else:
            df = _read_feed(spark, source, fl).withColumn("_path", F.regexp_replace("_file_name", "^file:(//)?", ""))
        df = (df.join(F.broadcast(sums), "_path", "left").drop("_path")
              .withColumn("_source", F.lit(source)).withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_load_ts", F.lit(load_ts).cast("timestamp")))
        loc = lake("bronze", f"{source}_raw")
        df.write.format("delta").mode("append").partitionBy("_batch_id").save(loc)
        register_table(spark, "bronze", f"{source}_raw", loc)
        counts = {}
        if not source.startswith("public_"):
            counts = {r["_file_checksum"]: r["n"] for r in spark.read.format("delta").load(loc)
                      .where(F.col("_batch_id") == batch_id).groupBy("_file_checksum").agg(F.count("*").alias("n")).collect()}
        for f in fl:
            manifest_rows.append((batch_id, load_ts, source, f["path"], f["checksum"], int(f["bytes"]), int(counts.get(f["checksum"], 1))))

    mdf = spark.createDataFrame(manifest_rows, "batch_id string, load_ts string, source string, file_path string, "
                                               "checksum string, bytes long, rows long")
    mdf = mdf.withColumn("load_ts", F.col("load_ts").cast("timestamp"))
    loc = lake("bronze", MANIFEST)
    mdf.write.format("delta").mode("append").save(loc)
    register_table(spark, "bronze", MANIFEST, loc)
    return result
