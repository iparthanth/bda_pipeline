"""Silver layer: clean, conformed, de-identified tables (+ quarantine and per-load DQ metrics).

* DQ-1 : every row is evaluated against the rule catalogue; failing rows go to ``silver.quarantine``
         with their reason codes (patient_id hashed there too - PRV-1).
* PRV-1: patient_id -> salted SHA-256 ``patient_hash``; exact age -> ``age_band``. Neither the raw ID nor
         the exact age exists beyond bronze.
* ING-4: MERGE on ``encounter_id`` makes loads idempotent: re-running a batch or receiving a resubmitted
         record never creates a duplicate; the earliest report date is kept as ``first_report_date``.
* ING-3: encounters are partitioned by (epi_year, adm1_pcode): most queries filter on a period and many
         dashboard/allocation queries on a division; both prune partitions. 6 years x 8 divisions = 48
         directories keeps files large enough (>= tens of MB) at SF=1.
"""
from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common.config import disease_code_map, lake, load_config, path
from pipeline.common.privacy import age_band, get_salt, hash_id
from pipeline.common.spark import register_table, table_exists
from pipeline.quality import rules as R

ENC_TABLE = "encounters"


def _bronze(spark: SparkSession, source: str, batch_id: str) -> DataFrame | None:
    loc = lake("bronze", f"{source}_raw")
    if not table_exists(spark, loc):
        return None
    return spark.read.format("delta").load(loc).where(F.col("_batch_id") == batch_id)


def _save(df: DataFrame, layer: str, table: str, spark: SparkSession, mode: str = "overwrite",
          partition_by: list[str] | None = None, replace_where: str | None = None) -> str:
    loc = lake(layer, table)
    w = df.write.format("delta").mode(mode)
    if mode == "overwrite":
        w = w.option("overwriteSchema", "true") if replace_where is None else w.option("replaceWhere", replace_where)
    if partition_by:
        w = w.partitionBy(*partition_by)
    w.save(loc)
    register_table(spark, layer, table, loc)
    return loc


# ------------------------------------------------------------------------------ reference / public sources
def silver_districts(spark: SparkSession, batch_id: str) -> None:
    """S1: district list (pcodes, names, division, centroid) from the raw GeoJSON properties."""
    raw = _bronze(spark, "public_boundaries", batch_id)
    rows: list[dict[str, Any]] = []
    status = "reference"
    if raw is not None:
        for r in raw.select("content").collect():
            gj = json.loads(bytes(r.content).decode("utf-8"))
            for f in gj.get("features", []):
                p = f["properties"]
                rows.append({k: p.get(k) for k in ["adm2_pcode", "adm2_name", "adm1_pcode", "adm1_name", "center_lat", "center_lon", "area_sqkm"]})
            status = "real"
    if not rows:
        if table_exists(spark, lake("silver", "districts")):
            return
        rows = pd.read_csv(f"{path('reference')}/districts.csv").to_dict("records")
    pdf = pd.DataFrame(rows).drop_duplicates("adm2_pcode").sort_values("adm2_pcode")
    pdf["source_status"] = status
    _save(spark.createDataFrame(pdf), "silver", "districts", spark)


def silver_population(spark: SparkSession, batch_id: str) -> None:
    """S2: population by district, age band and sex (denominators)."""
    raw = _bronze(spark, "public_population", batch_id)
    if raw is None or raw.limit(1).count() == 0:
        return
    from generator.fetch_public import HRSL_FILE, parse_cod_ps, parse_hrsl

    frames = []
    for r in raw.select("_file_name", "content").collect():
        text = bytes(r.content).decode("utf-8", errors="replace")
        pdf = pd.read_csv(io.StringIO(text))
        if "SYNTHETIC_FALLBACK" in r._file_name:
            pdf["source_status"] = "synthetic_fallback"
        elif r._file_name.endswith(HRSL_FILE):
            pdf = parse_hrsl(pdf)
            pdf["source_status"] = "real"
        else:
            if str(pdf.iloc[0, 0]).startswith("#"):
                pdf = pdf.iloc[1:]
            pdf = parse_cod_ps(pdf)
            pdf["source_status"] = "real"
        frames.append(pdf)
    pdf = pd.concat(frames)
    pdf["population"] = pdf["population"].astype(float)
    _save(spark.createDataFrame(pdf[["adm2_pcode", "age_band", "sex", "population", "source_status"]]), "silver", "population", spark)


def silver_weather(spark: SparkSession, batch_id: str) -> None:
    """S3: daily weather per district: raw NOAA GSOD station files interpolated to district centroids
    (small reference data, so pandas is used), raw Open-Meteo JSON, or the labelled fallback CSV."""
    raw = _bronze(spark, "public_weather", batch_id)
    if raw is None or raw.limit(1).count() == 0:
        return
    from generator.fetch_public import districts, gsod_to_districts, read_gsod, weather_range

    rows = raw.select("_file_name", "content").collect()
    gsod = [(r._file_name, bytes(r.content)) for r in rows if Path(r._file_name).name.startswith("gsod_")]
    rows = [r for r in rows if not Path(r._file_name).name.startswith("gsod_")]
    frames = [gsod_to_districts(read_gsod(gsod), districts(), *weather_range())] if gsod else []
    for r in rows:
        text = bytes(r.content).decode("utf-8")
        if r._file_name.endswith(".json"):
            p = json.loads(text)
            pdf = pd.DataFrame(p["daily"]).rename(columns={"time": "date"})
            pdf["adm2_pcode"] = p["_episcope"]["adm2_pcode"]
            pdf["source_status"] = "real"
        else:
            pdf = pd.read_csv(io.StringIO(text))
            pdf["source_status"] = "synthetic_fallback"
        frames.append(pdf)
    pdf = pd.concat(frames, ignore_index=True)
    sdf = (spark.createDataFrame(pdf)
           .select("adm2_pcode", F.to_date("date").alias("date"),
                   F.col("precipitation_sum").cast("double").alias("rain_mm"),
                   F.col("temperature_2m_mean").cast("double").alias("temp_c"),
                   F.col("relative_humidity_2m_mean").cast("double").alias("humidity"), "source_status")
           .dropDuplicates(["adm2_pcode", "date"]))
    _save(sdf, "silver", "weather_daily", spark)


# ------------------------------------------------------------------------------ facilities (S5)
def silver_facilities(spark: SparkSession, batch_id: str) -> dict | None:
    raw = _bronze(spark, "facilities", batch_id)
    if raw is None or raw.limit(1).count() == 0:
        return None
    dist = spark.read.format("delta").load(lake("silver", "districts")).select(
        F.col("adm2_pcode"), F.lit(1).alias("_adm2_valid"), "adm1_pcode")
    w = Window.partitionBy("facility_id").orderBy(F.monotonically_increasing_id())
    df = raw.withColumn("_dup_rank", F.row_number().over(w)).join(F.broadcast(dist), "adm2_pcode", "left")
    df = R.evaluate(df, R.facility_rules()).cache()
    valid = df.where(F.size("_dq_failures") == 0).select(
        "facility_id", "facility_name", "facility_type", "ownership", "adm2_pcode", "adm1_pcode", "adm3_pcode",
        F.col("latitude").cast("double").alias("latitude"), F.col("longitude").cast("double").alias("longitude"),
        F.col("beds").cast("int").alias("beds"), F.col("icu_beds").cast("int").alias("icu_beds"),
        "_batch_id", "_file_checksum")
    loc = lake("silver", "facilities")
    if table_exists(spark, loc):
        (DeltaTable.forPath(spark, loc).alias("t").merge(valid.alias("s"), "t.facility_id = s.facility_id")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        _save(valid, "silver", "facilities", spark)
    return _quarantine_and_report(spark, df, "facilities", batch_id, R.facility_rules(), keep_cols=[
        "facility_id", "facility_name", "adm2_pcode", "latitude", "longitude", "beds", "icu_beds"])


# ------------------------------------------------------------------------------ stock / beds (S6)
STOCK_QTY = ["iv_saline", "ns1_kits", "ors", "antimalarials", "occupied_beds"]
# parsed as strings first (numbers may arrive quoted, e.g. from JSON-array drops), then cast: a non-numeric
# quantity becomes NULL and fails rule S04 instead of silently becoming 0
STOCK_SCHEMA = T.StructType([T.StructField("snapshot_date", T.StringType()), T.StructField("facility_id", T.StringType())]
                            + [T.StructField(c, T.StringType()) for c in STOCK_QTY])


def silver_stock(spark: SparkSession, batch_id: str) -> dict | None:
    raw = _bronze(spark, "stock", batch_id)
    if raw is None or raw.limit(1).count() == 0:
        return None
    fac = spark.read.format("delta").load(lake("silver", "facilities")).select("facility_id", F.col("beds").alias("_beds"), "adm2_pcode")
    df = raw.withColumn("j", F.from_json("raw_json", STOCK_SCHEMA)).select("j.*", "_batch_id", "_file_checksum")
    df = df.select("snapshot_date", "facility_id", *[F.col(c).cast("long").alias(c) for c in STOCK_QTY], "_batch_id", "_file_checksum")
    df = df.join(F.broadcast(fac), "facility_id", "left")
    df = R.evaluate(df, R.stock_rules()).cache()
    valid = (df.where(F.size("_dq_failures") == 0)
             .withColumn("snapshot_date", R.parse_date("snapshot_date"))
             .dropDuplicates(["facility_id", "snapshot_date"])
             .withColumn("year", F.year("snapshot_date"))
             .drop("_dq_failures", "_beds"))
    loc = lake("silver", "stock")
    if table_exists(spark, loc):
        years = [r.year for r in valid.select("year").distinct().collect()]
        cond = "t.facility_id = s.facility_id AND t.snapshot_date = s.snapshot_date"
        if years:
            cond += f" AND t.year IN ({','.join(map(str, years))})"
        (DeltaTable.forPath(spark, loc).alias("t").merge(valid.alias("s"), cond)
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    else:
        _save(valid.repartition("year"), "silver", "stock", spark, partition_by=["year"])
    return _quarantine_and_report(spark, df, "stock", batch_id, R.stock_rules(),
                                  keep_cols=["facility_id", "snapshot_date", "iv_saline", "ns1_kits", "ors", "antimalarials", "occupied_beds"])


# ------------------------------------------------------------------------------ encounters (S4)
def _disease_group(icd3: F.Column) -> F.Column:
    m = disease_code_map()
    return F.coalesce(F.create_map(*[F.lit(x) for kv in m.items() for x in kv])[icd3], F.lit("other"))


def silver_encounters(spark: SparkSession, batch_id: str, load_ts: str) -> dict | None:
    raw = _bronze(spark, "encounters", batch_id)
    if raw is None:
        return None
    salt = get_salt()
    fac = spark.read.format("delta").load(lake("silver", "facilities")).select(
        "facility_id", F.col("adm2_pcode").alias("_fac_adm2"), "facility_type")
    dist = spark.read.format("delta").load(lake("silver", "districts")).select(
        F.col("adm2_pcode").alias("_fac_adm2"), "adm1_pcode", F.lit(1).alias("_fac_adm2_valid"))
    load_date = F.to_date(F.lit(load_ts[:10]))
    # ENG-3 optimisation 1: broadcast the small registry/district tables (no shuffle of the 25M rows)
    df = raw.join(F.broadcast(fac), "facility_id", "left").join(F.broadcast(dist), "_fac_adm2", "left")
    df = R.evaluate(df, R.encounter_rules(load_date)).withColumn("patient_hash", hash_id("patient_id", salt))
    df = df.drop("patient_id").persist()

    onset, report = R.parse_date("onset_date"), R.parse_date("report_date")
    week_start = F.date_trunc("week", onset).cast("date")
    valid = (df.where(F.size("_dq_failures") == 0)
             .select(
                 "encounter_id", "patient_hash", "facility_id", "facility_type",
                 F.col("_fac_adm2").alias("adm2_pcode"), "adm1_pcode",
                 onset.alias("onset_date"), report.alias("report_date"),
                 age_band(F.col("age_years").cast("int")).alias("age_band"),
                 F.upper(F.trim("sex")).alias("sex"),
                 F.upper(F.trim("icd10_code")).alias("icd10_code"),
                 _disease_group(F.substring(F.upper(F.trim("icd10_code")), 1, 3)).alias("disease_group"),
                 F.lower(F.trim("lab_confirmed")).isin("1", "true", "t", "yes", "y").alias("lab_confirmed"),
                 F.lower(F.trim("admitted")).isin("1", "true", "t", "yes", "y").alias("admitted"),
                 F.lower(F.trim("outcome")).alias("outcome"),
                 week_start.alias("week_start"),
                 F.year(F.date_add(week_start, 3)).alias("epi_year"),
                 F.weekofyear(onset).alias("epi_week"),
                 F.datediff(report, onset).alias("report_delay"),
                 "_batch_id", "_file_checksum")
             .withColumn("late_beyond_contract", F.col("report_delay") > load_config()["quality"]["max_report_delay_days"]))

    # resubmissions inside the batch: keep the newest version, remember the first report date
    w = Window.partitionBy("encounter_id")
    w_ord = w.orderBy(F.col("report_date").desc(), F.col("_file_checksum"))
    batch = (valid.withColumn("first_report_date", F.min("report_date").over(w))
             .withColumn("last_report_date", F.max("report_date").over(w))
             .withColumn("_rn", F.row_number().over(w_ord))
             .where("_rn = 1").drop("_rn")).persist()   # reused by the write and by the load statistics

    loc = lake("silver", ENC_TABLE)
    if table_exists(spark, loc):
        years = [r.epi_year for r in batch.select("epi_year").distinct().collect()]
        cond = ("t.encounter_id = s.encounter_id AND t.epi_year = s.epi_year AND t.adm1_pcode = s.adm1_pcode"
                + (f" AND t.epi_year IN ({','.join(map(str, years))})" if years else ""))
        upd = {c: f"s.{c}" for c in batch.columns if c not in ("first_report_date", "last_report_date", "report_date")}
        upd["first_report_date"] = "least(t.first_report_date, s.first_report_date)"
        upd["last_report_date"] = "greatest(t.last_report_date, s.last_report_date)"
        upd["report_date"] = "least(t.report_date, s.report_date)"
        (DeltaTable.forPath(spark, loc).alias("t").merge(batch.alias("s"), cond)
         .whenMatchedUpdate(set=upd).whenNotMatchedInsertAll().execute())
    else:
        _save(batch.repartition("epi_year", "adm1_pcode"), "silver", ENC_TABLE, spark, partition_by=["epi_year", "adm1_pcode"])

    report = _quarantine_and_report(spark, df, "encounters", batch_id, R.encounter_rules(load_date),
                                    keep_cols=["encounter_id", "patient_hash", "facility_id", "onset_date", "report_date",
                                               "age_years", "sex", "icd10_code", "lab_confirmed", "admitted", "outcome"])
    n_valid = report["rows_in"] - report["rows_quarantined"]
    report["duplicates_removed"] = n_valid - batch.count()
    agg = batch.agg(F.min("onset_date").alias("min_onset"), F.max("first_report_date").alias("max_report")).first()
    report["min_onset"] = str(agg.min_onset) if agg.min_onset else None
    report["max_report"] = str(agg.max_report) if agg.max_report else None
    df.unpersist()
    batch.unpersist()
    return report


# ------------------------------------------------------------------------------ quarantine + DQ metrics
def _quarantine_and_report(spark: SparkSession, df: DataFrame, source: str, batch_id: str, rules: list[R.Rule],
                           keep_cols: list[str]) -> dict:
    bad = df.where(F.size("_dq_failures") > 0)
    q = bad.select(F.lit(source).alias("source"), F.col("_dq_failures").alias("reason_codes"),
                   F.to_json(F.struct(*[F.col(c).cast("string") for c in keep_cols])).alias("record"),
                   "_batch_id", "_file_checksum")
    # idempotent: rows previously quarantined from the same files (forced re-ingest) are replaced, not duplicated
    qloc = lake("silver", "quarantine")
    if table_exists(spark, qloc):
        sums = [r._file_checksum for r in df.select("_file_checksum").distinct().collect() if r._file_checksum]
        if sums:
            DeltaTable.forPath(spark, qloc).delete((F.col("source") == source) & F.col("_file_checksum").isin(sums))
    _save(q, "silver", "quarantine", spark, mode="append", partition_by=["source"])
    stats = df.agg(F.count("*").alias("rows_in"), F.sum(F.when(F.size("_dq_failures") > 0, 1).otherwise(0)).alias("rows_quarantined")).first()
    fails = {r.rule: r.n for r in df.select(F.explode("_dq_failures").alias("rule")).groupBy("rule").agg(F.count("*").alias("n")).collect()}
    rows_in = int(stats.rows_in or 0)
    return {
        "source": source, "batch_id": batch_id, "rows_in": rows_in,
        "rows_quarantined": int(stats.rows_quarantined or 0),
        "rules": [{"rule_id": r.id, "description": r.description, "failed_rows": int(fails.get(r.id, 0)),
                   "pass_rate": (1 - fails.get(r.id, 0) / rows_in) if rows_in else 1.0} for r in rules],
    }
