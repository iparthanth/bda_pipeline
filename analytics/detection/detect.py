"""Outbreak detection per district and disease (AN-1), at scale in Spark.

Two established families are implemented and compared (AN-3):

* **EARS C1/C2/C3** (Hutwagner et al. 2003) with Spark *window functions*: the day's count is compared to
  the mean/SD of a 7-day baseline (C1: days t-7..t-1; C2: t-9..t-3 with a 2-day guard band; C3 = sum of the
  positive C2 exceedances over the last 3 days). SD is floored at max(sqrt(mean), 0.5) so sparse series
  do not divide by ~0 (documented deviation from the original).
* **Seasonal CUSUM** with grouped ``applyInPandas`` (one pandas UDF call per district x disease): the 7-day
  moving count is compared with a *historical-limits* baseline (Stroup et al. 1989 - same calendar window
  +-2 weeks in each previous year, scaled by this year's level over the last 2-7 weeks), standardised with a prospective
  over-dispersion estimate, and accumulated as a one-sided CUSUM (k = 0.5). Using previous seasons is what
  stops the monsoon dengue rise from being flagged every year (alert fatigue, conflict C1).

Every method runs on two inputs: ``n_signal`` (guarded: excludes back-filled records and duplicate
episodes) and ``n_naive`` (everything reported that day). Day-of-week reporting effects (Fri/Sat weekend)
are removed with national weekday factors estimated from history.

The generator's truth files are never read here (enforced by tests/test_detection_isolation.py).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common.config import lake, load_config
from pipeline.common.spark import register_table

KEYS = ["adm2_pcode", "disease_group"]


def weekday_factors(sig: DataFrame, col: str) -> DataFrame:
    by = sig.withColumn("dow", F.dayofweek("date")).groupBy("disease_group", "dow").agg(F.avg(col).alias("m"))
    tot = sig.groupBy("disease_group").agg(F.avg(col).alias("mt"))
    return by.join(tot, "disease_group").select("disease_group", "dow",
                                                F.greatest(F.col("m") / F.col("mt"), F.lit(0.2)).alias(f"dowf_{col}"))


def ears(df: DataFrame, x: str, prefix: str) -> DataFrame:
    w = Window.partitionBy(*KEYS).orderBy("date")
    for name, lo, hi in (("c1", -7, -1), ("c2", -9, -3)):
        mean = F.avg(x).over(w.rowsBetween(lo, hi))
        sd = F.stddev_samp(x).over(w.rowsBetween(lo, hi))
        sd_eff = F.greatest(F.coalesce(sd, F.lit(0.0)), F.sqrt(F.greatest(mean, F.lit(0.0))), F.lit(0.5))
        df = df.withColumn(f"{prefix}ears_{name}", (F.col(x) - mean) / sd_eff)
    df = df.withColumn("_c2pos", F.greatest(F.col(f"{prefix}ears_c2") - 1, F.lit(0.0)))
    df = df.withColumn(f"{prefix}ears_c3", F.sum("_c2pos").over(w.rowsBetween(-2, 0))).drop("_c2pos")
    return df


CUSUM_SCHEMA = T.StructType([
    T.StructField("adm2_pcode", T.StringType()), T.StructField("disease_group", T.StringType()),
    T.StructField("date", T.DateType()),
    T.StructField("cusum", T.DoubleType()), T.StructField("cusum_expected", T.DoubleType()),
    T.StructField("cusum_z", T.DoubleType()), T.StructField("cusum_sd", T.DoubleType()), T.StructField("sum7", T.DoubleType()),
    T.StructField("naive_cusum", T.DoubleType()), T.StructField("naive_sum7", T.DoubleType()),
])


def _cusum_series(x: np.ndarray, k: float, cap: float) -> tuple[np.ndarray, ...]:
    n = len(x)
    s7 = pd.Series(x).rolling(7, min_periods=7).sum().to_numpy()
    # historical limits: same window (+-14 days, weekly steps) in each previous year
    hist = []
    for y in (1, 2, 3, 4):
        for j in (-14, -7, 0, 7, 14):
            lag = 364 * y - j
            shifted = np.full(n, np.nan)
            if lag < n:
                shifted[lag:] = s7[: n - lag]
            hist.append(shifted)
    hist = np.vstack(hist)
    with np.errstate(all="ignore"):
        base_hist = np.nanmean(hist, axis=0)
        # level of this year vs. history over the last 2-7 weeks (2-week guard band so that a new outbreak
        # does not raise its own baseline); wide clip so year-to-year swings (e.g. a 2x dengue year) are absorbed
        recent = pd.Series(s7).shift(14).rolling(36, min_periods=21).mean().to_numpy()
        recent_hist = pd.Series(base_hist).shift(14).rolling(36, min_periods=21).mean().to_numpy()
        ratio = np.clip((recent + 1) / (recent_hist + 1), 0.25, 4.0)
        expected = base_hist * np.where(np.isnan(ratio), 1.0, ratio)
        fallback = pd.Series(s7).shift(7).rolling(28, min_periods=14).mean().to_numpy()   # < 1 year of history
        expected = np.where(np.isnan(expected), fallback, expected)
        expected = np.maximum(expected, 0.5)
        # CUSUM increments must be (nearly) independent: overlapping 7-day sums are strongly autocorrelated and
        # would make the CUSUM drift upwards. So the *daily* count is standardised against the daily share of
        # the seasonal 7-day expectation, with a prospective quasi-Poisson over-dispersion factor: the mean of
        # squared Pearson residuals over the past year, winsorised at 30 so past outbreaks do not inflate it.
        mu = expected / 7.0
        pearson = np.minimum((x - mu) ** 2 / mu, 30.0)
        phi = pd.Series(pearson).shift(7).rolling(365, min_periods=56).mean().to_numpy()
        phi = np.clip(np.where(np.isnan(phi), 2.0, phi), 1.0, 50.0)
        z = (x - mu) / np.sqrt(mu * phi)
        sd = np.sqrt(expected * phi)          # SD of the 7-day count, used for the dashboard threshold line
    c = np.zeros(n)
    acc = 0.0
    for i in range(n):
        zi = z[i]
        if np.isnan(zi):
            acc = 0.0
        else:
            acc = min(max(0.0, acc + zi - k), cap)
        c[i] = acc
    return c, expected, z, s7, sd


def cusum_udf(k: float, cap: float):
    def fn(pdf: pd.DataFrame) -> pd.DataFrame:
        pdf = pdf.sort_values("date")
        c, e, z, s7, sd = _cusum_series(pdf["x_signal"].to_numpy(dtype=float), k, cap)
        cn, _, _, s7n, _ = _cusum_series(pdf["x_naive"].to_numpy(dtype=float), k, cap)
        return pd.DataFrame({"adm2_pcode": pdf["adm2_pcode"].to_numpy(), "disease_group": pdf["disease_group"].to_numpy(),
                             "date": pdf["date"].to_numpy(), "cusum": c, "cusum_expected": e, "cusum_z": z, "cusum_sd": sd, "sum7": s7,
                             "naive_cusum": cn, "naive_sum7": s7n})
    return fn


def run(spark: SparkSession) -> DataFrame:
    cfg = load_config()["detection"]
    sig = spark.read.format("delta").load(lake("gold", "daily_signal"))
    fac = weekday_factors(sig, "n_signal").join(weekday_factors(sig, "n_naive"), ["disease_group", "dow"])
    df = (sig.withColumn("dow", F.dayofweek("date")).join(F.broadcast(fac), ["disease_group", "dow"])
          .withColumn("x_signal", F.col("n_signal") / F.col("dowf_n_signal"))
          .withColumn("x_naive", F.col("n_naive") / F.col("dowf_n_naive")))
    df = ears(df, "x_signal", "")
    df = ears(df, "x_naive", "naive_")
    w = Window.partitionBy(*KEYS).orderBy("date")
    df = (df.withColumn("raw7", F.sum("n_signal").over(w.rowsBetween(-6, 0)))
          .withColumn("naive_raw7", F.sum("n_naive").over(w.rowsBetween(-6, 0)))
          .withColumn("backfill7", F.sum("n_backfill").over(w.rowsBetween(-6, 0)))
          .withColumn("new_facility_share7", F.avg("new_facility_share").over(w.rowsBetween(-6, 0))))
    cus = (df.select(*KEYS, "date", "x_signal", "x_naive").groupBy(*KEYS)
           .applyInPandas(cusum_udf(float(cfg["cusum_k"]), float(max(cfg["cusum_thresholds"])) * 1.5), CUSUM_SCHEMA))
    out = (df.join(cus, KEYS + ["date"])
           .withColumn("reporting_flag",
                       F.when(F.col("new_facility_share7") > 0.15, F.lit("new_facility"))
                       .when(F.col("backfill7") > 0, F.lit("backfill_excluded")).otherwise(F.lit(None)))
           .select(*KEYS, "adm1_pcode", "date", "year", "n_signal", "n_naive", "n_backfill", "raw7", "naive_raw7",
                   "ears_c1", "ears_c2", "ears_c3", "naive_ears_c1", "naive_ears_c2", "naive_ears_c3",
                   "cusum", "cusum_expected", "cusum_z", "cusum_sd", "sum7", "naive_cusum", "naive_sum7",
                   "new_facility_share7", "backfill7", "reporting_flag", "bronze_batch_ids"))
    loc = lake("gold", "detection_daily")
    out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("year").save(loc)
    register_table(spark, "gold", "detection_daily", loc)
    return spark.read.format("delta").load(loc)


def alert_condition(method: str, threshold: float, naive: bool = False, min_count: int = 3) -> F.Column:
    """Alert rule used everywhere (evaluation, alerts table, streaming): statistic >= threshold, at least
    ``min_count`` cases in the last 7 days, and - for the guarded pipeline - no reporting-practice flag."""
    p = "naive_" if naive else ""
    count = F.col("naive_raw7" if naive else "raw7")
    cond = (F.col(f"{p}{method}") >= threshold) & (count >= min_count)
    if not naive:
        cond = cond & (F.coalesce(F.col("reporting_flag"), F.lit("")) != "new_facility")
    return cond


def publish_alerts(spark: SparkSession, method: str, threshold: float) -> None:
    """gold.alerts: every alert day at the operating point, with the evidence an official needs."""
    cfg = load_config()["detection"]
    d = spark.read.format("delta").load(lake("gold", "detection_daily"))
    stat = F.col(method)
    alerts = (d.where(alert_condition(method, threshold, min_count=int(cfg["min_count"])))
              .select(*KEYS, "adm1_pcode", "date", F.lit(method).alias("method"), F.lit(threshold).alias("threshold"),
                      stat.alias("statistic"), "raw7", "cusum_expected", "reporting_flag", "bronze_batch_ids"))
    review = (d.where((F.col(method) >= threshold) & (F.col("raw7") >= int(cfg["min_count"]))
                      & (F.col("reporting_flag") == "new_facility"))
              .select(*KEYS, "date").withColumn("status", F.lit("verify_reporting")))
    loc = lake("gold", "alerts")
    alerts.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
    register_table(spark, "gold", "alerts", loc)
    loc = lake("gold", "alerts_suppressed_reporting")
    review.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
    register_table(spark, "gold", "alerts_suppressed_reporting", loc)
