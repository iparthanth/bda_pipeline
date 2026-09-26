"""Seasonal preparedness calendar (AN-9) per division and disease group, from STL decomposition.

For each division x disease the weekly case series (gold.weekly_counts aggregated in Spark) is decomposed
with STL (period 52, robust) on log(1 + cases), in parallel with ``applyInPandas``. The seasonal
component averaged by ISO week gives a multiplicative seasonal index (mean 1). The *season* is the longest
circular run of >= ``min_season_weeks`` weeks with index >= 1 + ``rise_threshold``; its first week is the
season onset and its maximum the peak. Actions for the next 12 months are scheduled backwards from them:

* start prevention campaign  = onset - campaign lead (dengue larval control 8 weeks, others 4-6 weeks);
* pre-position supplies      = onset - ``preposition_lead_weeks`` (4);
* surge staffing / beds      = peak - 2 weeks.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline.common.config import lake, load_config
from pipeline.common.spark import register_table

IDX_SCHEMA = T.StructType([T.StructField("adm1_pcode", T.StringType()), T.StructField("disease_group", T.StringType()),
                           T.StructField("iso_week", T.IntegerType()), T.StructField("seasonal_index", T.DoubleType()),
                           T.StructField("seasonal_strength", T.DoubleType())])


def stl_udf(pdf: pd.DataFrame) -> pd.DataFrame:
    from statsmodels.tsa.seasonal import STL

    pdf = pdf.sort_values("week_start")
    y = np.log1p(pdf["cases"].to_numpy(dtype=float))
    div, dis = pdf.adm1_pcode.iloc[0], pdf.disease_group.iloc[0]
    if len(y) < 104 or np.nanstd(y) == 0:
        return pd.DataFrame(columns=[f.name for f in IDX_SCHEMA.fields])
    res = STL(y, period=52, robust=True).fit()
    wk = pd.to_datetime(pdf["week_start"]).dt.isocalendar().week.clip(upper=52).to_numpy()
    s = pd.Series(res.seasonal).groupby(wk).mean()
    s = s.reindex(range(1, 53)).interpolate(limit_direction="both")
    idx = np.exp(s - s.mean())
    strength = max(0.0, 1 - np.var(res.resid) / max(np.var(res.resid + res.seasonal), 1e-9))
    return pd.DataFrame({"adm1_pcode": div, "disease_group": dis, "iso_week": np.arange(1, 53),
                         "seasonal_index": idx.to_numpy(), "seasonal_strength": strength})


def _season(index: np.ndarray, thr: float, min_len: int) -> tuple[int, int, int] | None:
    above = index >= 1 + thr
    if not above.any() or above.all():
        return None
    n = len(index)
    start = int(np.argmin(above))          # a week below threshold: unroll the circle from there
    best, cur = None, None
    for k in range(1, n + 1):
        i = (start + k) % n
        if above[i]:
            cur = [i, 1] if cur is None else [cur[0], cur[1] + 1]
            if best is None or cur[1] > best[1]:
                best = list(cur)
        else:
            cur = None
    if best is None or best[1] < min_len:
        return None
    onset, length = best
    weeks = [(onset + j) % n for j in range(length)]
    peak = max(weeks, key=lambda w: index[w])
    return onset + 1, peak + 1, weeks[-1] + 1  # ISO week numbers (1-based)


def run(spark: SparkSession, as_of: dt.date) -> pd.DataFrame:
    cfg = load_config()["calendar"]
    wc = spark.read.format("delta").load(lake("gold", "weekly_counts"))
    div = wc.groupBy("adm1_pcode", "disease_group", "week_start").agg(F.sum("cases").alias("cases"))
    last = wc.agg(F.max("week_start")).first()[0]
    div = div.where(F.col("week_start") < F.lit(last))  # drop the incomplete current week
    idx = div.groupBy("adm1_pcode", "disease_group").applyInPandas(stl_udf, IDX_SCHEMA)
    loc = lake("gold", "seasonal_index")
    idx.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
    register_table(spark, "gold", "seasonal_index", loc)
    pdf = spark.read.format("delta").load(loc).toPandas()
    names = spark.read.format("delta").load(lake("silver", "districts")).select("adm1_pcode", "adm1_name").distinct().toPandas()
    names = dict(zip(names.adm1_pcode, names.adm1_name))

    start = pd.Timestamp(as_of) - pd.Timedelta(days=pd.Timestamp(as_of).weekday()) + pd.Timedelta(weeks=1)
    horizon = [start + pd.Timedelta(weeks=i) for i in range(52)]

    def next_date(iso_week: int, lead: int = 0) -> str:
        target = ((iso_week - 1 - lead) % 52) + 1
        for d in horizon:
            if min(d.isocalendar().week, 52) == target:
                return d.date().isoformat()
        return horizon[0].date().isoformat()

    rows = []
    for (dv, dis), g in pdf.groupby(["adm1_pcode", "disease_group"]):
        g = g.sort_values("iso_week")
        s = _season(g.seasonal_index.to_numpy(), float(cfg["rise_threshold"]), int(cfg["min_season_weeks"]))
        if s is None:
            rows.append({"adm1_pcode": dv, "division": names.get(dv, dv), "disease_group": dis, "seasonal": False,
                         "seasonal_strength": float(g.seasonal_strength.iloc[0])})
            continue
        onset, peak, end = s
        lead = int(cfg["campaign_lead_weeks"].get(dis, 4))
        rows.append({
            "adm1_pcode": dv, "division": names.get(dv, dv), "disease_group": dis, "seasonal": True,
            "seasonal_strength": float(g.seasonal_strength.iloc[0]),
            "onset_iso_week": onset, "peak_iso_week": peak, "end_iso_week": end,
            "peak_index": float(g.seasonal_index.max()),
            "campaign_start": next_date(onset, lead), "preposition_by": next_date(onset, int(cfg["preposition_lead_weeks"])),
            "season_onset": next_date(onset), "surge_staffing_from": next_date(peak, 2), "season_peak": next_date(peak),
            "season_end": next_date(end),
        })
    cal = pd.DataFrame(rows)
    loc = lake("gold", "preparedness_calendar")
    spark.createDataFrame(cal.astype({c: "object" for c in cal.columns if cal[c].dtype == "object"})).write.format("delta") \
        .mode("overwrite").option("overwriteSchema", "true").save(loc)
    register_table(spark, "gold", "preparedness_calendar", loc)
    return cal
