"""k-anonymity check for record-level extracts (PRV-4).

A research extract of de-identified encounters (no hashes, no IDs) keeps only quasi-identifiers
(age band, sex, place, time) and the disease group. ``check`` computes equivalence-class sizes in Spark
and walks a generalisation ladder until every released class has at least k records; residual classes
below k are suppressed and the share of suppressed records (information loss) is reported.

Ladder (from most to least detailed):
  L0 district  x ISO week   x 6 age bands x sex
  L1 district  x month      x 6 age bands x sex
  L2 division  x month      x 6 age bands x sex
  L3 division  x month      x 3 age bands (0-14, 15-44, 45+) x sex
  L4 division  x quarter    x 3 age bands
"""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.common.config import lake, load_config, path

def levels() -> list[tuple[str, list]]:
    """Generalisation ladder (built lazily: Spark columns need an active session)."""
    broad = (F.when(F.col("age_band").isin("0-4", "5-14"), "0-14")
             .when(F.col("age_band").isin("15-24", "25-44"), "15-44").otherwise("45+"))
    month = F.date_format("onset_date", "yyyy-MM")
    return [
        ("L0", [F.col("adm2_pcode").alias("place"), F.col("week_start").cast("string").alias("time"), F.col("age_band").alias("age"), "sex"]),
        ("L1", [F.col("adm2_pcode").alias("place"), month.alias("time"), F.col("age_band").alias("age"), "sex"]),
        ("L2", [F.col("adm1_pcode").alias("place"), month.alias("time"), F.col("age_band").alias("age"), "sex"]),
        ("L3", [F.col("adm1_pcode").alias("place"), month.alias("time"), broad.alias("age"), "sex"]),
        ("L4", [F.col("adm1_pcode").alias("place"), F.concat(F.year("onset_date"), F.lit("-Q"), F.quarter("onset_date")).alias("time"),
                broad.alias("age"), "sex"]),
    ]


def check(spark: SparkSession, disease: str | None = "malaria", max_suppressed: float = 0.01, write_extract: bool = True) -> list[dict]:
    k = int(load_config()["privacy"]["k_anonymity_k"])
    enc = spark.read.format("delta").load(lake("silver", "encounters")).where(F.col("disease_group") != "other")
    if disease:
        enc = enc.where(F.col("disease_group") == disease)
    enc = enc.cache()
    total = enc.count()
    report, chosen = [], None
    for name, cols in levels():
        ext = enc.select(*cols, "disease_group")
        classes = ext.groupBy("place", "time", "age", "sex", "disease_group").count()
        st = classes.agg(F.min("count").alias("kmin"), F.count("*").alias("n_classes"),
                         F.sum(F.when(F.col("count") < k, F.col("count")).otherwise(0)).alias("below_k")).first()
        row = {"level": name, "records": total, "k_min_before_suppression": int(st.kmin or 0),
               "classes": int(st.n_classes), "records_in_classes_below_k": int(st.below_k or 0),
               "suppressed_share": (st.below_k or 0) / max(total, 1), "k_required": k}
        row["meets_k_after_suppression"] = row["suppressed_share"] <= max_suppressed
        report.append(row)
        if chosen is None and row["meets_k_after_suppression"]:
            chosen = (name, ext, classes)
    if write_extract and chosen is not None:
        name, ext, classes = chosen
        ok = classes.where(F.col("count") >= k).drop("count")
        released = ext.join(ok, ["place", "time", "age", "sex", "disease_group"], "left_semi")
        out = Path(path("data_root")) / "exports" / f"research_extract_{disease or 'all'}_{name}"
        released.write.mode("overwrite").parquet(str(out))
        k_after = released.groupBy("place", "time", "age", "sex", "disease_group").count().agg(F.min("count")).first()[0]
        report.append({"level": f"{name}-released", "records": released.count(), "k_min_before_suppression": int(k_after or 0),
                       "classes": None, "records_in_classes_below_k": 0, "suppressed_share": 1 - released.count() / max(total, 1),
                       "k_required": k, "meets_k_after_suppression": bool((k_after or k) >= k), "path": str(out)})
    enc.unpersist()
    return report
