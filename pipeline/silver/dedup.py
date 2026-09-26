"""Cross-facility patient de-duplication (DQ-4).

Two encounters are the *same illness episode* when they share the salted patient hash, the disease group
and compatible attributes (same sex, same or adjacent age band - an age typed as 24 at one facility and
25 at another straddles a band boundary) and their onset dates are at most ``dedup_window_days`` (7) apart
(chained). Every later encounter of an episode is linked to the first one in ``silver.episode_links``;
gold case counts exclude linked duplicates (encounter counts keep them).

Only the four surveillance disease groups are de-duplicated: repeated routine visits (hypertension,
antenatal care, ...) are legitimate separate encounters.

Incremental mode recomputes links only for the patients that appear in the new batch.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.common.config import lake, load_config
from pipeline.common.privacy import age_band_labels
from pipeline.common.spark import register_table, table_exists

LINKS = "episode_links"


def compute_links(enc: DataFrame, window_days: int) -> DataFrame:
    bands = age_band_labels()
    band_idx = F.create_map(*[F.lit(x) for i, b in enumerate(bands) for x in (b, i)])
    e = (enc.where(F.col("disease_group") != "other")
         .select("encounter_id", "patient_hash", "disease_group", "onset_date", "sex", "facility_id", "adm2_pcode",
                 band_idx[F.col("age_band")].alias("band_i")))
    w = Window.partitionBy("patient_hash", "disease_group").orderBy("onset_date", "encounter_id")
    e = (e.withColumn("prev_onset", F.lag("onset_date").over(w))
         .withColumn("prev_sex", F.lag("sex").over(w))
         .withColumn("prev_band", F.lag("band_i").over(w)))
    linked = (F.col("prev_onset").isNotNull()
              & (F.datediff("onset_date", "prev_onset") <= window_days)
              & (F.col("sex") == F.col("prev_sex"))
              & (F.abs(F.col("band_i") - F.col("prev_band")) <= 1))
    e = e.withColumn("new_episode", F.when(linked, 0).otherwise(1))
    e = e.withColumn("episode_no", F.sum("new_episode").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    first = Window.partitionBy("patient_hash", "disease_group", "episode_no").orderBy("onset_date", "encounter_id")
    e = e.withColumn("episode_first", F.first("encounter_id").over(first))
    return (e.where(F.col("new_episode") == 0)
            .select("encounter_id", F.col("episode_first").alias("duplicate_of"), "patient_hash", "disease_group",
                    "facility_id", "onset_date"))


def update_links(spark: SparkSession, batch_id: str | None) -> int:
    """Recompute episode links (all patients on the first load, batch patients afterwards)."""
    window_days = int(load_config()["quality"]["dedup_window_days"])
    enc = spark.read.format("delta").load(lake("silver", "encounters"))
    loc = lake("silver", LINKS)
    if batch_id is None or not table_exists(spark, loc):
        links = compute_links(enc, window_days)
        links.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(loc)
    else:
        patients = enc.where(F.col("_batch_id") == batch_id).select("patient_hash").distinct()
        subset = enc.join(F.broadcast(patients), "patient_hash", "left_semi")
        links = compute_links(subset, window_days).cache()
        tgt = DeltaTable.forPath(spark, loc)
        tgt.alias("t").merge(patients.alias("p"), "t.patient_hash = p.patient_hash").whenMatchedDelete().execute()
        links.write.format("delta").mode("append").save(loc)
    register_table(spark, "silver", LINKS, loc)
    return spark.read.format("delta").load(loc).count()
