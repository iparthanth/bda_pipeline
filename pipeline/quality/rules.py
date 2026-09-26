"""Automated data-quality rules (DQ-1) expressed as Spark column predicates.

Each rule is ``Rule(id, description, fail)`` where ``fail`` is a boolean Column that is TRUE when the row
violates the rule. ``evaluate`` adds an array column ``_dq_failures`` with the IDs of every violated rule;
rows with a non-empty array go to the quarantine table with those reason codes, the rest continue to silver.
Rule IDs are stable and appear in the DQ report (DQ-2), the dashboard and the tests.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipeline.common.config import disease_code_map, load_config
from pipeline.common.schema import BACKGROUND_CODES, FALSE_VALUES, OUTCOMES, TRUE_VALUES

BD_BBOX = (20.5, 26.7, 88.0, 92.7)  # lat_min, lat_max, lon_min, lon_max
DATE_FMT = "yyyy-MM-dd"
ICD_PATTERN = r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,2})?$"


@dataclass(frozen=True)
class Rule:
    id: str
    description: str
    fail: Column


def _blank(c: str) -> Column:
    return F.col(c).isNull() | (F.trim(F.col(c)) == "")


def parse_date(c: str) -> Column:
    """Strict ISO date parse; malformed strings (e.g. 2023-13-45, 31/12/2023) give NULL."""
    return F.when(F.col(c).rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(F.col(c), DATE_FMT))


def _is_int(c: str) -> Column:
    return F.col(c).rlike(r"^-?\d+$")


def encounter_rules(load_date: Column) -> list[Rule]:
    q = load_config()["quality"]
    known_codes = list(disease_code_map()) + BACKGROUND_CODES
    onset, report = parse_date("onset_date"), parse_date("report_date")
    age = F.col("age_years").cast("int")
    boolean_ok = list(TRUE_VALUES | FALSE_VALUES)
    return [
        Rule("E01_SCHEMA", "row parses against the 11-column schema contract", F.col("_corrupt_record").isNotNull()),
        Rule("E02_ENCOUNTER_ID", "encounter_id present and well-formed", _blank("encounter_id") | ~F.col("encounter_id").rlike(r"^[A-Za-z0-9_-]{4,40}$")),
        Rule("E03_PATIENT_ID", "patient_id is a 10/13/17-digit national ID", _blank("patient_id") | ~F.col("patient_id").rlike(r"^(\d{10}|\d{13}|\d{17})$")),
        Rule("E04_FACILITY_ID", "facility_id present", _blank("facility_id")),
        Rule("E05_FACILITY_FK", "facility_id exists in the facility registry (referential integrity)", F.col("_fac_adm2").isNull() & ~_blank("facility_id")),
        Rule("E06_ONSET_DATE", "onset_date is a valid ISO date", onset.isNull()),
        Rule("E07_REPORT_DATE", "report_date is a valid ISO date", report.isNull()),
        Rule("E08_ONSET_LE_REPORT", "onset_date <= report_date", onset > report),
        Rule("E09_DELAY_PLAUSIBLE", f"report delay <= {q.get('max_plausible_delay_days', 60)} days",
             F.datediff(report, onset) > int(q.get("max_plausible_delay_days", 60))),
        Rule("E10_DATE_RANGE", "dates not before 2000-01-01 and not in the future", (onset < F.lit("2000-01-01").cast("date")) | (report > F.date_add(load_date, 1))),
        Rule("E11_AGE_RANGE", f"age_years is an integer in [{q['age_min']}, {q['age_max']}]",
             ~_is_int("age_years") | (age < q["age_min"]) | (age > q["age_max"]) | F.col("age_years").isNull()),
        Rule("E12_SEX", "sex in {M, F}", ~F.upper(F.trim(F.col("sex"))).isin(q["valid_sex"]) | F.col("sex").isNull()),
        Rule("E13_ICD10", "icd10_code is valid ICD-10 and a known category",
             F.col("icd10_code").isNull() | ~F.col("icd10_code").rlike(ICD_PATTERN) | ~F.substring("icd10_code", 1, 3).isin(known_codes)),
        Rule("E14_BOOLEAN", "lab_confirmed and admitted are booleans",
             ~F.lower(F.trim(F.col("lab_confirmed"))).isin(boolean_ok) | ~F.lower(F.trim(F.col("admitted"))).isin(boolean_ok)
             | F.col("lab_confirmed").isNull() | F.col("admitted").isNull()),
        Rule("E15_OUTCOME", "outcome in the allowed vocabulary", ~F.lower(F.trim(F.col("outcome"))).isin(OUTCOMES) | F.col("outcome").isNull()),
        Rule("E16_GEOGRAPHY", "facility district is a valid ADM2 pcode", F.col("_fac_adm2").isNotNull() & F.col("_fac_adm2_valid").isNull()),
    ]


def facility_rules() -> list[Rule]:
    lat, lon = F.col("latitude").cast("double"), F.col("longitude").cast("double")
    return [
        Rule("F01_FACILITY_ID", "facility_id present", _blank("facility_id")),
        Rule("F02_DUPLICATE", "facility_id unique in the registry load", F.col("_dup_rank") > 1),
        Rule("F03_COORDINATES", "coordinates inside Bangladesh",
             lat.isNull() | lon.isNull() | (lat < BD_BBOX[0]) | (lat > BD_BBOX[1]) | (lon < BD_BBOX[2]) | (lon > BD_BBOX[3])),
        Rule("F04_GEOGRAPHY", "adm2_pcode is a valid district", F.col("_adm2_valid").isNull()),
        Rule("F05_BEDS", "beds and icu_beds are non-negative integers, icu <= beds",
             ~_is_int("beds") | ~_is_int("icu_beds") | (F.col("beds").cast("int") < 0) | (F.col("icu_beds").cast("int") > F.col("beds").cast("int"))),
        Rule("F06_TYPE", "facility_type in {MCH, DH, UHC, SPEC, CLINIC}", ~F.col("facility_type").isin("MCH", "DH", "UHC", "SPEC", "CLINIC")),
    ]


def stock_rules() -> list[Rule]:
    qty = ["iv_saline", "ns1_kits", "ors", "antimalarials", "occupied_beds"]
    neg = F.lit(False)
    for c in qty:
        neg = neg | F.col(c).isNull() | (F.col(c) < 0)
    return [
        Rule("S01_JSON", "record is valid JSON with the contract fields", F.col("snapshot_date").isNull() & F.col("facility_id").isNull()),
        Rule("S02_FACILITY_FK", "facility exists in the registry", F.col("_beds").isNull()),
        Rule("S03_DATE", "snapshot_date is a valid ISO date", parse_date("snapshot_date").isNull()),
        Rule("S04_NON_NEGATIVE", "quantities present and non-negative", neg),
        Rule("S05_BEDS", "occupied_beds <= 1.5 x registered beds", F.col("occupied_beds") > 1.5 * F.col("_beds")),
    ]


def evaluate(df: DataFrame, rules: list[Rule]) -> DataFrame:
    """Add ``_dq_failures`` (array<string>) listing the IDs of every violated rule."""
    flags = [F.when(F.coalesce(r.fail, F.lit(False)), F.lit(r.id)) for r in rules]
    return df.withColumn("_dq_failures", F.array_compact(F.array(*flags)))


def rule_catalogue() -> list[dict]:
    """Rule IDs and descriptions (used by docs, the DQ report and the dashboard)."""
    out = []
    for group, rules in (("encounters", encounter_rules(F.current_date())), ("facilities", facility_rules()), ("stock", stock_rules())):
        out += [{"source": group, "rule_id": r.id, "description": r.description} for r in rules]
    return out
