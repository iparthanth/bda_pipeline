"""DQ-1 rules - boundary-value and equivalence-class tests for every encounter, facility and stock rule."""
from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from pipeline.quality import rules as R

VALID = {
    "encounter_id": "E3026230000001", "patient_id": "1234567890", "facility_id": "FAC10001",
    "onset_date": "2023-07-01", "report_date": "2023-07-04", "age_years": "34", "sex": "F",
    "icd10_code": "A90", "lab_confirmed": "1", "admitted": "0", "outcome": "recovered",
    "_corrupt_record": None, "_fac_adm2": "BD3026", "_fac_adm2_valid": 1,
}
LOAD_DATE = "2023-08-01"


def failures(spark, **overrides):
    row = {**VALID, **overrides}
    cols = list(row)
    df = spark.createDataFrame([tuple(row[c] for c in cols)], ", ".join(
        f"{c} {'int' if c == '_fac_adm2_valid' else 'string'}" for c in cols))
    out = R.evaluate(df, R.encounter_rules(F.to_date(F.lit(LOAD_DATE)))).first()["_dq_failures"]
    return set(out)


def test_valid_row_passes_all_rules(spark):
    assert failures(spark) == set()


def test_at_least_twelve_rules(spark):
    cat = R.rule_catalogue()
    assert len({r["rule_id"] for r in cat if r["source"] == "encounters"}) >= 12
    assert len({r["rule_id"] for r in cat}) == len(cat)  # rule IDs are unique


@pytest.mark.parametrize("age, ok", [("-1", False), ("0", True), ("1", True), ("119", True), ("120", True),
                                     ("121", False), ("212", False), ("abc", False), ("", False), ("34.5", False)])
def test_age_boundaries(spark, age, ok):
    assert ("E11_AGE_RANGE" not in failures(spark, age_years=age)) == ok


@pytest.mark.parametrize("onset, report, expect", [
    ("2023-07-04", "2023-07-04", set()),                       # delay 0 (lower boundary)
    ("2023-07-05", "2023-07-04", {"E08_ONSET_LE_REPORT"}),     # onset one day after report
    ("2023-05-05", "2023-07-04", set()),                       # delay 60 (upper boundary, accepted)
    ("2023-05-04", "2023-07-04", {"E09_DELAY_PLAUSIBLE"}),     # delay 61
    ("2023-13-45", "2023-07-04", {"E06_ONSET_DATE"}),
    ("2023-07-01", "31/12/2023", {"E07_REPORT_DATE"}),
    ("2023-07-01", "", {"E07_REPORT_DATE"}),
    ("1999-12-31", "2000-01-02", {"E10_DATE_RANGE"}),
    ("2023-07-30", "2023-08-03", {"E10_DATE_RANGE"}),          # report 2 days after the load date
    ("2023-07-30", "2023-08-02", set()),                       # load date + 1 (clock-skew tolerance boundary)
])
def test_date_rules(spark, onset, report, expect):
    got = failures(spark, onset_date=onset, report_date=report) & {
        "E06_ONSET_DATE", "E07_REPORT_DATE", "E08_ONSET_LE_REPORT", "E09_DELAY_PLAUSIBLE", "E10_DATE_RANGE"}
    assert got == expect


@pytest.mark.parametrize("code, ok", [("A90", True), ("A91", True), ("A09.0", True), ("J11.1", True), ("B50.9", True),
                                      ("I10", True), ("A9O", False), ("XYZ", False), ("", False), ("Q999", False),
                                      ("Z99", False), ("a90", False)])
def test_icd10(spark, code, ok):
    assert ("E13_ICD10" not in failures(spark, icd10_code=code)) == ok


@pytest.mark.parametrize("sex, ok", [("M", True), ("F", True), (" f ", True), ("X", False), ("U", False), ("", False), ("male", False)])
def test_sex(spark, sex, ok):
    assert ("E12_SEX" not in failures(spark, sex=sex)) == ok


@pytest.mark.parametrize("pid, ok", [("1234567890", True), ("1234567890123", True), ("12345678901234567", True),
                                     ("123456789", False), ("12345678901", False), ("", False), ("12345abcde", False)])
def test_patient_id(spark, pid, ok):
    assert ("E03_PATIENT_ID" not in failures(spark, patient_id=pid)) == ok


def test_booleans_outcome_ids(spark):
    assert "E14_BOOLEAN" in failures(spark, lab_confirmed="maybe")
    assert "E14_BOOLEAN" not in failures(spark, lab_confirmed="true", admitted="No")
    assert "E15_OUTCOME" in failures(spark, outcome="discharged!!")
    assert "E02_ENCOUNTER_ID" in failures(spark, encounter_id="")
    assert "E04_FACILITY_ID" in failures(spark, facility_id="")


def test_referential_integrity_and_geography(spark):
    assert "E05_FACILITY_FK" in failures(spark, _fac_adm2=None, _fac_adm2_valid=None)
    assert "E16_GEOGRAPHY" in failures(spark, _fac_adm2="BD9999", _fac_adm2_valid=None)
    assert "E01_SCHEMA" in failures(spark, _corrupt_record="garbage,row")


def test_multiple_failures_are_all_reported(spark):
    got = failures(spark, age_years="212", sex="X", icd10_code="XYZ")
    assert {"E11_AGE_RANGE", "E12_SEX", "E13_ICD10"} <= got


def test_facility_rules(spark):
    rows = [("FAC1", "DH", "23.8", "90.4", "100", "5", 1, 1), ("FAC2", "DH", "0.0", "90.4", "100", "5", 1, 1),
            ("FAC3", "DH", "23.8", "90.4", "10", "20", 1, 1), ("FAC4", "XX", "23.8", "90.4", "10", "0", 1, 1),
            ("FAC1", "DH", "23.8", "90.4", "100", "5", 2, 1), ("FAC5", "DH", "23.8", "90.4", "10", "0", 1, None)]
    df = spark.createDataFrame(rows, "facility_id string, facility_type string, latitude string, longitude string, beds string, "
                                     "icu_beds string, _dup_rank int, _adm2_valid int")
    got = {r.facility_id + str(r._dup_rank): set(r._dq_failures) for r in R.evaluate(df, R.facility_rules()).collect()}
    assert got["FAC11"] == set()
    assert "F03_COORDINATES" in got["FAC21"]
    assert "F05_BEDS" in got["FAC31"]
    assert "F06_TYPE" in got["FAC41"]
    assert "F02_DUPLICATE" in got["FAC12"]
    assert "F04_GEOGRAPHY" in got["FAC51"]


def test_stock_rules(spark):
    rows = [("2023-01-01", "FAC1", 10, 1, 1, 1, 50, 100), ("2023-01-01", "FAC1", -1, 1, 1, 1, 50, 100),
            ("2023-01-01", "FAC9", 10, 1, 1, 1, 50, None), ("2023-01-01", "FAC1", 10, 1, 1, 1, 151, 100),
            ("2023-02-30", "FAC1", 10, 1, 1, 1, 50, 100), ("2023-01-01", "FAC1", 10, 1, 1, 1, 150, 100)]
    df = spark.createDataFrame(rows, "snapshot_date string, facility_id string, iv_saline long, ns1_kits long, ors long, "
                                     "antimalarials long, occupied_beds long, _beds long")
    got = [set(r._dq_failures) for r in R.evaluate(df, R.stock_rules()).collect()]
    assert got[0] == set()
    assert "S04_NON_NEGATIVE" in got[1]
    assert "S02_FACILITY_FK" in got[2]
    assert "S05_BEDS" in got[3]       # 151 > 1.5 x 100
    assert "S03_DATE" in got[4]
    assert got[5] == set()            # exactly 1.5 x beds is allowed (boundary)
