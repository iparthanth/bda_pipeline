"""Schema contract for the landing-zone feeds (Section 5.2 of the brief).

The generator writes exactly these columns and the ingestion reads files by *sniffing their header*
against these contracts, so a drop can use any file names and any number of files (hidden test drop).
Bronze keeps every value as the raw string (ING-1: no modification); typing happens in silver.
"""
from __future__ import annotations

ENCOUNTER_COLUMNS = [
    "encounter_id", "patient_id", "facility_id", "onset_date", "report_date",
    "age_years", "sex", "icd10_code", "lab_confirmed", "admitted", "outcome",
]
FACILITY_COLUMNS = [
    "facility_id", "facility_name", "facility_type", "ownership", "adm2_pcode", "adm3_pcode",
    "latitude", "longitude", "beds", "icu_beds",
]
STOCK_COLUMNS = [
    "snapshot_date", "facility_id", "iv_saline", "ns1_kits", "ors", "antimalarials", "occupied_beds",
]

# Minimal column sets used to recognise a file's source from its header (extra columns are tolerated).
SIGNATURES = {
    "encounters": {"encounter_id", "patient_id", "facility_id", "onset_date", "report_date", "icd10_code"},
    "facilities": {"facility_id", "facility_type", "beds", "adm2_pcode"},
    "stock": {"snapshot_date", "facility_id", "iv_saline", "occupied_beds"},
}
SOURCE_COLUMNS = {"encounters": ENCOUNTER_COLUMNS, "facilities": FACILITY_COLUMNS, "stock": STOCK_COLUMNS}

OUTCOMES = ["recovered", "referred", "died", "lama", "unknown"]
TRUE_VALUES = {"1", "true", "t", "yes", "y"}
FALSE_VALUES = {"0", "false", "f", "no", "n"}

# Background (non-target) ICD-10 categories generated as routine encounters and accepted as valid.
BACKGROUND_CODES = [
    "I10", "E11", "J06", "J18", "K29", "K30", "R50", "A01", "N39", "O80", "Z34", "S06", "T14", "M54",
    "L30", "H10", "J45", "D50", "K52", "B34", "R10", "R51", "J20", "A15", "I20", "O47", "K35", "E86",
    "R05", "T78",
]
