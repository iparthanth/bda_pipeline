"""S2/S3 pinned real snapshots: HRSL band derivation and GSOD station-to-district interpolation."""
from __future__ import annotations

import gzip
import io

import numpy as np
import pandas as pd
import pytest

from generator import fetch_public as fp


def test_hrsl_bands_add_up_and_keep_real_layers():
    raw = pd.DataFrame([{"adm2_pcode": "BD9999", "population": 1000.0, "men": 480.0, "women": 520.0,
                         "children_under_five": 100.0, "youth_15_24": 200.0, "elderly_60_plus": 80.0}])
    out = fp.parse_hrsl(raw)
    assert set(out["age_band"]) == {"0-4", "5-14", "15-24", "25-44", "45-64", "65+"}
    assert out["population"].sum() == pytest.approx(1000.0)                     # nothing lost or invented
    band = out.groupby("age_band")["population"].sum()
    assert band["0-4"] == pytest.approx(100.0) and band["15-24"] == pytest.approx(200.0)   # real layers kept
    assert band["65+"] == pytest.approx(80.0 * (1 - fp.HRSL_SHARE_60_64))
    sex = out.groupby("sex")["population"].sum()
    assert sex["M"] / 1000.0 == pytest.approx(0.48)                            # district sex ratio applied


def _gsod_csv(station: str, lat: float, lon: float, days: list[str], temp_f: list[float], prcp_in: list[float]) -> bytes:
    df = pd.DataFrame({"STATION": station, "DATE": days, "LATITUDE": lat, "LONGITUDE": lon, "NAME": "TEST, BG",
                       "TEMP": temp_f, "DEWP": [t - 5 for t in temp_f], "PRCP": prcp_in})
    return gzip.compress(df.to_csv(index=False).encode())


def test_gsod_units_idw_and_climatology_fill():
    days = ["2024-01-01", "2024-01-02"]
    st = fp.read_gsod([
        ("a.csv.gz", _gsod_csv("A", 23.0, 90.0, days, [68.0, 9999.9], [0.10, 99.99])),   # day 2 missing at A
        ("b.csv.gz", _gsod_csv("B", 23.0, 91.0, days, [86.0, 86.0], [0.0, 1.00])),
    ])
    assert st.loc[st.station == "A", "temp_c"].iloc[0] == pytest.approx(20.0)             # F -> C
    assert st.loc[st.station == "A", "rain_mm"].iloc[0] == pytest.approx(2.54)            # inch -> mm
    assert np.isnan(st.loc[st.station == "A", "temp_c"].iloc[1])                          # 9999.9 = missing
    dist = pd.DataFrame({"adm2_pcode": ["AT_A", "MID"], "center_lat": [23.0, 23.0], "center_lon": [90.0, 90.5]})
    w = fp.gsod_to_districts(st, dist, "2024-01-01", "2024-01-03")
    a1 = w[(w.adm2_pcode == "AT_A") & (w.date == "2024-01-01")].iloc[0]
    assert a1.temperature_2m_mean == pytest.approx(20.0, abs=0.2)                          # station on top dominates
    mid1 = w[(w.adm2_pcode == "MID") & (w.date == "2024-01-01")].iloc[0]
    assert mid1.temperature_2m_mean == pytest.approx(25.0, abs=0.1)                        # equidistant -> mean
    a2 = w[(w.adm2_pcode == "AT_A") & (w.date == "2024-01-02")].iloc[0]
    assert a2.temperature_2m_mean == pytest.approx(30.0, abs=0.1) and a2.source_status == "real"   # A missing -> B
    last = w[w.date == "2024-01-03"]                                                        # after the last observation
    assert (last.source_status == "climatology_fill").all() and last.temperature_2m_mean.notna().all()


def test_pinned_snapshots_are_committed_and_real():
    from pathlib import Path

    from pipeline.common.config import path

    ref = Path(path("reference"))
    pop = pd.read_csv(ref / fp.HRSL_FILE)
    assert len(pop) == 64 and pop["population"].sum() > 1.5e8
    assert len(list((ref / "gsod").rglob("*.csv.gz"))) >= 100
