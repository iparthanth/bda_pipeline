"""Synthetic facility registry (S5): >= 600 facilities placed on the real OCHA COD-AB geography.

Every upazila gets an Upazila Health Complex, every district a District Hospital, divisional cities
get Medical College Hospitals, Dhaka gets specialised hospitals, and private clinics are added in
proportion to population (heavy skew towards Dhaka, which the Spark skew experiment exploits).
Names are synthetic and labelled as such.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

MCH_DISTRICTS = {"BD3026": 4, "BD2015": 1, "BD5081": 1, "BD4047": 1, "BD6091": 1, "BD1006": 1,
                 "BD5585": 1, "BD4561": 1, "BD2019": 1, "BD3029": 1, "BD5010": 1, "BD5527": 1}


def build_registry(dist: pd.DataFrame, upz: pd.DataFrame, rng: np.random.Generator) -> pd.DataFrame:
    rows: list[dict] = []

    def add(ftype, adm2, adm3, lat, lon, beds, icu, owner, name):
        rows.append({
            "facility_type": ftype, "adm2_pcode": adm2, "adm3_pcode": adm3,
            "latitude": round(lat + rng.normal(0, 0.01), 5), "longitude": round(lon + rng.normal(0, 0.01), 5),
            "beds": int(beds), "icu_beds": int(icu), "ownership": owner, "facility_name": name,
        })

    for _, d in dist.iterrows():
        add("DH", d.adm2_pcode, "", d.center_lat, d.center_lon, rng.integers(100, 251), rng.integers(0, 11),
            "public", f"{d.adm2_name} District Hospital (synthetic)")
        for k in range(MCH_DISTRICTS.get(d.adm2_pcode, 0)):
            add("MCH", d.adm2_pcode, "", d.center_lat, d.center_lon, rng.integers(500, 2601), rng.integers(10, 41),
                "public", f"{d.adm2_name} Medical College Hospital {k + 1} (synthetic)")
    for _, u in upz.iterrows():
        add("UHC", u.adm2_pcode, u.adm3_pcode, u.center_lat, u.center_lon, rng.choice([31, 50]), 0,
            "public", f"{u.adm3_name} Upazila Health Complex (synthetic)")
    dhaka = dist[dist.adm2_pcode == "BD3026"].iloc[0]
    for k in range(6):
        add("SPEC", "BD3026", "", dhaka.center_lat, dhaka.center_lon, rng.integers(250, 700), rng.integers(10, 31),
            "public", f"Dhaka Specialised Hospital {k + 1} (synthetic)")
    # private clinics ~ population^1.1 (Dhaka-heavy)
    pop = dist["population"].to_numpy(dtype=float)
    n_clin = np.maximum(1, np.round(190 * pop ** 1.1 / (pop ** 1.1).sum())).astype(int)
    upz_by_d = {k: g for k, g in upz.groupby("adm2_pcode")}
    for (_, d), n in zip(dist.iterrows(), n_clin):
        cand = upz_by_d.get(d.adm2_pcode)
        for k in range(n):
            u = cand.iloc[int(rng.integers(len(cand)))] if cand is not None else d
            add("CLINIC", d.adm2_pcode, getattr(u, "adm3_pcode", ""), u.center_lat, u.center_lon,
                rng.integers(10, 81), rng.integers(0, 4), "private", f"{d.adm2_name} Private Clinic {k + 1} (synthetic)")
    reg = pd.DataFrame(rows)
    reg.insert(0, "facility_id", [f"FAC{10001 + i}" for i in range(len(reg))])
    return reg


def facility_weights(reg: pd.DataFrame) -> dict[str, np.ndarray]:
    """Relative probability that an encounter of a disease group presents at each facility."""
    base = reg["beds"].to_numpy(dtype=float) ** 0.7
    typ = reg["facility_type"].to_numpy()
    w = {
        "background": base * np.where(typ == "CLINIC", 1.6, 1.0),
        "dengue": base * np.where(np.isin(typ, ["MCH", "SPEC"]), 1.8, 1.0),
        "diarrhoea": base * np.where(typ == "UHC", 1.5, 1.0),
        "ili": base,
        "malaria": base * np.where(typ == "UHC", 2.0, 1.0),
    }
    return w
