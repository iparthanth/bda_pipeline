"""EpiScope synthetic data generator (S4 encounters, S5 facility registry, S6 stock/bed snapshots).

Usage:  python -m generator.generate [--sf 1.0] [--seed 20260101] [--out data]

* Parameterised by a scale factor SF and a seed; deterministic for a given (SF, seed): every
  (district, year) chunk has its own ``SeedSequence([seed, district, year])`` so the output does not
  depend on the number of worker processes.
* SF = 1 produces > 25 million encounter records with report dates in the history window.
* Encounter reports up to ``history_end`` go to ``data/landing`` (the historical load); later reports go
  to ``data/drops/<YYYY-MM-DD>/`` as daily incremental drops in the *same schema* (used to demonstrate
  ING-4 and to rehearse the hidden 14-day test drop).
* Truth files (``ground_truth_outbreaks.csv`` etc.) are written to ``data/ground_truth`` which the
  detection code never reads (enforced by tests/test_detection_isolation.py).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv

from generator import epi_model
from generator.facilities import build_registry, facility_weights
from generator.fetch_public import districts, load_population, load_weather_daily
from pipeline.common.config import load_config, path
from pipeline.common.schema import BACKGROUND_CODES, ENCOUNTER_COLUMNS, FACILITY_COLUMNS, STOCK_COLUMNS

GROUPS = epi_model.GROUPS
AGE_BANDS = ["0-4", "5-14", "15-24", "25-44", "45-64", "65+"]
AGE_RANGE = {"0-4": (0, 4), "5-14": (5, 14), "15-24": (15, 24), "25-44": (25, 44), "45-64": (45, 64), "65+": (65, 95)}
AGE_RISK = {
    "dengue": [0.5, 1.0, 1.6, 1.4, 0.9, 0.7], "diarrhoea": [3.0, 1.0, 0.7, 0.8, 0.9, 1.4],
    "ili": [2.0, 1.5, 0.8, 0.7, 1.0, 1.8], "malaria": [1.0, 1.2, 1.1, 1.0, 0.9, 0.8],
    "background": [1.2, 0.7, 0.8, 1.0, 1.3, 1.8],
}
ICD = {
    "dengue": (["A90", "A91"], [0.85, 0.15]),
    "diarrhoea": (["A09", "A09.0", "A00", "A00.9"], [0.55, 0.35, 0.06, 0.04]),
    "ili": (["J11", "J11.1", "J10", "J10.1"], [0.45, 0.25, 0.2, 0.1]),
    "malaria": (["B50", "B50.9", "B51", "B52", "B53", "B54"], [0.45, 0.3, 0.13, 0.01, 0.01, 0.1]),
}
DELAY_MEAN = {"MCH": 2.0, "SPEC": 2.0, "DH": 2.5, "UHC": 3.5, "CLINIC": 4.0}
# Eid al-Fitr / Eid al-Adha (3-day closures) and fixed national holidays of Bangladesh.
EID = ["2021-05-13", "2021-07-21", "2022-05-03", "2022-07-10", "2023-04-22", "2023-06-29",
       "2024-04-11", "2024-06-17", "2025-03-31", "2025-06-07", "2026-03-20", "2026-05-27"]
FIXED_HOLIDAYS = ["02-21", "03-17", "03-26", "04-14", "05-01", "08-15", "12-16", "12-25"]
INVALID_MIX = {  # fraction of all rows (about 1% in total) - exercises every DQ rule
    "age": 0.0035, "sex": 0.002, "icd": 0.0015, "onset_after_report": 0.001, "bad_date": 0.0008,
    "very_late": 0.0005, "bool": 0.0005, "outcome": 0.0003, "patient_missing": 0.0003, "enc_missing": 0.0002,
}
ORPHAN_FRAC = 0.005
RESUBMIT_FRAC = 0.02
XFAC_DUP_FRAC = 0.012
REPEAT_PATIENT_FRAC = 0.003


# ------------------------------------------------------------------------------------------ helpers
def _calendar(start: dt.date, n: int) -> dict[str, np.ndarray]:
    days = pd.date_range(pd.Timestamp(start), periods=n, freq="D")
    hol = set()
    for e in EID:
        for k in range(3):
            hol.add((pd.Timestamp(e) + pd.Timedelta(days=k)).strftime("%Y-%m-%d"))
    off = np.array([(d.weekday() in (4, 5)) or d.strftime("%Y-%m-%d") in hol or d.strftime("%m-%d") in FIXED_HOLIDAYS
                    for d in days])
    nxt = np.arange(n)
    for i in range(n - 2, -1, -1):
        nxt[i] = nxt[i + 1] if off[i] else i
    prv = np.arange(n)
    for i in range(1, n):
        prv[i] = prv[i - 1] if off[i] else i
    return {"off": off, "next": nxt, "prev": prv}


def _date_str(base: np.datetime64, offsets: np.ndarray) -> np.ndarray:
    return (base + offsets.astype("timedelta64[D]")).astype(str)


def _write_csv(df: dict[str, np.ndarray], columns: list[str], out: Path) -> None:
    table = pa.table({c: pa.array(df[c].astype(str)) for c in columns})
    pacsv.write_csv(table, str(out))


# ------------------------------------------------------------------------------------------ worker
def gen_chunk(task: dict) -> dict:
    """Generate every encounter with onset in one (district, year) chunk."""
    t_start = time.time()
    rng = np.random.default_rng([task["seed"], task["d_idx"], task["year"]])
    t0, t1 = task["t0"], task["t1"]
    fac = task["facilities"]
    cal = task["calendar"]
    n_fac = len(fac["id"])
    base = np.datetime64(task["start"], "D")

    parts: dict[str, list[np.ndarray]] = {k: [] for k in ("onset", "group", "fac", "mult")}
    for gi, g in enumerate(GROUPS):
        lam = task["lam"][g] * task["mult"][g]
        size = epi_model.NB_SIZE[g]
        counts = rng.poisson(rng.gamma(size, lam / size))
        onset = np.repeat(np.arange(t0, t1), counts)
        if onset.size == 0:
            continue
        w = fac["w_" + g]
        parts["onset"].append(onset)
        parts["group"].append(np.full(onset.size, gi, dtype=np.int8))
        parts["fac"].append(rng.choice(n_fac, size=onset.size, p=w / w.sum()))
        parts["mult"].append(np.repeat(task["mult"][g], counts))
    if not parts["onset"]:
        return {"rows": 0}
    onset = np.concatenate(parts["onset"])
    group = np.concatenate(parts["group"])
    fidx = np.concatenate(parts["fac"])
    omult = np.concatenate(parts["mult"])

    n = onset.size

    # --- demographics consistent with S2 population structure
    band = np.empty(n, dtype=np.int8)
    for gi, g in enumerate(GROUPS):
        m = group == gi
        p = task["band_share"] * np.array(AGE_RISK[g])
        band[m] = rng.choice(6, size=m.sum(), p=p / p.sum())
    lo = np.array([AGE_RANGE[b][0] for b in AGE_BANDS])[band]
    hi = np.array([AGE_RANGE[b][1] for b in AGE_BANDS])[band]
    age = rng.integers(lo, hi + 1)
    sex = np.where(rng.random(n) < task["male_share"][band], "M", "F")

    # --- ICD-10 code (severe forms more frequent during outbreaks)
    icd = np.empty(n, dtype=object)
    for gi, g in enumerate(GROUPS):
        m = np.nonzero(group == gi)[0]
        if g == "background":
            icd[m] = rng.choice(BACKGROUND_CODES, size=m.size)
            continue
        codes, p = ICD[g]
        icd[m] = rng.choice(codes, size=m.size, p=p)
        if g == "dengue":
            severe = rng.random(m.size) < 0.12 * np.clip(omult[m] - 1, 0, 3)
            icd[m[severe]] = "A91"
        if g == "diarrhoea":
            chol = rng.random(m.size) < 0.10 * np.clip(omult[m] - 1, 0, 3)
            icd[m[chol]] = "A00"

    # --- severity cascade
    lab_p = np.array([0.55, 0.12, 0.05, 0.95, 0.1])[group]
    adm_p = np.array([0.22, 0.15, 0.05, 0.2, 0.08])[group].astype(float)
    is_d = group == 0
    adm_p[is_d] += 0.15 * np.clip((omult[is_d] - 1) / 3, 0, 1)            # dengue admissions rise in outbreaks
    adm_p[icd == "A91"] = 0.8
    adm_p[icd == "A00"] = 0.5
    ili_surge = (group == 2) & (omult > 1.3)
    adm_p[ili_surge & (band == 5)] = 0.25
    lab_p = np.where(ili_surge, 0.2, lab_p)
    lab = rng.random(n) < lab_p
    admitted = rng.random(n) < np.clip(adm_p, 0, 1)
    die_p = np.where(admitted, np.array([0.004, 0.001, 0.003, 0.004, 0.002])[group], 0.0002)
    die_p = np.where(icd == "A91", 0.02, die_p)
    die_p = np.where(ili_surge & (band == 5) & admitted, 0.03, die_p)
    u = rng.random(n)
    outcome = np.where(u < die_p, "died", np.where(u < die_p + 0.03, "referred",
                       np.where(u < die_p + 0.045, "lama", np.where(u < die_p + 0.06, "unknown", "recovered"))))

    # --- reporting delay: right-skewed 0-21 days, fewer reports on weekends/holidays
    mean = np.array([DELAY_MEAN[t] for t in fac["type"]])[fidx]
    size = 1.3
    delay = rng.negative_binomial(size, size / (size + mean))
    redo = delay > 21
    delay[redo] = rng.negative_binomial(size, size / (size + mean[redo]))
    delay = np.minimum(delay, 21)
    report = onset + delay
    report = np.minimum(report, len(cal["off"]) - 1)
    shift = cal["off"][report] & (rng.random(n) < 0.6)
    moved = cal["next"][report[shift]]
    too_late = moved - onset[shift] > 21
    moved[too_late] = np.maximum(cal["prev"][report[shift]][too_late], onset[shift][too_late])
    report[shift] = moved

    # --- identifiers
    pid = np.where(rng.random(n) < 0.7,
                   rng.integers(1_000_000_000, 9_999_999_999, n).astype(str),
                   np.char.add(rng.integers(19_000_000, 20_260_000, n).astype(str),
                               np.char.zfill(rng.integers(0, 1_000_000_000, n).astype(str), 9)))
    seq = np.arange(n)
    prefix = f"E{task['pcode'][2:]}{task['year'] % 100:02d}"
    truth_pairs: list[tuple[str, str, str]] = []

    # --- cross-facility duplicate visits of the same patient (DQ-4 ground truth)
    tgt = np.nonzero(group < 4)[0]
    dup_src = rng.choice(tgt, size=int(XFAC_DUP_FRAC * tgt.size), replace=False) if tgt.size and n_fac > 1 else np.array([], int)
    rep_src = rng.choice(n, size=int(REPEAT_PATIENT_FRAC * n), replace=False)
    extra = {}
    if dup_src.size or rep_src.size:
        src = np.concatenate([dup_src, rep_src])
        k = src.size
        new_on = onset[src].copy()
        new_on[: dup_src.size] += rng.integers(0, 4, dup_src.size)                 # same episode, <= 3 days later
        new_on[dup_src.size:] += rng.integers(30, 200, rep_src.size)               # separate episode
        new_fac = fidx[src].copy()
        new_fac[: dup_src.size] = (fidx[dup_src] + rng.integers(1, n_fac, dup_src.size)) % n_fac
        # data-entry noise between facilities: age typed +-1 or +-2 years, and a wrong sex in 3% of duplicates
        new_age = age[src] + np.where(rng.random(k) < 0.3, rng.choice([-2, -1, 1, 2], k), 0)
        new_age[dup_src.size:] = age[rep_src]
        new_sex = sex[src].copy()
        flip = np.zeros(k, dtype=bool)
        flip[: dup_src.size] = rng.random(dup_src.size) < 0.03
        new_sex[flip] = np.where(new_sex[flip] == "M", "F", "M")
        new_group = group[src].copy()
        new_icd = icd[src].copy()
        new_delay = np.minimum(rng.negative_binomial(size, size / (size + mean[src])), 21)
        extra = {
            "onset": new_on, "group": new_group, "fac": new_fac, "age": np.clip(new_age, 0, 95),
            "sex": new_sex, "icd": new_icd, "lab": lab[src], "adm": admitted[src], "outcome": outcome[src],
            "report": np.minimum(new_on + new_delay, len(cal["off"]) - 1), "pid": pid[src], "band": band[src],
        }
        valid = extra["onset"] <= task["last_day"]
        new_ids = seq[-1] + 1 + np.arange(k)
        for i, (s_idx, nid) in enumerate(zip(src, new_ids)):
            if valid[i]:
                label = "duplicate_visit" if i < dup_src.size else "distinct_episode"
                truth_pairs.append((f"{prefix}{s_idx:07d}", f"{prefix}{nid:07d}", label))
        onset = np.concatenate([onset, extra["onset"][valid]])
        group = np.concatenate([group, extra["group"][valid]])
        fidx = np.concatenate([fidx, extra["fac"][valid]])
        age = np.concatenate([age, extra["age"][valid]])
        sex = np.concatenate([sex, extra["sex"][valid]])
        icd = np.concatenate([icd, extra["icd"][valid]])
        lab = np.concatenate([lab, extra["lab"][valid]])
        admitted = np.concatenate([admitted, extra["adm"][valid]])
        outcome = np.concatenate([outcome, extra["outcome"][valid]])
        report = np.concatenate([report, extra["report"][valid]])
        pid = np.concatenate([pid, extra["pid"][valid]])
        seq = np.concatenate([seq, new_ids[valid]])
    # --- practice change (P4 artefact): a facility that only starts reporting part-way through; applied after
    # all records (including duplicate visits) exist, so nothing is reported from it before its start date
    keep = onset >= fac["reporting_start"][fidx]
    onset, group, fidx, age, sex, icd = onset[keep], group[keep], fidx[keep], age[keep], sex[keep], icd[keep]
    lab, admitted, outcome, report, pid, seq = lab[keep], admitted[keep], outcome[keep], report[keep], pid[keep], seq[keep]
    n = onset.size

    # --- reporting artefact: facility silent for 21 days, then back-fills every pending report on one day
    # (applied after all records - including duplicate visits - exist, so the facility is truly silent)
    bf = task.get("backfill")
    if bf is not None:
        m = (fidx == bf["fac_local"]) & (report >= bf["silent_from"]) & (report < bf["silent_from"] + bf["days"])
        report[m] = bf["silent_from"] + bf["days"]

    # facility x day aggregates (valid records only) feed the stock / bed-occupancy simulation
    key = (fidx.astype(np.int64) * task["n_days_total"] + onset) * 10 + group.astype(np.int64) * 2 + admitted
    agg_keys, agg_counts = np.unique(key, return_counts=True)

    enc_id = np.char.add(prefix, np.char.zfill(seq.astype(str), 7))
    cols = {
        "encounter_id": enc_id.astype(object), "patient_id": pid.astype(object),
        "facility_id": fac["id"][fidx].astype(object),
        "onset_date": _date_str(base, onset).astype(object), "report_date": _date_str(base, report).astype(object),
        "age_years": age.astype(str).astype(object), "sex": sex.astype(object), "icd10_code": icd,
        "lab_confirmed": np.where(lab, "1", "0").astype(object), "admitted": np.where(admitted, "1", "0").astype(object),
        "outcome": outcome.astype(object),
    }
    route = report.copy()  # day used to route the row to history or to a drop (never the corrupted string)

    # --- about 1% invalid values + 0.5% orphan facility keys (quality defects)
    perm = rng.permutation(n)
    pos = 0
    def take(frac):
        nonlocal pos
        k = int(round(frac * n))
        idx = perm[pos: pos + k]
        pos += k
        return idx
    i = take(INVALID_MIX["age"]); cols["age_years"][i] = rng.choice(["212", "-3", "150", "999", "abc"], i.size)
    i = take(INVALID_MIX["sex"]); cols["sex"][i] = rng.choice(["X", "U", "", "male"], i.size)
    i = take(INVALID_MIX["icd"]); cols["icd10_code"][i] = rng.choice(["A9O", "XYZ", "", "Q999", "123"], i.size)
    i = take(INVALID_MIX["onset_after_report"])
    cols["onset_date"][i] = _date_str(base, report[i] + rng.integers(1, 10, i.size))
    i = take(INVALID_MIX["bad_date"]); cols["report_date"][i] = rng.choice(["2023-13-45", "", "31/12/2023", "N/A"], i.size)
    i = take(INVALID_MIX["very_late"]); cols["onset_date"][i] = _date_str(base, onset[i] - 365)
    i = take(INVALID_MIX["bool"]); cols["lab_confirmed"][i] = rng.choice(["maybe", "2", "?"], i.size)
    i = take(INVALID_MIX["outcome"]); cols["outcome"][i] = rng.choice(["alive?", "discharged!!", "x"], i.size)
    i = take(INVALID_MIX["patient_missing"]); cols["patient_id"][i] = ""
    i = take(INVALID_MIX["enc_missing"]); cols["encounter_id"][i] = ""
    i = take(ORPHAN_FRAC); cols["facility_id"][i] = np.char.add("FAC9", rng.integers(1000, 9999, i.size).astype(str))

    # --- about 2% resubmitted records: same encounter_id, sent again 0-3 days later
    r = rng.choice(n, size=int(RESUBMIT_FRAC * n), replace=False)
    for c in cols:
        cols[c] = np.concatenate([cols[c], cols[c][r]])
    re_day = np.minimum(route[r] + rng.integers(0, 4, r.size), len(cal["off"]) - 1)
    if bf is not None:  # a silent facility does not resubmit either
        silent = (fidx[r] == bf["fac_local"]) & (re_day >= bf["silent_from"]) & (re_day < bf["silent_from"] + bf["days"])
        re_day[silent] = bf["silent_from"] + bf["days"]
    cols["report_date"][n:] = np.where(np.isin(cols["report_date"][r], ["", "2023-13-45", "31/12/2023", "N/A"]),
                                       cols["report_date"][r], _date_str(base, re_day))
    route = np.concatenate([route, re_day])

    # --- split into historical landing file and incremental drops
    hist = route <= task["hist_end"]
    drop = (route > task["hist_end"]) & (route <= task["last_day"])
    out = Path(task["landing_dir"]) / f"encounters_{task['pcode']}_{task['year']}.csv"
    if hist.any():
        _write_csv({c: v[hist] for c, v in cols.items()}, ENCOUNTER_COLUMNS, out)
    drop_df = pd.DataFrame({c: v[drop] for c, v in cols.items()})
    drop_df["_route"] = route[drop]
    return {
        "rows": int(hist.sum()), "drop_rows": int(drop.sum()), "drop": drop_df,
        "agg_keys": agg_keys, "agg_counts": agg_counts, "fac_global": fac["global_idx"],
        "pairs": truth_pairs, "resubmitted": int(r.size), "seconds": time.time() - t_start,
        "pcode": task["pcode"], "year": task["year"],
    }


# ------------------------------------------------------------------------------------------ stock (S6)
def simulate_stock(reg: pd.DataFrame, cases: np.ndarray, admits: np.ndarray, rng: np.random.Generator) -> dict[str, np.ndarray]:
    """Daily stock and bed occupancy per facility driven by that facility's own case load."""
    F, T, _ = cases.shape
    beds = reg["beds"].to_numpy(dtype=float)
    cons = {
        "iv_saline": 6 * admits[:, :, 0] + 8 * admits[:, :, 1] + 0.3 * cases[:, :, 1] + admits[:, :, 4] * 0.5,
        "ns1_kits": 1.3 * cases[:, :, 0],
        "ors": 10 * cases[:, :, 1],
        "antimalarials": 1.0 * cases[:, :, 3],
    }
    out: dict[str, np.ndarray] = {}
    phase = rng.integers(0, 28, F)
    for k, c in cons.items():
        c = c * rng.lognormal(0, 0.15, c.shape)
        avg = np.maximum(c.mean(axis=1), 0.3)
        target = avg * 30 * rng.uniform(0.6, 1.2, F) + 5                  # order-up-to ~30 days of average use
        level = target * rng.uniform(0.3, 1.0, F)
        hist = np.empty((F, T))
        for t in range(T):
            level = np.maximum(level - c[:, t], 0)
            resupply = ((t + phase) % 28 == 0) & (rng.random(F) > 0.15)      # 15% of monthly deliveries are missed
            level = np.where(resupply, np.maximum(level, target), level)
            hist[:, t] = level
        out[k] = np.round(hist)
    occ_base = beds * rng.uniform(0.45, 0.8, F)
    los_kernel = np.ones(4)
    adm_total = admits.sum(axis=2)
    in_stay = np.stack([np.convolve(a, los_kernel)[:T] for a in adm_total])
    out["occupied_beds"] = np.round(np.clip(occ_base[:, None] + in_stay + rng.normal(0, 2, (F, T)), 0, beds[:, None] * 1.3))
    return out


def write_stock(reg: pd.DataFrame, stock: dict[str, np.ndarray], start: dt.date, hist_end: int, last_day: int,
                landing: Path, drops: Path, rng: np.random.Generator) -> int:
    F = len(reg)
    fid = reg["facility_id"].to_numpy()
    n = 0
    for t in range(last_day + 1):
        day = (start + dt.timedelta(days=t)).isoformat()
        df = pd.DataFrame({"snapshot_date": day, "facility_id": fid,
                           **{k: stock[k][:, t].astype(int) for k in ["iv_saline", "ns1_kits", "ors", "antimalarials", "occupied_beds"]}})
        df = df[rng.random(F) > 0.01]                                   # ~1% facility-days not reported
        bad = rng.random(len(df)) < 0.003                               # quality defects
        df.loc[bad, "iv_saline"] = -df.loc[bad, "iv_saline"] - 1
        dup = df.sample(frac=0.001, random_state=int(rng.integers(1 << 31)))
        df = pd.concat([df, dup], ignore_index=True)
        df = df[STOCK_COLUMNS]
        target = landing / "stock" if t <= hist_end else drops / day
        target.mkdir(parents=True, exist_ok=True)
        df.to_json(target / f"stock_{day}.jsonl", orient="records", lines=True)
        n += len(df)
    return n


# ------------------------------------------------------------------------------------------ main
def main(argv: list[str] | None = None) -> None:
    cfg = load_config()
    g = cfg["generator"]
    ap = argparse.ArgumentParser(description="EpiScope synthetic generator")
    ap.add_argument("--sf", type=float, default=float(g["scale_factor"]))
    ap.add_argument("--seed", type=int, default=int(g["seed"]))
    ap.add_argument("--workers", type=int, default=int(g["workers"]))
    args = ap.parse_args(argv)
    t_all = time.time()

    start = dt.date.fromisoformat(g["start_date"])
    hist_end_date = dt.date.fromisoformat(g["history_end"])
    hist_end = (hist_end_date - start).days
    last_day = hist_end + int(g["drop_days"])
    n_days = last_day + 1
    n_cal = n_days + 30

    landing = Path(path("landing"))
    drops = Path(path("drops"))
    truth_dir = Path(path("ground_truth"))
    for p in (landing / "encounters", landing / "facilities", landing / "stock", drops, truth_dir):
        if p.exists():
            shutil.rmtree(p)
        p.mkdir(parents=True, exist_ok=True)

    dist = districts()
    pop = load_population()
    tot = pop.groupby("adm2_pcode")["population"].sum()
    dist["population"] = dist["adm2_pcode"].map(tot).astype(float)
    dist["weight_pop"] = dist["population"] / dist["population"].sum()
    upz = pd.read_csv(Path(path("reference")) / "upazilas.csv")

    rng_ref = np.random.default_rng([args.seed, 1])
    reg = build_registry(dist, upz, rng_ref)
    assert len(reg) >= int(g["n_facilities_min"]), "registry must hold at least 600 facilities"
    weights = facility_weights(reg)
    reg_start = np.zeros(len(reg), dtype=int)

    # P4 artefacts: (a) facility onboarding = reporting-practice change; (b) 21-day silence then back-fill
    onboard_district = "BD2019"
    cand = reg.index[(reg.adm2_pcode == onboard_district) & (reg.facility_type == "CLINIC")]
    onboard_idx = int(cand[0])
    reg.loc[onboard_idx, ["beds", "facility_type", "facility_name"]] = [900, "CLINIC", "Cumilla Private Hospital Network (synthetic)"]
    weights = facility_weights(reg)
    onboard_day = hist_end - 300
    reg_start[onboard_idx] = onboard_day
    bf_idx = int(reg.index[(reg.adm2_pcode == "BD4047") & (reg.facility_type == "MCH")][0])
    bf_day = hist_end - 400
    artefacts = pd.DataFrame([
        {"artefact_id": "ART-BACKFILL", "kind": "silence_then_backfill", "facility_id": reg.facility_id[bf_idx],
         "adm2_pcode": "BD4047", "start_date": (start + dt.timedelta(days=bf_day)).isoformat(),
         "end_date": (start + dt.timedelta(days=bf_day + 21)).isoformat(),
         "note": "facility silent for 21 days, then back-fills every pending report on one day"},
        {"artefact_id": "ART-ONBOARD", "kind": "reporting_practice_change", "facility_id": reg.facility_id[onboard_idx],
         "adm2_pcode": onboard_district, "start_date": (start + dt.timedelta(days=onboard_day)).isoformat(),
         "end_date": (start + dt.timedelta(days=onboard_day + 42)).isoformat(),
         "note": "large private network starts reporting: step increase in all counts, not an outbreak"},
    ])

    weather = load_weather_daily()
    rng_epi = np.random.default_rng([args.seed, 2])
    lam = epi_model.intensities(dist, weather, start, n_days, args.sf, rng_epi)
    obs = epi_model.build_outbreaks(dist, n_days, hist_end + 1, np.random.default_rng([args.seed, 3]))
    mult = epi_model.outbreak_multiplier(obs, len(dist), n_days)
    cal = _calendar(start, n_cal)

    # age/sex structure per district from S2
    pv = pop.pivot_table(index="adm2_pcode", columns=["age_band", "sex"], values="population", aggfunc="sum")

    tasks = []
    years = sorted({(start + dt.timedelta(days=t)).year for t in range(n_days)})
    for d_idx, pc in enumerate(dist["adm2_pcode"]):
        f_mask = (reg.adm2_pcode == pc).to_numpy()
        f_rows = np.nonzero(f_mask)[0]
        band_tot = np.array([pv.loc[pc, (b, "M")] + pv.loc[pc, (b, "F")] for b in AGE_BANDS], dtype=float)
        male = np.array([pv.loc[pc, (b, "M")] for b in AGE_BANDS], dtype=float) / band_tot
        fac = {
            "id": reg.facility_id.to_numpy()[f_rows], "type": reg.facility_type.to_numpy()[f_rows],
            "reporting_start": reg_start[f_rows], "global_idx": f_rows,
            **{f"w_{grp}": weights[grp][f_rows] for grp in GROUPS},
        }
        for y in years:
            t0 = max(0, (dt.date(y, 1, 1) - start).days)
            t1 = min(n_days, (dt.date(y + 1, 1, 1) - start).days)
            if t0 >= t1:
                continue
            task = {
                "seed": args.seed, "d_idx": d_idx, "pcode": pc, "year": y, "t0": t0, "t1": t1,
                "lam": {grp: lam[grp][d_idx, t0:t1] for grp in GROUPS},
                "mult": {grp: mult[grp][d_idx, t0:t1] for grp in GROUPS},
                "facilities": fac, "calendar": cal, "band_share": band_tot / band_tot.sum(), "male_share": male,
                "start": start.isoformat(), "hist_end": hist_end, "last_day": last_day, "n_days_total": n_days + 400,
                "landing_dir": str(landing / "encounters"),
            }
            if pc == "BD4047":
                task["backfill"] = {"fac_local": int(np.nonzero(f_rows == bf_idx)[0][0]), "silent_from": bf_day, "days": 21}
            task["_cost"] = float(sum(lam[grp][d_idx, t0:t1].sum() for grp in GROUPS))
            tasks.append(task)
    tasks.sort(key=lambda t: -t["_cost"])

    F = len(reg)
    cases = np.zeros((F, n_days, 5), dtype=np.float32)
    admits = np.zeros((F, n_days, 5), dtype=np.float32)
    drop_frames, pairs = [], []
    total_rows = total_drop = resub = 0
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        for res in ex.map(gen_chunk, tasks, chunksize=1):
            if not res.get("rows") and not res.get("drop_rows"):
                continue
            total_rows += res["rows"]
            total_drop += res["drop_rows"]
            resub += res["resubmitted"]
            if res["drop_rows"]:
                drop_frames.append(res["drop"])
            pairs.extend(res["pairs"])
            k = res["agg_keys"]
            adm = k % 2
            grp = (k // 2) % 5
            day = (k // 10) % (n_days + 400)
            floc = k // 10 // (n_days + 400)
            ok = day < n_days
            gidx = res["fac_global"][floc[ok]]
            np.add.at(cases, (gidx, day[ok], grp[ok]), res["agg_counts"][ok])
            np.add.at(admits, (gidx, day[ok], grp[ok]), res["agg_counts"][ok] * adm[ok])

    # daily incremental drops (same schema as the historical files)
    if drop_frames:
        dd = pd.concat(drop_frames, ignore_index=True)
        for route, part in dd.groupby("_route"):
            day = (start + dt.timedelta(days=int(route))).isoformat()
            (drops / day).mkdir(parents=True, exist_ok=True)
            part[ENCOUNTER_COLUMNS].sort_values("encounter_id").to_csv(drops / day / f"encounters_{day}.csv", index=False)

    # S5 registry (with two deliberate quality defects) - public reference file in the landing zone
    reg_out = reg.copy()
    bad = reg_out.iloc[[5]].copy()
    bad["latitude"] = 0.0
    bad["facility_id"] = "FAC99999"
    reg_out = pd.concat([reg_out, reg_out.iloc[[10]], bad], ignore_index=True)  # duplicate row + bad coordinates
    reg_out[FACILITY_COLUMNS].to_csv(landing / "facilities" / "facility_registry.csv", index=False)

    # S6 stock / occupancy
    stock = simulate_stock(reg, cases, admits, np.random.default_rng([args.seed, 4]))
    n_stock = write_stock(reg, stock, start, hist_end, last_day, landing, drops, np.random.default_rng([args.seed, 5]))

    # ground truth (never read by detection)
    t = np.arange(n_days)
    gt = []
    for o in obs:
        shape = mult[o.group][o.district_idx]  # combined multiplier, only used for excess magnitude
        win = (t >= o.start) & (t <= o.end)
        excess = float((lam[o.group][o.district_idx][win] * (shape[win] - 1)).sum())
        gt.append({
            "outbreak_id": o.outbreak_id, "adm2_pcode": dist.adm2_pcode[o.district_idx], "disease_group": o.group,
            "start_date": (start + dt.timedelta(days=o.start)).isoformat(),
            "peak_date": (start + dt.timedelta(days=o.peak)).isoformat(),
            "end_date": (start + dt.timedelta(days=min(o.end, n_days - 1))).isoformat(),
            "peak_multiplier": round(o.peak_mult, 3), "kind": o.kind, "expected_excess_cases": round(excess, 1),
        })
    pd.DataFrame(gt).to_csv(truth_dir / "ground_truth_outbreaks.csv", index=False)
    artefacts.to_csv(truth_dir / "reporting_artefacts.csv", index=False)
    pd.DataFrame(pairs, columns=["encounter_id_a", "encounter_id_b", "label"]).to_csv(truth_dir / "patient_pairs.csv", index=False)
    manifest = {
        "scale_factor": args.sf, "seed": args.seed, "start_date": start.isoformat(),
        "history_end": hist_end_date.isoformat(), "drop_days": int(g["drop_days"]),
        "encounter_rows_history": total_rows, "encounter_rows_drops": total_drop, "resubmitted_rows": resub,
        "facilities": int(len(reg)), "stock_rows": n_stock, "outbreaks": len({o.outbreak_id.split('-')[0] if o.kind == 'novel_respiratory' else o.outbreak_id for o in obs}),
        "outbreak_rows": len(obs), "seconds": round(time.time() - t_all, 1),
        "synthetic": True, "label": "SYNTHETIC DATA - EpiScope teaching case (CSE 4345)",
    }
    (truth_dir / "generator_manifest.json").write_text(json.dumps(manifest, indent=2))
    (landing / "encounters" / "_SYNTHETIC_DATA_README.txt").write_text(manifest["label"] + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    os.environ.setdefault("OMP_NUM_THREADS", "1")
    main()
