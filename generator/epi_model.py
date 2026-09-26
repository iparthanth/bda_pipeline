"""Epidemiological intensity model for the synthetic generator (Section 5.2 "epidemiological realism").

Produces the expected daily number of encounters lambda[g][d, t] per disease group g, district d and
onset day t, driven by the S3 weather (lagged rainfall/temperature), seasonality, inter-annual
variation and injected outbreaks. Noise (negative binomial) is added later, per chunk, in generate.py.

All calendar anchors are *relative* to the configured start/end dates, so the generator works for any
period (no hard-coded dates).
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np
import pandas as pd

GROUPS = ["dengue", "diarrhoea", "ili", "malaria", "background"]
TARGET_GROUPS = ["dengue", "diarrhoea", "ili", "malaria"]

# National expected encounters per day at SF = 1 (calibrated so SF=1 gives >= 25M records in 5 years).
TARGET_DAILY = {"dengue": 690.0, "diarrhoea": 2470.0, "ili": 2470.0, "malaria": 42.0, "background": 9150.0}
# Negative-binomial size k (var = mu + mu^2/k) of daily district counts: clearly over-dispersed but not
# more than aggregated surveillance counts typically are.
NB_SIZE = {"dengue": 8.0, "diarrhoea": 12.0, "ili": 10.0, "malaria": 3.0, "background": 40.0}
DENGUE_YEAR = [0.55, 1.0, 1.5, 1.0, 0.85]  # relative intensity by year index (cf. real 2019-2025 pattern)

# district-level amplitude (pcode -> relative risk); unspecified districts get a random lognormal draw
DENGUE_AMP = {"BD3026": 5.0, "BD2015": 2.5, "BD2022": 2.5, "BD3033": 2.0, "BD3067": 2.0, "BD1006": 1.5}
MALARIA_SHARE = {"BD2003": 0.40, "BD2084": 0.24, "BD2046": 0.15, "BD2022": 0.08, "BD4589": 0.015,
                 "BD4572": 0.015, "BD4561": 0.01, "BD6090": 0.015, "BD6058": 0.01, "BD6036": 0.01,
                 "BD6091": 0.01, "BD5549": 0.01, "BD2015": 0.01}
HILL = {"BD2003", "BD2084", "BD2046"}


@dataclass
class Outbreak:
    outbreak_id: str
    district_idx: int
    group: str
    start: int          # day index (onset scale)
    peak: int
    end: int
    peak_mult: float
    kind: str           # sharp | slow | seasonal_epidemic | flood | novel_respiratory | drop_window


def _rolling_mean(arr: np.ndarray, t_idx: np.ndarray, lag_lo: int, lag_hi: int) -> np.ndarray:
    """Mean of arr[:, t-lag_hi .. t-lag_lo] for every t (arr is [D, Tw], t_idx are positions in arr)."""
    cs = np.concatenate([np.zeros((arr.shape[0], 1)), np.cumsum(arr, axis=1)], axis=1)
    hi = t_idx - lag_lo + 1
    lo = t_idx - lag_hi
    return (cs[:, hi] - cs[:, lo]) / (lag_hi - lag_lo + 1)


def weather_matrix(weather: pd.DataFrame, pcodes: list[str], wdates: pd.DatetimeIndex, var: str) -> np.ndarray:
    """[D, len(wdates)] matrix; gaps (e.g. days not yet in the archive) filled with the district-day-of-year mean."""
    w = weather.copy()
    w["date"] = pd.to_datetime(w["date"])
    piv = w.pivot_table(index="adm2_pcode", columns="date", values=var).reindex(index=pcodes, columns=wdates)
    if piv.isna().to_numpy().any():
        doy_mean = w.assign(doy=w["date"].dt.dayofyear).groupby(["adm2_pcode", "doy"])[var].mean()
        for pc in pcodes:
            row = piv.loc[pc]
            miss = row.isna()
            if miss.any():
                fill = [doy_mean.get((pc, d.dayofyear), np.nan) for d in row.index[miss]]
                piv.loc[pc, miss] = fill
        piv = piv.T.ffill().bfill().T
    return piv.to_numpy(dtype=float)


def _gamma_shape(t: np.ndarray, start: int, peak: int, end: int) -> np.ndarray:
    """Smooth rise/fall between start and end with maximum 1 at peak."""
    out = np.zeros_like(t, dtype=float)
    rise = (t >= start) & (t <= peak)
    fall = (t > peak) & (t <= end)
    out[rise] = np.sin(0.5 * np.pi * (t[rise] - start) / max(peak - start, 1)) ** 2
    out[fall] = np.cos(0.5 * np.pi * (t[fall] - peak) / max(end - peak, 1)) ** 2
    return out


def _slow_shape(t: np.ndarray, start: int, peak: int, end: int) -> np.ndarray:
    out = np.zeros_like(t, dtype=float)
    rise = (t >= start) & (t <= peak)
    x = (t[rise] - start) / max(peak - start, 1)
    out[rise] = 1.0 / (1.0 + np.exp(-10 * (x - 0.65)))
    fall = (t > peak) & (t <= end)
    out[fall] = np.cos(0.5 * np.pi * (t[fall] - peak) / max(end - peak, 1)) ** 2
    return out


def seir_curve(n_days: int, beta: float = 0.42, sigma: float = 1 / 3, gamma: float = 1 / 5, i0: float = 2e-6) -> np.ndarray:
    """Daily new infections (fraction of population) from a deterministic SEIR model, normalised to max 1."""
    s, e, i = 1.0 - i0, 0.0, i0
    inc = np.zeros(n_days)
    for k in range(n_days):
        new_e = beta * s * i
        new_i = sigma * e
        new_r = gamma * i
        s, e, i = s - new_e, e + new_e - new_i, i + new_i - new_r
        inc[k] = new_i
    return inc / inc.max()


def build_outbreaks(dist: pd.DataFrame, n_days: int, hist_days: int, rng: np.random.Generator) -> list[Outbreak]:
    """>= 30 injected outbreaks of different size, duration and location (plus the novel respiratory surge)."""
    pcodes = dist["adm2_pcode"].tolist()
    idx = {pc: i for i, pc in enumerate(pcodes)}
    obs: list[Outbreak] = []
    lo, hi = 365, hist_days - 60  # after the first year (baseline burn-in) and before the history end

    def add(oid, d, g, start, rise, dur, mult, kind):
        obs.append(Outbreak(oid, d, g, int(start), int(start + rise), int(start + dur), float(mult), kind))

    # 1) 22 sharp local outbreaks of mixed size
    groups = ["dengue"] * 7 + ["diarrhoea"] * 8 + ["ili"] * 3 + ["malaria"] * 4
    rng.shuffle(groups)
    weights = dist["weight_pop"].to_numpy()
    for k, g in enumerate(groups):
        if g == "malaria":
            d = idx[rng.choice(sorted(HILL | {"BD2022"}))]
        else:
            d = int(rng.choice(len(pcodes), p=weights / weights.sum()))
        dur = int(rng.integers(14, 57))
        size = rng.choice(["small", "medium", "large"], p=[0.3, 0.4, 0.3])
        mult = {"small": rng.uniform(1.6, 2.2), "medium": rng.uniform(2.2, 3.5), "large": rng.uniform(3.5, 6.0)}[size]
        add(f"OB{k + 1:02d}", d, g, rng.integers(lo, hi), max(4, int(dur * rng.uniform(0.3, 0.5))), dur, mult, "sharp")

    # 2) three slow-growing outbreaks (logistic growth over 8-12 weeks)
    for k, (pc, g) in enumerate([("BD4041", "dengue"), ("BD5010", "diarrhoea"), ("BD2084", "malaria")]):
        dur = int(rng.integers(70, 100))
        add(f"SL{k + 1:02d}", idx[pc], g, rng.integers(lo, hi - 40), int(dur * 0.75), dur, rng.uniform(2.3, 3.2), "slow")

    # 3) big dengue season in year index 2 in five cities (cf. 2023)
    year2 = 2 * 365
    for k, pc in enumerate(["BD3026", "BD2015", "BD2022", "BD3033", "BD1006"]):
        s = year2 + 180 + int(rng.integers(-10, 15))
        add(f"DS{k + 1:02d}", idx[pc], "dengue", s, 55, 120, rng.uniform(1.8, 2.6), "seasonal_epidemic")

    # 4) post-flood cholera/diarrhoea outbreak in Sylhet division, June of year index 1 (cf. 2022 floods)
    for k, pc in enumerate(["BD6091", "BD6090"]):
        s = 365 + 160 + k * 3
        add(f"FL{k + 1:02d}", idx[pc], "diarrhoea", s, 8, 30, rng.uniform(3.0, 4.0), "flood")

    # 5) two outbreaks that begin at the end of the history and continue in the incremental drops
    for k, (pc, g) in enumerate([("BD4561", "diarrhoea"), ("BD5081", "ili")]):
        s = hist_days - 6 + 7 * k
        add(f"DR{k + 1:02d}", idx[pc], g, s, 10, 28, rng.uniform(3.0, 4.5), "drop_window")

    # 6) nationwide novel-respiratory surge, year index 3: starts in Dhaka, spreads with distance
    start0 = 3 * 365 + 60
    lat = np.radians(dist["center_lat"].to_numpy())
    lon = np.radians(dist["center_lon"].to_numpy())
    dh = idx["BD3026"]
    dkm = 6371 * np.arccos(np.clip(np.sin(lat) * np.sin(lat[dh]) + np.cos(lat) * np.cos(lat[dh]) * np.cos(lon - lon[dh]), -1, 1))
    for d in range(len(pcodes)):
        s = start0 + int(dkm[d] / 12 + rng.integers(0, 6))
        add(f"NRS-{pcodes[d]}", d, "ili", s, 0, 0, rng.uniform(3.0, 5.0), "novel_respiratory")
    return [o for o in obs if o.start < n_days]


def outbreak_multiplier(obs: list[Outbreak], D: int, T: int) -> dict[str, np.ndarray]:
    t = np.arange(T)
    mult = {g: np.ones((D, T)) for g in GROUPS}
    seir = seir_curve(160)
    for o in obs:
        if o.kind == "novel_respiratory":
            shape = np.zeros(T)
            seg = t[(t >= o.start) & (t < o.start + len(seir))]
            shape[seg] = seir[seg - o.start]
            peak = o.start + int(np.argmax(seir))
            o.peak = peak
            o.end = o.start + int(np.max(np.nonzero(seir >= 0.05)))
        elif o.kind == "slow":
            shape = _slow_shape(t, o.start, o.peak, o.end)
        else:
            shape = _gamma_shape(t, o.start, o.peak, o.end)
        mult[o.group][o.district_idx] += (o.peak_mult - 1.0) * shape
    return mult


def intensities(dist: pd.DataFrame, weather: pd.DataFrame, start: dt.date, n_days: int, sf: float,
                rng: np.random.Generator) -> dict[str, np.ndarray]:
    """Expected daily encounters by group, each a [D, T] array (before outbreaks and noise)."""
    pcodes = dist["adm2_pcode"].tolist()
    D, T = len(pcodes), n_days
    pad = 70
    wdates = pd.date_range(pd.Timestamp(start) - pd.Timedelta(days=pad), periods=T + pad, freq="D")
    rain = weather_matrix(weather, pcodes, wdates, "precipitation_sum")
    temp = weather_matrix(weather, pcodes, wdates, "temperature_2m_mean")
    hum = weather_matrix(weather, pcodes, wdates, "relative_humidity_2m_mean")
    t_idx = np.arange(T) + pad
    days = pd.date_range(pd.Timestamp(start), periods=T, freq="D")
    doy = days.dayofyear.to_numpy()
    year_idx = np.array([(d.year - start.year) for d in days])
    pop = dist["population"].to_numpy(dtype=float)

    def share(w: np.ndarray) -> np.ndarray:
        return (w / w.sum())[:, None]

    lam: dict[str, np.ndarray] = {}

    # Dengue: climate-driven, lag 4-8 weeks on rainfall and 3-6 weeks on temperature (Section 5.2).
    r_lag = _rolling_mean(rain, t_idx, 28, 56)
    t_lag = _rolling_mean(temp, t_idx, 21, 42)
    suit = np.exp(-(((t_lag - 28.5) / 3.5) ** 2))
    rain_eff = ((r_lag + 1.0) / (rain.mean(axis=1, keepdims=True) + 1.0)) ** 0.9
    f = 0.06 + suit * rain_eff
    f /= f.mean(axis=1, keepdims=True)
    amp = np.array([DENGUE_AMP.get(pc, rng.lognormal(-0.7, 0.4)) for pc in pcodes])
    ymult = np.array([DENGUE_YEAR[y % len(DENGUE_YEAR)] for y in year_idx])
    lam["dengue"] = TARGET_DAILY["dengue"] * share(pop * amp) * f * ymult[None, :]

    # Diarrhoea: bimodal (pre-monsoon + post-monsoon), heat effect, spikes 3-14 days after extreme rain.
    s = 0.65 + 0.55 * np.exp(-0.5 * ((doy - 125) / 25) ** 2) + 0.35 * np.exp(-0.5 * ((doy - 275) / 25) ** 2)
    s = s / s.mean()
    r3 = _rolling_mean(rain, np.arange(T + pad), 0, 2)
    thr = np.nanpercentile(r3[:, pad:], 99, axis=1, keepdims=True)
    extreme = (r3 > thr).astype(float)
    kernel = np.exp(-0.5 * ((np.arange(15) - 6) / 2.5) ** 2)
    kernel[:3] = 0
    flood = np.stack([np.convolve(e, kernel)[: T + pad] for e in extreme])[:, pad:]
    heat = np.exp(0.03 * (_rolling_mean(temp, t_idx, 0, 6) - 26.0))
    div = dist["adm1_name"].to_numpy()
    amp = np.where(np.isin(div, ["Barishal", "Khulna"]), 1.2, 1.0) * rng.lognormal(0, 0.15, D)
    amp[pcodes.index("BD3026")] = 1.4
    yr = rng.lognormal(0, 0.07, year_idx.max() + 1)[year_idx]
    lam["diarrhoea"] = TARGET_DAILY["diarrhoea"] * share(pop * amp) * s[None, :] * (1 + 0.6 * np.clip(flood, 0, 1.5)) * heat * yr[None, :]

    # ILI: rainy-season peak + smaller winter peak, modulated by humidity.
    s = 0.8 + 0.6 * np.exp(-0.5 * ((doy - 200) / 30) ** 2) + 0.35 * np.exp(-0.5 * (((doy + 182) % 365 - 197) / 20) ** 2)
    s = s / s.mean()
    h = np.exp(0.01 * (_rolling_mean(hum, t_idx, 0, 6) - 75.0))
    yr = rng.lognormal(0, 0.07, year_idx.max() + 1)[year_idx]
    lam["ili"] = TARGET_DAILY["ili"] * share(pop * rng.lognormal(0, 0.1, D)) * s[None, :] * h * yr[None, :]

    # Malaria: concentrated in the Chittagong Hill Tracts; rainfall lag 2-6 weeks; May-September season.
    shares = np.array([MALARIA_SHARE.get(pc, 0.0) for pc in pcodes])
    rest = 1.0 - shares.sum()
    shares = np.where(shares > 0, shares, rest / max((shares == 0).sum(), 1))
    s = 0.25 + np.exp(-0.5 * ((doy - 190) / 40) ** 2)
    s = s / s.mean()
    rm = _rolling_mean(rain, t_idx, 14, 42)
    reff = ((rm + 1.0) / (rain.mean(axis=1, keepdims=True) + 1.0)) ** 0.5
    reff /= reff.mean(axis=1, keepdims=True)
    lam["malaria"] = TARGET_DAILY["malaria"] * shares[:, None] * s[None, :] * reff

    # Background routine encounters (other ICD-10 codes), mild seasonality, better access in cities.
    access = np.where(np.array(pcodes) == "BD3026", 1.5, 1.0) * np.where(np.isin(pcodes, list(HILL)), 0.7, 1.0)
    s = 1.0 + 0.05 * np.sin(2 * np.pi * (doy - 150) / 365)
    lam["background"] = TARGET_DAILY["background"] * share(pop * access) * s[None, :]

    for g in lam:
        lam[g] = lam[g] * sf
    return lam
