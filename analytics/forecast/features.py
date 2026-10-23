"""Feature construction and probabilistic scoring shared by the forecasting models (pure pandas/numpy).

Features for a direct h-week-ahead model at origin week t (all known at the end of week t):

  lag_0, lag_1       log(1 + cases) in weeks t and t-1 (nowcast-corrected at prediction time)
  lag_2_5            log(1 + mean cases in weeks t-2..t-5)
  last_year          log(1 + mean cases in weeks t+h-53..t+h-51)  (same season last year)
  s1, c1, s2, c2     annual harmonics of the *target* week
  rain_lag           log(1 + rainfall summed over weeks t+h-8..t+h-4)   (dengue: 4-8 week lag)
  temp_lag           mean temperature over weeks t+h-8..t+h-4
  rain_recent_max    log(1 + max daily rainfall in weeks t-1..t)       (post-flood diarrhoea)
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats

FEATURES = ["lag_0", "lag_1", "lag_2_5", "last_year", "s1", "c1", "s2", "c2", "rain_lag", "temp_lag", "rain_recent_max"]
QUANTILES = [0.05, 0.1, 0.25, 0.5, 0.75, 0.9, 0.95]


def _roll(a: np.ndarray, lo: int, hi: int, fn=np.nanmean) -> np.ndarray:
    """fn(a[i+lo .. i+hi]) for every i (NaN outside the series)."""
    n = len(a)
    out = np.full(n, np.nan)
    for i in range(n):
        s, e = i + lo, i + hi
        if s >= 0 and e < n:
            out[i] = fn(a[s: e + 1])
    return out


def design(y: np.ndarray, weeks: pd.Series, rain: np.ndarray, temp: np.ndarray, maxrain: np.ndarray, h: int) -> pd.DataFrame:
    """Feature matrix for every origin index t (row t predicts y[t + h])."""
    ly = np.log1p(np.maximum(y, 0))
    n = len(y)
    woy_target = (pd.to_datetime(weeks) + pd.Timedelta(weeks=h)).dt.isocalendar().week.to_numpy(dtype=float)
    ang = 2 * np.pi * woy_target / 52.18
    df = pd.DataFrame({
        "lag_0": ly,
        "lag_1": np.r_[np.nan, ly[:-1]],
        "lag_2_5": np.log1p(_roll(y, -5, -2)),
        "last_year": np.log1p(_roll(y, h - 53, h - 51)),
        "s1": np.sin(ang), "c1": np.cos(ang), "s2": np.sin(2 * ang), "c2": np.cos(2 * ang),
        "rain_lag": np.log1p(_roll(rain, h - 8, h - 4, np.nansum)),
        "temp_lag": _roll(temp, h - 8, h - 4),
        "rain_recent_max": np.log1p(_roll(maxrain, -1, 0, np.nanmax)),
    })
    target = np.full(n, np.nan)
    target[: n - h] = y[h:]
    df["target"] = target
    return df


def nb_size(y: np.ndarray, mu: np.ndarray) -> float:
    """Method-of-moments negative-binomial size k (var = mu + mu^2 / k)."""
    num = np.sum(mu ** 2)
    den = np.sum((y - mu) ** 2 - mu)
    if den <= 0:
        return 1000.0
    return float(np.clip(num / den, 0.3, 1000.0))


def nb_quantiles(mu: np.ndarray, k: float, qs=QUANTILES) -> np.ndarray:
    mu = np.maximum(mu, 1e-6)
    p = k / (k + mu)
    return np.column_stack([stats.nbinom.ppf(q, k, p) for q in qs])


def wis(y: np.ndarray, q: np.ndarray, qs=QUANTILES) -> np.ndarray:
    """Weighted interval score (Bracher et al. 2021) from symmetric quantiles + median."""
    qs = list(qs)
    med = q[:, qs.index(0.5)]
    total = 0.5 * np.abs(y - med)
    k = 0
    for lo_q in [x for x in qs if x < 0.5]:
        alpha = 2 * lo_q
        lo, hi = q[:, qs.index(lo_q)], q[:, qs.index(round(1 - lo_q, 10))]
        interval = (hi - lo) + (2 / alpha) * (lo - y) * (y < lo) + (2 / alpha) * (y - hi) * (y > hi)
        total = total + alpha / 2 * interval
        k += 1
    return total / (k + 0.5)
