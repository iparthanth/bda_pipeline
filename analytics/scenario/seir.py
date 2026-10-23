"""Epidemic scenario module (AN-10): SEIR model calibrated on the novel-respiratory surge, with what-ifs.

1. The surge window is found from the data (no ground truth): national weekly ILI cases minus a seasonal
   baseline (median of the same ISO week in the other years); the longest run of weeks with excess > 30%
   of the baseline is the surge.
2. A deterministic SEIR model (latent 3 days, infectious 5 days) is fitted to the weekly *excess* cases:
   free parameters are the transmission rate beta, the initially infectious fraction I0 and the
   ascertainment (reporting) fraction rho; least squares on the square-root scale (variance stabilising).
3. What-if scenarios change beta from a given week: contact reduction of 20% / 40% starting at the week the
   surge would have been detected, 2 weeks earlier, or 2 weeks later. Outputs: weekly curves, peak size and
   timing, cases averted and extra bed-days (admission rate x length of stay from the data).
"""
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.optimize import least_squares

SIGMA, GAMMA = 1 / 3, 1 / 5


def simulate(beta_t: np.ndarray, i0: float, days: int) -> np.ndarray:
    s, e, i = 1.0 - i0, 0.0, i0
    inc = np.zeros(days)
    for t in range(days):
        new_e = beta_t[t] * s * i
        new_i = SIGMA * e
        s, e, i = s - new_e, e + new_e - new_i, i + new_i - GAMMA * i
        inc[t] = new_i
    return inc


def weekly(inc: np.ndarray) -> np.ndarray:
    n = len(inc) // 7
    return inc[: n * 7].reshape(n, 7).sum(axis=1)


def find_surge(nat: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    nat = nat.sort_values("week_start").copy()
    nat["iso_week"] = pd.to_datetime(nat.week_start).dt.isocalendar().week.clip(upper=52)
    nat["year"] = pd.to_datetime(nat.week_start).dt.year
    base = []
    for _, r in nat.iterrows():
        other = nat[(nat.iso_week == r.iso_week) & (nat.year != r.year)]
        base.append(other.cases.median() if len(other) else r.cases)
    nat["baseline"] = base
    nat["excess"] = (nat.cases - nat.baseline).clip(lower=0)
    flag = (nat.cases > 1.3 * nat.baseline).to_numpy()
    best, cur = (0, 0), None
    for i, f in enumerate(flag):
        if f:
            cur = (cur[0], i) if cur else (i, i)
            if cur[1] - cur[0] > best[1] - best[0]:
                best = cur
        else:
            cur = None
    lo, hi = max(0, best[0] - 2), min(len(nat) - 1, best[1] + 3)
    return nat, nat.iloc[lo: hi + 1]


def fit(window: pd.DataFrame, population: float) -> dict:
    y = window.excess.to_numpy(dtype=float)
    weeks = len(y)
    days = weeks * 7

    def model(theta):
        beta, log_i0, logit_rho = theta
        rho = 1 / (1 + np.exp(-logit_rho))
        inc = simulate(np.full(days, beta), np.exp(log_i0), days)
        return weekly(inc) * population * rho

    def resid(theta):
        return np.sqrt(np.maximum(model(theta), 0)) - np.sqrt(y)

    best = None
    for b0 in (0.3, 0.45, 0.6):
        for i0 in (-16.0, -13.0, -10.0):
            r = least_squares(resid, x0=[b0, i0, -3.0], bounds=([0.21, -25, -12], [2.0, -5, 5]))
            if best is None or r.cost < best.cost:
                best = r
    beta, log_i0, logit_rho = best.x
    rho = 1 / (1 + np.exp(-logit_rho))
    fitted = model(best.x)
    r2 = 1 - np.sum((y - fitted) ** 2) / max(np.sum((y - y.mean()) ** 2), 1e-9)
    return {"beta": float(beta), "R0": float(beta / GAMMA), "i0": float(np.exp(log_i0)), "rho": float(rho),
            "r2": float(r2), "weeks": weeks, "fitted": fitted.tolist(), "observed_excess": y.tolist(),
            "week_start": [str(pd.Timestamp(w).date()) for w in window.week_start]}


def scenarios(params: dict, population: float, admission_rate: float, los_days: float, detect_week: int) -> pd.DataFrame:
    weeks = params["weeks"] + 8
    days = weeks * 7
    rows = []
    specs = [("no intervention (fitted)", 0.0, None)]
    for red in (0.2, 0.4):
        for shift, label in ((0, "at detection"), (-2, "2 weeks earlier"), (2, "2 weeks later")):
            specs.append((f"{int(red * 100)}% contact reduction {label}", red, detect_week + shift))
    base_total = None
    for name, red, start_w in specs:
        beta_t = np.full(days, params["beta"])
        if start_w is not None:
            beta_t[max(0, start_w * 7):] *= (1 - red)
        cases = weekly(simulate(beta_t, params["i0"], days)) * population * params["rho"]
        total = float(cases.sum())
        base_total = total if base_total is None else base_total
        for w, c in enumerate(cases):
            rows.append({"scenario": name, "week": w, "reported_cases": float(c),
                         "bed_days": float(c * admission_rate * los_days)})
        rows.append({"scenario": name, "week": -1, "reported_cases": total,
                     "bed_days": float(total * admission_rate * los_days), "peak_week": int(np.argmax(cases)),
                     "peak_cases": float(cases.max()), "cases_averted": base_total - total})
    return pd.DataFrame(rows)
