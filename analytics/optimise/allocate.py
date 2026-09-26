"""Allocation optimiser (AN-7), efficiency-equity analysis (AN-8, conflict C3) and the what-if grid (VIS-5).

Formulation (Section 6.3 of the brief, extended with rapid-response teams):

    min  sum_{d,r} w_d h_r (u_{d,r} + beta q_{d,r}) + sum_d w_d H_team v_d + eps * sum_{d,r} c_r x_{d,r}
    s.t. u_{d,r} >= yhat_{d,r} - s_{d,r} - x_{d,r}                     (unmet demand)
         q_{d,r} >= u_{d,r} - gap_{d,r} / 2                            (unmet beyond half the gap)
         sum_{d,r} c_r x_{d,r} <= B                                    (budget)
         x_{d,r} >= alpha p_d sum_{d'} x_{d',r}                         (equity floor, 0 <= alpha < 1)
         sum_{d in div k} sum_r v_r x_{d,r} <= T_k                      (transport capacity per division)
         v_d >= tau_d - z_d,  sum_d z_d <= N_teams,  z_d integer        (rapid-response teams)
         x, u, v, z >= 0

* ``yhat`` = upper planning quantile (default 80%) of forecast demand over the next 4 weeks (AN-6);
  ``s`` = latest district stock from the S6 feed.
* Unmet demand is valued in patient-equivalents ``h_r`` per missing unit (config ``harm_per_unit``), so an ORS
  sachet and an ACT course are not treated as the same harm; ``w_d`` = 1 + alert_weight * [active alert] +
  growth_weight * [forecast growth > 20%].
* Convex (piecewise-linear) penalty: the part of a shortfall that *exceeds* half of the district's gap costs
  (1 + beta) times as much, i.e. filling the first half of every gap - the life-saving minimum - comes first. Without
  it the LP is degenerate (every unit has the same value per taka in every district) and splits ties arbitrarily.
* ``tau_d`` = number of diseases with an active alert in district d (demand for rapid-response teams).
* ``eps`` avoids spending budget that reduces no unmet demand.
* Solved as a MILP with SciPy's HiGHS interface (``scipy.optimize.milp``).

Baseline: the same budget split across resources in proportion to the national gap and across districts in
proportion to population.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import sparse
from scipy.optimize import Bounds, LinearConstraint, milp

from pipeline.common.config import load_config

STOCK_COL = {"iv_saline": "iv_saline", "ns1_kits": "ns1_kits", "ors": "ors", "antimalarial": "antimalarials"}


@dataclass
class Problem:
    districts: list[str]
    divisions: list[str]
    resources: list[str]
    yhat: np.ndarray            # [D, R]
    stock: np.ndarray           # [D, R]
    cost: np.ndarray            # [R]
    harm: np.ndarray            # [R] patient-equivalents per missing unit
    weight_kg: np.ndarray       # [R]
    pop_share: np.ndarray       # [D]
    w: np.ndarray               # [D]
    tau: np.ndarray             # [D] team demand
    n_teams: int
    team_cost: float
    transport_kg: float
    beta: float = 1.0
    team_harm: float = 20.0

    @property
    def gap(self) -> np.ndarray:
        return np.maximum(self.yhat - self.stock, 0)

    def full_gap_cost(self) -> float:
        return float((self.gap * self.cost[None, :]).sum())


def solve(p: Problem, budget: float, alpha: float) -> dict:
    D, R = p.yhat.shape
    nx = D * R
    # variable layout: x (D*R) | u (D*R) | q (D*R) | z (D) | v (D)
    n = 3 * nx + 2 * D
    ix = lambda d, r: d * R + r
    iu = lambda d, r: nx + d * R + r
    iq = lambda d, r: 2 * nx + d * R + r
    iz = lambda d: 3 * nx + d
    iv = lambda d: 3 * nx + D + d
    c = np.zeros(n)
    eps = 1e-6
    for d in range(D):
        for r in range(R):
            c[iu(d, r)] = p.w[d] * p.harm[r]
            c[iq(d, r)] = p.w[d] * p.harm[r] * p.beta
            c[ix(d, r)] = eps * p.cost[r]
        c[iv(d)] = p.w[d] * p.team_harm
    rows, cols, vals, lo, hi = [], [], [], [], []
    k = 0

    def add(entries, lb, ub):
        nonlocal k
        for j, v in entries:
            rows.append(k); cols.append(j); vals.append(v)
        lo.append(lb); hi.append(ub)
        k += 1

    need = p.yhat - p.stock
    gap = p.gap
    for d in range(D):
        for r in range(R):
            add([(iu(d, r), 1.0), (ix(d, r), 1.0)], need[d, r], np.inf)               # u + x >= yhat - s
            add([(iq(d, r), 1.0), (iu(d, r), -1.0)], -0.5 * gap[d, r], np.inf)        # q >= u - gap/2
    add([(ix(d, r), p.cost[r]) for d in range(D) for r in range(R)], -np.inf, budget)  # budget
    if alpha > 0:
        for r in range(R):
            for d in range(D):                                                          # x_dr - a p_d sum x_.r >= 0
                ent = [(ix(dd, r), -alpha * p.pop_share[d]) for dd in range(D)]
                ent = [(j, v + (1.0 if j == ix(d, r) else 0.0)) for j, v in ent]
                add(ent, 0.0, np.inf)
    for div in sorted(set(p.divisions)):
        ds = [d for d in range(D) if p.divisions[d] == div]
        add([(ix(d, r), p.weight_kg[r]) for d in ds for r in range(R)], -np.inf, p.transport_kg)
    for d in range(D):
        add([(iv(d), 1.0), (iz(d), 1.0)], p.tau[d], np.inf)                          # v + z >= tau
    add([(iz(d), 1.0) for d in range(D)], -np.inf, p.n_teams)
    A = sparse.csr_matrix((vals, (rows, cols)), shape=(k, n))
    integrality = np.zeros(n)
    integrality[3 * nx: 3 * nx + D] = 1
    res = milp(c, constraints=LinearConstraint(A, lo, hi), integrality=integrality,
               bounds=Bounds(np.zeros(n), np.full(n, np.inf)), options={"time_limit": 60, "mip_rel_gap": 1e-6})
    if not res.success:
        return {"status": res.message, "x": None}
    x = res.x[:nx].reshape(D, R)
    z = np.round(res.x[3 * nx: 3 * nx + D])
    return {"status": "optimal", "x": x, "teams": z, "objective": float(res.fun)} | evaluate(p, x, z)


def evaluate(p: Problem, x: np.ndarray, teams: np.ndarray | None = None) -> dict:
    """Plan quality. ``weighted_unmet`` (patient-equivalents, incl. the first-half penalty) is the optimiser's
    own criterion; ``unmet_bdt`` and ``gap_covered_pct`` value unmet units at purchase cost for a neutral view."""
    unmet = np.maximum(p.yhat - p.stock - x, 0)
    beyond_half = np.maximum(unmet - p.gap / 2, 0)
    harm_d = (unmet * p.harm + p.beta * beyond_half * p.harm).sum(axis=1)
    gap_cost = (p.gap * p.cost).sum(axis=1)
    unmet_cost = (unmet * p.cost).sum(axis=1)
    cov = np.where(gap_cost > 0, 1 - unmet_cost / np.maximum(gap_cost, 1e-9), 1.0)
    has_gap = gap_cost > 0
    sorted_cov = np.sort(cov[has_gap]) if has_gap.any() else np.array([1.0])
    n = len(sorted_cov)
    gini = float((2 * np.arange(1, n + 1) - n - 1) @ sorted_cov / (n * sorted_cov.sum())) if sorted_cov.sum() > 0 else 0.0
    team_unmet = float(np.maximum(p.tau - (teams if teams is not None else 0), 0).sum())
    return {
        "weighted_unmet": float((harm_d * p.w).sum() + (np.maximum(p.tau - (teams if teams is not None else 0), 0) * p.w).sum() * p.team_harm),
        "unmet_patient_eq": float((unmet * p.harm).sum()),
        "unmet_bdt": float(unmet_cost.sum()),
        "gap_covered_pct": float(100 * (1 - unmet_cost.sum() / max(gap_cost.sum(), 1e-9))),
        "min_district_coverage": float(cov[has_gap].min()) if has_gap.any() else 1.0,
        "districts_below_50pct": int(((cov < 0.5) & has_gap).sum()),
        "districts_with_nothing": int(((x.sum(axis=1) < 1e-6) & has_gap).sum()),
        "coverage_gini": gini,
        "spent_bdt": float((x * p.cost).sum()),
        "teams_unmet": team_unmet,
        "coverage_by_district": cov.tolist(),
    }


def baseline(p: Problem, budget: float) -> dict:
    gap_cost_r = (p.gap * p.cost).sum(axis=0)
    share_r = gap_cost_r / max(gap_cost_r.sum(), 1e-9)
    x = p.pop_share[:, None] * (budget * share_r / p.cost)[None, :]
    teams = np.floor(p.pop_share * p.n_teams)
    return {"status": "baseline", "x": x, "teams": teams} | evaluate(p, x, teams)


def build_problem(demand: pd.DataFrame, stock: pd.DataFrame, pop: pd.DataFrame, alerts: pd.DataFrame,
                  growth: pd.DataFrame) -> Problem:
    cfg = load_config()
    oc, q = cfg["optimise"], cfg["demand"]["planning_quantile"]
    qcol = f"q{int(round(q * 100))}"
    resources = list(oc["resources"])
    pop = pop.sort_values("adm2_pcode")
    districts = pop.adm2_pcode.tolist()
    yhat = demand.pivot(index="adm2_pcode", columns="resource", values=qcol).reindex(index=districts, columns=resources).fillna(0).to_numpy()
    st = stock.set_index("adm2_pcode").reindex(districts)
    stock_m = np.column_stack([st[STOCK_COL[r]].fillna(0).to_numpy() for r in resources])
    alert_d = set(alerts.adm2_pcode) if len(alerts) else set()
    tau = np.array([alerts[alerts.adm2_pcode == d].disease_group.nunique() if len(alerts) else 0 for d in districts], dtype=float)
    grow = set(growth[growth.growth > 0.2].adm2_pcode) if len(growth) else set()
    w = np.array([1.0 + oc["alert_weight"] * (d in alert_d) + oc["growth_weight"] * (d in grow) for d in districts])
    return Problem(
        districts=districts, divisions=pop.adm1_pcode.tolist(), resources=resources, yhat=yhat, stock=stock_m,
        cost=np.array([oc["resources"][r]["unit_cost"] for r in resources]),
        harm=np.array([oc["resources"][r]["harm_per_unit"] for r in resources]),
        beta=float(oc.get("first_half_extra_weight", 1.0)), team_harm=float(oc["teams"].get("harm_per_unit", 20.0)),
        weight_kg=np.array([oc["resources"][r]["unit_weight_kg"] for r in resources]),
        pop_share=(pop.population / pop.population.sum()).to_numpy(), w=w, tau=tau,
        n_teams=int(oc["teams"]["total_available"]), team_cost=float(oc["teams"]["unit_cost"]),
        transport_kg=float(oc["transport_capacity_kg_per_division"]))


def efficiency_equity(p: Problem) -> tuple[pd.DataFrame, dict]:
    oc = load_config()["optimise"]
    full = p.full_gap_cost()
    rows = []
    for bl in oc["budget_levels"]:
        for a in oc["equity_alphas"]:
            r = solve(p, bl * full, a)
            if r["x"] is None:
                rows.append({"budget_level": bl, "alpha": a, "status": r["status"]})
                continue
            rows.append({"budget_level": bl, "budget_bdt": bl * full, "alpha": a, "status": r["status"],
                         **{k: v for k, v in r.items() if k not in ("x", "teams", "coverage_by_district")}})
        b = baseline(p, bl * full)
        rows.append({"budget_level": bl, "budget_bdt": bl * full, "alpha": np.nan, "status": "population_baseline",
                     **{k: v for k, v in b.items() if k not in ("x", "teams", "coverage_by_district", "status")}})
    curve = pd.DataFrame(rows)
    # recommendation rule (C3): at the reference budget, the smallest alpha that leaves no district with a gap
    # and nothing shipped, provided it costs at most `max_efficiency_loss_pct` extra weighted unmet demand
    # compared with the purely efficient plan (alpha = 0); otherwise alpha = 0.
    ref = float(oc["budget_bdt"] or 0) / full if oc.get("budget_bdt") else 0.6
    ref = min(oc["budget_levels"], key=lambda b: abs(b - ref))
    at = curve[(curve.budget_level == ref) & (curve.status == "optimal")].sort_values("alpha")
    eff0 = at[at.alpha == 0].weighted_unmet.iloc[0]
    at = at.assign(loss_pct=100 * (at.weighted_unmet / max(eff0, 1e-9) - 1))
    ok = at[(at.districts_with_nothing == 0) & (at.loss_pct <= float(oc["max_efficiency_loss_pct"]) + 1e-9)]
    best = ok.iloc[0] if len(ok) else at.iloc[0]
    zero = at[at.alpha == 0].iloc[0]
    rec = {"budget_level": ref, "alpha": float(best.alpha),
           "rule": f"smallest alpha with no district left with nothing, efficiency loss <= {oc['max_efficiency_loss_pct']}%",
           "efficiency_loss_pct": float(best.loss_pct),
           "min_coverage_alpha0": float(zero.min_district_coverage), "min_coverage_recommended": float(best.min_district_coverage),
           "nothing_alpha0": int(zero.districts_with_nothing), "nothing_recommended": int(best.districts_with_nothing)}
    return curve, rec


def write_outputs(p: Problem, out_dir: Path) -> dict:
    oc = load_config()["optimise"]
    full = p.full_gap_cost()
    budget = float(oc["budget_bdt"]) if oc.get("budget_bdt") else 0.6 * full
    curve, rec = efficiency_equity(p)
    alpha = rec["alpha"]                      # the recommended plan uses the rule-based equity floor
    opt = solve(p, budget, alpha)
    base = baseline(p, budget)
    rows = []
    for d, pc in enumerate(p.districts):
        for r, res in enumerate(p.resources):
            rows.append({"adm2_pcode": pc, "resource": res, "demand_planning": p.yhat[d, r], "stock": p.stock[d, r],
                         "gap": p.gap[d, r], "recommended": opt["x"][d, r], "baseline_population": base["x"][d, r],
                         "weight": p.w[d]})
    alloc = pd.DataFrame(rows)
    teams = pd.DataFrame({"adm2_pcode": p.districts, "team_demand": p.tau, "teams_recommended": opt["teams"],
                          "teams_baseline": base["teams"]})
    out_dir.mkdir(parents=True, exist_ok=True)
    alloc.to_csv(out_dir / "allocation.csv", index=False)
    teams.to_csv(out_dir / "teams.csv", index=False)
    curve.to_csv(out_dir / "efficiency_equity.csv", index=False)
    summary = {"budget_bdt": budget, "full_gap_cost_bdt": full, "alpha": alpha, "recommended_policy": rec,
               "optimised": {k: v for k, v in opt.items() if k not in ("x", "teams", "coverage_by_district")},
               "population_baseline": {k: v for k, v in base.items() if k not in ("x", "teams", "coverage_by_district")}}
    (out_dir / "allocation_summary.json").write_text(json.dumps(summary, indent=2, default=float))
    return {"alloc": alloc, "teams": teams, "curve": curve, "summary": summary}


def scenario_grid(p_by_threshold: dict[float, Problem], out_dir: Path) -> pd.DataFrame:
    """VIS-5: allocations for every (detection threshold, budget level, equity alpha) combination."""
    oc = load_config()["optimise"]
    rows = []
    for thr, p in p_by_threshold.items():
        full = p.full_gap_cost()
        for bl in oc["budget_levels"]:
            for a in oc["equity_alphas"]:
                r = solve(p, bl * full, a)
                if r["x"] is None:
                    continue
                for d, pc in enumerate(p.districts):
                    rows.append({"threshold": thr, "budget_level": bl, "alpha": a, "adm2_pcode": pc,
                                 **{res: float(r["x"][d, j]) for j, res in enumerate(p.resources)},
                                 "teams": float(r["teams"][d]), "coverage": r["coverage_by_district"][d],
                                 "weight": float(p.w[d])})
    grid = pd.DataFrame(rows)
    grid.to_csv(out_dir / "scenario_grid.csv", index=False)
    return grid
