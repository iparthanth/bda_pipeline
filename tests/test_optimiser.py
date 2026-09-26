"""AN-7 optimiser constraints: budget, equity floor, transport capacity, team integrality and the baseline."""
from __future__ import annotations

import numpy as np
import pytest

from analytics.optimise.allocate import Problem, baseline, solve


def make_problem(seed=0, D=12, R=4, n_teams=5, transport=1e9):
    rng = np.random.default_rng(seed)
    yhat = rng.gamma(2, 500, (D, R))
    yhat[0] *= 8                                      # one high-need district (efficiency pull)
    stock = yhat * rng.uniform(0, 0.8, (D, R))
    pop = rng.uniform(1, 5, D)
    return Problem(districts=[f"D{i}" for i in range(D)], divisions=[f"V{i % 3}" for i in range(D)], resources=[f"r{j}" for j in range(R)],
                   yhat=yhat, stock=stock, cost=np.array([100.0, 300.0, 5.0, 250.0][:R]), harm=np.array([0.167, 0.23, 0.03, 1.0][:R]),
                   weight_kg=np.array([1.1, 0.05, 0.03, 0.05][:R]),
                   pop_share=pop / pop.sum(), w=1 + rng.integers(0, 2, D).astype(float), tau=rng.integers(0, 3, D).astype(float),
                   n_teams=n_teams, team_cost=1.5e5, transport_kg=transport)


@pytest.mark.parametrize("level", [0.0, 0.2, 0.6, 1.0, 1.5])
def test_budget_respected(level):
    p = make_problem()
    b = level * p.full_gap_cost()
    r = solve(p, b, 0.0)
    assert r["status"] == "optimal"
    assert (r["x"] * p.cost).sum() <= b + 1e-3 * max(b, 1)
    assert (r["x"] >= -1e-6).all()


@pytest.mark.parametrize("alpha", [0.1, 0.25, 0.5, 0.9])
def test_equity_floor_holds(alpha):
    p = make_problem(seed=1)
    r = solve(p, 0.5 * p.full_gap_cost(), alpha)
    x = r["x"]
    for j in range(x.shape[1]):
        tot = x[:, j].sum()
        assert (x[:, j] >= alpha * p.pop_share * tot - 1e-4 * max(tot, 1)).all()


def test_equity_costs_efficiency_but_raises_minimum():
    p = make_problem(seed=2)
    b = 0.4 * p.full_gap_cost()
    r0, r9 = solve(p, b, 0.0), solve(p, b, 0.9)
    assert r9["weighted_unmet"] >= r0["weighted_unmet"] - 1e-6        # equity is never free
    assert r9["districts_with_nothing"] <= r0["districts_with_nothing"]


def test_transport_capacity():
    p = make_problem(seed=3, transport=2000.0)
    r = solve(p, 2 * p.full_gap_cost(), 0.0)
    for div in set(p.divisions):
        idx = [i for i, d in enumerate(p.divisions) if d == div]
        assert (r["x"][idx] * p.weight_kg).sum() <= 2000.0 + 1e-3


def test_teams_integer_and_capped():
    p = make_problem(seed=4, n_teams=3)
    r = solve(p, p.full_gap_cost(), 0.0)
    t = r["teams"]
    assert np.allclose(t, np.round(t)) and t.sum() <= 3 and (t >= 0).all()


def test_optimiser_beats_population_baseline():
    p = make_problem(seed=5)
    b = 0.5 * p.full_gap_cost()
    assert solve(p, b, 0.0)["weighted_unmet"] <= baseline(p, b)["weighted_unmet"] + 1e-6


def test_zero_budget_ships_nothing():
    p = make_problem(seed=6)
    r = solve(p, 0.0, 0.25)
    assert np.allclose(r["x"], 0, atol=1e-6)
    assert r["unmet_bdt"] == pytest.approx((p.gap * p.cost).sum(), rel=1e-6)


def test_full_budget_closes_every_gap():
    p = make_problem(seed=7)
    r = solve(p, 1.0 * p.full_gap_cost() * 1.001, 0.0)
    assert r["gap_covered_pct"] == pytest.approx(100.0, abs=0.1)


def test_first_half_penalty_spreads_supplies():
    """With the convex penalty no district with a gap is left with nothing when the budget allows half of every gap."""
    p = make_problem(seed=8)
    r = solve(p, 0.55 * p.full_gap_cost(), 0.0)
    assert r["districts_with_nothing"] == 0
