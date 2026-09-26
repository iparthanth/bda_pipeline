"""Small-cell suppression (PRV-2).

Rule: no count from 1 to 4 appears in any dashboard output or export, and published totals must not let a
suppressed cell be recovered by subtraction.

* **Primary suppression** - every count in [1, k-1] (k = 5) is replaced by ``None`` (shown as "<5").
* **Secondary (complementary) suppression** - counts are published in a hierarchy
  district -> division -> national for every (disease, week). Whenever a published parent has exactly one
  suppressed child, the smallest non-zero published sibling is suppressed too, and the check is repeated
  until no parent has a single suppressed child (a single hidden child = parent - visible siblings).
  No other totals are published (no totals across weeks or across diseases), which is what makes the
  hierarchy check sufficient; ``audit`` re-checks every exported hierarchy.

``COUNT_KEYS`` lists every JSON field that holds a person-derived count; tests scan all exports for them.
"""
from __future__ import annotations

from typing import Iterable

import numpy as np

K = 5
COUNT_KEYS = {
    "cases", "observed", "nowcast", "known", "mean", "median", "q05", "q10", "q25", "q50", "q75", "q80", "q90", "q95",
    "count7", "expected7", "forecast_next", "admitted", "deaths", "rows_in", "rows_out", "rows_quarantined",
    "duplicates_removed", "failed_rows", "reported_cases", "fitted", "observed_excess", "demand_mean", "demand_q10",
    "demand_q50", "demand_q80", "demand_q90", "series_values",
}


def primary(x: float | int | None) -> float | int | None:
    """Suppress a single count."""
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    v = int(round(x))
    return None if 1 <= v < K else v


def suppress_array(a: Iterable) -> list:
    return [primary(v) for v in a]


def hierarchical(child: np.ndarray, groups: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Suppress a 2-level hierarchy for one (disease, week) slice.

    child  : [C] integer counts of the children (districts)
    groups : [C] parent index of each child (division)
    Returns (child_mask, parent_values, parent_mask, national) where masks are True = suppressed.
    Parents and the national total are also primary-suppressed and then protected in the same way.
    """
    child = np.asarray(child, dtype=float)
    groups = np.asarray(groups)
    n_par = int(groups.max()) + 1
    parent = np.array([child[groups == p].sum() for p in range(n_par)])
    nat = child.sum()
    cm = (child >= 1) & (child < K)
    pm = (parent >= 1) & (parent < K)
    nm = bool(1 <= nat < K)
    changed = True
    while changed:
        changed = False
        for p in range(n_par):                       # children of each division
            idx = np.nonzero(groups == p)[0]
            if not pm[p] and cm[idx].sum() == 1:
                cand = [i for i in idx if not cm[i] and child[i] > 0]
                if cand:
                    j = min(cand, key=lambda i: child[i])
                    cm[j] = True
                    changed = True
                else:                                 # nothing to hide with: hide the parent instead
                    pm[p] = True
                    changed = True
        if not nm and pm.sum() == 1:                  # divisions under the national total
            cand = [p for p in range(n_par) if not pm[p] and parent[p] > 0]
            if cand:
                pm[min(cand, key=lambda p: parent[p])] = True
            else:
                nm = True
            changed = True
        # a suppressed parent whose children are all visible would be recoverable by addition
        for p in range(n_par):
            idx = np.nonzero(groups == p)[0]
            if pm[p] and not cm[idx].any() and parent[p] > 0:
                cand = [i for i in idx if child[i] > 0]
                j = min(cand, key=lambda i: child[i])
                cm[j] = True
                changed = True
    return cm, parent, pm, np.array([nat, nm])


def audit(child: list, groups: list, parent: list, national) -> list[str]:
    """Return the list of recoverable suppressed values (empty list = safe)."""
    problems = []
    groups = np.asarray(groups)
    for p in range(len(parent)):
        idx = np.nonzero(groups == p)[0]
        hidden = [i for i in idx if child[i] is None]
        if parent[p] is not None and len(hidden) == 1:
            problems.append(f"child {hidden[0]} recoverable from parent {p}")
        if parent[p] is None and hidden == [] and any((child[i] or 0) > 0 for i in idx):
            problems.append(f"parent {p} recoverable from its children")
    hidden_p = [p for p in range(len(parent)) if parent[p] is None]
    if national is not None and len(hidden_p) == 1:
        problems.append(f"parent {hidden_p[0]} recoverable from national")
    for v in list(child) + list(parent) + [national]:
        if v is not None and 1 <= v < K:
            problems.append(f"small value {v} published")
    return problems
