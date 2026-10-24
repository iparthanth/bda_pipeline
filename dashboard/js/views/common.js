/* global d3 */
// Helpers shared by several views: series lookup with role-aware fallback and client-side alert status.

// Values of the weekly series for an area; for a district the user may not see in detail (district role),
// fall back to its division aggregate and say so.
export function seriesFor(store, area, disease) {
  const s = store.data.weekly.series;
  const k = `${area}|${disease}`;
  if (s[k]) return { values: s[k], aggregate: false, key: k };
  const d = store.data.meta.districts.find((x) => x.adm2_pcode === area);
  if (d && s[`${d.adm1_pcode}|${disease}`]) return { values: s[`${d.adm1_pcode}|${disease}`], aggregate: true, key: `${d.adm1_pcode}|${disease}` };
  return { values: null, aggregate: true, key: k };
}

export function population(store, code) {
  const m = store.data.meta;
  if (code === "BD") return d3.sum(m.divisions, (v) => v.population);
  const d = m.districts.find((x) => x.adm2_pcode === code);
  if (d) return d.population;
  const v = m.divisions.find((x) => x.adm1_pcode === code);
  return v ? v.population : null;
}

// Recompute the alert status of every table row for the what-if threshold (statistics, not counts).
export function statusAt(store, row) {
  const st = store.data.alerts.stats[`${row.adm2_pcode}|${row.disease_group}`];
  if (!st) return row.status;
  const i = st.stat.length - 1;
  const thr = store.state.thr;
  const s = st.stat[i] ?? 0;
  if (s >= thr && st.eligible[i] && st.new_facility[i]) return "verify_reporting";
  if (s >= thr && st.eligible[i]) return "alert";
  if (s >= 0.5 * thr && st.eligible[i]) return "watch";
  return "normal";
}

// Alert days (indices into alerts.dates) for a series at the current threshold.
export function alertDays(store, key) {
  const st = store.data.alerts.stats[key];
  if (!st) return [];
  const thr = store.state.thr;
  return store.data.alerts.dates.filter((_, i) => (st.stat[i] ?? 0) >= thr && st.eligible[i] && !st.new_facility[i]);
}
