// View 5: ranked early-warning table. Status is recomputed in the browser for the what-if alert threshold
// from the published detection statistics; clicking a row selects that district and disease everywhere.
/* global d3 */
import { fmt, STATUS } from "../util.js";
import { statusAt } from "./common.js";

const RANK = { alert: 0, verify_reporting: 1, watch: 2, normal: 3 };

export function init() {}

export function update(store) {
  const { area, disease } = store.state;
  const m = store.data.meta;
  const name = new Map(m.districts.map((d) => [d.adm2_pcode, d]));
  let rows = store.data.alerts.table.map((r) => ({ ...r, st: statusAt(store, r) }));
  rows.sort((a, b) => RANK[a.st] - RANK[b.st] || (b.statistic ?? 0) - (a.statistic ?? 0));
  const shown = rows.filter((r) => r.st !== "normal" || r.disease_group === disease).slice(0, 80);
  const t = d3.select("#alert-table");
  t.selectAll("*").remove();
  const cols = [
    ["Status", (r) => `<span class="status ${r.st}"><span class="dot"></span>${STATUS[r.st].icon} ${STATUS[r.st].label}</span>`],
    ["District", (r) => name.get(r.adm2_pcode)?.adm2_name ?? r.adm2_pcode],
    ["Division", (r) => name.get(r.adm2_pcode)?.adm1_name ?? ""],
    ["Disease", (r) => m.diseases[r.disease_group]],
    ["Signal", (r) => fmt.num(r.statistic, 1), true],
    ["Cases 7d", (r) => fmt.int(r.count7), true],
    ["Expected 7d", (r) => fmt.int(r.expected7), true],
    ["Forecast next wk", (r) => fmt.int(r.forecast_next), true],
    ["4-wk trend", (r) => (r.trend === null ? "–" : `${r.trend > 0.1 ? "↑" : r.trend < -0.1 ? "↓" : "→"} ${fmt.pct(r.trend)}`), true],
    ["Days in alert", (r) => r.days_in_alert, true],
  ];
  t.append("thead").append("tr").selectAll("th").data(cols).join("th").attr("class", (c) => (c[2] ? "num" : null)).text((c) => c[0]);
  t.append("tbody").selectAll("tr").data(shown).join("tr")
    .attr("class", (r) => `row${r.adm2_pcode === area && r.disease_group === disease ? " selected" : ""}`)
    .attr("tabindex", 0)
    .on("click", (e, r) => store.set({ area: r.adm2_pcode, disease: r.disease_group }))
    .on("keydown", (e, r) => { if (e.key === "Enter") store.set({ area: r.adm2_pcode, disease: r.disease_group }); })
    .selectAll("td").data((r) => cols.map((c) => [c, r])).join("td").attr("class", ([c]) => (c[2] ? "num" : null))
    .html(([c, r]) => c[1](r));
  const n = d3.rollup(rows, (v) => v.length, (r) => r.st);
  let cap = `${n.get("alert") || 0} alert(s), ${n.get("verify_reporting") || 0} to verify (reporting change), ${n.get("watch") || 0} on watch ` +
    `at threshold ${store.state.thr} (${store.data.alerts.method.toUpperCase()}). Signal = CUSUM of the 7-day count vs. the same season in earlier years. ` +
    "“Verify reporting” = a facility started reporting recently, so the rise may not be an outbreak.";
  if (store.data.alerts.division_summary) {
    const ds = store.data.alerts.division_summary.filter((d) => d.disease_group === disease && d.districts_alerting > 0);
    cap += ` Other divisions (aggregate): ${ds.map((d) => `${store.areaName(d.adm1_pcode)} ${d.districts_alerting} district(s) alerting`).join("; ") || "none alerting"}.`;
  }
  d3.select("#alert-caption").text(cap);
}
