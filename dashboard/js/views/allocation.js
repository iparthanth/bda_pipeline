// View 4: per district, forecast demand (80% interval + planning quantile), current stock, recommended
// allocation (current what-if scenario) and the population-proportional baseline, for one resource.
/* global d3 */
import { fmt, hideTip, legendItems, showTip, svgIn } from "../util.js";
import { scenarioCell } from "./whatif.js";

export function init() {}

export function update(store) {
  const { resource, area } = store.state;
  const m = store.data.meta;
  const names = new Map(m.districts.map((d) => [d.adm2_pcode, d.adm2_name]));
  const cell = scenarioCell(store);
  const rec = new Map(cell.adm2.map((pc, i) => [pc, cell[resource]?.[i] ?? 0]));
  const teams = new Map(cell.adm2.map((pc, i) => [pc, cell.teams[i]]));
  let rows = store.data.allocation.rows.filter((r) => r.resource === resource)
    .map((r) => ({ ...r, rec: rec.get(r.adm2_pcode) ?? r.recommended, teams: teams.get(r.adm2_pcode) ?? 0 }));
  rows.sort((a, b) => (b.gap ?? -1) - (a.gap ?? -1));
  let shown = rows.slice(0, 16);
  const sel = rows.find((r) => r.adm2_pcode === area);
  if (sel && !shown.includes(sel)) shown = [...shown.slice(0, 15), sel];
  const W = 820, rowH = 24, M = { t: 24, r: 20, b: 30, l: 120 };
  const H = M.t + shown.length * rowH + M.b;
  const svg = svgIn("#alloc", W, H).attr("aria-label", "Demand, stock and allocation by district");
  const maxv = d3.max(shown, (r) => Math.max(r.demand_q90 ?? 4, r.stock, r.rec, r.baseline)) || 1;
  const x = d3.scaleLinear().domain([0, maxv * 1.05]).nice().range([M.l, W - M.r]);
  const y = d3.scaleBand().domain(shown.map((r) => r.adm2_pcode)).range([M.t, H - M.b]).padding(0.25);
  svg.append("g").attr("class", "gridline").attr("transform", `translate(0,${H - M.b})`).call(d3.axisBottom(x).ticks(6).tickSize(-(H - M.t - M.b)).tickFormat(""));
  svg.append("g").attr("class", "axis").attr("transform", `translate(0,${H - M.b})`).call(d3.axisBottom(x).ticks(6).tickFormat(d3.format("~s")));
  const g = svg.append("g").selectAll("g").data(shown).join("g").attr("transform", (r) => `translate(0,${y(r.adm2_pcode)})`)
    .style("cursor", "pointer").on("click", (e, r) => store.set({ area: r.adm2_pcode }));
  const bh = y.bandwidth();
  g.append("rect").attr("x", M.l - 116).attr("width", W - 8).attr("height", bh).attr("fill", (r) => (r.adm2_pcode === area ? "var(--surface-2)" : "transparent"));
  g.append("text").attr("x", M.l - 8).attr("y", bh / 2).attr("dy", "0.35em").attr("text-anchor", "end")
    .style("font-weight", (r) => (r.adm2_pcode === area ? 700 : 400)).text((r) => names.get(r.adm2_pcode));
  g.append("rect").attr("x", x(0)).attr("y", bh * 0.15).attr("height", bh * 0.7).attr("rx", 3)
    .attr("width", (r) => Math.max(0, x(r.baseline) - x(0))).attr("fill", "none").attr("stroke", "var(--text-muted)").attr("stroke-dasharray", "3 2");
  g.append("rect").attr("x", x(0)).attr("y", bh * 0.3).attr("height", bh * 0.4).attr("rx", 3)
    .attr("width", (r) => Math.max(0, x(r.rec) - x(0))).attr("fill", "var(--series-1)");
  g.append("line").attr("x1", (r) => x(r.demand_q10 ?? 0)).attr("x2", (r) => x(r.demand_q90 ?? 4)).attr("y1", bh / 2).attr("y2", bh / 2)
    .attr("stroke", "var(--series-2)").attr("stroke-width", 2);
  g.append("line").attr("x1", (r) => x(r.demand_q80 ?? 4)).attr("x2", (r) => x(r.demand_q80 ?? 4)).attr("y1", 1).attr("y2", bh - 1)
    .attr("stroke", "var(--series-2)").attr("stroke-width", 2);
  g.append("path").attr("d", d3.symbol(d3.symbolDiamond, 60)).attr("transform", (r) => `translate(${x(r.stock)},${bh / 2})`)
    .attr("fill", "var(--text-primary)").attr("stroke", "var(--surface-1)").attr("stroke-width", 1.5);
  g.on("mousemove", (e, r) => showTip(e, `<div class="t">${names.get(r.adm2_pcode)} · ${m.resources[resource]}</div>
      Forecast demand (4 wk): ${fmt.int(r.demand_mean)} (80% interval ${fmt.int(r.demand_q10)}–${fmt.int(r.demand_q90)})<br>
      Planning level (80th pct.): <b>${fmt.int(r.demand_q80)}</b><br>Current stock: ${fmt.int(r.stock)}<br>
      Gap: ${r.gap === null ? "–" : fmt.int(Math.max(0, r.gap))}<br>Recommended shipment: <b>${fmt.int(r.rec)}</b><br>
      Population-share baseline: ${fmt.int(r.baseline)}<br>Response teams: ${r.teams}`)).on("mouseleave", hideTip);
  const beds = store.data.allocation.beds.find((b) => b.adm2_pcode === area);
  svg.append("text").attr("x", M.l).attr("y", 14).text(`${m.resources[resource]} · top ${Math.min(16, rows.length)} districts by supply gap` +
    (beds ? ` · ${names.get(area)} peak bed demand ≈ ${fmt.int(beds.demand_q80)} (80th pct.)` : ""));
  legendItems("#alloc-legend", [
    { line: true, color: "var(--series-2)", label: "Forecast demand, 10–90% (tick = planning 80th pct.)" },
    { color: "var(--text-primary)", label: "◆ Current stock" },
    { color: "var(--series-1)", label: "Recommended shipment (optimiser, current what-if)" },
    { color: "transparent", label: "▭ dashed = population-share baseline" },
  ]);
}
