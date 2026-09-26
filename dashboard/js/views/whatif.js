// View 6 (VIS-5): what-if sliders for budget, alert threshold and equity floor. Allocations come from a
// precomputed scenario grid (every combination was solved by the optimiser offline); alert statuses are
// recomputed in the browser from the published detection statistics.
/* global d3 */
import { fmt, hideTip, legendItems, showTip, svgIn } from "../util.js";

let wired = false;

export function scenarioCell(store) {
  const sc = store.data.allocation.scenarios;
  const { thr, budget, alpha } = store.state;
  return sc.cells[`${thr}|${budget}|${alpha}`] || Object.values(sc.cells)[0];
}

function slider(id, values, get, set, label) {
  const el = document.getElementById(id);
  el.min = 0; el.max = values.length - 1; el.step = 1;
  el.value = Math.max(0, values.indexOf(get()));
  el.oninput = () => set(values[+el.value]);
  el.setAttribute("aria-valuetext", label(get()));
}

export function init(store) {
  const sc = store.data.allocation.scenarios;
  slider("sl-budget", sc.budget_levels, () => store.state.budget, (v) => store.set({ budget: v }), (v) => `${v * 100}% of gap`);
  slider("sl-thr", sc.thresholds, () => store.state.thr, (v) => store.set({ thr: v }), (v) => `${v}`);
  slider("sl-alpha", sc.alphas, () => store.state.alpha, (v) => store.set({ alpha: v }), (v) => `${v}`);
  wired = true;
}

export function update(store) {
  if (!wired) return;
  const sc = store.data.allocation.scenarios;
  const s = store.state;
  const summ = store.data.allocation.summary;
  document.getElementById("sl-budget").value = sc.budget_levels.indexOf(s.budget);
  document.getElementById("sl-thr").value = sc.thresholds.indexOf(s.thr);
  document.getElementById("sl-alpha").value = sc.alphas.indexOf(s.alpha);
  document.getElementById("out-budget").textContent = fmt.bdt(s.budget * summ.full_gap_cost_bdt);
  document.getElementById("out-thr").textContent = s.thr;
  document.getElementById("out-alpha").textContent = s.alpha;
  const cell = scenarioCell(store);
  const cov = cell.coverage.filter((v) => v !== null);
  const tiles = [
    ["Budget", fmt.bdt(s.budget * summ.full_gap_cost_bdt), `${Math.round(s.budget * 100)}% of the full supply gap`],
    ["Lowest district coverage", fmt.pct(d3.min(cov)), "share of its supply gap met"],
    ["Districts < 50% covered", d3.sum(cov, (v) => v < 0.5), `of ${cov.length} with a gap`],
    ["Response teams sent", d3.sum(cell.teams), `${store.data.alerts.method.toUpperCase()} threshold ${s.thr}`],
  ];
  d3.select("#whatif-tiles").selectAll(".tile").data(tiles).join("div").attr("class", "tile")
    .html((t) => `<div class="k">${t[0]}</div><div class="v">${t[1]}</div><div class="d">${t[2]}</div>`);
  curve(store);
}

// Efficiency-equity curve (AN-8): total weighted unmet demand vs. the lowest district coverage, per alpha,
// at the selected budget; population-proportional baseline for comparison.
function curve(store) {
  const W = 420, H = 210, M = { t: 12, r: 12, b: 36, l: 56 };
  const svg = svgIn("#curve", W, H).attr("aria-label", "Efficiency-equity trade-off");
  const pts = store.data.allocation.curve.filter((r) => r.budget_level === store.state.budget && r.min_district_coverage !== null);
  const opt = pts.filter((r) => r.status === "optimal").sort((a, b) => a.alpha - b.alpha);
  const base = pts.find((r) => r.status === "population_baseline");
  if (!opt.length) return;
  const x = d3.scaleLinear().domain([0, 1]).range([M.l, W - M.r]);
  const y = d3.scaleLinear().domain([0, d3.max(pts, (p) => p.weighted_unmet) * 1.1]).nice().range([H - M.b, M.t]);
  svg.append("g").attr("class", "gridline").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(4).tickSize(-(W - M.l - M.r)).tickFormat(""));
  svg.append("g").attr("class", "axis").attr("transform", `translate(0,${H - M.b})`).call(d3.axisBottom(x).ticks(5).tickFormat(d3.format(".0%")));
  svg.append("g").attr("class", "axis").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(4).tickFormat(d3.format("~s")));
  svg.append("text").attr("x", (M.l + W - M.r) / 2).attr("y", H - 4).attr("text-anchor", "middle").text("Equity: lowest district coverage →");
  svg.append("text").attr("x", 4).attr("y", M.t - 2).text("Weighted unmet need, patient-equivalents (lower = more efficient)");
  svg.append("path").datum(opt).attr("fill", "none").attr("stroke", "var(--series-1)").attr("stroke-width", 2)
    .attr("d", d3.line().x((p) => x(p.min_district_coverage)).y((p) => y(p.weighted_unmet)));
  svg.selectAll("circle.o").data(opt).join("circle").attr("class", "o").attr("r", (p) => (p.alpha === store.state.alpha ? 7 : 5))
    .attr("cx", (p) => x(p.min_district_coverage)).attr("cy", (p) => y(p.weighted_unmet))
    .attr("fill", (p) => (p.alpha === store.state.alpha ? "var(--series-1)" : "var(--surface-1)")).attr("stroke", "var(--series-1)").attr("stroke-width", 2)
    .style("cursor", "pointer").on("click", (e, p) => store.set({ alpha: p.alpha }))
    .on("mousemove", (e, p) => showTip(e, `<div class="t">Equity floor α = ${p.alpha}</div>Weighted unmet need: ${fmt.num(p.weighted_unmet, 0)} patient-eq.<br>
      Gap covered: ${fmt.num(p.gap_covered_pct, 1)}%<br>Lowest district coverage: ${fmt.pct(p.min_district_coverage)}<br>Districts below 50%: ${p.districts_below_50pct}`))
    .on("mouseleave", hideTip);
  svg.selectAll("text.al").data(opt).join("text").attr("class", "al").attr("x", (p) => x(p.min_district_coverage) + 7)
    .attr("y", (p) => y(p.weighted_unmet) - 6).text((p) => `α=${p.alpha}`);
  if (base) {
    svg.append("rect").attr("x", x(base.min_district_coverage) - 5).attr("y", y(base.weighted_unmet) - 5).attr("width", 10).attr("height", 10)
      .attr("fill", "var(--series-2)").on("mousemove", (e) => showTip(e, `<div class="t">Population-proportional baseline</div>
        Weighted unmet need: ${fmt.num(base.weighted_unmet, 0)} patient-eq.<br>Lowest district coverage: ${fmt.pct(base.min_district_coverage)}`)).on("mouseleave", hideTip);
  }
  legendItems(d3.select("#curve").append("div").attr("class", "legend").node(), [
    { color: "var(--series-1)", label: "Optimised plan for each equity floor α (click to apply)" },
    { color: "var(--series-2)", label: "Population-proportional baseline" },
  ]);
}
