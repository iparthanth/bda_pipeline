// Method transparency panel: detection trade-off curve (conflict C1) and forecast skill vs. baseline.
/* global d3 */
import { fmt, hideTip, legendItems, showTip, svgIn } from "../util.js";

const MCOL = { cusum: "var(--series-1)", ears_c1: "var(--series-2)", ears_c2: "var(--series-3)", ears_c3: "var(--series-4)" };

export function init() {}

export function update(store) {
  const ev = store.data.evaluation;
  if (!ev || !ev.tradeoff) return;
  const W = 520, H = 250, M = { t: 14, r: 14, b: 34, l: 48 };
  const svg = svgIn("#eval", W, H).attr("aria-label", "Detection trade-off curve");
  const rows = ev.tradeoff.filter((r) => r.variant === "guarded");
  const x = d3.scaleLinear().domain([0, d3.min([d3.max(rows, (r) => r.false_alerts_per_district_year), 12])]).nice().range([M.l, W - M.r]);
  const y = d3.scaleLinear().domain([0, 1]).range([H - M.b, M.t]);
  svg.append("g").attr("class", "gridline").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(5).tickSize(-(W - M.l - M.r)).tickFormat(""));
  svg.append("g").attr("class", "axis").attr("transform", `translate(0,${H - M.b})`).call(d3.axisBottom(x).ticks(6));
  svg.append("g").attr("class", "axis").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(5).tickFormat(d3.format(".0%")));
  svg.append("text").attr("x", (M.l + W) / 2).attr("y", H - 4).attr("text-anchor", "middle").text("False alerts per district-year →");
  svg.append("text").attr("x", 4).attr("y", M.t - 3).text("Outbreaks detected (sensitivity)");
  const byM = d3.group(rows.filter((r) => r.false_alerts_per_district_year <= x.domain()[1]), (r) => r.method);
  for (const [m, rs] of byM) {
    const pts = rs.sort((a, b) => a.false_alerts_per_district_year - b.false_alerts_per_district_year);
    svg.append("path").datum(pts).attr("fill", "none").attr("stroke", MCOL[m]).attr("stroke-width", 2)
      .attr("d", d3.line().x((r) => x(r.false_alerts_per_district_year)).y((r) => y(r.sensitivity_local)));
    svg.selectAll(`circle.${m}`).data(pts).join("circle").attr("class", m).attr("r", 4)
      .attr("cx", (r) => x(r.false_alerts_per_district_year)).attr("cy", (r) => y(r.sensitivity_local))
      .attr("fill", "var(--surface-1)").attr("stroke", MCOL[m]).attr("stroke-width", 2)
      .on("mousemove", (e, r) => showTip(e, `<div class="t">${m.toUpperCase()} threshold ${r.threshold}</div>
        Sensitivity (local outbreaks): ${fmt.pct(r.sensitivity_local)}<br>Median days to detection: ${fmt.num(r.median_delay_local, 0)}<br>
        False alerts / district-year: ${fmt.num(r.false_alerts_per_district_year, 2)}`)).on("mouseleave", hideTip);
  }
  const op = ev.detection.operating_point;
  const o = rows.find((r) => r.method === op.method && r.threshold === op.threshold);
  if (o) svg.append("circle").attr("cx", x(Math.min(o.false_alerts_per_district_year, x.domain()[1]))).attr("cy", y(o.sensitivity_local)).attr("r", 9)
    .attr("fill", "none").attr("stroke", "var(--text-primary)").attr("stroke-width", 2);
  legendItems(d3.select("#eval").append("div").attr("class", "legend").node(), [
    ...[...byM.keys()].map((m) => ({ line: true, color: MCOL[m], label: m.toUpperCase().replace("_", " ") })),
    { color: "transparent", label: "◯ chosen operating point" },
  ]);
  const fm = (ev.forecast_metrics || []).filter((r) => r.disease_group === "all" && r.horizon === 1);
  const g = (mm) => fm.find((r) => r.model === mm);
  const glm = g("glm_nb"), base = g("seasonal_naive");
  const dd = ev.dedup || {};
  d3.select("#eval-text").html(
    `Operating point: <b>${op.method.toUpperCase()} ≥ ${op.threshold}</b> - detects ${fmt.pct(o?.sensitivity_local)} of injected local outbreaks ` +
    `(median ${fmt.num(o?.median_delay_local, 0)} days after onset) with ${fmt.num(o?.false_alerts_per_district_year, 2)} false alerts per district-year.` +
    (glm && base ? ` 1-week forecasts: mean absolute error ${fmt.num(glm.mae, 1)} vs ${fmt.num(base.mae, 1)} for “same week last year”; ` +
      `80% intervals contain the truth ${fmt.pct(glm.coverage80)} of the time; WIS ${fmt.pct(glm.relative_wis_vs_baseline)} of the baseline.` : "") +
    (dd.precision_all_links !== undefined ? ` Duplicate-patient matching precision ${fmt.pct(dd.precision_all_links)}, recall ${fmt.pct(dd.recall)}.` : ""));
}
