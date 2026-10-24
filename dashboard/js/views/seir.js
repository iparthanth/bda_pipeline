// SEIR what-if (AN-10): the model calibrated on the novel-respiratory surge and intervention scenarios.
/* global d3 */
import { fmt, hideTip, legendItems, showTip, svgIn } from "../util.js";

const COLORS = ["var(--text-primary)", "var(--series-1)", "var(--series-2)", "var(--series-3)", "var(--series-4)", "#e87ba4", "#4a3aa7"];

export function init() {}

export function update(store) {
  const s = store.data.seir;
  if (!s) return;
  const W = 520, H = 260, M = { t: 14, r: 12, b: 30, l: 52 };
  const svg = svgIn("#seir", W, H).attr("aria-label", "SEIR scenarios");
  const names = Object.keys(s.scenarios);
  const t0 = new Date(s.fit.week_start[0]);
  const series = names.map((n) => ({ n, v: s.scenarios[n].reported_cases.map((c, i) => ({ d: d3.timeWeek.offset(t0, i), c })) }));
  const obs = s.fit.observed_excess.map((c, i) => ({ d: new Date(s.fit.week_start[i]), c }));
  const x = d3.scaleTime().domain(d3.extent(series[0].v, (p) => p.d)).range([M.l, W - M.r]);
  const y = d3.scaleLinear().domain([0, d3.max([...series.flatMap((q) => q.v), ...obs], (p) => p.c ?? 0) * 1.05]).nice().range([H - M.b, M.t]);
  svg.append("g").attr("class", "gridline").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(4).tickSize(-(W - M.l - M.r)).tickFormat(""));
  svg.append("g").attr("class", "axis").attr("transform", `translate(0,${H - M.b})`).call(d3.axisBottom(x).ticks(5));
  svg.append("g").attr("class", "axis").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(4).tickFormat(d3.format("~s")));
  // show the fitted curve + the four most informative scenarios (20/40% at detection, 40% earlier/later)
  const keep = names.filter((n, i) => i === 0 || /40% .* (at detection|earlier|later)/.test(n) || /20% .* at detection/.test(n));
  keep.forEach((n, i) => {
    const q = series.find((z) => z.n === n);
    svg.append("path").datum(q.v).attr("fill", "none").attr("stroke", COLORS[i]).attr("stroke-width", i === 0 ? 2.5 : 2)
      .attr("stroke-dasharray", i === 0 ? null : "5 3").attr("d", d3.line().defined((p) => p.c !== null).x((p) => x(p.d)).y((p) => y(p.c)))
      .on("mousemove", (e) => { const sm = s.scenarios[n].summary; showTip(e, `<div class="t">${n}</div>Total reported: ${fmt.int(sm.reported_cases)}<br>
        Peak: ${fmt.int(sm.peak_cases)} in week ${sm.peak_week + 1}<br>Cases averted vs no intervention: ${fmt.int(sm.cases_averted)}<br>Bed-days: ${fmt.int(sm.bed_days)}`); })
      .on("mouseleave", hideTip);
  });
  svg.selectAll("circle").data(obs.filter((o) => o.c !== null)).join("circle").attr("cx", (p) => x(p.d)).attr("cy", (p) => y(p.c)).attr("r", 3)
    .attr("fill", "var(--surface-1)").attr("stroke", "var(--text-secondary)").attr("stroke-width", 1.5);
  legendItems("#seir-legend", [
    { color: "var(--text-secondary)", label: "○ observed excess ILI cases" },
    ...keep.map((n, i) => ({ line: true, dash: i > 0, color: COLORS[i], label: n })),
    { color: "transparent", label: `R0 ≈ ${fmt.num(s.fit.R0, 2)}, reporting ≈ ${fmt.pct(s.fit.rho)}, fit R² ${fmt.num(s.fit.r2, 2)}` },
  ]);
}
