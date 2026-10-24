// Preparedness calendar (AN-9): per division and disease, when to start prevention campaigns, pre-position
// supplies, and expect the season onset and peak over the next 12 months.
/* global d3 */
import { DISEASE_COLOR, fmt, hideTip, legendItems, showTip, svgIn } from "../util.js";

export function init() {}

export function update(store) {
  const { disease, area } = store.state;
  const cal = store.data.calendar.filter((c) => c.disease_group === disease);
  const W = 720, rowH = 26, M = { t: 26, r: 12, b: 24, l: 110 };
  const rows = cal.sort((a, b) => a.division.localeCompare(b.division));
  const H = M.t + rows.length * rowH + M.b;
  const svg = svgIn("#cal", W, H).attr("aria-label", "Seasonal preparedness calendar");
  const dates = rows.filter((r) => r.seasonal).flatMap((r) => [r.campaign_start, r.season_end].map((s) => new Date(s)));
  const t0 = d3.timeMonday.floor(new Date(store.data.meta.freshness.cases));
  const x = d3.scaleTime().domain([t0, d3.timeWeek.offset(t0, 53)]).range([M.l, W - M.r]);
  const y = d3.scaleBand().domain(rows.map((r) => r.adm1_pcode)).range([M.t, H - M.b]).padding(0.2);
  svg.append("g").attr("class", "gridline").attr("transform", `translate(0,${H - M.b})`).call(d3.axisBottom(x).ticks(d3.timeMonth.every(1)).tickSize(-(H - M.t - M.b)).tickFormat(""));
  svg.append("g").attr("class", "axis").attr("transform", `translate(0,${H - M.b})`).call(d3.axisBottom(x).ticks(d3.timeMonth.every(2)).tickFormat(d3.timeFormat("%b %y")));
  const col = DISEASE_COLOR[disease];
  const g = svg.append("g").selectAll("g").data(rows).join("g").attr("transform", (r) => `translate(0,${y(r.adm1_pcode)})`);
  const bh = y.bandwidth();
  const areaDiv = store.data.meta.districts.find((d) => d.adm2_pcode === area)?.adm1_pcode ?? area;
  g.append("text").attr("x", M.l - 8).attr("y", bh / 2).attr("dy", "0.35em").attr("text-anchor", "end")
    .style("font-weight", (r) => (r.adm1_pcode === areaDiv ? 700 : 400)).text((r) => r.division);
  // span helper handles seasons that wrap past the end of the 12-month window
  const span = (sel, a, b, cls, fill, op) => {
    const xa = x(new Date(a)), xb = x(new Date(b));
    const segs = xb >= xa ? [[xa, xb]] : [[xa, x.range()[1]], [x.range()[0], xb]];
    segs.forEach(([p, q]) => sel.append("rect").attr("class", cls).attr("x", p).attr("width", Math.max(2, q - p)).attr("y", 0).attr("height", bh).attr("rx", 3).attr("fill", fill).attr("opacity", op));
  };
  g.each(function (r) {
    const s = d3.select(this);
    if (!r.seasonal) {
      s.append("text").attr("x", M.l + 4).attr("y", bh / 2).attr("dy", "0.35em").text("no clear seasonal pattern - year-round readiness");
      return;
    }
    span(s, r.campaign_start, r.season_onset, "camp", "var(--series-3)", 0.55);
    span(s, r.season_onset, r.season_end, "season", col, 0.75);
    s.append("path").attr("d", d3.symbol(d3.symbolTriangle, 50)).attr("transform", `translate(${x(new Date(r.preposition_by))},${bh / 2}) rotate(180)`)
      .attr("fill", "var(--text-primary)");
    s.append("line").attr("x1", x(new Date(r.season_peak))).attr("x2", x(new Date(r.season_peak))).attr("y1", -2).attr("y2", bh + 2)
      .attr("stroke", "var(--text-primary)").attr("stroke-width", 2);
  }).on("mousemove", (e, r) => showTip(e, r.seasonal ? `<div class="t">${r.division} · ${store.data.meta.diseases[disease]}</div>
      Start prevention campaign: <b>${fmt.date(r.campaign_start)}</b><br>Pre-position supplies by: <b>${fmt.date(r.preposition_by)}</b><br>
      Season: ${fmt.date(r.season_onset)} – ${fmt.date(r.season_end)} (peak ~${fmt.date(r.season_peak)})<br>
      Surge staffing from: ${fmt.date(r.surge_staffing_from)}<br>Seasonal strength ${fmt.num(r.seasonal_strength, 2)} · peak ${fmt.num(r.peak_index, 1)}× average`
      : `<div class="t">${r.division}</div>No clear season (STL seasonal index never ≥15% above average for 3+ weeks).`))
    .on("mouseleave", hideTip);
  svg.append("text").attr("x", M.l).attr("y", 14).text(`${store.data.meta.diseases[disease]}: derived from STL seasonal decomposition of 2021+ weekly cases`);
  legendItems("#cal-legend", [
    { color: "var(--series-3)", opacity: 0.55, label: "Prevention campaign lead time" },
    { color: col, opacity: 0.75, label: "Expected season" },
    { color: "var(--text-primary)", label: "▼ pre-position supplies by" },
    { line: true, color: "var(--text-primary)", label: "expected peak" },
  ]);
}
