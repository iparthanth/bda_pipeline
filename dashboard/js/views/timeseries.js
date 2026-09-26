// View 2: weekly observed cases, nowcast, forecast median with 50/80/90% bands, alert threshold and alert
// markers; the context strip carries a brush that sets the global week range (focus + context, VIS-2/3).
/* global d3 */
import { DISEASE_COLOR, fmt, hideTip, legendItems, showTip, svgIn } from "../util.js";
import { alertDays, seriesFor } from "./common.js";

const W = 760, H = 390, M = { t: 16, r: 16, b: 24, l: 48 }, FOCUS_H = 260, CTX_T = 310, CTX_H = 48;
let svg, brushG, brush, silent = false;

export function init(store) {
  svg = svgIn("#ts", W, H).attr("aria-label", "Weekly cases with forecast and alert threshold");
  svg.append("defs").append("clipPath").attr("id", "ts-clip").append("rect")
    .attr("x", M.l).attr("y", M.t).attr("width", W - M.l - M.r).attr("height", FOCUS_H);
  svg.append("g").attr("class", "gridline gy");
  svg.append("g").attr("class", "axis ax");
  svg.append("g").attr("class", "axis ay");
  svg.append("g").attr("class", "focus").attr("clip-path", "url(#ts-clip)");
  svg.append("g").attr("class", "axis axc");
  svg.append("g").attr("class", "context");
  brushG = svg.append("g").attr("class", "brush");
  svg.append("rect").attr("class", "hover").attr("x", M.l).attr("y", M.t).attr("width", W - M.l - M.r).attr("height", FOCUS_H)
    .attr("fill", "transparent");
  svg.append("line").attr("class", "cross").attr("stroke", "var(--axis)").attr("y1", M.t).attr("y2", M.t + FOCUS_H).attr("opacity", 0);
}

const lo = (v) => (v === null || v === undefined ? 0 : v);   // suppressed quantile: lower bound 0
const hi = (v) => (v === null || v === undefined ? 4 : v);   // ... and upper bound 4

export function update(store) {
  if (!svg) return;
  const { area, disease, weekRange } = store.state;
  const weeks = store.data.weekly.weeks.map((w) => new Date(w));
  const ser = seriesFor(store, area, disease);
  const obs = (ser.values || []).map((v, i) => ({ d: weeks[i], v }));
  const key = ser.key;
  const fc = store.data.forecasts.forecast[key];
  const now = store.data.forecasts.nowcast[key];
  const thr = store.data.thresholds[key];
  const fcRows = fc ? fc.weeks.map((w, i) => ({ d: new Date(w), mean: fc.mean[i], q05: fc.q05[i], q10: fc.q10[i], q25: fc.q25[i], q50: fc.q50[i], q75: fc.q75[i], q90: fc.q90[i], q95: fc.q95[i] })) : [];
  const nowRows = now ? now.weeks.map((w, i) => ({ d: new Date(w), est: now.nowcast[i] })).sort((a, b) => a.d - b.d) : [];
  const thrRows = thr ? thr.map((v, i) => ({ d: weeks[i], v })) : [];

  const [a, b] = weekRange;
  const endDate = b >= weeks.length - 1 && fcRows.length ? fcRows[fcRows.length - 1].d : weeks[b];
  const x = d3.scaleTime().domain([weeks[a], endDate]).range([M.l, W - M.r]);
  const inRange = (d) => d >= weeks[a] && d <= endDate;
  const ymax = d3.max([
    d3.max(obs.filter((o) => inRange(o.d)), (o) => o.v ?? 4),
    d3.max(fcRows.filter((o) => inRange(o.d)), (o) => hi(o.q95)),
    d3.max(nowRows, (o) => (inRange(o.d) ? hi(o.est) : 0)),
    d3.max(thrRows.filter((o) => inRange(o.d)), (o) => o.v ?? 0),
  ]) || 5;
  const y = d3.scaleLinear().domain([0, ymax * 1.08]).nice().range([M.t + FOCUS_H, M.t]);

  svg.select(".gy").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(5).tickSize(-(W - M.l - M.r)).tickFormat(""));
  svg.select(".ay").attr("transform", `translate(${M.l},0)`).call(d3.axisLeft(y).ticks(5).tickFormat(d3.format("~s")));
  svg.select(".ax").attr("transform", `translate(0,${M.t + FOCUS_H})`).call(d3.axisBottom(x).ticks(6));

  const f = svg.select("g.focus");
  f.selectAll("*").remove();
  const col = DISEASE_COLOR[disease];
  if (fcRows.length) {
    const lastObs = obs.filter((o) => o.v !== null).slice(-1)[0];
    const bands = [["q05", "q95", 0.12], ["q10", "q90", 0.2], ["q25", "q75", 0.3]];
    bands.forEach(([l, u, op]) => f.append("path").datum(fcRows).attr("fill", "var(--band)").attr("opacity", op)
      .attr("d", d3.area().x((d) => x(d.d)).y0((d) => y(lo(d[l]))).y1((d) => y(hi(d[u])))));
    f.append("path").datum(fcRows).attr("fill", "none").attr("stroke", "var(--band)").attr("stroke-width", 2)
      .attr("d", d3.line().x((d) => x(d.d)).y((d) => y(d.q50 ?? 2)));
    if (lastObs) f.append("circle").attr("cx", x(fcRows[0].d)).attr("cy", y(fcRows[0].q50 ?? 2)).attr("r", 3).attr("fill", "var(--band)");
  }
  if (thrRows.length) {
    f.append("path").datum(thrRows.filter((d) => d.v !== null)).attr("fill", "none").attr("stroke", "var(--critical)")
      .attr("stroke-width", 1.5).attr("stroke-dasharray", "2 3")
      .attr("d", d3.line().x((d) => x(d.d)).y((d) => y(d.v)));
  }
  f.append("path").datum(obs).attr("fill", "none").attr("stroke", col).attr("stroke-width", 2)
    .attr("d", d3.line().defined((d) => d.v !== null).x((d) => x(d.d)).y((d) => y(d.v)));
  f.selectAll("rect.supp").data(obs.filter((o) => o.v === null)).join("rect").attr("class", "supp")
    .attr("x", (d) => x(d.d) - 3).attr("width", 6).attr("y", y(4)).attr("height", y(0) - y(4)).attr("fill", "url(#hatch)");
  if (nowRows.length) {
    f.append("path").datum(nowRows.filter((d) => d.est !== null)).attr("fill", "none").attr("stroke", col).attr("stroke-width", 2)
      .attr("stroke-dasharray", "5 4").attr("d", d3.line().x((d) => x(d.d)).y((d) => y(d.est)));
    f.selectAll("circle.now").data(nowRows.filter((d) => d.est !== null)).join("circle").attr("class", "now")
      .attr("cx", (d) => x(d.d)).attr("cy", (d) => y(d.est)).attr("r", 3.5).attr("fill", "var(--surface-1)").attr("stroke", col).attr("stroke-width", 2);
  }
  // alert markers (days at the what-if threshold, drawn in the week they fall in)
  const ad = alertDays(store, key).map((s) => d3.timeMonday.floor(new Date(s)));
  const uniq = [...new Set(ad.map((d) => +d))].map((t) => new Date(t));
  f.selectAll("path.alert").data(uniq).join("path").attr("class", "alert")
    .attr("d", d3.symbol(d3.symbolTriangle, 70)).attr("fill", "var(--critical)").attr("stroke", "var(--surface-1)").attr("stroke-width", 1.5)
    .attr("transform", (d) => `translate(${x(d)},${M.t + 8})`);

  // context strip with brush
  const xc = d3.scaleTime().domain(d3.extent(weeks)).range([M.l, W - M.r]);
  const yc = d3.scaleLinear().domain([0, d3.max(obs, (o) => o.v ?? 0) || 1]).range([CTX_T + CTX_H, CTX_T]);
  const c = svg.select("g.context");
  c.selectAll("*").remove();
  c.append("path").datum(obs).attr("fill", col).attr("opacity", 0.35)
    .attr("d", d3.area().defined((d) => d.v !== null).x((d) => xc(d.d)).y0(yc(0)).y1((d) => yc(d.v)));
  svg.select(".axc").attr("transform", `translate(0,${CTX_T + CTX_H})`).call(d3.axisBottom(xc).ticks(d3.timeYear.every(1)));
  brush = d3.brushX().extent([[M.l, CTX_T], [W - M.r, CTX_T + CTX_H]]).on("end", (ev) => {
    if (silent || !ev.selection) return;
    const [d0, d1] = ev.selection.map(xc.invert);
    const i0 = d3.bisectLeft(weeks, d3.timeMonday.floor(d0));
    const i1 = Math.max(i0, d3.bisectRight(weeks, d1) - 1);
    store.set({ weekRange: [Math.max(0, i0), Math.min(weeks.length - 1, i1)] });
  });
  brushG.call(brush);
  silent = true;
  brushG.call(brush.move, [xc(weeks[a]), xc(d3.timeDay.offset(weeks[b], 6))]);
  silent = false;

  // crosshair tooltip
  const all = [...obs.map((o) => o.d), ...fcRows.map((o) => o.d)];
  svg.select("rect.hover").on("mousemove", (e) => {
    const [mx] = d3.pointer(e);
    const d = all.reduce((p, q) => (Math.abs(x(q) - mx) < Math.abs(x(p) - mx) ? q : p), all[0]);
    svg.select("line.cross").attr("x1", x(d)).attr("x2", x(d)).attr("opacity", 1);
    const o = obs.find((r) => +r.d === +d), fr = fcRows.find((r) => +r.d === +d), nr = nowRows.find((r) => +r.d === +d);
    const t = thrRows.find((r) => +r.d === +d);
    showTip(e, `<div class="t">${store.areaName(key.split("|")[0])} · week of ${fmt.date(d)}</div>
      ${o ? `Reported cases: <b>${fmt.int(o.v)}</b><br>` : ""}
      ${nr ? `Nowcast (week still being reported): <b>${fmt.int(nr.est)}</b>${o ? ` (${fmt.int(o.v)} reported so far)` : ""}<br>` : ""}
      ${fr ? `Forecast median: <b>${fmt.int(fr.q50)}</b>; 80% interval ${fmt.int(fr.q10)}–${fmt.int(fr.q90)}<br>` : ""}
      ${t && t.v !== null ? `Alert threshold: ${fmt.int(t.v)}` : ""}`);
  }).on("mouseleave", () => { hideTip(); svg.select("line.cross").attr("opacity", 0); });

  legendItems("#ts-legend", [
    { line: true, color: col, label: `Reported (${ser.aggregate ? "aggregate: " : ""}${store.areaName(key.split("|")[0])})` },
    { line: true, color: col, dash: true, label: "Nowcast (still being reported)" },
    { line: true, color: "var(--band)", label: "Forecast median" },
    { color: "var(--band)", opacity: 0.35, label: "50 / 80 / 90% interval" },
    { line: true, color: "var(--critical)", dash: true, label: "Alert threshold (expected + 2 SD)" },
    { color: "var(--critical)", label: "▲ alert day(s) in week" },
    { color: "var(--hatch-bg)", hatch: true, label: "Suppressed week (<5)" },
  ]);
}
