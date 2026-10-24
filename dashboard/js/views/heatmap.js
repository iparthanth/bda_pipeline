// View 3: seasonal heatmap, epi-week (columns) x year (rows) for the selected area and disease.
/* global d3 */
import { fmt, hideTip, legendItems, seqScale, showTip, svgIn } from "../util.js";
import { seriesFor } from "./common.js";

const W = 560, M = { t: 18, r: 8, b: 26, l: 40 };
let svg;

const isoWeek = (d) => { const t = new Date(Date.UTC(d.getFullYear(), d.getMonth(), d.getDate() + 3)); const y0 = new Date(Date.UTC(t.getUTCFullYear(), 0, 4)); return 1 + Math.round(((t - y0) / 864e5 - 3 + ((y0.getUTCDay() + 6) % 7)) / 7); };

export function init() { svg = null; }

export function update(store) {
  const { area, disease, weekRange } = store.state;
  const weeks = store.data.weekly.weeks.map((w) => new Date(w));
  const ser = seriesFor(store, area, disease);
  if (!ser.values) return;
  const cells = weeks.map((d, i) => {
    const th = d3.timeDay.offset(d, 3);
    return { i, d, year: th.getFullYear(), wk: Math.min(isoWeek(d), 53), v: ser.values[i] };
  });
  const perYear = d3.rollup(cells, (v) => v.length, (c) => c.year);
  const years = [...perYear.keys()].filter((yr) => perYear.get(yr) >= 4);   // skip stub rows at the edges
  cells.splice(0, cells.length, ...cells.filter((c) => years.includes(c.year)));
  const cw = (W - M.l - M.r) / 53, ch = 22;
  const H = M.t + years.length * ch + M.b;
  svg = svgIn("#heat", W, H).attr("aria-label", "Seasonal heatmap of weekly cases");
  const x = (wk) => M.l + (wk - 1) * cw;
  const y = d3.scaleBand().domain(years).range([M.t, M.t + years.length * ch]).padding(0.08);
  const max = d3.quantile(cells.map((c) => c.v).filter((v) => v !== null).sort(d3.ascending), 0.98) || 1;
  const color = seqScale(max);
  svg.append("g").selectAll("rect").data(cells).join("rect")
    .attr("x", (c) => x(c.wk) + 1).attr("y", (c) => y(c.year)).attr("width", Math.max(1, cw - 2)).attr("height", y.bandwidth())
    .attr("rx", 2).attr("fill", (c) => (c.v === null ? "url(#hatch)" : color(Math.min(c.v, max))))
    .attr("stroke", (c) => (c.i >= weekRange[0] && c.i <= weekRange[1] ? "var(--focus)" : "none")).attr("stroke-width", 1.5)
    .style("cursor", "pointer")
    .on("click", (e, c) => store.set({ weekRange: [c.i, c.i] }))
    .on("mousemove", (e, c) => showTip(e, `<div class="t">${store.areaName(ser.key.split("|")[0])}</div>
      Epi-week ${c.wk}, ${c.year} (from ${fmt.date(c.d)})<br>Cases: <b>${fmt.int(c.v)}</b>`))
    .on("mouseleave", hideTip);
  svg.append("g").selectAll("text").data(years).join("text").attr("x", M.l - 6).attr("y", (yr) => y(yr) + y.bandwidth() / 2)
    .attr("dy", "0.35em").attr("text-anchor", "end").text((yr) => yr);
  svg.append("g").selectAll("text").data([1, 10, 20, 30, 40, 50]).join("text").attr("x", (wk) => x(wk) + cw / 2)
    .attr("y", H - 8).attr("text-anchor", "middle").text((wk) => `W${wk}`);
  svg.append("text").attr("x", M.l).attr("y", 11).text(`${store.data.meta.diseases[disease]} · ${store.areaName(ser.key.split("|")[0])}`);
  legendItems("#heat-legend", [
    { color: color.range()[0], label: "few cases" }, { color: color.range()[6], label: `≥ ${fmt.int(max)} cases/week` },
    { color: "var(--hatch-bg)", hatch: true, label: "suppressed (<5)" }, { color: "transparent", label: "outlined = selected period (click a cell to select)" },
  ]);
}
