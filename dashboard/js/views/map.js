// View 1: district choropleth of weekly incidence per 100,000 with alert markers (+ animated playback, VIS-7).
/* global d3, topojson */
import { fmt, hideTip, legendItems, seqScale, showTip, STATUS, svgIn } from "../util.js";
import { population, seriesFor, statusAt } from "./common.js";

let geo = null, svg, path, W = 520, H = 560, playing = null, playWeek = null;

export async function init(store) {
  if (!geo) {
    const topo = await (await fetch("data/geo/districts.topo.json")).json();
    geo = topojson.feature(topo, topo.objects[Object.keys(topo.objects)[0]]);
  }
  svg = svgIn("#map", W, H).attr("aria-label", "Map of Bangladesh districts coloured by incidence");
  const proj = d3.geoMercator().fitSize([W, H - 10], geo);
  path = d3.geoPath(proj);
  svg.append("g").attr("class", "districts").selectAll("path").data(geo.features).join("path")
    .attr("class", "district").attr("d", path).attr("tabindex", 0)
    .on("click", (e, f) => selectFeature(store, f))
    .on("keydown", (e, f) => { if (e.key === "Enter") selectFeature(store, f); })
    .on("mousemove", (e, f) => tooltip(store, e, f)).on("mouseleave", hideTip);
  svg.append("g").attr("class", "markers");
  d3.select("#btn-play").on("click", () => togglePlay(store));
  update(store);
}

function selectFeature(store, f) {
  const pc = f.properties.adm2_pcode;
  const m = store.data.meta;
  if (m.role.scope === "district" && pc !== m.role.district_pcode) {
    store.set({ area: f.properties.adm1_pcode });   // other districts only as their division aggregate
  } else {
    store.set({ area: store.state.area === pc ? "BD" : pc });
  }
}

function values(store) {
  const { disease } = store.state;
  const [a, b] = playWeek !== null ? [playWeek, playWeek] : store.state.weekRange;
  const n = b - a + 1;
  const out = new Map();
  geo.features.forEach((f) => {
    const pc = f.properties.adm2_pcode;
    const s = seriesFor(store, pc, disease);
    if (!s.values) return;
    const slice = s.values.slice(a, b + 1);
    const code = s.aggregate ? s.key.split("|")[0] : pc;
    const pop = population(store, code);
    const known = slice.filter((v) => v !== null);
    const suppressed = slice.length - known.length;
    out.set(pc, { rate: known.length ? (d3.sum(known) / n) / pop * 1e5 : null, suppressed, aggregate: s.aggregate, n,
      cases: d3.sum(known), code });
  });
  return out;
}

export function update(store) {
  if (!svg) return;
  const vals = values(store);
  const max = d3.quantile([...vals.values()].map((v) => v.rate).filter((v) => v !== null).sort(d3.ascending), 0.98) || 1;
  const color = seqScale(max);
  svg.selectAll("path.district")
    .attr("fill", (f) => {
      const v = vals.get(f.properties.adm2_pcode);
      if (!v || v.rate === null) return "url(#hatch)";
      return color(Math.min(v.rate, max));
    })
    .classed("aggregate", (f) => vals.get(f.properties.adm2_pcode)?.aggregate)
    .classed("selected", (f) => f.properties.adm2_pcode === store.state.area || f.properties.adm1_pcode === store.state.area);
  // hatch overlay for areas whose period includes suppressed weeks
  const over = svg.selectAll("path.hatch-over").data(geo.features.filter((f) => {
    const v = vals.get(f.properties.adm2_pcode); return v && v.rate !== null && v.suppressed > 0;
  }), (f) => f.properties.adm2_pcode);
  over.join("path").attr("class", "hatch-over").attr("d", path).attr("fill", "url(#hatch)").attr("opacity", 0.3).attr("pointer-events", "none");
  // alert markers from the ranked table (status recomputed at the what-if threshold)
  const rows = store.data.alerts.table.filter((r) => r.disease_group === store.state.disease);
  const alerts = rows.map((r) => ({ ...r, st: statusAt(store, r) })).filter((r) => r.st === "alert" || r.st === "verify_reporting");
  const cent = new Map(store.data.meta.districts.map((d) => [d.adm2_pcode, [d.center_lon, d.center_lat]]));
  const proj = path.projection();
  const g = svg.select("g.markers").selectAll("g.mk").data(playWeek === null ? alerts : [], (d) => d.adm2_pcode);
  const enter = g.enter().append("g").attr("class", "mk");
  enter.append("circle").attr("class", "marker").attr("r", 8);
  enter.append("text").attr("text-anchor", "middle").attr("dy", "0.35em").style("font-size", "9px").style("fill", "#fff").style("font-weight", 700);
  g.exit().remove();
  svg.select("g.markers").selectAll("g.mk")
    .attr("transform", (d) => `translate(${proj(cent.get(d.adm2_pcode))})`)
    .each(function (d) {
      d3.select(this).select("circle").attr("fill", d.st === "alert" ? "var(--critical)" : "var(--serious)");
      d3.select(this).select("text").text(STATUS[d.st].icon);
    });
  const weeks = store.data.weekly.weeks;
  const [a, b] = playWeek !== null ? [playWeek, playWeek] : store.state.weekRange;
  // say so when nothing is in alert, so an empty map is not mistaken for missing markers
  const quiet = playWeek === null && !alerts.length
    ? ` · no ${store.data.meta.diseases[store.state.disease].toLowerCase()} alerts on ${fmt.date(store.data.alerts.as_of)} at threshold ${store.state.thr}` : "";
  d3.select("#map-caption").text(`${store.data.meta.diseases[store.state.disease]} · mean weekly incidence, ${fmt.date(weeks[a])} – ${fmt.date(d3.timeDay.offset(new Date(weeks[b]), 6))}${quiet}`);
  const lg = d3.select("#map-legend");
  lg.selectAll("*").remove();
  const steps = color.range();
  const th = color.thresholds();
  steps.forEach((c, i) => {
    const s = lg.append("span");
    s.append("span").attr("class", "sw").style("background", c);
    s.append("span").text(i === 0 ? `< ${d3.format(".2~g")(th[0])}` : i === steps.length - 1 ? `≥ ${d3.format(".2~g")(th[i - 1])} per 100k/wk` : "");
  });
  legendItems(lg.append("span").attr("class", "legend").style("margin-top", "0").node(), [
    { color: "var(--hatch-bg)", hatch: true, label: "includes suppressed weeks (<5)" },
    { color: "var(--critical)", label: `${STATUS.alert.icon} alert` },
    { color: "var(--serious)", label: `${STATUS.verify_reporting.icon} verify reporting` },
  ]);
}

function tooltip(store, e, f) {
  const v = values(store).get(f.properties.adm2_pcode);
  const name = f.properties.adm2_name;
  if (!v) return showTip(e, `<div class="t">${name}</div>no data`);
  const row = store.data.alerts.table.find((r) => r.adm2_pcode === f.properties.adm2_pcode && r.disease_group === store.state.disease);
  const st = row ? STATUS[statusAt(store, row)] : null;
  showTip(e, `<div class="t">${name}${v.aggregate ? ` (shown as ${store.areaName(v.code)})` : ""}</div>
    Incidence: <b>${v.rate === null ? "suppressed" : fmt.num(v.rate, 2)}</b> per 100,000 per week<br>
    Cases in period: ${fmt.int(v.cases)}${v.suppressed ? ` + ${v.suppressed} suppressed week(s)` : ""}<br>
    ${st ? `Status: ${st.icon} ${st.label}` : ""}`);
}

function togglePlay(store) {
  const btn = d3.select("#btn-play");
  if (playing) {
    playing.stop(); playing = null; playWeek = null; btn.text("▶ Play spread"); update(store); return;
  }
  const n = store.data.weekly.weeks.length;
  let i = Math.max(0, n - 52);
  btn.text("■ Stop");
  playing = d3.interval(() => {
    playWeek = i;
    update(store);
    i += 1;
    if (i >= n) { playing.stop(); playing = null; playWeek = null; btn.text("▶ Play spread"); update(store); }
  }, 350);
}
