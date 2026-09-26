// Shared helpers: formatting, tooltip, colours, suppression-aware arithmetic.
/* global d3 */
export const DISEASE_COLOR = { dengue: "var(--series-1)", diarrhoea: "var(--series-2)", ili: "var(--series-3)", malaria: "var(--series-4)" };
export const STATUS = {
  alert: { label: "Alert", icon: "▲" },
  verify_reporting: { label: "Verify reporting", icon: "◆" },
  watch: { label: "Watch", icon: "●" },
  normal: { label: "Normal", icon: "○" },
};

// Sequential one-hue blue ramp (step 100 -> 700 of the validated palette), light->dark = low->high.
const RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"];
export function seqScale(max) {
  const dark = document.documentElement.dataset.theme === "dark" ||
    (!document.documentElement.dataset.theme && window.matchMedia("(prefers-color-scheme: dark)").matches);
  const r = dark ? [...RAMP].reverse() : RAMP;
  return d3.scaleQuantize().domain([0, max || 1]).range(r);
}

export const fmt = {
  int: (v) => (v === null || v === undefined ? "<5" : d3.format(",")(Math.round(v))),
  num: (v, p = 1) => (v === null || v === undefined || Number.isNaN(v) ? "–" : d3.format(`,.${p}f`)(v)),
  pct: (v) => (v === null || v === undefined ? "–" : d3.format(".0%")(v)),
  bdt: (v) => (v === null || v === undefined ? "–" : "৳" + d3.format(".3~s")(v).replace("G", "B")),
  date: (s) => (s ? d3.timeFormat("%d %b %Y")(new Date(s)) : "–"),
};
// A count cell is either a number or null (= suppressed, 1-4 cases).
export const isSupp = (v) => v === null;

const tip = () => document.getElementById("tooltip");
export function showTip(event, html) {
  const t = tip();
  t.innerHTML = html;
  t.hidden = false;
  const pad = 14;
  const { innerWidth: W, innerHeight: H } = window;
  const r = t.getBoundingClientRect();
  let x = event.clientX + pad, y = event.clientY + pad;
  if (x + r.width > W - 8) x = event.clientX - r.width - pad;
  if (y + r.height > H - 8) y = event.clientY - r.height - pad;
  t.style.left = `${Math.max(4, x)}px`;
  t.style.top = `${Math.max(4, y)}px`;
}
export function hideTip() { tip().hidden = true; }

export function svgIn(sel, width, height) {
  const root = d3.select(sel);
  root.selectAll("*").remove();
  return root.append("svg").attr("viewBox", `0 0 ${width} ${height}`).attr("role", "img");
}

export function legendItems(sel, items) {
  const el = d3.select(sel);
  el.selectAll("*").remove();
  items.forEach((it) => {
    const s = el.append("span");
    if (it.line) s.append("span").attr("class", "ln").style("border-color", it.color).style("border-top-style", it.dash ? "dashed" : "solid");
    else s.append("span").attr("class", "sw").style("background", it.color).style("opacity", it.opacity ?? 1)
      .style("background-image", it.hatch ? "repeating-linear-gradient(45deg, var(--hatch-line) 0 2px, transparent 2px 5px)" : null);
    s.append("span").text(it.label);
  });
}

export function freshness(meta) {
  document.querySelectorAll("[data-fresh]").forEach((el) => {
    const k = el.dataset.fresh;
    const labels = { cases: "Cases reported up to", stock: "Stock snapshot", forecast_origin: "Forecast from week of", detection: "Signals computed on" };
    el.textContent = `${labels[k] || "Data"} ${fmt.date(meta.freshness[k])}`;
  });
}
