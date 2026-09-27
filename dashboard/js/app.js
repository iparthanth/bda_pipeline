// EpiScope dashboard entry point: authentication, data loading, global state and linked views (VIS-3).
/* global d3 */
import { freshness } from "./util.js";
import * as map from "./views/map.js";
import * as ts from "./views/timeseries.js";
import * as heat from "./views/heatmap.js";
import * as table from "./views/table.js";
import * as whatif from "./views/whatif.js";
import * as alloc from "./views/allocation.js";
import * as cal from "./views/calendar.js";
import * as seir from "./views/seir.js";
import * as dq from "./views/dq.js";
import * as evaluation from "./views/evaluation.js";

const PARTS = ["meta", "weekly", "forecasts", "alerts", "thresholds", "allocation", "dq", "calendar", "seir", "evaluation"];
const VIEWS = [map, ts, heat, table, whatif, alloc, cal, seir, dq, evaluation];

function storage(fn, fallback = null) { try { return fn(); } catch { return fallback; } }

// Demo mode (server default): the local server lists its roles so the dashboard can sign in as national
// automatically and offer a "View as" switcher. With `--secure` the endpoint is off and a token is required.
let demoRoles = [];
async function loadDemoRoles() {
  try {
    const r = await fetch("/api/demo-roles");
    demoRoles = r.ok ? await r.json() : [];
  } catch { demoRoles = []; }
}

async function getToken() {
  const m = location.hash.match(/token=([^&]+)/);
  if (m) {
    storage(() => sessionStorage.setItem("episcope_token", m[1]));
    history.replaceState(null, "", location.pathname);  // do not leave the token in the address bar
    return m[1];
  }
  const saved = storage(() => sessionStorage.getItem("episcope_token"));
  if (saved && (!demoRoles.length || demoRoles.some((r) => r.token === saved))) return saved;
  return demoRoles.length ? demoRoles[0].token : null;  // national first
}

function roleSwitcher(token) {
  const sel = document.getElementById("sel-role");
  if (!demoRoles.length) { sel.parentElement.hidden = true; return; }
  sel.parentElement.hidden = false;
  const label = (r) => (r.scope === "national" ? "National (all districts)"
    : `District officer - ${store.areaName(r.district_pcode)}`);
  d3.select(sel).selectAll("option").data(demoRoles).join("option")
    .attr("value", (r) => r.token).text(label).property("selected", (r) => r.token === token);
  sel.onchange = (e) => {
    storage(() => sessionStorage.setItem("episcope_token", e.target.value));
    location.reload();
  };
}

async function api(name, token) {
  const r = await fetch(`/api/data/${name}.json`, { headers: { Authorization: `Bearer ${token}` } });
  if (r.status === 401) throw new Error("unauthorised");
  if (!r.ok) throw new Error(`${name}: HTTP ${r.status}`);
  return r.json();
}

export const store = {
  data: null,
  state: { disease: "dengue", area: "BD", resource: "iv_saline", weekRange: null, thr: null, budget: null, alpha: null },
  bus: d3.dispatch("change"),
  set(patch) {
    Object.assign(this.state, patch);
    this.bus.call("change", null, this.state);
  },
  // Which series key represents the current area for the current disease?
  key(area = this.state.area, disease = this.state.disease) { return `${area}|${disease}`; },
  areaName(code) {
    const m = this.data.meta;
    if (code === "BD") return "Bangladesh (national)";
    const d = m.districts.find((x) => x.adm2_pcode === code);
    if (d) return d.adm2_name;
    const v = m.divisions.find((x) => x.adm1_pcode === code);
    return v ? `${v.adm1_name} division` : code;
  },
};

async function start() {
  await loadDemoRoles();
  const token = await getToken();
  if (!token) return showLogin();
  let data;
  try {
    const parts = await Promise.all(PARTS.map((p) => api(p, token)));
    data = Object.fromEntries(PARTS.map((p, i) => [p, parts[i]]));
  } catch (e) {
    storage(() => sessionStorage.removeItem("episcope_token"));
    return showLogin(e.message === "unauthorised" ? "Token not recognised." : e.message);
  }
  store.data = data;
  const m = data.meta;
  const role = m.role;
  document.getElementById("session").textContent = role.scope === "national"
    ? "Role: national (all districts in detail)"
    : `Role: district officer - ${store.areaName(role.district_pcode)} in detail, other areas aggregated`;
  const synth = Object.entries(m.provenance).filter(([, s]) => s === "synthetic_fallback").map(([k]) => k.split("_")[0]);
  document.getElementById("banner").innerHTML = `<strong>${m.synthetic_notice}</strong>` +
    (synth.length ? ` Public sources replaced by labelled synthetic fallback in this build: ${synth.join(", ")}.` : " Public sources: real (see provenance).") +
    (m.weather_observed_until ? ` Weather is observed (NOAA GSOD stations) to ${m.weather_observed_until}; later days use station climatology.` : "") +
    ` Small counts (1-4) are shown as “&lt;5”.`;
  freshness(m);
  const w = data.weekly.weeks.length;
  const sc = data.allocation.scenarios;
  const op = m.operating_point;
  // the newest week is incomplete when the data end before its Sunday (e.g. 3 of 7 days) - leave it out of
  // the default period and the plotted series so it is not read as a sudden fall in cases
  store.lastFull = d3.timeDay.offset(new Date(data.weekly.weeks[w - 1]), 6) > new Date(m.freshness.cases) ? w - 2 : w - 1;
  store.state.weekRange = [store.lastFull - 7, store.lastFull];
  store.state.thr = sc.thresholds.includes(op.threshold) ? op.threshold : sc.thresholds[0];
  store.state.budget = sc.budget_levels.includes(0.6) ? 0.6 : sc.budget_levels[Math.floor(sc.budget_levels.length / 2)];
  const rec = data.allocation.summary.alpha;
  store.state.alpha = sc.alphas.reduce((a, b) => (Math.abs(b - rec) < Math.abs(a - rec) ? b : a));
  if (role.scope === "district") store.state.area = role.district_pcode;
  // open the allocation view on the resource the plan actually ships most of (by value)
  const shipped = d3.rollup(data.allocation.rows, (v) => d3.sum(v, (r) => r.recommended), (r) => r.resource);
  if (shipped.size) store.state.resource = [...shipped.entries()].sort((a, b) => b[1] - a[1])[0][0];
  controls(data);
  roleSwitcher(token);
  document.getElementById("app").hidden = false;
  VIEWS.forEach((v) => v.init(store));
  store.bus.on("change.views", (s) => { syncControls(s); VIEWS.forEach((v) => v.update(store)); });
  store.set({});
  window.addEventListener("resize", debounce(() => VIEWS.forEach((v) => v.init(store)) || store.set({}), 250));
}

function controls(data) {
  const m = data.meta;
  const dsel = d3.select("#sel-disease").on("change", (e) => store.set({ disease: e.target.value }));
  dsel.selectAll("option").data(Object.entries(m.diseases)).join("option").attr("value", (d) => d[0]).text((d) => d[1]);
  const rsel = d3.select("#sel-resource").on("change", (e) => store.set({ resource: e.target.value }));
  rsel.selectAll("option").data(Object.entries(m.resources)).join("option").attr("value", (d) => d[0]).text((d) => d[1]);
  const areas = [["BD", "Bangladesh (national)"]];
  m.divisions.forEach((v) => areas.push([v.adm1_pcode, `${v.adm1_name} division`]));
  const own = m.role.scope === "district" ? [m.role.district_pcode] : m.districts.map((d) => d.adm2_pcode);
  m.districts.filter((d) => own.includes(d.adm2_pcode)).sort((a, b) => a.adm2_name.localeCompare(b.adm2_name))
    .forEach((d) => areas.push([d.adm2_pcode, `  ${d.adm2_name}`]));
  const asel = d3.select("#sel-area").on("change", (e) => store.set({ area: e.target.value }));
  asel.selectAll("option").data(areas).join("option").attr("value", (d) => d[0]).text((d) => d[1]);
  d3.select("#btn-reset").on("click", () => {
    const w = data.weekly.weeks.length;
    store.set({ area: m.role.scope === "district" ? m.role.district_pcode : "BD", weekRange: [store.lastFull - 7, store.lastFull] });
  });
  d3.select("#btn-theme").on("click", () => {
    const root = document.documentElement;
    const dark = root.dataset.theme === "dark" || (!root.dataset.theme && matchMedia("(prefers-color-scheme: dark)").matches);
    root.dataset.theme = dark ? "light" : "dark";
    storage(() => localStorage.setItem("episcope_theme", root.dataset.theme));
    store.set({});
  });
}

function syncControls(s) {
  document.getElementById("sel-disease").value = s.disease;
  document.getElementById("sel-resource").value = s.resource;
  const asel = document.getElementById("sel-area");
  if ([...asel.options].some((o) => o.value === s.area)) asel.value = s.area;
}

function showLogin(msg = "") {
  document.getElementById("login").hidden = false;
  document.getElementById("login-error").textContent = msg;
  document.getElementById("login-form").onsubmit = (e) => {
    e.preventDefault();
    const t = document.getElementById("token").value.trim();
    storage(() => sessionStorage.setItem("episcope_token", t));
    location.reload();
  };
}

function debounce(fn, ms) { let t; return () => { clearTimeout(t); t = setTimeout(fn, ms); }; }

const savedTheme = storage(() => localStorage.getItem("episcope_theme"));
if (savedTheme) document.documentElement.dataset.theme = savedTheme;
start();
