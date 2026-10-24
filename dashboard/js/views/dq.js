// Data-quality report (DQ-2): rows in/out and quarantined per load, and pass rate of every rule.
/* global d3 */
import { fmt } from "../util.js";

export function init() {}

export function update(store) {
  const dq = store.data.dq;
  const root = d3.select("#dq");
  root.selectAll("*").remove();
  const loads = dq.loads.slice(-12).reverse();
  const t = root.append("div").attr("class", "table-wrap").append("table");
  t.append("thead").append("tr").selectAll("th").data(["Load (UTC)", "Feed", "Rows in", "Kept", "Quarantined", "Duplicates removed"])
    .join("th").attr("class", (d, i) => (i > 1 ? "num" : null)).text((d) => d);
  t.append("tbody").selectAll("tr").data(loads).join("tr").selectAll("td")
    .data((r) => [r.load_ts.replace("T", " "), r.source, fmt.int(r.rows_in), fmt.int(r.rows_out), fmt.int(r.rows_quarantined), fmt.int(r.duplicates_removed)])
    .join("td").attr("class", (d, i) => (i > 1 ? "num" : null)).text((d) => d);
  const last = dq.loads.length ? dq.loads[dq.loads.length - 1].batch_id : null;
  const rules = dq.rules.filter((r) => r.batch_id === last || !last);
  const first = dq.loads.length ? dq.loads[0].batch_id : null;
  const show = rules.length ? rules : dq.rules.filter((r) => r.batch_id === first);
  root.append("p").attr("class", "note").text(`Quality rules for the latest load (${show.length} rules). Failing rows go to quarantine with the rule ID as reason code.`);
  const t2 = root.append("div").attr("class", "table-wrap").append("table").attr("class", "dq-table");
  t2.append("thead").append("tr").selectAll("th").data(["Rule", "Checks that…", "Failed rows", "Pass rate", ""]).join("th")
    .attr("class", (d, i) => (i === 2 || i === 3 ? "num" : null)).text((d) => d);
  const tr = t2.append("tbody").selectAll("tr").data(show).join("tr");
  tr.append("td").text((r) => r.rule_id);
  tr.append("td").text((r) => r.description);
  tr.append("td").attr("class", "num").text((r) => fmt.int(r.failed_rows));
  tr.append("td").attr("class", "num").text((r) => `${d3.format(".3f")(100 * r.pass_rate)}%`);
  tr.append("td").attr("class", "bar").append("div").attr("class", "pass-bar")
    .style("width", (r) => `${Math.max(2, 100 * (r.pass_rate - 0.95) / 0.05)}%`).attr("title", "bar spans 95%–100%");
}
