"""Builds a self-contained HTML report from results/results.jsonl."""
import json
import os
import statistics
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "results", "results.jsonl")
DST = sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "results", "croc_benchmark_report.html")

SCEN_TITLE = {
    "01": "01 · Băng thông dual-relay (dynamic vs parity)",
    "02": "02 · Failover: tắt relay B khi đã nhận 30%",
    "03": "03 · Relay B qua Cloudflare Quick Tunnel thật",
    "04": "04 · Fan-out 1→4, buffer relay 4 MiB",
    "05": "05 · Fan-out: kick receiver chậm + resume 1-1",
    "06": "06 · Dual-relay + fan-out + tắt relay B + kick",
}

rows = [json.loads(l) for l in open(SRC, encoding="utf-8") if l.strip()]
ok_rows = [r for r in rows if "error" not in r]
err_rows = [r for r in rows if "error" in r]


def fast_receivers(r):
    """Receivers expected to finish (the kicked slow receiver r3 is excluded in 05/06)."""
    rs = r["receivers"]
    if r["scenario"] in ("05", "06"):
        return {k: v for k, v in rs.items() if k != "r3"}
    return rs


def agg(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return {"mean": statistics.fmean(vals), "min": min(vals), "max": max(vals), "n": len(vals),
            "sd": statistics.pstdev(vals) if len(vals) > 1 else 0.0}


def worst(r, key, fn=min):
    vals = [v.get(key) for v in fast_receivers(r).values() if v.get(key) is not None]
    return fn(vals) if vals else None


METRICS = [
    # id, label, unit, extractor, higher_is_better
    ("goodput", "Goodput (receiver chậm nhất)", "MB/s", lambda r: worst(r, "goodput_MBps", min), True),
    ("p50", "Throughput p50 (mẫu 250 ms)", "MB/s", lambda r: worst(r, "tput_p50", min), True),
    ("p95", "Throughput p95", "MB/s", lambda r: worst(r, "tput_p95", min), True),
    ("p99", "Throughput p99", "MB/s", lambda r: worst(r, "tput_p99", min), True),
    ("p5", "Throughput p5 (đuôi thấp)", "MB/s", lambda r: worst(r, "tput_p5", min), True),
    ("total", "Tổng thời gian (receiver chậm nhất)", "s", lambda r: (worst(r, "total_ms", max) or 0) / 1000 or None, False),
    ("handshake", "Thời gian tới byte đầu (handshake)", "ms", lambda r: worst(r, "handshake_ms", max), False),
    ("rttA", "TCP RTT sender→relay A (ss)", "ms", lambda r: r.get("rtt_relay-a_mean_ms"), False),
    ("rttB", "TCP RTT sender→relay B (ss)", "ms", lambda r: r.get("rtt_relay-b_mean_ms"), False),
    ("retrans", "TCP retransmits (sender, A+B)", "gói", lambda r: (r.get("tcp_retrans_relay-a") or 0) + (r.get("tcp_retrans_relay-b") or 0), False),
    ("overhead", "Byte relay gửi / byte payload", "×", lambda r: r.get("relay_bytes_overhead"), False),
    ("shareB", "Tỉ lệ byte qua relay B", "%", lambda r: (r.get("relay_b_share") or 0) * 100, None),
    ("rssA", "RSS đỉnh relay A", "MiB", lambda r: r.get("relay-a", {}).get("rss_peak_MiB"), False),
    ("rssB", "RSS đỉnh relay B", "MiB", lambda r: r.get("relay-b", {}).get("rss_peak_MiB"), False),
    ("cpuA", "CPU relay A", "% 1 core", lambda r: r.get("relay-a", {}).get("cpu_pct"), False),
    ("cpuB", "CPU relay B", "% 1 core", lambda r: r.get("relay-b", {}).get("cpu_pct"), False),
    ("cpuS", "CPU sender", "% 1 core", lambda r: r.get("sender", {}).get("cpu_pct"), False),
    ("rssS", "RSS đỉnh sender", "MiB", lambda r: r.get("sender", {}).get("rss_peak_MiB"), False),
    ("skA", "Socket send-buffer đỉnh relay A (kernel)", "MiB", lambda r: r.get("relay-a", {}).get("sock_wmem_peak_MiB"), False),
    ("skB", "Socket send-buffer đỉnh relay B (kernel)", "MiB", lambda r: r.get("relay-b", {}).get("sock_wmem_peak_MiB"), False),
    ("skS", "Socket send-buffer đỉnh sender (kernel)", "MiB", lambda r: r.get("sender", {}).get("sock_wmem_peak_MiB"), False),
    ("stall", "Failover: thời gian đứng hình", "ms", lambda r: worst(r, "failover_stall_ms", max), False),
    ("recovery", "Failover: thời gian hồi tốc độ ≥50%", "ms", lambda r: worst(r, "failover_recovery_ms", max), False),
    ("spread", "Fan-out: chênh lệch hoàn tất giữa receiver", "s",
     lambda r: (lambda v: (max(v) - min(v)) / 1000 if len(v) > 1 else None)(
         [x.get("total_ms") for x in fast_receivers(r).values() if x.get("total_ms")]), False),
    ("kick", "Fan-out: thời điểm r3 bị kick", "s",
     lambda r: (r["receivers"].get("r3", {}).get("total_ms") or 0) / 1000 or None if r["scenario"] in ("05", "06") else None, None),
    ("resume", "Resume 1-1 của r3", "s", lambda r: (r.get("resume") or {}).get("resume_ms", 0) / 1000 or None, False),
]

SCEN_METRICS = {
    "01": ["goodput", "p50", "p95", "p99", "p5", "total", "handshake", "rttA", "rttB", "retrans", "overhead", "shareB",
           "rssA", "rssB", "cpuA", "cpuB", "cpuS", "rssS", "skA", "skB", "skS"],
    "02": ["goodput", "p50", "p95", "p5", "total", "stall", "recovery", "rttA", "retrans", "overhead", "shareB",
           "rssA", "rssB", "cpuS", "skS"],
    "03": ["goodput", "p50", "p95", "p99", "p5", "total", "handshake", "rttA", "rttB", "retrans", "overhead", "shareB",
           "rssA", "rssB", "cpuS"],
    "04": ["goodput", "p50", "p95", "p5", "total", "handshake", "spread", "rssA", "cpuA", "skA", "cpuS", "rssS"],
    "05": ["goodput", "p50", "p95", "total", "kick", "resume", "spread", "rssA", "cpuA", "skA"],
    "06": ["goodput", "p50", "p95", "p5", "total", "stall", "recovery", "kick", "spread", "shareB", "rssA", "rssB", "cpuS"],
}
MBY = {m[0]: m for m in METRICS}

grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))  # case -> metric -> (ver,size) -> vals
cases = {}
sizes = sorted({r["size_MiB"] for r in ok_rows})
series_5g = defaultdict(dict)
checks = defaultdict(lambda: {"runs": 0, "sha_fail": 0, "fallback": 0})
for r in ok_rows:
    key = (r["scenario"], r["case"])
    cases[key] = True
    run_failed = any(not v.get("sha_ok") for v in fast_receivers(r).values())
    for mid, *_ in METRICS:
        if run_failed and mid not in ("kick", "resume"):
            continue  # a transfer that did not deliver the file has no meaningful performance numbers
        try:
            v = MBY[mid][3](r)
        except Exception:
            v = None
        grouped[key][mid][(r["version"], r["size_MiB"])].append(v)
    c = checks[(key, r["version"])]
    c["runs"] += 1
    c["sha_fail"] += sum(1 for v in fast_receivers(r).values() if not v.get("sha_ok"))
    c["fallback"] += 1 if r.get("fallback_warn") else 0
    if r["size_MiB"] == max(sizes) and r.get("rep", 0) == 0:
        rr = fast_receivers(r)
        if rr:
            k0 = sorted(rr)[0]
            series_5g[key][r["version"]] = rr[k0].get("rate_series", [])

data = {"sizes": sizes, "cases": [], "metrics": {m[0]: {"label": m[1], "unit": m[2], "hib": m[4]} for m in METRICS}}
for key in sorted(cases):
    sc, name = key
    entry = {"scenario": sc, "case": name, "metrics": {}, "series": series_5g.get(key, {}),
             "checks": {v: checks[(key, v)] for v in ("base", "fix")}}
    for mid in SCEN_METRICS.get(sc, []):
        vals = {}
        for ver in ("base", "fix"):
            vals[ver] = [agg(grouped[key][mid].get((ver, s), [])) for s in sizes]
        if any(a for v in vals.values() for a in v):
            entry["metrics"][mid] = vals
    data["cases"].append(entry)
data["errors"] = [{k: e[k] for k in ("scenario", "case", "version", "size_MiB", "error")} for e in err_rows]
data["n_runs"] = len(ok_rows)

HTML = r"""<!doctype html>
<html lang="vi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>croc dual-relay benchmark</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
:root { color-scheme: light;
  --surface-0:#f7f7f5; --surface-1:#ffffff; --border:#e3e3e0; --text-primary:#191919; --text-secondary:#666666;
  --grid:#ececea; --series-1:#257de0; --series-2:#f47230; --good:#008a17; --bad:#c62f30; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) { color-scheme: dark;
  --surface-0:#121212; --surface-1:#191919; --border:#2e2e2e; --text-primary:#efefef; --text-secondary:#9c9c9c;
  --grid:#2a2a2a; --series-1:#338ced; --series-2:#e76624; --good:#0aac23; --bad:#eb6263; } }
:root[data-theme="dark"] { color-scheme: dark;
  --surface-0:#121212; --surface-1:#191919; --border:#2e2e2e; --text-primary:#efefef; --text-secondary:#9c9c9c;
  --grid:#2a2a2a; --series-1:#338ced; --series-2:#e76624; --good:#0aac23; --bad:#eb6263; }
* { box-sizing: border-box; }
body { margin:0; background:var(--surface-0); color:var(--text-primary);
  font:14px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif; }
main { max-width:1280px; margin:0 auto; padding:24px 16px 64px; }
h1 { font-size:22px; margin:0 0 4px; } h2 { font-size:18px; margin:40px 0 8px; } h3 { font-size:15px; margin:20px 0 8px; }
p, li { color:var(--text-secondary); } .muted { color:var(--text-secondary); }
.legend { display:flex; gap:20px; align-items:center; margin:12px 0; flex-wrap:wrap; }
.sw { display:inline-block; width:14px; height:3px; border-radius:2px; vertical-align:middle; margin-right:6px; }
.grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:12px; }
.card { background:var(--surface-1); border:1px solid var(--border); border-radius:10px; padding:12px; min-width:0; }
.card h4 { margin:0 0 6px; font-size:13px; font-weight:600; }
.card .unit { font-weight:400; color:var(--text-secondary); }
.chartbox { position:relative; height:180px; }
.wide .chartbox { height:260px; }
table { border-collapse:collapse; width:100%; font-variant-numeric:tabular-nums; font-size:13px; }
th, td { border-bottom:1px solid var(--border); padding:6px 8px; text-align:right; white-space:nowrap; }
th:first-child, td:first-child, th:nth-child(2), td:nth-child(2) { text-align:left; }
.tablewrap { overflow-x:auto; background:var(--surface-1); border:1px solid var(--border); border-radius:10px; }
.up { color:var(--good); } .down { color:var(--bad); }
details { margin-top:8px; } summary { cursor:pointer; color:var(--text-secondary); }
.toolbar { display:flex; gap:8px; flex-wrap:wrap; margin:16px 0; }
.toolbar button { background:var(--surface-1); color:var(--text-primary); border:1px solid var(--border);
  border-radius:999px; padding:4px 12px; cursor:pointer; font:inherit; }
.toolbar button[aria-pressed="true"] { border-color:var(--text-primary); }
</style>
</head>
<body>
<main>
<h1>croc dual-relay: bản cũ vs bản mới</h1>
<p>Docker trên Windows (Docker Desktop, 12 CPU, 8 GB). Băng thông <code>tc</code> của các kịch bản gốc được giữ tỉ lệ và nâng trần lên <b>1 Gbit/s</b>
(12→500, 24→1000, 6→250, 20→833 Mbit/s; receiver chậm giữ 600 kbit/s để vẫn bị kick). Mỗi điểm là trung bình các lần lặp (3 lần cho ≤1 GiB, 1 lần cho 2–5 GiB và kịch bản 03);
di chuột để xem min/max và số lần chạy. <span id="nruns"></span></p>
<div class="legend">
  <span><span class="sw" style="background:var(--series-1)"></span>Bản cũ — <code>58a317db</code> (origin/main)</span>
  <span><span class="sw" style="background:var(--series-2)"></span>Bản mới — <code>18a1b529</code> + vá cửa sổ BDP</span>
</div>
<div class="toolbar" id="toolbar"></div>
<h2>Tóm tắt goodput (receiver chậm nhất)</h2>
<p>Đơn vị MB/s. Lần chạy mà receiver chính không nhận đủ file (SHA lỗi) bị loại khỏi số liệu hiệu năng và được đếm ở cột “SHA lỗi”.</p>
<div class="tablewrap"><table id="summary"></table></div>
<div id="cases"></div>
<h2>Lỗi khi chạy</h2><div id="errors" class="muted"></div>
</main>
<script>
const DATA = __DATA__;
const css = n => getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const fmt = (v, u) => v == null ? "—" : (Math.abs(v) >= 100 ? v.toFixed(0) : Math.abs(v) >= 10 ? v.toFixed(1) : v.toFixed(2)) + (u ? " " + u : "");
const sizeLabel = s => s >= 1024 ? (s / 1024) + " GiB" : s + " MiB";
const SC = { "01":"01 · Băng thông dual-relay", "02":"02 · Failover relay B @30%", "03":"03 · Cloudflare Quick Tunnel thật",
  "04":"04 · Fan-out 1→4, buffer 4 MiB", "05":"05 · Kick receiver chậm + resume", "06":"06 · Dual-relay + fan-out + kill B + kick" };
document.getElementById("nruns").textContent = DATA.n_runs + " lần chạy hợp lệ.";

// Summary table
const sum = document.getElementById("summary");
let head = "<tr><th>Kịch bản</th><th>Trường hợp</th>";
DATA.sizes.forEach(s => head += `<th>${sizeLabel(s)} cũ</th><th>${sizeLabel(s)} mới</th><th>Δ</th>`);
head += "<th>SHA lỗi (cũ/mới)</th><th>Rơi về 1 relay (cũ/mới)</th></tr>";
let body = "";
DATA.cases.forEach(c => {
  const g = c.metrics.goodput; if (!g) return;
  body += `<tr><td>${c.scenario}</td><td>${c.case}</td>`;
  DATA.sizes.forEach((s, i) => {
    const b = g.base[i], f = g.fix[i];
    const d = b && f ? (f.mean / b.mean - 1) * 100 : null;
    body += `<td>${b ? fmt(b.mean) : "—"}</td><td>${f ? fmt(f.mean) : "—"}</td>` +
      `<td class="${d == null ? "" : d >= 3 ? "up" : d <= -3 ? "down" : ""}">${d == null ? "—" : (d > 0 ? "+" : "") + d.toFixed(0) + "%"}</td>`;
  });
  body += `<td>${c.checks.base.sha_fail}/${c.checks.fix.sha_fail}</td><td>${c.checks.base.fallback}/${c.checks.fix.fallback}</td></tr>`;
});
sum.innerHTML = head + body;

// Per-case sections
const root = document.getElementById("cases");
const charts = [];
const scenarios = [...new Set(DATA.cases.map(c => c.scenario))];
const toolbar = document.getElementById("toolbar");
const mkBtn = (label, val) => { const b = document.createElement("button"); b.textContent = label; b.dataset.v = val;
  b.setAttribute("aria-pressed", val === "all"); b.onclick = () => { toolbar.querySelectorAll("button").forEach(x => x.setAttribute("aria-pressed", x === b));
  document.querySelectorAll("section[data-sc]").forEach(s => s.hidden = !(val === "all" || s.dataset.sc === val)); }; toolbar.appendChild(b); };
mkBtn("Tất cả", "all"); scenarios.forEach(s => mkBtn(SC[s] || s, s));

function lineChart(canvas, labels, datasets, unit) {
  const grid = css("--grid"), txt = css("--text-secondary");
  return new Chart(canvas, { type: "line", data: { labels, datasets },
    options: { responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: "index", intersect: false },
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: ctx => { const a = ctx.dataset.agg ? ctx.dataset.agg[ctx.dataIndex] : null;
          return `${ctx.dataset.label}: ${fmt(ctx.parsed.y, unit)}` + (a && a.n > 1 ? ` (min ${fmt(a.min)}, max ${fmt(a.max)}, n=${a.n})` : a ? " (n=1)" : ""); } } } },
      scales: { x: { grid: { display: false }, ticks: { color: txt } },
        y: { beginAtZero: true, grid: { color: grid }, border: { display: false }, ticks: { color: txt, maxTicksLimit: 5, callback: v => fmt(v) } } } } });
}
const ds = (label, agg, color) => ({ label, data: agg.map(a => a ? a.mean : null), agg, borderColor: color, backgroundColor: color,
  borderWidth: 2, pointRadius: 4, pointHoverRadius: 6, spanGaps: true, tension: 0 });

DATA.cases.forEach((c, ci) => {
  let sec = root.querySelector(`section[data-sc="${c.scenario}"]`);
  if (!sec) { sec = document.createElement("section"); sec.dataset.sc = c.scenario;
    sec.innerHTML = `<h2>${SC[c.scenario] || c.scenario}</h2>`; root.appendChild(sec); }
  const h = document.createElement("h3"); h.textContent = c.case; sec.appendChild(h);
  const grid = document.createElement("div"); grid.className = "grid"; sec.appendChild(grid);
  const labels = DATA.sizes.map(sizeLabel);
  Object.entries(c.metrics).forEach(([mid, v]) => {
    const m = DATA.metrics[mid];
    const card = document.createElement("div"); card.className = "card";
    card.innerHTML = `<h4>${m.label} <span class="unit">(${m.unit}${m.hib === true ? ", cao hơn tốt hơn" : m.hib === false ? ", thấp hơn tốt hơn" : ""})</span></h4><div class="chartbox"><canvas></canvas></div>`;
    grid.appendChild(card);
    charts.push(() => lineChart(card.querySelector("canvas"), labels,
      [ds("Bản cũ", v.base, css("--series-1")), ds("Bản mới", v.fix, css("--series-2"))], m.unit));
  });
  if (c.series && (c.series.base || c.series.fix)) {
    const card = document.createElement("div"); card.className = "card wide"; card.style.gridColumn = "1 / -1";
    card.innerHTML = `<h4>Dữ liệu đã nhận theo thời gian — ${sizeLabel(Math.max(...DATA.sizes))}, lần chạy đầu <span class="unit">(MB, trục x = giây kể từ khi receiver khởi động)</span></h4><div class="chartbox"><canvas></canvas></div>`;
    grid.appendChild(card);
    charts.push(() => {
      const mk = (lab, pts, col) => ({ label: lab, data: (pts || []).map(p => ({ x: p[0], y: p[1] })), borderColor: col, backgroundColor: col,
        borderWidth: 2, pointRadius: 0, pointHoverRadius: 5, tension: 0 });
      const txt = css("--text-secondary"), gridc = css("--grid");
      return new Chart(card.querySelector("canvas"), { type: "line",
        data: { datasets: [mk("Bản cũ", c.series.base, css("--series-1")), mk("Bản mới", c.series.fix, css("--series-2"))] },
        options: { responsive: true, maintainAspectRatio: false, animation: false, parsing: false,
          interaction: { mode: "nearest", axis: "x", intersect: false },
          plugins: { legend: { display: true, labels: { color: txt, boxHeight: 2 } },
            tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${fmt(ctx.parsed.y, "MB")} tại ${ctx.parsed.x.toFixed(1)} s` } } },
          scales: { x: { type: "linear", grid: { display: false }, ticks: { color: txt } },
            y: { beginAtZero: true, grid: { color: gridc }, border: { display: false }, ticks: { color: txt } } } } });
    });
  }
  // table view
  const det = document.createElement("details");
  let t = "<summary>Bảng số liệu</summary><div class='tablewrap'><table><tr><th>Metric</th><th>Bản</th>" + DATA.sizes.map(s => `<th>${sizeLabel(s)}</th>`).join("") + "</tr>";
  Object.entries(c.metrics).forEach(([mid, v]) => ["base", "fix"].forEach(ver => {
    t += `<tr><td>${DATA.metrics[mid].label} (${DATA.metrics[mid].unit})</td><td>${ver === "base" ? "cũ" : "mới"}</td>` +
      v[ver].map(a => `<td>${a ? fmt(a.mean) + (a.n > 1 ? " ±" + fmt(a.sd) : "") : "—"}</td>`).join("") + "</tr>"; }));
  det.innerHTML = t + "</table></div>"; sec.appendChild(det);
});
charts.forEach(f => f());
const er = document.getElementById("errors");
er.innerHTML = DATA.errors.length ? "<ul>" + DATA.errors.map(e => `<li>${e.scenario} ${e.case} ${sizeLabel(e.size_MiB)} ${e.version}: ${e.error}</li>`).join("") + "</ul>" : "Không có.";
</script>
</body>
</html>
"""
os.makedirs(os.path.dirname(DST), exist_ok=True)
with open(DST, "w", encoding="utf-8") as f:
    f.write(HTML.replace("__DATA__", json.dumps(data)))
print(DST, len(ok_rows), "runs", len(err_rows), "errors")
