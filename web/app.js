/* Parquet 数据文件优化实验室 — 前端逻辑（无框架、无外部依赖） */
"use strict";

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => [...root.querySelectorAll(sel)];

const state = {
  options: null,
  manifest: null,
  metric: "file_size_bytes",
  sort: { key: "file_size_bytes", dir: 1 },
  querySeq: 0,
};
window.__parquetLab = { state, renderAll, renderTable, renderChart,
  renderReco, renderLineage, openDrawer, openFail, parseQueryParam };

/* ---------------- 工具 ---------------- */
const fmtInt = (n) => (n == null ? "—" : Number(n).toLocaleString("zh-CN"));
const fmtMs = (n) => (n == null ? "—" : `${Number(n).toFixed(2)}`);
const fmtBytes = (n) => {
  if (n == null) return "—";
  const units = ["B", "KB", "MB", "GB"];
  let v = n, u = 0;
  while (v >= 1024 && u < units.length - 1) { v /= 1024; u++; }
  return `${v.toFixed(u === 0 ? 0 : 2)} ${units[u]}`;
};
const esc = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) throw new Error(`${r.status} ${(await r.json().catch(() => ({}))).error || r.statusText}`);
  return r.json();
}

/* ---------------- 配置表单 ---------------- */
function renderOptions() {
  const o = state.options;
  $("#compressionBox").innerHTML = o.compressions.map((c, i) =>
    `<label><input type="checkbox" name="comp" value="${c}" ${i < 3 ? "checked" : ""}> ${c}</label>`).join("");
  $("#rowGroupBox").innerHTML = o.row_group_sizes.map((r, i) =>
    `<label><input type="checkbox" name="rg" value="${r}" ${[0, 50000].includes(r) ? "checked" : ""}> ${r === 0 ? "默认(单组)" : fmtInt(r) + " 行/组"}</label>`).join("");
  const profileLabel = {
    defaults: "全列字典（默认）", no_dictionary: "关闭字典/全 PLAIN",
    delta: "DELTA 整型+字节串", byte_stream_split: "BYTE_STREAM_SPLIT 数值",
    selective_dict: "仅低基数列字典", mixed: "混合（字典+delta+BSS）",
  };
  $("#profileBox").innerHTML = o.profiles.map((p) =>
    `<label><input type="checkbox" name="prof" value="${p}" checked> ${profileLabel[p] || p}</label>`).join("");
}

function queryRow(q = { qtype: "point", field: "category", repeats: 5 }) {
  const o = state.options;
  const id = `q${state.querySeq++}`;
  const fieldOptions = (type) => {
    const fields = type === "point" ? Object.keys(o.point_fields)
      : type === "range" ? o.range_fields : o.scan_fields;
    return fields.map((f) =>
      `<option value="${f}" ${f === q.field ? "selected" : ""}>${f}</option>`).join("");
  };
  const paramPlaceholder = (type) =>
    type === "point" ? "目标值（留空自动采样）"
    : type === "range" ? "下界,上界（留空取 25%–75% 分位）"
    : "子串（默认 warranty）";
  const paramValue = (type) => {
    if (type === "point") return q.needle ?? "";
    if (type === "range") return (q.lower != null && q.upper != null)
      ? `${q.lower},${q.upper}` : "";
    return q.substring && q.substring !== "warranty" ? q.substring : "";
  };
  const div = document.createElement("div");
  div.className = "query-row";
  div.dataset.id = id;
  div.innerHTML = `
    <select class="q-type">
      <option value="point" ${q.qtype === "point" ? "selected" : ""}>点查 =</option>
      <option value="range" ${q.qtype === "range" ? "selected" : ""}>范围聚合</option>
      <option value="scan" ${q.qtype === "scan" ? "selected" : ""}>文本扫描</option>
    </select>
    <select class="q-field">${fieldOptions(q.qtype)}</select>
    <input type="text" class="q-param" placeholder="${paramPlaceholder(q.qtype)}"
      value="${esc(paramValue(q.qtype))}" title="查询参数">
    <input type="number" class="q-repeats" value="${q.repeats}" min="1" max="31" title="重复次数">
    <button class="btn ghost small q-del" type="button">删</button>`;
  div.querySelector(".q-type").addEventListener("change", (e) => {
    const type = e.target.value;
    div.querySelector(".q-field").innerHTML = fieldOptions(type);
    div.querySelector(".q-param").placeholder = paramPlaceholder(type);
    div.querySelector(".q-param").value = "";
  });
  div.querySelector(".q-del").addEventListener("click", () => div.remove());
  return div;
}

function parseQueryParam(qtype, raw) {
  const v = (raw || "").trim();
  if (!v) return {};
  if (qtype === "point") return { needle: isNaN(Number(v)) ? v : Number(v) };
  if (qtype === "scan") return { substring: v };
  // range: "lower,upper"
  const parts = v.split(",").map((x) => x.trim());
  if (parts.length !== 2 || parts.some((x) => x === "")) {
    throw new Error("范围参数需为 '下界,上界' 形式");
  }
  const conv = (x) => {
    const n = Number(x);
    return Number.isFinite(n) ? n : x;  // 时间字符串保留原样，由 Arrow 转换
  };
  return { lower: conv(parts[0]), upper: conv(parts[1]) };
}

function collectConfig() {
  const checked = (name) => $$(`input[name="${name}"]:checked`).map((e) => e.value);
  const queries = $$(".query-row").map((row) => {
    const qtype = $(".q-type", row).value;
    return {
      qtype,
      field: $(".q-field", row).value,
      repeats: Math.max(1, parseInt($(".q-repeats", row).value, 10) || 5),
      ...parseQueryParam(qtype, $(".q-param", row).value),
    };
  });
  return {
    matrix_mode: "default",
    dataset: {
      rows: parseInt($("#dsRows").value, 10),
      seed: parseInt($("#dsSeed").value, 10),
      region_null_pct: parseFloat($("#nullRegion").value),
      remark_null_pct: parseFloat($("#nullRemark").value),
      event_null_pct: parseFloat($("#nullEvent").value),
    },
    compressions: checked("comp"),
    row_group_sizes: checked("rg").map(Number),
    profiles: checked("prof"),
    add_sorted: $("#addSorted").checked,
    queries,
  };
}

/* ---------------- 运行 ---------------- */
async function startRun() {
  let cfg;
  try {
    cfg = collectConfig();
  } catch (e) {
    setStatus(e.message, true);
    return;
  }
  if (!cfg.compressions.length || !cfg.profiles.length || !cfg.row_group_sizes.length) {
    setStatus("请至少选择一个压缩算法、编码 profile 与行组大小", true);
    return;
  }
  if (!cfg.queries.length) { setStatus("请至少配置一条查询", true); return; }
  for (const q of cfg.queries) {
    if (q.qtype === "range" && q.lower != null && Number.isNaN(Number(q.lower))
        && !/^\d{4}-\d{2}-\d{2}T/.test(q.lower)) {
      setStatus(`范围查询 ${q.field} 的下界格式无法识别（数字或 ISO 时间）`, true);
      return;
    }
  }

  $("#runBtn").disabled = true;
  setStatus("实验运行中：生成数据 → 逐方案写入与读回核对 → 查询基准…");
  try {
    const { job_id } = await api("/api/runs", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify(cfg),
    });
    pollJob(job_id);
  } catch (e) {
    setStatus(`启动失败：${e.message}`, true);
    $("#runBtn").disabled = false;
  }
}

function pollJob(jobId) {
  const timer = setInterval(async () => {
    try {
      const job = await api(`/api/runs/${jobId}`);
      if (job.status === "running") return;
      clearInterval(timer);
      $("#runBtn").disabled = false;
      if (job.status === "error") { setStatus(`实验出错：${job.error}`, true); return; }
      state.manifest = job.manifest;
      setStatus(`实验完成：run_id=${job.run_id}（共 ${state.manifest.schemes.length} 个方案）`);
      renderAll();
    } catch (e) {
      clearInterval(timer);
      $("#runBtn").disabled = false;
      setStatus(`轮询失败：${e.message}`, true);
    }
  }, 1200);
}
function setStatus(text, bad = false) {
  const el = $("#runStatus");
  el.textContent = text;
  el.style.color = bad ? "var(--critical)" : "var(--text-secondary)";
}

/* ---------------- 渲染 ---------------- */
function okSchemes() {
  return state.manifest.schemes.filter((s) => s.status === "ok");
}
function qMedian(s, qtype, field) {
  const q = (s.queries || []).find((x) =>
    x.qtype === qtype && (field ? x.field === field : true));
  return q ? q.median_ms : null;
}
function metricValue(s, metric) {
  if (metric.startsWith("q:")) {
    const [, qtype, field] = metric.split(":");
    return qMedian(s, qtype, field);
  }
  return s[metric];
}
const BASE_METRICS = [
  ["file_size_bytes", "文件大小"],
  ["write_ms", "写入耗时"],
];
const METRIC_META = {
  file_size_bytes: { label: "文件大小", fmt: fmtBytes, lowerBetter: true },
  write_ms: { label: "写入耗时 (ms)", fmt: fmtMs, lowerBetter: true },
};
function metricLabel(metric) {
  if (metric.startsWith("q:")) {
    const [, qtype, field] = metric.split(":");
    const t = { point: "点查", range: "范围聚合", scan: "文本扫描" }[qtype];
    return `${t} ${field} 中位 (ms)`;
  }
  return METRIC_META[metric].label;
}
function metricFmt(metric) {
  return metric === "file_size_bytes" ? fmtBytes : fmtMs;
}

/* 根据本次实验的查询配置重建指标切换条 */
function renderMetricSeg() {
  const qs = state.manifest.query_configs;
  const seg = $("#metricSeg");
  const buttons = [
    ...BASE_METRICS.map(([k, label]) => `<button data-metric="${k}">${label}</button>`),
    ...qs.map((q) => {
      const t = { point: "点查", range: "范围", scan: "扫描" }[q.qtype];
      return `<button data-metric="q:${q.qtype}:${q.field}">${t}·${q.field}</button>`;
    }),
  ];
  // 保留当前选中（若仍存在），否则默认文件大小
  const keep = seg.querySelector("button.active")?.dataset.metric;
  seg.innerHTML = buttons.join("");
  const chosen = (keep && seg.querySelector(`[data-metric="${keep}"]`))
    ? keep : "file_size_bytes";
  seg.querySelector(`[data-metric="${chosen}"]`).classList.add("active");
  state.metric = chosen;
}

function renderAll() {
  renderMetricSeg();
  renderLineage();
  renderReco();
  renderChart();
  renderTable();
  ["#lineageCard", "#recoCard", "#chartCard", "#tableCard"].forEach((id) =>
    $(id).classList.remove("hidden"));
}

function renderLineage() {
  const m = state.manifest;
  const d = m.dataset;
  const checks = okSchemes()[0]?.verify.checks || {};
  const failedCount = m.schemes.filter((s) => s.status !== "ok").length;
  $("#engineMeta").innerHTML =
    `pyarrow ${esc(m.engine.pyarrow)}<br>run ${esc(m.run_id)}`;
  $("#lineageBody").innerHTML = `
    <div class="lineage-grid">
      <div class="kv"><div class="k">输入行数</div><div class="v">${fmtInt(d.rows)}</div></div>
      <div class="kv"><div class="k">生成器参数指纹 (spec)</div><div class="v code">${esc(d.fingerprint)}</div></div>
      <div class="kv"><div class="k">数据内容指纹 (sha256 截断)</div><div class="v code">${esc(d.content_fingerprint)}</div></div>
      <div class="kv"><div class="k">随机种子 / 空值率</div>
        <div class="v">seed=${d.spec.seed} · region ${d.spec.region_null_pct} · remark ${d.spec.remark_null_pct} · event ${d.spec.event_null_pct}</div></div>
      <div class="kv"><div class="k">列数 / 源数据文件</div>
        <div class="v">${d.columns.length} 列 ·
          <a href="/download/${m.run_id}/data/_source.arrow" download>_source.arrow</a>
          · <a href="/download/${m.run_id}/data/_source_preview.json" download>预览 JSON</a></div></div>
      <div class="kv"><div class="k">方案统计</div>
        <div class="v">${okSchemes().length} 个通过 · ${failedCount} 个失败/不支持</div></div>
    </div>
    <div class="check-list">
      ${Object.entries(checks).map(([k, v]) =>
        `<span class="pill good" title="${esc(v)}">✓ ${esc(k)}：${esc(v)}</span>`).join("")}
    </div>
    <p class="micro">完整输入参数与每条查询配置见
      <a href="/download/${m.run_id}/manifest.json" download>manifest.json</a>，
      用相同种子与参数重跑可复现本页全部数字。</p>`;
}

function renderReco() {
  const rows = okSchemes();
  const best = (metric) => rows.reduce((a, b) =>
    (metricValue(a, metric) ?? Infinity) <= (metricValue(b, metric) ?? Infinity) ? a : b);
  const qMetrics = state.manifest.query_configs.map((q) => {
    const t = { point: "🎯 点查", range: "📐 范围聚合", scan: "📝 文本扫描" }[q.qtype];
    return [`${t}最优（${q.field}，中位数）`, `q:${q.qtype}:${q.field}`,
      "取多次重复的中位数，单次偶然最快不影响结论"];
  });
  const cards = [
    ["🏆 最小体积", "file_size_bytes", "压缩与编码共同决定磁盘与网络成本"],
    ["⚡ 最快写入", "write_ms", "高频落盘场景优先；排序方案另含排序成本列"],
    ...qMetrics,
  ];
  $("#recoBody").innerHTML = cards.map(([title, metric, note]) => {
    const s = best(metric);
    if (!s || metricValue(s, metric) == null) return "";
    return `<div class="reco">
      <div class="crown">${title}</div>
      <div class="name">${esc(s.name)}</div>
      <dl>
        <dt>${esc(metricLabel(metric))}</dt><dd>${metricFmt(metric)(metricValue(s, metric))}</dd>
        <dt>压缩 / profile</dt><dd>${esc(s.params.compression)} / ${esc(s.params.profile)}</dd>
        <dt>行组 / 排序</dt><dd>${s.params.row_group_size || "auto"} / ${s.params.sort_columns.join("+") || "无"}</dd>
        <dt>文件大小</dt><dd>${fmtBytes(s.file_size_bytes)}</dd>
      </dl>
      <p class="micro">${esc(note)}</p>
    </div>`;
  }).join("");
}

/* ---------------- SVG 条形图 ---------------- */
const SERIES_VARS = ["--series-1", "--series-2", "--series-3", "--series-4", "--series-5", "--series-6", "--series-7", "--series-8"];
function colorFor(scheme) {
  const idx = state.options.compressions.indexOf(scheme.params.compression);
  return `var(${SERIES_VARS[(idx < 0 ? 0 : idx) % SERIES_VARS.length]})`;
}

function renderChart() {
  const metric = state.metric;
  const label = metricLabel(metric), fmt = metricFmt(metric);
  let rows = okSchemes().filter((s) => metricValue(s, metric) != null);
  rows.sort((a, b) => metricValue(a, metric) - metricValue(b, metric));
  const max = Math.max(...rows.map((s) => metricValue(s, metric)));
  const labelW = 230, valueW = 92, rowH = 26, top = 8, bottom = 24;
  const barW = Math.max(120, 760 - labelW - valueW);
  const h = top * 2 + rows.length * rowH + bottom;
  const ticks = 4;
  const svg = [`<svg width="100%" viewBox="0 0 ${labelW + barW + valueW + 20} ${h}" role="img"
      aria-label="${esc(label)}横向条形图">`];
  for (let i = 0; i <= ticks; i++) {
    const x = labelW + (barW * i) / ticks;
    svg.push(`<line class="grid-line" x1="${x}" y1="${top}" x2="${x}" y2="${h - bottom}"/>`);
    svg.push(`<text class="tick" x="${x}" y="${h - 7}" text-anchor="middle">${esc(fmt(max * i / ticks))}</text>`);
  }
  rows.forEach((s, i) => {
    const y = top + i * rowH;
    const v = metricValue(s, metric);
    const w = Math.max(2, (v / max) * barW);
    svg.push(`<g class="bar-row" data-name="${esc(s.name)}">
      <text class="bar-label" x="${labelW - 8}" y="${y + rowH / 2 + 4}" text-anchor="end">${esc(s.name.length > 34 ? s.name.slice(0, 33) + "…" : s.name)}</text>
      <rect class="bar-rect" x="${labelW}" y="${y + 4}" width="${w}" height="${rowH - 10}" fill="${colorFor(s)}" opacity="0.9"/>
      <text class="bar-value" x="${labelW + w + 6}" y="${y + rowH / 2 + 4}">${esc(fmt(v))}</text>
    </g>`);
  });
  svg.push(`<line class="axis-line" x1="${labelW}" y1="${h - bottom}" x2="${labelW + barW}" y2="${h - bottom}"/></svg>`);
  $("#barChart").innerHTML = svg.join("");
  $("#chartNote").textContent =
    `共 ${rows.length} 个通过核对的方案，按${label}升序（越短越优）排列；颜色按压缩算法区分；点击条形可查看该方案列编码。`;
  bindChartTooltip(rows, metric);
}

function bindChartTooltip(rows, metric) {
  const tip = ensureTooltip();
  $$(".bar-row", $("#barChart")).forEach((g) => {
    const s = rows.find((r) => r.name === g.dataset.name);
    g.addEventListener("mousemove", (e) => {
      tip.style.display = "block";
      tip.style.left = `${Math.min(e.clientX + 14, innerWidth - 320)}px`;
      tip.style.top = `${e.clientY + 14}px`;
      tip.innerHTML = `<div class="tt-title">${esc(s.name)}</div>
        <div class="tt-row"><span>${esc(metricLabel(metric))}</span><b>${esc(metricFmt(metric)(metricValue(s, metric)))}</b></div>
        <div class="tt-row"><span>文件大小</span><b>${fmtBytes(s.file_size_bytes)}</b></div>
        <div class="tt-row"><span>写入</span><b>${fmtMs(s.write_ms)} ms</b></div>
        ${(s.queries || []).map((q) =>
          `<div class="tt-row"><span>${q.qtype}/${q.field} 中位</span><b>${fmtMs(q.median_ms)} ms</b></div>`).join("")}`;
    });
    g.addEventListener("mouseleave", () => (tip.style.display = "none"));
    g.addEventListener("click", () => openDrawer(s));
  });
}
function ensureTooltip() {
  let t = $("#chartTooltip");
  if (!t) { t = document.createElement("div"); t.id = "chartTooltip"; document.body.appendChild(t); }
  return t;
}

/* ---------------- 明细表 ---------------- */
function renderTable() {
  const qcfgs = state.manifest.query_configs;
  const head = $("#schemeHead");
  const cols = [
    ["name", "方案"], ["status", "状态"], ["params.compression", "压缩"],
    ["params.profile", "编码 profile"], ["params.row_group_size", "行组"],
    ["sortKey", "排序"], ["file_size_bytes", "大小"], ["write_ms", "写入 ms"],
    ["sort_ms", "排序 ms"], ...qcfgs.map((q, i) => [`q${i}`, `${q.qtype}/${q.field} 中位 (各次)`]),
    ["enc", "列编码"], ["dl", "产物"],
  ];
  head.innerHTML = cols.map(([k, label]) =>
    `<th data-key="${esc(k)}" class="${k.startsWith("q") || ["file_size_bytes", "write_ms", "sort_ms"].includes(k) ? "num" : ""}">${esc(label)}${state.sort.key === k ? (state.sort.dir === 1 ? " ▲" : " ▼") : ""}</th>`).join("");
  $$("th", head).forEach((th) => th.addEventListener("click", () => {
    const k = th.dataset.key;
    state.sort.dir = state.sort.key === k ? -state.sort.dir : 1;
    state.sort.key = k;
    renderTable();
  }));

  const filter = $("#tableFilter").value.trim().toLowerCase();
  const showFailed = $("#showFailed").checked;
  let rows = state.manifest.schemes.filter((s) =>
    (showFailed || s.status === "ok") &&
    (!filter || s.name.toLowerCase().includes(filter)));
  rows = sortRows(rows, state.sort.key, state.sort.dir);

  $("tbody", $("#schemeTable")).innerHTML = rows.map((s) => {
    const p = s.params;
    if (s.status !== "ok") {
      return `<tr>
        <td>${esc(s.name)}</td>
        <td><span class="status-failed">${s.status === "verified_failed" ? "核对失败" : "不支持/失败"}</span>
          <button class="link-btn" data-fail="${esc(s.name)}">原因</button></td>
        <td>${esc(p.compression)}</td><td>${esc(p.profile)}</td>
        <td class="num">${p.row_group_size || "auto"}</td>
        <td>${(p.sort_columns || []).join("+") || "—"}</td>
        <td colspan="7" class="status-failed">${esc(s.failure_reason || "未知错误")}</td>
      </tr>`;
    }
    return `<tr>
      <td>${esc(s.name)}</td>
      <td><span class="status-ok">✓ 通过</span></td>
      <td>${esc(p.compression)}</td>
      <td>${esc(p.profile)}</td>
      <td class="num">${p.row_group_size || "auto"}（${s.metadata.row_groups} 组）</td>
      <td>${(p.sort_columns || []).join("+") || "—"}${s.sort_ms ? ` <span class="runs">(${fmtMs(s.sort_ms)} ms)</span>` : ""}</td>
      <td class="num">${fmtBytes(s.file_size_bytes)}</td>
      <td class="num">${fmtMs(s.write_ms)}</td>
      <td class="num">${s.sort_ms ? fmtMs(s.sort_ms) : "—"}</td>
      ${s.queries.map((q) => `<td class="timing-cell num"><span class="med">${fmtMs(q.median_ms)}</span>
        <span class="runs">[${q.timings_ms.map((t) => t.toFixed(1)).join(", ")}]</span></td>`).join("")}
      <td><button class="link-btn" data-enc="${esc(s.name)}">查看列编码</button></td>
      <td><a href="/download/${state.manifest.run_id}/data/${esc(s.file)}" download>parquet</a></td>
    </tr>`;
  }).join("");

  $$("[data-enc]").forEach((b) => b.addEventListener("click", () =>
    openDrawer(state.manifest.schemes.find((s) => s.name === b.dataset.enc))));
  $$("[data-fail]").forEach((b) => b.addEventListener("click", () =>
    openFail(state.manifest.schemes.find((s) => s.name === b.dataset.fail))));
}

function sortRows(rows, key, dir) {
  const get = (s) => {
    if (key.startsWith("q")) {
      const i = Number(key.slice(1));
      return s.queries?.[i]?.median_ms ?? Infinity;
    }
    if (key.includes(".")) return s[key.split(".")[0]]?.[key.split(".")[1]] ?? "";
    if (key === "sortKey") return (s.params.sort_columns || []).join(",");
    if (key === "enc" || key === "dl" || key === "status") return s.status === "ok" ? 1 : 0;
    return s[key] ?? (typeof dir === "number" ? Infinity : "");
  };
  return [...rows].sort((a, b) => {
    const va = get(a), vb = get(b);
    if (typeof va === "number" && typeof vb === "number") return (va - vb) * dir;
    return String(va).localeCompare(String(vb), "zh") * dir;
  });
}

/* ---------------- 列编码抽屉 ---------------- */
function openDrawer(s) {
  if (!s?.metadata) return;
  $("#drawerTitle").textContent = `列编码详情 — ${s.name}`;
  const m = state.manifest;
  const cols = Object.entries(s.metadata.columns);
  $("#drawerBody").innerHTML = `
    <div class="lineage-grid" style="margin-bottom:12px">
      <div class="kv"><div class="k">行组数 / Parquet 版本</div><div class="v">${s.metadata.row_groups} / ${esc(s.metadata.format_version)}</div></div>
      <div class="kv"><div class="k">写入器</div><div class="v">${esc(s.metadata.created_by)}</div></div>
      <div class="kv"><div class="k">输入指纹</div><div class="v code">${esc(m.dataset.fingerprint)}</div></div>
      <div class="kv"><div class="k">产物</div><div class="v"><a href="/download/${m.run_id}/data/${esc(s.file)}" download>${esc(s.file)}</a></div></div>
    </div>
    ${cols.map(([name, c]) => `
      <div class="col-card">
        <h4>${esc(name)} <span class="pill good">${esc(c.logical_type)}</span></h4>
        <dl>
          <dt>物理类型</dt><dd>${esc(c.physical_type)}</dd>
          <dt>压缩</dt><dd>${esc(c.compression)}</dd>
          <dt>编码</dt><dd>${c.encodings.map((e) => `<span class="enc-tag">${esc(e)}</span>`).join("")}</dd>
          <dt>字典页</dt><dd>${c.has_dictionary ? `${c.dictionary_pages} 个（已启用字典编码）` : "无（未走字典）"}</dd>
          <dt>统计信息</dt><dd>${c.stats_present ? "✓ 页/行组统计存在，支持谓词裁剪" : "✗ 无统计"}</dd>
          <dt>占用 / 未压缩</dt><dd>${fmtBytes(c.compressed_bytes)} / ${fmtBytes(c.uncompressed_bytes)}（压缩比 ${c.compression_ratio}×）</dd>
        </dl>
      </div>`).join("")}`;
  $("#drawer").classList.remove("hidden");
  $("#drawerMask").classList.remove("hidden");
}
function closeDrawer() {
  $("#drawer").classList.add("hidden");
  $("#drawerMask").classList.add("hidden");
}

function openFail(s) {
  $("#modalTitle").textContent = `方案失败 — ${s.name}`;
  $("#modalBody").textContent =
    `参数：${JSON.stringify(s.params, null, 2)}\n\n引擎报错：${s.failure_reason || "—"}` +
    (s.failure_hint ? `\n\n原因说明：${s.failure_hint}` : "") +
    `\n\n该方案已从推荐与图表中排除。`;
  $("#modalMask").classList.remove("hidden");
}

/* ---------------- 初始化 ---------------- */
async function init() {
  state.options = await api("/api/options");
  renderOptions();
  [
    { qtype: "point", field: "category", repeats: 5 },
    { qtype: "range", field: "amount", repeats: 5 },
    { qtype: "scan", field: "remark", repeats: 5 },
  ].forEach((q) => $("#queryList").appendChild(queryRow(q)));

  $("#addQuery").addEventListener("click", () =>
    $("#queryList").appendChild(queryRow()));
  $("#runBtn").addEventListener("click", startRun);
  $("#tableFilter").addEventListener("input", () => state.manifest && renderTable());
  $("#showFailed").addEventListener("change", () => state.manifest && renderTable());
  $("#metricSeg").addEventListener("click", (e) => {
    const b = e.target.closest("button");
    if (!b) return;
    $$("#metricSeg button").forEach((x) => x.classList.remove("active"));
    b.classList.add("active");
    state.metric = b.dataset.metric;
    if (state.manifest) renderChart();
  });
  $("#drawerClose").addEventListener("click", closeDrawer);
  $("#drawerMask").addEventListener("click", closeDrawer);
  $("#modalClose").addEventListener("click", () => $("#modalMask").classList.add("hidden"));
  $("#modalMask").addEventListener("click", (e) => {
    if (e.target === $("#modalMask")) $("#modalMask").classList.add("hidden");
  });
}
init().catch((e) => setStatus(`初始化失败：${e.message}`, true));
