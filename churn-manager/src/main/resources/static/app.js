const state = {
  token: localStorage.getItem("managerToken") || "",
  currentUser: "",
  projects: [],
  selectedProjectId: null,
  catalog: [],
  pipelines: [],
  jobs: [],
  schedules: []
};

const projectFields = [
  "name", "description", "host", "sshPort", "projectRoot", "scriptsDir", "alertOutputDir",
  "experimentResultsDir", "plotsDir", "logsDir", "originDataDir", "transformedDataDir",
  "hiveDb", "hdfsLandingDir", "pythonCommand", "sparkSubmitCommand", "beelineCommand",
  "sshUsername", "sshPassword", "sshPrivateKey", "mysqlUrl", "mysqlUsername", "mysqlPassword",
  "hiveJdbcUrl", "hiveUsername", "hivePassword"
];

function qs(id) { return document.getElementById(id); }

function setMsg(message, isError = true) {
  const box = qs("globalMsg");
  box.style.color = isError ? "#b91c1c" : "#0f766e";
  box.textContent = message || "";
}

async function api(path, options = {}, anonymous = false) {
  const headers = Object.assign({ "Content-Type": "application/json" }, options.headers || {});
  if (!anonymous && state.token) {
    headers["Authorization"] = `Bearer ${state.token}`;
  }
  const resp = await fetch(path, Object.assign({}, options, { headers }));
  let payload = null;
  try {
    payload = await resp.json();
  } catch (e) {
    throw new Error(`接口响应不是 JSON: ${path}`);
  }

  if (!resp.ok || !payload.success) {
    throw new Error(payload.error || `请求失败: ${resp.status}`);
  }
  return payload.data;
}

function getSelectedProjectId() {
  const value = qs("projectSelect").value;
  if (!value) throw new Error("请先选择项目。");
  return Number(value);
}

function parseOverrides() {
  const text = qs("runtimeOverrides").value || "";
  const result = {};
  text.split(/\r?\n/).map(v => v.trim()).filter(Boolean).forEach(line => {
    const i = line.indexOf("=");
    if (i > 0) {
      const key = line.slice(0, i).trim();
      const value = line.slice(i + 1).trim();
      if (key) result[key] = value;
    }
  });
  return result;
}

function collectProjectForm() {
  const body = {};
  for (const f of projectFields) {
    body[f] = qs(f).value;
  }
  if (body.sshPort) body.sshPort = Number(body.sshPort);
  return body;
}

function fillProjectForm(project) {
  for (const f of projectFields) {
    if (!["sshPassword", "mysqlPassword", "hivePassword", "sshPrivateKey"].includes(f)) {
      qs(f).value = project[f] ?? "";
    } else {
      qs(f).value = "";
    }
  }
}

function renderProjects() {
  const select = qs("projectSelect");
  select.innerHTML = "";
  for (const p of state.projects) {
    const opt = document.createElement("option");
    opt.value = p.id;
    opt.textContent = `${p.id} | ${p.name} @ ${p.host}`;
    select.appendChild(opt);
  }
  if (state.selectedProjectId) {
    select.value = String(state.selectedProjectId);
  } else if (state.projects.length > 0) {
    state.selectedProjectId = state.projects[0].id;
    select.value = String(state.selectedProjectId);
  }
}

function renderCatalog() {
  const sel = qs("stepCatalog");
  const box = qs("pipelineSteps");
  sel.innerHTML = "";
  box.innerHTML = "";
  state.catalog.forEach(step => {
    const opt = document.createElement("option");
    opt.value = step.key;
    opt.textContent = `${step.displayName} (${step.key})`;
    sel.appendChild(opt);

    const item = document.createElement("label");
    const cb = document.createElement("input");
    cb.type = "checkbox";
    cb.value = step.key;
    cb.checked = true;
    item.appendChild(cb);
    item.append(` ${step.displayName}`);
    box.appendChild(item);
  });
}

function renderPipelines() {
  const sel = qs("pipelineList");
  sel.innerHTML = "";
  state.pipelines.forEach(p => {
    const opt = document.createElement("option");
    opt.value = p.id;
    const stepNames = (p.steps || []).map(s => s.displayName).join(" -> ");
    opt.textContent = `${p.name} | ${stepNames}`;
    sel.appendChild(opt);
  });
}

function renderJobs() {
  const tbody = qs("jobTable").querySelector("tbody");
  tbody.innerHTML = "";
  state.jobs.forEach(j => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${j.id}</td>
      <td>${j.displayName || "-"}</td>
      <td>${j.status || "-"}</td>
      <td>${j.triggerSource || "-"}</td>
      <td>${j.createdAt || "-"}</td>
      <td><button data-job="${j.id}">日志</button></td>`;
    tbody.appendChild(tr);
  });
}

function renderSchedules() {
  const tbody = qs("scheduleTable").querySelector("tbody");
  tbody.innerHTML = "";
  state.schedules.forEach(s => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${s.id}</td>
      <td>${s.name}</td>
      <td>${s.cronExpression}</td>
      <td>${s.enabled}</td>
      <td>
        <button data-toggle="${s.id}" class="warn">切换</button>
        <button data-del="${s.id}" class="danger">删除</button>
      </td>`;
    tbody.appendChild(tr);
  });
}

function renderFileList(files) {
  const tbody = qs("fileTable").querySelector("tbody");
  tbody.innerHTML = "";
  files.forEach(f => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${f.name}</td>
      <td>${f.directory ? "目录" : "文件"}</td>
      <td>${f.size || 0}</td>
      <td><button data-path="${f.path}" data-directory="${f.directory}">${f.directory ? "进入" : "预览"}</button></td>`;
    tbody.appendChild(tr);
  });
}

function renderDashboard(data) {
  qs("dashboardSource").textContent = `数据来源: ${data.source}`;
  const summary = data.summary || {};
  const kpis = [
    ["统计日期", summary.stat_date || "-"],
    ["活跃用户", summary.total_active_users || 0],
    ["高风险占比", summary.high_risk_rate || 0],
    ["平均流失概率", summary.avg_churn_prob || 0],
    ["Top卡关地图", summary.top_stuck_map_id || "-"]
  ];

  const kpiGrid = qs("kpiGrid");
  kpiGrid.innerHTML = "";
  for (const [k, v] of kpis) {
    const item = document.createElement("div");
    item.className = "kpi";
    item.innerHTML = `<div class="k">${k}</div><div class="v">${v}</div>`;
    kpiGrid.appendChild(item);
  }

  const risk = data.riskDistribution || { high: 0, medium: 0, low: 0 };
  const total = Math.max(1, Number(risk.high || 0) + Number(risk.medium || 0) + Number(risk.low || 0));
  const bars = qs("riskBars");
  bars.innerHTML = "";
  [["高风险", risk.high], ["中风险", risk.medium], ["低风险", risk.low]].forEach(([name, count]) => {
    const pct = (Number(count || 0) / total) * 100;
    const row = document.createElement("div");
    row.className = "bar";
    row.innerHTML = `<span>${name}</span><div class="bar-track"><div class="bar-fill" style="width:${pct.toFixed(1)}%"></div></div><span>${count}</span>`;
    bars.appendChild(row);
  });

  const riskBody = qs("riskUserTable").querySelector("tbody");
  riskBody.innerHTML = "";
  (data.riskUsers || []).forEach(u => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${u.user_id}</td><td>${u.risk_level}</td><td>${u.churn_prob}</td><td>${u.top_reason_1 || "-"}</td>`;
    riskBody.appendChild(tr);
  });

  const reasonBody = qs("reasonTable").querySelector("tbody");
  reasonBody.innerHTML = "";
  (data.reasonCounts || []).forEach(r => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${r.reason}</td><td>${r.count}</td>`;
    reasonBody.appendChild(tr);
  });

  const corrBody = qs("corrTable").querySelector("tbody");
  corrBody.innerHTML = "";
  (data.correlationTop || []).forEach(c => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${c.feature}</td><td>${c.abs_corr_with_label}</td>`;
    corrBody.appendChild(tr);
  });

  const modelBody = qs("modelTable").querySelector("tbody");
  modelBody.innerHTML = "";
  (data.modelComparison || []).slice(0, 20).forEach(m => {
    const tr = document.createElement("tr");
    tr.innerHTML = `<td>${m.model}</td><td>${m.window}</td><td>${m.auc}</td><td>${m.f1}</td>`;
    modelBody.appendChild(tr);
  });
}

async function loadProjects() {
  state.projects = await api("/api/projects");
  renderProjects();
}

async function loadCatalog() {
  state.catalog = await api("/api/catalog/steps");
  renderCatalog();
}

async function loadPipelines() {
  const id = getSelectedProjectId();
  state.pipelines = await api(`/api/projects/${id}/pipelines`);
  renderPipelines();
}

async function loadJobs() {
  const id = getSelectedProjectId();
  state.jobs = await api(`/api/projects/${id}/jobs`);
  renderJobs();
}

async function loadSchedules() {
  const id = getSelectedProjectId();
  state.schedules = await api(`/api/projects/${id}/schedules`);
  renderSchedules();
}

async function bootstrapAuth() {
  const status = await api("/api/auth/status", {}, true);
  const initialized = !!status.initialized;
  qs("initHint").textContent = status.persistentSecretConfigured
    ? "系统已配置持久化密钥。"
    : "警告: MANAGER_SECRET_KEY 未配置，重启后已保存敏感配置将无法解密。";

  if (!initialized) {
    qs("initBlock").classList.remove("hidden");
    qs("loginBlock").classList.add("hidden");
    qs("initUsername").value = status.recommendedAdminUsername || "admin";
    qs("initPassword").value = status.recommendedAdminPassword || "adminmvp123";
  } else {
    qs("initBlock").classList.add("hidden");
    qs("loginBlock").classList.remove("hidden");
    qs("loginUsername").value = status.recommendedAdminUsername || "admin";
  }
}

async function enterApp() {
  qs("authLayer").classList.add("hidden");
  qs("app").classList.remove("hidden");
  const me = await api("/api/auth/me");
  state.currentUser = me.username;
  qs("currentUser").textContent = state.currentUser;
  await loadProjects();
  await loadCatalog();
}

function bindEvents() {
  qs("initBtn").addEventListener("click", async () => {
    try {
      await api("/api/auth/init", {
        method: "POST",
        body: JSON.stringify({ username: qs("initUsername").value, password: qs("initPassword").value })
      }, true);
      qs("authMsg").textContent = "初始化成功，请登录。";
      await bootstrapAuth();
    } catch (e) {
      qs("authMsg").textContent = e.message;
    }
  });

  qs("loginBtn").addEventListener("click", async () => {
    try {
      const data = await api("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({ username: qs("loginUsername").value, password: qs("loginPassword").value })
      }, true);
      state.token = data.token;
      localStorage.setItem("managerToken", data.token);
      await enterApp();
    } catch (e) {
      qs("authMsg").textContent = e.message;
    }
  });

  qs("logoutBtn").addEventListener("click", () => {
    localStorage.removeItem("managerToken");
    location.reload();
  });

  qs("refreshProjectsBtn").addEventListener("click", async () => {
    try { await loadProjects(); setMsg("项目列表已刷新", false); } catch (e) { setMsg(e.message); }
  });

  qs("loadProjectBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      state.selectedProjectId = id;
      const detail = await api(`/api/projects/${id}`);
      fillProjectForm(detail);
      await Promise.all([loadPipelines(), loadJobs(), loadSchedules()]);
      setMsg(`已加载项目 ${detail.name}`, false);
    } catch (e) { setMsg(e.message); }
  });

  qs("createProjectBtn").addEventListener("click", async () => {
    try {
      const created = await api("/api/projects", { method: "POST", body: JSON.stringify(collectProjectForm()) });
      state.selectedProjectId = created.id;
      await loadProjects();
      qs("projectSelect").value = String(created.id);
      setMsg(`项目 ${created.name} 创建成功`, false);
    } catch (e) { setMsg(e.message); }
  });

  qs("updateProjectBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      const updated = await api(`/api/projects/${id}`, { method: "PUT", body: JSON.stringify(collectProjectForm()) });
      setMsg(`项目 ${updated.name} 更新成功`, false);
      await loadProjects();
    } catch (e) { setMsg(e.message); }
  });

  qs("testConnectionBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      const result = await api(`/api/projects/${id}/test-connection`, { method: "POST" });
      qs("connectionResult").textContent = JSON.stringify(result, null, 2);
      setMsg("连接测试完成", false);
    } catch (e) { setMsg(e.message); }
  });

  qs("listFilesBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      const scope = encodeURIComponent(qs("fileScope").value);
      const path = encodeURIComponent(qs("filePath").value || "");
      const files = await api(`/api/projects/${id}/files?scope=${scope}&path=${path}`);
      renderFileList(files);
      setMsg(`读取到 ${files.length} 个条目`, false);
    } catch (e) { setMsg(e.message); }
  });

  qs("fileTable").addEventListener("click", async (ev) => {
    const btn = ev.target.closest("button[data-path]");
    if (!btn) return;
    const path = btn.getAttribute("data-path");
    const id = getSelectedProjectId();
    try {
      const isDirectory = btn.getAttribute("data-directory") === "true";
      if (isDirectory) {
        const scope = encodeURIComponent(qs("fileScope").value);
        const files = await api(`/api/projects/${id}/files?scope=${scope}&path=${encodeURIComponent(path)}`);
        qs("filePath").value = path;
        renderFileList(files);
        setMsg("目录加载完成", false);
        return;
      }

      const data = await api(`/api/projects/${id}/file-content?scope=absolute&path=${encodeURIComponent(path)}`);
      qs("filePreview").textContent = data.content || "";
      qs("filePath").value = path;
      setMsg("文件预览完成", false);
    } catch (e) {
      setMsg(`文件浏览失败: ${e.message}`);
    }
  });

  qs("refreshPipelineBtn").addEventListener("click", async () => {
    try { await loadPipelines(); setMsg("Pipeline 已刷新", false); } catch (e) { setMsg(e.message); }
  });

  qs("createPipelineBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      const selected = Array.from(qs("pipelineSteps").querySelectorAll("input[type=checkbox]:checked")).map(el => el.value);
      const payload = {
        name: qs("pipelineName").value || `自定义Pipeline-${Date.now()}`,
        description: qs("pipelineDesc").value || "",
        stepKeys: selected
      };
      await api(`/api/projects/${id}/pipelines`, { method: "POST", body: JSON.stringify(payload) });
      await loadPipelines();
      setMsg("Pipeline 创建成功", false);
    } catch (e) { setMsg(e.message); }
  });

  qs("runStepBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      const stepKey = qs("stepCatalog").value;
      if (!stepKey) throw new Error("请选择要执行的步骤");
      const data = await api(`/api/projects/${id}/steps/${encodeURIComponent(stepKey)}/run`, {
        method: "POST",
        body: JSON.stringify({ runtimeOverrides: parseOverrides() })
      });
      setMsg(`步骤已提交，任务ID=${data.id}`, false);
      await loadJobs();
    } catch (e) { setMsg(e.message); }
  });

  qs("runPipelineBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      const pipelineId = qs("pipelineList").value;
      if (!pipelineId) throw new Error("请选择要执行的 Pipeline");
      const data = await api(`/api/projects/${id}/pipelines/${pipelineId}/run`, {
        method: "POST",
        body: JSON.stringify({ runtimeOverrides: parseOverrides() })
      });
      setMsg(`Pipeline 已提交，任务ID=${data.id}`, false);
      await loadJobs();
    } catch (e) { setMsg(e.message); }
  });

  qs("refreshJobsBtn").addEventListener("click", async () => {
    try {
      await Promise.all([loadJobs(), loadSchedules()]);
      setMsg("任务与调度已刷新", false);
    } catch (e) { setMsg(e.message); }
  });

  qs("jobTable").addEventListener("click", async (ev) => {
    const btn = ev.target.closest("button[data-job]");
    if (!btn) return;
    try {
      const id = btn.getAttribute("data-job");
      const data = await api(`/api/jobs/${id}/log`);
      qs("jobLog").textContent = data.logPreview || "";
      setMsg(`任务 ${id} 日志加载完成`, false);
    } catch (e) { setMsg(e.message); }
  });

  qs("createScheduleBtn").addEventListener("click", async () => {
    try {
      const projectId = getSelectedProjectId();
      const pipelineId = qs("pipelineList").value;
      if (!pipelineId) throw new Error("请先选择一个 Pipeline 用于定时调度。");
      const payload = {
        name: qs("scheduleName").value || `schedule-${Date.now()}`,
        pipelineId: Number(pipelineId),
        cronExpression: qs("scheduleCron").value,
        enabled: qs("scheduleEnabled").checked
      };
      await api(`/api/projects/${projectId}/schedules`, { method: "POST", body: JSON.stringify(payload) });
      await loadSchedules();
      setMsg("调度创建成功", false);
    } catch (e) { setMsg(e.message); }
  });

  qs("scheduleTable").addEventListener("click", async (ev) => {
    const t = ev.target;
    const toggleId = t.getAttribute("data-toggle");
    const delId = t.getAttribute("data-del");
    try {
      if (toggleId) {
        const row = state.schedules.find(v => String(v.id) === String(toggleId));
        if (!row) throw new Error("调度记录不存在或已刷新，请重试。");
        await api(`/api/schedules/${toggleId}/toggle`, {
          method: "POST",
          body: JSON.stringify({ enabled: !row.enabled })
        });
        await loadSchedules();
        setMsg("调度状态已切换", false);
      }
      if (delId) {
        await api(`/api/schedules/${delId}`, { method: "DELETE" });
        await loadSchedules();
        setMsg("调度已删除", false);
      }
    } catch (e) { setMsg(e.message); }
  });

  qs("loadDashboardBtn").addEventListener("click", async () => {
    try {
      const id = getSelectedProjectId();
      const data = await api(`/api/projects/${id}/dashboard`);
      renderDashboard(data);
      setMsg("看板数据加载完成", false);
    } catch (e) { setMsg(e.message); }
  });
}

async function main() {
  bindEvents();
  await bootstrapAuth();
  if (state.token) {
    try {
      await enterApp();
    } catch {
      localStorage.removeItem("managerToken");
      state.token = "";
      qs("authLayer").classList.remove("hidden");
      qs("app").classList.add("hidden");
      await bootstrapAuth();
    }
  }
}

main().catch(err => {
  setMsg(err.message || String(err));
});
