// js/api.js  — Shared HTTP client

const TOKEN_KEY = 'cm_token';
const USER_KEY  = 'cm_user';

function getToken() { return localStorage.getItem(TOKEN_KEY); }
function setToken(t) { localStorage.setItem(TOKEN_KEY, t); }
function clearAuth() { localStorage.removeItem(TOKEN_KEY); localStorage.removeItem(USER_KEY); }

function getUser() {
    try { return JSON.parse(localStorage.getItem(USER_KEY) || 'null'); } catch { return null; }
}
function setUser(u) { localStorage.setItem(USER_KEY, JSON.stringify(u)); }

/**
 * Core fetch wrapper. Returns parsed JSON body (.data) or throws on error.
 */
async function apiFetch(method, path, body, opts = {}) {
    const headers = { 'Content-Type': 'application/json' };
    const token = getToken();
    if (token) headers['Authorization'] = 'Bearer ' + token;

    const res = await fetch(API_BASE + path, {
        method,
        headers,
        body: body != null ? JSON.stringify(body) : undefined,
        ...opts,
    });

    let json;
    try { json = await res.json(); } catch { json = { code: res.status, message: res.statusText }; }

    if (!res.ok || json.code >= 400) {
        if (res.status === 401 || json.code === 401) {
            clearAuth();
            location.href = '../pages/login.html';
        }
        const err = new Error(json.message || `HTTP ${res.status}`);
        err.status = res.status;
        err.body = json;
        throw err;
    }
    return json.data;
}

const api = {
    get:    (path)       => apiFetch('GET',    path, null),
    post:   (path, body) => apiFetch('POST',   path, body),
    put:    (path, body) => apiFetch('PUT',    path, body),
    delete: (path)       => apiFetch('DELETE', path, null),

    // Auth
    login:   (username, password) => apiFetch('POST', '/api/auth/login', { username, password }),
    me:      ()                    => apiFetch('GET',  '/api/auth/me',   null),

    // Projects
    listProjects:      ()            => apiFetch('GET',    '/api/projects'),
    getProject:        (id)          => apiFetch('GET',    `/api/projects/${id}`),
    createProject:     (body)        => apiFetch('POST',   '/api/projects', body),
    updateProject:     (id, body)    => apiFetch('PUT',    `/api/projects/${id}`, body),
    deleteProject:     (id)          => apiFetch('DELETE', `/api/projects/${id}`),
    testConnection:    (id)          => apiFetch('POST',   `/api/projects/${id}/test-connection`),
    listFiles:         (id, scope, path) => apiFetch('GET',`/api/projects/${id}/files?scope=${encodeURIComponent(scope||'')}&path=${encodeURIComponent(path||'')}`),
    getFileContent:    (id, path, maxLines=500) => apiFetch('GET',    `/api/projects/${id}/file-content?path=${encodeURIComponent(path)}&maxLines=${maxLines}`),

    // Steps / Pipelines / Jobs
    listSteps:         ()            => apiFetch('GET',    '/api/steps'),
    listPipelines:     (pid)         => apiFetch('GET',    `/api/projects/${pid}/pipelines`),
    createPipeline:    (pid, body)   => apiFetch('POST',   `/api/projects/${pid}/pipelines`, body),
    updatePipeline:    (pid, lid, body) => apiFetch('PUT', `/api/projects/${pid}/pipelines/${lid}`, body),
    deletePipeline:    (pid, lid)    => apiFetch('DELETE', `/api/projects/${pid}/pipelines/${lid}`),
    runStep:           (pid, key, overrides) => apiFetch('POST', `/api/projects/${pid}/steps/${key}/run`, { overrides }),
    runPipeline:       (pid, lid, overrides) => apiFetch('POST', `/api/projects/${pid}/pipelines/${lid}/run`, { overrides }),
    listJobs:          (pid, page, size) => apiFetch('GET', `/api/projects/${pid}/jobs?page=${page||0}&size=${size||20}`),
    getJob:            (jid)         => apiFetch('GET',    `/api/jobs/${jid}`),
    getJobLog:         (jid)         => apiFetch('GET',    `/api/jobs/${jid}/log`),

    // Schedules
    listSchedules:     (pid)         => apiFetch('GET',    `/api/projects/${pid}/schedules`),
    createSchedule:    (pid, body)   => apiFetch('POST',   `/api/projects/${pid}/schedules`, body),
    updateSchedule:    (pid, id, b)  => apiFetch('PUT',    `/api/projects/${pid}/schedules/${id}`, b),
    toggleSchedule:    (pid, id)     => apiFetch('POST',   `/api/projects/${pid}/schedules/${id}/toggle`),
    runScheduleNow:    (pid, id)     => apiFetch('POST',   `/api/projects/${pid}/schedules/${id}/run-now`),
    deleteSchedule:    (pid, id)     => apiFetch('DELETE', `/api/projects/${pid}/schedules/${id}`),

    // Dashboard
    getDashboard:      (pid)         => apiFetch('GET',    `/api/dashboard?projectId=${pid||''}`),
    getDashboardFallback: ()         => apiFetch('GET',    '/api/dashboard/fallback'),
};

/** Require auth; redirect if missing */
function requireAuth() {
    if (!getToken()) { location.href = '../pages/login.html'; }
}

/** Render a flash message inside a container element */
function showFlash(el, msg, type = 'error') {
    if (!el) return;
    const cls = type === 'error' ? 'error' : type === 'success' ? 'success' : type === 'warning' ? 'warning' : 'info';
    el.innerHTML = `<div class="alert-banner alert-${cls}">${escHtml(msg)}</div>`;
    const delay = type === 'error' ? 6000 : 3500;
    setTimeout(() => { if (el) el.innerHTML = ''; }, delay);
}

/** Escape HTML */
function escHtml(str) {
    return String(str ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

/** Format ISO timestamp */
function fmtTime(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('zh-CN', { hour12: false });
}

function fmtDate(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleDateString('zh-CN');
}

/** Risk badge HTML */
function riskBadge(level) {
    const map = { high: ['danger', '高风险'], medium: ['warning', '中风险'], low: ['success', '低风险'] };
    const [cls, label] = map[level] || ['secondary', level || '—'];
    return `<span class="badge badge-${cls}">${escHtml(label)}</span>`;
}

/** Job status badge — 3 display states: running / ended / aborted */
function jobStatusBadge(status) {
    const map = {
        PENDING:   ['info',      '运行中'],
        RUNNING:   ['info',      '运行中'],
        SUCCESS:   ['success',   '已结束'],
        FAILED:    ['danger',    '已中止'],
        CANCELLED: ['danger',    '已中止'],
    };
    const [cls, label] = map[status] || ['secondary', status || '—'];
    return `<span class="badge badge-${cls}">${label}</span>`;
}

/** Simple modal helpers */
function openModal(id) { document.getElementById(id)?.classList.remove('hidden'); }
function closeModal(id) { document.getElementById(id)?.classList.add('hidden'); }

/** Tab switching */
function initTabs(containerSelector) {
    const container = document.querySelector(containerSelector);
    if (!container) return;
    container.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            container.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            container.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
            btn.classList.add('active');
            const target = btn.dataset.tab;
            container.querySelector(`.tab-panel[data-tab="${target}"]`)?.classList.add('active');
        });
    });
}
