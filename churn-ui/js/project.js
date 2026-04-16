// js/project.js — Project detail page

(function () {
    requireAuth();

    const params    = new URLSearchParams(location.search);
    const projectId = parseInt(params.get('id') || '0');
    if (!projectId) { location.href = 'projects.html'; return; }

    // -----------------------------------------------------------------------
    // DOM refs
    // -----------------------------------------------------------------------
    const userLabel     = document.getElementById('userLabel');
    const logoutBtn     = document.getElementById('logoutBtn');
    const flashEl       = document.getElementById('flash');
    const projectNameEl = document.getElementById('projectName');
    const topbarTitle   = document.getElementById('topbarTitle');

    userLabel.textContent = getUser()?.username || '';
    logoutBtn.addEventListener('click', () => { clearAuth(); location.href = 'login.html'; });

    // -----------------------------------------------------------------------
    // Tabs
    // -----------------------------------------------------------------------
    initTabs('#tabsContainer');

    // -----------------------------------------------------------------------
    // Project overview
    // -----------------------------------------------------------------------
    let project = null;

    async function loadProject() {
        try {
            project = await api.getProject(projectId);
            projectNameEl.textContent = project.name;
            topbarTitle.textContent   = project.name + ' — 项目详情';
            renderOverview(project);
        } catch (err) {
            showFlash(flashEl, err.message, 'error');
        }
    }

    function renderOverview(p) {
        const el = document.getElementById('overviewContent');
        const row = (label, val) => `<tr><td class="text-muted" style="width:160px;padding:7px 12px;">${escHtml(label)}</td><td style="padding:7px 12px;">${escHtml(val ?? '—')}</td></tr>`;
        el.innerHTML = `
          <div class="form-row">
            <div>
              <p class="nav-section-title" style="padding:0;margin-bottom:8px;color:#888;">基本信息</p>
              <table class="styled"><tbody>
                ${row('项目名称', p.name)}
                ${row('描述', p.description)}
                ${row('主机地址', p.host + ':' + (p.sshPort || 22))}
                ${row('SSH 用户', p.sshUsername)}
                ${row('Python 命令', p.pythonCommand)}
                ${row('创建时间', fmtTime(p.createdAt))}
                ${row('更新时间', fmtTime(p.updatedAt))}
              </tbody></table>
            </div>
            <div>
              <p class="nav-section-title" style="padding:0;margin-bottom:8px;color:#888;">目录配置</p>
              <table class="styled"><tbody>
                ${row('项目根目录', p.projectRoot)}
                ${row('脚本目录', p.scriptsDir)}
                ${row('告警输出', p.alertOutputDir)}
                ${row('实验结果', p.experimentResultsDir)}
                ${row('日志目录', p.logsDir)}
                ${row('Hive 数据库', p.hiveDb)}
              </tbody></table>
            </div>
          </div>`;
    }

    // -----------------------------------------------------------------------
    // File browser
    // -----------------------------------------------------------------------
    const fileScopeSelect = document.getElementById('fileScopeSelect');
    const filePathInput   = document.getElementById('filePathInput');
    const fileListEl      = document.getElementById('fileList');
    const fileContentEl   = document.getElementById('fileContentViewer');
    const fileContentPath = document.getElementById('fileContentPath');

    document.getElementById('browseBtn').addEventListener('click', () => {
        browseFiles(fileScopeSelect.value, filePathInput.value.trim());
    });
    filePathInput.addEventListener('keydown', e => { if (e.key === 'Enter') browseFiles(fileScopeSelect.value, filePathInput.value.trim()); });

    async function browseFiles(scope, path) {
        fileListEl.innerHTML = '<span class="spinner"></span>';
        try {
            const entries = await api.listFiles(projectId, scope, path);
            renderFileList(entries, scope, path);
        } catch (err) {
            fileListEl.innerHTML = `<p class="text-danger">${escHtml(err.message)}</p>`;
        }
    }

    function renderFileList(entries, scope, currentPath) {
        if (!entries?.length) { fileListEl.innerHTML = '<p class="text-muted">目录为空</p>'; return; }
        fileListEl.innerHTML = entries.map(e => {
            const icon = e.isDirectory ? '📁' : '📄';
            return `<div class="file-entry" onclick="handleFileEntry(${JSON.stringify({ ...e, _scope: scope, _parent: currentPath })})">
              <span class="icon">${icon}</span>
              <span>${escHtml(e.name)}</span>
              ${!e.isDirectory ? `<span class="ml-auto text-muted" style="font-size:.78rem;">${e.size != null ? e.size + ' B' : ''}</span>` : ''}
            </div>`;
        }).join('');
    }

    window.handleFileEntry = async (entry) => {
        if (entry.isDirectory) {
            filePathInput.value = entry.path || '';
            browseFiles(entry._scope, entry.path || '');
        } else {
            // Show file content
            fileContentPath.textContent = entry.path || entry.name;
            fileContentEl.classList.remove('hidden');
            fileContentEl.querySelector('.code-viewer').textContent = '加载中…';
            try {
                const data = await api.getFileContent(projectId, entry.path);
                fileContentEl.querySelector('.code-viewer').textContent = data?.content || '(空文件)';
            } catch (err) {
                fileContentEl.querySelector('.code-viewer').textContent = '读取失败：' + err.message;
            }
        }
    };

    document.getElementById('closeFileContent').addEventListener('click', () => fileContentEl.classList.add('hidden'));

    // -----------------------------------------------------------------------
    // Pipelines
    // -----------------------------------------------------------------------
    let steps = [];
    let pipelines = [];

    async function loadPipelines() {
        [steps, pipelines] = await Promise.all([api.listSteps(), api.listPipelines(projectId)]);
        renderPipelines();
    }

    function renderPipelines() {
        const el = document.getElementById('pipelineList');
        if (!pipelines.length) { el.innerHTML = '<p class="text-muted" style="padding:12px 0;">暂无流水线，点击「新建流水线」创建</p>'; return; }
        el.innerHTML = pipelines.map(pl => {
            let keys = [];
            try { keys = JSON.parse(pl.stepKeysJson); } catch {}
            const stepNames = keys.map(k => steps.find(s => s.key === k)?.displayName || k).join(' → ');
            return `<div class="card" style="margin-bottom:10px;">
              <div class="card-header" style="margin-bottom:8px;">
                <span class="card-title">${escHtml(pl.name)}</span>
                <div class="btn-group">
                  <button class="btn btn-primary btn-sm" onclick="runPipeline(${pl.id})">▶ 运行</button>
                  <button class="btn btn-danger btn-sm" onclick="delPipeline(${pl.id})">删除</button>
                </div>
              </div>
              <p class="text-muted" style="font-size:.82rem;">${escHtml(pl.description || '')}</p>
              <p style="margin-top:6px;font-size:.82rem;">步骤：${escHtml(stepNames)}</p>
            </div>`;
        }).join('');
    }

    // New pipeline modal
    document.getElementById('newPipelineBtn').addEventListener('click', () => {
        document.getElementById('pipelineForm').reset();
        renderStepCheckboxes();
        openModal('pipelineModal');
    });
    document.getElementById('cancelPipelineBtn').addEventListener('click', () => closeModal('pipelineModal'));
    document.getElementById('savePipelineBtn').addEventListener('click', async () => {
        const name = document.getElementById('plName').value.trim();
        const desc = document.getElementById('plDesc').value.trim();
        const keys = [...document.querySelectorAll('#stepCheckList input:checked')].map(cb => cb.value);
        if (!name) { showFlash(flashEl, '流水线名称不能为空', 'error'); return; }
        if (!keys.length) { showFlash(flashEl, '至少选择一个步骤', 'error'); return; }
        try {
            await api.createPipeline(projectId, { name, description: desc, stepKeys: keys });
            closeModal('pipelineModal');
            loadPipelines();
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    });

    function renderStepCheckboxes() {
        const el = document.getElementById('stepCheckList');
        el.innerHTML = steps.map(s => `
          <label style="display:flex;align-items:flex-start;gap:8px;padding:6px 0;cursor:pointer;font-size:.87rem;">
            <input type="checkbox" value="${escHtml(s.key)}" style="margin-top:2px;">
            <span><strong>${escHtml(s.displayName)}</strong><br><span class="text-muted">${escHtml(s.description)}</span></span>
          </label>`).join('');
    }

    window.runPipeline = async (pipelineId) => {
        try {
            const job = await api.runPipeline(projectId, pipelineId, {});
            showFlash(flashEl, `流水线已提交，任务 ID: ${job.id}`, 'success');
            setTimeout(() => loadJobs(), 1200);
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    };

    window.delPipeline = async (pipelineId) => {
        if (!confirm('确认删除该流水线？')) return;
        try {
            await api.deletePipeline(projectId, pipelineId);
            loadPipelines();
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    };

    // Single step run
    async function loadStepCards() {
        if (!steps.length) steps = await api.listSteps();
        const el = document.getElementById('stepCards');
        el.innerHTML = steps.map(s => `
          <div class="card" style="margin-bottom:8px;display:flex;align-items:center;gap:12px;padding:12px 16px;">
            <div style="flex:1;">
              <strong style="font-size:.9rem;">${escHtml(s.displayName)}</strong>
              <p class="text-muted" style="font-size:.8rem;margin-top:2px;">${escHtml(s.description)}</p>
            </div>
            <span class="badge badge-${s.executor === 'spark' ? 'info' : 'secondary'}">${escHtml(s.executor)}</span>
            <button class="btn btn-outline btn-sm" onclick="runSingleStep('${escHtml(s.key)}')">▶ 运行</button>
          </div>`).join('');
    }

    window.runSingleStep = async (key) => {
        try {
            const job = await api.runStep(projectId, key, {});
            showFlash(flashEl, `步骤已提交，任务 ID: ${job.id}`, 'success');
            setTimeout(() => loadJobs(), 1200);
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    };

    // -----------------------------------------------------------------------
    // Jobs
    // -----------------------------------------------------------------------
    let jobsPage = 0;
    const jobsSize = 20;
    let jobsTotal = 0;

    async function loadJobs(page) {
        if (page !== undefined) jobsPage = page;
        const el = document.getElementById('jobsTbody');
        el.innerHTML = '<tr><td colspan="7" style="text-align:center;"><span class="spinner"></span></td></tr>';
        try {
            const data = await api.listJobs(projectId, jobsPage, jobsSize);
            jobsTotal = data.total;
            renderJobs(data.items);
            renderJobsPagination();
        } catch (err) {
            el.innerHTML = `<tr><td colspan="7" class="text-danger">${escHtml(err.message)}</td></tr>`;
        }
    }

    function renderJobs(items) {
        const el = document.getElementById('jobsTbody');
        if (!items?.length) { el.innerHTML = '<tr><td colspan="7" class="text-muted" style="text-align:center;padding:16px;">暂无任务记录</td></tr>'; return; }
        el.innerHTML = items.map(j => `<tr>
          <td>${j.id}</td>
          <td>${escHtml(j.name || j.stepKey || '—')}</td>
          <td>${jobStatusBadge(j.status)}</td>
          <td>${escHtml(j.triggerSource || '—')}</td>
          <td>${fmtTime(j.createdAt)}</td>
          <td>${fmtTime(j.finishedAt)}</td>
          <td><button class="btn btn-outline btn-sm" onclick="viewJobLog(${j.id})">查看日志</button></td>
        </tr>`).join('');
    }

    function renderJobsPagination() {
        const el = document.getElementById('jobsPagination');
        const totalPages = Math.ceil(jobsTotal / jobsSize);
        el.innerHTML = `
          <button class="page-btn" onclick="loadJobs(${jobsPage - 1})" ${jobsPage === 0 ? 'disabled' : ''}>上一页</button>
          <span class="page-info">第 ${jobsPage + 1} / ${totalPages || 1} 页 · 共 ${jobsTotal} 条</span>
          <button class="page-btn" onclick="loadJobs(${jobsPage + 1})" ${jobsPage >= totalPages - 1 ? 'disabled' : ''}>下一页</button>`;
    }

    // Job log modal
    const logModal = document.getElementById('logModal');
    const logContent = document.getElementById('logContent');
    let logPollingTimer = null;

    window.viewJobLog = async (jobId) => {
        clearInterval(logPollingTimer);
        document.getElementById('logModalTitle').textContent = `任务日志 #${jobId}`;
        logContent.textContent = '加载中…';
        openModal('logModal');
        await refreshLog(jobId);
        // Auto-refresh if running
        logPollingTimer = setInterval(async () => {
            const job = await api.getJob(jobId).catch(() => null);
            if (!job || job.status === 'SUCCESS' || job.status === 'FAILED' || job.status === 'CANCELLED') {
                clearInterval(logPollingTimer);
            }
            await refreshLog(jobId);
        }, 3000);
    };

    async function refreshLog(jobId) {
        try {
            const data = await api.getJobLog(jobId);
            logContent.textContent = data.log || '(暂无日志)';
            logContent.scrollTop = logContent.scrollHeight;
        } catch (err) {
            logContent.textContent = '读取失败：' + err.message;
        }
    }

    document.getElementById('closeLogModal').addEventListener('click', () => {
        clearInterval(logPollingTimer);
        closeModal('logModal');
    });

    // -----------------------------------------------------------------------
    // Schedules
    // -----------------------------------------------------------------------
    let schedules = [];

    async function loadSchedules() {
        try {
            schedules = await api.listSchedules(projectId);
            renderSchedules();
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    }

    function renderSchedules() {
        const el = document.getElementById('scheduleList');
        if (!schedules.length) { el.innerHTML = '<p class="text-muted" style="padding:12px 0;">暂无调度任务</p>'; return; }
        el.innerHTML = `<table class="styled"><thead><tr>
          <th>名称</th><th>目标</th><th>Cron</th><th>状态</th><th>上次触发</th><th>操作</th>
        </tr></thead><tbody>${schedules.map(s => `<tr>
          <td>${escHtml(s.name)}</td>
          <td>${s.pipelineId ? `Pipeline #${s.pipelineId}` : s.stepKey || '—'}</td>
          <td><code>${escHtml(s.cronExpression)}</code></td>
          <td><span class="badge ${s.enabled ? 'badge-success' : 'badge-secondary'}">${s.enabled ? '启用' : '停用'}</span></td>
          <td>${fmtTime(s.lastTriggeredAt)}</td>
          <td>
            <div class="btn-group">
              <button class="btn btn-outline btn-sm" onclick="toggleSchedule(${s.id})">${s.enabled ? '停用' : '启用'}</button>
              <button class="btn btn-danger btn-sm" onclick="deleteSchedule(${s.id})">删除</button>
            </div>
          </td>
        </tr>`).join('')}</tbody></table>`;
    }

    document.getElementById('newScheduleBtn').addEventListener('click', async () => {
        if (!pipelines.length && !steps.length) {
            [steps, pipelines] = await Promise.all([api.listSteps(), api.listPipelines(projectId)]).catch(() => [[], []]);
        }
        renderScheduleTargets();
        openModal('scheduleModal');
    });
    document.getElementById('cancelScheduleBtn').addEventListener('click', () => closeModal('scheduleModal'));
    document.getElementById('saveScheduleBtn').addEventListener('click', async () => {
        const name = document.getElementById('schName').value.trim();
        const cron = document.getElementById('schCron').value.trim();
        const targetVal = document.getElementById('schTarget').value;
        if (!name || !cron) { showFlash(flashEl, '名称和 Cron 不能为空', 'error'); return; }
        const body = { name, cronExpression: cron, enabled: true };
        if (targetVal.startsWith('pipeline:')) body.pipelineId = parseInt(targetVal.split(':')[1]);
        else if (targetVal.startsWith('step:')) body.stepKey = targetVal.split(':')[1];
        try {
            await api.createSchedule(projectId, body);
            closeModal('scheduleModal');
            loadSchedules();
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    });

    function renderScheduleTargets() {
        const sel = document.getElementById('schTarget');
        sel.innerHTML = [
            ...pipelines.map(p => `<option value="pipeline:${p.id}">流水线: ${escHtml(p.name)}</option>`),
            ...steps.map(s => `<option value="step:${s.key}">步骤: ${escHtml(s.displayName)}</option>`),
        ].join('') || '<option value="">（暂无可选目标）</option>';
    }

    window.toggleSchedule = async (id) => {
        try { await api.toggleSchedule(projectId, id); loadSchedules(); }
        catch (err) { showFlash(flashEl, err.message, 'error'); }
    };

    window.deleteSchedule = async (id) => {
        if (!confirm('确认删除该调度？')) return;
        try { await api.deleteSchedule(projectId, id); loadSchedules(); }
        catch (err) { showFlash(flashEl, err.message, 'error'); }
    };

    // -----------------------------------------------------------------------
    // Tab switch: lazy-load data
    // -----------------------------------------------------------------------
    let tabLoaded = {};
    document.querySelectorAll('#tabsContainer .tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;
            if (tabLoaded[tab]) return;
            tabLoaded[tab] = true;
            if (tab === 'files') browseFiles('scripts', '');
            if (tab === 'pipelines') { loadPipelines(); loadStepCards(); }
            if (tab === 'jobs') loadJobs(0);
            if (tab === 'schedules') loadSchedules();
        });
    });

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------
    loadProject();
})();
