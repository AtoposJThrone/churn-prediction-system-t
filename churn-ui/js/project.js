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
    const fileScopeSelect   = document.getElementById('fileScopeSelect');
    const fileListEl        = document.getElementById('fileList');
    const currentBrowsePath = document.getElementById('currentBrowsePath');

    // Module-level state — avoids JSON.stringify-in-onclick double-quote breakage
    let _fb = { entries: [], scope: 'scripts', path: '' };

    window.onScopeChange = () => browseFiles(fileScopeSelect.value, '');

    async function browseFiles(scope, path) {
        fileListEl.innerHTML = '<span class="spinner"></span>';
        try {
            const entries = await api.listFiles(projectId, scope, path || '');
            renderFileList(entries, scope, path || '');
        } catch (err) {
            fileListEl.innerHTML = `<p class="text-danger">${escHtml(err.message)}</p>`;
        }
    }

    function fmtSize(bytes) {
        if (bytes == null) return '';
        if (bytes < 1024) return bytes + ' B';
        if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
        return (bytes / 1048576).toFixed(1) + ' MB';
    }

    function renderFileList(entries, scope, currentPath) {
        _fb = { entries, scope, path: currentPath };
        currentBrowsePath.textContent = currentPath || '/';

        let upRow = '';
        if (currentPath) {
            const parent = currentPath.replace(/\/[^\/]+$/, '') || '';
            upRow = `<div class="file-entry fb-up" data-parent="${escAttr(parent)}">
              <span class="icon">📂</span><span class="text-muted">..</span>
              <button class="btn btn-outline btn-xs" style="margin-left:auto;">返回上级</button>
            </div>`;
        }

        if (!entries?.length) {
            fileListEl.innerHTML = upRow + '<p class="text-muted" style="padding:8px 4px;">目录为空</p>';
            return;
        }

        fileListEl.innerHTML = upRow + entries.map((e, i) => {
            const icon = e.directory ? '📁' : '📄';
            const sizeStr = fmtSize(e.size);
            return `<div class="file-entry fb-row" data-idx="${i}">
              <span class="icon">${icon}</span>
              <span class="file-name">${escHtml(e.name)}</span>
              ${sizeStr ? `<span class="file-size">${sizeStr}</span>` : ''}
              <button class="btn btn-outline btn-xs" style="margin-left:auto;flex-shrink:0;">${e.directory ? '进入 ›' : '查看'}</button>
            </div>`;
        }).join('');
    }

    // Event delegation — no JSON needed in onclick attributes
    fileListEl.addEventListener('click', e => {
        const upEl = e.target.closest('.fb-up');
        if (upEl) { browseFiles(_fb.scope, upEl.dataset.parent || ''); return; }

        const row = e.target.closest('.fb-row[data-idx]');
        if (!row) return;
        const entry = _fb.entries[parseInt(row.dataset.idx, 10)];
        if (!entry) return;
        if (entry.directory) {
            browseFiles(_fb.scope, entry.path || '');
        } else {
            openFilePreview(entry);
        }
    });

    // ---- File preview modal ----
    const TEXT_EXTS = new Set(['py','sql','txt','csv','json','log','md','xml','yaml','yml',
                                'sh','bash','conf','properties','java','js','ts','html','css',
                                'ini','toml','r','scala','out']);

    async function openFilePreview(entry) {
        const ext = (entry.name.match(/\.([^.]+)$/) || [])[1]?.toLowerCase() || '';
        document.getElementById('previewModalPath').textContent = entry.path || entry.name;
        document.getElementById('previewModalExt').textContent = ext ? '.' + ext : '';
        const contentEl = document.getElementById('previewContent');
        const infoEl    = document.getElementById('previewLineInfo');
        contentEl.textContent = '加载中…';
        infoEl.textContent = '';
        openModal('filePreviewModal');

        if (!TEXT_EXTS.has(ext)) {
            contentEl.textContent = `暂不支持预览 .${ext || '此类型'} 文件（仅支持文本类文件）`;
            return;
        }
        try {
            const data = await api.getFileContent(projectId, entry.path, 500);
            const text = data?.content ?? '(空文件)';
            contentEl.textContent = text;
            const lineCount = (text.match(/\n/g) || []).length + 1;
            infoEl.textContent = text.includes('仅显示前 500 行') ? '仅显示前 500 行（文件较大）' : `共 ${lineCount} 行`;
        } catch (err) {
            contentEl.textContent = '读取失败：' + err.message;
        }
    }

    document.getElementById('closePreviewBtn').addEventListener('click', () => closeModal('filePreviewModal'));

    function escAttr(s) { return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;'); }

    // -----------------------------------------------------------------------
    // Pipeline Workbench
    // -----------------------------------------------------------------------
    let steps = [];         // StepCatalog entries (for schedule targets)
    let pipelines = [];     // PipelineTemplate list
    let scriptFiles = [];   // .py files detected from scripts dir
    let wbItems = [];       // workbench components: [{uid,type,label,scriptFile,startCmd,postCmd}]
    let wbSelectedIdx = -1; // selected workbench item index
    let currentPlId = null; // loaded pipeline id (null = new)

    // --- Load data ---
    async function loadPipelineTab() {
        [steps, pipelines] = await Promise.all([
            api.listSteps().catch(() => []),
            api.listPipelines(projectId).catch(() => []),
        ]);
        renderPipelineSelect();
        loadScriptFiles();
    }

    async function loadScriptFiles() {
        const listEl = document.getElementById('scriptFileList');
        listEl.innerHTML = '<span class="spinner" style="margin:16px auto;display:block;"></span>';
        try {
            const entries = await api.listFiles(projectId, 'scripts', '');
            scriptFiles = (entries || []).filter(e => !e.directory && e.name.endsWith('.py'));
            renderScriptList();
        } catch (err) {
            listEl.innerHTML = `<p class="text-muted" style="padding:12px;font-size:.82rem;">加载失败：${escHtml(err.message)}</p>`;
        }
    }

    function renderScriptList() {
        const el = document.getElementById('scriptFileList');
        if (!scriptFiles.length) {
            el.innerHTML = '<p class="text-muted" style="padding:12px;font-size:.82rem;">未检测到 .py 脚本文件。请确认项目已配置脚本目录。</p>';
            return;
        }
        el.innerHTML = scriptFiles.map(f => `
          <div class="wb-script-item" data-file="${escAttr(f.name)}">
            <span class="icon">📄</span>
            <span class="fname">${escHtml(f.name)}</span>
            <button class="btn btn-outline btn-xs">+ 添加</button>
          </div>`).join('');
    }

    // Script list click delegation
    document.getElementById('scriptFileList').addEventListener('click', e => {
        const item = e.target.closest('.wb-script-item');
        if (!item) return;
        addScriptToWb(item.dataset.file);
    });

    document.getElementById('refreshScriptsBtn').addEventListener('click', loadScriptFiles);

    // --- Workbench operations ---
    function genUid() { return 'c' + Date.now().toString(36) + Math.random().toString(36).substr(2, 4); }

    function addScriptToWb(filename) {
        wbItems.push({
            uid: genUid(),
            type: 'script',
            label: filename,
            scriptFile: filename,
            startCmd: '',
            postCmd: '',
        });
        renderWorkbench();
        selectWbItem(wbItems.length - 1);
    }

    function addPresetToWb(label, cmd) {
        wbItems.push({
            uid: genUid(),
            type: 'preset',
            label: label,
            scriptFile: null,
            startCmd: cmd,
            postCmd: '',
        });
        renderWorkbench();
        selectWbItem(wbItems.length - 1);
    }

    function renderWorkbench() {
        const el = document.getElementById('workbenchList');
        if (!wbItems.length) {
            el.innerHTML = '<p class="wb-placeholder">将左侧脚本添加到此处，或创建预设命令组件</p>';
            return;
        }
        el.innerHTML = wbItems.map((item, i) => {
            const sel = i === wbSelectedIdx ? ' selected' : '';
            const typeLabel = item.type === 'preset' ? '预设' : '脚本';
            const typeCls   = item.type === 'preset' ? 'badge-info' : 'badge-secondary';
            const cmdOk = item.startCmd?.trim();
            const cmdHtml = cmdOk
                ? '<span class="wb-cmd-status wb-cmd-ok">✓ 已配置</span>'
                : '<span class="wb-cmd-status wb-cmd-miss">✗ 未配置</span>';
            return `<div class="wb-item${sel}" data-idx="${i}">
              <span class="wb-seq">${i + 1}</span>
              <span class="wb-label">${escHtml(item.label)}</span>
              <span class="wb-type badge ${typeCls}">${typeLabel}</span>
              ${cmdHtml}
              <div class="wb-actions">
                <button class="btn btn-outline btn-xs wb-up" title="上移">↑</button>
                <button class="btn btn-outline btn-xs wb-down" title="下移">↓</button>
                <button class="btn btn-danger btn-xs wb-del" title="移除">×</button>
              </div>
            </div>`;
        }).join('');
    }

    // Workbench click delegation
    document.getElementById('workbenchList').addEventListener('click', e => {
        const row = e.target.closest('.wb-item');
        if (!row) return;
        const idx = parseInt(row.dataset.idx, 10);
        if (e.target.closest('.wb-up'))  { moveWbItem(idx, -1); return; }
        if (e.target.closest('.wb-down')) { moveWbItem(idx, 1); return; }
        if (e.target.closest('.wb-del')) { removeWbItem(idx); return; }
        selectWbItem(idx);
    });

    function moveWbItem(idx, dir) {
        const target = idx + dir;
        if (target < 0 || target >= wbItems.length) return;
        [wbItems[idx], wbItems[target]] = [wbItems[target], wbItems[idx]];
        if (wbSelectedIdx === idx) wbSelectedIdx = target;
        else if (wbSelectedIdx === target) wbSelectedIdx = idx;
        renderWorkbench();
    }

    function removeWbItem(idx) {
        wbItems.splice(idx, 1);
        if (wbSelectedIdx === idx) { wbSelectedIdx = -1; renderCmdConfig(); }
        else if (wbSelectedIdx > idx) wbSelectedIdx--;
        renderWorkbench();
    }

    function selectWbItem(idx) {
        // Save current config before switching
        saveCmdConfigSilent();
        wbSelectedIdx = idx;
        renderWorkbench();
        renderCmdConfig();
    }

    // --- Command config area ---
    function renderCmdConfig() {
        const bodyEl = document.getElementById('cmdConfigBody');
        const labelEl = document.getElementById('cmdConfigLabel');
        if (wbSelectedIdx < 0 || !wbItems[wbSelectedIdx]) {
            labelEl.textContent = '';
            bodyEl.innerHTML = '<p class="wb-placeholder">请在工作台中选择一个组件来配置其启动命令</p>';
            return;
        }
        const item = wbItems[wbSelectedIdx];
        labelEl.textContent = `当前: [${wbSelectedIdx + 1}] ${item.label}`;
        const scriptHint = item.scriptFile
            ? `提示: 可使用 \${TD_CHURN_SCRIPTS_DIR}/${escHtml(item.scriptFile)}`
            : '';
        bodyEl.innerHTML = `
          <div class="form-group">
            <label class="form-label">启动命令（槽位 1）<span class="required">*</span></label>
            <textarea id="compStartCmd" class="form-control code-input" rows="2"
              placeholder="如: python3 \${TD_CHURN_SCRIPTS_DIR}/${escAttr(item.scriptFile || 'script.py')}">${escHtml(item.startCmd || '')}</textarea>
            ${scriptHint ? `<span class="form-text">${scriptHint}</span>` : ''}
          </div>
          <div class="form-group">
            <label class="form-label">结束后命令（槽位 2，可选）</label>
            <textarea id="compPostCmd" class="form-control code-input" rows="2"
              placeholder="脚本运行结束后执行的命令，如清理临时文件等">${escHtml(item.postCmd || '')}</textarea>
          </div>
          <button class="btn btn-primary btn-sm" id="saveCompCmdBtn">💾 保存命令配置</button>
        `;
        document.getElementById('saveCompCmdBtn').addEventListener('click', () => {
            saveCmdConfigSilent();
            renderWorkbench();
            showFlash(flashEl, '命令配置已保存', 'success');
        });
    }

    function saveCmdConfigSilent() {
        if (wbSelectedIdx < 0 || !wbItems[wbSelectedIdx]) return;
        const startEl = document.getElementById('compStartCmd');
        const postEl  = document.getElementById('compPostCmd');
        if (startEl) wbItems[wbSelectedIdx].startCmd = startEl.value;
        if (postEl)  wbItems[wbSelectedIdx].postCmd  = postEl.value;
    }

    // --- Preset command ---
    document.getElementById('addPresetBtn').addEventListener('click', () => {
        const label = document.getElementById('presetLabel').value.trim();
        const cmd   = document.getElementById('presetCmd').value.trim();
        if (!label) { showFlash(flashEl, '请输入预设名称', 'error'); return; }
        if (!cmd)   { showFlash(flashEl, '请输入命令内容', 'error'); return; }
        addPresetToWb(label, cmd);
        document.getElementById('presetLabel').value = '';
        document.getElementById('presetCmd').value = '';
        showFlash(flashEl, `预设命令「${label}」已添加到工作台`, 'success');
    });

    // --- Pipeline selector ---
    function renderPipelineSelect() {
        const sel = document.getElementById('pipelineSelect');
        sel.innerHTML = '<option value="">— 新建流水线 —</option>' +
            pipelines.map(p => `<option value="${p.id}">${escHtml(p.name)}</option>`).join('');
        if (currentPlId) sel.value = currentPlId;
    }

    window.onPipelineSelect = function () {
        saveCmdConfigSilent();
        const id = document.getElementById('pipelineSelect').value;
        if (!id) {
            // New pipeline
            currentPlId = null;
            wbItems = [];
            wbSelectedIdx = -1;
            document.getElementById('plName2').value = '';
            document.getElementById('plDesc2').value = '';
            renderWorkbench();
            renderCmdConfig();
            return;
        }
        const pl = pipelines.find(p => p.id == id);
        if (!pl) return;
        currentPlId = pl.id;
        document.getElementById('plName2').value = pl.name || '';
        document.getElementById('plDesc2').value = pl.description || '';
        // Parse steps from stepKeysJson
        let parsedSteps = [];
        try { parsedSteps = JSON.parse(pl.stepKeysJson); } catch {}
        // Convert old format (string array) to new format
        if (parsedSteps.length && typeof parsedSteps[0] === 'string') {
            parsedSteps = parsedSteps.map(k => {
                const def = steps.find(s => s.key === k);
                return { uid: k, type: 'script', label: def?.displayName || k, scriptFile: def?.scriptFile || k, startCmd: '', postCmd: '' };
            });
        }
        wbItems = parsedSteps.map(s => ({ ...s, uid: s.uid || genUid() }));
        wbSelectedIdx = -1;
        renderWorkbench();
        renderCmdConfig();
    };

    // --- Save pipeline ---
    document.getElementById('savePipelineBtn2').addEventListener('click', async () => {
        saveCmdConfigSilent();
        const name = document.getElementById('plName2').value.trim();
        if (!name) { showFlash(flashEl, '请输入流水线名称', 'error'); return; }
        if (!wbItems.length) { showFlash(flashEl, '工作台中至少需要一个组件', 'error'); return; }
        for (const item of wbItems) {
            if (!item.startCmd?.trim()) {
                showFlash(flashEl, `组件「${item.label}」缺少启动命令，请在命令配置区设置`, 'error');
                return;
            }
        }
        const body = {
            name,
            description: document.getElementById('plDesc2').value.trim(),
            steps: wbItems,
        };
        try {
            let pl;
            if (currentPlId) {
                pl = await api.updatePipeline(projectId, currentPlId, body);
                showFlash(flashEl, '流水线已更新', 'success');
            } else {
                pl = await api.createPipeline(projectId, body);
                currentPlId = pl.id;
                showFlash(flashEl, '流水线已创建', 'success');
            }
            pipelines = await api.listPipelines(projectId);
            renderPipelineSelect();
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    });

    // --- Run pipeline ---
    document.getElementById('runPipelineBtn2').addEventListener('click', async () => {
        if (!currentPlId) { showFlash(flashEl, '请先保存流水线', 'warning'); return; }
        try {
            const job = await api.runPipeline(projectId, currentPlId, {});
            showFlash(flashEl, `流水线已提交，任务 ID: ${job.id}`, 'success');
            setTimeout(() => loadJobs(), 1200);
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    });

    // --- Delete pipeline ---
    document.getElementById('delPipelineBtn2').addEventListener('click', async () => {
        if (!currentPlId) { showFlash(flashEl, '没有选中的流水线', 'warning'); return; }
        if (!confirm('确认删除该流水线？')) return;
        try {
            await api.deletePipeline(projectId, currentPlId);
            currentPlId = null;
            wbItems = [];
            wbSelectedIdx = -1;
            document.getElementById('plName2').value = '';
            document.getElementById('plDesc2').value = '';
            pipelines = await api.listPipelines(projectId);
            renderPipelineSelect();
            renderWorkbench();
            renderCmdConfig();
            showFlash(flashEl, '流水线已删除', 'success');
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
    });

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
          <td style="display:flex;gap:6px;flex-wrap:wrap;">
            <button class="btn btn-outline btn-sm" onclick="viewJobLog(${j.id})">查看日志</button>
            ${(j.status === 'RUNNING' || j.status === 'PENDING') ? `<button class="btn btn-sm" style="background:#e53e3e;color:#fff;border:none;" onclick="cancelJob(${j.id})">中止</button>` : ''}
          </td>
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

    window.cancelJob = async (jobId) => {
        if (!confirm(`确定要中止任务 #${jobId} 吗？`)) return;
        try {
            await api.cancelJob(jobId);
            showFlash(flashEl, `任务 #${jobId} 已中止`, 'success');
            loadJobs();
        } catch (err) {
            showFlash(flashEl, '中止失败：' + err.message, 'error');
        }
    };

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
          <td><code style="font-size:.78rem;">${escHtml(s.cronExpression)}</code></td>
          <td><span class="badge ${s.enabled ? 'badge-success' : 'badge-secondary'}">${s.enabled ? '启用' : '停用'}</span></td>
          <td>${fmtTime(s.lastTriggeredAt)}</td>
          <td>
            <div class="btn-group">
              <button class="btn btn-primary btn-sm" onclick="runScheduleNow(${s.id})">▶ 立即运行</button>
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
        initSchModal();
        openModal('scheduleModal');
    });
    document.getElementById('cancelScheduleBtn').addEventListener('click', () => closeModal('scheduleModal'));

    // ---- Friendly cron builder ----
    function initSchModal() {
        // populate hour/min/monthday selects once
        const hourSel = document.getElementById('schHour');
        const minSel  = document.getElementById('schMin');
        const daysSel = document.getElementById('schMonthday');
        if (!hourSel.options.length) {
            for (let h = 0; h < 24; h++) hourSel.add(new Option(String(h).padStart(2, '0') + ':xx', h));
            for (let m = 0; m < 60; m++) minSel.add(new Option('xx:' + String(m).padStart(2, '0'), m));
            for (let d = 1; d <= 31; d++) daysSel.add(new Option(d + ' 日', d));
        }
        hourSel.value = 2; minSel.value = 0;  // default 02:00
        document.getElementById('schFreq').value = 'daily';
        document.getElementById('schName').value = '';
        onSchFreqChange();
    }

    window.onSchFreqChange = function () {
        const freq = document.getElementById('schFreq').value;
        document.getElementById('schWeekdayGroup').style.display  = freq === 'weekly'   ? '' : 'none';
        document.getElementById('schMonthdayGroup').style.display = freq === 'monthly'  ? '' : 'none';
        document.getElementById('schHourGroup').style.display     = freq === 'interval' ? 'none' : '';
        document.getElementById('schMinGroup').style.display      = freq === 'interval' ? 'none' : '';
        document.getElementById('schIntervalGroup').style.display = freq === 'interval' ? '' : 'none';
        updateCronPreview();
    };

    function buildCronExpression() {
        const freq = document.getElementById('schFreq').value;
        const h    = document.getElementById('schHour').value;
        const m    = document.getElementById('schMin').value;
        switch (freq) {
            case 'daily':    return `0 ${m} ${h} * * *`;
            case 'hourly':   return `0 ${m} * * * *`;
            case 'weekly':   return `0 ${m} ${h} * * ${document.getElementById('schWeekday').value}`;
            case 'monthly':  return `0 ${m} ${h} ${document.getElementById('schMonthday').value} * *`;
            case 'interval': {
                const n = parseInt(document.getElementById('schIntervalMin').value) || 30;
                return `0 0/${n} * * * *`;
            }
            default: return `0 ${m} ${h} * * *`;
        }
    }

    function updateCronPreview() {
        const preview = document.getElementById('schCronPreview');
        if (preview) preview.value = buildCronExpression();
    }

    // Live preview on any change
    ['schHour', 'schMin', 'schWeekday', 'schMonthday', 'schIntervalMin'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.addEventListener('change', updateCronPreview);
    });
    const intEl = document.getElementById('schIntervalMin');
    if (intEl) intEl.addEventListener('input', updateCronPreview);

    document.getElementById('saveScheduleBtn').addEventListener('click', async () => {
        const name = document.getElementById('schName').value.trim();
        const cron = buildCronExpression();
        const targetVal = document.getElementById('schTarget').value;
        if (!name) { showFlash(flashEl, '调度名称不能为空', 'error'); return; }
        if (!targetVal) { showFlash(flashEl, '请选择调度目标（流水线或步骤）', 'error'); return; }
        const body = { name, cronExpression: cron, enabled: true };
        if (targetVal.startsWith('pipeline:')) body.pipelineId = parseInt(targetVal.split(':')[1]);
        else if (targetVal.startsWith('step:')) body.stepKey = targetVal.split(':')[1];
        try {
            await api.createSchedule(projectId, body);
            closeModal('scheduleModal');
            showFlash(flashEl, `调度「${name}」已创建，Cron: ${cron}`, 'success');
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

    window.runScheduleNow = async (id) => {
        try {
            const job = await api.runScheduleNow(projectId, id);
            showFlash(flashEl, `已立即触发，任务 ID: ${job.id}`, 'success');
            setTimeout(() => loadJobs(), 1200);
        } catch (err) { showFlash(flashEl, err.message, 'error'); }
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
            if (tab === 'files') browseFiles(fileScopeSelect.value, '');
            if (tab === 'pipelines') loadPipelineTab();
            if (tab === 'jobs') loadJobs(0);
            if (tab === 'schedules') loadSchedules();
        });
    });

    // -----------------------------------------------------------------------
    // Init
    // -----------------------------------------------------------------------
    window.loadJobs = loadJobs;   // expose for HTML onclick="loadJobs()"
    loadProject();
})();
