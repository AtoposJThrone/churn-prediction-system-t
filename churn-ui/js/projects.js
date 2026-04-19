// js/projects.js — Project list page
(function () {
    requireAuth();

    // DOM refs
    const userLabel     = document.getElementById('userLabel');
    const logoutBtn     = document.getElementById('logoutBtn');
    const flashEl       = document.getElementById('flash');
    const tbodyEl       = document.getElementById('projectsTbody');
    const newBtn        = document.getElementById('newProjectBtn');
    const modalOverlay  = document.getElementById('projectModal');
    const modalTitle    = document.getElementById('modalTitle');
    const modalForm     = document.getElementById('projectForm');
    const cancelBtn     = document.getElementById('cancelModalBtn');
    const saveBtn       = document.getElementById('saveProjectBtn');
    const testConnBtn   = document.getElementById('testConnBtn');
    const connResult    = document.getElementById('connResult');
    const modalFlashEl  = document.getElementById('modalFlash');
    const deleteModal   = document.getElementById('deleteModal');
    const confirmDelBtn = document.getElementById('confirmDeleteBtn');
    const cancelDelBtn  = document.getElementById('cancelDeleteBtn');

    let projects = [];
    let editingId = null;
    let deletingId = null;

    userLabel.textContent = getUser()?.username || '';
    logoutBtn.addEventListener('click', () => { clearAuth(); location.href = 'login.html'; });

    // ---- Load list ----
    async function loadProjects() {
        tbodyEl.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:20px;"><span class="spinner"></span></td></tr>';
        try {
            projects = await api.listProjects();
            renderTable();
        } catch (err) {
            showFlash(flashEl, err.message, 'error');
            tbodyEl.innerHTML = '<tr><td colspan="6" class="text-muted" style="text-align:center;padding:20px;">加载失败</td></tr>';
        }
    }

    function renderTable() {
        if (!projects.length) {
            tbodyEl.innerHTML = '<tr><td colspan="6" class="text-muted" style="text-align:center;padding:24px;">暂无项目，点击右上角「新建项目」开始</td></tr>';
            return;
        }
        tbodyEl.innerHTML = projects.map(p => `
            <tr>
              <td>${escHtml(p.name)}</td>
              <td class="ellipsis">${escHtml(p.description || '—')}</td>
              <td>${escHtml(p.host || '—')}</td>
              <td><span class="text-muted" style="font-size:.8rem;">${fmtDate(p.createdAt)}</span></td>
              <td><span class="text-muted" style="font-size:.8rem;">${fmtDate(p.updatedAt)}</span></td>
              <td>
                <div class="btn-group">
                  <a href="project.html?id=${p.id}" class="btn btn-outline btn-sm">详情</a>
                  <button class="btn btn-outline btn-sm" onclick="editProject(${p.id})">编辑</button>
                  <button class="btn btn-danger btn-sm" onclick="confirmDelete(${p.id})">删除</button>
                </div>
              </td>
            </tr>`).join('');
    }

    // ---- Modal: new / edit ----
    newBtn.addEventListener('click', () => openModalForm(null));

    window.editProject = async (id) => {
        try {
            const p = await api.getProject(id);
            openModalForm(p);
        } catch (err) {
            showFlash(flashEl, err.message, 'error');
        }
    };

    function openModalForm(p) {
        editingId = p ? p.id : null;
        modalTitle.textContent = p ? '编辑项目' : '新建项目';
        connResult.textContent = '';
        modalFlashEl.innerHTML = '';
        fillForm(p);
        openModal('projectModal');
    }

    function fillForm(p) {
        const f = modalForm;
        const v = (name, val) => { const el = f.querySelector(`[name="${name}"]`); if (el) el.value = val ?? ''; };
        v('name', p?.name); v('description', p?.description);
        v('host', p?.host); v('sshPort', p?.sshPort || 22);
        v('projectRoot', p?.projectRoot); v('scriptsDir', p?.scriptsDir);
        v('alertOutputDir', p?.alertOutputDir); v('experimentResultsDir', p?.experimentResultsDir);
        v('plotsDir', p?.plotsDir); v('logsDir', p?.logsDir);
        v('originDataDir', p?.originDataDir); v('transformedDataDir', p?.transformedDataDir);
        v('hiveDb', p?.hiveDb); v('hdfsLandingDir', p?.hdfsLandingDir);
        v('pythonCommand', p?.pythonCommand || 'python3');
        v('sparkSubmitCommand', p?.sparkSubmitCommand || 'spark-submit --master yarn --deploy-mode client');
        v('beelineCommand', p?.beelineCommand || 'beeline -u');
        v('hadoopConfDir', p?.hadoopConfDir || '');
        v('yarnConfDir', p?.yarnConfDir || '');
        // credentials: leave blank on edit (server won't overwrite empty)
        v('sshUsername', p?.sshUsername); v('sshPassword', ''); v('sshPrivateKey', '');
        v('mysqlUrl', p?.mysqlUrl); v('mysqlUsername', p?.mysqlUsername); v('mysqlPassword', '');
        v('hiveJdbcUrl', p?.hiveJdbcUrl); v('hiveUsername', p?.hiveUsername); v('hivePassword', '');
    }

    cancelBtn.addEventListener('click', () => closeModal('projectModal'));
    modalOverlay.addEventListener('click', (e) => { if (e.target === modalOverlay) closeModal('projectModal'); });

    // Test connection button
    testConnBtn.addEventListener('click', async () => {
        if (!editingId) {
            connResult.innerHTML = '<span style="color:#f59e0b">请先保存项目后再测试连接。</span>';
            return;
        }
        testConnBtn.disabled = true;
        connResult.innerHTML = '<span style="color:var(--muted)">测试中，最长等待 20 秒…</span>';
        try {
            const data = await api.testConnection(editingId);

            // SSH result
            const sshOk  = data.ssh?.ok;
            const sshMsg = data.ssh?.message || (sshOk ? '连接成功' : '连接失败');
            const sshMs  = data.ssh?.elapsedMs != null ? `（${data.ssh.elapsedMs} ms）` : '';
            const sshLine = sshOk
                ? `<span style="color:#22c55e">✅ SSH 连接成功 ${sshMs}</span>`
                : `<span style="color:#ef4444">❌ SSH 连接失败 ${sshMs}：${escHtml(sshMsg)}</span>`;

            // MySQL result
            let mysqlLine = '';
            if (data.mysql) {
                const myOk  = data.mysql.ok;
                const myMsg = data.mysql.message || (myOk ? '连接成功' : '连接失败');
                const myMs  = data.mysql.elapsedMs != null ? `（${data.mysql.elapsedMs} ms）` : '';
                mysqlLine = myOk
                    ? `<span style="color:#22c55e">✅ MySQL 连接成功 ${myMs}</span>`
                    : `<span style="color:#ef4444">❌ MySQL 连接失败 ${myMs}：${escHtml(myMsg)}</span>`;
            }

            connResult.innerHTML = [sshLine, mysqlLine].filter(Boolean).join('<br>');
        } catch (err) {
            connResult.innerHTML = `<span style="color:#ef4444">❌ 请求出错：${escHtml(err.message)}</span>`;
        } finally {
            testConnBtn.disabled = false;
        }
    });

    // Save
    saveBtn.addEventListener('click', async () => {
        const body = collectForm();
        if (!body.name?.trim()) { showFlash(modalFlashEl, '项目名称不能为空。', 'error'); return; }
        if (!body.host?.trim()) { showFlash(modalFlashEl, '主机地址不能为空。', 'error'); return; }
        saveBtn.disabled = true;
        try {
            if (editingId) {
                await api.updateProject(editingId, body);
                closeModal('projectModal');
                showFlash(flashEl, '项目已更新。', 'success');
            } else {
                await api.createProject(body);
                closeModal('projectModal');
                showFlash(flashEl, '项目已创建。', 'success');
            }
            loadProjects();
        } catch (err) {
            showFlash(modalFlashEl, err.message, 'error');
        } finally {
            saveBtn.disabled = false;
        }
    });

    function collectForm() {
        const f = modalForm;
        const g = name => f.querySelector(`[name="${name}"]`)?.value || '';
        return {
            name: g('name'), description: g('description'),
            host: g('host'), sshPort: parseInt(g('sshPort')) || 22,
            projectRoot: g('projectRoot'), scriptsDir: g('scriptsDir'),
            alertOutputDir: g('alertOutputDir'), experimentResultsDir: g('experimentResultsDir'),
            plotsDir: g('plotsDir'), logsDir: g('logsDir'),
            originDataDir: g('originDataDir'), transformedDataDir: g('transformedDataDir'),
            hiveDb: g('hiveDb'), hdfsLandingDir: g('hdfsLandingDir'),
            pythonCommand: g('pythonCommand'), sparkSubmitCommand: g('sparkSubmitCommand'),
            beelineCommand: g('beelineCommand'),
            hadoopConfDir: g('hadoopConfDir') || null,
            yarnConfDir: g('yarnConfDir') || null,
            sshUsername: g('sshUsername'), sshPassword: g('sshPassword') || null,
            sshPrivateKey: g('sshPrivateKey') || null,
            mysqlUrl: g('mysqlUrl'), mysqlUsername: g('mysqlUsername'), mysqlPassword: g('mysqlPassword') || null,
            hiveJdbcUrl: g('hiveJdbcUrl'), hiveUsername: g('hiveUsername'), hivePassword: g('hivePassword') || null,
        };
    }

    // ---- Delete ----
    window.confirmDelete = (id) => {
        deletingId = id;
        const p = projects.find(x => x.id === id);
        document.getElementById('deleteProjectName').textContent = p?.name || id;
        openModal('deleteModal');
    };
    cancelDelBtn.addEventListener('click', () => closeModal('deleteModal'));
    confirmDelBtn.addEventListener('click', async () => {
        if (!deletingId) return;
        confirmDelBtn.disabled = true;
        try {
            await api.deleteProject(deletingId);
            closeModal('deleteModal');
            showFlash(flashEl, '项目已删除。', 'success');
            loadProjects();
        } catch (err) {
            showFlash(flashEl, err.message, 'error');
        } finally {
            confirmDelBtn.disabled = false;
        }
    });

    loadProjects();
})();
