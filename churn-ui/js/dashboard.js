// js/dashboard.js — Data dashboard page

(function () {
    requireAuth();

    const userLabel  = document.getElementById('userLabel');
    const logoutBtn  = document.getElementById('logoutBtn');
    const projectSel = document.getElementById('projectSelect');
    const loadBtn    = document.getElementById('loadBtn');
    const flashEl    = document.getElementById('flash');
    const sourceEl   = document.getElementById('dataSourceLabel');

    userLabel.textContent = getUser()?.username || '';
    logoutBtn.addEventListener('click', () => { clearAuth(); location.href = 'login.html'; });

    // ---- ECharts 初始化（暗色主题） ----
    const BG = 'transparent';
    const pieCht   = echarts.init(document.getElementById('pieChart'),   'dark');
    const trendCht = echarts.init(document.getElementById('trendChart'), 'dark');
    const modelCht = echarts.init(document.getElementById('modelChart'), 'dark');
    const featCht  = echarts.init(document.getElementById('featChart'),  'dark');
    const distCht  = echarts.init(document.getElementById('distChart'),  'dark');
    window.addEventListener('resize', () => [pieCht, trendCht, modelCht, featCht, distCht].forEach(c => c.resize()));

    // 共用暗色轴线样式
    const AXIS_COLOR = '#8b949e';
    const GRID_LINE  = { lineStyle: { color: '#21262d' } };
    const AXIS_LINE  = { lineStyle: { color: '#30363d' } };

    // ---- 项目选择器 ----
    async function initProjectSelect() {
        try {
            const projects = await api.listProjects();
            if (projects.length > 0) {
                projectSel.innerHTML =
                    `<option value="">— 内置演示数据 —</option>` +
                    projects.map(p => `<option value="${p.id}">${escHtml(p.name)}</option>`).join('');
            } else {
                projectSel.innerHTML = `<option value="">— 内置演示数据（暂无项目）—</option>`;
            }
        } catch {
            projectSel.innerHTML = `<option value="">— 内置演示数据 —</option>`;
        }
    }

    // 切换项目自动加载
    projectSel.addEventListener('change', loadDashboard);
    loadBtn.addEventListener('click', loadDashboard);

    async function loadDashboard() {
        loadBtn.disabled = true;
        loadBtn.innerHTML = '<span class="spinner"></span>';
        flashEl.innerHTML = '';
        try {
            const pid = projectSel.value ? parseInt(projectSel.value) : null;
            const data = pid ? await api.getDashboard(pid) : await api.getDashboardFallback();
            renderDashboard(data);
        } catch (err) {
            showFlash(flashEl, '数据加载失败：' + err.message + '（请确认后端服务已启动）', 'error');
        } finally {
            loadBtn.disabled = false;
            loadBtn.textContent = '加载 / 刷新';
        }
    }

    function renderDashboard(data) {
        const srcMap = { remote: '远程服务器', mysql: 'MySQL ADS', fallback: '内置演示数据' };
        sourceEl.textContent = '数据来源：' + (srcMap[data.dataSource] || data.dataSource || '未知');

        try { renderKpi(data.summary); } catch(e) { console.warn('KPI render failed', e); }
        try { renderRiskPie(data.riskDistribution); } catch(e) { console.warn('Pie render failed', e); }
        try { renderRiskTrend(data.riskTrend); } catch(e) { console.warn('Trend render failed', e); }
        try { renderModelComparison(data.modelComparison); } catch(e) { console.warn('Model render failed', e); }
        try { renderFeatureImportance(data.featureImportance); } catch(e) { console.warn('Feature render failed', e); }
        try { renderChurnProbDist(data.recentAlerts); } catch(e) { console.warn('Dist render failed', e); }
        try { setAlerts(data.recentAlerts); } catch(e) { console.warn('Alerts render failed', e); }
    }

    // ---- KPI tiles ----
    function renderKpi(s) {
        document.getElementById('kpiTotal').textContent      = s?.totalActiveUsers?.toLocaleString() ?? '—';
        document.getElementById('kpiHigh').textContent       = s?.highRiskCount?.toLocaleString()    ?? '—';
        document.getElementById('kpiMedium').textContent     = s?.mediumRiskCount?.toLocaleString()  ?? '—';
        document.getElementById('kpiRate').textContent       = s ? (s.highRiskRate * 100).toFixed(1) + '%' : '—';
        document.getElementById('kpiAvgProb').textContent    = s ? (s.avgChurnProb * 100).toFixed(1) + '%' : '—';
        document.getElementById('kpiDate').textContent       = s?.statDate ?? '—';
        document.getElementById('kpiStuckMap').textContent   = s?.topStuckMapId ?? '—';
        document.getElementById('kpiNoTutorial').textContent = s?.d1NoTutorialCount?.toLocaleString() ?? '—';
    }

    // ---- 风险分布饼图 ----
    function renderRiskPie(data) {
        pieCht.setOption({
            backgroundColor: BG,
            tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
            legend: { bottom: 4, textStyle: { color: AXIS_COLOR }, data: data?.map(d => d.name) },
            series: [{
                type: 'pie', radius: ['42%', '70%'],
                label: { show: false },
                data: data?.map(d => ({
                    name: d.name, value: d.value,
                    itemStyle: { color: d.name === '高风险' ? '#f85149' : d.name === '中风险' ? '#e3b341' : '#3fb950' }
                })) || [],
            }]
        }, true);
    }

    // ---- 风险趋势堆叠柱状图 ----
    function renderRiskTrend(data) {
        if (!data?.length) return;
        const dates  = data.map(d => d.date || d.stat_date || '');
        const high   = data.map(d => d.highRisk   ?? d.high_risk_count   ?? 0);
        const medium = data.map(d => d.mediumRisk  ?? d.medium_risk_count ?? 0);
        const low    = data.map(d => d.lowRisk     ?? d.low_risk_count    ?? 0);
        trendCht.setOption({
            backgroundColor: BG,
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            legend: { data: ['高风险', '中风险', '低风险'], textStyle: { color: AXIS_COLOR } },
            grid: { left: 44, right: 16, bottom: 34, top: 40 },
            xAxis: { type: 'category', data: dates,
                     axisLabel: { color: AXIS_COLOR, rotate: dates.length > 15 ? 30 : 0, fontSize: 10 },
                     axisLine: AXIS_LINE },
            yAxis: { type: 'value', axisLabel: { color: AXIS_COLOR }, splitLine: GRID_LINE },
            series: [
                { name: '高风险', type: 'bar', stack: 'risk', data: high,   itemStyle: { color: '#f85149' } },
                { name: '中风险', type: 'bar', stack: 'risk', data: medium, itemStyle: { color: '#e3b341' } },
                { name: '低风险', type: 'bar', stack: 'risk', data: low,    itemStyle: { color: '#3fb950' } },
            ]
        }, true);
    }

    // ---- 模型性能对毕柱状图 ----
    function renderModelComparison(data) {
        if (!data?.length) return;
        const labels = data.map(d => `${d.model}\n${d.window}`);
        const aucs   = data.map(d => d.auc ? parseFloat(d.auc).toFixed(4) : 0);
        const f1s    = data.map(d => d.f1  ? parseFloat(d.f1).toFixed(4)  : 0);
        modelCht.setOption({
            backgroundColor: BG,
            tooltip: { trigger: 'axis' },
            legend: { data: ['AUC', 'F1'], textStyle: { color: AXIS_COLOR } },
            grid: { left: 48, right: 16, bottom: 60, top: 40 },
            xAxis: { type: 'category', data: labels,
                     axisLabel: { interval: 0, rotate: 30, color: AXIS_COLOR, fontSize: 10 },
                     axisLine: AXIS_LINE },
            yAxis: { type: 'value', min: 0.5, max: 1, axisLabel: { color: AXIS_COLOR }, splitLine: GRID_LINE },
            series: [
                { name: 'AUC', type: 'bar', data: aucs, itemStyle: { color: '#58a6ff' } },
                { name: 'F1',  type: 'bar', data: f1s,  itemStyle: { color: '#79c0ff' } },
            ]
        }, true);
    }

    // ---- 特征重要性水平柱状图（Top 10） ----
    function renderFeatureImportance(data) {
        if (!data?.length) return;
        const top = [...data].sort((a, b) => b.importance - a.importance).slice(0, 10);
        featCht.setOption({
            backgroundColor: BG,
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            grid: { left: 160, right: 60, bottom: 20, top: 10 },
            xAxis: { type: 'value', axisLabel: { color: AXIS_COLOR }, splitLine: GRID_LINE },
            yAxis: { type: 'category', data: top.map(d => d.feature).reverse(),
                     axisLabel: { color: '#c9d1d9', fontSize: 12 } },
            series: [{
                type: 'bar',
                data: top.map(d => parseFloat(d.importance).toFixed(4)).reverse(),
                itemStyle: {
                    color: {
                        type: 'linear', x: 0, y: 0, x2: 1, y2: 0,
                        colorStops: [
                            { offset: 0, color: '#1f6feb' },
                            { offset: 1, color: '#58a6ff' }
                        ]
                    }
                },
                label: { show: true, position: 'right', fontSize: 11, color: AXIS_COLOR },
            }]
        }, true);
    }

    // ---- 流失概率分布直方图 ----
    function renderChurnProbDist(alerts) {
        if (!alerts?.length) return;
        const buckets = Array(10).fill(0);
        alerts.forEach(a => {
            const idx = Math.min(9, Math.floor((parseFloat(a.churnProb) || 0) * 10));
            buckets[idx]++;
        });
        const labels = ['0–10%','10–20%','20–30%','30–40%','40–50%','50–60%','60–70%','70–80%','80–90%','90–100%'];
        distCht.setOption({
            backgroundColor: BG,
            tooltip: { trigger: 'axis' },
            grid: { left: 40, right: 16, bottom: 54, top: 16 },
            xAxis: { type: 'category', data: labels,
                     axisLabel: { rotate: 30, interval: 0, color: AXIS_COLOR, fontSize: 10 },
                     axisLine: AXIS_LINE },
            yAxis: { type: 'value', axisLabel: { color: AXIS_COLOR }, splitLine: GRID_LINE },
            series: [{ type: 'bar', data: buckets.map((v, i) => ({
                value: v,
                itemStyle: { color: i < 5 ? '#58a6ff' : i < 8 ? '#e3b341' : '#f85149' }
            })) }]
        }, true);
    }

    // ---- 告警表格：排序 + 筛选 ----
    let _alerts = [];
    let _sortField = 'churnProb';
    let _sortDir   = -1;  // -1 降序
    let _filter    = { riskLevel: '', finalAlert: '', topReason: '' };

    function setAlerts(alerts) {
        _alerts = alerts || [];
        refreshAlertTable();
    }

    function refreshAlertTable() {
        let rows = [..._alerts];
        // 筛选
        if (_filter.riskLevel)  rows = rows.filter(a => (a.riskLevel ?? a.risk_level) === _filter.riskLevel);
        if (_filter.finalAlert !== '') rows = rows.filter(a => String(a.finalAlert ?? a.final_alert) === _filter.finalAlert);
        if (_filter.topReason) {
            const kw = _filter.topReason.toLowerCase();
            rows = rows.filter(a => (a.topReason ?? a.top_reason_1 ?? '').toLowerCase().includes(kw));
        }
        // 排序
        const riskOrder = { high: 2, medium: 1, low: 0 };
        rows.sort((a, b) => {
            let va, vb;
            switch (_sortField) {
                case 'churnProb':
                    va = parseFloat(a.churnProb ?? a.churn_prob) || 0;
                    vb = parseFloat(b.churnProb ?? b.churn_prob) || 0;
                    break;
                case 'riskLevel':
                    va = riskOrder[a.riskLevel ?? a.risk_level] ?? -1;
                    vb = riskOrder[b.riskLevel ?? b.risk_level] ?? -1;
                    break;
                case 'finalAlert':
                    va = parseInt(a.finalAlert ?? a.final_alert) || 0;
                    vb = parseInt(b.finalAlert ?? b.final_alert) || 0;
                    break;
                case 'topReason':
                    va = (a.topReason ?? a.top_reason_1 ?? '').toLowerCase();
                    vb = (b.topReason ?? b.top_reason_1 ?? '').toLowerCase();
                    break;
                default: va = 0; vb = 0;
            }
            return va < vb ? _sortDir : va > vb ? -_sortDir : 0;
        });
        rows = rows.slice(0, 50);
        // 更新排序箭头图标
        ['churnProb', 'riskLevel', 'finalAlert', 'topReason'].forEach(f => {
            const el = document.getElementById('sort_' + f);
            if (el) el.textContent = f === _sortField ? (_sortDir === -1 ? ' ↓' : ' ↑') : ' ↕';
        });
        const countEl = document.getElementById('alertCount');
        if (countEl) countEl.textContent = `显示 ${rows.length} 条`;
        renderAlertRows(rows);
    }

    // 排序单击（由 HTML onclick 调用）
    window.sortAlert = function (field) {
        if (_sortField === field) _sortDir *= -1;
        else { _sortField = field; _sortDir = -1; }
        refreshAlertTable();
    };

    document.getElementById('filterRisk').addEventListener('change', e => {
        _filter.riskLevel = e.target.value; refreshAlertTable();
    });
    document.getElementById('filterFinalAlert').addEventListener('change', e => {
        _filter.finalAlert = e.target.value; refreshAlertTable();
    });
    document.getElementById('filterReason').addEventListener('input', e => {
        _filter.topReason = e.target.value; refreshAlertTable();
    });
    document.getElementById('clearFiltersBtn').addEventListener('click', () => {
        _filter = { riskLevel: '', finalAlert: '', topReason: '' };
        document.getElementById('filterRisk').value = '';
        document.getElementById('filterFinalAlert').value = '';
        document.getElementById('filterReason').value = '';
        refreshAlertTable();
    });

    function renderAlertRows(rows) {
        const el = document.getElementById('alertTbody');
        if (!rows?.length) {
            el.innerHTML = '<tr><td colspan="5" class="text-muted" style="text-align:center;padding:14px;">暂无数据</td></tr>';
            return;
        }
        el.innerHTML = rows.map(a => `<tr>
          <td style="font-family:monospace;font-size:.82rem;">${escHtml(String(a.userId ?? a.user_id ?? '—'))}</td>
          <td>${(parseFloat(a.churnProb ?? a.churn_prob) * 100).toFixed(1)}%</td>
          <td>${riskBadge(a.riskLevel ?? a.risk_level)}</td>
          <td><span class="badge ${(a.finalAlert ?? a.final_alert) == '1' ? 'badge-danger' : 'badge-secondary'}">${(a.finalAlert ?? a.final_alert) == '1' ? '已触发' : '未触发'}</span></td>
          <td class="ellipsis">${escHtml(a.topReason ?? a.top_reason_1 ?? '—')}</td>
        </tr>`).join('');
    }

    // ---- 初始化 ----
    initProjectSelect();
    loadDashboard();
})();
