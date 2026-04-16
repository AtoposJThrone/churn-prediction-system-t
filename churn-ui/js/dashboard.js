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

    // Load project list for selector
    async function initProjectSelect() {
        try {
            const projects = await api.listProjects();
            projectSel.innerHTML = `<option value="">使用内置演示数据</option>` +
                projects.map(p => `<option value="${p.id}">${escHtml(p.name)}</option>`).join('');
        } catch { /* ignore — still works with fallback */ }
    }

    loadBtn.addEventListener('click', loadDashboard);

    async function loadDashboard() {
        loadBtn.disabled = true;
        loadBtn.innerHTML = '<span class="spinner"></span>';
        try {
            const pid = projectSel.value ? parseInt(projectSel.value) : null;
            const data = pid ? await api.getDashboard(pid) : await api.getDashboardFallback();
            renderDashboard(data);
        } catch (err) {
            showFlash(flashEl, '数据加载失败：' + err.message, 'error');
        } finally {
            loadBtn.disabled = false;
            loadBtn.textContent = '加载 / 刷新';
        }
    }

    function renderDashboard(data) {
        sourceEl.textContent = '数据来源：' + (data.dataSource === 'remote' ? '远程服务器' : data.dataSource === 'mysql' ? 'MySQL ADS' : '内置演示数据');

        renderKpi(data.summary);
        renderRiskPie(data.riskDistribution);
        renderRiskTrend(data.riskTrend);
        renderModelComparison(data.modelComparison);
        renderFeatureImportance(data.featureImportance);
        renderAlertTable(data.recentAlerts);
        renderChurnProbDist(data.recentAlerts);
    }

    // ---- KPI tiles ----
    function renderKpi(s) {
        document.getElementById('kpiTotal').textContent     = s?.totalActiveUsers?.toLocaleString() ?? '—';
        document.getElementById('kpiHigh').textContent      = s?.highRiskCount?.toLocaleString()   ?? '—';
        document.getElementById('kpiMedium').textContent    = s?.mediumRiskCount?.toLocaleString() ?? '—';
        document.getElementById('kpiRate').textContent      = s ? (s.highRiskRate * 100).toFixed(1) + '%' : '—';
        document.getElementById('kpiAvgProb').textContent   = s ? (s.avgChurnProb * 100).toFixed(1) + '%' : '—';
        document.getElementById('kpiDate').textContent      = s?.statDate ?? '—';
        document.getElementById('kpiStuckMap').textContent  = s?.topStuckMapId ?? '—';
        document.getElementById('kpiNoTutorial').textContent = s?.d1NoTutorialCount?.toLocaleString() ?? '—';
    }

    // ---- Risk distribution pie ----
    let pieCht = null;
    function renderRiskPie(data) {
        const el = document.getElementById('pieChart');
        if (!pieCht) pieCht = echarts.init(el);
        pieCht.setOption({
            tooltip: { trigger: 'item', formatter: '{b}: {c} ({d}%)' },
            legend: { bottom: 0, data: data?.map(d => d.name) },
            series: [{
                type: 'pie', radius: ['42%', '70%'],
                label: { show: false },
                data: data?.map(d => ({
                    name: d.name, value: d.value,
                    itemStyle: { color: d.name === '高风险' ? '#dc3545' : d.name === '中风险' ? '#fd7e14' : '#28a745' }
                })) || [],
            }]
        });
    }

    // ---- Risk trend bar ----
    let trendCht = null;
    function renderRiskTrend(data) {
        if (!data?.length) return;
        const el = document.getElementById('trendChart');
        if (!trendCht) trendCht = echarts.init(el);
        const dates  = data.map(d => d.date || d.stat_date || '');
        const high   = data.map(d => d.highRisk ?? d.high_risk_count ?? 0);
        const medium = data.map(d => d.mediumRisk ?? d.medium_risk_count ?? 0);
        const low    = data.map(d => d.lowRisk ?? d.low_risk_count ?? 0);
        trendCht.setOption({
            tooltip: { trigger: 'axis' },
            legend: { data: ['高风险', '中风险', '低风险'] },
            grid: { left: 40, right: 16, bottom: 30, top: 36 },
            xAxis: { type: 'category', data: dates },
            yAxis: { type: 'value' },
            series: [
                { name: '高风险', type: 'bar', stack: 'risk', data: high,   itemStyle: { color: '#dc3545' } },
                { name: '中风险', type: 'bar', stack: 'risk', data: medium, itemStyle: { color: '#fd7e14' } },
                { name: '低风险', type: 'bar', stack: 'risk', data: low,    itemStyle: { color: '#28a745' } },
            ]
        });
    }

    // ---- Model comparison bar ----
    let modelCht = null;
    function renderModelComparison(data) {
        if (!data?.length) return;
        const el = document.getElementById('modelChart');
        if (!modelCht) modelCht = echarts.init(el);
        const labels = data.map(d => `${d.model}\n${d.window}`);
        const aucs   = data.map(d => d.auc ? parseFloat(d.auc).toFixed(4) : 0);
        const f1s    = data.map(d => d.f1  ? parseFloat(d.f1).toFixed(4)  : 0);
        modelCht.setOption({
            tooltip: { trigger: 'axis' },
            legend: { data: ['AUC', 'F1'] },
            grid: { left: 48, right: 16, bottom: 56, top: 36 },
            xAxis: { type: 'category', data: labels, axisLabel: { interval: 0, rotate: 30 } },
            yAxis: { type: 'value', min: 0.5, max: 1 },
            series: [
                { name: 'AUC', type: 'bar', data: aucs, itemStyle: { color: '#3b6bb5' } },
                { name: 'F1',  type: 'bar', data: f1s,  itemStyle: { color: '#17a2b8' } },
            ]
        });
    }

    // ---- Feature importance horizontal bar ----
    let featCht = null;
    function renderFeatureImportance(data) {
        if (!data?.length) return;
        const el = document.getElementById('featChart');
        if (!featCht) featCht = echarts.init(el);
        const top = [...data].sort((a, b) => b.importance - a.importance).slice(0, 12);
        featCht.setOption({
            tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
            grid: { left: 140, right: 24, bottom: 30, top: 16 },
            xAxis: { type: 'value' },
            yAxis: { type: 'category', data: top.map(d => d.feature).reverse() },
            series: [{
                type: 'bar',
                data: top.map(d => parseFloat(d.importance).toFixed(4)).reverse(),
                itemStyle: { color: '#3b6bb5' },
                label: { show: true, position: 'right', fontSize: 11 },
            }]
        });
    }

    // ---- Churn prob distribution histogram ----
    let distCht = null;
    function renderChurnProbDist(alerts) {
        if (!alerts?.length) return;
        const el = document.getElementById('distChart');
        if (!distCht) distCht = echarts.init(el);
        // Build 10 buckets [0,0.1), [0.1,0.2), …
        const buckets = Array(10).fill(0);
        alerts.forEach(a => {
            const idx = Math.min(9, Math.floor((parseFloat(a.churnProb) || 0) * 10));
            buckets[idx]++;
        });
        const labels = ['0–10%','10–20%','20–30%','30–40%','40–50%','50–60%','60–70%','70–80%','80–90%','90–100%'];
        distCht.setOption({
            tooltip: { trigger: 'axis' },
            grid: { left: 40, right: 16, bottom: 50, top: 16 },
            xAxis: { type: 'category', data: labels, axisLabel: { rotate: 30, interval: 0 } },
            yAxis: { type: 'value' },
            series: [{ type: 'bar', data: buckets,
                itemStyle: { color: (params) => params.dataIndex >= 5 ? '#dc3545' : '#3b6bb5' } }]
        });
    }

    // ---- Alert table ----
    function renderAlertTable(alerts) {
        const el = document.getElementById('alertTbody');
        if (!alerts?.length) { el.innerHTML = '<tr><td colspan="5" class="text-muted" style="text-align:center;padding:14px;">暂无数据</td></tr>'; return; }
        el.innerHTML = alerts.slice(0, 50).map(a => `<tr>
          <td>${escHtml(a.userId ?? a.user_id ?? '—')}</td>
          <td>${(parseFloat(a.churnProb ?? a.churn_prob) * 100).toFixed(1)}%</td>
          <td>${riskBadge(a.riskLevel ?? a.risk_level)}</td>
          <td><span class="badge ${(a.finalAlert ?? a.final_alert) == '1' || (a.finalAlert ?? a.final_alert) === true ? 'badge-danger' : 'badge-secondary'}">${(a.finalAlert ?? a.final_alert) == '1' ? '已触发' : '未触发'}</span></td>
          <td class="ellipsis">${escHtml(a.topReason ?? a.top_reason_1 ?? '—')}</td>
        </tr>`).join('');
    }

    // Resize charts on window resize
    window.addEventListener('resize', () => {
        [pieCht, trendCht, modelCht, featCht, distCht].forEach(c => c?.resize());
    });

    // Init
    initProjectSelect();
    loadDashboard();
})();
