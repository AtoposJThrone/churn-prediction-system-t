// js/login.js — Login page logic
(function () {
    // If already logged in, go to projects
    if (getToken()) {
        location.href = 'projects.html';
        return;
    }

    const form      = document.getElementById('loginForm');
    const errEl     = document.getElementById('loginError');
    const submitBtn = document.getElementById('submitBtn');

    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        errEl.textContent = '';
        const username = form.username.value.trim();
        const password = form.password.value;
        if (!username || !password) { errEl.textContent = '请输入用户名和密码。'; return; }

        submitBtn.disabled = true;
        submitBtn.innerHTML = '<span class="spinner"></span> 登录中...';

        try {
            const data = await api.login(username, password);
            setToken(data.token);
            setUser({ username: data.username });
            location.href = 'projects.html';
        } catch (err) {
            errEl.textContent = err.message || '登录失败，请检查用户名和密码。';
            submitBtn.disabled = false;
            submitBtn.textContent = '登  录';
        }
    });
})();
