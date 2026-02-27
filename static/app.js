// ==================== DOM ====================
const tokenHeader   = document.getElementById("token-header");
const tokenBody     = document.getElementById("token-body");
const tokenInput    = document.getElementById("token-input");
const tokenSaveBtn  = document.getElementById("token-save-btn");
const tokenStatus   = document.getElementById("token-status");
const codeInput     = document.getElementById("code-input");
const addCodeBtn    = document.getElementById("add-code-btn");
const tagList       = document.getElementById("tag-list");
const startDateEl   = document.getElementById("start-date");
const endDateEl     = document.getElementById("end-date");
const yearsEl       = document.getElementById("years");
const startBtn      = document.getElementById("start-btn");
const progressSec   = document.getElementById("progress-section");
const taskStateEl   = document.getElementById("task-state");
const progressBar   = document.getElementById("progress-bar");
const logArea       = document.getElementById("log-area");
const fileList      = document.getElementById("file-list");
const refreshBtn    = document.getElementById("refresh-files-btn");

// ==================== 股票代码列表 ====================
const stockCodes = [];

function normalizeCode(raw) {
    const s = raw.trim().toUpperCase();
    if (!s) return null;
    // 已带后缀
    if (/^\d{6}\.(SH|SZ)$/.test(s)) return s;
    // 纯数字6位，自动补后缀
    if (/^\d{6}$/.test(s)) {
        return (s[0] === "6" || s[0] === "9") ? s + ".SH" : s + ".SZ";
    }
    return null; // 格式不对
}

function addCode() {
    const code = normalizeCode(codeInput.value);
    if (!code) {
        codeInput.classList.add("shake");
        setTimeout(() => codeInput.classList.remove("shake"), 400);
        return;
    }
    if (stockCodes.includes(code)) {
        codeInput.value = "";
        return;
    }
    stockCodes.push(code);
    renderTags();
    codeInput.value = "";
    codeInput.focus();
}

function removeCode(code) {
    const idx = stockCodes.indexOf(code);
    if (idx !== -1) {
        stockCodes.splice(idx, 1);
        renderTags();
    }
}

function renderTags() {
    tagList.innerHTML = stockCodes.map(code => `
        <span class="tag">
            ${code}
            <span class="tag-close" data-code="${code}">&times;</span>
        </span>
    `).join("");
}

tagList.addEventListener("click", (e) => {
    const close = e.target.closest(".tag-close");
    if (close) removeCode(close.dataset.code);
});

addCodeBtn.addEventListener("click", addCode);
codeInput.addEventListener("keydown", (e) => {
    if (e.key === "Enter") { e.preventDefault(); addCode(); }
});

// ==================== 日期默认值 ====================
function formatDate(d) {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const day = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${day}`;
}

function setDefaultDates() {
    const today = new Date();
    endDateEl.value = formatDate(today);
    const start = new Date(today);
    start.setFullYear(start.getFullYear() - 3);
    startDateEl.value = formatDate(start);
}

// 年数变化时联动起始日期
yearsEl.addEventListener("change", () => {
    const years = parseInt(yearsEl.value) || 3;
    const end = endDateEl.value ? new Date(endDateEl.value) : new Date();
    const start = new Date(end);
    start.setFullYear(start.getFullYear() - years);
    startDateEl.value = formatDate(start);
});

// ==================== API helpers ====================
async function api(path, opts = {}) {
    const res = await fetch(path, {
        headers: { "Content-Type": "application/json" },
        ...opts,
    });
    if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || res.statusText);
    }
    return res.json();
}

// ==================== Token ====================
let tokenConfigured = false;

async function loadTokenStatus() {
    try {
        const data = await api("/api/token");
        tokenConfigured = data.configured;
        if (data.configured) {
            tokenStatus.textContent = data.masked;
            tokenStatus.className = "status-badge ok";
            tokenBody.style.display = "none";
        } else {
            tokenStatus.textContent = "未配置";
            tokenStatus.className = "status-badge error";
            tokenBody.style.display = "block";
        }
    } catch { /* ignore */ }
}

tokenHeader.addEventListener("click", () => {
    tokenBody.style.display = tokenBody.style.display === "none" ? "block" : "none";
});

// 阻止点击 token-body 内部时冒泡到 header
document.getElementById("token-body").addEventListener("click", (e) => e.stopPropagation());

tokenSaveBtn.addEventListener("click", async () => {
    const token = tokenInput.value.trim();
    if (!token) return;
    try {
        await api("/api/token", {
            method: "POST",
            body: JSON.stringify({ token }),
        });
        tokenInput.value = "";
        await loadTokenStatus();
    } catch (e) {
        alert("保存失败: " + e.message);
    }
});

// ==================== Query ====================
startBtn.addEventListener("click", async () => {
    if (!stockCodes.length) { alert("请先添加股票代码"); return; }

    const codes = stockCodes.join(",");
    const body = { codes, years: parseInt(yearsEl.value) || 3 };
    if (startDateEl.value) body.start_date = startDateEl.value.replace(/-/g, "");
    if (endDateEl.value)   body.end_date   = endDateEl.value.replace(/-/g, "");

    startBtn.disabled = true;
    logArea.textContent = "";

    try {
        const data = await api("/api/query", {
            method: "POST",
            body: JSON.stringify(body),
        });
        progressSec.style.display = "block";
        setTaskState("running");
        progressBar.style.width = "0%";
        progressBar.textContent = "0%";
        progressBar.classList.add("active");
        connectWS(data.task_id);
    } catch (e) {
        alert(e.message);
        startBtn.disabled = false;
    }
});

// ==================== WebSocket ====================
let currentWS = null;

function cleanupWS() {
    if (currentWS) {
        currentWS.onmessage = null;
        currentWS.onerror = null;
        currentWS.onclose = null;
        if (currentWS.readyState <= WebSocket.OPEN) currentWS.close();
        currentWS = null;
    }
}

function finishTask() {
    startBtn.disabled = false;
    progressBar.classList.remove("active");
}

function connectWS(taskId) {
    cleanupWS();

    const proto = location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(`${proto}//${location.host}/ws/progress/${taskId}`);
    currentWS = ws;

    ws.onmessage = (ev) => {
        const msg = JSON.parse(ev.data);

        if (msg.type === "log") {
            appendLog(msg.text);
        } else if (msg.type === "status") {
            updateProgress(msg.progress, msg.total);
        } else if (msg.type === "complete") {
            setTaskState("completed");
            finishTask();
            updateProgress(1, 1);
            loadFiles();
        } else if (msg.type === "error") {
            setTaskState("error");
            finishTask();
            if (msg.text) appendLog("ERROR: " + msg.text);
        }
    };

    ws.onerror = () => {
        appendLog("WebSocket 连接错误");
        setTaskState("error");
        finishTask();
    };

    ws.onclose = () => {
        finishTask();
        currentWS = null;
    };
}

function appendLog(text) {
    logArea.textContent += text + "\n";
    logArea.scrollTop = logArea.scrollHeight;
}

function updateProgress(done, total) {
    if (total <= 0) return;
    const pct = Math.round((done / total) * 100);
    progressBar.style.width = pct + "%";
    progressBar.textContent = `${done}/${total} (${pct}%)`;
}

function setTaskState(state) {
    const labels = { running: "运行中", completed: "已完成", error: "出错" };
    taskStateEl.textContent = labels[state] || state;
    taskStateEl.className = "status-badge " + state;
}

// ==================== Files ====================
async function deleteFile(path) {
    if (!confirm("确定删除该文件？")) return;
    try {
        const res = await fetch(`/api/files/${path}`, { method: "DELETE" });
        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            alert("删除失败: " + (err.detail || res.statusText));
        }
        loadFiles();
    } catch (e) {
        alert("删除失败: " + e.message);
    }
}

async function loadFiles() {
    try {
        const files = await api("/api/files");
        if (!files.length) {
            fileList.innerHTML = '<em class="empty-hint">暂无文件</em>';
            return;
        }
        fileList.innerHTML = files.map(f => `
            <div class="file-item">
                <a href="/api/download/${f.path}" download>${f.name}</a>
                <div class="file-meta">
                    <span class="file-size">${(f.size / 1024).toFixed(1)} KB</span>
                    <a href="/api/download/${f.path}" download class="file-dl">下载</a>
                    <button class="file-del" onclick="deleteFile('${f.path.replace(/'/g, "\\'")}')">删除</button>
                </div>
            </div>
        `).join("");
    } catch { fileList.innerHTML = '<em class="empty-hint">加载失败</em>'; }
}

refreshBtn.addEventListener("click", loadFiles);

// ==================== Init ====================
setDefaultDates();
loadTokenStatus();
loadFiles();

