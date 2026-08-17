package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"strings"
	"time"
)

const (
	authUsername = "jdsec"
	authPassword = "jdsec001"
	cookieName   = "qps_token"
)

var (
	taskManager  = NewTaskManager()
	proxyManager = NewProxyManager()
	port         int
)

func init() {
	flag.IntVar(&port, "port", 8080, "Web 服务端口")
}

func main() {
	flag.Parse()
	mux := http.NewServeMux()
	mux.HandleFunc("/login", handleLogin)
	mux.HandleFunc("/", authMiddleware(handleIndex))
	mux.HandleFunc("/api/start", authMiddleware(handleStart))
	mux.HandleFunc("/api/stop", authMiddleware(handleStop))
	mux.HandleFunc("/api/tasks", authMiddleware(handleTasks))
	mux.HandleFunc("/api/stats/", authMiddleware(handleStats))
	mux.HandleFunc("/api/clean", authMiddleware(handleClean))
	mux.HandleFunc("/api/export/", authMiddleware(handleExport))
	mux.HandleFunc("/api/proxies", authMiddleware(handleProxies))
	mux.HandleFunc("/api/proxies/", authMiddleware(handleProxyAction))
	addr := fmt.Sprintf(":%d", port)
	log.Printf("HTTP QPS Load Tester started: http://0.0.0.0%s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("listen failed: %v", err)
	}
}

// ==================== Auth ====================

func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !isAuthenticated(r) {
			if strings.HasPrefix(r.URL.Path, "/api/") {
				jsonError(w, 401, "未登录或会话已过期")
			} else {
				http.Redirect(w, r, "/login", http.StatusFound)
			}
			return
		}
		next(w, r)
	}
}

func isAuthenticated(r *http.Request) bool {
	cookie, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	expected := simpleToken(authUsername, authPassword)
	return subtle.ConstantTimeCompare([]byte(cookie.Value), []byte(expected)) == 1
}

func simpleToken(user, pass string) string {
	return fmt.Sprintf("%x", user) + "|" + fmt.Sprintf("%x", pass)
}

func setAuthCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     cookieName,
		Value:    simpleToken(authUsername, authPassword),
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   86400,
	})
}

// ==================== Login ====================

func handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(loginHTML))
		return
	}
	username := r.FormValue("username")
	password := r.FormValue("password")
	if subtle.ConstantTimeCompare([]byte(username), []byte(authUsername)) == 1 &&
		subtle.ConstantTimeCompare([]byte(password), []byte(authPassword)) == 1 {
		setAuthCookie(w)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}
	w.WriteHeader(http.StatusUnauthorized)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"error": "账号或密码错误"})
}

// ==================== Index ====================

func handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	tmpl := template.Must(template.New("index").Parse(indexHTML))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, nil)
}

// ==================== API: Start ====================

func handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, 405, "POST only")
		return
	}
	var req TaskConfig
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, 400, "parse body failed: "+err.Error())
		return
	}
	if err := ValidateConfig(&req); err != nil {
		jsonError(w, 400, err.Error())
		return
	}
	// Validate proxy IDs
	for _, pid := range req.Proxies {
		if pid != "local" {
			if _, ok := proxyManager.GetProxy(pid); !ok {
				jsonError(w, 400, "proxy not found: "+pid)
				return
			}
		}
	}
	taskID := generateID()
	task := NewTask(taskID, &req, proxyManager)
	taskManager.AddTask(task)
	if err := task.Start(); err != nil {
		jsonError(w, 500, err.Error())
		return
	}
	log.Printf("task started: %s -> %s (c=%d proxies=%v)", taskID, req.URL, req.Concurrency, req.Proxies)
	jsonOK(w, map[string]interface{}{"taskId": taskID, "status": "running", "url": req.URL})
}

// ==================== API: Stop ====================

func handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, 405, "POST only")
		return
	}
	var body struct {
		TaskID string `json:"taskId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		jsonError(w, 400, "parse failed")
		return
	}
	task, ok := taskManager.GetTask(body.TaskID)
	if !ok {
		jsonError(w, 404, "task not found")
		return
	}
	task.Stop()
	jsonOK(w, map[string]interface{}{"taskId": body.TaskID, "status": "stopped"})
}

// ==================== API: Tasks ====================

func handleTasks(w http.ResponseWriter, r *http.Request) {
	jsonOK(w, taskManager.ListTasks())
}

// ==================== API: SSE Stats ====================

func handleStats(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Path[len("/api/stats/"):]
	if taskID == "" {
		jsonError(w, 400, "missing task id")
		return
	}
	task, ok := taskManager.GetTask(taskID)
	if !ok {
		jsonError(w, 404, "task not found")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	flusher, ok := w.(http.Flusher)
	if !ok {
		jsonError(w, 500, "SSE not supported")
		return
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	writeSSE(w, flusher, task.Snapshot())
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			snap := task.Snapshot()
			writeSSE(w, flusher, snap)
			if snap.Status == "stopped" {
				fmt.Fprintf(w, "event: done\ndata: {}\n\n")
				flusher.Flush()
				return
			}
		}
	}
}

// ==================== API: Clean ====================

func handleClean(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		jsonError(w, 405, "POST only")
		return
	}
	count := taskManager.CleanStopped()
	jsonOK(w, map[string]interface{}{"cleaned": count})
}

// ==================== API: Export CSV ====================

func handleExport(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Path[len("/api/export/"):]
	if taskID == "" {
		jsonError(w, 400, "missing task id")
		return
	}
	task, ok := taskManager.GetTask(taskID)
	if !ok {
		jsonError(w, 404, "task not found")
		return
	}
	data, err := task.ExportCSV()
	if err != nil {
		jsonError(w, 500, "export failed")
		return
	}
	filename := fmt.Sprintf("qps_%s_%s.csv", taskID, time.Now().Format("20060102_150405"))
	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", "attachment; filename="+filename)
	w.Write(data)
}

// ==================== API: Proxies (CRUD) ====================

func handleProxies(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		jsonOK(w, proxyManager.ListProxies())
	case http.MethodPost:
		var p SSHProxy
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			jsonError(w, 400, "parse failed")
			return
		}
		result, err := proxyManager.AddProxy(&p)
		if err != nil {
			jsonError(w, 400, err.Error())
			return
		}
		jsonOK(w, result.MaskPassword())
	default:
		jsonError(w, 405, "GET/POST only")
	}
}

func handleProxyAction(w http.ResponseWriter, r *http.Request) {
	// /api/proxies/{id}/test  or  /api/proxies/{id} (DELETE)
	path := r.URL.Path[len("/api/proxies/"):]
	parts := strings.SplitN(path, "/", 2)
	id := parts[0]

	if id == "" {
		jsonError(w, 400, "missing proxy id")
		return
	}

	if len(parts) == 2 && parts[1] == "test" && r.Method == http.MethodPost {
		err := proxyManager.TestProxy(id)
		if err != nil {
			jsonOK(w, map[string]interface{}{"ok": false, "error": err.Error()})
		} else {
			jsonOK(w, map[string]interface{}{"ok": true})
		}
		return
	}

	switch r.Method {
	case http.MethodDelete:
		if err := proxyManager.DeleteProxy(id); err != nil {
			jsonError(w, 404, err.Error())
			return
		}
		jsonOK(w, map[string]interface{}{"deleted": id})
	case http.MethodPut:
		var p SSHProxy
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			jsonError(w, 400, "parse failed")
			return
		}
		if err := proxyManager.UpdateProxy(id, &p); err != nil {
			jsonError(w, 400, err.Error())
			return
		}
		jsonOK(w, map[string]interface{}{"updated": id})
	default:
		jsonError(w, 405, "method not allowed")
	}
}

// ==================== Utils ====================

func writeSSE(w http.ResponseWriter, flusher http.Flusher, data interface{}) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return
	}
	fmt.Fprintf(w, "data: %s\n\n", jsonData)
	flusher.Flush()
}

func jsonOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func jsonError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func generateID() string {
	b := make([]byte, 6)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// ==================== Login Page ====================

const loginHTML = `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>HTTP QPS - Login</title><style>*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh;display:flex;align-items:center;justify-content:center}.lb{background:#1e293b;border:1px solid #334155;border-radius:12px;padding:40px;width:360px}.lb h2{font-size:20px;color:#f8fafc;margin-bottom:24px;text-align:center;font-weight:600}.fg{margin-bottom:16px}.fg label{display:block;font-size:13px;color:#94a3b8;margin-bottom:6px}.fg input{width:100%;background:#0f172a;border:1px solid #334155;border-radius:8px;padding:10px 12px;color:#e2e8f0;font-size:14px;outline:none}.fg input:focus{border-color:#3b82f6}.lbtn{width:100%;padding:10px;background:#3b82f6;color:#fff;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;margin-top:8px}.lbtn:hover{background:#2563eb}.err{color:#ef4444;font-size:13px;margin-bottom:12px;text-align:center;display:none}</style></head><body><div class="lb"><h2>HTTP QPS Load Tester</h2><div class="err" id="err"></div><form id="lf" onsubmit="return doLogin(event)"><div class="fg"><label>账号</label><input type="text" id="u" autocomplete="username" required autofocus></div><div class="fg"><label>密码</label><input type="password" id="p" autocomplete="current-password" required></div><button class="lbtn" type="submit">登录</button></form></div><script>async function doLogin(e){e.preventDefault();const fd=new FormData();fd.append('username',document.getElementById('u').value);fd.append('password',document.getElementById('p').value);try{const r=await fetch('/login',{method:'POST',body:fd});const d=await r.json();if(d.status==='ok'){window.location.href='/';return;}const el=document.getElementById('err');el.style.display='block';el.textContent=d.error||'登录失败';}catch(ex){document.getElementById('err').style.display='block';document.getElementById('err').textContent='网络错误'}return false}</script></body></html>`

// ==================== Main Page ====================

const indexHTML = `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>HTTP QPS Load Tester</title><style>
*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}.header{background:#1e293b;padding:14px 32px;border-bottom:1px solid #334155;display:flex;align-items:center;justify-content:space-between}.header h1{font-size:17px;color:#f8fafc;font-weight:600}.hr{display:flex;align-items:center;gap:14px}.hr span{font-size:13px;color:#94a3b8}.lo{padding:4px 14px;background:transparent;border:1px solid #475569;border-radius:6px;color:#94a3b8;font-size:12px;cursor:pointer}.lo:hover{border-color:#94a3b8;color:#e2e8f0}.container{max-width:1260px;margin:0 auto;padding:24px}.card{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:24px;margin-bottom:20px}.card-title{font-size:15px;font-weight:600;color:#f8fafc;margin-bottom:16px}.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.fg{display:flex;flex-direction:column;gap:6px}.fg.full{grid-column:1/-1}label{font-size:13px;color:#94a3b8;font-weight:500}input,select,textarea{background:#0f172a;border:1px solid #334155;border-radius:8px;padding:10px 12px;color:#e2e8f0;font-size:14px;outline:none;transition:border-color .2s}input:focus,select:focus,textarea:focus{border-color:#3b82f6}textarea{resize:vertical;font-family:'Menlo','Consolas',monospace;font-size:13px}.btn{padding:10px 24px;border:none;border-radius:8px;font-size:14px;font-weight:600;cursor:pointer;transition:all .2s}.btn-primary{background:#3b82f6;color:#fff}.btn-primary:hover{background:#2563eb}.btn-danger{background:#ef4444;color:#fff}.btn-danger:hover{background:#dc2626}.btn-secondary{background:#334155;color:#e2e8f0}.btn-secondary:hover{background:#475569}.btn-sm{padding:4px 12px;font-size:12px}.btn-row{display:flex;gap:12px;margin-top:16px}.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:16px}.stat-card{background:#0f172a;border-radius:10px;padding:16px;text-align:center}.stat-value{font-size:28px;font-weight:700;color:#3b82f6}.stat-value.green{color:#22c55e}.stat-value.red{color:#ef4444}.stat-value.yellow{color:#eab308}.stat-label{font-size:12px;color:#94a3b8;margin-top:4px}.chart-container{background:#0f172a;border-radius:10px;padding:16px;margin-top:16px;position:relative}.chart-container canvas{width:100%;height:200px}.sb-section{margin-top:16px}.sb-section .st{font-size:13px;color:#94a3b8;margin-bottom:10px;font-weight:600}.sb-wrap{display:flex;height:28px;border-radius:6px;overflow:hidden;background:#1e293b}.sb-seg{display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:600;color:#fff;min-width:0;overflow:hidden;white-space:nowrap;text-overflow:ellipsis;padding:0 6px;transition:width .5s ease}.sb-seg.s2{background:#22c55e}.sb-seg.s3{background:#eab308}.sb-seg.s4{background:#f97316}.sb-seg.s5{background:#ef4444}.sb-seg.s0{background:#64748b}.sb-legend{display:flex;flex-wrap:wrap;gap:12px;margin-top:10px}.li{display:flex;align-items:center;gap:6px;font-size:12px;color:#cbd5e1}.ld{width:10px;height:10px;border-radius:3px;flex-shrink:0}.ld.s0{background:#64748b}.ld.s2{background:#22c55e}.ld.s3{background:#eab308}.ld.s4{background:#f97316}.ld.s5{background:#ef4444}.log-section{margin-top:16px}.log-section .st{font-size:13px;color:#94a3b8;margin-bottom:10px;font-weight:600}.log-container{background:#0f172a;border-radius:10px;padding:0;max-height:260px;overflow-y:auto;font-family:'Menlo','Consolas','Courier New',monospace;font-size:12px}.log-container::-webkit-scrollbar{width:6px}.log-container::-webkit-scrollbar-track{background:transparent}.log-container::-webkit-scrollbar-thumb{background:#334155;border-radius:3px}.log-row{display:flex;align-items:center;padding:5px 12px;border-bottom:1px solid #1e293b;gap:10px}.log-row:last-child{border-bottom:none}.log-row:hover{background:#1e293b}.log-status{min-width:48px;text-align:center;font-weight:700;padding:1px 0;border-radius:4px;font-size:12px}.log-status.s2{color:#22c55e;background:#052e16}.log-status.s3{color:#eab308;background:#422006}.log-status.s4{color:#f97316;background:#431407}.log-status.s5{color:#ef4444;background:#450a0a}.log-status.s0{color:#64748b;background:#1e293b}.log-time{min-width:80px;color:#64748b}.log-proxy{min-width:60px;color:#3b82f6;font-weight:600}.log-duration{min-width:70px;color:#94a3b8;text-align:right}.log-error{color:#f87171;font-size:11px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px}.proxy-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.proxy-list{display:flex;flex-direction:column;gap:8px}.proxy-item{background:#0f172a;border-radius:8px;padding:10px 14px;display:flex;justify-content:space-between;align-items:center}.proxy-info{display:flex;gap:10px;align-items:center;font-size:13px;flex-wrap:wrap}.proxy-info .pid{font-family:monospace;color:#64748b;font-size:12px}.proxy-info .pname{color:#f8fafc;font-weight:600}.proxy-info .phost{color:#94a3b8}.proxy-info .puser{color:#94a3b8}.proxy-actions{display:flex;gap:6px;flex-shrink:0}.proxy-status{font-size:11px;padding:2px 8px;border-radius:4px}.proxy-status.on{background:#065f46;color:#6ee7b7}.proxy-status.off{background:#334155;color:#64748b}.proxy-form{background:#0f172a;border-radius:8px;padding:16px}.proxy-form .fg{margin-bottom:10px}.proxy-form label{font-size:12px}.proxy-form input{padding:8px 10px;font-size:13px}.proxy-check-row{display:flex;flex-wrap:wrap;gap:10px;margin-top:12px}.proxy-check{display:flex;align-items:center;gap:6px;padding:6px 14px;background:#0f172a;border:1px solid #334155;border-radius:8px;cursor:pointer;font-size:13px;color:#94a3b8;transition:all .2s}.proxy-check:hover{border-color:#3b82f6;color:#e2e8f0}.proxy-check.selected{border-color:#3b82f6;background:#1e3a5f;color:#f8fafc}.proxy-check input{accent-color:#3b82f6;width:16px;height:16px}.proxy-breakdown{margin-top:16px}.pb-item{background:#0f172a;border-radius:8px;padding:10px 14px;margin-bottom:6px;display:flex;justify-content:space-between;align-items:center;font-size:13px}.pb-name{color:#f8fafc;font-weight:600;min-width:80px}.pb-stats{display:flex;gap:16px;color:#94a3b8}.pb-stats b{color:#e2e8f0}.task-list{display:flex;flex-direction:column;gap:10px}.task-item{background:#0f172a;border-radius:8px;padding:14px 18px}.task-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}.task-left{display:flex;align-items:center;gap:12px}.task-id{font-family:monospace;color:#94a3b8;font-size:13px}.task-url{color:#e2e8f0;font-size:13px;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.task-badge{font-size:11px;font-weight:600;padding:2px 10px;border-radius:999px}.task-badge.running{background:#065f46;color:#6ee7b7}.task-badge.stopped{background:#334155;color:#94a3b8}.task-meta{display:flex;flex-wrap:wrap;gap:6px 18px;font-size:12px;color:#64748b;margin-top:6px}.task-meta b{color:#94a3b8;font-weight:500}.task-actions{margin-top:10px;display:flex;gap:8px}.hidden{display:none}.headers-container{display:flex;flex-direction:column;gap:8px}.header-row{display:flex;gap:8px}.header-row input{flex:1}.mode-toggle{display:flex;gap:8px}.mode-toggle label{display:flex;align-items:center;gap:4px;cursor:pointer;font-size:13px;color:#94a3b8}.mode-toggle input[type=radio]{accent-color:#3b82f6}.checkbox-label{display:flex;align-items:center;gap:6px;cursor:pointer;font-size:13px;color:#94a3b8}.checkbox-label input{accent-color:#3b82f6;width:16px;height:16px}.two-col{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-top:16px}.modal-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.6);z-index:1000;display:none;align-items:center;justify-content:center}.modal-overlay.open{display:flex}.modal{background:#1e293b;border:1px solid #334155;border-radius:12px;width:680px;max-height:80vh;overflow-y:auto;padding:24px}.modal-title{font-size:16px;font-weight:600;color:#f8fafc;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center}.modal-close{background:transparent;border:none;color:#94a3b8;font-size:20px;cursor:pointer;padding:0 4px}.modal-close:hover{color:#f8fafc}
</style></head><body>
<div class="header"><h1>HTTP QPS Load Tester</h1><div class="hr"><button class="lo" onclick="openProxyModal()">代理管理</button><span>jdsec</span><button class="lo" onclick="logout()">退出</button></div></div>
<div class="container">

<!-- Config -->
<div class="card">
<div class="card-title">压测配置</div>
<div class="form-grid">
<div class="fg full"><label>目标 URL *</label><input type="text" id="url" placeholder="https://example.com/api"></div>
<div class="fg"><label>HTTP 方法</label><select id="method"><option value="GET">GET</option><option value="POST">POST</option><option value="PUT">PUT</option><option value="DELETE">DELETE</option><option value="HEAD">HEAD</option><option value="PATCH">PATCH</option></select></div>
<div class="fg"><label>并发数 (每个来源)</label><input type="number" id="concurrency" value="50" min="1" max="5000"></div>
<div class="fg"><label>压测模式</label><div class="mode-toggle" style="margin-top:6px"><label><input type="radio" name="mode" value="duration" checked> 按时间</label><label><input type="radio" name="mode" value="count"> 按请求数</label></div></div>
<div class="fg" id="durationGroup"><label>持续时间</label><select id="duration"><option value="10s">10 秒</option><option value="30s" selected>30 秒</option><option value="1m">1 分钟</option><option value="2m">2 分钟</option><option value="5m">5 分钟</option></select></div>
<div class="fg hidden" id="countGroup"><label>总请求数</label><input type="number" id="totalReqs" value="10000" min="1" max="1000000"></div>
<div class="fg"><label>请求超时</label><select id="timeout"><option value="5s">5 秒</option><option value="10s" selected>10 秒</option><option value="30s">30 秒</option></select></div>
<div class="fg"><label class="checkbox-label"><input type="checkbox" id="keepalive" checked> Keep-Alive</label></div>
<div class="fg full"><label>请求 Body</label><textarea id="body" rows="3" placeholder='{"key":"value"}'></textarea></div>
<div class="fg full"><label>自定义 Headers</label><div class="headers-container" id="headersContainer"><div class="header-row"><input type="text" placeholder="Key" class="header-key"><input type="text" placeholder="Value" class="header-value"><button class="btn btn-danger btn-sm" onclick="this.parentElement.remove()">X</button></div></div><button class="btn btn-secondary btn-sm" style="margin-top:8px" onclick="addHeaderRow()">+ Header</button></div>
<div class="fg full"><label>请求来源 (可多选)</label><div class="proxy-check-row" id="proxyCheckRow"><label class="proxy-check selected" id="pcLocal"><input type="checkbox" id="chkLocal" checked onchange="toggleCheck('pcLocal')"> Local (本机)</label></div></div>
</div>
<div class="btn-row"><button class="btn btn-primary" id="startBtn" onclick="startTest()">开始压测</button><button class="btn btn-danger hidden" id="stopBtn" onclick="stopTest()">停止</button></div>
</div>

<!-- Stats -->
<div class="card hidden" id="statsCard">
<div class="card-title">实时统计</div>
<div class="stats-grid">
<div class="stat-card"><div class="stat-value" id="statQPS">0</div><div class="stat-label">QPS</div></div>
<div class="stat-card"><div class="stat-value green" id="statSuccess">0</div><div class="stat-label">成功</div></div>
<div class="stat-card"><div class="stat-value red" id="statFail">0</div><div class="stat-label">失败</div></div>
<div class="stat-card"><div class="stat-value yellow" id="statRate">0%</div><div class="stat-label">成功率</div></div>
<div class="stat-card"><div class="stat-value" id="statAvg">0ms</div><div class="stat-label">平均耗时</div></div>
<div class="stat-card"><div class="stat-value" id="statMin">0ms</div><div class="stat-label">最小耗时</div></div>
<div class="stat-card"><div class="stat-value" id="statMax">0ms</div><div class="stat-label">最大耗时</div></div>
<div class="stat-card"><div class="stat-value" id="statTotal">0</div><div class="stat-label">总请求数</div></div>
</div>
<div class="chart-container"><canvas id="qpsChart"></canvas></div>
<div id="proxyBreakdown" class="proxy-breakdown hidden"></div>
<div class="two-col">
<div class="sb-section"><div class="st">状态码分布</div><div class="sb-wrap" id="sbWrap"><div class="sb-seg s2" style="width:0%"></div></div><div class="sb-legend" id="sbLegend"><div style="color:#64748b;font-size:12px">等待数据...</div></div></div>
<div class="log-section"><div class="st">最近响应 <span id="logCount" style="color:#64748b;font-weight:400"></span></div><div class="log-container" id="logContainer"><div style="color:#475569;padding:16px;text-align:center">等待请求...</div></div></div>
</div>
</div>

<!-- Task List -->
<div class="card">
<div class="card-title" style="justify-content:space-between"><span>任务列表</span><button class="btn btn-secondary btn-sm" onclick="cleanTasks()">清理已停止</button></div>
<div class="task-list" id="taskList"><div style="color:#64748b;font-size:13px;text-align:center;padding:20px">暂无任务</div></div>
</div>
</div>

<!-- Proxy Modal -->
<div class="modal-overlay" id="proxyModal">
<div class="modal">
<div class="modal-title"><span>SSH 代理管理</span><button class="modal-close" onclick="closeProxyModal()">x</button></div>
<div class="proxy-grid">
<div>
<div style="font-size:13px;color:#94a3b8;margin-bottom:10px">已配置的代理</div>
<div class="proxy-list" id="proxyList"><div style="color:#64748b;font-size:13px;text-align:center;padding:16px">暂无代理，请在右侧添加</div></div>
</div>
<div class="proxy-form">
<div style="font-size:13px;color:#94a3b8;margin-bottom:10px">添加新代理</div>
<div class="fg"><label>名称 (可选)</label><input type="text" id="pxName" placeholder="如: 美东节点"></div>
<div class="fg"><label>Host *</label><input type="text" id="pxHost" placeholder="1.2.3.4"></div>
<div class="fg"><label>SSH 端口</label><input type="number" id="pxPort" value="22"></div>
<div class="fg"><label>用户名 *</label><input type="text" id="pxUser" placeholder="root"></div>
<div class="fg"><label>密码 *</label><input type="password" id="pxPass" placeholder="密码"></div>
<div class="btn-row"><button class="btn btn-primary btn-sm" onclick="addProxy()">添加</button></div>
</div>
</div>
</div>
</div>

<script>
let currentTaskId=null,eventSource=null,qpsHistory=[],lastLogLength=0,proxies=[];
const MAX_QPS=120;
document.querySelectorAll('input[name=mode]').forEach(r=>{r.addEventListener('change',function(){document.getElementById('durationGroup').classList.toggle('hidden',this.value!=='duration');document.getElementById('countGroup').classList.toggle('hidden',this.value!=='count')})});
document.getElementById('method').addEventListener('change',function(){document.getElementById('body').parentElement.style.display=['POST','PUT','PATCH'].includes(this.value)?'':'none'});
if(!['POST','PUT','PATCH'].includes(document.getElementById('method').value)){document.getElementById('body').parentElement.style.display='none'}
function addHeaderRow(){const c=document.getElementById('headersContainer'),r=document.createElement('div');r.className='header-row';r.innerHTML='<input type="text" placeholder="Key" class="header-key"><input type="text" placeholder="Value" class="header-value"><button class="btn btn-danger btn-sm" onclick="this.parentElement.remove()">X</button>';c.appendChild(r)}
function getHeaders(){const h={};document.querySelectorAll('.header-row').forEach(r=>{const k=r.querySelector('.header-key').value.trim(),v=r.querySelector('.header-value').value.trim();if(k)h[k]=v});return h}
function logout(){document.cookie='qps_token=;Path=/;Max-Age=0';window.location.href='/login'}
function toggleCheck(id){const el=document.getElementById(id);el.classList.toggle('selected',el.querySelector('input').checked)}

// Proxy modal
function openProxyModal(){document.getElementById('proxyModal').classList.add('open');loadProxies()}
function closeProxyModal(){document.getElementById('proxyModal').classList.remove('open')}

// Proxy management
async function loadProxies(){const r=await fetch('/api/proxies');proxies=await r.json();renderProxyList();renderProxyChecks()}
function renderProxyList(){const el=document.getElementById('proxyList');if(!proxies.length){el.innerHTML='<div style="color:#64748b;font-size:13px;text-align:center;padding:16px">暂无代理</div>';return}
el.innerHTML=proxies.map(p=>'<div class="proxy-item"><div class="proxy-info"><span class="pid">'+p.id+'</span><span class="pname">'+esc(p.name)+'</span><span class="phost">'+p.host+':'+p.port+'</span><span class="puser">'+esc(p.username)+'</span></div><div class="proxy-actions"><button class="btn btn-secondary btn-sm" onclick="testProxy(\''+p.id+'\')">测试</button><button class="btn btn-danger btn-sm" onclick="delProxy(\''+p.id+'\')">删除</button></div></div>').join('')}
function renderProxyChecks(){const row=document.getElementById('proxyCheckRow');row.innerHTML='';const lbl=document.createElement('label');lbl.className='proxy-check selected';lbl.id='pcLocal';lbl.innerHTML='<input type="checkbox" id="chkLocal" checked onchange="toggleCheck(\'pcLocal\')"> Local (本机)';row.appendChild(lbl);proxies.forEach(p=>{const lbl=document.createElement('label');lbl.className='proxy-check';lbl.id='pc_'+p.id;lbl.innerHTML='<input type="checkbox" onchange="toggleCheck(\'pc_'+p.id+'\')"> '+esc(p.name)+' ('+p.host+')';row.appendChild(lbl)})}
async function addProxy(){const d={name:document.getElementById('pxName').value.trim(),host:document.getElementById('pxHost').value.trim(),port:parseInt(document.getElementById('pxPort').value)||22,username:document.getElementById('pxUser').value.trim(),password:document.getElementById('pxPass').value};if(!d.host||!d.username||!d.password){alert('Host、用户名、密码必填');return}const r=await fetch('/api/proxies',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)});const res=await r.json();if(res.error){alert(res.error);return}document.getElementById('pxName').value='';document.getElementById('pxHost').value='';document.getElementById('pxPort').value='22';document.getElementById('pxUser').value='';document.getElementById('pxPass').value='';loadProxies()}
async function delProxy(id){if(!confirm('确定删除?'))return;await fetch('/api/proxies/'+id,{method:'DELETE'});loadProxies()}
async function testProxy(id){const r=await fetch('/api/proxies/'+id+'/test',{method:'POST'});const d=await r.json();alert(d.ok?'连接成功!':'连接失败: '+d.error)}

function getSelectedProxies(){const s=['local'];if(document.getElementById('chkLocal').checked)s[0]='local';else s.shift();proxies.forEach(p=>{const el=document.getElementById('pc_'+p.id);if(el&&el.querySelector('input').checked)s.push(p.id)});return s.length?s:['local']}

async function startTest(){
const url=document.getElementById('url').value.trim();if(!url){alert('请输入目标 URL');return}
const mode=document.querySelector('input[name=mode]:checked').value;
const body={url,method:document.getElementById('method').value,concurrency:parseInt(document.getElementById('concurrency').value)||50,headers:getHeaders(),timeout:document.getElementById('timeout').value,keepAlive:document.getElementById('keepalive').checked,body:document.getElementById('body').value,proxies:getSelectedProxies()};
if(mode==='duration')body.duration=document.getElementById('duration').value;else body.totalReqs=parseInt(document.getElementById('totalReqs').value)||10000;
const resp=await fetch('/api/start',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
if(resp.status===401){logout();return}const data=await resp.json();if(data.error){alert(data.error);return}
currentTaskId=data.taskId;qpsHistory=[];lastLogLength=0;
document.getElementById('statsCard').classList.remove('hidden');document.getElementById('startBtn').classList.add('hidden');document.getElementById('stopBtn').classList.remove('hidden');
document.getElementById('logContainer').innerHTML='<div style="color:#475569;padding:16px;text-align:center">等待请求...</div>';
startSSE(data.taskId);refreshTasks()}

async function stopTest(){if(!currentTaskId)return;await fetch('/api/stop',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({taskId:currentTaskId})});document.getElementById('startBtn').classList.remove('hidden');document.getElementById('stopBtn').classList.add('hidden');currentTaskId=null;if(eventSource){eventSource.close();eventSource=null}refreshTasks()}

function startSSE(tid){if(eventSource)eventSource.close();eventSource=new EventSource('/api/stats/'+tid);eventSource.onmessage=function(e){updateStats(JSON.parse(e.data))};eventSource.addEventListener('done',function(){eventSource.close();eventSource=null;document.getElementById('startBtn').classList.remove('hidden');document.getElementById('stopBtn').classList.add('hidden');currentTaskId=null;refreshTasks()});eventSource.onerror=function(){eventSource.close();eventSource=null}}

function updateStats(s){
document.getElementById('statQPS').textContent=s.qps.toFixed(1);document.getElementById('statSuccess').textContent=s.successCount.toLocaleString();document.getElementById('statFail').textContent=s.failCount.toLocaleString();document.getElementById('statRate').textContent=s.successRate.toFixed(1)+'%';document.getElementById('statAvg').textContent=s.avgMs.toFixed(2)+'ms';document.getElementById('statMin').textContent=s.minMs.toFixed(2)+'ms';document.getElementById('statMax').textContent=s.maxMs.toFixed(2)+'ms';document.getElementById('statTotal').textContent=s.totalReqs.toLocaleString();
qpsHistory.push(s.qps);if(qpsHistory.length>MAX_QPS)qpsHistory.shift();drawChart();
updateSB(s.statusCodes||{},s.totalReqs);updateLog(s.recentLogs||[]);
// Proxy breakdown
const pb=document.getElementById('proxyBreakdown');const bd=s.proxyBreakdown||[];
if(bd.length>1){pb.classList.remove('hidden');pb.innerHTML='<div class="st" style="margin-bottom:8px">按来源统计</div>'+bd.map(b=>'<div class="pb-item"><span class="pb-name">'+esc(b.proxyName)+'</span><div class="pb-stats">QPS <b>'+b.qps.toFixed(1)+'</b> | 成功 <b>'+b.successCount+'</b> | 失败 <b>'+b.failCount+'</b> | 均耗 <b>'+b.avgMs.toFixed(1)+'ms</b></div></div>').join('')}else{pb.classList.add('hidden')}}

function scc(c){if(c===0)return's0';const m=Math.floor(c/100);if(m===2)return's2';if(m===3)return's3';if(m===4)return's4';if(m===5)return's5';return's0'}
function scl(c){if(c===0)return'ERR';const l={200:'200 OK',201:'201 Created',204:'204 No Content',301:'301 Moved',302:'302 Found',304:'304 Not Modified',400:'400 Bad Request',401:'401 Unauthorized',403:'403 Forbidden',404:'404 Not Found',429:'429 Too Many',500:'500 Internal',502:'502 Bad Gateway',503:'503 Unavailable',504:'504 Timeout'};return l[c]||String(c)}

function updateSB(codes,total){const ent=Object.entries(codes).sort((a,b)=>Number(b[0])-Number(a[0]));if(!ent.length)return;const w=document.getElementById('sbWrap');w.innerHTML='';ent.forEach(([c,n])=>{const p=total>0?(n/total*100):0;const s=document.createElement('div');s.className='sb-seg '+scc(Number(c));s.style.width=Math.max(p,0.5)+'%';if(p>8)s.textContent=c;s.title=scl(Number(c))+': '+n+' ('+p.toFixed(1)+'%)';w.appendChild(s)});const lg=document.getElementById('sbLegend');lg.innerHTML='';ent.forEach(([c,n])=>{const p=total>0?(n/total*100).toFixed(1):'0.0';const i=document.createElement('div');i.className='li';i.innerHTML='<span class="ld '+scc(Number(c))+'"></span>'+scl(Number(c))+' <b>'+n.toLocaleString()+'</b> <span style="color:#64748b">('+p+'%)</span>';lg.appendChild(i)})}

function updateLog(logs){if(!logs||!logs.length)return;const ct=document.getElementById('logContainer'),cn=document.getElementById('logCount');const nw=logs.slice(lastLogLength);lastLogLength=logs.length;cn.textContent='('+logs.length+' 条)';if(!nw.length&&ct.querySelector('.log-row'))return;if(!ct.querySelector('.log-row'))ct.innerHTML='';nw.forEach(l=>{const r=document.createElement('div');r.className='log-row';const cl=scc(l.statusCode),st=l.statusCode===0?'ERR':String(l.statusCode);const ts=l.ts?new Date(l.ts).toLocaleTimeString('zh-CN',{hour12:false}):'';const pn=l.proxyId?getProxyName(l.proxyId):'';let h='<span class="log-time">'+ts+'</span><span class="log-status '+cl+'">'+st+'</span>';if(pn)h+='<span class="log-proxy">'+esc(pn)+'</span>';h+='<span class="log-duration">'+l.durationMs.toFixed(1)+'ms</span>';if(l.error)h+='<span class="log-error">'+esc(l.error)+'</span>';r.innerHTML=h;ct.appendChild(r)});while(ct.children.length>100)ct.removeChild(ct.firstChild);ct.scrollTop=ct.scrollHeight}

function getProxyName(pid){const p=proxies.find(x=>x.id===pid);return p?p.name:pid==='local'?'Local':pid}

function esc(s){return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}

function drawChart(){const cv=document.getElementById('qpsChart'),cx=cv.getContext('2d'),dp=window.devicePixelRatio||1,rc=cv.getBoundingClientRect();cv.width=rc.width*dp;cv.height=rc.height*dp;cx.scale(dp,dp);const w=rc.width,h=rc.height,pd={t:20,r:20,b:30,l:60},cw=w-pd.l-pd.r,ch=h-pd.t-pd.b;cx.fillStyle='#0f172a';cx.fillRect(0,0,w,h);if(qpsHistory.length<2)return;const mx=Math.max(...qpsHistory,1)*1.2;cx.strokeStyle='#1e293b';cx.lineWidth=1;for(let i=0;i<=4;i++){const y=pd.t+(ch/4)*i;cx.beginPath();cx.moveTo(pd.l,y);cx.lineTo(pd.l+cw,y);cx.stroke();cx.fillStyle='#64748b';cx.font='11px monospace';cx.textAlign='right';cx.fillText(Math.round(mx-(mx/4)*i),pd.l-8,y+4)}const gd=cx.createLinearGradient(0,pd.t,0,pd.t+ch);gd.addColorStop(0,'rgba(59,130,246,0.3)');gd.addColorStop(1,'rgba(59,130,246,0)');cx.beginPath();for(let i=0;i<qpsHistory.length;i++){const x=pd.l+(i/(qpsHistory.length-1))*cw,y=pd.t+ch-(qpsHistory[i]/mx)*ch;if(i===0)cx.moveTo(x,y);else cx.lineTo(x,y)}cx.strokeStyle='#3b82f6';cx.lineWidth=2;cx.stroke();cx.lineTo(pd.l+cw,pd.t+ch);cx.lineTo(pd.l,pd.t+ch);cx.closePath();cx.fillStyle=gd;cx.fill();cx.fillStyle='#f8fafc';cx.font='bold 12px sans-serif';cx.textAlign='right';cx.fillText('QPS: '+qpsHistory[qpsHistory.length-1].toFixed(0),pd.l+cw,pd.t-4)}

async function refreshTasks(){const r=await fetch('/api/tasks');if(r.status===401){logout();return}const tasks=await r.json(),el=document.getElementById('taskList');if(!tasks||!tasks.length){el.innerHTML='<div style="color:#64748b;font-size:13px;text-align:center;padding:20px">暂无任务</div>';return}
el.innerHTML=tasks.map(t=>{const dur=t.durationLimit||('-'+t.totalReqLimit);const elapsed=t.elapsedMs?fmtDur(t.elapsedMs):'-';const proxyInfo=t.proxies&&t.proxies.length?t.proxies.map(p=>p==='local'?'Local':getProxyName(p)).join('+'):'Local';const bodyInfo=t.body?'<b>Body:</b> '+t.body.substring(0,50)+(t.body.length>50?'...':''):'';const hdrInfo=t.headers?'<b>Headers:</b> '+t.headers.substring(0,60)+'':'';return'<div class="task-item"><div class="task-head"><div class="task-left"><span class="task-id">'+t.taskId+'</span><span class="task-url">'+t.method+' '+t.url+'</span><span class="task-badge '+t.status+'">'+t.status+'</span></div></div><div class="task-meta"><span><b>创建:</b> '+t.createdAt+'</span><span><b>开始:</b> '+(t.startedAt||'-')+'</span><span><b>结束:</b> '+(t.finishedAt||'-')+'</span><span><b>并发:</b> '+t.concurrency+'</span><span><b>来源:</b> '+esc(proxyInfo)+'</span><span><b>目标:</b> '+dur+'</span><span><b>已用:</b> '+elapsed+'</span><span><b>QPS:</b> '+t.qps+'</span><span><b>成功:</b> '+t.successCount+'</span><span><b>失败:</b> '+t.failCount+'</span>'+(bodyInfo?'<span>'+bodyInfo+'</span>':'')+(hdrInfo?'<span>'+hdrInfo+'</span>':'')+'</div><div class="task-actions"><button class="btn btn-secondary btn-sm" onclick="exportCSV(\''+t.taskId+'\')">下载记录 CSV</button>'+(t.status==='running'?'<button class="btn btn-danger btn-sm" onclick="stopTask(\''+t.taskId+'\')">停止</button>':'')+'</div></div>'}).join('')}

function fmtDur(ms){const s=Math.floor(ms/1000),m=Math.floor(s/60),h=Math.floor(m/60);if(h>0)return h+'h'+(m%60)+'m'+(s%60)+'s';if(m>0)return m+'m'+(s%60)+'s';return s+'s'}

async function exportCSV(tid){const r=await fetch('/api/export/'+tid);if(r.status===401){logout();return}const blob=await r.blob();const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='qps_'+tid+'.csv';a.click();URL.revokeObjectURL(a.href)}
async function stopTask(tid){await fetch('/api/stop',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({taskId:tid})});if(tid===currentTaskId){document.getElementById('startBtn').classList.remove('hidden');document.getElementById('stopBtn').classList.add('hidden');currentTaskId=null;if(eventSource){eventSource.close();eventSource=null}}refreshTasks()}
async function cleanTasks(){await fetch('/api/clean',{method:'POST'});refreshTasks()}

loadProxies();refreshTasks();setInterval(refreshTasks,5000);
window.addEventListener('resize',function(){if(qpsHistory.length>=2)drawChart()});
</script></body></html>
`
