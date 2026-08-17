package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// userAgents pool of realistic browser User-Agent strings
var userAgents = []string{
	// Chrome - Windows
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
	// Chrome - Mac
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	// Chrome - Linux
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
	// Firefox - Windows
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
	// Firefox - Mac
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0",
	// Firefox - Linux
	"Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
	"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
	// Safari - Mac
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
	// Edge - Windows
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.0.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
	// Mobile - iPhone
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
	"Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/125.0.6422.80 Mobile/15E148 Safari/604.1",
	// Mobile - Android Chrome
	"Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.113 Mobile Safari/537.36",
	"Mozilla/5.0 (Linux; Android 14; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.179 Mobile Safari/537.36",
	// Mobile - Android Firefox
	"Mozilla/5.0 (Android 14; Mobile; rv:126.0) Gecko/126.0 Firefox/126.0",
	// Tablet - iPad
	"Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
	// WeChat
	"Mozilla/5.0 (Linux; Android 14; Pixel 8 Build/UQ1A.240205.004; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/125.0.6422.113 Mobile Safari/537.36 MicroMessenger/8.0.49.2602(0x28003135) NetType/WIFI Language/zh_CN",
	// Windows WeChat
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x70021439) WindowsWechat",
}

// randomUA returns a random User-Agent from the pool
func randomUA() string {
	return userAgents[rand.Intn(len(userAgents))]
}

// Duration custom JSON unmarshal
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		dur, err := time.ParseDuration(value)
		if err != nil {
			return err
		}
		d.Duration = dur
		return nil
	default:
		return fmt.Errorf("cannot unmarshal Duration")
	}
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// RequestLog single request log entry
type RequestLog struct {
	Seq        int       `json:"seq"`
	StatusCode int       `json:"statusCode"`
	DurationMs float64   `json:"durationMs"`
	Error      string    `json:"error,omitempty"`
	Timestamp  time.Time `json:"ts"`
	ProxyID    string    `json:"proxyId,omitempty"`
}

// ProxyStatsItem per-proxy stats in snapshot
type ProxyStatsItem struct {
	ProxyID      string  `json:"proxyId"`
	ProxyName    string  `json:"proxyName"`
	QPS          float64 `json:"qps"`
	TotalReqs    int64   `json:"totalReqs"`
	SuccessCount int64   `json:"successCount"`
	FailCount    int64   `json:"failCount"`
	AvgMs        float64 `json:"avgMs"`
}

// StatsSnapshot JSON-friendly stats for SSE
type StatsSnapshot struct {
	TaskID        string            `json:"taskId"`
	URL           string            `json:"url"`
	Method        string            `json:"method"`
	Body          string            `json:"body,omitempty"`
	Concurrency   int               `json:"concurrency"`
	Status        string            `json:"status"`
	QPS           float64           `json:"qps"`
	TotalReqs     int64             `json:"totalReqs"`
	SuccessCount  int64             `json:"successCount"`
	FailCount     int64             `json:"failCount"`
	SuccessRate   float64           `json:"successRate"`
	AvgMs         float64           `json:"avgMs"`
	MinMs         float64           `json:"minMs"`
	MaxMs         float64           `json:"maxMs"`
	StatusCodes   map[int]int64     `json:"statusCodes"`
	ElapsedMs     int64             `json:"elapsedMs"`
	DurationLimit string            `json:"durationLimit,omitempty"`
	TotalReqLimit int64             `json:"totalReqLimit,omitempty"`
	RecentLogs    []RequestLog      `json:"recentLogs"`
	CreatedAt     string            `json:"createdAt"`
	StartedAt     string            `json:"startedAt"`
	FinishedAt    string            `json:"finishedAt,omitempty"`
	Headers       string            `json:"headers,omitempty"`
	TimeoutSec    int               `json:"timeoutSec"`
	KeepAlive     bool              `json:"keepAlive"`
	ProxyBreakdown []ProxyStatsItem `json:"proxyBreakdown,omitempty"`
}

// ProxyCounter per-proxy atomic counters
type ProxyCounter struct {
	TotalReqs    int64
	SuccessCount int64
	FailCount    int64
	TotalDur     int64 // nanoseconds
	lastReqs     int64
	lastTime     time.Time
}

// TaskStats internal stats with atomic ops
type TaskStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailCount     int64
	TotalDuration int64
	MinDuration   int64
	MaxDuration   int64
	StatusCodes   map[int]int64
	StatusCodeMux sync.Mutex
	RecentLogs    []RequestLog
	LogMux        sync.Mutex
	AllLogs       []RequestLog
	AllLogMux     sync.Mutex
	ProxyCounters map[string]*ProxyCounter // key: proxyID
	ProxyCounterMux sync.Mutex
}

// TaskConfig load test config
type TaskConfig struct {
	URL         string            `json:"url"`
	Method      string            `json:"method"`
	Body        string            `json:"body"`
	Concurrency int               `json:"concurrency"`
	Duration    Duration          `json:"duration"`
	TotalReqs   int64             `json:"totalReqs"`
	Headers     map[string]string `json:"headers"`
	Timeout     Duration          `json:"timeout"`
	KeepAlive   bool              `json:"keepAlive"`
	CreatedAt   time.Time         `json:"createdAt"`
	Proxies     []string          `json:"proxies"` // "local" or proxyID
}

// Task single load test task
type Task struct {
	ID        string
	Config    *TaskConfig
	Stats     *TaskStats
	Ctx       context.Context
	Cancel    context.CancelFunc
	Wg        sync.WaitGroup
	CreatedAt time.Time
	StartedAt time.Time
	FinishedAt time.Time
	lastReqs  int64
	lastTime  time.Time
	bodyBytes []byte
	proxyMgr  *ProxyManager // reference to global proxy manager
	proxyNames map[string]string // proxyID -> name for display
}

// NewTask creates a load test task
func NewTask(id string, cfg *TaskConfig, pm *ProxyManager) *Task {
	t := &Task{
		ID:        id,
		Config:    cfg,
		CreatedAt: time.Now(),
		Stats: &TaskStats{
			MinDuration:   math.MaxInt64,
			StatusCodes:   make(map[int]int64),
			RecentLogs:    make([]RequestLog, 0, 50),
			AllLogs:       make([]RequestLog, 0, 50000),
			ProxyCounters: make(map[string]*ProxyCounter),
		},
		proxyMgr:   pm,
		proxyNames: make(map[string]string),
	}
	if cfg.Body != "" {
		t.bodyBytes = []byte(cfg.Body)
	}
	// Build proxy name map
	t.proxyNames["local"] = "Local"
	for _, pid := range cfg.Proxies {
		if pid != "local" {
			if p, ok := pm.GetProxy(pid); ok {
				t.proxyNames[pid] = p.Name
			}
		}
	}
	return t
}

// Start launches the load test
func (t *Task) Start() error {
	t.Ctx, t.Cancel = context.WithCancel(context.Background())
	t.StartedAt = time.Now()
	t.lastTime = time.Now()

	// Ensure proxies list has at least "local"
	proxies := t.Config.Proxies
	if len(proxies) == 0 {
		proxies = []string{"local"}
	}

	// Initialize per-proxy counters
	for _, pid := range proxies {
		t.Stats.ProxyCounterMux.Lock()
		if _, ok := t.Stats.ProxyCounters[pid]; !ok {
			t.Stats.ProxyCounters[pid] = &ProxyCounter{
				lastTime: time.Now(),
			}
		}
		t.Stats.ProxyCounterMux.Unlock()
	}

	// Start SSH tunnels for non-local proxies
	localPorts := make(map[string]int) // proxyID -> localPort
	for _, pid := range proxies {
		if pid != "local" {
			port, err := t.proxyMgr.StartProxy(pid)
			if err != nil {
				// Stop any already-started proxies
				t.proxyMgr.StopAll()
				return fmt.Errorf("SSH proxy %s failed: %w", t.proxyNames[pid], err)
			}
			localPorts[pid] = port
		}
	}

	reqChan := make(chan struct{}, t.Config.Concurrency*len(proxies)*2)

	// Start workers per proxy
	for _, pid := range proxies {
		client := t.createHTTPClient(pid, localPorts)
		for i := 0; i < t.Config.Concurrency; i++ {
			t.Wg.Add(1)
			go func(proxyID string, c *http.Client) {
				defer t.Wg.Done()
				t.worker(c, reqChan, proxyID)
			}(pid, client)
		}
	}

	// Producer
	t.Wg.Add(1)
	go func() {
		defer t.Wg.Done()
		defer close(reqChan)
		if t.Config.TotalReqs > 0 {
			for i := int64(0); i < t.Config.TotalReqs; i++ {
				select {
				case <-t.Ctx.Done():
					return
				case reqChan <- struct{}{}:
				}
			}
		} else {
			deadline := time.Now().Add(t.Config.Duration.Duration)
			for time.Now().Before(deadline) {
				select {
				case <-t.Ctx.Done():
					return
				case reqChan <- struct{}{}:
				}
			}
		}
	}()

	// Completion watcher
	go func() {
		t.Wg.Wait()
		t.FinishedAt = time.Now()
		t.proxyMgr.StopAll()
		t.Cancel()
	}()

	return nil
}

// createHTTPClient builds an http.Client for a given proxy
func (t *Task) createHTTPClient(proxyID string, localPorts map[string]int) *http.Client {
	transport := &http.Transport{
		MaxIdleConnsPerHost: t.Config.Concurrency,
		DisableKeepAlives:   !t.Config.KeepAlive,
	}

	if proxyID != "local" {
		port := localPorts[proxyID]
		proxyAddr := fmt.Sprintf("127.0.0.1:%d", port)
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return socks5DialContext(ctx, network, addr, proxyAddr)
		}
	}

	return &http.Client{
		Transport: transport,
		Timeout:   t.Config.Timeout.Duration,
	}
}

// Stop stops the load test
func (t *Task) Stop() {
	if t.Cancel != nil {
		t.Cancel()
	}
	t.proxyMgr.StopAll()
}

// IsRunning checks if running
func (t *Task) IsRunning() bool {
	if t.Ctx == nil {
		return false
	}
	select {
	case <-t.Ctx.Done():
		return false
	default:
		return true
	}
}

// Snapshot returns current stats
func (t *Task) Snapshot() StatsSnapshot {
	total := atomic.LoadInt64(&t.Stats.TotalRequests)
	success := atomic.LoadInt64(&t.Stats.SuccessCount)
	fail := atomic.LoadInt64(&t.Stats.FailCount)
	totalDur := atomic.LoadInt64(&t.Stats.TotalDuration)
	minDur := atomic.LoadInt64(&t.Stats.MinDuration)
	maxDur := atomic.LoadInt64(&t.Stats.MaxDuration)

	if minDur == math.MaxInt64 {
		minDur = 0
	}

	now := time.Now()
	currentReqs := total
	intervalReqs := currentReqs - t.lastReqs
	elapsed := now.Sub(t.lastTime).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	qps := float64(intervalReqs) / elapsed
	t.lastReqs = currentReqs
	t.lastTime = now

	var avgMs, overallQPS float64
	if total > 0 {
		avgMs = float64(totalDur) / float64(total) / 1e6
		overallQPS = float64(success) / (float64(totalDur) / 1e9)
	}

	status := "running"
	if !t.IsRunning() {
		status = "stopped"
	}

	t.Stats.StatusCodeMux.Lock()
	codes := make(map[int]int64, len(t.Stats.StatusCodes))
	for k, v := range t.Stats.StatusCodes {
		codes[k] = v
	}
	t.Stats.StatusCodeMux.Unlock()

	t.Stats.LogMux.Lock()
	logs := make([]RequestLog, len(t.Stats.RecentLogs))
	copy(logs, t.Stats.RecentLogs)
	t.Stats.LogMux.Unlock()

	headersStr := ""
	if len(t.Config.Headers) > 0 {
		parts := make([]string, 0, len(t.Config.Headers))
		for k, v := range t.Config.Headers {
			parts = append(parts, k+": "+v)
		}
		headersStr = strings.Join(parts, "; ")
	}

	// Per-proxy breakdown
	t.Stats.ProxyCounterMux.Lock()
	proxyBreakdown := make([]ProxyStatsItem, 0, len(t.Stats.ProxyCounters))
	for pid, pc := range t.Stats.ProxyCounters {
		pcReqs := atomic.LoadInt64(&pc.TotalReqs)
		pcSuccess := atomic.LoadInt64(&pc.SuccessCount)
		pcFail := atomic.LoadInt64(&pc.FailCount)
		pcDur := atomic.LoadInt64(&pc.TotalDur)

		pcInterval := pcReqs - pc.lastReqs
		pcElapsed := now.Sub(pc.lastTime).Seconds()
		if pcElapsed <= 0 {
			pcElapsed = 1
		}
		pcQPS := float64(pcInterval) / pcElapsed
		pc.lastReqs = pcReqs
		pc.lastTime = now

		var pcAvg float64
		if pcReqs > 0 {
			pcAvg = float64(pcDur) / float64(pcReqs) / 1e6
		}

		proxyBreakdown = append(proxyBreakdown, ProxyStatsItem{
			ProxyID:      pid,
			ProxyName:    t.proxyNames[pid],
			QPS:          math.Round(pcQPS*10) / 10,
			TotalReqs:    pcReqs,
			SuccessCount: pcSuccess,
			FailCount:    pcFail,
			AvgMs:        math.Round(pcAvg*100) / 100,
		})
	}
	t.Stats.ProxyCounterMux.Unlock()

	snap := StatsSnapshot{
		TaskID:        t.ID,
		URL:           t.Config.URL,
		Method:        t.Config.Method,
		Body:          t.Config.Body,
		Concurrency:   t.Config.Concurrency,
		Status:        status,
		QPS:           math.Round(qps*10) / 10,
		TotalReqs:     total,
		SuccessCount:  success,
		FailCount:     fail,
		SuccessRate:   successRate(success, total),
		AvgMs:         math.Round(avgMs*100) / 100,
		MinMs:         math.Round(float64(minDur)/1e6*100) / 100,
		MaxMs:         math.Round(float64(maxDur)/1e6*100) / 100,
		StatusCodes:   codes,
		ElapsedMs:     time.Since(t.StartedAt).Milliseconds(),
		RecentLogs:    logs,
		CreatedAt:     t.CreatedAt.Format("2006-01-02 15:04:05"),
		StartedAt:     t.StartedAt.Format("2006-01-02 15:04:05"),
		Headers:       headersStr,
		TimeoutSec:    int(t.Config.Timeout.Duration.Seconds()),
		KeepAlive:     t.Config.KeepAlive,
		ProxyBreakdown: proxyBreakdown,
	}

	if t.Config.Duration.Duration > 0 {
		snap.DurationLimit = t.Config.Duration.Duration.String()
	}
	if t.Config.TotalReqs > 0 {
		snap.TotalReqLimit = t.Config.TotalReqs
	}
	if !t.FinishedAt.IsZero() {
		snap.FinishedAt = t.FinishedAt.Format("2006-01-02 15:04:05")
	}
	if status == "stopped" && total > 0 {
		snap.QPS = math.Round(overallQPS*10) / 10
	}

	return snap
}

// ExportCSV exports all logs as CSV
func (t *Task) ExportCSV() ([]byte, error) {
	t.Stats.AllLogMux.Lock()
	logs := make([]RequestLog, len(t.Stats.AllLogs))
	copy(logs, t.Stats.AllLogs)
	t.Stats.AllLogMux.Unlock()

	var buf bytes.Buffer
	buf.WriteString("\xEF\xBB\xBF")
	w := csv.NewWriter(&buf)
	w.Write([]string{"序号", "时间", "状态码", "耗时(ms)", "代理", "错误信息"})
	for _, log := range logs {
		errMsg := log.Error
		sc := fmt.Sprintf("%d", log.StatusCode)
		if log.StatusCode == 0 {
			sc = "ERR"
		}
		pName := t.proxyNames[log.ProxyID]
		if pName == "" {
			pName = log.ProxyID
		}
		w.Write([]string{
			fmt.Sprintf("%d", log.Seq),
			log.Timestamp.Format("15:04:05.000"),
			sc,
			fmt.Sprintf("%.2f", log.DurationMs),
			pName,
			errMsg,
		})
	}
	w.Flush()
	return buf.Bytes(), nil
}

func (t *Task) worker(client *http.Client, reqChan <-chan struct{}, proxyID string) {
	for range reqChan {
		select {
		case <-t.Ctx.Done():
			return
		default:
		}
		t.sendRequest(client, proxyID)
	}
}

func (t *Task) sendRequest(client *http.Client, proxyID string) {
	start := time.Now()

	var bodyReader io.Reader
	if t.bodyBytes != nil {
		bodyReader = bytes.NewReader(t.bodyBytes)
	}

	req, err := http.NewRequestWithContext(t.Ctx, t.Config.Method, t.Config.URL, bodyReader)
	if err != nil {
		seq := atomic.AddInt64(&t.Stats.TotalRequests, 1)
		atomic.AddInt64(&t.Stats.FailCount, 1)
		t.incProxyCounter(proxyID, 0, true, 0)
		t.appendLog(RequestLog{
			Seq: int(seq), StatusCode: 0, DurationMs: 0,
			Error: err.Error(), Timestamp: start, ProxyID: proxyID,
		}, true)
		return
	}

	for k, v := range t.Config.Headers {
		req.Header.Set(k, v)
	}

	// Random User-Agent (skip if user explicitly set one)
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", randomUA())
	}

	resp, err := client.Do(req)
	elapsed := time.Since(start).Nanoseconds()

	seq := atomic.AddInt64(&t.Stats.TotalRequests, 1)
	atomic.AddInt64(&t.Stats.TotalDuration, elapsed)

	for {
		oldMin := atomic.LoadInt64(&t.Stats.MinDuration)
		if elapsed >= oldMin {
			break
		}
		if atomic.CompareAndSwapInt64(&t.Stats.MinDuration, oldMin, elapsed) {
			break
		}
	}
	for {
		oldMax := atomic.LoadInt64(&t.Stats.MaxDuration)
		if elapsed <= oldMax {
			break
		}
		if atomic.CompareAndSwapInt64(&t.Stats.MaxDuration, oldMax, elapsed) {
			break
		}
	}

	logEntry := RequestLog{
		Seq: int(seq), DurationMs: float64(elapsed) / 1e6,
		Timestamp: start, ProxyID: proxyID,
	}

	if err != nil {
		atomic.AddInt64(&t.Stats.FailCount, 1)
		logEntry.StatusCode = 0
		logEntry.Error = err.Error()
		t.incProxyCounter(proxyID, elapsed, true, 0)
		t.appendLog(logEntry, true)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	atomic.AddInt64(&t.Stats.SuccessCount, 1)
	logEntry.StatusCode = resp.StatusCode

	t.Stats.StatusCodeMux.Lock()
	t.Stats.StatusCodes[resp.StatusCode]++
	t.Stats.StatusCodeMux.Unlock()

	t.incProxyCounter(proxyID, elapsed, false, resp.StatusCode)
	t.appendLog(logEntry, false)
}

// incProxyCounter updates per-proxy counters
func (t *Task) incProxyCounter(proxyID string, elapsed int64, isFail bool, statusCode int) {
	t.Stats.ProxyCounterMux.Lock()
	pc, ok := t.Stats.ProxyCounters[proxyID]
	if !ok {
		pc = &ProxyCounter{lastTime: time.Now()}
		t.Stats.ProxyCounters[proxyID] = pc
	}
	t.Stats.ProxyCounterMux.Unlock()

	atomic.AddInt64(&pc.TotalReqs, 1)
	atomic.AddInt64(&pc.TotalDur, elapsed)
	if isFail {
		atomic.AddInt64(&pc.FailCount, 1)
	} else {
		atomic.AddInt64(&pc.SuccessCount, 1)
	}
}

// appendLog appends request log
func (t *Task) appendLog(log RequestLog, isFail bool) {
	t.Stats.LogMux.Lock()
	if len(t.Stats.RecentLogs) >= 50 {
		t.Stats.RecentLogs = t.Stats.RecentLogs[1:]
	}
	t.Stats.RecentLogs = append(t.Stats.RecentLogs, log)
	t.Stats.LogMux.Unlock()

	t.Stats.AllLogMux.Lock()
	shouldStore := true
	if !isFail && len(t.Stats.AllLogs) > 10000 {
		shouldStore = log.Seq%10 == 0
	}
	if shouldStore {
		t.Stats.AllLogs = append(t.Stats.AllLogs, log)
	}
	t.Stats.AllLogMux.Unlock()
}

func successRate(success, total int64) float64 {
	if total == 0 {
		return 0
	}
	return math.Round(float64(success)/float64(total)*10000) / 100
}

// ==================== TaskManager ====================

type TaskManager struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

func NewTaskManager() *TaskManager {
	return &TaskManager{tasks: make(map[string]*Task)}
}

func (tm *TaskManager) AddTask(task *Task) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tasks[task.ID] = task
}

func (tm *TaskManager) GetTask(id string) (*Task, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t, ok := tm.tasks[id]
	return t, ok
}

func (tm *TaskManager) RemoveTask(id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.tasks, id)
}

func (tm *TaskManager) ListTasks() []StatsSnapshot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	result := make([]StatsSnapshot, 0, len(tm.tasks))
	for _, t := range tm.tasks {
		result = append(result, t.Snapshot())
	}
	return result
}

func (tm *TaskManager) CleanStopped() int {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	count := 0
	for id, t := range tm.tasks {
		if !t.IsRunning() {
			delete(tm.tasks, id)
			count++
		}
	}
	return count
}

// ValidateConfig validates task config
func ValidateConfig(cfg *TaskConfig) error {
	if cfg.URL == "" {
		return fmt.Errorf("URL 不能为空")
	}
	if !strings.HasPrefix(cfg.URL, "http://") && !strings.HasPrefix(cfg.URL, "https://") {
		return fmt.Errorf("URL 必须以 http:// 或 https:// 开头")
	}
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("并发数必须 > 0")
	}
	if cfg.Concurrency > 5000 {
		return fmt.Errorf("并发数不能超过 5000")
	}
	if cfg.Duration.Duration == 0 && cfg.TotalReqs == 0 {
		return fmt.Errorf("请指定持续时间或总请求数")
	}
	if cfg.Duration.Duration > 0 && cfg.TotalReqs > 0 {
		return fmt.Errorf("持续时间和总请求数互斥")
	}
	if cfg.Duration.Duration > 10*time.Minute {
		return fmt.Errorf("持续时间不能超过 10 分钟")
	}
	if cfg.TotalReqs > 1000000 {
		return fmt.Errorf("总请求数不能超过 1000000")
	}
	if cfg.Method == "" {
		cfg.Method = "GET"
	}
	methods := map[string]bool{"GET": true, "POST": true, "PUT": true, "DELETE": true, "HEAD": true, "PATCH": true}
	if !methods[strings.ToUpper(cfg.Method)] {
		return fmt.Errorf("不支持的 HTTP 方法: %s", cfg.Method)
	}
	if cfg.Timeout.Duration == 0 {
		cfg.Timeout = Duration{10 * time.Second}
	}
	if cfg.CreatedAt.IsZero() {
		cfg.CreatedAt = time.Now()
	}
	if len(cfg.Proxies) == 0 {
		cfg.Proxies = []string{"local"}
	}
	return nil
}
