package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Duration 自定义 JSON 反序列化，支持 "30s", "1m", "5m" 等格式
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

// RequestLog 单条请求日志
type RequestLog struct {
	Seq        int       `json:"seq"`
	StatusCode int       `json:"statusCode"`
	DurationMs float64   `json:"durationMs"`
	Error      string    `json:"error,omitempty"`
	Timestamp  time.Time `json:"ts"`
}

// StatsSnapshot 是 JSON 友好的统计快照，用于 SSE 推送
type StatsSnapshot struct {
	TaskID        string        `json:"taskId"`
	URL           string        `json:"url"`
	Method        string        `json:"method"`
	Body          string        `json:"body,omitempty"`
	Concurrency   int           `json:"concurrency"`
	Status        string        `json:"status"` // "running" | "stopped"
	QPS           float64       `json:"qps"`
	TotalReqs     int64         `json:"totalReqs"`
	SuccessCount  int64         `json:"successCount"`
	FailCount     int64         `json:"failCount"`
	SuccessRate   float64       `json:"successRate"`
	AvgMs         float64       `json:"avgMs"`
	MinMs         float64       `json:"minMs"`
	MaxMs         float64       `json:"maxMs"`
	StatusCodes   map[int]int64 `json:"statusCodes"`
	ElapsedMs     int64         `json:"elapsedMs"`
	DurationLimit string        `json:"durationLimit,omitempty"`
	TotalReqLimit int64         `json:"totalReqLimit,omitempty"`
	RecentLogs    []RequestLog  `json:"recentLogs"`
	// 详细信息
	CreatedAt   string `json:"createdAt"`
	StartedAt   string `json:"startedAt"`
	FinishedAt  string `json:"finishedAt,omitempty"`
	Headers     string `json:"headers,omitempty"`
	TimeoutSec  int    `json:"timeoutSec"`
	KeepAlive   bool   `json:"keepAlive"`
}

// TaskStats 内部统计结构（使用 atomic 操作）
type TaskStats struct {
	TotalRequests int64
	SuccessCount  int64
	FailCount     int64
	TotalDuration int64 // nanoseconds
	MinDuration   int64 // nanoseconds
	MaxDuration   int64 // nanoseconds
	StatusCodes   map[int]int64
	StatusCodeMux sync.Mutex
	RecentLogs    []RequestLog
	LogMux        sync.Mutex
	AllLogs       []RequestLog // 完整日志，用于导出
	AllLogMux     sync.Mutex
}

// TaskConfig 压测任务配置
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
}

// Task 单个压测任务
type Task struct {
	ID     string
	Config *TaskConfig
	Stats  *TaskStats
	Ctx    context.Context
	Cancel context.CancelFunc
	Wg     sync.WaitGroup
	// 时间记录
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	// 用于计算实时 QPS
	lastReqs   int64
	lastTime   time.Time
	currentQPS float64
	// Body []byte 缓存
	bodyBytes []byte
}

// NewTask 创建压测任务
func NewTask(id string, cfg *TaskConfig) *Task {
	t := &Task{
		ID:        id,
		Config:    cfg,
		CreatedAt: time.Now(),
		Stats: &TaskStats{
			MinDuration: math.MaxInt64,
			StatusCodes: make(map[int]int64),
			RecentLogs:  make([]RequestLog, 0, 50),
			AllLogs:     make([]RequestLog, 0, 50000),
		},
	}
	if cfg.Body != "" {
		t.bodyBytes = []byte(cfg.Body)
	}
	return t
}

// Start 启动压测
func (t *Task) Start() {
	t.Ctx, t.Cancel = context.WithCancel(context.Background())
	t.StartedAt = time.Now()
	t.lastTime = time.Now()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: t.Config.Concurrency,
			DisableKeepAlives:   !t.Config.KeepAlive,
		},
		Timeout: t.Config.Timeout.Duration,
	}

	reqChan := make(chan struct{}, t.Config.Concurrency*2)

	// 启动 worker
	for i := 0; i < t.Config.Concurrency; i++ {
		t.Wg.Add(1)
		go func() {
			defer t.Wg.Done()
			t.worker(client, reqChan)
		}()
	}

	// 启动生产者
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

	// 后台协程：任务完成后记录结束时间并 cancel
	go func() {
		t.Wg.Wait()
		t.FinishedAt = time.Now()
		t.Cancel()
	}()
}

// Stop 停止压测
func (t *Task) Stop() {
	if t.Cancel != nil {
		t.Cancel()
	}
}

// IsRunning 判断是否在运行
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

// Snapshot 获取当前统计快照
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

	// 计算实时 QPS
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
	t.currentQPS = qps

	var avgMs, overallQPS float64
	if total > 0 {
		avgMs = float64(totalDur) / float64(total) / 1e6
		overallQPS = float64(success) / (float64(totalDur) / 1e9)
	}

	status := "running"
	if !t.IsRunning() {
		status = "stopped"
	}

	// 复制状态码 map
	t.Stats.StatusCodeMux.Lock()
	codes := make(map[int]int64, len(t.Stats.StatusCodes))
	for k, v := range t.Stats.StatusCodes {
		codes[k] = v
	}
	t.Stats.StatusCodeMux.Unlock()

	// 复制最近请求日志
	t.Stats.LogMux.Lock()
	logs := make([]RequestLog, len(t.Stats.RecentLogs))
	copy(logs, t.Stats.RecentLogs)
	t.Stats.LogMux.Unlock()

	// Headers 序列化
	headersStr := ""
	if len(t.Config.Headers) > 0 {
		parts := make([]string, 0, len(t.Config.Headers))
		for k, v := range t.Config.Headers {
			parts = append(parts, k+": "+v)
		}
		headersStr = strings.Join(parts, "; ")
	}

	snap := StatsSnapshot{
		TaskID:       t.ID,
		URL:          t.Config.URL,
		Method:       t.Config.Method,
		Body:         t.Config.Body,
		Concurrency:  t.Config.Concurrency,
		Status:       status,
		QPS:          math.Round(qps*10) / 10,
		TotalReqs:    total,
		SuccessCount: success,
		FailCount:    fail,
		SuccessRate:  successRate(success, total),
		AvgMs:        math.Round(avgMs*100) / 100,
		MinMs:        math.Round(float64(minDur)/1e6*100) / 100,
		MaxMs:        math.Round(float64(maxDur)/1e6*100) / 100,
		StatusCodes:  codes,
		ElapsedMs:    time.Since(t.StartedAt).Milliseconds(),
		RecentLogs:   logs,
		CreatedAt:    t.CreatedAt.Format("2006-01-02 15:04:05"),
		StartedAt:    t.StartedAt.Format("2006-01-02 15:04:05"),
		Headers:      headersStr,
		TimeoutSec:   int(t.Config.Timeout.Duration.Seconds()),
		KeepAlive:    t.Config.KeepAlive,
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

	// 如果已停止，使用整体 QPS
	if status == "stopped" && total > 0 {
		snap.QPS = math.Round(overallQPS*10) / 10
	}

	return snap
}

// ExportCSV 导出完整请求日志为 CSV
func (t *Task) ExportCSV() ([]byte, error) {
	t.Stats.AllLogMux.Lock()
	logs := make([]RequestLog, len(t.Stats.AllLogs))
	copy(logs, t.Stats.AllLogs)
	t.Stats.AllLogMux.Unlock()

	var buf bytes.Buffer
	buf.WriteString("\xEF\xBB\xBF") // UTF-8 BOM，方便 Excel 识别

	w := csv.NewWriter(&buf)
	w.Write([]string{"序号", "时间", "状态码", "耗时(ms)", "错误信息"})

	for _, log := range logs {
		errMsg := log.Error
		statusCode := fmt.Sprintf("%d", log.StatusCode)
		if log.StatusCode == 0 {
			statusCode = "ERR"
		}
		w.Write([]string{
			fmt.Sprintf("%d", log.Seq),
			log.Timestamp.Format("15:04:05.000"),
			statusCode,
			fmt.Sprintf("%.2f", log.DurationMs),
			errMsg,
		})
	}

	w.Flush()
	return buf.Bytes(), nil
}

func (t *Task) worker(client *http.Client, reqChan <-chan struct{}) {
	for range reqChan {
		select {
		case <-t.Ctx.Done():
			return
		default:
		}
		t.sendRequest(client)
	}
}

func (t *Task) sendRequest(client *http.Client) {
	start := time.Now()

	var bodyReader io.Reader
	if t.bodyBytes != nil {
		bodyReader = bytes.NewReader(t.bodyBytes)
	}

	req, err := http.NewRequestWithContext(t.Ctx, t.Config.Method, t.Config.URL, bodyReader)
	if err != nil {
		atomic.AddInt64(&t.Stats.FailCount, 1)
		seq := atomic.AddInt64(&t.Stats.TotalRequests, 1)
		t.appendLog(RequestLog{
			Seq:        int(seq),
			StatusCode: 0,
			DurationMs: 0,
			Error:      err.Error(),
			Timestamp:  start,
		}, true)
		return
	}

	// 设置自定义 Headers
	for k, v := range t.Config.Headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	elapsed := time.Since(start).Nanoseconds()

	seq := atomic.AddInt64(&t.Stats.TotalRequests, 1)
	atomic.AddInt64(&t.Stats.TotalDuration, elapsed)

	// 更新最小耗时 (CAS)
	for {
		oldMin := atomic.LoadInt64(&t.Stats.MinDuration)
		if elapsed >= oldMin {
			break
		}
		if atomic.CompareAndSwapInt64(&t.Stats.MinDuration, oldMin, elapsed) {
			break
		}
	}
	// 更新最大耗时 (CAS)
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
		Seq:        int(seq),
		DurationMs: float64(elapsed) / 1e6,
		Timestamp:  start,
	}

	if err != nil {
		atomic.AddInt64(&t.Stats.FailCount, 1)
		logEntry.StatusCode = 0
		logEntry.Error = err.Error()
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

	t.appendLog(logEntry, false)
}

// appendLog 追加请求日志：recent 保留最近 50 条，all 保留全部
func (t *Task) appendLog(log RequestLog, isFail bool) {
	// recent logs (滑动窗口)
	t.Stats.LogMux.Lock()
	if len(t.Stats.RecentLogs) >= 50 {
		t.Stats.RecentLogs = t.Stats.RecentLogs[1:]
	}
	t.Stats.RecentLogs = append(t.Stats.RecentLogs, log)
	t.Stats.LogMux.Unlock()

	// all logs (完整记录，用于导出)
	// 对失败请求，全部记录；对成功请求，采样记录（超过1万条时每10条记1条）
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

// TaskManager 管理多个压测任务
type TaskManager struct {
	mu    sync.RWMutex
	tasks map[string]*Task
}

// NewTaskManager 创建任务管理器
func NewTaskManager() *TaskManager {
	return &TaskManager{
		tasks: make(map[string]*Task),
	}
}

// AddTask 添加任务
func (tm *TaskManager) AddTask(task *Task) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tasks[task.ID] = task
}

// GetTask 获取任务
func (tm *TaskManager) GetTask(id string) (*Task, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	t, ok := tm.tasks[id]
	return t, ok
}

// RemoveTask 移除任务
func (tm *TaskManager) RemoveTask(id string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.tasks, id)
}

// ListTasks 列出所有任务快照
func (tm *TaskManager) ListTasks() []StatsSnapshot {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	result := make([]StatsSnapshot, 0, len(tm.tasks))
	for _, t := range tm.tasks {
		result = append(result, t.Snapshot())
	}
	return result
}

// CleanStopped 清理已停止的任务
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

// ValidateConfig 校验配置
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
		return fmt.Errorf("持续时间和总请求数互斥，请只指定一个")
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
	return nil
}
