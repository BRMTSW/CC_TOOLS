package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Stats struct {
	TotalRequests int64
	SuccessCount  int64
	FailCount     int64
	TotalDuration int64 // nanoseconds
	MinDuration   int64 // nanoseconds
	MaxDuration   int64 // nanoseconds
	StatusCodes   map[int]int64
	StatusCodeMux sync.Mutex
}

type Config struct {
	URL         string
	Method      string
	Concurrency int
	Duration    time.Duration
	TotalReqs   int64
	Headers     []string
	Timeout     time.Duration
	KeepAlive   bool
}

func main() {
	cfg := parseFlags()

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "❌ 配置错误: %v\n", err)
		os.Exit(1)
	}

	printBanner(cfg)

	stats := &Stats{
		MinDuration: math.MaxInt64,
		StatusCodes: make(map[int]int64),
	}

	client := createHTTPClient(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 信号监听
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n🛑 收到停止信号，正在退出...")
		cancel()
	}()

	// 请求分配 channel
	reqChan := make(chan struct{}, cfg.Concurrency*2)

	// 启动实时统计
	var stopStats atomic.Bool
	go liveStats(ctx, stats, &stopStats)

	// 启动 worker
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			worker(ctx, cfg, client, reqChan, stats, id)
		}(i)
	}

	// 分发请求
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		if cfg.TotalReqs > 0 {
			for i := int64(0); i < cfg.TotalReqs; i++ {
				select {
				case <-ctx.Done():
					return
				case reqChan <- struct{}{}:
				}
			}
		} else {
			deadline := time.Now().Add(cfg.Duration)
			for time.Now().Before(deadline) {
				select {
				case <-ctx.Done():
					return
				case reqChan <- struct{}{}:
				}
			}
		}
	}()

	// 等待生产者完成
	<-producerDone
	close(reqChan)

	// 等待所有 worker 完成
	wg.Wait()

	stopStats.Store(true)
	time.Sleep(100 * time.Millisecond) // 等待统计协程退出

	printFinalReport(stats, cfg)
}

func parseFlags() *Config {
	cfg := &Config{}

	flag.StringVar(&cfg.URL, "url", "", "目标 URL (例如 https://example.com/api)")
	flag.StringVar(&cfg.Method, "method", "GET", "HTTP 方法 (GET/POST/PUT/DELETE)")
	flag.IntVar(&cfg.Concurrency, "c", 10, "并发数 (goroutine 数量)")
	flag.DurationVar(&cfg.Duration, "duration", 0, "压测持续时间 (例如 30s, 1m, 5m)，与 -n 互斥")
	flag.Int64Var(&cfg.TotalReqs, "n", 0, "总请求数 (例如 10000)，与 -duration 互斥")
	flag.Var((*stringSliceFlag)(&cfg.Headers), "H", "自定义 Header (可多次使用，格式: 'Key: Value')")
	flag.DurationVar(&cfg.Timeout, "timeout", 10*time.Second, "单个请求超时时间")
	flag.BoolVar(&cfg.KeepAlive, "keepalive", true, "是否启用 Keep-Alive")

	flag.Parse()

	// 默认：如果没指定 duration 和 n，则默认 10s
	if cfg.Duration == 0 && cfg.TotalReqs == 0 {
		cfg.Duration = 10 * time.Second
	}

	return cfg
}

func validateConfig(cfg *Config) error {
	if cfg.URL == "" {
		return fmt.Errorf("请使用 -url 指定目标 URL")
	}
	parsed, err := url.Parse(cfg.URL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("URL 格式无效，需要 http:// 或 https:// 开头")
	}
	if cfg.Concurrency <= 0 {
		return fmt.Errorf("并发数必须 > 0")
	}
	if cfg.Duration > 0 && cfg.TotalReqs > 0 {
		return fmt.Errorf("-duration 和 -n 互斥，请只指定一个")
	}
	return nil
}

func printBanner(cfg *Config) {
	fmt.Println("========================================")
	fmt.Println("   🔥 HTTP QPS 压测工具 (Go版)")
	fmt.Println("========================================")
	fmt.Printf("  目标 URL : %s\n", cfg.URL)
	fmt.Printf("  方法     : %s\n", cfg.Method)
	fmt.Printf("  并发数   : %d\n", cfg.Concurrency)
	if cfg.TotalReqs > 0 {
		fmt.Printf("  总请求数 : %d\n", cfg.TotalReqs)
	} else {
		fmt.Printf("  持续时间 : %v\n", cfg.Duration)
	}
	fmt.Printf("  超时     : %v\n", cfg.Timeout)
	fmt.Printf("  KeepAlive: %v\n", cfg.KeepAlive)
	for _, h := range cfg.Headers {
		fmt.Printf("  Header   : %s\n", h)
	}
	fmt.Println("========================================")
	fmt.Println("⏳ 开始压测... (Ctrl+C 停止)")
	fmt.Println()
}

func createHTTPClient(cfg *Config) *http.Client {
	transport := &http.Transport{
		MaxIdleConnsPerHost: cfg.Concurrency,
		DisableKeepAlives:   !cfg.KeepAlive,
	}
	return &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
	}
}

func worker(ctx context.Context, cfg *Config, client *http.Client, reqChan <-chan struct{}, stats *Stats, id int) {
	for range reqChan {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sendRequest(ctx, cfg, client, stats)
	}
}

func sendRequest(ctx context.Context, cfg *Config, client *http.Client, stats *Stats) {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, cfg.Method, cfg.URL, nil)
	if err != nil {
		atomic.AddInt64(&stats.FailCount, 1)
		atomic.AddInt64(&stats.TotalRequests, 1)
		return
	}

	// 设置自定义 Headers
	for _, h := range cfg.Headers {
		parts := strings.SplitN(h, ":", 2)
		if len(parts) == 2 {
			req.Header.Set(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
		}
	}

	resp, err := client.Do(req)
	elapsed := time.Since(start).Nanoseconds()

	atomic.AddInt64(&stats.TotalRequests, 1)
	atomic.AddInt64(&stats.TotalDuration, elapsed)

	// 更新最小耗时 (CAS)
	for {
		oldMin := atomic.LoadInt64(&stats.MinDuration)
		if elapsed >= oldMin {
			break
		}
		if atomic.CompareAndSwapInt64(&stats.MinDuration, oldMin, elapsed) {
			break
		}
	}
	// 更新最大耗时 (CAS)
	for {
		oldMax := atomic.LoadInt64(&stats.MaxDuration)
		if elapsed <= oldMax {
			break
		}
		if atomic.CompareAndSwapInt64(&stats.MaxDuration, oldMax, elapsed) {
			break
		}
	}

	if err != nil {
		atomic.AddInt64(&stats.FailCount, 1)
		return
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body) // 读完 body 以便复用连接

	atomic.AddInt64(&stats.SuccessCount, 1)

	stats.StatusCodeMux.Lock()
	stats.StatusCodes[resp.StatusCode]++
	stats.StatusCodeMux.Unlock()
}

func liveStats(ctx context.Context, stats *Stats, stopStats *atomic.Bool) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	lastReqs := int64(0)
	lastTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if stopStats.Load() {
				return
			}
			now := time.Now()
			currentReqs := atomic.LoadInt64(&stats.TotalRequests)
			intervalReqs := currentReqs - lastReqs
			elapsed := now.Sub(lastTime).Seconds()
			if elapsed <= 0 {
				elapsed = 1
			}
			qps := float64(intervalReqs) / elapsed
			success := atomic.LoadInt64(&stats.SuccessCount)
			fail := atomic.LoadInt64(&stats.FailCount)
			total := currentReqs

			fmt.Printf("  📊 实时: QPS=%.0f | 总请求=%d | 成功=%d | 失败=%d | 成功率=%.1f%%\n",
				qps, total, success, fail, successRate(success, total))

			lastReqs = currentReqs
			lastTime = now
		}
	}
}

func printFinalReport(stats *Stats, cfg *Config) {
	total := atomic.LoadInt64(&stats.TotalRequests)
	success := atomic.LoadInt64(&stats.SuccessCount)
	fail := atomic.LoadInt64(&stats.FailCount)
	totalDur := atomic.LoadInt64(&stats.TotalDuration)
	minDur := atomic.LoadInt64(&stats.MinDuration)
	maxDur := atomic.LoadInt64(&stats.MaxDuration)

	if minDur == math.MaxInt64 {
		minDur = 0
	}

	var avgDur, qps float64
	if total > 0 {
		avgDur = float64(totalDur) / float64(total) / 1e6 // ms
		qps = float64(success) / (float64(totalDur) / 1e9)
	}
	minMs := float64(minDur) / 1e6
	maxMs := float64(maxDur) / 1e6

	fmt.Println()
	fmt.Println("========================================")
	fmt.Println("   📋 压测报告")
	fmt.Println("========================================")
	fmt.Printf("  目标 URL   : %s\n", cfg.URL)
	fmt.Printf("  总请求数   : %d\n", total)
	fmt.Printf("  成功请求数 : %d\n", success)
	fmt.Printf("  失败请求数 : %d\n", fail)
	fmt.Printf("  成功率     : %.2f%%\n", successRate(success, total))
	fmt.Println("────────────────────────────────────────")
	fmt.Printf("  QPS        : %.2f\n", qps)
	fmt.Printf("  平均耗时   : %.2f ms\n", avgDur)
	fmt.Printf("  最小耗时   : %.2f ms\n", minMs)
	fmt.Printf("  最大耗时   : %.2f ms\n", maxMs)
	fmt.Println("────────────────────────────────────────")
	fmt.Println("  状态码分布:")
	stats.StatusCodeMux.Lock()
	for code, count := range stats.StatusCodes {
		fmt.Printf("    %d : %d 次\n", code, count)
	}
	stats.StatusCodeMux.Unlock()
	fmt.Println("========================================")
}

func successRate(success, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(success) / float64(total) * 100
}

// ========== 自定义 flag 类型 ==========

type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ", ") }
func (s *stringSliceFlag) Set(val string) error {
	*s = append(*s, val)
	return nil
}
