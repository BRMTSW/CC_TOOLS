# CC TOOLS - HTTP QPS Load Tester

HTTP 压力测试工具，支持 CLI 和 Web 两种模式。静态编译，无 glibc 依赖，兼容所有 Linux 发行版。

## 功能

- 高并发 QPS 压测（goroutine 池，最大 5000 并发）
- 请求 Body 支持（POST/PUT/PATCH）
- 自定义 Headers
- Keep-Alive 开关
- Web 模式：浏览器操作，实时 SSE 推送统计
- 实时 QPS 折线图（Canvas）
- 状态码分布条 + 图例
- 响应日志实时滚动（含时间戳）
- 任务管理（多任务并行、启动/停止）
- CSV 详细记录下载
- 登录认证

## 快速使用

```bash
chmod +x http-qps-web-linux-amd64
./http-qps-web-linux-amd64 -port 8080
# 浏览器访问 http://<ip>:8080
```

## CLI 版本

```bash
./http-qps-cli-linux-amd64 -url https://example.com -c 100 -duration 30s

# 完整参数
./http-qps-cli-linux-amd64 \
  -url https://example.com/api \
  -method POST \
  -c 200 \
  -duration 1m \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer token123" \
  -timeout 10s \
  -keepalive=true
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `-url` | 必填 | 目标 URL |
| `-method` | GET | HTTP 方法 |
| `-c` | 10 | 并发数 |
| `-duration` | 10s | 压测时长（与 -n 互斥）|
| `-n` | 0 | 总请求数（与 -duration 互斥）|
| `-H` | 无 | 自定义 Header，可多次指定 |
| `-timeout` | 10s | 单请求超时 |
| `-keepalive` | true | 启用 Keep-Alive |

## Web 版本

```bash
./http-qps-web-linux-amd64 -port 8080
# 浏览器访问 http://<ip>:8080
```

### API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/start` | 启动压测任务 |
| POST | `/api/stop` | 停止任务 |
| GET | `/api/tasks` | 任务列表 |
| GET | `/api/stats/{id}` | SSE 实时数据流 |
| GET | `/api/export/{id}` | 下载 CSV 记录 |
| POST | `/api/clean` | 清理已停止任务 |

## 编译

```bash
# CLI（静态编译，无 glibc 依赖）
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o http-qps-cli .

# Web
cd web
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o http-qps-web .
```

## 目录结构

```
├── main.go          # CLI 入口
├── go.mod
├── release/
│   ├── http-qps-cli-linux-amd64
│   └── http-qps-web-linux-amd64
├── web/
│   ├── main.go      # Web 服务入口 + 前端页面
│   ├── engine.go    # 压测引擎
│   └── go.mod
```
