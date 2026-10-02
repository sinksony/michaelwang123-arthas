// Package main 是 Arthas WebSocket 中继服务器的入口点。
//
// 本文件负责服务器的完整生命周期管理：
//   - 初始化日志、CORS 控制、Hub 等核心组件
//   - 配置 HTTP 路由（/ping 健康检查 + /ws WebSocket 升级）
//   - 启动 HTTP 服务器并监听指定端口
//   - 等待关闭信号（Task 8.2 将实现信号处理和优雅关闭）
//
// 📚 学习要点: Go 程序的入口点
// Go 程序从 main 包的 main() 函数开始执行。
// 与 C/Java 不同，Go 的 main() 没有参数和返回值。
// 程序退出码通过 os.Exit() 设置（而非 return）。
// 当 main() 返回时，所有 goroutine 会被强制终止（不会等待它们完成）。
// 这就是为什么我们需要在 main() 中阻塞，直到收到关闭信号。
//
// 📚 学习要点: 服务器启动顺序的重要性
// 初始化顺序必须严格遵守：
// 1. 日志初始化（后续所有组件都依赖日志）
// 2. 配置加载（CORS 等安全配置必须在接受连接前就绪）
// 3. 业务组件初始化（Hub）
// 4. 路由注册
// 5. 启动监听
// 如果顺序错误（如先启动监听再配置 CORS），会有短暂的安全窗口期。
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/arthas/arthas-server/internal/dailytopic"
	"github.com/arthas/arthas-server/internal/hub"
	"github.com/arthas/arthas-server/internal/logger"
	"github.com/arthas/arthas-server/internal/match"
	"github.com/arthas/arthas-server/internal/network"
	"github.com/arthas/arthas-server/internal/static"
)

// Version 是服务器版本号，可在编译时通过 ldflags 注入。
//
// 📚 学习要点: Go 的 ldflags 机制
// 使用 `go build -ldflags "-X main.Version=v2.0.0"` 可以在编译时覆盖此变量的值。
// 这是 Go 程序实现「构建时配置」的标准方式：
//   - CI/CD 流水线注入 git tag 或 commit hash 作为版本号
//   - 无需配置文件或环境变量，版本信息直接编译进二进制文件
//   - 运行时零开销（编译时已确定值）
//
// 典型用法（在 Dockerfile 或 CI 脚本中）：
//
//	go build -ldflags "-s -w -X main.Version=$(git describe --tags)" ./cmd/server
//
// -s: 去除符号表（减小二进制体积）
// -w: 去除 DWARF 调试信息（进一步减小体积）
// -X: 设置包级变量的值
var Version = "dev"

// resolvePort 根据三层优先级解析服务器监听端口。
//
// 📚 学习要点: 配置优先级模式的可测试性
// 将配置解析逻辑提取为独立函数有两个好处：
// 1. 可测试性 — 可以直接传入不同参数组合验证优先级逻辑
// 2. 单一职责 — main() 负责编排，resolvePort 负责端口决策
//
// 优先级：CLI flag > 环境变量 > 默认值
// flagVal: --port 命令行参数值（0 表示未设置，sentinel value）
// envVal: PORT 环境变量的值（空字符串表示未设置）
// defaultVal: 默认端口 "8080"
func resolvePort(flagVal int, envVal string, defaultVal string) string {
	if flagVal != 0 {
		return fmt.Sprintf("%d", flagVal)
	}
	if envVal != "" {
		return envVal
	}
	return defaultVal
}

// resolveOrigins 根据三层优先级解析允许的 WebSocket 来源。
//
// 📚 学习要点: Origins 配置的安全含义
// ALLOWED_ORIGINS 控制哪些网页可以建立 WebSocket 连接：
// - 空字符串 → 允许所有来源（适合本地开发和 Tier 1 单二进制模式）
// - "https://chat.example.com" → 仅允许指定域名（生产环境安全配置）
//
// 优先级：CLI flag > 环境变量 > 默认值（空字符串）
// flagVal: --allowed-origins 命令行参数值（空字符串表示未设置）
// envVal: ALLOWED_ORIGINS 环境变量的值
// defaultVal: 默认值（空字符串，表示允许所有来源）
func resolveOrigins(flagVal string, envVal string, defaultVal string) string {
	if flagVal != "" {
		return flagVal
	}
	if envVal != "" {
		return envVal
	}
	return defaultVal
}

// resolveMaxPublicRooms 根据两层优先级解析 Hub 最大公开房间数。
//
// 优先级：环境变量 > flag 默认值
// 由于 flag 默认值就是 200，无法区分"用户显式传了 200"和"未设置"，
// 因此策略是：如果环境变量设置了有效正整数则使用它，否则使用 flag 值。
// flagVal: --max-public-rooms 命令行参数值
// envVal: MAX_PUBLIC_ROOMS 环境变量的值
func resolveMaxPublicRooms(flagVal int, envVal string) int {
	if envVal != "" {
		if v, err := strconv.Atoi(envVal); err == nil && v > 0 {
			return v
		}
	}
	return flagVal
}

// main 是服务器入口函数，负责初始化所有组件并启动 HTTP 服务器。
//
// 执行流程：
// 1. 初始化结构化日志（必须最先执行，后续组件依赖日志输出）
// 2. 读取 ALLOWED_ORIGINS 环境变量，初始化 CORS Origin 控制
// 3. 创建 Hub 并启动事件循环（在独立 goroutine 中）
// 4. 注册 HTTP 路由（/ping + /ws）
// 5. 读取 PORT 环境变量，创建 http.Server
// 6. 在 goroutine 中启动 ListenAndServe
// 7. 输出启动日志
// 8. 阻塞等待（Task 8.2 将替换为信号处理）
func main() {
	// ─── Step 0: 解析命令行参数 ─────────────────────────────────────────────
	//
	// 📚 学习要点: flag 包与配置优先级
	// Go 标准库 flag 包提供简洁的命令行参数解析。
	// 自托管部署支持三层配置优先级：CLI flag > 环境变量 > 默认值。
	// 这遵循 12-Factor App 原则，同时为单二进制用户提供便捷的 CLI 体验：
	//   - Docker 部署：通过环境变量配置（docker-compose.yml 中设置）
	//   - 单二进制部署：通过 --port 和 --allowed-origins 快速启动
	//   - 两者都不设置：使用安全的默认值（8080 端口，允许所有来源）
	//
	// 📚 学习要点: --version flag 用于运维确认部署版本
	// 单二进制模式下，用户可能想快速确认当前运行的版本号。
	// `./arthas-server --version` 输出版本后立即退出，不启动服务器。
	// 版本号通过 ldflags 在编译时注入（见 Version 变量的注释）。
	versionFlag := flag.Bool("version", false, "Print version and exit")
	portFlag := flag.Int("port", 0, "HTTP listen port (default: $PORT or 8080)")
	originsFlag := flag.String("allowed-origins", "", "Comma-separated allowed origins (default: $ALLOWED_ORIGINS or *)")
	maxPublicRoomsFlag := flag.Int("max-public-rooms", 200, "Maximum number of public rooms in Hub (default: $MAX_PUBLIC_ROOMS or 200)")
	disableDailyTopic := flag.Bool("disable-daily-topic", false, "Disable daily topic room feature (env: DISABLE_DAILY_TOPIC)")
	disableRandomMatch := flag.Bool("disable-random-match", false, "Disable random match feature (env: DISABLE_RANDOM_MATCH)")
	flag.Parse()

	if *versionFlag {
		fmt.Println(Version)
		os.Exit(0)
	}

	// ─── Step 1: 初始化结构化日志 ───────────────────────────────────────────
	//
	// 📚 学习要点: 日志初始化必须在所有 goroutine 启动之前完成。
	// log.SetFlags() 和 log.SetOutput() 修改全局 logger 的内部状态，
	// 这些操作本身不是并发安全的（写操作）。
	// 但一旦配置完成，后续的 log.Printf() 调用是并发安全的（内部有 mutex 保护）。
	// 因此：Init() 在 main 中最先调用，确保后续所有日志调用都是安全的。
	logger.Init()

	// ─── Step 2: 初始化 CORS Origin 控制 ────────────────────────────────────
	//
	// 📚 学习要点: 安全配置必须在接受连接之前完成
	// 如果先启动服务器再配置 CORS，会有一个短暂的时间窗口，
	// 在此期间任何 Origin 的连接都会被接受（安全漏洞）。
	// 正确做法：先完成所有安全配置，再开始监听端口。
	//
	// 📚 学习要点: Origins 配置优先级 — flag > env > default
	// 与端口类似，origins 也支持三层优先级：
	//   - --allowed-origins flag：CLI 用户直接指定
	//   - ALLOWED_ORIGINS 环境变量：Docker 部署通过 compose 注入
	//   - 默认值（空字符串传给 InitOriginControl）：允许所有来源
	// 空字符串传给 InitOriginControl 时，它会允许所有 Origin（向后兼容）。
	origins := resolveOrigins(*originsFlag, os.Getenv("ALLOWED_ORIGINS"), "")
	network.InitOriginControl(origins)

	// ─── Step 3: 创建 Hub 并启动事件循环 ────────────────────────────────────
	//
	// 📚 学习要点: Hub 在独立 goroutine 中运行事件循环
	// 这是 Go 中常见的「actor 模型」实现：
	// - 一个 goroutine 独占状态（clients map）
	// - 其他 goroutine 通过 channel 发送消息来请求状态变更
	// - 消除了对共享状态的竞态条件，无需复杂的锁策略
	//
	// Hub.Run() 是一个无限循环，通过 select 多路复用处理：
	// - 客户端注册（register channel）
	// - 客户端注销（unregister channel）
	// - 关闭信号（done channel）
	wsHub := network.NewHub()
	go wsHub.Run()

	// ─── Hub Directory (Public Room Listing) ─────────────────────────────────
	maxPublicRooms := resolveMaxPublicRooms(*maxPublicRoomsFlag, os.Getenv("MAX_PUBLIC_ROOMS"))
	hubRegistry := hub.NewHubRegistry(maxPublicRooms)
	wsHub.SetHubRegistry(hubRegistry)

	hubRateLimiter := hub.NewRateLimiter(30, time.Minute)
	hubHandler := hub.NewHubHandler(hub.HubHandlerConfig{
		Registry:       hubRegistry,
		RateLimiter:    hubRateLimiter,
		AllowedOrigins: origins,
		ActivityGetter: wsHub.ActivityTracker(),
		OnlineCountFn:  wsHub.ClientCount,
	})

	// ─── Daily Topic Scheduler ───────────────────────────────────────────────
	//
	// 条件启动每日话题调度器：仅在未被 flag 或环境变量禁用时启动。
	// LoadTopics 失败时仅记录错误，不阻止服务器启动（graceful degradation）。
	if !*disableDailyTopic && os.Getenv("DISABLE_DAILY_TOPIC") != "true" {
		topics, err := dailytopic.LoadTopics()
		if err != nil {
			logger.Error("Server", "failed to load daily topics: %v", err)
		} else {
			scheduler := dailytopic.NewScheduler(topics, wsHub, nil)
			scheduler.Start()
			defer scheduler.Stop()
		}
	}

	// ─── Random Match Server ─────────────────────────────────────────────────
	//
	// Conditional initialization: disabled via --disable-random-match flag or
	// DISABLE_RANDOM_MATCH=true environment variable. When disabled, the Hub's
	// range router returns M001 for any 0x20-0x2F message and matchServer remains nil.
	//
	// Priority: CLI flag > env var (flag takes precedence if set).
	var matchServer *match.MatchServer
	var matchEnabledFn func() bool
	var matchQueueSizeFn func() int
	if !*disableRandomMatch && os.Getenv("DISABLE_RANDOM_MATCH") != "true" {
		matchCfg, err := match.ParseEnv()
		if err != nil {
			logger.Error("Server", "invalid match configuration: %v", err)
			os.Exit(1)
		}
		if err := matchCfg.Validate(); err != nil {
			logger.Error("Server", "match configuration validation failed: %v", err)
			os.Exit(1)
		}
		matchServer = match.NewMatchServer(matchCfg, wsHub)
		matchServer.Run()
		wsHub.SetMatchServer(matchServer)

		matchEnabledFn = func() bool { return true }
		matchQueueSizeFn = matchServer.QueueSize

		logger.Info("Server", "random match enabled (queue max: %d, timeout: %v)",
			matchCfg.MaxQueueSize, matchCfg.MatchTimeout)
	} else {
		logger.Info("Server", "random match disabled")
	}

	// ─── Hub Stats Handler ──────────────────────────────────────────────────
	// /api/hub/stats — lightweight endpoint for match feature discovery.
	// Returns online count, match enabled status, and queue size.
	// No rate limiting (lightweight read-only data).
	// Created after MatchServer init so that match functions are available.
	statsHandler := hub.NewHubStatsHandler(hub.HubStatsHandlerConfig{
		AllowedOrigins:   origins,
		OnlineCountFn:    wsHub.ClientCount,
		MatchEnabledFn:   matchEnabledFn,
		MatchQueueSizeFn: matchQueueSizeFn,
	})

	// ─── Step 4: 注册 HTTP 路由 ─────────────────────────────────────────────
	//
	// 📚 学习要点: 为什么使用 http.NewServeMux() 而非 http.DefaultServeMux？
	//
	// http.DefaultServeMux 是一个全局变量（包级别的 *http.ServeMux）。
	// 使用全局变量的问题：
	// 1. 测试隔离性差 — 多个测试并行运行时会互相干扰（路由注册是全局的）
	// 2. 隐式依赖 — 任何包都可以通过 http.Handle() 注册路由，难以追踪
	// 3. 安全风险 — 第三方库可能偷偷注册调试路由（如 /debug/pprof）
	//
	// 显式创建 ServeMux 的优势：
	// 1. 可测试 — 每个测试可以创建独立的 mux，互不影响
	// 2. 可审计 — 所有路由在一个地方注册，一目了然
	// 3. 可组合 — 可以为不同端口创建不同的 mux（如管理端口 vs 业务端口）
	mux := http.NewServeMux()

	// /ping — 健康检查端点
	// 用途：容器编排器（Docker HEALTHCHECK、K8s liveness probe）和
	// 外部保活服务（cron-job.org）通过此端点验证服务存活。
	mux.HandleFunc("/ping", handlePing)

	// /ws — WebSocket 升级端点
	// 所有客户端通过此端点建立 WebSocket 连接。
	// ServeWs 内部处理：Origin 验证 → HTTP Upgrade → 创建 Client → 启动读写 goroutine
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		network.ServeWs(wsHub, w, r)
	})

	// /api/hub — Hub directory API endpoint
	// Returns paginated list of public rooms as JSON. Rate-limited per IP.
	mux.Handle("/api/hub", hubHandler)

	// /api/hub/stats — Hub stats API endpoint
	// Returns online count, match enabled status, and match queue size as JSON.
	mux.Handle("/api/hub/stats", statsHandler)

	// / — 前端静态文件服务（SPA fallback）
	//
	// 📚 学习要点: 路由优先级与 ServeMux 匹配规则
	// Go 1.22+ 的 ServeMux 按最长前缀匹配路由。
	// /ws 和 /ping 是精确路径，优先于 / 的通配匹配。
	// 因此即使 static.Handler() 注册在 /，也不会拦截 /ws 和 /ping 请求。
	//
	// 📚 学习要点: 为什么在 /ws 和 /ping 之后注册？
	// 虽然 ServeMux 的匹配规则保证精确路径优先，
	// 但代码顺序上「先注册精确路径，后注册通配」更符合阅读直觉，
	// 让维护者一眼看出路由优先级。
	//
	// SPA 路由覆盖说明：
	// static.Handler() 对所有不存在于 dist/ 中的路径返回 index.html。
	// 这涵盖了所有前端路由，包括：
	//   - /room/:id — 聊天房间
	//   - /match/:token — Random Match 邀请链接（React Router 处理）
	//   - 其他客户端路由
	mux.Handle("/", static.Handler())

	// ─── Step 5: 端口解析（flag > env > default）────────────────────────────
	//
	// 📚 学习要点: 三层配置优先级的实现模式
	// 配置优先级 flag > env > default 是 CLI 工具的常见模式：
	//   1. 命令行参数最优先（用户显式指定，意图最明确）
	//   2. 环境变量次之（容器/CI 环境的标准配置方式）
	//   3. 默认值兜底（零配置即可运行）
	//
	// portFlag 默认值为 0（表示"未设置"），这是 sentinel value 模式：
	// 用一个不可能是有效端口的值来区分"用户设置了 0"和"用户没设置"。
	// 对于端口号，0 不是有效的监听端口，所以可以安全地用作 sentinel。
	port := resolvePort(*portFlag, os.Getenv("PORT"), "8080")

	// ─── Step 6: 创建 http.Server ───────────────────────────────────────────
	//
	// 📚 学习要点: http.Server 结构体 vs http.ListenAndServe 便捷函数
	//
	// http.ListenAndServe(addr, handler) 内部创建一个默认配置的 Server：
	//   srv := &http.Server{Addr: addr, Handler: handler}
	//   return srv.ListenAndServe()
	//
	// 直接使用 http.Server 结构体可以：
	// - 设置安全超时参数（ReadHeaderTimeout 防止 slowloris 攻击）
	// - 调用 Shutdown() 实现优雅关闭（Task 8.2）
	// - 在测试中使用 httptest.NewServer 替换
	// - 设置 TLS 配置（如果需要直接终止 TLS）
	//
	// 📚 学习要点: ReadHeaderTimeout 与 Slowloris 攻击
	// Slowloris 是一种 DoS 攻击：攻击者发送 HTTP 请求但极慢地发送头部，
	// 每隔几秒发送一个字节，保持连接打开但不完成请求。
	// 如果没有超时限制，服务器的连接池会被这些「僵尸连接」耗尽。
	//
	// ReadHeaderTimeout 设置读取请求头的最大时间：
	// - 10 秒对正常客户端绰绰有余（即使在高延迟网络下）
	// - 但会切断 slowloris 攻击者的慢速连接
	// - 注意：这只保护 HTTP 层，WebSocket 连接升级后不受此限制
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	// ─── Step 7: 在 goroutine 中启动服务器 ──────────────────────────────────
	//
	// 📚 学习要点: 为什么在 goroutine 中启动 ListenAndServe？
	// srv.ListenAndServe() 是阻塞调用：
	// - 它会一直运行，处理传入的 HTTP 请求
	// - 只有在出错或 Shutdown() 被调用时才会返回
	//
	// 如果在 main goroutine 中直接调用，后续代码（信号监听、日志输出）永远不会执行。
	// 放在 goroutine 中让 main 函数可以继续执行后续逻辑。
	//
	// 📚 学习要点: http.ErrServerClosed 的语义
	// 当 srv.Shutdown() 被调用时，ListenAndServe 返回 http.ErrServerClosed。
	// 这是正常的关闭流程，不应视为错误。
	// 只有其他错误（如端口被占用、权限不足）才需要报告并退出。
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("Server", "listen failed: %v", err)
			os.Exit(1)
		}
	}()

	// ─── Step 8: 输出启动日志 ───────────────────────────────────────────────
	//
	// 📚 学习要点: 启动日志的作用
	// 启动日志是运维的第一道防线：
	// - 确认服务已成功启动（而非卡在初始化阶段）
	// - 记录关键配置（端口、版本），便于排查「部署了错误版本」的问题
	// - 时间戳帮助计算启动耗时和确认部署时间
	//
	// 在容器平台中，启动日志通常是判断容器是否健康的第一个信号。
	// 如果容器启动后没有输出此日志，说明初始化过程中出了问题。
	logger.Info("Server", "started on :%s (version %s) at %s", port, Version, time.Now().Format(time.RFC3339))

	// ─── Step 9: 等待关闭信号 ──────────────────────────────────────────────
	//
	// 📚 学习要点: os/signal 包的信号处理
	// signal.Notify 将操作系统信号转发到 Go channel。
	// 缓冲区大小为 1：即使 main goroutine 暂时没有读取，信号也不会丢失。
	// 如果缓冲区为 0（无缓冲 channel），信号到达时如果没有 goroutine 在等待接收，
	// 信号会被丢弃。缓冲区为 1 确保至少能存储一个待处理的信号。
	//
	// SIGTERM: 容器编排器（Docker/K8s）发送的优雅停止信号
	//   - Docker: `docker stop` 先发 SIGTERM，等待 grace period 后发 SIGKILL
	//   - K8s: Pod 终止时先发 SIGTERM，默认 30 秒后发 SIGKILL
	// SIGINT: 用户按 Ctrl+C 时发送（开发环境常用）
	//
	// 📚 学习要点: 为什么不监听 SIGKILL？
	// SIGKILL (kill -9) 无法被捕获或忽略，操作系统直接终止进程。
	// 这是设计上的安全机制：确保任何进程都可以被强制终止。
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	logger.Info("Server", "shutting down...")

	// ─── Step 10: 两阶段优雅关闭 ──────────────────────────────────────────
	//
	// 📚 学习要点: context.WithTimeout 创建带截止时间的 context
	// context 是 Go 中传递「取消信号」和「截止时间」的标准机制。
	// WithTimeout 返回一个新的 context，它会在指定时间后自动取消。
	// 5 秒后 context 的 Done() channel 会被关闭，所有监听它的操作都会收到通知。
	//
	// 📚 学习要点: defer cancel() 的重要性
	// 即使 context 超时后会自动取消，defer cancel() 仍然是必须的。
	// 原因：WithTimeout 内部启动了一个 timer goroutine，
	// 如果不调用 cancel()，这个 goroutine 会一直存活直到超时，造成资源泄漏。
	// defer cancel() 确保函数返回时立即释放 timer 资源。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Phase 1: 停止接受新连接，等待活跃 HTTP 请求完成
	//
	// 📚 学习要点: Server.Shutdown 的行为
	// Shutdown 执行以下操作（按顺序）：
	// 1. 关闭所有 listener（不再接受新的 TCP 连接）
	// 2. 关闭所有空闲连接（keep-alive 但无活跃请求的连接）
	// 3. 等待所有活跃的 HTTP 请求完成（或 context 超时）
	// 4. 返回 nil（成功）或 context.DeadlineExceeded（超时）
	//
	// 重要限制：Shutdown 不会等待 WebSocket 连接！
	// 原因：WebSocket 连接通过 HTTP Hijack 脱离了 http.Server 的管理。
	// Hijack 后，底层 TCP 连接的所有权转移给了应用代码（我们的 Client）。
	// http.Server 不再跟踪这些连接，Shutdown() 对它们完全无感知。
	// 这就是为什么我们需要 Phase 2 来主动关闭 WebSocket 连接。
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("Server", "HTTP shutdown error: %v", err)
	}

	// Phase 2: Stop MatchServer goroutine (if enabled)
	//
	// MatchServer.Stop() signals its internal ticker goroutine to exit.
	// This must happen before Hub.Stop() because Hub.Stop() closes client connections,
	// and MatchServer may still be processing queue entries referencing those clients.
	if matchServer != nil {
		matchServer.Stop()
	}

	// Phase 3: 关闭所有 WebSocket 连接
	//
	// WebSocket 连接通过 HTTP Hijack 脱离了 http.Server 的管理。
	// Shutdown() 对它们无感知，必须由 Hub 主动关闭。
	//
	// Hub.Stop() 的内部行为：
	// 1. close(done) — 通知 Hub.Run() 退出事件循环
	// 2. 遍历所有 client，client.closeSend()（关闭 client.done）— 通知 writePump 退出
	//    （注意：不关闭 client.send 通道，避免「向已关闭 channel 发送」的 panic）
	// 3. writePump 退出时发送 WebSocket Close 帧，然后关闭底层连接
	// 4. 底层连接关闭后，readPump 的 ReadMessage() 返回错误，readPump 退出
	//
	// 这个级联关闭确保每个客户端都能收到 Close 帧（如果网络允许），
	// 实现了「优雅断开」而非「粗暴切断」。
	wsHub.Stop()

	// Phase 4: 等待所有客户端 goroutine 退出（带超时保护）
	//
	// 📚 学习要点: WaitGroup + select 实现「等待或超时」模式
	// 这是 Go 中处理「最多等 N 秒」的标准模式：
	// - 在 goroutine 中调用阻塞操作（hub.Wait()）
	// - 阻塞操作完成时关闭一个 channel（done）
	// - 在 select 中同时等待 done 和 context 超时
	//
	// 为什么不直接 hub.Wait()？
	// 如果某个客户端的 goroutine 因为网络问题卡住（如 TCP FIN 未收到 ACK），
	// Wait() 会永久阻塞，导致进程无法退出。
	// 加上超时保护确保进程在 5 秒内一定退出（满足容器编排器的要求）。
	done := make(chan struct{})
	go func() {
		wsHub.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("Server", "all connections closed gracefully")
	case <-shutdownCtx.Done():
		logger.Warn("Server", "shutdown timeout, forcing exit")
	}

	os.Exit(0)
}

// handlePing 是健康检查端点的处理函数。
//
// 用途：
//   - 容器编排器（Docker HEALTHCHECK、K8s liveness/readiness probe）
//   - 外部保活服务（cron-job.org 每 10 分钟 GET /ping）
//   - 运维手动验证服务存活（curl https://backend/ping）
//
// 设计要求：
//   - 无状态：不依赖任何外部资源（数据库、缓存等）
//   - 无认证：任何人都可以访问（健康检查不应有认证门槛）
//   - 极低延迟：< 1ms 响应时间（仅返回固定字符串）
//   - 幂等：多次调用结果相同，无副作用
//
// 📚 学习要点: HTTP Handler 的签名
// Go 的 HTTP handler 有两种形式：
// 1. http.Handler 接口：实现 ServeHTTP(w, r) 方法的结构体
// 2. http.HandlerFunc 类型：func(w, r) 签名的普通函数
//
// HandleFunc 接受第二种形式，内部将其适配为 Handler 接口。
// 对于简单的端点（如 /ping），直接使用函数更简洁。
// 对于需要依赖注入的复杂端点，使用结构体实现 Handler 接口更合适。
//
// 📚 学习要点: ResponseWriter 的写入顺序
// HTTP 响应的写入必须按以下顺序：
// 1. 设置响应头（Header().Set(...)）
// 2. 写入状态码（WriteHeader(...)）
// 3. 写入响应体（Write(...)）
//
// 如果先调用 Write() 再设置 Header，头部修改会被忽略，
// 因为 Write() 会隐式调用 WriteHeader(200) 并发送头部。
func handlePing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}
