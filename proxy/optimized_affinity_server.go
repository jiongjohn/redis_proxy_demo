package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"redis-proxy-demo/lib/logger"
)

// OptimizedAffinityServer 优化的连接亲和性代理服务器
type OptimizedAffinityServer struct {
	config   Config
	handler  *OptimizedAffinityHandler
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewOptimizedAffinityServer 创建优化的连接亲和性服务器
func NewOptimizedAffinityServer(config Config) (*OptimizedAffinityServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建优化的亲和性配置
	affinityConfig := OptimizedAffinityConfig{
		RedisAddr:           fmt.Sprintf("%s:%d", "localhost", 6379),
		RedisPassword:       "",
		MaxConnections:      int(config.PoolMaxActive),
		PrePoolSize:         100, // 默认预连接池大小
		IdleTimeout:         config.MaxIdleTime,
		HealthCheckInterval: 30 * time.Second,
		BufferSize:          32 * 1024,
		ConnectTimeout:      5 * time.Second,
		PrewarmConnections:  20, // 默认预热连接数
	}

	// 使用配置的值
	if config.PoolMaxActive == 0 {
		affinityConfig.MaxConnections = 1000
	}
	if config.MaxIdleTime == 0 {
		affinityConfig.IdleTimeout = 5 * time.Minute
	}

	// 创建优化的亲和性处理器
	handler, err := NewOptimizedAffinityHandler(affinityConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建优化的亲和性处理器失败: %w", err)
	}

	return &OptimizedAffinityServer{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// NewOptimizedAffinityServerWithFullConfig 使用完整配置创建优化的亲和性服务器
func NewOptimizedAffinityServerWithFullConfig(config Config, redisAddr, redisPassword string,
	maxConnections int, prePoolSize int, prewarmConnections int, idleTimeout, healthCheckInterval time.Duration) (*OptimizedAffinityServer, error) {

	ctx, cancel := context.WithCancel(context.Background())

	affinityConfig := OptimizedAffinityConfig{
		RedisAddr:           redisAddr,
		RedisPassword:       redisPassword,
		MaxConnections:      maxConnections,
		PrePoolSize:         prePoolSize,
		IdleTimeout:         idleTimeout,
		HealthCheckInterval: healthCheckInterval,
		BufferSize:          64 * 1024, // 64KB缓冲区
		ConnectTimeout:      3 * time.Second,
		PrewarmConnections:  prewarmConnections,
	}

	handler, err := NewOptimizedAffinityHandler(affinityConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建优化的亲和性处理器失败: %w", err)
	}

	return &OptimizedAffinityServer{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// ListenAndServe 启动优化的亲和性代理服务器
func (server *OptimizedAffinityServer) ListenAndServe() error {
	// 启动TCP服务器
	listener, err := net.Listen("tcp", server.config.Address)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", server.config.Address, err)
	}

	server.listener = listener
	logger.Info(fmt.Sprintf("🚀 优化的Redis Affinity代理服务器启动于 %s", server.config.Address))
	logger.Info(fmt.Sprintf("📡 使用预连接池连接到Redis: %s", server.config.RedisAddr))
	logger.Info(fmt.Sprintf("🏊 预连接池大小: %d", server.handler.config.PrePoolSize))
	logger.Info(fmt.Sprintf("🔥 预热连接数: %d", server.handler.config.PrewarmConnections))
	logger.Info("✅ WATCH/MULTI/EXEC命令完全支持")
	logger.Info("⚡ 零连接建立延迟 - 预连接池优化")

	// 设置优雅关闭
	server.setupGracefulShutdown()

	// 启动统计报告
	go server.statsReporter()

	// 接受连接
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-server.ctx.Done():
				logger.Info("优化的affinity代理服务器停止接受连接")
				return nil
			default:
				logger.Error(fmt.Sprintf("接受连接失败: %v", err))
				continue
			}
		}

		// 使用优化的亲和性处理器处理连接
		server.wg.Add(1)
		go func() {
			defer server.wg.Done()
			server.handler.Handle(server.ctx, conn)
		}()
	}
}

// setupGracefulShutdown 设置优雅关闭处理
func (server *OptimizedAffinityServer) setupGracefulShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("收到关闭信号，开始优雅关闭...")

		// 停止接受新连接
		if server.listener != nil {
			server.listener.Close()
		}

		// 取消上下文
		server.cancel()

		// 等待所有连接处理完成
		done := make(chan struct{})
		go func() {
			server.wg.Wait()
			close(done)
		}()

		// 等待最多30秒
		select {
		case <-done:
			logger.Info("所有连接已优雅关闭")
		case <-time.After(30 * time.Second):
			logger.Warn("优雅关闭超时，强制退出")
		}

		// 关闭处理器
		if server.handler != nil {
			server.handler.Close()
		}

		os.Exit(0)
	}()
}

// statsReporter 统计报告器
func (server *OptimizedAffinityServer) statsReporter() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-server.ctx.Done():
			return
		case <-ticker.C:
			server.reportStats()
		}
	}
}

// reportStats 报告统计信息
func (server *OptimizedAffinityServer) reportStats() {
	if server.handler == nil {
		return
	}

	stats := server.handler.GetStats()
	logger.Info(fmt.Sprintf("📊 优化Affinity服务器统计:"))
	logger.Info(fmt.Sprintf("├── 活跃连接: %d", stats.ActiveConnections))
	logger.Info(fmt.Sprintf("├── 总连接数: %d", stats.TotalConnections))
	logger.Info(fmt.Sprintf("├── 已创建: %d", stats.ConnectionsCreated))
	logger.Info(fmt.Sprintf("├── 已关闭: %d", stats.ConnectionsClosed))
	logger.Info(fmt.Sprintf("├── 传输字节: %d", stats.BytesTransferred))
	logger.Info(fmt.Sprintf("├── 池命中: %d", stats.PoolHits))
	logger.Info(fmt.Sprintf("├── 池未命中: %d", stats.PoolMisses))
	logger.Info(fmt.Sprintf("└── 预连接数: %d", stats.PreConnections))

	// 计算命中率
	totalRequests := stats.PoolHits + stats.PoolMisses
	if totalRequests > 0 {
		hitRate := float64(stats.PoolHits) / float64(totalRequests) * 100
		logger.Info(fmt.Sprintf("🎯 预连接池命中率: %.2f%%", hitRate))
	}
}

// Close 关闭服务器
func (server *OptimizedAffinityServer) Close() error {
	logger.Info("关闭优化的Affinity服务器...")

	// 关闭监听器
	if server.listener != nil {
		server.listener.Close()
	}

	// 取消上下文
	server.cancel()

	// 等待所有goroutine完成
	server.wg.Wait()

	// 关闭处理器
	if server.handler != nil {
		return server.handler.Close()
	}

	logger.Info("✅ 优化的Affinity服务器已关闭")
	return nil
}
