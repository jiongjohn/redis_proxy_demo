package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"redis-proxy-demo/lib/logger"
)

// PoolServer 基于连接池的Redis代理服务器
type PoolServer struct {
	config   Config
	handler  *PoolHandler
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewPoolServerWithDetailedConfig 使用详细配置创建池服务器
func NewPoolServerWithDetailedConfig(config Config,
	maxPoolSize, minIdleConns, maxIdleConns int,
	idleTimeout, maxLifetime, cleanupInterval, sessionTimeout, commandTimeout, connectionHoldTime time.Duration) (*PoolServer, error) {

	ctx, cancel := context.WithCancel(context.Background())

	handlerConfig := PoolHandlerConfig{
		RedisAddr:          config.RedisAddr,
		RedisPassword:      config.RedisPassword,
		MaxPoolSize:        maxPoolSize,
		MinIdleConns:       minIdleConns,
		MaxIdleConns:       maxIdleConns,
		IdleTimeout:        idleTimeout,
		MaxLifetime:        maxLifetime,
		CleanupInterval:    cleanupInterval,
		SessionTimeout:     sessionTimeout,
		CommandTimeout:     commandTimeout,
		ConnectionHoldTime: connectionHoldTime,
		DefaultDatabase:    0,
		DefaultClientName:  "redis-proxy-pool-client",
	}

	handler, err := NewPoolHandler(handlerConfig)
	if err != nil {
		fmt.Printf("创建池处理器失败: %w\n", err)
		cancel()
		return nil, fmt.Errorf("创建池处理器失败: %w", err)
	}
	fmt.Printf("创建池处理器成功\n")

	return &PoolServer{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// ListenAndServe 启动池代理服务器
func (s *PoolServer) ListenAndServe() error {
	// 创建TCP监听器
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return fmt.Errorf("监听地址 %s 失败: %w", s.config.Address, err)
	}

	s.listener = listener

	// 记录启动信息
	logger.Info(fmt.Sprintf("🚀 Redis连接池代理服务器启动"))
	logger.Info(fmt.Sprintf("📡 监听地址: %s", s.config.Address))
	logger.Info(fmt.Sprintf("🎯 目标Redis: %s", s.config.RedisAddr))
	logger.Info(fmt.Sprintf("🏊 连接池配置:"))
	logger.Info(fmt.Sprintf("  ├── 最大连接数: %d", s.handler.config.MaxPoolSize))
	logger.Info(fmt.Sprintf("  ├── 最大空闲连接: %d", s.handler.config.MaxIdleConns))
	logger.Info(fmt.Sprintf("  ├── 最小空闲连接: %d", s.handler.config.MinIdleConns))
	logger.Info(fmt.Sprintf("  ├── 空闲超时: %v", s.handler.config.IdleTimeout))
	logger.Info(fmt.Sprintf("  ├── 连接最大生命周期: %v", s.handler.config.MaxLifetime))
	logger.Info(fmt.Sprintf("  └── 清理间隔: %v", s.handler.config.CleanupInterval))

	logger.Info("✅ 特性支持:")
	logger.Info("  ✅ 智能命令分类 (普通/初始化/会话)")
	logger.Info("  ✅ 连接池复用优化")
	logger.Info("  ✅ 粘性会话支持 (事务/订阅/阻塞命令)")
	logger.Info("  ✅ 多上下文连接隔离 (跨DB/用户)")
	logger.Info("  ✅ 自动连接清理和健康检查")
	logger.Info("  ✅ 完整的统计和监控")

	// 设置优雅关闭
	s.setupGracefulShutdown()

	// 启动统计报告器
	go s.statsReporter()

	// 接受连接循环
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				logger.Info("服务器停止接受新连接")
				return nil
			default:
				logger.Error(fmt.Sprintf("接受连接失败: %v", err))
				continue
			}
		}

		// 使用池处理器处理连接
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handler.Handle(s.ctx, conn)
		}()
	}
}

// setupGracefulShutdown 设置优雅关闭
func (s *PoolServer) setupGracefulShutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

	go func() {
		sig := <-c
		logger.Info(fmt.Sprintf("收到信号 %v, 开始优雅关闭池服务器...", sig))
		_ = s.Shutdown()
	}()
}

// statsReporter 统计报告器
func (s *PoolServer) statsReporter() {
	ticker := time.NewTicker(60 * time.Second) // 每分钟报告一次
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.reportServerStats()
		}
	}
}

// reportServerStats 报告服务器统计信息
func (s *PoolServer) reportServerStats() {
	handlerStats := s.handler.GetStats()         // 已经返回副本
	poolStats := s.handler.GetPoolManagerStats() // 已经返回副本

	logger.Info("🌟 池服务器统计报告")
	logger.Info("=" + strings.Repeat("=", 50))

	// 会话统计
	logger.Info(fmt.Sprintf("👥 会话统计:"))
	logger.Info(fmt.Sprintf("  ├── 活跃会话: %d", handlerStats.ActiveSessions))
	logger.Info(fmt.Sprintf("  ├── 总会话数: %d", handlerStats.TotalSessions))
	logger.Info(fmt.Sprintf("  ├── 创建会话: %d", handlerStats.SessionsCreated))
	logger.Info(fmt.Sprintf("  └── 关闭会话: %d", handlerStats.SessionsClosed))

	// 命令统计
	logger.Info(fmt.Sprintf("⚡ 命令统计:"))
	logger.Info(fmt.Sprintf("  ├── 总命令数: %d", handlerStats.CommandsProcessed))
	logger.Info(fmt.Sprintf("  ├── 普通命令: %d", handlerStats.NormalCommands))
	logger.Info(fmt.Sprintf("  ├── 初始化命令: %d", handlerStats.InitCommands))
	logger.Info(fmt.Sprintf("  ├── 会话命令: %d", handlerStats.SessionCommands))
	logger.Info(fmt.Sprintf("  └── 错误数: %d", handlerStats.ErrorsEncountered))

	// 连接池统计
	logger.Info(fmt.Sprintf("🏊 连接池统计:"))
	logger.Info(fmt.Sprintf("  ├── 池数量: %d", poolStats.PoolCount))
	logger.Info(fmt.Sprintf("  ├── 总池数: %d", poolStats.TotalPools))
	logger.Info(fmt.Sprintf("  ├── 创建池: %d", poolStats.PoolsCreated))
	logger.Info(fmt.Sprintf("  └── 关闭池: %d", poolStats.PoolsClosed))

	// 最后活动时间
	if !handlerStats.LastActivity.IsZero() {
		logger.Info(fmt.Sprintf("⏰ 最后活动: %v", handlerStats.LastActivity.Format(time.RFC3339)))
	}

	if !poolStats.LastCleanup.IsZero() {
		logger.Info(fmt.Sprintf("🧹 最后清理: %v", poolStats.LastCleanup.Format(time.RFC3339)))
	}

	logger.Info("=" + strings.Repeat("=", 50))
}

// Shutdown 优雅关闭服务器
func (s *PoolServer) Shutdown() error {
	logger.Info("开始关闭池服务器...")

	// 取消上下文，停止接受新连接
	s.cancel()

	// 关闭监听器
	if s.listener != nil {
		err := s.listener.Close()
		if err != nil {
			logger.Warn(fmt.Sprintf("关闭监听器失败: %v", err))
		}
	}

	// 关闭处理器
	if s.handler != nil {
		err := s.handler.Close()
		if err != nil {
			logger.Warn(fmt.Sprintf("关闭处理器失败: %v", err))
		}
	}

	// 等待所有连接处理完成（带超时）
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	// 等待优雅关闭或超时
	select {
	case <-done:
		logger.Info("✅ 池服务器优雅关闭完成")
	case <-time.After(30 * time.Second):
		logger.Warn("⚠️ 池服务器关闭超时，强制退出")
	}

	return nil
}

// GetAddress 获取服务器监听地址
func (s *PoolServer) GetAddress() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.Address
}

// GetHandlerStats 获取处理器统计信息
func (s *PoolServer) GetHandlerStats() HandlerStats {
	return s.handler.GetStats()
}

// GetPoolManagerStats 获取池管理器统计信息
func (s *PoolServer) GetPoolManagerStats() PoolManagerStats {
	return s.handler.GetPoolManagerStats()
}

// IsHealthy 检查服务器健康状态
func (s *PoolServer) IsHealthy() bool {
	if s.handler == nil {
		return false
	}

	stats := s.GetHandlerStats()

	// 简单的健康检查：活跃会话数合理，错误率不高
	errorRate := float64(stats.ErrorsEncountered) / float64(stats.CommandsProcessed)

	return stats.ActiveSessions >= 0 &&
		stats.CommandsProcessed >= 0 &&
		(stats.CommandsProcessed == 0 || errorRate < 0.1) // 错误率小于10%
}

// GetConnectionCount 获取当前连接数（会话数）
func (s *PoolServer) GetConnectionCount() int64 {
	return s.GetHandlerStats().ActiveSessions
}

// GetMaxConnections 获取最大连接数
func (s *PoolServer) GetMaxConnections() int {
	return s.handler.config.MaxPoolSize
}

// GetDetailedStatus 获取详细状态信息
func (s *PoolServer) GetDetailedStatus() map[string]interface{} {
	handlerStats := s.GetHandlerStats()  // 已经返回副本
	poolStats := s.GetPoolManagerStats() // 已经返回副本

	return map[string]interface{}{
		"server_type":   "Connection Pool Proxy",
		"address":       s.GetAddress(),
		"redis_target":  s.config.RedisAddr,
		"healthy":       s.IsHealthy(),
		"handler_stats": handlerStats,
		"pool_stats":    poolStats,
		"config": map[string]interface{}{
			"max_pool_size":       s.handler.config.MaxPoolSize,
			"max_idle_conns":      s.handler.config.MaxIdleConns,
			"min_idle_conns":      s.handler.config.MinIdleConns,
			"idle_timeout":        s.handler.config.IdleTimeout.String(),
			"max_lifetime":        s.handler.config.MaxLifetime.String(),
			"cleanup_interval":    s.handler.config.CleanupInterval.String(),
			"session_timeout":     s.handler.config.SessionTimeout.String(),
			"command_timeout":     s.handler.config.CommandTimeout.String(),
			"default_database":    s.handler.config.DefaultDatabase,
			"default_client_name": s.handler.config.DefaultClientName,
		},
		"features": []string{
			"智能命令分类",
			"连接池复用优化",
			"粘性会话支持",
			"多上下文连接隔离",
			"自动连接清理",
			"健康检查",
			"统计监控",
		},
	}
}

// EnableDebugMode 启用调试模式
func (s *PoolServer) EnableDebugMode() {
	// TODO: 实现调试模式，可以输出更详细的日志
	logger.Info("🐛 调试模式已启用")
}

// DisableDebugMode 禁用调试模式
func (s *PoolServer) DisableDebugMode() {
	logger.Info("🐛 调试模式已禁用")
}
