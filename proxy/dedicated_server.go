package proxy

import (
	"context"
	"fmt"
	"net"
	"redis-proxy-demo/pool"
	"sync"
	"time"

	"redis-proxy-demo/config"
	"redis-proxy-demo/lib/logger"
)

// DedicatedServer 专用服务器
type DedicatedServer struct {
	handler  *DedicatedHandler
	listener net.Listener
	config   DedicatedServerConfig
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	stats    *DedicatedServerStats
}

// DedicatedServerConfig 专用服务器配置
type DedicatedServerConfig struct {
	ListenAddr        string         // 监听地址
	RedisAddr         string         // Redis地址
	RedisPassword     string         // Redis密码
	MaxConnections    int            // 最大连接数
	InitConnections   int            // 初始连接数
	WaitTimeout       time.Duration  // 获取连接等待超时
	IdleTimeout       time.Duration  // 连接空闲超时
	SessionTimeout    time.Duration  // 会话超时
	CommandTimeout    time.Duration  // 命令超时
	DefaultDatabase   int            // 默认数据库
	DefaultClientName string         // 默认客户端名
	CacheConfig       *config.Config // 缓存配置
}

// DedicatedServerStats 专用服务器统计
type DedicatedServerStats struct {
	ConnectionsAccepted int64     `json:"connections_accepted"`
	ConnectionsRejected int64     `json:"connections_rejected"`
	ActiveConnections   int64     `json:"active_connections"`
	TotalConnections    int64     `json:"total_connections"`
	StartTime           time.Time `json:"start_time"`
	mu                  sync.RWMutex
}

// NewDedicatedServer 创建专用服务器
func NewDedicatedServer(config DedicatedServerConfig) (*DedicatedServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建处理器配置
	handlerConfig := DedicatedHandlerConfig{
		RedisAddr:         config.RedisAddr,
		RedisPassword:     config.RedisPassword,
		MaxConnections:    config.MaxConnections,
		InitConnections:   config.InitConnections,
		WaitTimeout:       config.WaitTimeout,
		IdleTimeout:       config.IdleTimeout,
		SessionTimeout:    config.SessionTimeout,
		CommandTimeout:    config.CommandTimeout,
		DefaultDatabase:   config.DefaultDatabase,
		DefaultClientName: config.DefaultClientName,
		CacheConfig:       config.CacheConfig,
	}

	// 创建处理器
	handler, err := NewDedicatedHandler(handlerConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建处理器失败: %w", err)
	}

	server := &DedicatedServer{
		handler: handler,
		config:  config,
		ctx:     ctx,
		cancel:  cancel,
		stats: &DedicatedServerStats{
			StartTime: time.Now(),
		},
	}

	return server, nil
}

// Start 启动服务器
func (s *DedicatedServer) Start() error {
	// 创建监听器
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("监听失败: %w", err)
	}
	s.listener = listener

	logger.Info(fmt.Sprintf("🚀 专用Redis代理服务器启动: %s -> %s",
		s.config.ListenAddr, s.config.RedisAddr))
	logger.Info(fmt.Sprintf("📋 配置: 最大连接=%d, 初始连接=%d, 等待超时=%v, 空闲超时=%v",
		s.config.MaxConnections, s.config.InitConnections,
		s.config.WaitTimeout, s.config.IdleTimeout))

	// 启动统计报告协程
	s.wg.Add(1)
	go s.statsReporter()

	// 启动接受连接的协程
	s.wg.Add(1)
	go s.acceptLoop()

	return nil
}

// acceptLoop 接受连接循环
func (s *DedicatedServer) acceptLoop() {
	defer s.wg.Done()

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// 设置接受超时
		if tcpListener, ok := s.listener.(*net.TCPListener); ok {
			tcpListener.SetDeadline(time.Now().Add(1 * time.Second))
		}

		conn, err := s.listener.Accept()
		if err != nil {
			// 检查是否是超时错误
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			// 检查是否是服务器关闭
			if s.ctx.Err() != nil {
				return
			}
			logger.Error(fmt.Sprintf("接受连接失败: %v", err))

			s.stats.mu.Lock()
			s.stats.ConnectionsRejected++
			s.stats.mu.Unlock()
			continue
		}

		// 更新统计
		s.stats.mu.Lock()
		s.stats.ConnectionsAccepted++
		s.stats.ActiveConnections++
		s.stats.TotalConnections++
		s.stats.mu.Unlock()

		// 启动处理协程
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

// handleConnection 处理单个连接
func (s *DedicatedServer) handleConnection(conn net.Conn) {
	defer func() {
		s.wg.Done()
		s.stats.mu.Lock()
		s.stats.ActiveConnections--
		s.stats.mu.Unlock()
	}()

	// 设置TCP选项
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// 使用处理器处理连接
	s.handler.Handle(s.ctx, conn)
}

// statsReporter 统计报告器
func (s *DedicatedServer) statsReporter() {
	defer s.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.reportStats()
		}
	}
}

// reportStats 报告统计信息
func (s *DedicatedServer) reportStats() {
	serverStats := s.GetStats()
	handlerStats := s.handler.GetStats()
	poolStats := s.handler.pool.GetStats()

	uptime := time.Since(serverStats.StartTime)

	logger.Info(fmt.Sprintf("📊 专用服务器统计 - 运行时间: %v", uptime))
	logger.Info(fmt.Sprintf("🔌 连接统计 - 接受: %d, 拒绝: %d, 活跃: %d, 总计: %d",
		serverStats.ConnectionsAccepted, serverStats.ConnectionsRejected,
		serverStats.ActiveConnections, serverStats.TotalConnections))
	logger.Info(fmt.Sprintf("📝 处理器统计 - 会话: %d/%d, 命令: %d, 错误: %d",
		handlerStats.ActiveSessions, handlerStats.TotalSessions,
		handlerStats.CommandsProcessed, handlerStats.ErrorsEncountered))
	logger.Info(fmt.Sprintf("🏊 连接池统计 - 总连接: %d, 活跃: %d, 空闲: %d, 请求: %d, 成功: %d, 超时: %d",
		poolStats.TotalConnections, poolStats.ActiveConnections, poolStats.IdleConnections,
		poolStats.GetRequests, poolStats.GetSuccesses, poolStats.GetTimeouts))
}

// Stop 停止服务器
func (s *DedicatedServer) Stop() error {
	logger.Info("停止专用Redis代理服务器...")

	// 取消上下文
	s.cancel()

	// 关闭监听器
	if s.listener != nil {
		s.listener.Close()
	}

	// 关闭处理器
	if s.handler != nil {
		s.handler.Close()
	}

	// 等待所有协程结束
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	// 等待最多10秒
	select {
	case <-done:
		logger.Info("✅ 专用Redis代理服务器已停止")
	case <-time.After(10 * time.Second):
		logger.Warn("⚠️ 专用Redis代理服务器停止超时")
	}

	return nil
}

// GetStats 获取服务器统计信息
func (s *DedicatedServer) GetStats() DedicatedServerStats {
	s.stats.mu.RLock()
	defer s.stats.mu.RUnlock()

	return DedicatedServerStats{
		ConnectionsAccepted: s.stats.ConnectionsAccepted,
		ConnectionsRejected: s.stats.ConnectionsRejected,
		ActiveConnections:   s.stats.ActiveConnections,
		TotalConnections:    s.stats.TotalConnections,
		StartTime:           s.stats.StartTime,
	}
}

// GetHandlerStats 获取处理器统计信息
func (s *DedicatedServer) GetHandlerStats() DedicatedHandlerStats {
	return s.handler.GetStats()
}

// GetPoolStats 获取连接池统计信息
func (s *DedicatedServer) GetPoolStats() pool.DedicatedPoolStats {
	return s.handler.pool.GetStats()
}

// GetDetailedStats 获取详细统计信息
func (s *DedicatedServer) GetDetailedStats() map[string]interface{} {
	serverStats := s.GetStats()
	handlerStats := s.GetHandlerStats()
	poolStats := s.GetPoolStats()

	return map[string]interface{}{
		"server": map[string]interface{}{
			"uptime":               time.Since(serverStats.StartTime).String(),
			"connections_accepted": serverStats.ConnectionsAccepted,
			"connections_rejected": serverStats.ConnectionsRejected,
			"active_connections":   serverStats.ActiveConnections,
			"total_connections":    serverStats.TotalConnections,
			"start_time":           serverStats.StartTime,
		},
		"handler": map[string]interface{}{
			"active_sessions":    handlerStats.ActiveSessions,
			"total_sessions":     handlerStats.TotalSessions,
			"sessions_created":   handlerStats.SessionsCreated,
			"sessions_closed":    handlerStats.SessionsClosed,
			"commands_processed": handlerStats.CommandsProcessed,
			"errors_encountered": handlerStats.ErrorsEncountered,
			"last_activity":      handlerStats.LastActivity,
		},
		"pool": map[string]interface{}{
			"total_connections":   poolStats.TotalConnections,
			"active_connections":  poolStats.ActiveConnections,
			"idle_connections":    poolStats.IdleConnections,
			"connections_created": poolStats.ConnectionsCreated,
			"connections_closed":  poolStats.ConnectionsClosed,
			"get_requests":        poolStats.GetRequests,
			"get_successes":       poolStats.GetSuccesses,
			"get_timeouts":        poolStats.GetTimeouts,
		},
		"config": map[string]interface{}{
			"listen_addr":         s.config.ListenAddr,
			"redis_addr":          s.config.RedisAddr,
			"max_connections":     s.config.MaxConnections,
			"init_connections":    s.config.InitConnections,
			"wait_timeout":        s.config.WaitTimeout.String(),
			"idle_timeout":        s.config.IdleTimeout.String(),
			"session_timeout":     s.config.SessionTimeout.String(),
			"command_timeout":     s.config.CommandTimeout.String(),
			"default_database":    s.config.DefaultDatabase,
			"default_client_name": s.config.DefaultClientName,
		},
	}
}

// IsRunning 检查服务器是否正在运行
func (s *DedicatedServer) IsRunning() bool {
	select {
	case <-s.ctx.Done():
		return false
	default:
		return s.listener != nil
	}
}

// GetListenAddr 获取监听地址
func (s *DedicatedServer) GetListenAddr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.ListenAddr
}

// GetRedisAddr 获取Redis地址
func (s *DedicatedServer) GetRedisAddr() string {
	return s.config.RedisAddr
}
