package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"redis-proxy-demo/lib/logger"
)

// OptimizedAffinityHandler 优化的连接亲和性处理器
// 使用预连接池减少连接建立开销
type OptimizedAffinityHandler struct {
	config      OptimizedAffinityConfig
	preConnPool *PreConnectionPool // 预连接池
	activeConns sync.Map           // net.Conn -> *OptimizedAffinityConnection
	stats       *OptimizedAffinityStats
	closing     chan struct{}
	wg          sync.WaitGroup
	connCount   int64 // 活跃连接数
}

// OptimizedAffinityConfig 优化的亲和性配置
type OptimizedAffinityConfig struct {
	RedisAddr           string        // Redis服务器地址
	RedisPassword       string        // Redis密码
	MaxConnections      int           // 最大并发连接数
	PrePoolSize         int           // 预连接池大小
	IdleTimeout         time.Duration // 空闲超时
	HealthCheckInterval time.Duration // 健康检查间隔
	BufferSize          int           // 缓冲区大小
	ConnectTimeout      time.Duration // 连接超时
	PrewarmConnections  int           // 预热连接数
}

// OptimizedAffinityConnection 优化的亲和性连接
type OptimizedAffinityConnection struct {
	clientConn net.Conn       // 客户端连接
	redisConn  *PreConnection // 预连接池中的Redis连接
	lastActive int64          // 最后活跃时间（Unix纳秒）
	closing    chan struct{}  // 关闭信号
	closed     int32          // 关闭标志（原子操作）
	bytesRx    int64          // 接收字节数
	bytesTx    int64          // 发送字节数
}

// PreConnection 预连接结构
type PreConnection struct {
	conn       net.Conn
	inUse      int32 // 使用标志（原子操作）
	created    int64 // 创建时间
	lastUsed   int64 // 最后使用时间
	usageCount int64 // 使用次数
	healthy    int32 // 健康状态（原子操作）
}

// PreConnectionPool 预连接池
type PreConnectionPool struct {
	connections chan *PreConnection
	config      OptimizedAffinityConfig
	closed      int32
	totalConns  int64
	activeConns int64
	// 统计信息
	poolHits   int64
	poolMisses int64
	created    int64
	destroyed  int64
}

// OptimizedAffinityStats 优化的统计信息
type OptimizedAffinityStats struct {
	ActiveConnections  int64 `json:"active_connections"`
	TotalConnections   int64 `json:"total_connections"`
	BytesTransferred   int64 `json:"bytes_transferred"`
	ConnectionsCreated int64 `json:"connections_created"`
	ConnectionsClosed  int64 `json:"connections_closed"`
	PoolHits           int64 `json:"pool_hits"`
	PoolMisses         int64 `json:"pool_misses"`
	PreConnections     int64 `json:"pre_connections"`
	LastActivity       int64 `json:"last_activity"`
}

// NewOptimizedAffinityHandler 创建优化的亲和性处理器
func NewOptimizedAffinityHandler(config OptimizedAffinityConfig) (*OptimizedAffinityHandler, error) {
	// 设置默认值
	if config.BufferSize == 0 {
		config.BufferSize = 32 * 1024
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = 5 * time.Minute
	}
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 30 * time.Second
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 1000
	}
	if config.PrePoolSize == 0 {
		config.PrePoolSize = 100 // 预连接池大小
	}
	if config.ConnectTimeout == 0 {
		config.ConnectTimeout = 5 * time.Second
	}
	if config.PrewarmConnections == 0 {
		config.PrewarmConnections = 20 // 预热连接数
	}

	// 创建预连接池
	preConnPool, err := NewPreConnectionPool(config)
	if err != nil {
		return nil, fmt.Errorf("创建预连接池失败: %w", err)
	}

	handler := &OptimizedAffinityHandler{
		config:      config,
		preConnPool: preConnPool,
		stats:       &OptimizedAffinityStats{},
		closing:     make(chan struct{}),
	}

	// 启动后台任务
	go handler.healthCheckLoop()
	go handler.idleConnectionCleanup()
	go handler.statsReporter()

	logger.Info(fmt.Sprintf("✅ 优化的Affinity处理器已启动 - 预连接池大小: %d, 预热连接: %d",
		config.PrePoolSize, config.PrewarmConnections))

	return handler, nil
}

// NewPreConnectionPool 创建预连接池
func NewPreConnectionPool(config OptimizedAffinityConfig) (*PreConnectionPool, error) {
	pool := &PreConnectionPool{
		connections: make(chan *PreConnection, config.PrePoolSize),
		config:      config,
	}

	// 预热连接池
	for i := 0; i < config.PrewarmConnections; i++ {
		conn, err := pool.createPreConnection()
		if err != nil {
			logger.Warn(fmt.Sprintf("预热连接 %d 创建失败: %v", i, err))
			continue
		}

		select {
		case pool.connections <- conn:
			atomic.AddInt64(&pool.totalConns, 1)
			atomic.AddInt64(&pool.created, 1)
		default:
			conn.Close()
		}
	}

	// 启动连接池维护协程
	go pool.maintainPool()

	logger.Info(fmt.Sprintf("🏊 预连接池已创建 - 容量: %d, 预热: %d",
		config.PrePoolSize, config.PrewarmConnections))

	return pool, nil
}

// createPreConnection 创建预连接
func (p *PreConnectionPool) createPreConnection() (*PreConnection, error) {
	conn, err := net.DialTimeout("tcp", p.config.RedisAddr, p.config.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	// TCP优化
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	preConn := &PreConnection{
		conn:     conn,
		created:  time.Now().UnixNano(),
		lastUsed: time.Now().UnixNano(),
		healthy:  1,
	}

	// 处理Redis认证
	if p.config.RedisPassword != "" {
		err = p.authenticateConnection(preConn)
		if err != nil {
			conn.Close()
			return nil, err
		}
	}

	return preConn, nil
}

// authenticateConnection 认证连接
func (p *PreConnectionPool) authenticateConnection(preConn *PreConnection) error {
	authCmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n",
		len(p.config.RedisPassword), p.config.RedisPassword)

	_, err := preConn.conn.Write([]byte(authCmd))
	if err != nil {
		return fmt.Errorf("failed to send AUTH command: %w", err)
	}

	// 读取AUTH响应
	buffer := make([]byte, 1024)
	preConn.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	n, err := preConn.conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("读取AUTH响应失败: %w", err)
	}

	response := string(buffer[:n])
	if !contains(response, "+OK") {
		return fmt.Errorf("redis authentication failed: %s", response)
	}

	// 清除读取超时
	preConn.conn.SetReadDeadline(time.Time{})
	return nil
}

// GetConnection 从预连接池获取连接
func (p *PreConnectionPool) GetConnection() (*PreConnection, error) {
	if atomic.LoadInt32(&p.closed) != 0 {
		return nil, fmt.Errorf("connection pool is closed")
	}

	select {
	case conn := <-p.connections:
		// 检查连接健康状态
		if atomic.LoadInt32(&conn.healthy) == 0 || p.isConnectionStale(conn) {
			conn.Close()
			atomic.AddInt64(&p.destroyed, 1)
			atomic.AddInt64(&p.totalConns, -1)
			// 尝试创建新连接
			newConn, err := p.createPreConnection()
			if err != nil {
				atomic.AddInt64(&p.poolMisses, 1)
				return nil, err
			}
			atomic.AddInt64(&p.created, 1)
			atomic.AddInt64(&p.totalConns, 1)
			atomic.StoreInt32(&newConn.inUse, 1)
			atomic.AddInt64(&newConn.usageCount, 1)
			atomic.StoreInt64(&newConn.lastUsed, time.Now().UnixNano())
			atomic.AddInt64(&p.poolHits, 1)
			return newConn, nil
		}

		atomic.StoreInt32(&conn.inUse, 1)
		atomic.AddInt64(&conn.usageCount, 1)
		atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
		atomic.AddInt64(&p.poolHits, 1)
		atomic.AddInt64(&p.activeConns, 1)
		return conn, nil

	default:
		// 池中没有可用连接，创建新连接
		if atomic.LoadInt64(&p.totalConns) < int64(p.config.PrePoolSize) {
			conn, err := p.createPreConnection()
			if err != nil {
				atomic.AddInt64(&p.poolMisses, 1)
				return nil, err
			}
			atomic.AddInt64(&p.created, 1)
			atomic.AddInt64(&p.totalConns, 1)
			atomic.StoreInt32(&conn.inUse, 1)
			atomic.AddInt64(&conn.usageCount, 1)
			atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
			atomic.AddInt64(&p.activeConns, 1)
			return conn, nil
		}

		// 等待连接归还
		select {
		case conn := <-p.connections:
			atomic.StoreInt32(&conn.inUse, 1)
			atomic.AddInt64(&conn.usageCount, 1)
			atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
			atomic.AddInt64(&p.poolHits, 1)
			atomic.AddInt64(&p.activeConns, 1)
			return conn, nil
		case <-time.After(100 * time.Millisecond):
			atomic.AddInt64(&p.poolMisses, 1)
			return nil, fmt.Errorf("连接池超时")
		}
	}
}

// ReturnConnection 归还连接到池
func (p *PreConnectionPool) ReturnConnection(conn *PreConnection) {
	if conn == nil {
		return
	}

	atomic.StoreInt32(&conn.inUse, 0)
	atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
	atomic.AddInt64(&p.activeConns, -1)

	select {
	case p.connections <- conn:
		// 成功归还
	default:
		// 池满了，关闭连接
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
		atomic.AddInt64(&p.totalConns, -1)
	}
}

// isConnectionStale 检查连接是否过期
func (p *PreConnectionPool) isConnectionStale(conn *PreConnection) bool {
	// 连接超过5分钟未使用认为过期
	return time.Now().UnixNano()-atomic.LoadInt64(&conn.lastUsed) > int64(5*time.Minute)
}

// Close 关闭预连接
func (pc *PreConnection) Close() {
	if pc.conn != nil {
		pc.conn.Close()
	}
	atomic.StoreInt32(&pc.healthy, 0)
}

// maintainPool 维护连接池
func (p *PreConnectionPool) maintainPool() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if atomic.LoadInt32(&p.closed) != 0 {
				return
			}
			p.cleanupStaleConnections()
			p.ensureMinConnections()
		}
	}
}

// cleanupStaleConnections 清理过期连接
func (p *PreConnectionPool) cleanupStaleConnections() {
	var staleConns []*PreConnection

	// 收集过期连接
cleanup_loop:
	for {
		select {
		case conn := <-p.connections:
			if p.isConnectionStale(conn) {
				staleConns = append(staleConns, conn)
			} else {
				// 连接还有效，放回池中
				select {
				case p.connections <- conn:
				default:
					conn.Close()
					atomic.AddInt64(&p.destroyed, 1)
					atomic.AddInt64(&p.totalConns, -1)
				}
			}
		default:
			break cleanup_loop
		}
	}
	// 关闭过期连接
	for _, conn := range staleConns {
		conn.Close()
		atomic.AddInt64(&p.destroyed, 1)
		atomic.AddInt64(&p.totalConns, -1)
	}

	if len(staleConns) > 0 {
		logger.Debug(fmt.Sprintf("清理了 %d 个过期预连接", len(staleConns)))
	}
}

// ensureMinConnections 确保最小连接数
func (p *PreConnectionPool) ensureMinConnections() {
	minConns := p.config.PrewarmConnections
	currentConns := len(p.connections)

	if currentConns < minConns {
		needed := minConns - currentConns
	create_loop:
		for i := 0; i < needed; i++ {
			conn, err := p.createPreConnection()
			if err != nil {
				logger.Warn(fmt.Sprintf("补充预连接失败: %v", err))
				continue
			}

			select {
			case p.connections <- conn:
				atomic.AddInt64(&p.totalConns, 1)
				atomic.AddInt64(&p.created, 1)
			default:
				conn.Close()
				break create_loop // 池满了，跳出循环
			}
		}

		if needed > 0 {
			logger.Debug(fmt.Sprintf("补充了 %d 个预连接", needed))
		}
	}
}

// Handle 处理客户端连接（优化版本）
func (h *OptimizedAffinityHandler) Handle(ctx context.Context, clientConn net.Conn) {
	// 检查连接限制
	if !h.checkConnectionLimit() {
		logger.Warn(fmt.Sprintf("连接数超限，拒绝客户端: %s", clientConn.RemoteAddr()))
		clientConn.Close()
		return
	}

	h.wg.Add(1)
	defer h.wg.Done()

	logger.Info(fmt.Sprintf("🔗 新客户端连接 (优化Affinity模式): %s", clientConn.RemoteAddr()))

	// 从预连接池获取Redis连接
	redisConn, err := h.preConnPool.GetConnection()
	if err != nil {
		logger.Error(fmt.Sprintf("从预连接池获取连接失败: %v", err))
		clientConn.Close()
		return
	}

	// 创建优化的亲和性连接
	affinityConn := &OptimizedAffinityConnection{
		clientConn: clientConn,
		redisConn:  redisConn,
		lastActive: time.Now().UnixNano(),
		closing:    make(chan struct{}),
	}

	// 存储连接
	h.activeConns.Store(clientConn, affinityConn)
	atomic.AddInt64(&h.connCount, 1)

	// 更新统计
	atomic.AddInt64(&h.stats.ActiveConnections, 1)
	atomic.AddInt64(&h.stats.TotalConnections, 1)
	atomic.AddInt64(&h.stats.ConnectionsCreated, 1)
	atomic.StoreInt64(&h.stats.LastActivity, time.Now().UnixNano())

	logger.Info(fmt.Sprintf("✅ 使用预连接为客户端 %s 建立连接 (使用次数: %d)",
		clientConn.RemoteAddr(), atomic.LoadInt64(&redisConn.usageCount)))

	// 启动双向数据转发
	go h.forwardDataOptimized(affinityConn, clientConn, redisConn.conn, "Client->Redis")
	go h.forwardDataOptimized(affinityConn, redisConn.conn, clientConn, "Redis->Client")

	// 等待连接关闭
	h.waitForConnectionClose(affinityConn)

	// 清理
	h.cleanupConnectionOptimized(affinityConn)
}

// forwardDataOptimized 优化的数据转发
func (h *OptimizedAffinityHandler) forwardDataOptimized(affinityConn *OptimizedAffinityConnection, src, dst net.Conn, direction string) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error(fmt.Sprintf("数据转发异常 [%s]: %v", direction, r))
		}
		affinityConn.signalClose()
	}()

	buffer := make([]byte, h.config.BufferSize)

	for {
		select {
		case <-affinityConn.closing:
			return
		default:
		}

		// 使用合理的读取超时时间
		readTimeout := 30 * time.Second
		src.SetReadDeadline(time.Now().Add(readTimeout))

		n, err := src.Read(buffer)
		if err != nil {
			// 检查是否是超时错误
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				logger.Debug(fmt.Sprintf("读取超时 [%s], 发送超时错误", direction))

				// 直接给目标端发送超时错误，不关闭连接
				h.sendTimeoutErrorToDst(dst, direction)
				continue // 继续循环，保持连接
			}

			// 其他错误才退出
			if err != io.EOF && !isConnectionClosed(err) {
				logger.Debug(fmt.Sprintf("读取错误 [%s]: %v", direction, err))
			}
			return
		}

		if n > 0 {
			// 更新活跃时间
			atomic.StoreInt64(&affinityConn.lastActive, time.Now().UnixNano())

			// 转发数据
			_, err = dst.Write(buffer[:n])
			if err != nil {
				logger.Debug(fmt.Sprintf("写入错误 [%s]: %v", direction, err))
				return
			}

			// 更新统计（使用原子操作）
			if direction == "Client->Redis" {
				atomic.AddInt64(&affinityConn.bytesTx, int64(n))
			} else {
				atomic.AddInt64(&affinityConn.bytesRx, int64(n))
			}
			atomic.AddInt64(&h.stats.BytesTransferred, int64(n))
			atomic.StoreInt64(&h.stats.LastActivity, time.Now().UnixNano())

			logger.Debug(fmt.Sprintf("📡 转发 %d 字节 [%s]", n, direction))
		}
	}
}

// sendTimeoutErrorToDst 直接向目标端发送超时错误
func (h *OptimizedAffinityHandler) sendTimeoutErrorToDst(dst net.Conn, direction string) {
	var errorMsg string

	if direction == "Redis->Client" {
		// Redis 超时，向客户端发送 Redis 格式的错误响应
		errorMsg = "-ERR proxy timeout: no response from Redis server\r\n"
	} else {
		// 客户端超时，向 Redis 发送 PING 保持连接活跃
		errorMsg = "*1\r\n$4\r\nPING\r\n"
	}

	_, err := dst.Write([]byte(errorMsg))
	if err != nil {
		logger.Debug(fmt.Sprintf("发送超时处理消息失败 [%s]: %v", direction, err))
	} else {
		if direction == "Redis->Client" {
			logger.Debug(fmt.Sprintf("已向客户端发送超时错误 [%s]", direction))
		} else {
			logger.Debug(fmt.Sprintf("已向Redis发送PING保持连接 [%s]", direction))
		}
	}
}

// signalClose 信号关闭
func (ac *OptimizedAffinityConnection) signalClose() {
	if atomic.CompareAndSwapInt32(&ac.closed, 0, 1) {
		close(ac.closing)
	}
}

// waitForConnectionClose 等待连接关闭
func (h *OptimizedAffinityHandler) waitForConnectionClose(affinityConn *OptimizedAffinityConnection) {
	<-affinityConn.closing
	logger.Debug(fmt.Sprintf("检测到连接关闭: %s", affinityConn.clientConn.RemoteAddr()))
}

// cleanupConnectionOptimized 优化的连接清理
func (h *OptimizedAffinityHandler) cleanupConnectionOptimized(affinityConn *OptimizedAffinityConnection) {
	if atomic.LoadInt32(&affinityConn.closed) == 0 {
		return
	}

	logger.Info(fmt.Sprintf("🧹 清理连接: %s (TX: %d, RX: %d)",
		affinityConn.clientConn.RemoteAddr(),
		atomic.LoadInt64(&affinityConn.bytesTx),
		atomic.LoadInt64(&affinityConn.bytesRx)))

	// 关闭客户端连接
	if affinityConn.clientConn != nil {
		affinityConn.clientConn.Close()
	}

	// 归还Redis连接到预连接池
	if affinityConn.redisConn != nil {
		h.preConnPool.ReturnConnection(affinityConn.redisConn)
	}

	// 从映射中删除
	h.activeConns.Delete(affinityConn.clientConn)
	atomic.AddInt64(&h.connCount, -1)

	// 更新统计
	atomic.AddInt64(&h.stats.ActiveConnections, -1)
	atomic.AddInt64(&h.stats.ConnectionsClosed, 1)

	logger.Debug("✅ 连接清理完成")
}

// checkConnectionLimit 检查连接限制
func (h *OptimizedAffinityHandler) checkConnectionLimit() bool {
	return atomic.LoadInt64(&h.connCount) < int64(h.config.MaxConnections)
}

// GetStats 获取统计信息
func (h *OptimizedAffinityHandler) GetStats() OptimizedAffinityStats {
	return OptimizedAffinityStats{
		ActiveConnections:  atomic.LoadInt64(&h.stats.ActiveConnections),
		TotalConnections:   atomic.LoadInt64(&h.stats.TotalConnections),
		BytesTransferred:   atomic.LoadInt64(&h.stats.BytesTransferred),
		ConnectionsCreated: atomic.LoadInt64(&h.stats.ConnectionsCreated),
		ConnectionsClosed:  atomic.LoadInt64(&h.stats.ConnectionsClosed),
		PoolHits:           atomic.LoadInt64(&h.preConnPool.poolHits),
		PoolMisses:         atomic.LoadInt64(&h.preConnPool.poolMisses),
		PreConnections:     atomic.LoadInt64(&h.preConnPool.totalConns),
		LastActivity:       atomic.LoadInt64(&h.stats.LastActivity),
	}
}

// 后台任务方法（简化版本，主要使用原子操作）

// healthCheckLoop 健康检查循环
func (h *OptimizedAffinityHandler) healthCheckLoop() {
	ticker := time.NewTicker(h.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.performHealthCheck()
		}
	}
}

// performHealthCheck 执行健康检查
func (h *OptimizedAffinityHandler) performHealthCheck() {
	var unhealthyConnections []net.Conn

	h.activeConns.Range(func(key, value interface{}) bool {
		clientConn := key.(net.Conn)
		affinityConn := value.(*OptimizedAffinityConnection)

		// 简单的健康检查：检查连接是否超时
		if time.Now().UnixNano()-atomic.LoadInt64(&affinityConn.lastActive) > int64(h.config.IdleTimeout*2) {
			unhealthyConnections = append(unhealthyConnections, clientConn)
		}
		return true
	})

	// 清理不健康的连接
	for _, clientConn := range unhealthyConnections {
		if value, ok := h.activeConns.Load(clientConn); ok {
			if affinityConn, ok := value.(*OptimizedAffinityConnection); ok {
				logger.Debug(fmt.Sprintf("清理不健康连接: %s", clientConn.RemoteAddr()))
				affinityConn.signalClose()
			}
		}
	}

	if len(unhealthyConnections) > 0 {
		logger.Info(fmt.Sprintf("健康检查完成，清理了 %d 个不健康连接", len(unhealthyConnections)))
	}
}

// idleConnectionCleanup 空闲连接清理
func (h *OptimizedAffinityHandler) idleConnectionCleanup() {
	ticker := time.NewTicker(h.config.IdleTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.cleanupIdleConnections()
		}
	}
}

// cleanupIdleConnections 清理空闲连接
func (h *OptimizedAffinityHandler) cleanupIdleConnections() {
	var idleConnections []net.Conn
	idleThreshold := time.Now().UnixNano() - int64(h.config.IdleTimeout)

	h.activeConns.Range(func(key, value interface{}) bool {
		clientConn := key.(net.Conn)
		affinityConn := value.(*OptimizedAffinityConnection)

		if atomic.LoadInt64(&affinityConn.lastActive) < idleThreshold {
			idleConnections = append(idleConnections, clientConn)
		}
		return true
	})

	// 清理空闲连接
	for _, clientConn := range idleConnections {
		if value, ok := h.activeConns.Load(clientConn); ok {
			if affinityConn, ok := value.(*OptimizedAffinityConnection); ok {
				idleDuration := time.Duration(time.Now().UnixNano() - atomic.LoadInt64(&affinityConn.lastActive))
				logger.Debug(fmt.Sprintf("清理空闲连接: %s (空闲时间: %v)",
					clientConn.RemoteAddr(), idleDuration))
				affinityConn.signalClose()
			}
		}
	}

	if len(idleConnections) > 0 {
		logger.Info(fmt.Sprintf("空闲清理完成，移除了 %d 个空闲连接", len(idleConnections)))
	}
}

// statsReporter 统计报告器
func (h *OptimizedAffinityHandler) statsReporter() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.reportStats()
		}
	}
}

// reportStats 报告统计信息
func (h *OptimizedAffinityHandler) reportStats() {
	stats := h.GetStats()
	logger.Info(fmt.Sprintf("📊 优化Affinity统计 - 活跃: %d, 总计: %d, 创建: %d, 关闭: %d, 字节: %d, 池命中: %d, 池未命中: %d, 预连接: %d",
		stats.ActiveConnections, stats.TotalConnections,
		stats.ConnectionsCreated, stats.ConnectionsClosed, stats.BytesTransferred,
		stats.PoolHits, stats.PoolMisses, stats.PreConnections))
}

// Close 关闭处理器
func (h *OptimizedAffinityHandler) Close() error {
	logger.Info("关闭优化的Affinity处理器...")

	close(h.closing)

	// 关闭所有活跃连接
	h.activeConns.Range(func(key, value interface{}) bool {
		if affinityConn, ok := value.(*OptimizedAffinityConnection); ok {
			affinityConn.signalClose()
		}
		return true
	})

	// 等待所有协程完成
	h.wg.Wait()

	// 关闭预连接池
	if h.preConnPool != nil {
		atomic.StoreInt32(&h.preConnPool.closed, 1)
		close(h.preConnPool.connections)

		// 关闭池中所有连接
		for {
			select {
			case conn := <-h.preConnPool.connections:
				conn.Close()
			default:
				goto pool_closed
			}
		}
	pool_closed:
	}

	logger.Info("✅ 优化的Affinity处理器已关闭")
	return nil
}
