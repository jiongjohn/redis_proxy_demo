package pool

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"redis-proxy-demo/lib/logger"
)

// ConnectionContext 表示连接上下文信息
type ConnectionContext struct {
	Database        int    // 数据库编号
	Username        string // 用户名（Redis 6.0+）
	Password        string // 密码（Redis 6.0+ 支持用户名/密码）
	ClientName      string // 客户端名称
	ProtocolVersion int    // 协议版本
	// 客户端跟踪（RESP3 Client Tracking）简单支持
	TrackingEnabled bool   // 是否开启跟踪
	TrackingOptions string // 额外跟踪选项（简化存储原始参数）
}

// DedicatedConnection 专用连接
type DedicatedConnection struct {
	Conn        net.Conn
	id          string
	createdAt   time.Time
	lastUsed    int64 // Unix纳秒时间戳，使用原子操作
	inUse       int32 // 是否正在使用，使用原子操作
	boundClient string
	context     *ConnectionContext
	mu          sync.RWMutex
}

// DedicatedConnectionPool 专用连接池
type DedicatedConnectionPool struct {
	redisAddr      string
	redisPassword  string
	maxSize        int
	waitTimeout    time.Duration
	idleTimeout    time.Duration
	connections    []*DedicatedConnection
	availableQueue chan *DedicatedConnection
	mu             sync.RWMutex
	stats          *DedicatedPoolStats
	ctx            context.Context
	cancel         context.CancelFunc
	closed         int32
	helloV3Cache   string // 缓存的HELLO 3响应
}

// DedicatedPoolStats 连接池统计
type DedicatedPoolStats struct {
	TotalConnections   int64 `json:"total_connections"`
	ActiveConnections  int64 `json:"active_connections"`
	IdleConnections    int64 `json:"idle_connections"`
	ConnectionsCreated int64 `json:"connections_created"`
	ConnectionsClosed  int64 `json:"connections_closed"`
	GetRequests        int64 `json:"get_requests"`
	GetSuccesses       int64 `json:"get_successes"`
	GetTimeouts        int64 `json:"get_timeouts"`
	mu                 sync.RWMutex
}

// DedicatedPoolConfig 连接池配置
type DedicatedPoolConfig struct {
	RedisAddr     string
	RedisPassword string
	MaxSize       int           // 最大连接数
	InitSize      int           // 初始连接数
	WaitTimeout   time.Duration // 获取连接等待超时
	IdleTimeout   time.Duration // 连接空闲超时
}

// NewDedicatedConnectionPool 创建专用连接池
func NewDedicatedConnectionPool(config DedicatedPoolConfig) (*DedicatedConnectionPool, error) {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &DedicatedConnectionPool{
		redisAddr:      config.RedisAddr,
		redisPassword:  config.RedisPassword,
		maxSize:        config.MaxSize,
		waitTimeout:    config.WaitTimeout,
		idleTimeout:    config.IdleTimeout,
		connections:    make([]*DedicatedConnection, 0, config.MaxSize),
		availableQueue: make(chan *DedicatedConnection, config.MaxSize),
		stats:          &DedicatedPoolStats{},
		ctx:            ctx,
		cancel:         cancel,
	}

	// 预创建初始连接
	for i := 0; i < config.InitSize; i++ {
		conn, err := pool.createConnection()
		if err != nil {
			// 清理已创建的连接
			pool.Close()
			return nil, fmt.Errorf("创建初始连接失败: %w", err)
		}
		pool.connections = append(pool.connections, conn)
		pool.availableQueue <- conn
	}

	// 只设置创建计数，其他统计基于实际数据计算
	pool.stats.ConnectionsCreated = int64(config.InitSize)

	// 启动清理协程
	go pool.cleanupLoop()

	logger.Info(fmt.Sprintf("专用连接池创建成功: 初始连接数=%d, 最大连接数=%d", config.InitSize, config.MaxSize))
	return pool, nil
}

// GetConnection 获取连接 - 优化版本，减少锁竞争
func (p *DedicatedConnectionPool) GetConnection(clientID string, connCtx *ConnectionContext) (*DedicatedConnection, error) {
	if atomic.LoadInt32(&p.closed) != 0 {
		return nil, fmt.Errorf("连接池已关闭")
	}

	atomic.AddInt64(&p.stats.GetRequests, 1)

	// 添加诊断日志
	queueLen := len(p.availableQueue)
	totalConns := len(p.connections)
	logger.Debug(fmt.Sprintf("获取连接请求: 客户端=%s, 队列长度=%d, 总连接数=%d", clientID, queueLen, totalConns))

	// 快速路径：尝试从可用队列获取连接（非阻塞）
	select {
	case conn := <-p.availableQueue:
		// 检查连接是否有效
		if p.isConnectionValid(conn) {
			err := p.bindConnection(conn, clientID, connCtx)
			if err != nil {
				// 连接初始化失败，关闭连接并重试
				logger.Debug(fmt.Sprintf("连接绑定失败，重试: %v", err))
				p.closeConnection(conn)
				// 快速失败，不递归重试
				atomic.AddInt64(&p.stats.GetTimeouts, 1)
				return nil, fmt.Errorf("连接绑定失败: %w", err)
			}
			atomic.AddInt64(&p.stats.GetSuccesses, 1)
			logger.Debug(fmt.Sprintf("成功获取现有连接: %s -> %s", clientID, conn.id))
			return conn, nil
		} else {
			// 连接无效，关闭连接
			logger.Debug(fmt.Sprintf("连接无效，关闭连接: %s", conn.id))
			p.closeConnection(conn)
		}
	default:
		// 队列为空，继续到慢速路径
	}

	// 慢速路径：尝试创建新连接或等待
	return p.getConnectionSlowPath(clientID, connCtx)
}

// getConnectionSlowPath 慢速路径：创建新连接或等待可用连接
func (p *DedicatedConnectionPool) getConnectionSlowPath(clientID string, connCtx *ConnectionContext) (*DedicatedConnection, error) {
	// 检查是否可以创建新连接（无锁检查）
	currentSize := len(p.connections)
	if currentSize < p.maxSize {
		// 尝试创建新连接
		conn, err := p.tryCreateConnection(clientID, connCtx)
		if err == nil {
			return conn, nil
		}
		// 创建失败，继续到等待逻辑
		logger.Debug(fmt.Sprintf("创建连接失败: %v", err))
	}

	// 连接池已满或创建失败，等待可用连接
	logger.Debug(fmt.Sprintf("连接池已满或创建失败，等待可用连接: %s", clientID))

	timeout := time.NewTimer(p.waitTimeout)
	defer timeout.Stop()

	for {
		select {
		case conn := <-p.availableQueue:
			if p.isConnectionValid(conn) {
				err := p.bindConnection(conn, clientID, connCtx)
				if err != nil {
					p.closeConnection(conn)
					continue // 继续等待下一个连接
				}
				atomic.AddInt64(&p.stats.GetSuccesses, 1)
				logger.Debug(fmt.Sprintf("等待获取连接成功: %s -> %s", clientID, conn.id))
				return conn, nil
			} else {
				p.closeConnection(conn)
				continue // 继续等待下一个连接
			}
		case <-timeout.C:
			atomic.AddInt64(&p.stats.GetTimeouts, 1)
			logger.Debug(fmt.Sprintf("等待连接超时: %s (超时时间: %v)", clientID, p.waitTimeout))
			return nil, fmt.Errorf("获取连接超时")
		case <-p.ctx.Done():
			return nil, fmt.Errorf("连接池已关闭")
		}
	}
}

// tryCreateConnection 尝试创建新连接（带锁保护）
func (p *DedicatedConnectionPool) tryCreateConnection(clientID string, connCtx *ConnectionContext) (*DedicatedConnection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 再次检查连接数（双重检查）
	if len(p.connections) >= p.maxSize {
		return nil, fmt.Errorf("连接池已满")
	}

	// 创建新连接
	conn, err := p.createConnection()
	if err != nil {
		return nil, fmt.Errorf("创建连接失败: %w", err)
	}

	p.connections = append(p.connections, conn)
	atomic.AddInt64(&p.stats.ConnectionsCreated, 1) // 只更新创建计数

	// 绑定连接
	err = p.bindConnection(conn, clientID, connCtx)
	if err != nil {
		p.closeConnection(conn)
		return nil, fmt.Errorf("连接绑定失败: %w", err)
	}

	logger.Debug(fmt.Sprintf("成功创建新连接: %s -> %s", clientID, conn.id))
	return conn, nil
}

// createConnection 创建新连接
func (p *DedicatedConnectionPool) createConnection() (*DedicatedConnection, error) {
	conn, err := CreateConnection(&RedisConnConfig{
		Addr:     p.redisAddr,
		Password: p.redisPassword,
	})

	if err != nil {
		return nil, err
	}

	connID := fmt.Sprintf("dedicated_%d", time.Now().UnixNano())

	return &DedicatedConnection{
		Conn:      conn,
		id:        connID,
		createdAt: time.Now(),
		lastUsed:  time.Now().UnixNano(),
		inUse:     0, // 新连接初始为空闲状态
	}, nil
}

// bindConnection 绑定连接到客户端
func (p *DedicatedConnectionPool) bindConnection(conn *DedicatedConnection, clientID string, connCtx *ConnectionContext) error {
	return p.bindConnectionWithStats(conn, clientID, connCtx, true)
}

// bindConnectionWithStats 绑定连接到客户端，可选择是否更新统计
func (p *DedicatedConnectionPool) bindConnectionWithStats(conn *DedicatedConnection, clientID string, connCtx *ConnectionContext, updateStats bool) error {
	conn.mu.Lock()
	defer conn.mu.Unlock()

	// 设置连接为使用中
	atomic.StoreInt32(&conn.inUse, 1)
	atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
	conn.boundClient = clientID

	// 应用成功后才设置context
	conn.context = connCtx

	logger.Debug(fmt.Sprintf("绑定连接: %s -> %s", clientID, conn.id))
	return nil
}

// ReleaseConnection 释放连接
func (p *DedicatedConnectionPool) ReleaseConnection(conn *DedicatedConnection) {
	if conn == nil {
		return
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	if atomic.LoadInt32(&conn.inUse) == 0 {
		return // 已经释放
	}

	atomic.StoreInt32(&conn.inUse, 0)
	atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
	conn.boundClient = ""
	conn.context = nil

	// 将连接放回可用队列 - 修复：阻塞等待而不是直接关闭
	select {
	case p.availableQueue <- conn:
		// 成功放回队列，统计基于实际数据计算
		logger.Debug(fmt.Sprintf("释放连接: %s", conn.id))
	case <-time.After(5 * time.Second):
		// 超时后关闭连接（这种情况应该很少发生）
		logger.Debug(fmt.Sprintf("释放连接超时，关闭连接: %s", conn.id))
		p.closeConnection(conn)
	case <-p.ctx.Done():
		// 连接池关闭，直接关闭连接
		p.closeConnection(conn)
	}
}

// isConnectionValid 检查连接是否有效
func (p *DedicatedConnectionPool) isConnectionValid(conn *DedicatedConnection) bool {
	if conn == nil || conn.Conn == nil {
		return false
	}

	// 检查连接是否已被标记为使用中
	if atomic.LoadInt32(&conn.inUse) != 0 {
		return false
	}

	// 检查连接是否超时（空闲时间过长）
	lastUsed := atomic.LoadInt64(&conn.lastUsed)
	if lastUsed > 0 && time.Since(time.Unix(0, lastUsed)) > p.idleTimeout {
		logger.Debug(fmt.Sprintf("连接 %s 空闲超时，最后使用时间: %v", conn.id, time.Unix(0, lastUsed)))
		return false
	}

	// 简单的TCP连接状态检查（不发送数据）
	// 设置一个很短的超时来检查连接是否可写
	conn.Conn.SetWriteDeadline(time.Now().Add(1 * time.Millisecond))

	// 尝试写入0字节来检查连接状态
	_, err := conn.Conn.Write([]byte{})

	// 立即清除deadline
	conn.Conn.SetDeadline(time.Time{})

	if err != nil {
		logger.Debug(fmt.Sprintf("连接 %s 网络检查失败: %v", conn.id, err))
		return false
	}

	return true
}

// cleanupExpiredConnections 清理过期的空闲连接
func (p *DedicatedConnectionPool) cleanupExpiredConnections() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	var validConnections []*DedicatedConnection
	var expiredCount int

	// 遍历所有连接，移除过期的
	for _, conn := range p.connections {
		if conn == nil {
			continue
		}

		// 检查连接是否过期
		lastUsed := atomic.LoadInt64(&conn.lastUsed)
		if lastUsed > 0 && now.Sub(time.Unix(0, lastUsed)) > p.idleTimeout {
			// 连接过期，只关闭TCP连接，不更新统计（统计会在下面统一更新）
			if conn.Conn != nil {
				conn.Conn.Close()
			}
			expiredCount++
			logger.Debug(fmt.Sprintf("清理过期连接: %s, 最后使用: %v", conn.id, time.Unix(0, lastUsed)))
		} else {
			// 连接有效，保留
			validConnections = append(validConnections, conn)
		}
	}

	// 更新连接列表
	p.connections = validConnections

	// 更新统计 - 正确更新所有相关计数
	if expiredCount > 0 {
		atomic.AddInt64(&p.stats.ConnectionsClosed, int64(expiredCount))
		atomic.AddInt64(&p.stats.TotalConnections, -int64(expiredCount))
		atomic.AddInt64(&p.stats.IdleConnections, -int64(expiredCount)) // 清理的都是空闲连接
		logger.Debug(fmt.Sprintf("清理了 %d 个过期连接，剩余连接: %d", expiredCount, len(validConnections)))
	}

	// 清理availableQueue中的过期连接
	queueSize := len(p.availableQueue)
	for i := 0; i < queueSize; i++ {
		select {
		case conn := <-p.availableQueue:
			if p.isConnectionValid(conn) {
				// 连接有效，放回队列
				select {
				case p.availableQueue <- conn:
				default:
					// 队列满了，关闭连接
					p.closeConnectionUnsafe(conn)
				}
			} else {
				// 连接无效，已经在isConnectionValid中处理了
				logger.Debug(fmt.Sprintf("从队列中移除无效连接: %s", conn.id))
			}
		default:
			// 队列为空，退出循环
			return
		}
	}
}

// closeConnectionUnsafe 关闭连接（不获取锁，用于已持有锁的场景）
func (p *DedicatedConnectionPool) closeConnectionUnsafe(conn *DedicatedConnection) {
	if conn == nil || conn.Conn == nil {
		return
	}

	// 从连接列表中移除
	for i, c := range p.connections {
		if c == conn {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			break
		}
	}

	conn.Conn.Close()

	// 只更新关闭计数，其他统计基于实际数据计算
	atomic.AddInt64(&p.stats.ConnectionsClosed, 1)

	logger.Debug(fmt.Sprintf("关闭连接(unsafe): %s", conn.id))
}

// closeConnection 关闭连接
func (p *DedicatedConnectionPool) closeConnection(conn *DedicatedConnection) {
	if conn == nil || conn.Conn == nil {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// 从连接列表中移除
	for i, c := range p.connections {
		if c == conn {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			break
		}
	}

	conn.Conn.Close()

	// 只更新关闭计数，其他统计基于实际数据计算
	atomic.AddInt64(&p.stats.ConnectionsClosed, 1)

	logger.Debug(fmt.Sprintf("关闭连接: %s", conn.id))
}

// cleanupLoop 清理循环
func (p *DedicatedConnectionPool) cleanupLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.cleanupExpiredConnections()
		}
	}
}

// cleanup 清理空闲连接
func (p *DedicatedConnectionPool) cleanup() {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now().UnixNano()
	closedCount := 0

	// 清理空闲超时的连接
	for i := len(p.connections) - 1; i >= 0; i-- {
		conn := p.connections[i]

		if atomic.LoadInt32(&conn.inUse) == 0 {
			lastUsed := atomic.LoadInt64(&conn.lastUsed)
			if time.Duration(now-lastUsed) > p.idleTimeout {
				// 从可用队列中移除（如果存在）
				select {
				case <-p.availableQueue:
				default:
				}

				p.closeConnection(conn)
				closedCount++
			}
		}
	}

	if closedCount > 0 {
		logger.Info(fmt.Sprintf("清理空闲连接: 关闭 %d 个连接, 剩余 %d 个连接", closedCount, len(p.connections)))
	}
}

// GetStats 获取统计信息
func (p *DedicatedConnectionPool) GetStats() DedicatedPoolStats {
	p.stats.mu.RLock()
	defer p.stats.mu.RUnlock()

	// 获取实际连接数
	p.mu.RLock()
	actualTotal := int64(len(p.connections))
	availableCount := int64(len(p.availableQueue))
	p.mu.RUnlock()

	// 计算活跃连接数 = 总连接数 - 可用队列中的连接数
	actualActive := actualTotal - availableCount

	return DedicatedPoolStats{
		TotalConnections:   actualTotal,    // 使用实际连接数
		ActiveConnections:  actualActive,   // 计算得出的活跃连接数
		IdleConnections:    availableCount, // 队列中的空闲连接数
		ConnectionsCreated: p.stats.ConnectionsCreated,
		ConnectionsClosed:  p.stats.ConnectionsClosed,
		GetRequests:        p.stats.GetRequests,
		GetSuccesses:       p.stats.GetSuccesses,
		GetTimeouts:        p.stats.GetTimeouts,
	}
}

// Close 关闭连接池
func (p *DedicatedConnectionPool) Close() error {
	atomic.StoreInt32(&p.closed, 1)
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	// 关闭所有连接
	for _, conn := range p.connections {
		if conn.Conn != nil {
			conn.Conn.Close()
		}
	}

	// 清空队列
	close(p.availableQueue)
	for range p.availableQueue {
		// 清空队列
	}

	p.connections = p.connections[:0]
	logger.Info("专用连接池已关闭")
	return nil
}
