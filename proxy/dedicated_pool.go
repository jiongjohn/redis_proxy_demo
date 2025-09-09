package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"redis-proxy-demo/lib/logger"
)

// DedicatedConnection 专用连接
type DedicatedConnection struct {
	conn        net.Conn
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

	pool.stats.TotalConnections = int64(config.InitSize)
	pool.stats.IdleConnections = int64(config.InitSize)
	pool.stats.ConnectionsCreated = int64(config.InitSize)

	// 启动清理协程
	go pool.cleanupLoop()

	logger.Info(fmt.Sprintf("专用连接池创建成功: 初始连接数=%d, 最大连接数=%d", config.InitSize, config.MaxSize))
	return pool, nil
}

// GetConnection 获取连接
func (p *DedicatedConnectionPool) GetConnection(clientID string, connCtx *ConnectionContext) (*DedicatedConnection, error) {
	if atomic.LoadInt32(&p.closed) != 0 {
		return nil, fmt.Errorf("连接池已关闭")
	}

	atomic.AddInt64(&p.stats.GetRequests, 1)

	// 添加诊断日志
	queueLen := len(p.availableQueue)
	totalConns := len(p.connections)
	logger.Debug(fmt.Sprintf("获取连接请求: 客户端=%s, 队列长度=%d, 总连接数=%d", clientID, queueLen, totalConns))

	// 尝试从可用队列获取连接
	select {
	case conn := <-p.availableQueue:
		// 检查连接是否有效
		if p.isConnectionValid(conn) {
			err := p.bindConnection(conn, clientID, connCtx)
			if err != nil {
				// 连接初始化失败，关闭连接并重试
				logger.Debug(fmt.Sprintf("连接绑定失败，重试: %v", err))
				p.closeConnection(conn)
				return p.createAndBindConnection(clientID, connCtx)
			}
			atomic.AddInt64(&p.stats.GetSuccesses, 1)
			logger.Debug(fmt.Sprintf("成功获取现有连接: %s -> %s", clientID, conn.id))
			return conn, nil
		} else {
			// 连接无效，关闭并创建新连接
			logger.Debug(fmt.Sprintf("连接无效，关闭并创建新连接: %s", conn.id))
			p.closeConnection(conn)
			// 直接创建新连接，不再递归调用createAndBindConnection避免无限循环
			return p.createNewConnection(clientID, connCtx)
		}
	default:
		// 没有可用连接，尝试创建新连接
		logger.Debug(fmt.Sprintf("队列为空，创建新连接: %s", clientID))
		return p.createAndBindConnection(clientID, connCtx)
	}
}

// createAndBindConnection 创建并绑定连接
func (p *DedicatedConnectionPool) createAndBindConnection(clientID string, connCtx *ConnectionContext) (*DedicatedConnection, error) {
	p.mu.Lock()

	// 检查是否超过最大连接数
	currentSize := len(p.connections)
	if currentSize >= p.maxSize {
		logger.Debug(fmt.Sprintf("连接池已满 (%d/%d)，等待可用连接: %s", currentSize, p.maxSize, clientID))
		p.mu.Unlock() // 释放锁后等待可用连接
		// 等待可用连接
		select {
		case conn := <-p.availableQueue:
			// 重新获取锁来操作连接
			p.mu.Lock()
			defer p.mu.Unlock()
			logger.Debug(fmt.Sprintf("从等待队列获取连接: %s -> %s", clientID, conn.id))
			if p.isConnectionValid(conn) {
				err := p.bindConnection(conn, clientID, connCtx)
				if err != nil {
					p.closeConnection(conn)
					atomic.AddInt64(&p.stats.GetTimeouts, 1)
					return nil, fmt.Errorf("连接初始化失败: %w", err)
				}
				atomic.AddInt64(&p.stats.GetSuccesses, 1)
				return conn, nil
			} else {
				p.closeConnection(conn)
				atomic.AddInt64(&p.stats.GetTimeouts, 1)
				return nil, fmt.Errorf("获取到无效连接")
			}
		case <-time.After(p.waitTimeout):
			atomic.AddInt64(&p.stats.GetTimeouts, 1)
			logger.Debug(fmt.Sprintf("等待连接超时: %s (超时时间: %v)", clientID, p.waitTimeout))
			return nil, fmt.Errorf("获取连接超时")
		case <-p.ctx.Done():
			return nil, fmt.Errorf("连接池已关闭")
		}
	}

	// 如果没有超过最大连接数，继续创建新连接（此时仍持有锁）
	logger.Debug(fmt.Sprintf("创建新连接: %s (当前池大小: %d/%d)", clientID, currentSize, p.maxSize))
	defer p.mu.Unlock()

	// 创建新连接
	conn, err := p.createConnection()
	if err != nil {
		return nil, fmt.Errorf("创建连接失败: %w", err)
	}

	p.connections = append(p.connections, conn)
	atomic.AddInt64(&p.stats.TotalConnections, 1)
	atomic.AddInt64(&p.stats.ConnectionsCreated, 1)
	atomic.AddInt64(&p.stats.ActiveConnections, 1) // 新连接直接标记为活跃

	err = p.bindConnectionWithStats(conn, clientID, connCtx, false) // 不更新统计，因为已经手动更新了
	if err != nil {
		// 绑定失败，需要清理统计和连接
		p.connections = p.connections[:len(p.connections)-1]
		atomic.AddInt64(&p.stats.TotalConnections, -1)
		atomic.AddInt64(&p.stats.ActiveConnections, -1)
		conn.conn.Close()
		return nil, fmt.Errorf("绑定连接失败: %w", err)
	}

	atomic.AddInt64(&p.stats.GetSuccesses, 1)

	logger.Debug(fmt.Sprintf("创建新连接: %s -> %s (池大小: %d/%d)", clientID, conn.id, len(p.connections), p.maxSize))
	return conn, nil
}

// createConnection 创建新连接
func (p *DedicatedConnectionPool) createConnection() (*DedicatedConnection, error) {
	conn, err := net.DialTimeout("tcp", p.redisAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	// TCP优化
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	connID := fmt.Sprintf("dedicated_%d", time.Now().UnixNano())

	return &DedicatedConnection{
		conn:      conn,
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

	// 检查连接之前的状态
	wasInUse := atomic.LoadInt32(&conn.inUse) == 1

	atomic.StoreInt32(&conn.inUse, 1)
	atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
	conn.boundClient = clientID

	// 先应用连接上下文（认证、选择数据库等），然后再设置context
	err := p.applyConnectionContext(conn, connCtx)
	if err != nil {
		return err
	}

	// 应用成功后才设置context
	conn.context = connCtx

	// 更新统计：连接变为活跃状态
	if updateStats && !wasInUse {
		atomic.AddInt64(&p.stats.ActiveConnections, 1)
		atomic.AddInt64(&p.stats.IdleConnections, -1)
	}

	logger.Debug(fmt.Sprintf("绑定连接: %s -> %s", clientID, conn.id))
	return nil
}

// applyConnectionContext 应用连接上下文
func (p *DedicatedConnectionPool) applyConnectionContext(conn *DedicatedConnection, connCtx *ConnectionContext) error {
	if connCtx == nil {
		return nil
	}

	// 执行Redis认证
	if connCtx.Username != "" || connCtx.Password != "" || p.redisPassword != "" {
		var authCmd string
		if connCtx.Username != "" {
			// AUTH <username> <password>
			pwd := connCtx.Password
			authCmd = fmt.Sprintf("*3\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(connCtx.Username), connCtx.Username,
				len(pwd), pwd)
		} else {
			// 单参数AUTH
			pwd := connCtx.Password
			if pwd == "" {
				pwd = p.redisPassword
			}
			authCmd = fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n", len(pwd), pwd)
		}

		err := p.executeCommand(conn.conn, authCmd)
		if err != nil {
			return fmt.Errorf("认证失败: %w", err)
		}
	}

	// 如果要求RESP3，发送 HELLO 3
	// 只有在协议版本改变或者连接是新创建的时候才发送HELLO命令
	needHello := false
	if connCtx.ProtocolVersion == 3 {
		if conn.context == nil || conn.context.ProtocolVersion != 3 {
			needHello = true
		}
	}

	logger.Debug(fmt.Sprintf("应用上下文: 协议版本=%d, 需要HELLO=%v, 连接上下文=%v, 连接上下文详情=%+v",
		connCtx.ProtocolVersion, needHello, conn.context != nil, conn.context))

	if needHello {
		helloCmd := "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
		logger.Debug("发送HELLO 3命令到Redis服务器")
		err := p.executeCommandAndCacheHello(conn.conn, helloCmd)
		if err != nil {
			return fmt.Errorf("HELLO 3失败: %w", err)
		}
		logger.Debug(fmt.Sprintf("HELLO 3命令执行成功，缓存长度: %d", len(p.helloV3Cache)))
	}

	// 设置数据库
	if connCtx.Database != 0 {
		selectCmd := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n",
			len(fmt.Sprintf("%d", connCtx.Database)), connCtx.Database)
		err := p.executeCommand(conn.conn, selectCmd)
		if err != nil {
			return fmt.Errorf("选择数据库失败: %w", err)
		}
	}

	// 设置客户端名称
	if connCtx.ClientName != "" {
		nameCmd := fmt.Sprintf("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$%d\r\n%s\r\n",
			len(connCtx.ClientName), connCtx.ClientName)
		err := p.executeCommand(conn.conn, nameCmd)
		if err != nil {
			return fmt.Errorf("设置客户端名称失败: %w", err)
		}
	}

	// 设置客户端跟踪
	if connCtx.TrackingEnabled {
		trackingCmd := "*3\r\n$6\r\nCLIENT\r\n$8\r\nTRACKING\r\n$2\r\nON\r\n"
		err := p.executeCommand(conn.conn, trackingCmd)
		if err != nil {
			return fmt.Errorf("设置客户端跟踪失败: %w", err)
		}
	}

	return nil
}

// executeCommand 执行Redis命令
func (p *DedicatedConnectionPool) executeCommand(conn net.Conn, cmd string) error {
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err := conn.Write([]byte(cmd))
	if err != nil {
		return err
	}

	// 读取响应
	buffer := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	response := string(buffer[:n])
	if !containsOK(response) && response[0] != '%' { // RESP3 map响应以%开头
		return fmt.Errorf("命令执行失败: %s", response)
	}

	// 清除deadline
	conn.SetDeadline(time.Time{})
	return nil
}

// executeCommandAndCacheHello 执行HELLO命令并缓存响应
func (p *DedicatedConnectionPool) executeCommandAndCacheHello(conn net.Conn, cmd string) error {
	helloCmd := "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n"
	conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_, err := conn.Write([]byte(helloCmd))
	if err != nil {
		return fmt.Errorf("发送HELLO 3命令失败: %w", err)
	}
	buffer := make([]byte, 4096)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("读取HELLO 3响应失败: %w", err)
	}
	// 注意：调用方在创建连接时已持有 pool.mu，这里直接写入避免死锁

	p.mu.Lock()
	p.helloV3Cache = string(buffer[:n])
	p.mu.Unlock()

	// 清除deadline
	conn.SetDeadline(time.Time{})
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
		// 成功放回队列，更新统计
		atomic.AddInt64(&p.stats.ActiveConnections, -1)
		atomic.AddInt64(&p.stats.IdleConnections, 1)
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
	if conn == nil || conn.conn == nil {
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
	conn.conn.SetWriteDeadline(time.Now().Add(1 * time.Millisecond))

	// 尝试写入0字节来检查连接状态
	_, err := conn.conn.Write([]byte{})

	// 立即清除deadline
	conn.conn.SetDeadline(time.Time{})

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
			if conn.conn != nil {
				conn.conn.Close()
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
			break
		}
	}
}

// closeConnectionUnsafe 关闭连接（不获取锁，用于已持有锁的场景）
func (p *DedicatedConnectionPool) closeConnectionUnsafe(conn *DedicatedConnection) {
	if conn == nil || conn.conn == nil {
		return
	}

	// 从连接列表中移除
	for i, c := range p.connections {
		if c == conn {
			p.connections = append(p.connections[:i], p.connections[i+1:]...)
			break
		}
	}

	conn.conn.Close()

	atomic.AddInt64(&p.stats.TotalConnections, -1)
	atomic.AddInt64(&p.stats.ConnectionsClosed, 1)

	if atomic.LoadInt32(&conn.inUse) == 1 {
		atomic.AddInt64(&p.stats.ActiveConnections, -1)
	} else {
		atomic.AddInt64(&p.stats.IdleConnections, -1)
	}

	logger.Debug(fmt.Sprintf("关闭连接(unsafe): %s", conn.id))
}

// createNewConnection 直接创建新连接，用于替换无效连接
func (p *DedicatedConnectionPool) createNewConnection(clientID string, connCtx *ConnectionContext) (*DedicatedConnection, error) {
	// 直接创建新连接，不检查池大小限制（因为我们是在替换无效连接）
	logger.Debug(fmt.Sprintf("直接创建新连接替换无效连接: %s", clientID))

	// 连接到Redis
	conn, err := net.Dial("tcp", p.redisAddr)
	if err != nil {
		atomic.AddInt64(&p.stats.GetTimeouts, 1)
		return nil, fmt.Errorf("连接Redis失败: %w", err)
	}

	// 创建连接对象
	id := fmt.Sprintf("dedicated_%d", time.Now().UnixNano())
	dedicatedConn := &DedicatedConnection{
		id:       id,
		conn:     conn,
		lastUsed: time.Now().UnixNano(),
		inUse:    1, // 标记为使用中
	}

	// 添加到连接池（需要获取锁）
	p.mu.Lock()
	p.connections = append(p.connections, dedicatedConn)
	atomic.AddInt64(&p.stats.TotalConnections, 1)
	atomic.AddInt64(&p.stats.ConnectionsCreated, 1)
	atomic.AddInt64(&p.stats.ActiveConnections, 1) // 新连接直接标记为活跃
	p.mu.Unlock()

	// 绑定连接（不持有锁，避免死锁）- 不更新统计，因为已经手动更新了
	err = p.bindConnectionWithStats(dedicatedConn, clientID, connCtx, false)
	if err != nil {
		p.closeConnection(dedicatedConn)
		atomic.AddInt64(&p.stats.GetTimeouts, 1)
		return nil, fmt.Errorf("连接初始化失败: %w", err)
	}

	atomic.AddInt64(&p.stats.GetSuccesses, 1)
	logger.Debug(fmt.Sprintf("成功创建并绑定新连接: %s -> %s", clientID, id))
	return dedicatedConn, nil
}

// closeConnection 关闭连接
func (p *DedicatedConnectionPool) closeConnection(conn *DedicatedConnection) {
	if conn == nil || conn.conn == nil {
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

	conn.conn.Close()

	atomic.AddInt64(&p.stats.TotalConnections, -1)
	atomic.AddInt64(&p.stats.ConnectionsClosed, 1)

	if atomic.LoadInt32(&conn.inUse) == 1 {
		atomic.AddInt64(&p.stats.ActiveConnections, -1)
	} else {
		atomic.AddInt64(&p.stats.IdleConnections, -1)
	}

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

	return DedicatedPoolStats{
		TotalConnections:   p.stats.TotalConnections,
		ActiveConnections:  p.stats.ActiveConnections,
		IdleConnections:    p.stats.IdleConnections,
		ConnectionsCreated: p.stats.ConnectionsCreated,
		ConnectionsClosed:  p.stats.ConnectionsClosed,
		GetRequests:        p.stats.GetRequests,
		GetSuccesses:       p.stats.GetSuccesses,
		GetTimeouts:        p.stats.GetTimeouts,
	}
}

// GetHelloV3Response 获取缓存的HELLO 3响应
func (p *DedicatedConnectionPool) GetHelloV3Response() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.helloV3Cache
}

// Close 关闭连接池
func (p *DedicatedConnectionPool) Close() error {
	atomic.StoreInt32(&p.closed, 1)
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	// 关闭所有连接
	for _, conn := range p.connections {
		if conn.conn != nil {
			conn.conn.Close()
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
