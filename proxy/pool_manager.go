package proxy

import (
	"context"
	"crypto/md5"
	"fmt"
	"net"
	"sync"
	"time"

	"redis-proxy-demo/lib/logger"
)

// ConnectionContext 表示连接上下文信息
type ConnectionContext struct {
	Database        int    // 数据库编号
	Username        string // 用户名（Redis 6.0+）
	ClientName      string // 客户端名称
	ProtocolVersion int    // 协议版本
}

// Hash 生成上下文的唯一标识
func (ctx *ConnectionContext) Hash() string {
	data := fmt.Sprintf("db:%d|user:%s|name:%s|proto:%d",
		ctx.Database, ctx.Username, ctx.ClientName, ctx.ProtocolVersion)
	return fmt.Sprintf("%x", md5.Sum([]byte(data)))
}

// PooledConnection 池化连接包装
type PooledConnection struct {
	Conn         net.Conn             // 底层连接
	Context      *ConnectionContext   // 连接上下文
	State        RedisConnectionState // 连接状态
	LastUsed     time.Time            // 最后使用时间
	CreatedAt    time.Time            // 创建时间
	UsageCount   int64                // 使用次数
	InUse        bool                 // 是否正在使用
	StickyClient string               // 粘性客户端ID（用于会话命令）
	mu           sync.RWMutex         // 保护连接状态
}

// UpdateState 更新连接状态
func (pc *PooledConnection) UpdateState(newState RedisConnectionState) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.State = newState
	pc.LastUsed = time.Now()
}

// IsIdle 检查连接是否空闲
func (pc *PooledConnection) IsIdle(timeout time.Duration) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return !pc.InUse && time.Since(pc.LastUsed) > timeout
}

// CanReuse 检查连接是否可以复用
func (pc *PooledConnection) CanReuse(clientID string) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	// 如果连接有粘性客户端，只能被该客户端使用
	if pc.StickyClient != "" {
		return pc.StickyClient == clientID && pc.State == StateNormal
	}

	// 普通连接只有在Normal状态才能复用
	return pc.State == StateNormal && !pc.InUse
}

// ConnectionPool 连接池
type ConnectionPool struct {
	contextHash   string               // 上下文哈希
	context       *ConnectionContext   // 连接上下文
	connections   []*PooledConnection  // 连接列表
	maxSize       int                  // 最大连接数
	minIdle       int                  // 最小空闲连接数
	maxIdle       int                  // 最大空闲连接数
	idleTimeout   time.Duration        // 空闲超时时间
	maxLifetime   time.Duration        // 连接最大生命周期
	redisAddr     string               // Redis地址
	redisPassword string               // Redis密码
	mu            sync.RWMutex         // 保护连接列表
	stats         *ConnectionPoolStats // 池统计信息
}

// ConnectionPoolStats 连接池统计
type ConnectionPoolStats struct {
	TotalConnections   int64 `json:"total_connections"`
	ActiveConnections  int64 `json:"active_connections"`
	IdleConnections    int64 `json:"idle_connections"`
	ConnectionsCreated int64 `json:"connections_created"`
	ConnectionsClosed  int64 `json:"connections_closed"`
	GetRequests        int64 `json:"get_requests"`
	GetSuccesses       int64 `json:"get_successes"`
	mu                 sync.RWMutex
}

// ManagerStats 管理器统计信息
type ManagerStats struct {
	TotalPools       int                            `json:"total_pools"`
	TotalConnections int64                          `json:"total_connections"`
	PoolStats        map[string]ConnectionPoolStats `json:"pool_stats"`
}

// NewConnectionPool 创建新的连接池
func NewConnectionPool(ctx *ConnectionContext, redisAddr, redisPassword string,
	maxSize, minIdle, maxIdle int, idleTimeout, maxLifetime time.Duration) *ConnectionPool {

	return &ConnectionPool{
		contextHash:   ctx.Hash(),
		context:       ctx,
		connections:   make([]*PooledConnection, 0, maxSize),
		maxSize:       maxSize,
		minIdle:       minIdle,
		maxIdle:       maxIdle,
		idleTimeout:   idleTimeout,
		maxLifetime:   maxLifetime,
		redisAddr:     redisAddr,
		redisPassword: redisPassword,
		stats:         &ConnectionPoolStats{},
	}
}

// Get 从连接池获取连接
func (pool *ConnectionPool) Get(clientID string) (*PooledConnection, error) {
	pool.stats.mu.Lock()
	pool.stats.GetRequests++
	pool.stats.mu.Unlock()

	pool.mu.Lock()
	defer pool.mu.Unlock()

	// 首先尝试找到可复用的连接
	for _, conn := range pool.connections {
		if conn.CanReuse(clientID) {
			conn.mu.Lock()
			conn.InUse = true
			conn.LastUsed = time.Now()
			conn.UsageCount++
			conn.mu.Unlock()

			pool.stats.mu.Lock()
			pool.stats.GetSuccesses++
			pool.stats.ActiveConnections++
			pool.stats.IdleConnections--
			pool.stats.mu.Unlock()

			logger.Debug(fmt.Sprintf("复用连接池连接: %s -> %s (使用次数: %d)",
				clientID, conn.Conn.RemoteAddr(), conn.UsageCount))
			return conn, nil
		}
	}

	// 如果没有可复用的连接且还能创建新连接
	if len(pool.connections) < pool.maxSize {
		conn, err := pool.createConnection()
		if err != nil {
			return nil, fmt.Errorf("创建新连接失败: %w", err)
		}

		conn.mu.Lock()
		conn.InUse = true
		conn.LastUsed = time.Now()
		conn.UsageCount++
		conn.mu.Unlock()

		pool.connections = append(pool.connections, conn)

		pool.stats.mu.Lock()
		pool.stats.GetSuccesses++
		pool.stats.TotalConnections++
		pool.stats.ActiveConnections++
		pool.stats.ConnectionsCreated++
		pool.stats.mu.Unlock()

		logger.Info(fmt.Sprintf("创建新连接池连接: %s -> %s (池大小: %d/%d)",
			clientID, conn.Conn.RemoteAddr(), len(pool.connections), pool.maxSize))
		return conn, nil
	}

	return nil, fmt.Errorf("连接池已满且无可用连接")
}

// Return 将连接归还给连接池
func (pool *ConnectionPool) Return(conn *PooledConnection, clientID string) {
	if conn == nil {
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	conn.mu.Lock()
	defer conn.mu.Unlock()

	// 检查连接是否应该被释放
	if conn.State != StateNormal || conn.StickyClient != "" {
		// 会话连接不能立即释放，保持粘性
		if conn.StickyClient == "" {
			conn.StickyClient = clientID
		}
		logger.Debug(fmt.Sprintf("连接保持粘性绑定: %s -> %s (状态: %s)",
			clientID, conn.Conn.RemoteAddr(), conn.State))
	} else {
		// 普通连接可以释放复用
		conn.InUse = false
		conn.LastUsed = time.Now()
		conn.StickyClient = "" // 清除粘性绑定

		pool.stats.mu.Lock()
		pool.stats.ActiveConnections--
		pool.stats.IdleConnections++
		pool.stats.mu.Unlock()

		logger.Debug(fmt.Sprintf("归还连接到连接池: %s -> %s",
			clientID, conn.Conn.RemoteAddr()))
	}
}

// Release 释放粘性连接（当会话结束时）
func (pool *ConnectionPool) Release(conn *PooledConnection, clientID string) {
	if conn == nil {
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	conn.mu.Lock()
	defer conn.mu.Unlock()

	if conn.StickyClient == clientID {
		conn.InUse = false
		conn.LastUsed = time.Now()
		conn.StickyClient = ""
		conn.State = StateNormal

		pool.stats.mu.Lock()
		pool.stats.ActiveConnections--
		pool.stats.IdleConnections++
		pool.stats.mu.Unlock()

		logger.Debug(fmt.Sprintf("释放粘性连接: %s -> %s",
			clientID, conn.Conn.RemoteAddr()))
	}
}

// createConnection 创建新的Redis连接
func (pool *ConnectionPool) createConnection() (*PooledConnection, error) {
	conn, err := net.DialTimeout("tcp", pool.redisAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("连接Redis失败: %w", err)
	}

	// 执行Redis认证（如果需要）
	if pool.redisPassword != "" {
		authCmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n",
			len(pool.redisPassword), pool.redisPassword)

		_, err = conn.Write([]byte(authCmd))
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("发送AUTH命令失败: %w", err)
		}

		// 读取AUTH响应
		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("读取AUTH响应失败: %w", err)
		}

		response := string(buffer[:n])
		if !containsOK(response) {
			conn.Close()
			return nil, fmt.Errorf("Redis认证失败: %s", response)
		}
	}

	// 应用连接上下文
	err = pool.applyContext(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("应用连接上下文失败: %w", err)
	}

	return &PooledConnection{
		Conn:         conn,
		Context:      pool.context,
		State:        StateNormal,
		LastUsed:     time.Now(),
		CreatedAt:    time.Now(),
		UsageCount:   0,
		InUse:        false,
		StickyClient: "",
	}, nil
}

// applyContext 应用连接上下文
func (pool *ConnectionPool) applyContext(conn net.Conn) error {
	// 设置数据库
	if pool.context.Database != 0 {
		selectCmd := fmt.Sprintf("*2\r\n$6\r\nSELECT\r\n$%d\r\n%d\r\n",
			len(fmt.Sprintf("%d", pool.context.Database)), pool.context.Database)

		_, err := conn.Write([]byte(selectCmd))
		if err != nil {
			return fmt.Errorf("发送SELECT命令失败: %w", err)
		}

		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			return fmt.Errorf("读取SELECT响应失败: %w", err)
		}

		response := string(buffer[:n])
		if !containsOK(response) {
			return fmt.Errorf("SELECT命令失败: %s", response)
		}
	}

	// 设置客户端名称
	if pool.context.ClientName != "" {
		nameCmd := fmt.Sprintf("*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$%d\r\n%s\r\n",
			len(pool.context.ClientName), pool.context.ClientName)

		_, err := conn.Write([]byte(nameCmd))
		if err != nil {
			return fmt.Errorf("发送CLIENT SETNAME命令失败: %w", err)
		}

		buffer := make([]byte, 1024)
		n, err := conn.Read(buffer)
		if err != nil {
			return fmt.Errorf("读取CLIENT SETNAME响应失败: %w", err)
		}

		response := string(buffer[:n])
		if !containsOK(response) {
			return fmt.Errorf("CLIENT SETNAME命令失败: %s", response)
		}
	}

	return nil
}

// Cleanup 清理过期和空闲连接
func (pool *ConnectionPool) Cleanup() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	var activeConnections []*PooledConnection
	closedCount := 0

	for _, conn := range pool.connections {
		shouldClose := false

		conn.mu.RLock()
		// 检查连接是否过期
		if pool.maxLifetime > 0 && time.Since(conn.CreatedAt) > pool.maxLifetime {
			shouldClose = true
		}
		// 检查连接是否空闲超时
		if !shouldClose && !conn.InUse && conn.StickyClient == "" &&
			pool.idleTimeout > 0 && time.Since(conn.LastUsed) > pool.idleTimeout {
			shouldClose = true
		}
		conn.mu.RUnlock()

		if shouldClose {
			conn.Conn.Close()
			closedCount++

			pool.stats.mu.Lock()
			pool.stats.TotalConnections--
			pool.stats.ConnectionsClosed++
			if conn.InUse {
				pool.stats.ActiveConnections--
			} else {
				pool.stats.IdleConnections--
			}
			pool.stats.mu.Unlock()
		} else {
			activeConnections = append(activeConnections, conn)
		}
	}

	pool.connections = activeConnections

	if closedCount > 0 {
		logger.Info(fmt.Sprintf("连接池清理完成: 关闭 %d 个过期/空闲连接, 剩余 %d 个连接",
			closedCount, len(pool.connections)))
	}
}

// Close 关闭连接池
func (pool *ConnectionPool) Close() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for _, conn := range pool.connections {
		conn.Conn.Close()
	}
	pool.connections = pool.connections[:0]

	logger.Info(fmt.Sprintf("连接池已关闭: 上下文=%s", pool.contextHash))
}

// GetStats 获取连接池统计信息
func (pool *ConnectionPool) GetStats() ConnectionPoolStats {
	pool.stats.mu.RLock()
	defer pool.stats.mu.RUnlock()

	// 创建副本以避免锁值复制
	return ConnectionPoolStats{
		TotalConnections:   pool.stats.TotalConnections,
		ActiveConnections:  pool.stats.ActiveConnections,
		IdleConnections:    pool.stats.IdleConnections,
		ConnectionsCreated: pool.stats.ConnectionsCreated,
		ConnectionsClosed:  pool.stats.ConnectionsClosed,
		GetRequests:        pool.stats.GetRequests,
		GetSuccesses:       pool.stats.GetSuccesses,
	}
}

// Size 返回连接池当前大小
func (pool *ConnectionPool) Size() int {
	pool.mu.RLock()
	defer pool.mu.RUnlock()
	return len(pool.connections)
}

// ActiveCount 返回活跃连接数
func (pool *ConnectionPool) ActiveCount() int64 {
	pool.stats.mu.RLock()
	defer pool.stats.mu.RUnlock()
	return pool.stats.ActiveConnections
}

// IdleCount 返回空闲连接数
func (pool *ConnectionPool) IdleCount() int64 {
	pool.stats.mu.RLock()
	defer pool.stats.mu.RUnlock()
	return pool.stats.IdleConnections
}

// 辅助函数
func containsOK(response string) bool {
	return len(response) >= 3 && response[:3] == "+OK"
}

// PoolManager 连接池管理器
type PoolManager struct {
	pools           map[string]*ConnectionPool // contextHash -> pool
	classifier      *CommandClassifier         // 命令分类器
	redisAddr       string                     // Redis地址
	redisPassword   string                     // Redis密码
	maxSize         int                        // 每个池的最大连接数
	minIdle         int                        // 每个池的最小空闲连接数
	maxIdle         int                        // 每个池的最大空闲连接数
	idleTimeout     time.Duration              // 空闲超时时间
	maxLifetime     time.Duration              // 连接最大生命周期
	cleanupInterval time.Duration              // 清理间隔
	mu              sync.RWMutex               // 保护连接池映射
	ctx             context.Context            // 上下文
	cancel          context.CancelFunc         // 取消函数
	stats           *PoolManagerStats          // 管理器统计
}

// PoolManagerStats 池管理器统计
type PoolManagerStats struct {
	PoolCount    int       `json:"pool_count"`
	TotalPools   int64     `json:"total_pools"`
	PoolsCreated int64     `json:"pools_created"`
	PoolsClosed  int64     `json:"pools_closed"`
	LastCleanup  time.Time `json:"last_cleanup"`
	mu           sync.RWMutex
}

// NewPoolManager 创建连接池管理器
func NewPoolManager(redisAddr, redisPassword string, maxSize, minIdle, maxIdle int,
	idleTimeout, maxLifetime, cleanupInterval time.Duration) *PoolManager {

	ctx, cancel := context.WithCancel(context.Background())

	pm := &PoolManager{
		pools:           make(map[string]*ConnectionPool),
		classifier:      NewCommandClassifier(),
		redisAddr:       redisAddr,
		redisPassword:   redisPassword,
		maxSize:         maxSize,
		minIdle:         minIdle,
		maxIdle:         maxIdle,
		idleTimeout:     idleTimeout,
		maxLifetime:     maxLifetime,
		cleanupInterval: cleanupInterval,
		ctx:             ctx,
		cancel:          cancel,
		stats:           &PoolManagerStats{},
	}

	// 启动清理协程
	go pm.cleanupLoop()

	return pm
}

// GetConnection 获取连接
func (pm *PoolManager) GetConnection(connCtx *ConnectionContext, clientID string) (*PooledConnection, error) {
	contextHash := connCtx.Hash()

	pm.mu.RLock()
	pool, exists := pm.pools[contextHash]
	pm.mu.RUnlock()

	if !exists {
		// 创建新的连接池
		pm.mu.Lock()
		// 双重检查
		if pool, exists = pm.pools[contextHash]; !exists {
			pool = NewConnectionPool(connCtx, pm.redisAddr, pm.redisPassword,
				pm.maxSize, pm.minIdle, pm.maxIdle, pm.idleTimeout, pm.maxLifetime)
			pm.pools[contextHash] = pool

			pm.stats.mu.Lock()
			pm.stats.PoolCount++
			pm.stats.TotalPools++
			pm.stats.PoolsCreated++
			pm.stats.mu.Unlock()

			logger.Info(fmt.Sprintf("创建新连接池: 上下文=%s, 客户端=%s", contextHash, clientID))
		}
		pm.mu.Unlock()
	}

	return pool.Get(clientID)
}

// ReturnConnection 归还连接
func (pm *PoolManager) ReturnConnection(conn *PooledConnection, clientID string) {
	if conn == nil {
		return
	}

	contextHash := conn.Context.Hash()

	pm.mu.RLock()
	pool, exists := pm.pools[contextHash]
	pm.mu.RUnlock()

	if exists {
		pool.Return(conn, clientID)
	}
}

// ReleaseConnection 释放粘性连接
func (pm *PoolManager) ReleaseConnection(conn *PooledConnection, clientID string) {
	if conn == nil {
		return
	}

	contextHash := conn.Context.Hash()

	pm.mu.RLock()
	pool, exists := pm.pools[contextHash]
	pm.mu.RUnlock()

	if exists {
		pool.Release(conn, clientID)
	}
}

// cleanupLoop 清理循环
func (pm *PoolManager) cleanupLoop() {
	ticker := time.NewTicker(pm.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.cleanup()
		}
	}
}

// cleanup 清理过期连接和空的连接池
func (pm *PoolManager) cleanup() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	emptyPools := make([]string, 0)

	for contextHash, pool := range pm.pools {
		pool.Cleanup()

		// 如果池为空且没有活跃连接，可以考虑删除
		if pool.Size() == 0 && pool.ActiveCount() == 0 {
			emptyPools = append(emptyPools, contextHash)
		}
	}

	// 删除空的连接池
	for _, contextHash := range emptyPools {
		pool := pm.pools[contextHash]
		pool.Close()
		delete(pm.pools, contextHash)

		pm.stats.mu.Lock()
		pm.stats.PoolCount--
		pm.stats.PoolsClosed++
		pm.stats.mu.Unlock()

		logger.Debug(fmt.Sprintf("删除空连接池: 上下文=%s", contextHash))
	}

	pm.stats.mu.Lock()
	pm.stats.LastCleanup = time.Now()
	pm.stats.mu.Unlock()

	if len(emptyPools) > 0 {
		logger.Info(fmt.Sprintf("连接池管理器清理完成: 删除 %d 个空池, 剩余 %d 个池",
			len(emptyPools), len(pm.pools)))
	}
}

// Close 关闭池管理器
func (pm *PoolManager) Close() {
	logger.Info("关闭连接池管理器...")

	pm.cancel()

	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, pool := range pm.pools {
		pool.Close()
	}
	pm.pools = make(map[string]*ConnectionPool)

	logger.Info("✅ 连接池管理器已关闭")
}

// GetStats 获取管理器统计信息
func (pm *PoolManager) GetStats() PoolManagerStats {
	pm.stats.mu.RLock()
	defer pm.stats.mu.RUnlock()

	// 创建副本以避免锁值复制
	return PoolManagerStats{
		PoolCount:    pm.stats.PoolCount,
		TotalPools:   pm.stats.TotalPools,
		PoolsCreated: pm.stats.PoolsCreated,
		PoolsClosed:  pm.stats.PoolsClosed,
		LastCleanup:  pm.stats.LastCleanup,
	}
}

// GetClassifier 获取命令分类器
func (pm *PoolManager) GetClassifier() *CommandClassifier {
	return pm.classifier
}
