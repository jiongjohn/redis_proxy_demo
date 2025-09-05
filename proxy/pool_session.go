package proxy

import (
	"net"
	"sync"
	"time"
)

// PoolClientSession 表示客户端会话状态
type PoolClientSession struct {
	ID               string               // 会话ID
	ClientConn       net.Conn             // 客户端连接
	Context          *ConnectionContext   // 连接上下文
	State            RedisConnectionState // 当前状态
	CurrentRedisConn *PooledConnection    // 当前使用的Redis连接
	CreatedAt        time.Time            // 创建时间
	LastActivity     time.Time            // 最后活动时间（用于连接保持）
	LastCommand      string               // 最后执行的命令
	CommandCount     int64                // 命令计数
	mu               sync.RWMutex         // 保护会话状态
}

// UpdateState 更新会话状态
func (s *PoolClientSession) UpdateState(newState RedisConnectionState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = newState
}

// GetState 获取会话状态
func (s *PoolClientSession) GetState() RedisConnectionState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.State
}

// SetRedisConnection 设置Redis连接
func (s *PoolClientSession) SetRedisConnection(conn *PooledConnection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CurrentRedisConn = conn
}

// GetRedisConnection 获取Redis连接
func (s *PoolClientSession) GetRedisConnection() *PooledConnection {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.CurrentRedisConn
}

// IncrementCommandCount 增加命令计数
func (s *PoolClientSession) IncrementCommandCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CommandCount++
}

// SetLastCommand 设置最后执行的命令
func (s *PoolClientSession) SetLastCommand(command string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastCommand = command
}

// GetCommandCount 获取命令计数
func (s *PoolClientSession) GetCommandCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.CommandCount
}

// IsExpired 检查会话是否已过期
func (s *PoolClientSession) IsExpired(timeout time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return time.Since(s.LastActivity) > timeout
}

// ClearRedisConnection 清除Redis连接引用
func (s *PoolClientSession) ClearRedisConnection() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CurrentRedisConn = nil
}

// UpdateLastActivity 更新最后活动时间
func (s *PoolClientSession) UpdateLastActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActivity = time.Now()
}

// GetLastActivity 获取最后活动时间
func (s *PoolClientSession) GetLastActivity() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LastActivity
}

// IsConnectionIdle 检查连接是否空闲超时
func (s *PoolClientSession) IsConnectionIdle(holdTime time.Duration) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// 只有当状态为Normal且有非粘性连接时才检查空闲时间
	if s.State != StateNormal || s.CurrentRedisConn == nil {
		return false
	}
	// 检查是否是粘性连接，粘性连接不适用此逻辑
	if s.CurrentRedisConn.StickyClient == s.ID {
		return false
	}
	return time.Since(s.LastActivity) > holdTime
}
