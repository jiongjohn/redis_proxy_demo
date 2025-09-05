package proxy

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

var (
	ErrPoolClosed = errors.New("connection pool is closed")
)

// PingOptimizer PING命令优化器
type PingOptimizer struct {
	// 预分配的PING响应
	pongResponse []byte
	// 统计信息（使用原子操作避免锁）
	pingCount int64
	// 连接池（专门用于PING命令的轻量级连接池）
	pingConnPool *PingConnectionPool
}

// PingConnectionPool 专门用于PING命令的轻量级连接池
type PingConnectionPool struct {
	connections chan net.Conn
	redisAddr   string
	mu          sync.RWMutex
	closed      bool
}

// NewPingOptimizer 创建PING优化器
func NewPingOptimizer(redisAddr string, poolSize int) *PingOptimizer {
	optimizer := &PingOptimizer{
		pongResponse: []byte("+PONG\r\n"),
		pingConnPool: NewPingConnectionPool(redisAddr, poolSize),
	}
	return optimizer
}

// NewPingConnectionPool 创建PING连接池
func NewPingConnectionPool(redisAddr string, poolSize int) *PingConnectionPool {
	pool := &PingConnectionPool{
		connections: make(chan net.Conn, poolSize),
		redisAddr:   redisAddr,
	}

	// 预热连接池
	for i := 0; i < poolSize/2; i++ {
		if conn, err := net.Dial("tcp", redisAddr); err == nil {
			select {
			case pool.connections <- conn:
			default:
				conn.Close()
			}
		}
	}

	return pool
}

// GetConnection 获取连接
func (p *PingConnectionPool) GetConnection() (net.Conn, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, ErrPoolClosed
	}
	p.mu.RUnlock()

	select {
	case conn := <-p.connections:
		return conn, nil
	default:
		// 池中没有连接，创建新连接
		return net.Dial("tcp", p.redisAddr)
	}
}

// ReturnConnection 归还连接
func (p *PingConnectionPool) ReturnConnection(conn net.Conn) {
	if conn == nil {
		return
	}

	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		conn.Close()
		return
	}
	p.mu.RUnlock()

	select {
	case p.connections <- conn:
	default:
		// 池已满，关闭连接
		conn.Close()
	}
}

// Close 关闭连接池
func (p *PingConnectionPool) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	p.mu.Unlock()

	close(p.connections)
	for conn := range p.connections {
		conn.Close()
	}
}

// HandlePingFast 快速处理PING命令
func (o *PingOptimizer) HandlePingFast(clientConn net.Conn) error {
	atomic.AddInt64(&o.pingCount, 1)

	// 直接返回PONG，不转发到Redis（适用于健康检查场景）
	_, err := clientConn.Write(o.pongResponse)
	return err
}

// HandlePingWithRedis 通过Redis处理PING命令（保持一致性）
func (o *PingOptimizer) HandlePingWithRedis(clientConn net.Conn) error {
	atomic.AddInt64(&o.pingCount, 1)

	// 从专用连接池获取连接
	redisConn, err := o.pingConnPool.GetConnection()
	if err != nil {
		return err
	}
	defer o.pingConnPool.ReturnConnection(redisConn)

	// 发送PING到Redis
	_, err = redisConn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		return err
	}

	// 读取响应并转发
	buffer := make([]byte, 64) // PONG响应很小，64字节足够
	n, err := redisConn.Read(buffer)
	if err != nil {
		return err
	}

	_, err = clientConn.Write(buffer[:n])
	return err
}

// GetPingCount 获取PING命令统计
func (o *PingOptimizer) GetPingCount() int64 {
	return atomic.LoadInt64(&o.pingCount)
}

// Close 关闭优化器
func (o *PingOptimizer) Close() {
	if o.pingConnPool != nil {
		o.pingConnPool.Close()
	}
}
