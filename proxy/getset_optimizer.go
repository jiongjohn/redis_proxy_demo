package proxy

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// GetSetOptimizer GET/SET命令优化器
type GetSetOptimizer struct {
	// 连接池 - 专门用于GET/SET的长连接池
	connectionPool *FastConnectionPool
	// 统计信息（原子操作）
	getCount int64
	setCount int64
	hitCount int64
	// 配置
	config GetSetOptimizerConfig
}

// GetSetOptimizerConfig 优化器配置
type GetSetOptimizerConfig struct {
	RedisAddr      string
	PoolSize       int
	MaxIdleTime    time.Duration
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	BufferSize     int
	EnablePipeline bool
	PipelineSize   int
}

// FastConnectionPool 快速连接池
type FastConnectionPool struct {
	connections chan *FastConnection
	redisAddr   string
	config      GetSetOptimizerConfig
	mu          sync.RWMutex
	closed      bool
	activeConns int64
}

// FastConnection 快速连接
type FastConnection struct {
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	lastUsed time.Time
	inUse    int32
	created  time.Time
}

// NewGetSetOptimizer 创建GET/SET优化器
func NewGetSetOptimizer(config GetSetOptimizerConfig) *GetSetOptimizer {
	optimizer := &GetSetOptimizer{
		config:         config,
		connectionPool: NewFastConnectionPool(config),
	}
	return optimizer
}

// NewFastConnectionPool 创建快速连接池
func NewFastConnectionPool(config GetSetOptimizerConfig) *FastConnectionPool {
	pool := &FastConnectionPool{
		connections: make(chan *FastConnection, config.PoolSize),
		redisAddr:   config.RedisAddr,
		config:      config,
	}

	// 预热连接池
	go pool.warmUp()

	// 启动清理协程
	go pool.cleanup()

	return pool
}

// warmUp 预热连接池
func (p *FastConnectionPool) warmUp() {
	// 创建初始连接
	for i := 0; i < p.config.PoolSize/2; i++ {
		conn, err := p.createConnection()
		if err != nil {
			continue
		}
		select {
		case p.connections <- conn:
			atomic.AddInt64(&p.activeConns, 1)
		default:
			conn.Close()
		}
	}
}

// createConnection 创建新连接
func (p *FastConnectionPool) createConnection() (*FastConnection, error) {
	conn, err := net.DialTimeout("tcp", p.redisAddr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	// 设置TCP选项优化
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	fastConn := &FastConnection{
		conn:     conn,
		reader:   bufio.NewReaderSize(conn, p.config.BufferSize),
		writer:   bufio.NewWriterSize(conn, p.config.BufferSize),
		created:  time.Now(),
		lastUsed: time.Now(),
	}

	return fastConn, nil
}

// GetConnection 获取连接
func (p *FastConnectionPool) GetConnection() (*FastConnection, error) {
	p.mu.RLock()
	if p.closed {
		p.mu.RUnlock()
		return nil, fmt.Errorf("connection pool is closed")
	}
	p.mu.RUnlock()

	select {
	case conn := <-p.connections:
		// 检查连接是否还有效
		if time.Since(conn.lastUsed) > p.config.MaxIdleTime {
			conn.Close()
			atomic.AddInt64(&p.activeConns, -1)
			return p.createConnection()
		}
		atomic.StoreInt32(&conn.inUse, 1)
		return conn, nil
	default:
		// 池中没有可用连接，创建新连接
		if atomic.LoadInt64(&p.activeConns) < int64(p.config.PoolSize) {
			conn, err := p.createConnection()
			if err == nil {
				atomic.AddInt64(&p.activeConns, 1)
				atomic.StoreInt32(&conn.inUse, 1)
			}
			return conn, err
		}
		// 等待连接归还
		select {
		case conn := <-p.connections:
			atomic.StoreInt32(&conn.inUse, 1)
			return conn, nil
		case <-time.After(100 * time.Millisecond):
			return nil, fmt.Errorf("connection pool timeout")
		}
	}
}

// ReturnConnection 归还连接
func (p *FastConnectionPool) ReturnConnection(conn *FastConnection) {
	if conn == nil {
		return
	}

	atomic.StoreInt32(&conn.inUse, 0)
	conn.lastUsed = time.Now()

	select {
	case p.connections <- conn:
		// 成功归还
	default:
		// 池满了，关闭连接
		conn.Close()
		atomic.AddInt64(&p.activeConns, -1)
	}
}

// Close 关闭连接
func (c *FastConnection) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// cleanup 清理过期连接
func (p *FastConnectionPool) cleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		p.mu.RLock()
		if p.closed {
			p.mu.RUnlock()
			return
		}
		p.mu.RUnlock()

		// 清理过期连接
		for {
			select {
			case conn := <-p.connections:
				if time.Since(conn.lastUsed) > p.config.MaxIdleTime {
					conn.Close()
					atomic.AddInt64(&p.activeConns, -1)
				} else {
					// 连接还有效，放回池中
					select {
					case p.connections <- conn:
					default:
						conn.Close()
						atomic.AddInt64(&p.activeConns, -1)
					}
				}
			default:
				goto cleanup_done
			}
		}
	cleanup_done:
	}
}

// HandleGetCommand 处理GET命令
func (o *GetSetOptimizer) HandleGetCommand(clientConn net.Conn, key string) error {
	atomic.AddInt64(&o.getCount, 1)

	// 获取连接
	conn, err := o.connectionPool.GetConnection()
	if err != nil {
		return err
	}
	defer o.connectionPool.ReturnConnection(conn)

	// 发送GET命令
	_, err = conn.writer.WriteString(fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key))
	if err != nil {
		return err
	}

	err = conn.writer.Flush()
	if err != nil {
		return err
	}

	// 设置读取超时
	conn.conn.SetReadDeadline(time.Now().Add(o.config.ReadTimeout))

	// 读取并转发响应
	return o.forwardResponse(conn, clientConn)
}

// HandleSetCommand 处理SET命令
func (o *GetSetOptimizer) HandleSetCommand(clientConn net.Conn, key, value string) error {
	atomic.AddInt64(&o.setCount, 1)

	// 获取连接
	conn, err := o.connectionPool.GetConnection()
	if err != nil {
		return err
	}
	defer o.connectionPool.ReturnConnection(conn)

	// 发送SET命令
	_, err = conn.writer.WriteString(fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key, len(value), value))
	if err != nil {
		return err
	}

	err = conn.writer.Flush()
	if err != nil {
		return err
	}

	// 设置读取超时
	conn.conn.SetReadDeadline(time.Now().Add(o.config.ReadTimeout))

	// 读取并转发响应
	return o.forwardResponse(conn, clientConn)
}

// forwardResponse 转发响应
func (o *GetSetOptimizer) forwardResponse(redisConn *FastConnection, clientConn net.Conn) error {
	buffer := make([]byte, o.config.BufferSize)

	for {
		n, err := redisConn.reader.Read(buffer)
		if err != nil {
			if n == 0 {
				break
			}
		}

		if n > 0 {
			_, writeErr := clientConn.Write(buffer[:n])
			if writeErr != nil {
				return writeErr
			}

			// 检查是否是完整的Redis响应
			if o.isCompleteResponse(buffer[:n]) {
				break
			}
		}

		if err != nil {
			break
		}
	}

	return nil
}

// isCompleteResponse 检查是否是完整的Redis响应
func (o *GetSetOptimizer) isCompleteResponse(data []byte) bool {
	if len(data) < 2 {
		return false
	}

	// 简单检查：以\r\n结尾的响应
	return data[len(data)-2] == '\r' && data[len(data)-1] == '\n'
}

// GetStats 获取统计信息
func (o *GetSetOptimizer) GetStats() (int64, int64, int64) {
	return atomic.LoadInt64(&o.getCount), atomic.LoadInt64(&o.setCount), atomic.LoadInt64(&o.hitCount)
}

// Close 关闭优化器
func (o *GetSetOptimizer) Close() {
	o.connectionPool.mu.Lock()
	o.connectionPool.closed = true
	o.connectionPool.mu.Unlock()

	// 关闭所有连接
	for {
		select {
		case conn := <-o.connectionPool.connections:
			conn.Close()
		default:
			return
		}
	}
}
