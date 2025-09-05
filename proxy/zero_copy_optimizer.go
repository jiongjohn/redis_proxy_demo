package proxy

import (
	"bufio"
	"net"
	"sync/atomic"
	"time"
)

// ZeroCopyOptimizer 零拷贝连接优化器
// 专门用于高频GET/SET操作，最小化连接池开销
type ZeroCopyOptimizer struct {
	// 预分配的长连接数组（避免map查找开销）
	connections []*DirectConnection
	// 轮询索引（避免锁竞争）
	roundRobin uint64
	// 连接数量
	connCount int
	// Redis地址
	redisAddr string
	// 统计信息（仅使用原子操作）
	totalRequests uint64
	totalErrors   uint64
	// 控制
	closed int32
}

// DirectConnection 直连连接（最小化结构体大小）
type DirectConnection struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
	// 使用原子操作避免锁
	inUse    int32
	lastUsed int64 // Unix纳秒时间戳
	// 预分配的缓冲区
	readBuf  []byte
	writeBuf []byte
}

// ZeroCopyConfig 零拷贝优化器配置
type ZeroCopyConfig struct {
	RedisAddr    string
	ConnCount    int // 预分配连接数
	BufferSize   int // 缓冲区大小
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewZeroCopyOptimizer 创建零拷贝优化器
func NewZeroCopyOptimizer(config ZeroCopyConfig) (*ZeroCopyOptimizer, error) {
	optimizer := &ZeroCopyOptimizer{
		connections: make([]*DirectConnection, config.ConnCount),
		connCount:   config.ConnCount,
		redisAddr:   config.RedisAddr,
	}

	// 预创建所有连接
	for i := 0; i < config.ConnCount; i++ {
		conn, err := optimizer.createDirectConnection(config.BufferSize)
		if err != nil {
			// 清理已创建的连接
			for j := 0; j < i; j++ {
				if optimizer.connections[j] != nil {
					optimizer.connections[j].conn.Close()
				}
			}
			return nil, err
		}
		optimizer.connections[i] = conn
	}

	return optimizer, nil
}

// createDirectConnection 创建直连连接
func (z *ZeroCopyOptimizer) createDirectConnection(bufferSize int) (*DirectConnection, error) {
	conn, err := net.DialTimeout("tcp", z.redisAddr, 2*time.Second)
	if err != nil {
		return nil, err
	}

	// TCP优化
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	return &DirectConnection{
		conn:     conn,
		reader:   bufio.NewReaderSize(conn, bufferSize),
		writer:   bufio.NewWriterSize(conn, bufferSize),
		readBuf:  make([]byte, bufferSize),
		writeBuf: make([]byte, 0, bufferSize),
		lastUsed: time.Now().UnixNano(),
	}, nil
}

// getConnection 获取可用连接（使用轮询避免锁竞争）
func (z *ZeroCopyOptimizer) getConnection() *DirectConnection {
	if atomic.LoadInt32(&z.closed) != 0 {
		return nil
	}

	// 使用原子操作实现无锁轮询
	for attempts := 0; attempts < z.connCount*2; attempts++ {
		index := atomic.AddUint64(&z.roundRobin, 1) % uint64(z.connCount)
		conn := z.connections[index]

		// 尝试获取连接（CAS操作）
		if atomic.CompareAndSwapInt32(&conn.inUse, 0, 1) {
			atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
			return conn
		}
	}

	// 如果所有连接都在使用，等待一个短暂时间后重试
	time.Sleep(100 * time.Microsecond)

	// 再次尝试
	for i := 0; i < z.connCount; i++ {
		conn := z.connections[i]
		if atomic.CompareAndSwapInt32(&conn.inUse, 0, 1) {
			atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
			return conn
		}
	}

	return nil
}

// returnConnection 归还连接（仅原子操作）
func (z *ZeroCopyOptimizer) returnConnection(conn *DirectConnection) {
	if conn != nil {
		atomic.StoreInt32(&conn.inUse, 0)
	}
}

// HandleGetCommandZeroCopy 零拷贝处理GET命令
func (z *ZeroCopyOptimizer) HandleGetCommandZeroCopy(clientConn net.Conn, key string) error {
	atomic.AddUint64(&z.totalRequests, 1)

	// 获取连接
	conn := z.getConnection()
	if conn == nil {
		atomic.AddUint64(&z.totalErrors, 1)
		return ErrNoAvailableConnection
	}
	defer z.returnConnection(conn)

	// 构建GET命令（直接写入缓冲区，避免字符串拼接）
	conn.writeBuf = conn.writeBuf[:0] // 重置缓冲区
	conn.writeBuf = append(conn.writeBuf, "*2\r\n$3\r\nGET\r\n$"...)
	conn.writeBuf = appendInt(conn.writeBuf, len(key))
	conn.writeBuf = append(conn.writeBuf, "\r\n"...)
	conn.writeBuf = append(conn.writeBuf, key...)
	conn.writeBuf = append(conn.writeBuf, "\r\n"...)

	// 发送命令
	_, err := conn.conn.Write(conn.writeBuf)
	if err != nil {
		atomic.AddUint64(&z.totalErrors, 1)
		return err
	}

	// 设置读取超时
	conn.conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// 零拷贝转发响应
	return z.forwardResponseZeroCopy(conn, clientConn)
}

// HandleSetCommandZeroCopy 零拷贝处理SET命令
func (z *ZeroCopyOptimizer) HandleSetCommandZeroCopy(clientConn net.Conn, key, value string) error {
	atomic.AddUint64(&z.totalRequests, 1)

	// 获取连接
	conn := z.getConnection()
	if conn == nil {
		atomic.AddUint64(&z.totalErrors, 1)
		return ErrNoAvailableConnection
	}
	defer z.returnConnection(conn)

	// 构建SET命令
	conn.writeBuf = conn.writeBuf[:0]
	conn.writeBuf = append(conn.writeBuf, "*3\r\n$3\r\nSET\r\n$"...)
	conn.writeBuf = appendInt(conn.writeBuf, len(key))
	conn.writeBuf = append(conn.writeBuf, "\r\n"...)
	conn.writeBuf = append(conn.writeBuf, key...)
	conn.writeBuf = append(conn.writeBuf, "\r\n$"...)
	conn.writeBuf = appendInt(conn.writeBuf, len(value))
	conn.writeBuf = append(conn.writeBuf, "\r\n"...)
	conn.writeBuf = append(conn.writeBuf, value...)
	conn.writeBuf = append(conn.writeBuf, "\r\n"...)

	// 发送命令
	_, err := conn.conn.Write(conn.writeBuf)
	if err != nil {
		atomic.AddUint64(&z.totalErrors, 1)
		return err
	}

	// 设置读取超时
	conn.conn.SetReadDeadline(time.Now().Add(2 * time.Second))

	// 零拷贝转发响应
	return z.forwardResponseZeroCopy(conn, clientConn)
}

// forwardResponseZeroCopy 零拷贝转发响应
func (z *ZeroCopyOptimizer) forwardResponseZeroCopy(redisConn *DirectConnection, clientConn net.Conn) error {
	// 使用预分配的缓冲区
	buffer := redisConn.readBuf

	for {
		n, err := redisConn.conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() && n == 0 {
				break // 读取超时，响应完成
			}
			return err
		}

		if n > 0 {
			_, err = clientConn.Write(buffer[:n])
			if err != nil {
				return err
			}

			// 检查是否是完整的Redis响应（简单检查）
			if n < len(buffer) {
				break // 可能是最后一个包
			}
		} else {
			break
		}
	}

	return nil
}

// appendInt 高效的整数转字符串追加（避免strconv.Itoa分配）
func appendInt(buf []byte, i int) []byte {
	if i == 0 {
		return append(buf, '0')
	}

	// 处理负数
	if i < 0 {
		buf = append(buf, '-')
		i = -i
	}

	// 计算位数
	digits := 0
	temp := i
	for temp > 0 {
		digits++
		temp /= 10
	}

	// 预分配空间
	start := len(buf)
	buf = buf[:start+digits]

	// 从后往前填充数字
	for j := digits - 1; j >= 0; j-- {
		buf[start+j] = byte('0' + i%10)
		i /= 10
	}

	return buf
}

// GetStats 获取统计信息
func (z *ZeroCopyOptimizer) GetStats() (totalRequests, totalErrors uint64, activeConnections int) {
	totalRequests = atomic.LoadUint64(&z.totalRequests)
	totalErrors = atomic.LoadUint64(&z.totalErrors)

	// 计算活跃连接数
	for _, conn := range z.connections {
		if atomic.LoadInt32(&conn.inUse) == 1 {
			activeConnections++
		}
	}

	return
}

// Close 关闭优化器
func (z *ZeroCopyOptimizer) Close() error {
	atomic.StoreInt32(&z.closed, 1)

	// 等待所有连接释放
	for {
		allFree := true
		for _, conn := range z.connections {
			if atomic.LoadInt32(&conn.inUse) == 1 {
				allFree = false
				break
			}
		}
		if allFree {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// 关闭所有连接
	for _, conn := range z.connections {
		if conn != nil && conn.conn != nil {
			conn.conn.Close()
		}
	}

	return nil
}

var (
	ErrNoAvailableConnection = &ConnectionError{"no available connection"}
)

type ConnectionError struct {
	msg string
}

func (e *ConnectionError) Error() string {
	return e.msg
}
