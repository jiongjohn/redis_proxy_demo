package pool

import (
	"fmt"
	"net"
	"redis-proxy-demo/lib/logger"
	"strings"
	"time"
)

type RedisConnConfig struct {
	Addr     string
	Password string
}

func CreateConnection(redisConfig *RedisConnConfig) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", redisConfig.Addr, 5*time.Second)
	if err != nil {
		return nil, err
	}

	// TCP优化
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// Redis认证
	if redisConfig.Password != "" {
		err = authenticateConnection(conn, redisConfig)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("redis认证失败: %w", err)
		}
		logger.Debug("Redis连接认证成功")
	}

	return conn, nil
}

// authenticateConnection 对Redis连接进行认证
func authenticateConnection(conn net.Conn, redisConfig *RedisConnConfig) error {
	// 构建RESP格式的AUTH命令
	authCmd := fmt.Sprintf("*2\r\n$4\r\nAUTH\r\n$%d\r\n%s\r\n",
		len(redisConfig.Password), redisConfig.Password)

	// 发送AUTH命令
	_, err := conn.Write([]byte(authCmd))
	if err != nil {
		return fmt.Errorf("发送AUTH命令失败: %w", err)
	}

	// 设置读取超时
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	defer conn.SetReadDeadline(time.Time{})

	// 读取AUTH响应
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return fmt.Errorf("读取AUTH响应失败: %w", err)
	}

	response := string(buffer[:n])
	// 检查是否认证成功（Redis会返回+OK）
	if !strings.Contains(response, "+OK") {
		return fmt.Errorf("redis认证失败，响应: %s", response)
	}

	return nil
}
