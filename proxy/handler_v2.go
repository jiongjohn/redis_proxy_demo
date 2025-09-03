package proxy

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"redis-proxy-demo/redis/proto"
)

// ProxyHandlerV2 基于go-redis proto包的新一代处理器
type ProxyHandlerV2 struct {
	RedisAddr string
}

// Handle 处理客户端连接 - 使用go-redis proto包
func (h *ProxyHandlerV2) Handle(clientConn net.Conn) error {
	defer func() {
		if err := clientConn.Close(); err != nil {
			log.Printf("[ERROR] Failed to close client connection: %v", err)
		}
	}()

	log.Printf("[INFO][handler_v2] 📥 New connection from %s", clientConn.RemoteAddr())

	// 创建RESP协议读取器和写入器
	reader := proto.NewReader(clientConn)
	clientWriter := bufio.NewWriter(clientConn)
	writer := proto.NewWriter(clientWriter)

	for {
		// 使用go-redis Reader解析命令
		cmd, err := reader.ReadReply()
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("[INFO][handler_v2] 🔌 Connection closed by client %s", clientConn.RemoteAddr())
				return nil
			}
			log.Printf("[ERROR][handler_v2] Failed to read command: %v", err)
			return err
		}

		// 转换命令为字符串数组格式
		args, err := h.convertToArgs(cmd)
		if err != nil {
			log.Printf("[ERROR][handler_v2] Failed to convert command: %v", err)
			return h.sendError(writer, fmt.Sprintf("ERR invalid command format: %v", err))
		}

		log.Printf("[DEBUG][handler_v2] 📝 Parsed command: %v", args)

		// 转发命令到Redis
		result, err := h.forwardToRedis(args)
		if err != nil {
			log.Printf("[ERROR][handler_v2] Failed to forward command: %v", err)
			return h.sendError(writer, fmt.Sprintf("ERR %v", err))
		}

		// 使用go-redis Writer发送响应
		if err := writer.WriteArg(result); err != nil {
			log.Printf("[ERROR][handler_v2] Failed to write response: %v", err)
			return err
		}

		// 确保响应被发送到客户端
		if err := clientWriter.Flush(); err != nil {
			log.Printf("[ERROR][handler_v2] Failed to flush response: %v", err)
			return err
		}
	}
}

// convertToArgs 将go-redis解析的命令转换为字符串数组
func (h *ProxyHandlerV2) convertToArgs(cmd interface{}) ([]string, error) {
	switch v := cmd.(type) {
	case []interface{}:
		// 标准的RESP数组命令 (如: *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n)
		args := make([]string, len(v))
		for i, arg := range v {
			args[i] = fmt.Sprintf("%v", arg)
		}
		return args, nil
	case string:
		// Inline命令处理 (如: SET key value)
		parts := strings.Fields(v)
		if len(parts) == 0 {
			return nil, fmt.Errorf("empty command")
		}
		return parts, nil
	default:
		return nil, fmt.Errorf("unexpected command format: %T", v)
	}
}

// forwardToRedis 转发命令到Redis后端
func (h *ProxyHandlerV2) forwardToRedis(args []string) (interface{}, error) {
	// 连接到Redis
	redisConn, err := net.DialTimeout("tcp", h.RedisAddr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to redis: %v", err)
	}
	defer redisConn.Close()

	// 创建Redis连接的读写器
	redisBufferWriter := bufio.NewWriter(redisConn)
	redisWriter := proto.NewWriter(redisBufferWriter)
	redisReader := proto.NewReader(redisConn)

	// 将args转换为interface{}数组以便WriteArgs处理
	cmdArgs := make([]interface{}, len(args))
	for i, arg := range args {
		cmdArgs[i] = arg
	}

	// 发送命令到Redis
	if err := redisWriter.WriteArgs(cmdArgs); err != nil {
		return nil, fmt.Errorf("failed to write to redis: %v", err)
	}

	// 确保数据被发送
	if err := redisBufferWriter.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush to redis: %v", err)
	}

	// 读取Redis响应
	response, err := redisReader.ReadReply()
	if err != nil {
		return nil, fmt.Errorf("failed to read redis response: %v", err)
	}

	return response, nil
}

// sendError 发送错误响应
func (h *ProxyHandlerV2) sendError(writer *proto.Writer, message string) error {
	return writer.WriteArg(fmt.Sprintf("-ERR %s", message))
}
