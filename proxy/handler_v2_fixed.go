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

// ProxyHandlerV2Fixed 修复版本 - 混合使用proto包和inline命令处理
type ProxyHandlerV2Fixed struct {
	RedisAddr string
}

// Handle 处理客户端连接 - 混合解析策略
func (h *ProxyHandlerV2Fixed) Handle(clientConn net.Conn) error {
	defer func() {
		if err := clientConn.Close(); err != nil {
			log.Printf("[ERROR] Failed to close client connection: %v", err)
		}
	}()

	log.Printf("[INFO][handler_v2_fixed] 📥 New connection from %s", clientConn.RemoteAddr())

	// 创建bufio读取器用于前瞻检查
	clientReader := bufio.NewReader(clientConn)
	clientWriter := bufio.NewWriter(clientConn)
	writer := proto.NewWriter(clientWriter)

	for {
		// 前瞻检查命令类型
		firstByte, err := clientReader.Peek(1)
		if err != nil {
			if err.Error() == "EOF" {
				log.Printf("[INFO][handler_v2_fixed] 🔌 Connection closed by client %s", clientConn.RemoteAddr())
				return nil
			}
			log.Printf("[ERROR][handler_v2_fixed] Failed to peek: %v", err)
			return err
		}

		var args []string

		if firstByte[0] == '*' {
			// RESP数组命令 - 使用go-redis Reader
			protoReader := proto.NewReader(clientReader)
			cmd, err := protoReader.ReadReply()
			if err != nil {
				log.Printf("[ERROR][handler_v2_fixed] Failed to read RESP command: %v", err)
				return err
			}

			args, err = h.convertToArgs(cmd)
			if err != nil {
				log.Printf("[ERROR][handler_v2_fixed] Failed to convert RESP command: %v", err)
				return h.sendError(writer, clientWriter, fmt.Sprintf("ERR invalid command format: %v", err))
			}
		} else {
			// Inline命令 - 使用自定义解析
			line, err := clientReader.ReadBytes('\n')
			if err != nil {
				log.Printf("[ERROR][handler_v2_fixed] Failed to read inline command: %v", err)
				return err
			}

			// 移除\r\n
			cmdLine := strings.TrimSpace(string(line))
			if cmdLine == "" {
				continue
			}

			args, err = h.parseInlineCommand(cmdLine)
			if err != nil {
				log.Printf("[ERROR][handler_v2_fixed] Failed to parse inline command: %v", err)
				return h.sendError(writer, clientWriter, fmt.Sprintf("ERR invalid inline command: %v", err))
			}
		}

		log.Printf("[DEBUG][handler_v2_fixed] 📝 Parsed command: %v", args)

		// 转发命令到Redis
		result, err := h.forwardToRedis(args)
		if err != nil {
			log.Printf("[ERROR][handler_v2_fixed] Failed to forward command: %v", err)
			return h.sendError(writer, clientWriter, fmt.Sprintf("ERR %v", err))
		}

		// 使用go-redis Writer发送响应
		if err := writer.WriteArg(result); err != nil {
			log.Printf("[ERROR][handler_v2_fixed] Failed to write response: %v", err)
			return err
		}

		// 确保响应被发送到客户端
		if err := clientWriter.Flush(); err != nil {
			log.Printf("[ERROR][handler_v2_fixed] Failed to flush response: %v", err)
			return err
		}
	}
}

// parseInlineCommand 解析inline命令 (如: SET key value)
func (h *ProxyHandlerV2Fixed) parseInlineCommand(line string) ([]string, error) {
	// 简化版本的引号解析 (复用之前验证的逻辑)
	return h.parseQuotedArgs(line), nil
}

// parseQuotedArgs 解析带引号的参数 (简化版本)
func (h *ProxyHandlerV2Fixed) parseQuotedArgs(input string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	i := 0
	for i < len(input) {
		char := input[i]

		switch char {
		case '\\':
			// Handle escape sequences
			if i+1 < len(input) {
				nextChar := input[i+1]
				if inQuote {
					switch nextChar {
					case '\'':
						if quoteChar == '\'' {
							current.WriteByte('\'')
							i++ // Skip the next character
						} else {
							current.WriteByte(char)
						}
					case '"':
						if quoteChar == '"' {
							current.WriteByte('"')
							i++ // Skip the next character
						} else {
							current.WriteByte(char)
						}
					case '\\':
						current.WriteByte('\\')
						i++ // Skip the next character
					default:
						current.WriteByte(char)
					}
				} else {
					current.WriteByte(char)
				}
			} else {
				current.WriteByte(char)
			}

		case '\'', '"':
			if !inQuote {
				inQuote = true
				quoteChar = char
			} else if char == quoteChar {
				inQuote = false
				quoteChar = 0
			} else {
				current.WriteByte(char)
			}

		case ' ', '\t':
			if inQuote {
				current.WriteByte(char)
			} else {
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
				// Skip multiple spaces
				for i+1 < len(input) && (input[i+1] == ' ' || input[i+1] == '\t') {
					i++
				}
			}

		default:
			current.WriteByte(char)
		}

		i++
	}

	// Add final argument if exists
	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}

// convertToArgs 将go-redis解析的命令转换为字符串数组
func (h *ProxyHandlerV2Fixed) convertToArgs(cmd interface{}) ([]string, error) {
	switch v := cmd.(type) {
	case []interface{}:
		args := make([]string, len(v))
		for i, arg := range v {
			args[i] = fmt.Sprintf("%v", arg)
		}
		return args, nil
	default:
		return nil, fmt.Errorf("unexpected command format: %T", v)
	}
}

// forwardToRedis 转发命令到Redis后端
func (h *ProxyHandlerV2Fixed) forwardToRedis(args []string) (interface{}, error) {
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
func (h *ProxyHandlerV2Fixed) sendError(writer *proto.Writer, clientWriter *bufio.Writer, message string) error {
	if err := writer.WriteArg(fmt.Sprintf("-ERR %s", message)); err != nil {
		return err
	}
	return clientWriter.Flush()
}
