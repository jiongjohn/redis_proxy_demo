package proxy

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"redis-proxy-demo/lib/logger"
	"redis-proxy-demo/redis/proto"

	"github.com/redis/go-redis/v9"
)

// GoRedisHandlerV2Fixed Final enhanced go-redis handler using proto package for parsing only
type GoRedisHandlerV2Fixed struct {
	sessionManager *ClientSessionManager
	ctx            context.Context
}

// NewGoRedisHandlerV2Fixed creates a new fixed enhanced go-redis based handler with proto support
func NewGoRedisHandlerV2Fixed(config GoRedisConfig) *GoRedisHandlerV2Fixed {
	sessionManager := NewClientSessionManager(config)
	if sessionManager == nil {
		return nil
	}

	return &GoRedisHandlerV2Fixed{
		sessionManager: sessionManager,
		ctx:            context.Background(),
	}
}

// Handle handles a client connection with enhanced parsing and session state management
func (h *GoRedisHandlerV2Fixed) Handle(ctx context.Context, conn net.Conn) {
	defer func() {
		h.sessionManager.RemoveSession(conn)
		conn.Close()
	}()

	logger.Info(fmt.Sprintf("🔗 New client connection (go-redis v2 fixed mode): %s", conn.RemoteAddr()))

	// Create session for this client
	session := h.sessionManager.CreateSession(conn)
	if session == nil {
		logger.Error("Failed to create client session")
		return
	}

	// Create enhanced parser using proto package
	clientReader := bufio.NewReader(conn)

	for {
		// Parse command using mixed strategy (RESP + Inline)
		args, err := h.parseCommand(clientReader)
		if err != nil {
			errMsg := err.Error()
			// Handle normal connection closures gracefully
			if errMsg == "EOF" ||
				strings.Contains(errMsg, "connection closed") ||
				strings.Contains(errMsg, "use of closed network connection") {
				logger.Info("Connection closed: " + errMsg + " from " + conn.RemoteAddr().String())
			} else {
				logger.Error("Parse error: " + errMsg)
				h.sendError(conn, err)
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		// Handle the command
		err = h.handleCommand(session, args)
		if err != nil {
			logger.Error(fmt.Sprintf("Command handling error: %v", err))
			// Send error response to client
			h.sendError(conn, err)
		}
	}
}

// parseCommand parses command using mixed strategy (RESP arrays + inline commands)
func (h *GoRedisHandlerV2Fixed) parseCommand(reader *bufio.Reader) ([]string, error) {
	// Peek first byte to determine command type
	firstByte, err := reader.Peek(1)
	if err != nil {
		return nil, err
	}

	if firstByte[0] == '*' {
		// RESP array command - use proto Reader
		protoReader := proto.NewReader(reader)
		cmd, err := protoReader.ReadReply()
		if err != nil {
			return nil, fmt.Errorf("failed to read RESP command: %w", err)
		}

		return h.convertToArgs(cmd)
	} else {
		// Inline command - use custom parsing with full escape support
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, fmt.Errorf("failed to read inline command: %w", err)
		}

		// Remove \r\n
		cmdLine := strings.TrimSpace(string(line))
		if cmdLine == "" {
			return []string{}, nil
		}

		return h.parseInlineCommand(cmdLine), nil
	}
}

// convertToArgs converts go-redis parsed command to string slice
func (h *GoRedisHandlerV2Fixed) convertToArgs(cmd interface{}) ([]string, error) {
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

// parseInlineCommand parses inline commands with full escape character support
func (h *GoRedisHandlerV2Fixed) parseInlineCommand(line string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	i := 0
	for i < len(line) {
		char := line[i]

		switch char {
		case '\\':
			// Handle escape sequences
			if i+1 < len(line) {
				nextChar := line[i+1]
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
				for i+1 < len(line) && (line[i+1] == ' ' || line[i+1] == '\t') {
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

// handleCommand processes a single Redis command (same logic as original but using direct RESP responses)
func (h *GoRedisHandlerV2Fixed) handleCommand(session *ClientSession, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("empty command")
	}

	cmdName := strings.ToUpper(args[0])
	logger.Debug(fmt.Sprintf("📝 Session %s executing: %s %v", session.ID, cmdName, args[1:]))

	// Handle special stateful commands
	switch cmdName {
	//case "HELLO":
	//	return h.handleHello(session, args[1:])
	//case "CLIENT":
	//	return h.handleClient(session, args[1:])
	case "WATCH":
		return h.handleWatch(session, args[1:])
	case "UNWATCH":
		return h.handleUnwatch(session)
	case "MULTI":
		return h.handleMulti(session)
	case "EXEC":
		return h.handleExec(session)
	case "DISCARD":
		return h.handleDiscard(session)
	case "SELECT":
		return h.handleSelect(session, args)
	case "QUIT":
		return h.handleQuit(session)
	default:
		return h.handleRegularCommand(session, cmdName, args)
	}
}

// Enhanced command handlers with direct RESP responses (no proto wrapper)

// forceFlush forces the TCP connection to flush its buffers immediately
func (h *GoRedisHandlerV2Fixed) forceFlush(conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		// Force flush by disabling and re-enabling Nagle's algorithm
		tcpConn.SetNoDelay(false)
		tcpConn.SetNoDelay(true)
	}
}

func (h *GoRedisHandlerV2Fixed) handleWatch(session *ClientSession, keys []string) error {
	if len(keys) == 0 {
		return h.sendError(session.ClientConn, fmt.Errorf("wrong number of arguments for 'watch' command"))
	}

	err := session.StartWatch(keys)
	if err != nil {
		return h.sendError(session.ClientConn, err)
	}

	h.sessionManager.UpdateStats(false, true)
	return h.sendSimpleString(session.ClientConn, "OK")
}

func (h *GoRedisHandlerV2Fixed) handleUnwatch(session *ClientSession) error {
	session.ClearWatch()
	return h.sendSimpleString(session.ClientConn, "OK")
}

func (h *GoRedisHandlerV2Fixed) handleMulti(session *ClientSession) error {
	session.StartTransaction()
	return h.sendSimpleString(session.ClientConn, "OK")
}

func (h *GoRedisHandlerV2Fixed) handleExec(session *ClientSession) error {
	results, err := session.ExecuteTransaction(h.ctx)
	if err != nil {
		if err == redis.TxFailedErr {
			return h.sendNull(session.ClientConn)
		}
		return h.sendError(session.ClientConn, err)
	}

	h.sessionManager.UpdateStats(true, false)
	return h.sendArray(session.ClientConn, results)
}

func (h *GoRedisHandlerV2Fixed) handleDiscard(session *ClientSession) error {
	session.DiscardTransaction()
	return h.sendSimpleString(session.ClientConn, "OK")
}

func (h *GoRedisHandlerV2Fixed) handleSelect(session *ClientSession, args []string) error {
	if len(args) != 2 {
		return h.sendError(session.ClientConn, fmt.Errorf("wrong number of arguments for 'select' command"))
	}

	db, err := strconv.Atoi(args[1])
	if err != nil {
		return h.sendError(session.ClientConn, fmt.Errorf("invalid database number"))
	}

	err = session.SelectDatabase(db)
	if err != nil {
		return h.sendError(session.ClientConn, err)
	}

	return h.sendSimpleString(session.ClientConn, "OK")
}

func (h *GoRedisHandlerV2Fixed) handleQuit(session *ClientSession) error {
	err := h.sendSimpleString(session.ClientConn, "OK")
	if err == nil {
		session.ClientConn.Close()
	}
	return nil
}

func (h *GoRedisHandlerV2Fixed) handleHello(session *ClientSession, args []string) error {
	// HELLO command is a RESP3 protocol handshake
	// Should return a map with server information

	// Default to RESP3 if version specified as 3, otherwise RESP2
	respVersion := "2"
	if len(args) > 0 && args[0] == "3" {
		respVersion = "3"
	}

	// Create server information map for RESP3
	if respVersion == "3" {
		// Send RESP3 map response
		serverInfo := map[string]interface{}{
			"server":  "redis",
			"version": "6.2.0",
			"proto":   3,
			"id":      1,
			"mode":    "standalone",
			"role":    "master",
			"modules": []interface{}{},
		}
		return h.sendMap(session.ClientConn, serverInfo)
	} else {
		// For RESP2, return array with server info
		response := []interface{}{
			"server", "redis",
			"version", "6.2.0",
			"proto", 2,
			"id", 1,
			"mode", "standalone",
			"role", "master",
		}
		return h.sendArray(session.ClientConn, response)
	}
}

func (h *GoRedisHandlerV2Fixed) handleClient(session *ClientSession, args []string) error {
	// CLIENT command handles various client-related operations
	// For proxy compatibility, we handle common subcommands

	if len(args) == 0 {
		return h.sendError(session.ClientConn, errors.New("wrong number of arguments for CLIENT command"))
	}

	subCommand := strings.ToUpper(args[0])
	switch subCommand {
	case "SETINFO":
		// CLIENT SETINFO - client library sets information about itself
		// Just return OK, no need to track client info in proxy
		return h.sendSimpleString(session.ClientConn, "OK")
	case "GETNAME":
		// CLIENT GETNAME - return client name (null if not set)
		return h.sendNull(session.ClientConn)
	case "SETNAME":
		// CLIENT SETNAME - set client name
		// For proxy, just return OK without actually storing the name
		return h.sendSimpleString(session.ClientConn, "OK")
	case "ID":
		// CLIENT ID - return unique client ID
		// Use session ID as client ID
		sessionID := session.ID
		// Extract numeric part from session ID (e.g., "session_1" -> 1)
		if len(sessionID) > 8 && sessionID[:8] == "session_" {
			if id := sessionID[8:]; len(id) > 0 {
				return h.sendBulkString(session.ClientConn, id)
			}
		}
		return h.sendBulkString(session.ClientConn, "1")
	default:
		// For other CLIENT subcommands, forward to Redis
		return h.handleRegularCommand(session, "CLIENT", append([]string{"CLIENT"}, args...))
	}
}

func (h *GoRedisHandlerV2Fixed) handleRegularCommand(session *ClientSession, cmdName string, args []string) error {
	cmdArgs := make([]interface{}, len(args))
	for i, arg := range args {
		cmdArgs[i] = arg
	}

	cmd := redis.NewCmd(h.ctx, cmdArgs...)

	if session.IsInTx {
		session.AddToTransaction(cmd)
		return h.sendSimpleString(session.ClientConn, "QUEUED")
	}

	err := session.rdb.Process(h.ctx, cmd)
	if err != nil {
		if err == redis.Nil {
			return h.sendNull(session.ClientConn)
		}
		return h.sendError(session.ClientConn, err)
	}

	res, _ := cmd.Result()
	fmt.Printf("res : %+v, val: %+v, %T", res, cmd.Val(), cmd)
	h.sessionManager.UpdateStats(false, false)

	return h.sendCommandResult(session.ClientConn, cmd)
}

// Direct RESP response helpers (no proto package wrapping)

func (h *GoRedisHandlerV2Fixed) sendSimpleString(conn net.Conn, s string) error {
	response := fmt.Sprintf("+%s\r\n", s)
	_, err := conn.Write([]byte(response))
	if err == nil {
		h.forceFlush(conn)
	}
	return err
}

func (h *GoRedisHandlerV2Fixed) sendError(conn net.Conn, err error) error {
	response := fmt.Sprintf("-ERR %s\r\n", err.Error())
	_, writeErr := conn.Write([]byte(response))
	if writeErr == nil {
		h.forceFlush(conn)
	}
	return writeErr
}

func (h *GoRedisHandlerV2Fixed) sendNull(conn net.Conn) error {
	_, err := conn.Write([]byte("$-1\r\n"))
	return err
}

func (h *GoRedisHandlerV2Fixed) sendInteger(conn net.Conn, i int64) error {
	response := fmt.Sprintf(":%d\r\n", i)
	_, err := conn.Write([]byte(response))
	return err
}

func (h *GoRedisHandlerV2Fixed) sendBulkString(conn net.Conn, s string) error {
	if s == "" {
		return h.sendNull(conn)
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	_, err := conn.Write([]byte(response))
	return err
}

func (h *GoRedisHandlerV2Fixed) sendArray(conn net.Conn, items []interface{}) error {
	// Send array header
	response := fmt.Sprintf("*%d\r\n", len(items))

	for _, item := range items {
		switch v := item.(type) {
		case redis.Cmder:
			if err := v.Err(); err != nil {
				response += fmt.Sprintf("-ERR %s\r\n", err.Error())
			} else {
				switch cmd := v.(type) {
				case *redis.StringCmd:
					result := cmd.Val()
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(result), result)
				case *redis.IntCmd:
					result := cmd.Val()
					response += fmt.Sprintf(":%d\r\n", result)
				case *redis.StatusCmd:
					result := cmd.Val()
					response += fmt.Sprintf("+%s\r\n", result)
				default:
					response += "+OK\r\n"
				}
			}
		case string:
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		case int64:
			response += fmt.Sprintf(":%d\r\n", v)
		default:
			response += "+OK\r\n"
		}
	}

	_, err := conn.Write([]byte(response))
	if err == nil {
		h.forceFlush(conn)
	}
	return err
}

func (h *GoRedisHandlerV2Fixed) sendMap(conn net.Conn, data map[string]interface{}) error {
	// RESP3 map format: %<number-of-key-value-pairs>\r\n
	response := fmt.Sprintf("%%%d\r\n", len(data))

	for key, value := range data {
		// Add key as bulk string
		response += fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)

		// Add value based on type
		switch v := value.(type) {
		case string:
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		case int, int64:
			response += fmt.Sprintf(":%d\r\n", v)
		case []interface{}:
			// For arrays (like modules), send as array
			response += fmt.Sprintf("*%d\r\n", len(v))
			for _, item := range v {
				switch item := item.(type) {
				case string:
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(item), item)
				default:
					response += fmt.Sprintf("$%d\r\n%s\r\n", len(fmt.Sprintf("%v", item)), fmt.Sprintf("%v", item))
				}
			}
		default:
			// Default to string representation
			str := fmt.Sprintf("%v", v)
			response += fmt.Sprintf("$%d\r\n%s\r\n", len(str), str)
		}
	}

	_, err := conn.Write([]byte(response))
	if err == nil {
		h.forceFlush(conn)
	}
	return err
}

func (h *GoRedisHandlerV2Fixed) sendCommandResult(conn net.Conn, cmd redis.Cmder) error {
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			return h.sendNull(conn)
		}
		return h.sendError(conn, err)
	}

	switch c := cmd.(type) {
	case *redis.Cmd:
		result, err := c.Result()
		if err != nil {
			if err == redis.Nil {
				return h.sendNull(conn)
			}
			return h.sendError(conn, err)
		}
		return h.sendGenericResult(conn, result)
	case *redis.StringCmd:
		result, err := c.Result()
		if err != nil {
			if err == redis.Nil {
				return h.sendNull(conn)
			}
			return h.sendError(conn, err)
		}
		return h.sendBulkString(conn, result)
	case *redis.IntCmd:
		result := c.Val()
		return h.sendInteger(conn, result)
	case *redis.StatusCmd:
		result := c.Val()
		return h.sendSimpleString(conn, result)
	case *redis.BoolCmd:
		result := c.Val()
		if result {
			return h.sendInteger(conn, 1)
		}
		return h.sendInteger(conn, 0)
	case *redis.DurationCmd:
		result := c.Val()
		return h.sendInteger(conn, int64(result.Seconds()))
	case *redis.StringSliceCmd:
		results := c.Val()
		items := make([]interface{}, len(results))
		for i, v := range results {
			items[i] = v
		}
		return h.sendArray(conn, items)
	default:
		return h.sendSimpleString(conn, "OK")
	}
}

func (h *GoRedisHandlerV2Fixed) sendGenericResult(conn net.Conn, result interface{}) error {
	switch v := result.(type) {
	case string:
		if isStatusString(v) {
			return h.sendSimpleString(conn, v)
		}
		return h.sendBulkString(conn, v)
	case int64:
		return h.sendInteger(conn, v)
	case int:
		return h.sendInteger(conn, int64(v))
	case []interface{}:
		return h.sendArray(conn, v)
	case []string:
		items := make([]interface{}, len(v))
		for i, s := range v {
			items[i] = s
		}
		return h.sendArray(conn, items)
	case nil:
		return h.sendNull(conn)
	default:
		return h.sendBulkString(conn, fmt.Sprintf("%v", v))
	}
}

// Close closes the handler
func (h *GoRedisHandlerV2Fixed) Close() error {
	if h.sessionManager != nil {
		return h.sessionManager.Close()
	}
	return nil
}
