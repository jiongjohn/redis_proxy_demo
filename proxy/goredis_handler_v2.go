package proxy

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	"redis-proxy-demo/lib/logger"
	"redis-proxy-demo/redis/proto"

	"github.com/redis/go-redis/v9"
)

// GoRedisHandlerV2 Enhanced go-redis handler using proto package for parsing
type GoRedisHandlerV2 struct {
	sessionManager *ClientSessionManager
	ctx            context.Context
}

// NewGoRedisHandlerV2 creates a new enhanced go-redis based handler with proto support
func NewGoRedisHandlerV2(config GoRedisConfig) *GoRedisHandlerV2 {
	sessionManager := NewClientSessionManager(config)
	if sessionManager == nil {
		return nil
	}

	return &GoRedisHandlerV2{
		sessionManager: sessionManager,
		ctx:            context.Background(),
	}
}

// Handle handles a client connection with enhanced parsing and session state management
func (h *GoRedisHandlerV2) Handle(ctx context.Context, conn net.Conn) {
	defer func() {
		h.sessionManager.RemoveSession(conn)
		conn.Close()
	}()

	logger.Info(fmt.Sprintf("🔗 New client connection (go-redis v2 mode): %s", conn.RemoteAddr()))

	// Create session for this client
	session := h.sessionManager.CreateSession(conn)
	if session == nil {
		logger.Error("Failed to create client session")
		return
	}

	// Create enhanced parser using proto package
	clientReader := bufio.NewReader(conn)
	clientWriter := bufio.NewWriter(conn)

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
				h.sendError(conn, clientWriter, err)
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		// Handle the command
		err = h.handleCommand(session, args, clientWriter)
		if err != nil {
			logger.Error(fmt.Sprintf("Command handling error: %v", err))
			// Send error response to client
			h.sendError(conn, clientWriter, err)
		}
	}
}

// parseCommand parses command using mixed strategy (RESP arrays + inline commands)
func (h *GoRedisHandlerV2) parseCommand(reader *bufio.Reader) ([]string, error) {
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
func (h *GoRedisHandlerV2) convertToArgs(cmd interface{}) ([]string, error) {
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
func (h *GoRedisHandlerV2) parseInlineCommand(line string) []string {
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

// handleCommand processes a single Redis command (same logic as original)
func (h *GoRedisHandlerV2) handleCommand(session *ClientSession, args []string, writer *bufio.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("empty command")
	}

	cmdName := strings.ToUpper(args[0])
	logger.Debug(fmt.Sprintf("📝 Session %s executing: %s %v", session.ID, cmdName, args[1:]))

	// Handle special stateful commands
	switch cmdName {
	case "WATCH":
		return h.handleWatch(session, args[1:], writer)
	case "UNWATCH":
		return h.handleUnwatch(session, writer)
	case "MULTI":
		return h.handleMulti(session, writer)
	case "EXEC":
		return h.handleExec(session, writer)
	case "DISCARD":
		return h.handleDiscard(session, writer)
	case "SELECT":
		return h.handleSelect(session, args, writer)
	case "QUIT":
		return h.handleQuit(session, writer)
	default:
		return h.handleRegularCommand(session, cmdName, args, writer)
	}
}

// Enhanced command handlers with proto-based responses

func (h *GoRedisHandlerV2) handleWatch(session *ClientSession, keys []string, writer *bufio.Writer) error {
	if len(keys) == 0 {
		return h.sendError(session.ClientConn, writer, fmt.Errorf("wrong number of arguments for 'watch' command"))
	}

	err := session.StartWatch(keys)
	if err != nil {
		return h.sendError(session.ClientConn, writer, err)
	}

	h.sessionManager.UpdateStats(false, true)
	return h.sendSimpleString(session.ClientConn, writer, "OK")
}

func (h *GoRedisHandlerV2) handleUnwatch(session *ClientSession, writer *bufio.Writer) error {
	session.ClearWatch()
	return h.sendSimpleString(session.ClientConn, writer, "OK")
}

func (h *GoRedisHandlerV2) handleMulti(session *ClientSession, writer *bufio.Writer) error {
	session.StartTransaction()
	return h.sendSimpleString(session.ClientConn, writer, "OK")
}

func (h *GoRedisHandlerV2) handleExec(session *ClientSession, writer *bufio.Writer) error {
	results, err := session.ExecuteTransaction(h.ctx)
	if err != nil {
		if err == redis.TxFailedErr {
			return h.sendNull(session.ClientConn, writer)
		}
		return h.sendError(session.ClientConn, writer, err)
	}

	h.sessionManager.UpdateStats(true, false)
	return h.sendArray(session.ClientConn, writer, results)
}

func (h *GoRedisHandlerV2) handleDiscard(session *ClientSession, writer *bufio.Writer) error {
	session.DiscardTransaction()
	return h.sendSimpleString(session.ClientConn, writer, "OK")
}

func (h *GoRedisHandlerV2) handleSelect(session *ClientSession, args []string, writer *bufio.Writer) error {
	if len(args) != 2 {
		return h.sendError(session.ClientConn, writer, fmt.Errorf("wrong number of arguments for 'select' command"))
	}

	db, err := strconv.Atoi(args[1])
	if err != nil {
		return h.sendError(session.ClientConn, writer, fmt.Errorf("invalid database number"))
	}

	err = session.SelectDatabase(db)
	if err != nil {
		return h.sendError(session.ClientConn, writer, err)
	}

	return h.sendSimpleString(session.ClientConn, writer, "OK")
}

func (h *GoRedisHandlerV2) handleQuit(session *ClientSession, writer *bufio.Writer) error {
	err := h.sendSimpleString(session.ClientConn, writer, "OK")
	if err == nil {
		session.ClientConn.Close()
	}
	return nil
}

func (h *GoRedisHandlerV2) handleRegularCommand(session *ClientSession, cmdName string, args []string, writer *bufio.Writer) error {
	cmdArgs := make([]interface{}, len(args))
	for i, arg := range args {
		cmdArgs[i] = arg
	}

	cmd := redis.NewCmd(h.ctx, cmdArgs...)

	if session.IsInTx {
		session.AddToTransaction(cmd)
		return h.sendSimpleString(session.ClientConn, writer, "QUEUED")
	}

	err := session.rdb.Process(h.ctx, cmd)
	if err != nil {
		if err == redis.Nil {
			return h.sendNull(session.ClientConn, writer)
		}
		return h.sendError(session.ClientConn, writer, err)
	}

	h.sessionManager.UpdateStats(false, false)
	return h.sendCommandResult(session.ClientConn, writer, cmd)
}

// Enhanced RESP response helpers using proto package for output

func (h *GoRedisHandlerV2) sendSimpleString(conn net.Conn, writer *bufio.Writer, s string) error {
	protoWriter := proto.NewWriter(writer)
	err := protoWriter.WriteArg(fmt.Sprintf("+%s", s))
	if err != nil {
		return err
	}
	return writer.Flush()
}

func (h *GoRedisHandlerV2) sendError(conn net.Conn, writer *bufio.Writer, err error) error {
	protoWriter := proto.NewWriter(writer)
	writeErr := protoWriter.WriteArg(fmt.Sprintf("-ERR %s", err.Error()))
	if writeErr != nil {
		return writeErr
	}
	return writer.Flush()
}

func (h *GoRedisHandlerV2) sendNull(conn net.Conn, writer *bufio.Writer) error {
	protoWriter := proto.NewWriter(writer)
	err := protoWriter.WriteArg("$-1")
	if err != nil {
		return err
	}
	return writer.Flush()
}

func (h *GoRedisHandlerV2) sendInteger(conn net.Conn, writer *bufio.Writer, i int64) error {
	protoWriter := proto.NewWriter(writer)
	err := protoWriter.WriteArg(fmt.Sprintf(":%d", i))
	if err != nil {
		return err
	}
	return writer.Flush()
}

func (h *GoRedisHandlerV2) sendBulkString(conn net.Conn, writer *bufio.Writer, s string) error {
	protoWriter := proto.NewWriter(writer)
	var err error
	if s == "" {
		err = protoWriter.WriteArg("$-1")
	} else {
		err = protoWriter.WriteArg(s)
	}
	if err != nil {
		return err
	}
	return writer.Flush()
}

func (h *GoRedisHandlerV2) sendArray(conn net.Conn, writer *bufio.Writer, items []interface{}) error {
	protoWriter := proto.NewWriter(writer)

	// Convert items to format expected by WriteArg
	responses := make([]interface{}, 0, len(items)+1)
	responses = append(responses, fmt.Sprintf("*%d", len(items)))

	for _, item := range items {
		switch v := item.(type) {
		case redis.Cmder:
			if err := v.Err(); err != nil {
				responses = append(responses, fmt.Sprintf("-ERR %s", err.Error()))
			} else {
				switch cmd := v.(type) {
				case *redis.StringCmd:
					result := cmd.Val()
					responses = append(responses, result)
				case *redis.IntCmd:
					result := cmd.Val()
					responses = append(responses, fmt.Sprintf(":%d", result))
				case *redis.StatusCmd:
					result := cmd.Val()
					responses = append(responses, fmt.Sprintf("+%s", result))
				default:
					responses = append(responses, "+OK")
				}
			}
		case string:
			responses = append(responses, v)
		case int64:
			responses = append(responses, fmt.Sprintf(":%d", v))
		default:
			responses = append(responses, "+OK")
		}
	}

	// Write all responses
	for _, resp := range responses {
		if err := protoWriter.WriteArg(resp); err != nil {
			return err
		}
	}

	return writer.Flush()
}

func (h *GoRedisHandlerV2) sendCommandResult(conn net.Conn, writer *bufio.Writer, cmd redis.Cmder) error {
	if err := cmd.Err(); err != nil {
		if err == redis.Nil {
			return h.sendNull(conn, writer)
		}
		return h.sendError(conn, writer, err)
	}

	switch c := cmd.(type) {
	case *redis.Cmd:
		result, err := c.Result()
		if err != nil {
			if err == redis.Nil {
				return h.sendNull(conn, writer)
			}
			return h.sendError(conn, writer, err)
		}
		return h.sendGenericResult(conn, writer, result)
	case *redis.StringCmd:
		result, err := c.Result()
		if err != nil {
			if err == redis.Nil {
				return h.sendNull(conn, writer)
			}
			return h.sendError(conn, writer, err)
		}
		return h.sendBulkString(conn, writer, result)
	case *redis.IntCmd:
		result := c.Val()
		return h.sendInteger(conn, writer, result)
	case *redis.StatusCmd:
		result := c.Val()
		return h.sendSimpleString(conn, writer, result)
	case *redis.BoolCmd:
		result := c.Val()
		if result {
			return h.sendInteger(conn, writer, 1)
		}
		return h.sendInteger(conn, writer, 0)
	case *redis.DurationCmd:
		result := c.Val()
		return h.sendInteger(conn, writer, int64(result.Seconds()))
	case *redis.StringSliceCmd:
		results := c.Val()
		items := make([]interface{}, len(results))
		for i, v := range results {
			items[i] = v
		}
		return h.sendArray(conn, writer, items)
	default:
		return h.sendSimpleString(conn, writer, "OK")
	}
}

func (h *GoRedisHandlerV2) sendGenericResult(conn net.Conn, writer *bufio.Writer, result interface{}) error {
	switch v := result.(type) {
	case string:
		if isStatusString(v) {
			return h.sendSimpleString(conn, writer, v)
		}
		return h.sendBulkString(conn, writer, v)
	case int64:
		return h.sendInteger(conn, writer, v)
	case int:
		return h.sendInteger(conn, writer, int64(v))
	case []interface{}:
		return h.sendArray(conn, writer, v)
	case []string:
		items := make([]interface{}, len(v))
		for i, s := range v {
			items[i] = s
		}
		return h.sendArray(conn, writer, items)
	case nil:
		return h.sendNull(conn, writer)
	default:
		return h.sendBulkString(conn, writer, fmt.Sprintf("%v", v))
	}
}

// Note: isStatusString function is already defined in goredis_handler.go

// Close closes the handler
func (h *GoRedisHandlerV2) Close() error {
	if h.sessionManager != nil {
		return h.sessionManager.Close()
	}
	return nil
}
