package proxy

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	redisif "redis-proxy-demo/interface/redis"
	"redis-proxy-demo/lib/logger"
	"redis-proxy-demo/redis/parser"
	"redis-proxy-demo/redis/protocol"

	"github.com/redis/go-redis/v9"
)

// GoRedisHandler handles Redis commands using go-redis client with session state
type GoRedisHandler struct {
	sessionManager *ClientSessionManager
	ctx            context.Context
}

// NewGoRedisHandler creates a new go-redis based handler
func NewGoRedisHandler(config GoRedisConfig) *GoRedisHandler {
	sessionManager := NewClientSessionManager(config)
	if sessionManager == nil {
		return nil
	}

	return &GoRedisHandler{
		sessionManager: sessionManager,
		ctx:            context.Background(),
	}
}

// Handle handles a client connection with session state management
func (h *GoRedisHandler) Handle(ctx context.Context, conn net.Conn) {
	defer func() {
		h.sessionManager.RemoveSession(conn)
		conn.Close()
	}()

	logger.Info(fmt.Sprintf("🔗 New client connection (go-redis mode): %s", conn.RemoteAddr()))

	// Create session for this client
	session := h.sessionManager.CreateSession(conn)
	if session == nil {
		logger.Error("Failed to create client session")
		return
	}

	// Parse commands from client connection
	ch := parser.ParseStream(conn)

	for payload := range ch {
		if payload.Err != nil {
			errMsg := payload.Err.Error()
			// Handle normal connection closures gracefully
			if errMsg == "connection closed" ||
				strings.Contains(errMsg, "use of closed network connection") ||
				strings.Contains(errMsg, "EOF") {
				logger.Info("Connection closed: " + errMsg + " from " + conn.RemoteAddr().String())
			} else {
				logger.Error("Parse error: " + errMsg)
			}
			return
		}

		if payload.Data != nil {
			// Handle the command
			err := h.handleCommand(session, payload.Data)
			if err != nil {
				logger.Error(fmt.Sprintf("Command handling error: %v", err))
				// Send error response to client
				h.sendError(conn, err)
			}
		}
	}
}

// handleCommand processes a single Redis command
func (h *GoRedisHandler) handleCommand(session *ClientSession, data redisif.Reply) error {
	// Convert parser.Data to command
	args, err := h.parseCommand(data)
	if err != nil {
		return fmt.Errorf("failed to parse command: %w", err)
	}

	if len(args) == 0 {
		return fmt.Errorf("empty command")
	}

	cmdName := strings.ToUpper(args[0])
	logger.Debug(fmt.Sprintf("📝 Session %s executing: %s %v", session.ID, cmdName, args[1:]))

	// Handle special stateful commands
	switch cmdName {
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

// handleWatch handles WATCH command
func (h *GoRedisHandler) handleWatch(session *ClientSession, keys []string) error {
	if len(keys) == 0 {
		return h.sendError(session.ClientConn, fmt.Errorf("wrong number of arguments for 'watch' command"))
	}

	err := session.StartWatch(keys)
	if err != nil {
		return h.sendError(session.ClientConn, err)
	}

	// Update stats
	h.sessionManager.UpdateStats(false, true)

	return h.sendSimpleString(session.ClientConn, "OK")
}

// handleUnwatch handles UNWATCH command
func (h *GoRedisHandler) handleUnwatch(session *ClientSession) error {
	session.ClearWatch()
	return h.sendSimpleString(session.ClientConn, "OK")
}

// handleMulti handles MULTI command
func (h *GoRedisHandler) handleMulti(session *ClientSession) error {
	session.StartTransaction()
	return h.sendSimpleString(session.ClientConn, "OK")
}

// handleExec handles EXEC command
func (h *GoRedisHandler) handleExec(session *ClientSession) error {
	results, err := session.ExecuteTransaction(h.ctx)
	if err != nil {
		// Transaction failed (likely due to WATCH)
		if err == redis.TxFailedErr {
			return h.sendNull(session.ClientConn)
		}
		return h.sendError(session.ClientConn, err)
	}

	// Update stats
	h.sessionManager.UpdateStats(true, false)

	// Send results as array
	return h.sendArray(session.ClientConn, results)
}

// handleDiscard handles DISCARD command
func (h *GoRedisHandler) handleDiscard(session *ClientSession) error {
	session.DiscardTransaction()
	return h.sendSimpleString(session.ClientConn, "OK")
}

// handleSelect handles SELECT command
func (h *GoRedisHandler) handleSelect(session *ClientSession, args []string) error {
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

// handleQuit handles QUIT command
func (h *GoRedisHandler) handleQuit(session *ClientSession) error {
	err := h.sendSimpleString(session.ClientConn, "OK")
	if err == nil {
		// Close the connection after sending OK
		session.ClientConn.Close()
	}
	return nil // Don't return error to avoid double error handling
}

// handleRegularCommand handles regular Redis commands
func (h *GoRedisHandler) handleRegularCommand(session *ClientSession, cmdName string, args []string) error {
	// Convert args to interface{} slice (including command name)
	cmdArgs := make([]interface{}, len(args))
	for i, arg := range args {
		cmdArgs[i] = arg
	}

	// Create command using generic NewCmd
	cmd := redis.NewCmd(h.ctx, cmdArgs...)

	// If we're in a transaction, queue the command
	if session.IsInTx {
		session.AddToTransaction(cmd)
		return h.sendSimpleString(session.ClientConn, "QUEUED")
	}

	// Execute immediately using the shared Redis client
	err := session.rdb.Process(h.ctx, cmd)
	if err != nil {
		// Special handling for redis.Nil - this should return null, not an error
		if err == redis.Nil {
			return h.sendNull(session.ClientConn)
		}
		return h.sendError(session.ClientConn, err)
	}

	// Update stats
	h.sessionManager.UpdateStats(false, false)

	// Send result to client
	return h.sendCommandResult(session.ClientConn, cmd)
}

// parseCommand converts redisif.Reply to string slice
func (h *GoRedisHandler) parseCommand(data redisif.Reply) ([]string, error) {
	switch d := data.(type) {
	case *protocol.MultiBulkReply:
		args := make([]string, len(d.Args))
		for i, arg := range d.Args {
			args[i] = string(arg)
		}
		return args, nil
	default:
		return nil, fmt.Errorf("expected array command")
	}
}

// RESP response helpers

func (h *GoRedisHandler) sendSimpleString(conn net.Conn, s string) error {
	response := fmt.Sprintf("+%s\r\n", s)
	_, err := conn.Write([]byte(response))
	return err
}

func (h *GoRedisHandler) sendError(conn net.Conn, err error) error {
	response := fmt.Sprintf("-ERR %s\r\n", err.Error())
	_, writeErr := conn.Write([]byte(response))
	return writeErr
}

func (h *GoRedisHandler) sendNull(conn net.Conn) error {
	_, err := conn.Write([]byte("$-1\r\n"))
	return err
}

func (h *GoRedisHandler) sendInteger(conn net.Conn, i int64) error {
	response := fmt.Sprintf(":%d\r\n", i)
	_, err := conn.Write([]byte(response))
	return err
}

func (h *GoRedisHandler) sendBulkString(conn net.Conn, s string) error {
	if s == "" {
		return h.sendNull(conn)
	}
	response := fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
	_, err := conn.Write([]byte(response))
	return err
}

func (h *GoRedisHandler) sendArray(conn net.Conn, items []interface{}) error {
	// Send array header
	response := fmt.Sprintf("*%d\r\n", len(items))

	for _, item := range items {
		switch v := item.(type) {
		case redis.Cmder:
			// Handle command result
			if err := v.Err(); err != nil {
				response += fmt.Sprintf("-ERR %s\r\n", err.Error())
			} else {
				// Get result based on command type
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
	return err
}

func (h *GoRedisHandler) sendCommandResult(conn net.Conn, cmd redis.Cmder) error {
	if err := cmd.Err(); err != nil {
		// Special handling for redis.Nil - this should return null, not an error
		if err == redis.Nil {
			return h.sendNull(conn)
		}
		return h.sendError(conn, err)
	}

	// For generic redis.Cmd, get the result as interface{}
	switch c := cmd.(type) {
	case *redis.Cmd:
		result, err := c.Result()
		if err != nil {
			// Special handling for redis.Nil - this should return null, not an error
			if err == redis.Nil {
				return h.sendNull(conn)
			}
			return h.sendError(conn, err)
		}
		return h.sendGenericResult(conn, result)
	case *redis.StringCmd:
		result, err := c.Result()
		if err != nil {
			// Special handling for redis.Nil - this should return null, not an error
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
		// Default to OK for unknown command types
		return h.sendSimpleString(conn, "OK")
	}
}

// sendGenericResult handles generic redis command results
func (h *GoRedisHandler) sendGenericResult(conn net.Conn, result interface{}) error {
	switch v := result.(type) {
	case string:
		// Handle status strings that should be sent as simple strings (+) not bulk strings ($)
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
		// Try to convert to string as fallback
		return h.sendBulkString(conn, fmt.Sprintf("%v", v))
	}
}

// isStatusString checks if a string should be sent as RESP simple string (+) instead of bulk string ($)
func isStatusString(s string) bool {
	// Common Redis status responses that should be sent as simple strings
	statusStrings := []string{
		"OK",
		"PONG",
		"QUEUED",
		"NOKEY",
		"WRONGTYPE",
	}

	for _, status := range statusStrings {
		if s == status {
			return true
		}
	}

	// Check for other common status patterns
	if strings.HasPrefix(s, "FULLRESYNC") ||
		strings.HasPrefix(s, "CONTINUE") ||
		strings.HasPrefix(s, "NOAUTH") ||
		strings.HasPrefix(s, "LOADING") {
		return true
	}

	return false
}

// Close closes the handler
func (h *GoRedisHandler) Close() error {
	if h.sessionManager != nil {
		return h.sessionManager.Close()
	}
	return nil
}
