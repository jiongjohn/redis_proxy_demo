package proxy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"redis-proxy-demo/lib/logger"
	"redis-proxy-demo/redis/proto"
)

// DedicatedClientSession 专用客户端会话
type DedicatedClientSession struct {
	ID           string
	ClientConn   net.Conn
	RedisConn    *DedicatedConnection
	Context      *ConnectionContext
	CreatedAt    time.Time
	LastActivity time.Time
	CommandCount int64
	mu           sync.RWMutex
}

// DedicatedHandler 专用处理器
type DedicatedHandler struct {
	pool           *DedicatedConnectionPool
	sessions       map[net.Conn]*DedicatedClientSession
	sessionCounter int64
	config         DedicatedHandlerConfig
	ctx            context.Context
	cancel         context.CancelFunc
	mu             sync.RWMutex
	stats          *DedicatedHandlerStats
	// 预生成的响应缓存
	helloV2 string
	helloV3 string
}

// DedicatedHandlerConfig 专用处理器配置
type DedicatedHandlerConfig struct {
	RedisAddr         string
	RedisPassword     string
	MaxConnections    int           // 最大连接数
	InitConnections   int           // 初始连接数
	WaitTimeout       time.Duration // 获取连接等待超时
	IdleTimeout       time.Duration // 连接空闲超时
	SessionTimeout    time.Duration // 会话超时
	CommandTimeout    time.Duration // 命令超时
	DefaultDatabase   int           // 默认数据库
	DefaultClientName string        // 默认客户端名
}

// DedicatedHandlerStats 专用处理器统计
type DedicatedHandlerStats struct {
	ActiveSessions    int64     `json:"active_sessions"`
	TotalSessions     int64     `json:"total_sessions"`
	SessionsCreated   int64     `json:"sessions_created"`
	SessionsClosed    int64     `json:"sessions_closed"`
	CommandsProcessed uint64    `json:"commands_processed"`
	ErrorsEncountered int64     `json:"errors_encountered"`
	LastActivity      time.Time `json:"last_activity"`
	mu                sync.RWMutex
}

// NewDedicatedHandler 创建专用处理器
func NewDedicatedHandler(config DedicatedHandlerConfig) (*DedicatedHandler, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建连接池
	poolConfig := DedicatedPoolConfig{
		RedisAddr:     config.RedisAddr,
		RedisPassword: config.RedisPassword,
		MaxSize:       config.MaxConnections,
		InitSize:      config.InitConnections,
		WaitTimeout:   config.WaitTimeout,
		IdleTimeout:   config.IdleTimeout,
	}

	pool, err := NewDedicatedConnectionPool(poolConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("创建连接池失败: %w", err)
	}

	handler := &DedicatedHandler{
		pool:           pool,
		sessions:       make(map[net.Conn]*DedicatedClientSession),
		sessionCounter: 0,
		config:         config,
		ctx:            ctx,
		cancel:         cancel,
		stats:          &DedicatedHandlerStats{},
		helloV2:        "+OK\r\n",
		helloV3:        "",
	}

	// 尝试获取缓存的HELLO 3响应
	// 通过创建一个RESP3上下文来触发HELLO命令的执行和缓存
	resp3Context := &ConnectionContext{
		Database:        config.DefaultDatabase,
		Username:        "",
		Password:        "",
		ClientName:      config.DefaultClientName,
		ProtocolVersion: 3,
		TrackingEnabled: false,
		TrackingOptions: "",
	}

	// 获取一个连接来触发HELLO 3命令的执行
	testConn, err := pool.GetConnection("hello_test", resp3Context)
	if err == nil {
		// 立即释放连接
		pool.ReleaseConnection(testConn)
		// 获取缓存的HELLO响应
		handler.helloV3 = pool.GetHelloV3Response()
		if handler.helloV3 != "" {
			logger.Debug(fmt.Sprintf("✅ 获取到HELLO 3响应缓存: %d 字节", len(handler.helloV3)))
		} else {
			logger.Warn("⚠️ HELLO 3响应缓存为空")
		}
	} else {
		logger.Warn(fmt.Sprintf("❌ 无法获取HELLO 3响应缓存: %v", err))
	}

	// 启动会话清理协程
	go handler.sessionCleanupLoop()
	go handler.statsReporter()

	return handler, nil
}

// Handle 处理客户端连接
func (h *DedicatedHandler) Handle(ctx context.Context, clientConn net.Conn) {
	defer func() {
		h.cleanupSession(clientConn)
		clientConn.Close()
	}()

	// 创建客户端会话
	session := h.createSession(clientConn)
	if session == nil {
		logger.Error("创建客户端会话失败")
		return
	}

	logger.Info(fmt.Sprintf("🔗 新客户端连接 (专用模式): %s -> 会话: %s",
		clientConn.RemoteAddr(), session.ID))

	// 创建命令解析器
	reader := bufio.NewReader(clientConn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 解析命令并保存原始数据
		args, rawData, err := h.parseCommandWithRaw(reader)
		if err != nil {
			if h.isConnectionClosed(err) {
				logger.Info(fmt.Sprintf("客户端连接关闭: %s", clientConn.RemoteAddr()))
			} else {
				logger.Error(fmt.Sprintf("解析命令失败: %v", err))
			}
			return
		}

		if len(args) == 0 {
			continue
		}

		// 处理命令
		err = h.handleCommand(session, args, rawData)
		if err != nil {
			logger.Error(fmt.Sprintf("处理命令失败: %v", err))
			h.sendError(clientConn, err)
			// 根据错误类型决定是否断开连接
			if h.shouldDisconnectOnError(err) {
				return
			}
		}
	}
}

// createSession 创建客户端会话
func (h *DedicatedHandler) createSession(clientConn net.Conn) *DedicatedClientSession {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sessionCounter++
	sessionID := fmt.Sprintf("dedicated_session_%d", h.sessionCounter)

	// 创建默认连接上下文
	connCtx := &ConnectionContext{
		Database:        h.config.DefaultDatabase,
		Username:        "",
		Password:        "",
		ClientName:      h.config.DefaultClientName,
		ProtocolVersion: 2, // 默认RESP2
		TrackingEnabled: false,
		TrackingOptions: "",
	}

	session := &DedicatedClientSession{
		ID:           sessionID,
		ClientConn:   clientConn,
		RedisConn:    nil,
		Context:      connCtx,
		CreatedAt:    time.Now(),
		LastActivity: time.Now(),
		CommandCount: 0,
	}

	h.sessions[clientConn] = session

	h.stats.mu.Lock()
	h.stats.ActiveSessions++
	h.stats.TotalSessions++
	h.stats.SessionsCreated++
	h.stats.mu.Unlock()

	logger.Debug(fmt.Sprintf("创建会话: %s, 上下文: %s", sessionID, connCtx.Hash()))
	return session
}

// cleanupSession 清理会话
func (h *DedicatedHandler) cleanupSession(clientConn net.Conn) {
	h.mu.Lock()
	session, exists := h.sessions[clientConn]
	if exists {
		delete(h.sessions, clientConn)
	}
	h.mu.Unlock()

	if !exists {
		return
	}

	logger.Info(fmt.Sprintf("🧹 清理会话: %s (命令数: %d)", session.ID, session.GetCommandCount()))

	// 释放Redis连接
	if session.RedisConn != nil {
		h.pool.ReleaseConnection(session.RedisConn)
		session.RedisConn = nil
	}

	h.stats.mu.Lock()
	h.stats.ActiveSessions--
	h.stats.SessionsClosed++
	h.stats.mu.Unlock()
}

// handleCommand 处理Redis命令
func (h *DedicatedHandler) handleCommand(session *DedicatedClientSession, args []string, rawData []byte) error {
	if len(args) == 0 {
		return fmt.Errorf("空命令")
	}

	commandName := strings.ToUpper(args[0])
	session.SetLastCommand(commandName)
	session.IncrementCommandCount()
	session.UpdateLastActivity()

	// 更新统计（优化：仅更新命令计数，减少锁竞争）
	atomic.AddUint64(&h.stats.CommandsProcessed, 1)

	// 特殊处理 QUIT：本地回复并断开客户端TCP
	if commandName == "QUIT" {
		h.handleQuit(session)
		return nil
	}

	logger.Debug(fmt.Sprintf("📝 会话 %s 执行命令: %s %v", session.ID, commandName, args[1:]))

	// 处理初始化命令
	if h.isInitCommand(commandName) {
		return h.handleInitCommand(session, args)
	}

	// 确保有Redis连接（优化：减少不必要的连接获取）
	if session.RedisConn == nil {
		conn, err := h.pool.GetConnection(session.ID, session.Context)
		if err != nil {
			return fmt.Errorf("获取Redis连接失败: %w", err)
		}
		session.RedisConn = conn
		// 连接成功获取后，记录日志（仅在获取新连接时）
		logger.Debug(fmt.Sprintf("会话 %s 获取新Redis连接", session.ID))
	}

	// 转发命令到Redis
	err := h.forwardCommandRaw(session, rawData)
	if err != nil {
		// 连接出错，释放连接
		if session.RedisConn != nil {
			h.pool.ReleaseConnection(session.RedisConn)
			session.RedisConn = nil
		}
		return err
	}

	return nil
}

// ForwardOneRESPResponseWithProto 基于proto库的零缓冲流式转发
// 直接在解析过程中转发数据块，无需中间缓冲区
func (h *DedicatedHandler) ForwardOneRESPResponseWithProto(redisConn net.Conn, clientConn net.Conn, timeout time.Duration) error {
	// 设置读超时
	redisConn.SetReadDeadline(time.Now().Add(timeout))
	defer redisConn.SetDeadline(time.Time{})

	// 创建流式转发器
	streamForwarder := &StreamForwarder{
		source: redisConn,
		target: clientConn,
	}

	// 创建proto.Reader，使用流式转发器作为数据源
	protoReader := proto.NewReaderSize(streamForwarder, 64*1024)

	// 解析一个完整的RESP响应，数据在解析过程中直接转发
	_, err := protoReader.ReadReply()
	if err != nil {
		return fmt.Errorf("流式解析RESP响应失败: %w", err)
	}

	return nil
}

// StreamForwarder 实现io.Reader接口的流式转发器
// 在读取数据的同时直接转发到目标连接，无需缓冲
type StreamForwarder struct {
	source io.Reader // 数据源（Redis连接）
	target io.Writer // 数据目标（客户端连接）
}

// Read 实现io.Reader接口，读取数据的同时直接转发
func (sf *StreamForwarder) Read(p []byte) (n int, err error) {
	// 从源读取数据
	n, err = sf.source.Read(p)
	if err != nil {
		return n, err
	}

	// 如果读取到数据，立即转发到目标
	if n > 0 {
		written, writeErr := sf.target.Write(p[:n])
		if writeErr != nil {
			return written, writeErr
		}
		// 确保完全写入
		if written != n {
			return written, fmt.Errorf("部分写入: 期望%d字节，实际写入%d字节", n, written)
		}
	}

	return n, err
}

// forwardResponseWithProto 使用proto库的流式RESP转发
func (h *DedicatedHandler) forwardResponseWithProto(session *DedicatedClientSession) error {
	timeout := h.config.CommandTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return h.ForwardOneRESPResponseWithProto(session.RedisConn.conn, session.ClientConn, timeout)
}

// isInitCommand 判断是否是初始化命令
func (h *DedicatedHandler) isInitCommand(commandName string) bool {
	if commandName == "" || commandName == "CLIENT" {
		// hello 暂时不处理
		return false
	}
	switch commandName {
	case "SELECT", "AUTH", "HELLO", "CLIENT":
		return true
	default:
		return false
	}
}

// handleInitCommand 处理初始化命令
func (h *DedicatedHandler) handleInitCommand(session *DedicatedClientSession, args []string) error {
	commandName := strings.ToUpper(args[0])

	switch commandName {
	case "SELECT":
		return h.handleInitSelect(session, args)
	case "AUTH":
		return h.handleInitAuth(session, args)
	case "HELLO":
		return h.handleInitHello(session, args)
	case "CLIENT":
		return h.handleInitClient(session, args)
	default:
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	}
}

// handleInitSelect 处理 SELECT 命令
func (h *DedicatedHandler) handleInitSelect(session *DedicatedClientSession, args []string) error {
	if len(args) != 2 {
		h.sendError(session.ClientConn, fmt.Errorf("SELECT命令参数错误"))
		return nil
	}
	var dbNum int
	if _, err := fmt.Sscanf(args[1], "%d", &dbNum); err != nil {
		h.sendError(session.ClientConn, fmt.Errorf("无效的数据库编号: %s", args[1]))
		return nil
	}

	session.mu.Lock()
	session.Context.Database = dbNum
	session.mu.Unlock()

	h.resetSessionConnection(session)
	h.sendSimpleString(session.ClientConn, "OK")
	return nil
}

// handleInitAuth 处理 AUTH 命令
func (h *DedicatedHandler) handleInitAuth(session *DedicatedClientSession, args []string) error {
	if len(args) == 2 {
		session.mu.Lock()
		session.Context.Username = ""
		session.Context.Password = args[1]
		session.mu.Unlock()
	} else if len(args) == 3 {
		session.mu.Lock()
		session.Context.Username = args[1]
		session.Context.Password = args[2]
		session.mu.Unlock()
	} else {
		h.sendError(session.ClientConn, fmt.Errorf("AUTH命令参数错误"))
		return nil
	}

	//h.resetSessionConnection(session)
	h.sendSimpleString(session.ClientConn, "OK")
	return nil
}

// handleInitHello 处理 HELLO 命令
func (h *DedicatedHandler) handleInitHello(session *DedicatedClientSession, args []string) error {
	needReset := false

	if len(args) > 1 {
		if args[1] == "3" {
			session.mu.Lock()
			oldVersion := session.Context.ProtocolVersion
			session.Context.ProtocolVersion = 3
			session.mu.Unlock()
			if oldVersion != 3 {
				needReset = true
			}
		}
		// 解析可选项
		for i := 2; i < len(args); i++ {
			switch strings.ToUpper(args[i]) {
			case "AUTH":
				if i+2 < len(args) {
					session.mu.Lock()
					session.Context.Username = args[i+1]
					session.Context.Password = args[i+2]
					session.mu.Unlock()
					needReset = true
					i += 2
				}
			case "SETNAME":
				if i+1 < len(args) {
					session.mu.Lock()
					session.Context.ClientName = args[i+1]
					session.mu.Unlock()
					needReset = true
					i += 1
				}
			}
		}
	}

	// 只有在真正需要时才重置连接
	if needReset {
		h.resetSessionConnection(session)
	}

	h.sendHelloResponse(session)
	return nil
}

// handleInitClient 处理 CLIENT 子命令
func (h *DedicatedHandler) handleInitClient(session *DedicatedClientSession, args []string) error {
	if len(args) < 2 {
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	}

	sub := strings.ToUpper(args[1])
	needReset := false

	switch sub {
	case "SETNAME":
		if len(args) >= 3 {
			session.mu.Lock()
			oldName := session.Context.ClientName
			session.Context.ClientName = args[2]
			session.mu.Unlock()
			// 只有在客户端名称真正改变时才重置连接
			if oldName != args[2] {
				needReset = true
			}
		}
	case "TRACKING":
		if len(args) > 2 {
			flag := strings.ToUpper(args[2])
			session.mu.Lock()
			oldEnabled := session.Context.TrackingEnabled
			session.Context.TrackingEnabled = (flag == "ON")
			session.Context.TrackingOptions = strings.Join(args[3:], " ")
			session.mu.Unlock()
			// 只有在跟踪状态改变时才重置连接
			if oldEnabled != (flag == "ON") {
				needReset = true
			}
		}
	case "SETINFO":
		// SETINFO命令只是设置客户端信息，不需要重置连接
		// 本地接受并返回OK
	}

	// 只有在真正需要时才重置连接
	if needReset {
		h.resetSessionConnection(session)
	}

	h.sendSimpleString(session.ClientConn, "OK")
	return nil
}

// forwardCommandRaw 转发原始命令数据到Redis
func (h *DedicatedHandler) forwardCommandRaw(session *DedicatedClientSession, rawData []byte) error {
	if session.RedisConn == nil {
		return fmt.Errorf("没有可用的Redis连接")
	}

	// 优化：减少超时设置的系统调用开销
	session.RedisConn.conn.SetWriteDeadline(time.Now().Add(h.config.CommandTimeout))

	// 直接发送原始数据到Redis
	if _, err := session.RedisConn.conn.Write(rawData); err != nil {
		return fmt.Errorf("发送命令到Redis失败: %w", err)
	}

	// 使用proto库的流式转发
	return h.forwardResponseWithProto(session)
}

// forwardResponse 零拷贝高性能转发Redis响应到客户端
func (h *DedicatedHandler) forwardResponse(session *DedicatedClientSession) error {
	// 使用io.Copy进行零拷贝转发，但需要处理超时
	session.RedisConn.conn.SetReadDeadline(time.Now().Add(h.config.CommandTimeout))
	defer session.RedisConn.conn.SetDeadline(time.Time{})

	// 使用更大的缓冲区，一次性读写
	buffer := make([]byte, 65536) // 64KB缓冲区，减少系统调用
	totalBytes := 0

	for {
		// 设置较短的读超时来快速检测响应结束
		if totalBytes > 0 {
			session.RedisConn.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
		}

		n, err := session.RedisConn.conn.Read(buffer)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if totalBytes > 0 {
					break // 已读取数据，超时表示结束
				}
				return fmt.Errorf("读取Redis响应超时")
			}
			if err == io.EOF && totalBytes > 0 {
				break
			}
			return fmt.Errorf("读取Redis响应失败: %w", err)
		}

		if n > 0 {
			totalBytes += n
			// 直接写入，避免额外拷贝
			if _, err = session.ClientConn.Write(buffer[:n]); err != nil {
				return fmt.Errorf("发送响应失败: %w", err)
			}
		} else {
			break
		}
	}

	return nil
}

// parseCommandWithRaw 解析客户端命令并保存原始数据
func (h *DedicatedHandler) parseCommandWithRaw(reader *bufio.Reader) ([]string, []byte, error) {
	// 检查第一个字节以判断命令格式
	firstByte, err := reader.Peek(1)
	if err != nil {
		return nil, nil, fmt.Errorf("无法读取命令类型: %w", err)
	}

	switch firstByte[0] {
	case '*': // RESP数组命令格式
		var rawBuffer bytes.Buffer
		teeReader := io.TeeReader(reader, &rawBuffer)

		// 使用proto Reader解析命令
		protoReader := proto.NewReader(teeReader)
		cmd, err := protoReader.ReadReply()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read RESP command: %w", err)
		}

		// 转换为字符串数组参数
		args, err := h.convertToArgs(cmd)
		if err != nil {
			return nil, nil, err
		}

		return args, rawBuffer.Bytes(), nil

	default:
		// Inline命令格式
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, nil, fmt.Errorf("无法读取inline命令: %w", err)
		}

		// 解析inline命令
		lineStr := strings.TrimSpace(string(line))
		if len(lineStr) == 0 {
			return []string{}, line, nil
		}

		parts := strings.Fields(lineStr)
		if len(parts) == 0 {
			return []string{}, line, nil
		}

		return parts, line, nil
	}
}

// convertToArgs 将proto解析的结果转换为字符串数组参数
func (h *DedicatedHandler) convertToArgs(cmd interface{}) ([]string, error) {
	switch v := cmd.(type) {
	case []interface{}:
		args := make([]string, len(v))
		for i, item := range v {
			switch arg := item.(type) {
			case string:
				args[i] = arg
			case []byte:
				args[i] = string(arg)
			default:
				args[i] = fmt.Sprintf("%v", arg)
			}
		}
		return args, nil
	default:
		return nil, fmt.Errorf("unsupported command format: %T", cmd)
	}
}

// resetSessionConnection 重置会话连接
func (h *DedicatedHandler) resetSessionConnection(session *DedicatedClientSession) {
	if session.RedisConn != nil {
		h.pool.ReleaseConnection(session.RedisConn)
		session.RedisConn = nil
	}
}

// handleQuit 处理QUIT命令
func (h *DedicatedHandler) handleQuit(session *DedicatedClientSession) {
	h.sendSimpleString(session.ClientConn, "OK")
	if session.RedisConn != nil {
		h.pool.ReleaseConnection(session.RedisConn)
		session.RedisConn = nil
	}
	session.ClientConn.Close()
}

// 辅助函数

// sendError 发送错误响应给客户端
func (h *DedicatedHandler) sendError(conn net.Conn, err error) {
	response := fmt.Sprintf("-ERR %s\r\n", err.Error())
	conn.Write([]byte(response))

	h.stats.mu.Lock()
	h.stats.ErrorsEncountered++
	h.stats.mu.Unlock()
}

// sendSimpleString 发送简单字符串回复
func (h *DedicatedHandler) sendSimpleString(conn net.Conn, s string) {
	response := fmt.Sprintf("+%s\r\n", s)
	conn.Write([]byte(response))
}

// sendHelloResponse 发送HELLO响应
func (h *DedicatedHandler) sendHelloResponse(session *DedicatedClientSession) {
	session.mu.RLock()
	pv := session.Context.ProtocolVersion
	session.mu.RUnlock()

	if pv == 3 {
		// 优先使用缓存的真实HELLO响应
		if h.helloV3 != "" {
			logger.Debug(fmt.Sprintf("发送缓存的HELLO 3响应: %d 字节, 内容: %q", len(h.helloV3), h.helloV3))
			session.ClientConn.Write([]byte(h.helloV3))
			return // 真实的HELLO响应是完整的，不需要额外的OK
		}

		// 如果没有缓存，尝试从连接池获取
		if cachedResp := h.pool.GetHelloV3Response(); cachedResp != "" {
			h.helloV3 = cachedResp
			session.ClientConn.Write([]byte(h.helloV3))
			return // 真实的HELLO响应是完整的，不需要额外的OK
		}

		// 如果都没有，使用模拟的响应作为后备
		resp := "%7\r\n"                              // Map with 7 key-value pairs
		resp += "$6\r\nserver\r\n$5\r\nredis\r\n"     // "server" => "redis"
		resp += "$7\r\nversion\r\n$5\r\n7.0.0\r\n"    // "version" => "7.0.0"
		resp += "$5\r\nproto\r\n:3\r\n"               // "proto" => 3
		resp += "$2\r\nid\r\n:1\r\n"                  // "id" => 1
		resp += "$4\r\nmode\r\n$10\r\nstandalone\r\n" // "mode" => "standalone"
		resp += "$4\r\nrole\r\n$6\r\nmaster\r\n"      // "role" => "master"
		resp += "$7\r\nmodules\r\n*0\r\n"             // "modules" => []

		session.ClientConn.Write([]byte(resp))
	}
	// 对于RESP2或模拟的RESP3响应，发送OK
	h.sendSimpleString(session.ClientConn, "OK")
}

// isConnectionClosed 检查是否是连接关闭错误
func (h *DedicatedHandler) isConnectionClosed(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	return strings.Contains(errStr, "use of closed network connection") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "EOF") ||
		strings.Contains(errStr, "broken pipe")
}

// shouldDisconnectOnError 判断错误是否需要断开连接
func (h *DedicatedHandler) shouldDisconnectOnError(err error) bool {
	return h.isConnectionClosed(err)
}

// sessionCleanupLoop 会话清理循环
func (h *DedicatedHandler) sessionCleanupLoop() {
	ticker := time.NewTicker(h.config.SessionTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.cleanupExpiredSessions()
		}
	}
}

// cleanupExpiredSessions 清理过期会话
func (h *DedicatedHandler) cleanupExpiredSessions() {
	h.mu.RLock()
	expiredSessions := make([]*DedicatedClientSession, 0)

	for _, session := range h.sessions {
		session.mu.RLock()
		if time.Since(session.LastActivity) > h.config.SessionTimeout {
			expiredSessions = append(expiredSessions, session)
		}
		session.mu.RUnlock()
	}
	h.mu.RUnlock()

	// 清理过期会话
	for _, session := range expiredSessions {
		logger.Debug(fmt.Sprintf("清理过期会话: %s", session.ID))
		h.cleanupSession(session.ClientConn)
	}
}

// statsReporter 统计报告器
func (h *DedicatedHandler) statsReporter() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.reportStats()
		}
	}
}

// reportStats 报告统计信息
func (h *DedicatedHandler) reportStats() {
	stats := h.GetStats()
	poolStats := h.pool.GetStats()

	logger.Info(fmt.Sprintf("📊 专用处理器统计 - 会话: %d/%d, 命令: %d, 错误: %d",
		stats.ActiveSessions, stats.TotalSessions, stats.CommandsProcessed, stats.ErrorsEncountered))

	logger.Info(fmt.Sprintf("📊 专用连接池统计 - 总连接: %d, 活跃: %d, 空闲: %d, 创建: %d, 关闭: %d",
		poolStats.TotalConnections, poolStats.ActiveConnections, poolStats.IdleConnections,
		poolStats.ConnectionsCreated, poolStats.ConnectionsClosed))
}

// GetStats 获取处理器统计信息
func (h *DedicatedHandler) GetStats() DedicatedHandlerStats {
	h.stats.mu.RLock()
	defer h.stats.mu.RUnlock()

	return DedicatedHandlerStats{
		ActiveSessions:    h.stats.ActiveSessions,
		TotalSessions:     h.stats.TotalSessions,
		SessionsCreated:   h.stats.SessionsCreated,
		SessionsClosed:    h.stats.SessionsClosed,
		CommandsProcessed: atomic.LoadUint64(&h.stats.CommandsProcessed),
		ErrorsEncountered: h.stats.ErrorsEncountered,
		LastActivity:      h.stats.LastActivity,
	}
}

// Close 关闭处理器
func (h *DedicatedHandler) Close() error {
	logger.Info("关闭专用处理器...")

	h.cancel()

	// 清理所有会话
	h.mu.RLock()
	sessions := make([]*DedicatedClientSession, 0, len(h.sessions))
	for _, session := range h.sessions {
		sessions = append(sessions, session)
	}
	h.mu.RUnlock()

	for _, session := range sessions {
		h.cleanupSession(session.ClientConn)
	}

	// 关闭连接池
	h.pool.Close()

	logger.Info("✅ 专用处理器已关闭")
	return nil
}

// DedicatedClientSession 方法

// GetCommandCount 获取命令计数
func (s *DedicatedClientSession) GetCommandCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.CommandCount
}

// SetLastCommand 设置最后命令（这里简化处理，不存储具体命令）
func (s *DedicatedClientSession) SetLastCommand(cmd string) {
	// 简化处理，不存储具体命令
}

// IncrementCommandCount 增加命令计数
func (s *DedicatedClientSession) IncrementCommandCount() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.CommandCount++
}

// UpdateLastActivity 更新最后活动时间
func (s *DedicatedClientSession) UpdateLastActivity() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LastActivity = time.Now()
}
