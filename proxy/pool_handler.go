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
	"time"

	"redis-proxy-demo/lib/logger"
	"redis-proxy-demo/redis/proto"
)

// PoolHandler 基于连接池的代理处理器
type PoolHandler struct {
	poolManager    *PoolManager                    // 连接池管理器
	sessions       map[net.Conn]*PoolClientSession // 客户端会话映射
	sessionCounter int64                           // 会话计数器
	config         PoolHandlerConfig               // 处理器配置
	ctx            context.Context                 // 上下文
	cancel         context.CancelFunc              // 取消函数
	mu             sync.RWMutex                    // 保护会话映射
	stats          *HandlerStats                   // 处理器统计
}

// PoolHandlerConfig 池处理器配置
type PoolHandlerConfig struct {
	RedisAddr          string        // Redis地址
	RedisPassword      string        // Redis密码
	MaxPoolSize        int           // 每个池的最大连接数
	MinIdleConns       int           // 最小空闲连接数
	MaxIdleConns       int           // 最大空闲连接数
	IdleTimeout        time.Duration // 空闲超时
	MaxLifetime        time.Duration // 连接最大生命周期
	CleanupInterval    time.Duration // 清理间隔
	SessionTimeout     time.Duration // 会话超时
	CommandTimeout     time.Duration // 命令超时
	ConnectionHoldTime time.Duration // 非粘性连接保持时间（默认30秒）
	DefaultDatabase    int           // 默认数据库
	DefaultClientName  string        // 默认客户端名
}

// HandlerStats 处理器统计
type HandlerStats struct {
	ActiveSessions    int64     `json:"active_sessions"`
	TotalSessions     int64     `json:"total_sessions"`
	SessionsCreated   int64     `json:"sessions_created"`
	SessionsClosed    int64     `json:"sessions_closed"`
	CommandsProcessed int64     `json:"commands_processed"`
	NormalCommands    int64     `json:"normal_commands"`
	InitCommands      int64     `json:"init_commands"`
	SessionCommands   int64     `json:"session_commands"`
	ErrorsEncountered int64     `json:"errors_encountered"`
	LastActivity      time.Time `json:"last_activity"`
	mu                sync.RWMutex
}

// NewPoolHandler 创建新的池处理器
func NewPoolHandler(config PoolHandlerConfig) *PoolHandler {
	ctx, cancel := context.WithCancel(context.Background())

	// 创建连接池管理器
	poolManager := NewPoolManager(
		config.RedisAddr,
		config.RedisPassword,
		config.MaxPoolSize,
		config.MinIdleConns,
		config.MaxIdleConns,
		config.IdleTimeout,
		config.MaxLifetime,
		config.CleanupInterval,
	)

	handler := &PoolHandler{
		poolManager:    poolManager,
		sessions:       make(map[net.Conn]*PoolClientSession),
		sessionCounter: 0,
		config:         config,
		ctx:            ctx,
		cancel:         cancel,
		stats:          &HandlerStats{},
	}

	// 启动会话清理协程
	go handler.sessionCleanupLoop()
	go handler.statsReporter()

	return handler
}

// Handle 处理客户端连接
func (h *PoolHandler) Handle(ctx context.Context, clientConn net.Conn) {
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

	logger.Info(fmt.Sprintf("🔗 新客户端连接 (池模式): %s -> 会话: %s",
		clientConn.RemoteAddr(), session.ID))

	// 创建命令解析器
	reader := bufio.NewReader(clientConn)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 移除客户端连接的读取超时，允许长时间空闲连接
		// Redis客户端通常保持长连接，不应该因为暂时不发送命令而被强制断开
		// if h.config.CommandTimeout > 0 {
		// 	clientConn.SetReadDeadline(time.Now().Add(h.config.CommandTimeout))
		// }

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

		// 获取redis连接
		//redisConn, err := h.getRedisConnectionByArgs(session, args)
		//if err != nil {
		//	logger.Error(fmt.Sprintf("获取Redis连接失败: %v", err))
		//	h.sendError(clientConn, err)
		//	return
		//}

		// 处理命令（传入原始数据）
		err = h.handleCommandWithRaw(session, args, rawData)
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
func (h *PoolHandler) createSession(clientConn net.Conn) *PoolClientSession {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.sessionCounter++
	sessionID := fmt.Sprintf("pool_session_%d", h.sessionCounter)

	// 创建默认连接上下文
	context := &ConnectionContext{
		Database:        h.config.DefaultDatabase,
		Username:        "", // TODO: 支持用户名
		ClientName:      h.config.DefaultClientName,
		ProtocolVersion: 2, // 默认RESP2
	}

	session := &PoolClientSession{
		ID:               sessionID,
		ClientConn:       clientConn,
		Context:          context,
		State:            StateNormal,
		CurrentRedisConn: nil,
		CreatedAt:        time.Now(),
		LastActivity:     time.Now(), // 初始化最后活动时间
		LastCommand:      "",
		CommandCount:     0,
	}

	h.sessions[clientConn] = session

	h.stats.mu.Lock()
	h.stats.ActiveSessions++
	h.stats.TotalSessions++
	h.stats.SessionsCreated++
	h.stats.mu.Unlock()

	logger.Debug(fmt.Sprintf("创建会话: %s, 上下文: %s", sessionID, context.Hash()))
	return session
}

// cleanupSession 清理会话
func (h *PoolHandler) cleanupSession(clientConn net.Conn) {
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
	if redisConn := session.GetRedisConnection(); redisConn != nil {
		if session.GetState() != StateNormal {
			// 会话状态异常，释放粘性连接
			h.poolManager.ReleaseConnection(redisConn, session.ID)
		} else {
			// 正常归还连接
			h.poolManager.ReturnConnection(redisConn, session.ID)
		}
		// 清除会话中的连接引用
		session.ClearRedisConnection()
	}

	h.stats.mu.Lock()
	h.stats.ActiveSessions--
	h.stats.SessionsClosed++
	h.stats.mu.Unlock()
}

// getRedisConnectionByArgs
func (h *PoolHandler) getRedisConnectionByArgs(session *PoolClientSession, args []string) (*PooledConnection, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("空命令")
	}

	commandName := strings.ToUpper(args[0])
	session.SetLastCommand(commandName)
	session.IncrementCommandCount()
	session.UpdateLastActivity() // 更新最后活动时间

	// 更新统计
	h.stats.mu.Lock()
	h.stats.CommandsProcessed++
	h.stats.LastActivity = time.Now()
	h.stats.mu.Unlock()

	logger.Debug(fmt.Sprintf("📝 会话 %s 执行命令: %s %v", session.ID, commandName, args[1:]))

	// 分类命令
	classifier := h.poolManager.GetClassifier()
	cmdType := classifier.ClassifyCommand(commandName)

	// 更新命令类型统计
	h.stats.mu.Lock()
	switch cmdType {
	case NORMAL:
		h.stats.NormalCommands++
		return h.getRedisConnection(session)
	case INIT:
		h.stats.InitCommands++
		return h.getInitCommandRedisConn(session, args)
	case SESSION:
		h.stats.SessionCommands++
		return h.getStickyRedisConnection(session)
	}
	return h.getRedisConnection(session)
}

// getInitCommandRedisConn
func (h *PoolHandler) getInitCommandRedisConn(session *PoolClientSession, args []string) (*PooledConnection, error) {
	commandName := strings.ToUpper(args[0])

	// 特殊处理一些初始化命令（这些需要解析参数）
	switch commandName {
	case "SELECT":
		return h.handleSelectRedisConn(session, args)
	case "AUTH":
		return h.getRedisConnection(session)
	case "HELLO":
		return h.handleHelloRedisConn(session, args)
	case "CLIENT":
		if len(args) > 1 && strings.ToUpper(args[1]) == "SETNAME" {
			return h.handleClientSetNameRedisConn(session, args)
		}
	}

	// 其他初始化命令直接转发原始数据
	return h.getRedisConnection(session)
}

// handleCommandWithRaw 处理Redis命令（带原始数据）
func (h *PoolHandler) handleCommandWithRaw(session *PoolClientSession, args []string, rawData []byte) error {
	if len(args) == 0 {
		return fmt.Errorf("空命令")
	}

	commandName := strings.ToUpper(args[0])
	session.SetLastCommand(commandName)
	session.IncrementCommandCount()
	session.UpdateLastActivity() // 更新最后活动时间

	// 更新统计
	h.stats.mu.Lock()
	h.stats.CommandsProcessed++
	h.stats.LastActivity = time.Now()
	h.stats.mu.Unlock()

	logger.Debug(fmt.Sprintf("📝 会话 %s 执行命令: %s %v", session.ID, commandName, args[1:]))

	// 分类命令
	classifier := h.poolManager.GetClassifier()
	cmdType := classifier.ClassifyCommand(commandName)

	// 更新命令类型统计
	h.stats.mu.Lock()
	switch cmdType {
	case NORMAL:
		h.stats.NormalCommands++
	case INIT:
		h.stats.InitCommands++
	case SESSION:
		h.stats.SessionCommands++
	}
	h.stats.mu.Unlock()

	// 根据命令类型执行不同的处理逻辑
	switch cmdType {
	case NORMAL:
		return h.handleNormalCommandWithRaw(session, args, rawData)
	case INIT:
		return h.handleInitCommandWithRaw(session, args, rawData)
	case SESSION:
		return h.handleSessionCommandWithRaw(session, args, rawData)
	default:
		return h.handleNormalCommandWithRaw(session, args, rawData) // 默认当作普通命令处理
	}
}

// handleNormalCommandWithRaw 处理普通命令（带原始数据）
func (h *PoolHandler) handleNormalCommandWithRaw(session *PoolClientSession, args []string, rawData []byte) error {
	// 获取Redis连接
	redisConn, err := h.getRedisConnection(session)
	if err != nil {
		return fmt.Errorf("获取Redis连接失败: %w", err)
	}

	// 转发命令到Redis（使用原始数据）
	return h.forwardCommandRaw(session, redisConn, rawData)
}

// handleInitCommandWithRaw 处理初始化命令（带原始数据）
func (h *PoolHandler) handleInitCommandWithRaw(session *PoolClientSession, args []string, rawData []byte) error {
	commandName := strings.ToUpper(args[0])

	// 特殊处理一些初始化命令（这些需要解析参数）
	switch commandName {
	case "SELECT":
		return h.handleSelect(session, args, rawData)
	case "AUTH":
		return h.handleAuth(session, args, rawData)
	case "HELLO":
		return h.handleHello(session, args, rawData)
	case "CLIENT":
		if len(args) > 1 && strings.ToUpper(args[1]) == "SETNAME" {
			return h.handleClientSetName(session, args, rawData)
		}
	}

	// 其他初始化命令直接转发原始数据
	redisConn, err := h.getRedisConnection(session)
	if err != nil {
		return fmt.Errorf("获取Redis连接失败: %w", err)
	}

	return h.forwardCommandRaw(session, redisConn, rawData)
}

// handleSessionCommandWithRaw 处理会话命令（带原始数据）
func (h *PoolHandler) handleSessionCommandWithRaw(session *PoolClientSession, args []string, rawData []byte) error {
	commandName := strings.ToUpper(args[0])

	// 获取或创建粘性连接
	redisConn, err := h.getStickyRedisConnection(session)
	if err != nil {
		return fmt.Errorf("获取粘性Redis连接失败: %w", err)
	}

	// 更新连接状态
	classifier := h.poolManager.GetClassifier()
	newState := classifier.DetermineStateTransition(session.GetState(), commandName)
	session.UpdateState(newState)
	redisConn.UpdateState(newState)

	// 转发命令（使用原始数据）
	err = h.forwardCommandRaw(session, redisConn, rawData)
	if err != nil {
		return err
	}

	// 如果会话结束，释放粘性连接
	if newState == StateNormal && session.GetState() != StateNormal {
		h.poolManager.ReleaseConnection(redisConn, session.ID)
		session.SetRedisConnection(nil)
	}

	return nil
}

// getRedisConnection 获取Redis连接
func (h *PoolHandler) getRedisConnection(session *PoolClientSession) (*PooledConnection, error) {
	// 如果已有连接且可复用，直接返回
	if existingConn := session.GetRedisConnection(); existingConn != nil {
		if existingConn.CanReuse(session.ID) {
			return existingConn, nil
		}
	}

	// 从连接池获取新连接
	conn, err := h.poolManager.GetConnection(session.Context, session.ID)
	if err != nil {
		return nil, err
	}

	session.SetRedisConnection(conn)
	return conn, nil
}

// getStickyRedisConnection 获取粘性Redis连接
func (h *PoolHandler) getStickyRedisConnection(session *PoolClientSession) (*PooledConnection, error) {
	// 检查是否已有粘性连接
	if existingConn := session.GetRedisConnection(); existingConn != nil {
		if existingConn.StickyClient == session.ID {
			return existingConn, nil
		}
	}

	// 创建新的粘性连接
	conn, err := h.poolManager.GetConnection(session.Context, session.ID)
	if err != nil {
		return nil, err
	}

	// 设置粘性绑定
	conn.mu.Lock()
	conn.StickyClient = session.ID
	conn.mu.Unlock()

	session.SetRedisConnection(conn)
	return conn, nil
}

// forwardCommandRaw 转发原始命令数据到Redis
func (h *PoolHandler) forwardCommandRaw(session *PoolClientSession, redisConn *PooledConnection, rawData []byte) error {

	// 直接发送原始数据到Redis
	_, err := redisConn.Conn.Write(rawData)
	if err != nil {
		return fmt.Errorf("发送原始命令到Redis失败: %w", err)
	}

	// 直接转发Redis响应到客户端（不进行协议解析）
	buffer := make([]byte, 4096)

	// 设置读取超时
	redisConn.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	// Redis响应通常可以一次读取完成
	// 对于大响应，使用循环读取直到超时或EOF
	totalBytes := 0

	for {
		n, err := redisConn.Conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
				// EOF表示Redis连接关闭，正常退出
				break
			}
			// 对于超时错误，如果已经读取到数据，则认为响应完成
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() && totalBytes > 0 {
				break
			}
			return fmt.Errorf("读取Redis响应失败: %w", err)
		}

		if n > 0 {
			totalBytes += n
			// 直接转发到客户端
			_, err = session.ClientConn.Write(buffer[:n])
			if err != nil {
				return fmt.Errorf("发送响应到客户端失败: %w", err)
			}

			// 设置更短的超时来检查是否还有更多数据
			// 大多数Redis响应都能一次读取完成
			//redisConn.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		} else {
			// 没有读取到数据，可能响应已完成
			break
		}
	}

	return nil
}

// buildRESPCommand 构建RESP格式命令
func (h *PoolHandler) buildRESPCommand(args []string) string {
	command := fmt.Sprintf("*%d\r\n", len(args))
	for _, arg := range args {
		command += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return command
}

// parseCommandWithRaw 解析客户端命令并保存原始数据
func (h *PoolHandler) parseCommandWithRaw(reader *bufio.Reader) ([]string, []byte, error) {
	// 检查第一个字节以判断命令格式
	firstByte, err := reader.Peek(1)
	if err != nil {
		return nil, nil, fmt.Errorf("无法读取命令类型: %w", err)
	}

	switch firstByte[0] {
	case '*': // RESP数组命令格式 (如 go-redis发送的)
		// 使用proto库解析，同时保存原始数据
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
		// Inline命令格式 (如 telnet/redis-cli发送的)
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return nil, nil, fmt.Errorf("无法读取inline命令: %w", err)
		}

		// 解析inline命令 (简单的空格分割)
		lineStr := strings.TrimSpace(string(line))
		if len(lineStr) == 0 {
			// 空命令应该被忽略，而不是视为错误
			// 返回空的args切片，主循环会跳过处理
			return []string{}, line, nil
		}

		parts := strings.Fields(lineStr)
		if len(parts) == 0 {
			// 同样，无效格式也返回空切片而不是错误
			return []string{}, line, nil
		}

		return parts, line, nil
	}
}

// convertToArgs 将proto解析的结果转换为字符串数组参数
func (h *PoolHandler) convertToArgs(cmd interface{}) ([]string, error) {
	switch v := cmd.(type) {
	case []interface{}:
		// RESP数组命令
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

// 特殊命令处理器

// handleSelectRedisConn 处理SELECT命令
func (h *PoolHandler) handleSelectRedisConn(session *PoolClientSession, args []string) (*PooledConnection, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("SELECT命令参数错误")
	}

	// 解析数据库编号
	var dbNum int
	if _, err := fmt.Sscanf(args[1], "%d", &dbNum); err != nil {
		return nil, fmt.Errorf("无效的数据库编号: %s", args[1])
	}

	// 更新会话上下文
	session.mu.Lock()
	session.Context.Database = dbNum
	session.mu.Unlock()

	// 如果有当前连接，需要释放并获取新的连接（不同DB的连接不能复用）
	if existingConn := session.GetRedisConnection(); existingConn != nil {
		h.poolManager.ReturnConnection(existingConn, session.ID)
		session.SetRedisConnection(nil)
	}

	// 获取新连接并执行SELECT
	return h.getRedisConnection(session)
}

// handleSelect 处理SELECT命令
func (h *PoolHandler) handleSelect(session *PoolClientSession, args []string, rawData []byte) error {
	if len(args) != 2 {
		return fmt.Errorf("SELECT命令参数错误")
	}

	// 解析数据库编号
	var dbNum int
	if _, err := fmt.Sscanf(args[1], "%d", &dbNum); err != nil {
		return fmt.Errorf("无效的数据库编号: %s", args[1])
	}

	// 更新会话上下文
	session.mu.Lock()
	session.Context.Database = dbNum
	session.mu.Unlock()

	// 如果有当前连接，需要释放并获取新的连接（不同DB的连接不能复用）
	if existingConn := session.GetRedisConnection(); existingConn != nil {
		h.poolManager.ReturnConnection(existingConn, session.ID)
		session.SetRedisConnection(nil)
	}

	// 获取新连接并执行SELECT
	redisConn, err := h.getRedisConnection(session)
	if err != nil {
		return err
	}

	return h.forwardCommandRaw(session, redisConn, rawData)
}

// handleAuth 处理AUTH命令
func (h *PoolHandler) handleAuth(session *PoolClientSession, args []string, rawData []byte) error {
	// AUTH命令会影响连接的认证状态，需要特殊处理
	// 这里简化处理，实际应该更新连接上下文
	redisConn, err := h.getRedisConnection(session)
	if err != nil {
		return err
	}

	return h.forwardCommandRaw(session, redisConn, rawData)
}

// handleHelloRedisConn 处理HELLO命令
func (h *PoolHandler) handleHelloRedisConn(session *PoolClientSession, args []string) (*PooledConnection, error) {
	// HELLO命令用于协议协商，需要更新协议版本
	if len(args) > 1 {
		if args[1] == "3" {
			session.mu.Lock()
			session.Context.ProtocolVersion = 3
			session.mu.Unlock()
		}
	}

	return h.getRedisConnection(session)
}

// handleHello 处理HELLO命令
func (h *PoolHandler) handleHello(session *PoolClientSession, args []string, rawData []byte) error {
	// HELLO命令用于协议协商，需要更新协议版本
	if len(args) > 1 {
		if args[1] == "3" {
			session.mu.Lock()
			session.Context.ProtocolVersion = 3
			session.mu.Unlock()
		}
	}

	redisConn, err := h.getRedisConnection(session)
	if err != nil {
		return err
	}

	return h.forwardCommandRaw(session, redisConn, rawData)
}

// handleClientSetNameRedisConn 处理CLIENT SETNAME命令
func (h *PoolHandler) handleClientSetNameRedisConn(session *PoolClientSession, args []string) (*PooledConnection, error) {
	if len(args) >= 3 {
		session.mu.Lock()
		session.Context.ClientName = args[2]
		session.mu.Unlock()
	}

	return h.getRedisConnection(session)
}

// handleClientSetName 处理CLIENT SETNAME命令
func (h *PoolHandler) handleClientSetName(session *PoolClientSession, args []string, rawData []byte) error {
	if len(args) >= 3 {
		session.mu.Lock()
		session.Context.ClientName = args[2]
		session.mu.Unlock()
	}

	redisConn, err := h.getRedisConnection(session)
	if err != nil {
		return err
	}

	return h.forwardCommandRaw(session, redisConn, rawData)
}

// 辅助函数

// sendError 发送错误响应给客户端
func (h *PoolHandler) sendError(conn net.Conn, err error) {
	response := fmt.Sprintf("-ERR %s\r\n", err.Error())
	conn.Write([]byte(response))

	h.stats.mu.Lock()
	h.stats.ErrorsEncountered++
	h.stats.mu.Unlock()
}

// isConnectionClosed 检查是否是连接关闭错误
func (h *PoolHandler) isConnectionClosed(err error) bool {
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
func (h *PoolHandler) shouldDisconnectOnError(err error) bool {
	// 连接相关错误需要断开
	if h.isConnectionClosed(err) {
		return true
	}

	// TODO: 根据错误类型决定是否断开连接
	return false
}

// sessionCleanupLoop 会话清理循环
func (h *PoolHandler) sessionCleanupLoop() {
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
func (h *PoolHandler) cleanupExpiredSessions() {
	h.mu.RLock()
	expiredSessions := make([]*PoolClientSession, 0)

	for _, session := range h.sessions {
		session.mu.RLock()
		if time.Since(session.CreatedAt) > h.config.SessionTimeout {
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

	// 清理空闲连接
	h.cleanupIdleConnections()
}

// cleanupIdleConnections 清理空闲连接
func (h *PoolHandler) cleanupIdleConnections() {
	// 只有在配置了ConnectionHoldTime时才执行清理
	if h.config.ConnectionHoldTime <= 0 {
		return
	}

	h.mu.RLock()
	idleSessions := make([]*PoolClientSession, 0)

	for _, session := range h.sessions {
		if session.IsConnectionIdle(h.config.ConnectionHoldTime) {
			idleSessions = append(idleSessions, session)
		}
	}
	h.mu.RUnlock()

	// 释放空闲连接
	for _, session := range idleSessions {
		if redisConn := session.GetRedisConnection(); redisConn != nil {
			// 只处理非粘性连接
			if redisConn.StickyClient == "" || redisConn.StickyClient != session.ID {
				logger.Debug(fmt.Sprintf("释放空闲连接: 会话 %s", session.ID))
				h.poolManager.ReturnConnection(redisConn, session.ID)
				session.SetRedisConnection(nil)
			}
		}
	}
}

// statsReporter 统计报告器
func (h *PoolHandler) statsReporter() {
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
func (h *PoolHandler) reportStats() {
	stats := h.GetStats() // 使用GetStats方法获取副本
	poolStats := h.poolManager.GetStats()

	logger.Info(fmt.Sprintf("📊 池处理器统计 - 会话: %d/%d, 命令: %d (普通:%d, 初始化:%d, 会话:%d), 错误: %d",
		stats.ActiveSessions, stats.TotalSessions, stats.CommandsProcessed,
		stats.NormalCommands, stats.InitCommands, stats.SessionCommands,
		stats.ErrorsEncountered))

	logger.Info(fmt.Sprintf("📊 连接池统计 - 池数量: %d, 创建: %d, 关闭: %d",
		poolStats.PoolCount, poolStats.PoolsCreated, poolStats.PoolsClosed))
}

// Close 关闭处理器
func (h *PoolHandler) Close() error {
	logger.Info("关闭池处理器...")

	h.cancel()

	// 清理所有会话
	h.mu.RLock()
	sessions := make([]*PoolClientSession, 0, len(h.sessions))
	for _, session := range h.sessions {
		sessions = append(sessions, session)
	}
	h.mu.RUnlock()

	for _, session := range sessions {
		h.cleanupSession(session.ClientConn)
	}

	// 关闭连接池管理器
	h.poolManager.Close()

	logger.Info("✅ 池处理器已关闭")
	return nil
}

// GetStats 获取处理器统计信息
func (h *PoolHandler) GetStats() HandlerStats {
	h.stats.mu.RLock()
	defer h.stats.mu.RUnlock()

	// 创建副本以避免锁值复制
	return HandlerStats{
		ActiveSessions:    h.stats.ActiveSessions,
		TotalSessions:     h.stats.TotalSessions,
		SessionsCreated:   h.stats.SessionsCreated,
		SessionsClosed:    h.stats.SessionsClosed,
		CommandsProcessed: h.stats.CommandsProcessed,
		NormalCommands:    h.stats.NormalCommands,
		InitCommands:      h.stats.InitCommands,
		SessionCommands:   h.stats.SessionCommands,
		ErrorsEncountered: h.stats.ErrorsEncountered,
		LastActivity:      h.stats.LastActivity,
	}
}

// GetPoolManagerStats 获取池管理器统计信息
func (h *PoolHandler) GetPoolManagerStats() PoolManagerStats {
	return h.poolManager.GetStats()
}
