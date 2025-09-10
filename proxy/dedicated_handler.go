package proxy

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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

// ConnectionContext 表示连接上下文信息
type ConnectionContext struct {
	Database        int    // 数据库编号
	Username        string // 用户名（Redis 6.0+）
	Password        string // 密码（Redis 6.0+ 支持用户名/密码）
	ClientName      string // 客户端名称
	ProtocolVersion int    // 协议版本
	// 客户端跟踪（RESP3 Client Tracking）简单支持
	TrackingEnabled bool   // 是否开启跟踪
	TrackingOptions string // 额外跟踪选项（简化存储原始参数）
}

// DedicatedClientSession 专用客户端会话
type DedicatedClientSession struct {
	ID           string
	ClientConn   net.Conn
	RedisConn    *DedicatedConnection
	Context      *ConnectionContext
	CreatedAt    time.Time
	LastActivity time.Time
	CommandCount int64
	isInline     bool
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
	}

	// 启动会话清理协程（暂时不需要清理， ）
	//go handler.sessionCleanupLoop()
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
		args, rawData, err := h.parseCommandWithRaw(session, reader)
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
			// 如果客户端已关闭/重置，直接结束会话，避免在已开始流式转发后再回写错误帧
			if h.isConnectionClosed(err) {
				logger.Info(fmt.Sprintf("客户端连接关闭: %s", clientConn.RemoteAddr()))
				return
			}
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

	logger.Debug(fmt.Sprintf("创建会话: %s, 上下文: %v", sessionID, connCtx))
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

	logger.Debug(fmt.Sprintf("📝 [inline=%t]会话 %s 执行命令: %s %v", session.isInline, session.ID, commandName, args[1:]))

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

	// 决定是否使用流式转发：对小/简单回复命令（如GET/SET/PING）走非流式，以适配 redis-benchmark
	//useStreaming := h.shouldUseStreaming(args)

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
	// 创建流式转发器，使用滑动读超时，避免一次性deadline导致长响应中途超时
	streamForwarder := &StreamForwarder{
		source:      redisConn,
		target:      clientConn,
		readTimeout: timeout,
	}

	// 创建proto.Reader，使用流式转发器作为数据源
	protoReader := proto.NewReaderSize(streamForwarder, 64*1024)

	// 使用 ReadReply 进行流式解析，虽然性能略低于 DiscardNext，但保证正确性
	// 在流式转发场景中，数据已经被转发，我们只需要确保协议解析正确
	_, err := protoReader.ReadReply()
	if err != nil {
		// proto.Nil 不是错误，是正常的 Redis nil 响应（如 GET 不存在的 key，SPOP 空集合等）
		if errors.Is(err, proto.Nil) {
			return nil
		}

		// 客户端在流式写入过程中断开属于正常情况，不再作为解析错误对待
		errStr := err.Error()
		if strings.Contains(errStr, "broken pipe") ||
			strings.Contains(errStr, "use of closed network connection") ||
			strings.Contains(errStr, "connection reset by peer") ||
			strings.Contains(errStr, "EOF") {
			return err
		}
		return fmt.Errorf("流式跳过RESP响应失败: %w", err)
	}

	return nil
}

// StreamForwarder 实现io.Reader接口的流式转发器
// 在读取数据的同时直接转发到目标连接，无需缓冲
type StreamForwarder struct {
	source      io.Reader // 数据源（Redis连接）
	target      io.Writer // 数据目标（客户端连接）
	readTimeout time.Duration
}

// Read 实现io.Reader接口，读取数据的同时直接转发
func (sf *StreamForwarder) Read(p []byte) (n int, err error) {
	// 滑动读超时：每次读取前刷新源连接的读deadline，避免中长响应中途超时
	if c, ok := sf.source.(net.Conn); ok && sf.readTimeout > 0 {
		c.SetReadDeadline(time.Now().Add(sf.readTimeout))
	}
	// 从源读取数据
	n, err = sf.source.Read(p)
	if err != nil {
		return n, err
	}

	// 如果读取到数据，立即转发到目标
	if n > 0 {
		// 尝试为写入设置较短的写超时，避免客户端阻塞导致长期挂起
		if c, ok := sf.target.(net.Conn); ok {
			c.SetWriteDeadline(time.Now().Add(5 * time.Second))
			defer c.SetWriteDeadline(time.Time{})
		}
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

	if session.isInline {
		return h.forwardResponse(session)
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
func (h *DedicatedHandler) parseCommandWithRaw(session *DedicatedClientSession, reader *bufio.Reader) ([]string, []byte, error) {
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

		session.UpdateIsInline(false)
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
		session.UpdateIsInline(true)
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

// shouldUseStreaming 针对可能很大的响应或未知响应结构使用流式；
// 对常见小响应命令（redis-benchmark 典型：GET/SET/PING/INCR/DECR/DEL/EXISTS）禁用流式
func (h *DedicatedHandler) shouldUseStreaming(args []string) bool {
	if len(args) == 0 {
		return true
	}
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "GET", "SET", "PING", "INCR", "DECR", "DEL", "EXISTS", "GETSET":
		return false
	}
	// 对可能有大包或未知大小的命令启用流式
	switch cmd {
	case "MGET", "MSET", "SCAN", "HGETALL", "LRANGE", "SRANDMEMBER", "ZRANGE", "ZSCORE", "ZSCAN", "SSCAN", "HSCAN":
		return true
	}
	return true
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
	if h.isConnectionClosed(err) {
		return true
	}

	// 连接池相关错误也需要断开客户端连接
	errStr := err.Error()
	if strings.Contains(errStr, "获取Redis连接失败") ||
		strings.Contains(errStr, "连接池已满") ||
		strings.Contains(errStr, "等待连接超时") ||
		strings.Contains(errStr, "连接超时") {
		logger.Warn(fmt.Sprintf("连接池错误，断开客户端: %v", err))
		return true
	}

	return false
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

// UpdateIsInline
func (s *DedicatedClientSession) UpdateIsInline(isInline bool) {
	if s.isInline == isInline {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.isInline = isInline
}
