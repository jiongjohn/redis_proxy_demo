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
	// 预生成的HELLO响应缓存
	helloV2 string
	helloV3 string
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
func NewPoolHandler(config PoolHandlerConfig) (*PoolHandler, error) {
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
		helloV2:        "+OK\r\n",
		helloV3:        "",
	}

	// 预热两个上下文（RESP2 和 RESP3），每个上下文至少建立2个连接
	prewarm := func(protoVer int) error {
		connCtx := &ConnectionContext{
			Database:        config.DefaultDatabase,
			Username:        "",
			Password:        "",
			ClientName:      config.DefaultClientName,
			ProtocolVersion: protoVer,
			TrackingEnabled: false,
			TrackingOptions: "",
		}
		// 创建或获取池，并获取两个连接再归还，确保可用
		for i := 0; i < 2; i++ {
			conn, err := poolManager.GetConnection(connCtx, fmt.Sprintf("prewarm_%d_%d", protoVer, i))
			if err != nil {
				return fmt.Errorf("预热协议 %d 连接失败: %w", protoVer, err)
			}
			poolManager.ReturnConnection(conn, fmt.Sprintf("prewarm_%d_%d", protoVer, i))
		}
		return nil
	}

	if err := prewarm(2); err != nil {
		logger.Error(fmt.Sprintf("预热RESP2失败: %v", err))
		return nil, err
	}
	if err := prewarm(3); err != nil {
		logger.Error(fmt.Sprintf("预热RESP3失败: %v", err))
		return nil, err
	}

	// 从连接池管理器中读取并缓存 HELLO 响应（特别是 RESP3）
	if v3 := poolManager.GetHelloResponse(3); v3 != "" {
		handler.helloV3 = v3
	}

	// 启动会话清理协程
	go handler.sessionCleanupLoop()
	go handler.statsReporter()

	return handler, nil
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
	connCtx := &ConnectionContext{
		Database:        h.config.DefaultDatabase,
		Username:        "", // TODO: 支持用户名
		Password:        "",
		ClientName:      h.config.DefaultClientName,
		ProtocolVersion: 2, // 默认RESP2
		TrackingEnabled: false,
		TrackingOptions: "",
	}

	session := &PoolClientSession{
		ID:               sessionID,
		ClientConn:       clientConn,
		Context:          connCtx,
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

	logger.Debug(fmt.Sprintf("创建会话: %s, 上下文: %s", sessionID, connCtx.Hash()))
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

	// 特殊处理 QUIT：本地回复并断开客户端TCP，不转发到Redis
	if commandName == "QUIT" {
		h.handleQuit(session)
		return nil
	}

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
	// 根据会话状态选择连接类型：会话状态使用粘性连接
	var redisConn *PooledConnection
	var err error
	if session.GetState() != StateNormal {
		redisConn, err = h.getStickyRedisConnection(session)
	} else {
		redisConn, err = h.getRedisConnection(session)
	}
	if err != nil {
		return fmt.Errorf("获取Redis连接失败: %w", err)
	}

	// 如果是临时连接（非粘性），执行完后归还
	defer func() {
		if redisConn.StickyClient == "" || redisConn.StickyClient != session.ID {
			h.poolManager.ReturnConnection(redisConn, session.ID)
			session.SetRedisConnection(nil)
		}
	}()

	// 转发命令到Redis（使用原始数据）
	err = h.forwardCommandRaw(session, redisConn, rawData)
	if err != nil {
		return err
	}

	return nil
}

// handleInitCommandWithRaw 处理初始化命令（带原始数据）
func (h *PoolHandler) handleInitCommandWithRaw(session *PoolClientSession, args []string, rawData []byte) error {
	commandName := strings.ToUpper(args[0])

	// 本地处理初始化命令：仅更新上下文并返回OK/HELLO响应，不获取连接
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
		// 其他初始化命令：统一返回OK（简化）
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	}
}

// handleInitSelect 处理 INIT: SELECT
func (h *PoolHandler) handleInitSelect(session *PoolClientSession, args []string) error {
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

// handleInitAuth 处理 INIT: AUTH
func (h *PoolHandler) handleInitAuth(session *PoolClientSession, args []string) error {
	// AUTH <password> 或 AUTH <username> <password>
	if len(args) == 2 {
		session.mu.Lock()
		session.Context.Username = ""
		session.Context.Password = args[1]
		session.mu.Unlock()
		h.resetSessionConnection(session)
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	}
	if len(args) == 3 {
		session.mu.Lock()
		session.Context.Username = args[1]
		session.Context.Password = args[2]
		session.mu.Unlock()
		h.resetSessionConnection(session)
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	}
	h.sendError(session.ClientConn, fmt.Errorf("AUTH命令参数错误"))
	return nil
}

// handleInitHello 处理 INIT: HELLO
func (h *PoolHandler) handleInitHello(session *PoolClientSession, args []string) error {
	// HELLO [protover [AUTH username password] [SETNAME clientname]]
	if len(args) > 1 {
		if args[1] == "3" {
			session.mu.Lock()
			session.Context.ProtocolVersion = 3
			session.mu.Unlock()
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
					h.resetSessionConnection(session)
					i += 2
				}
			case "SETNAME":
				if i+1 < len(args) {
					session.mu.Lock()
					session.Context.ClientName = args[i+1]
					session.mu.Unlock()
					h.resetSessionConnection(session)
					i += 1
				}
			}
		}
	}
	// 返回HELLO响应（简化）
	h.sendHelloResponse(session)
	return nil
}

// handleInitClient 处理 INIT: CLIENT 子命令
func (h *PoolHandler) handleInitClient(session *PoolClientSession, args []string) error {
	if len(args) < 2 {
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	}
	sub := strings.ToUpper(args[1])
	switch sub {
	case "SETNAME":
		if len(args) >= 3 {
			session.mu.Lock()
			session.Context.ClientName = args[2]
			session.mu.Unlock()
			h.resetSessionConnection(session)
			h.sendSimpleString(session.ClientConn, "OK")
			return nil
		}
		h.sendError(session.ClientConn, fmt.Errorf("CLIENT SETNAME 参数错误"))
		return nil
	case "TRACKING":
		if len(args) > 2 {
			flag := strings.ToUpper(args[2])
			session.mu.Lock()
			session.Context.TrackingEnabled = (flag == "ON")
			session.Context.TrackingOptions = strings.Join(args[3:], " ")
			session.mu.Unlock()
			h.resetSessionConnection(session)
			h.sendSimpleString(session.ClientConn, "OK")
			return nil
		}
		h.sendError(session.ClientConn, fmt.Errorf("CLIENT TRACKING 参数错误"))
		return nil
	case "SETINFO":
		// go-redis 会发送：CLIENT SETINFO LIB-NAME <name> [LIB-VER <ver>]
		// 我们本地接受并返回OK，不触发连接获取
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	default:
		// 其他子命令统一OK
		h.sendSimpleString(session.ClientConn, "OK")
		return nil
	}
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
	prevState := session.GetState()
	newState := classifier.DetermineStateTransition(prevState, commandName)
	session.UpdateState(newState)
	redisConn.UpdateState(newState)

	// 转发命令（使用原始数据）
	err = h.forwardCommandRaw(session, redisConn, rawData)
	if err != nil {
		return err
	}

	// 如果会话结束（从非Normal回到Normal），释放粘性连接
	if prevState != StateNormal && newState == StateNormal {
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

	// 设置读取超时（首包较长，给足时间；后续缩短）
	redisConn.Conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	totalBytes := 0
	firstRead := true

	for {
		n, err := redisConn.Conn.Read(buffer)
		if err != nil {
			if err == io.EOF {
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
			_, err = session.ClientConn.Write(buffer[:n])
			if err != nil {
				return fmt.Errorf("发送响应到客户端失败: %w", err)
			}

			// 首次读到数据后，改为较短的尾包等待，尽快返回给客户端
			if firstRead {
				firstRead = false
				redisConn.Conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			}
		} else {
			break
		}
	}

	return nil
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

// 辅助函数

// sendError 发送错误响应给客户端
func (h *PoolHandler) sendError(conn net.Conn, err error) {
	response := fmt.Sprintf("-ERR %s\r\n", err.Error())
	conn.Write([]byte(response))

	h.stats.mu.Lock()
	h.stats.ErrorsEncountered++
	h.stats.mu.Unlock()
}

// sendSimpleString 发送简单字符串回复（+OK）
func (h *PoolHandler) sendSimpleString(conn net.Conn, s string) {
	response := fmt.Sprintf("+%s\r\n", s)
	conn.Write([]byte(response))
}

// sendHelloResponse 发送简化的HELLO响应
func (h *PoolHandler) sendHelloResponse(session *PoolClientSession) {
	// 如果协商为RESP3，返回一个标准化的HELLO map响应；否则返回+OK
	session.mu.RLock()
	pv := session.Context.ProtocolVersion
	session.mu.RUnlock()
	if pv == 3 {
		if h.helloV3 == "" {
			// 尝试从poolManager获取一次
			h.helloV3 = h.poolManager.GetHelloResponse(3)
		}
		if h.helloV3 != "" {
			session.ClientConn.Write([]byte(h.helloV3))
			return
		} else {
			// 如果实在获取不到 就模拟一个给客户端
			version := "7.0.0"
			mode := "standalone"
			role := "master"
			resp := ""
			resp += "%7\r\n"
			resp += "$6\r\nserver\r\n$5\r\nredis\r\n"
			resp += "$7\r\nversion\r\n$" + fmt.Sprintf("%d\r\n%s\r\n", len(version), version)
			resp += "$5\r\nproto\r\n:3\r\n"
			resp += "$2\r\nid\r\n:1\r\n"
			resp += "$4\r\nmode\r\n$" + fmt.Sprintf("%d\r\n%s\r\n", len(mode), mode)
			resp += "$4\r\nrole\r\n$" + fmt.Sprintf("%d\r\n%s\r\n", len(role), role)
			resp += "$7\r\nmodules\r\n*0\r\n"
			session.ClientConn.Write([]byte(resp))
		}
	}
	h.sendSimpleString(session.ClientConn, "OK")
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
		// tcp 链接超过SessionTimeout不操作的删除
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

	// 新增: 聚合连接统计
	agg := h.poolManager.GetAggregateConnectionStats()
	logger.Info(fmt.Sprintf("🏊 连接汇总 - 池:%d, 总连接:%d, 活跃:%d, 空闲:%d, 每池上限:%d, 总容量:%d",
		agg.NumPools, agg.TotalConnections, agg.ActiveConnections, agg.IdleConnections, agg.MaxPerPool, agg.TotalCapacity))
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

func (h *PoolHandler) resetSessionConnection(session *PoolClientSession) {
	if existingConn := session.GetRedisConnection(); existingConn != nil {
		h.poolManager.ReturnConnection(existingConn, session.ID)
		session.SetRedisConnection(nil)
	}
}

func (h *PoolHandler) handleQuit(session *PoolClientSession) {
	// Redis 语义：返回 +OK 然后关闭连接
	h.sendSimpleString(session.ClientConn, "OK")
	// 立即释放/归还当前会话上的Redis连接
	if redisConn := session.GetRedisConnection(); redisConn != nil {
		if redisConn.StickyClient == session.ID {
			h.poolManager.ReleaseConnection(redisConn, session.ID)
		} else {
			h.poolManager.ReturnConnection(redisConn, session.ID)
		}
		session.SetRedisConnection(nil)
	}
	_ = session.ClientConn.Close()
}
