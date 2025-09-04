package proxy

import (
	"strings"
)

// CommandType 表示命令类型
type CommandType int

const (
	// NORMAL 普通命令：无状态一问一答，执行完可释放回连接池
	NORMAL CommandType = iota
	// INIT 初始化命令：影响连接上下文，但在相同上下文下可复用
	INIT
	// SESSION 长会话命令：需要保持连接sticky直到会话结束
	SESSION
)

// String 返回命令类型的字符串表示
func (ct CommandType) String() string {
	switch ct {
	case NORMAL:
		return "NORMAL"
	case INIT:
		return "INIT"
	case SESSION:
		return "SESSION"
	default:
		return "UNKNOWN"
	}
}

// RedisConnectionState 表示Redis连接状态
type RedisConnectionState int

const (
	// StateNormal 普通状态
	StateNormal RedisConnectionState = iota
	// StateInTransaction 事务状态
	StateInTransaction
	// StateSubscribing 订阅状态
	StateSubscribing
	// StateBlocking 阻塞命令状态
	StateBlocking
	// StateMonitoring 监控状态
	StateMonitoring
)

// String 返回连接状态的字符串表示
func (cs RedisConnectionState) String() string {
	switch cs {
	case StateNormal:
		return "Normal"
	case StateInTransaction:
		return "InTransaction"
	case StateSubscribing:
		return "Subscribing"
	case StateBlocking:
		return "Blocking"
	case StateMonitoring:
		return "Monitoring"
	default:
		return "Unknown"
	}
}

// CommandClassifier Redis命令分类器
type CommandClassifier struct {
	commandTypes map[string]CommandType
}

// NewCommandClassifier 创建新的命令分类器
func NewCommandClassifier() *CommandClassifier {
	classifier := &CommandClassifier{
		commandTypes: make(map[string]CommandType),
	}
	classifier.initCommandTypes()
	return classifier
}

// initCommandTypes 初始化命令类型映射表
func (cc *CommandClassifier) initCommandTypes() {
	// ✅ 普通命令 (NORMAL) - 无状态一问一答
	normalCommands := []string{
		// Key-Value 操作
		"GET", "SET", "DEL", "EXISTS", "TYPE", "TTL", "EXPIRE", "EXPIREAT",
		"PERSIST", "RENAME", "RENAMENX", "DUMP", "RESTORE", "KEYS", "SCAN",
		"RANDOMKEY", "DBSIZE", "FLUSHDB", "FLUSHALL",

		// String 操作
		"INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT", "APPEND", "STRLEN",
		"SETRANGE", "GETRANGE", "GETSET", "MGET", "MSET", "MSETNX", "SETEX", "SETNX",

		// Hash 操作
		"HGET", "HSET", "HDEL", "HEXISTS", "HGETALL", "HKEYS", "HVALS", "HLEN",
		"HMGET", "HMSET", "HINCRBY", "HINCRBYFLOAT", "HSCAN", "HSTRLEN", "HSETNX",

		// List 操作
		"LPUSH", "RPUSH", "LPOP", "RPOP", "LLEN", "LRANGE", "LTRIM", "LINDEX",
		"LINSERT", "LREM", "LSET", "LPUSHX", "RPUSHX", "RPOPLPUSH",

		// Set 操作
		"SADD", "SREM", "SCARD", "SMEMBERS", "SISMEMBER", "SRANDMEMBER", "SPOP",
		"SMOVE", "SUNION", "SINTER", "SDIFF", "SUNIONSTORE", "SINTERSTORE",
		"SDIFFSTORE", "SSCAN",

		// Sorted Set 操作
		"ZADD", "ZREM", "ZCARD", "ZCOUNT", "ZSCORE", "ZINCRBY", "ZRANK", "ZREVRANK",
		"ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZREMRANGEBYRANK",
		"ZREMRANGEBYSCORE", "ZUNIONSTORE", "ZINTERSTORE", "ZSCAN", "ZLEXCOUNT",
		"ZRANGEBYLEX", "ZREVRANGEBYLEX", "ZREMRANGEBYLEX",

		// 位图操作
		"SETBIT", "GETBIT", "BITCOUNT", "BITPOS", "BITOP", "BITFIELD",

		// HyperLogLog
		"PFADD", "PFCOUNT", "PFMERGE",

		// Geo
		"GEOADD", "GEODIST", "GEOHASH", "GEOPOS", "GEORADIUS", "GEORADIUSBYMEMBER",

		// Stream (非阻塞)
		"XADD", "XDEL", "XTRIM", "XLEN", "XRANGE", "XREVRANGE", "XINFO",

		// 服务器信息命令
		"PING", "ECHO", "TIME", "INFO", "CONFIG GET", "MEMORY USAGE",
		"OBJECT", "DEBUG OBJECT", "SLOWLOG",

		// Lua 脚本
		"EVAL", "EVALSHA", "SCRIPT EXISTS", "SCRIPT FLUSH", "SCRIPT KILL", "SCRIPT LOAD",
	}

	for _, cmd := range normalCommands {
		cc.commandTypes[cmd] = NORMAL
	}

	// ⚙️ 初始化命令 (INIT) - 影响连接上下文但可复用
	initCommands := []string{
		"HELLO", "AUTH", "SELECT", "CLIENT SETNAME", "CLIENT TRACKING",
		"CLIENT SETINFO", "CLIENT GETREDIR", "CLIENT ID", "CLIENT GETNAME",
		"READONLY", "READWRITE",
	}

	for _, cmd := range initCommands {
		cc.commandTypes[cmd] = INIT
	}

	// 🔒 长会话命令 (SESSION) - 需要保持连接sticky
	sessionCommands := []string{
		// 事务相关
		"WATCH", "UNWATCH", "MULTI", "EXEC", "DISCARD",

		// 发布订阅
		"SUBSCRIBE", "PSUBSCRIBE", "UNSUBSCRIBE", "PUNSUBSCRIBE", "PUBLISH",
		"PUBSUB",

		// 阻塞命令
		"BLPOP", "BRPOP", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX",
		"XREAD", "XREADGROUP", // 注意：只有带BLOCK参数的才是阻塞的，这里简化处理

		// 监控/调试
		"MONITOR", "SCRIPT DEBUG",

		// 其他特殊命令
		"SYNC", "PSYNC", "QUIT", // QUIT虽然是结束命令，但需要特殊处理
	}

	for _, cmd := range sessionCommands {
		cc.commandTypes[cmd] = SESSION
	}
}

// ClassifyCommand 对Redis命令进行分类
func (cc *CommandClassifier) ClassifyCommand(command string) CommandType {
	cmd := strings.ToUpper(strings.TrimSpace(command))

	// 处理复合命令（如 "CONFIG GET"）
	if cmdType, exists := cc.commandTypes[cmd]; exists {
		return cmdType
	}

	// 处理命令前缀匹配
	parts := strings.Fields(cmd)
	if len(parts) > 1 {
		// 尝试匹配前两个单词的组合
		prefix := strings.Join(parts[:2], " ")
		if cmdType, exists := cc.commandTypes[prefix]; exists {
			return cmdType
		}
	}

	// 默认当作普通命令处理
	return NORMAL
}

// IsSessionCommand 检查是否是会话命令
func (cc *CommandClassifier) IsSessionCommand(command string) bool {
	return cc.ClassifyCommand(command) == SESSION
}

// IsInitCommand 检查是否是初始化命令
func (cc *CommandClassifier) IsInitCommand(command string) bool {
	return cc.ClassifyCommand(command) == INIT
}

// IsNormalCommand 检查是否是普通命令
func (cc *CommandClassifier) IsNormalCommand(command string) bool {
	return cc.ClassifyCommand(command) == NORMAL
}

// DetermineStateTransition 根据命令确定连接状态转换
func (cc *CommandClassifier) DetermineStateTransition(currentState RedisConnectionState, command string) RedisConnectionState {
	cmd := strings.ToUpper(strings.TrimSpace(command))

	switch currentState {
	case StateNormal:
		switch cmd {
		case "WATCH", "MULTI":
			return StateInTransaction
		case "SUBSCRIBE", "PSUBSCRIBE":
			return StateSubscribing
		case "BLPOP", "BRPOP", "BRPOPLPUSH", "BZPOPMIN", "BZPOPMAX":
			return StateBlocking
		case "MONITOR":
			return StateMonitoring
		}

	case StateInTransaction:
		switch cmd {
		case "EXEC", "DISCARD":
			return StateNormal
		case "UNWATCH":
			// UNWATCH 在事务外执行时回到Normal，事务内继续保持
			// 这里简化处理，具体需要根据业务逻辑调整
			return StateNormal
		}

	case StateSubscribing:
		switch cmd {
		case "UNSUBSCRIBE", "PUNSUBSCRIBE":
			// 需要检查是否还有其他订阅，这里简化处理
			return StateNormal
		}

	case StateBlocking:
		// 阻塞命令执行完毕后回到Normal状态
		// 实际实现中需要根据命令的返回结果来判断
		return StateNormal

	case StateMonitoring:
		// MONITOR命令通常需要显式quit或断开连接才结束
		// 这里保持监控状态直到连接断开
		return StateMonitoring
	}

	return currentState
}

// CanReuseConnection 检查在给定状态下连接是否可以复用
func (cc *CommandClassifier) CanReuseConnection(state RedisConnectionState) bool {
	return state == StateNormal
}

// GetCommandDescription 获取命令类型的详细描述
func (cc *CommandClassifier) GetCommandDescription(cmdType CommandType) string {
	switch cmdType {
	case NORMAL:
		return "无状态一问一答，执行完可释放回连接池"
	case INIT:
		return "影响连接上下文，但在相同上下文下可复用"
	case SESSION:
		return "需要保持连接sticky直到会话结束"
	default:
		return "未知命令类型"
	}
}
