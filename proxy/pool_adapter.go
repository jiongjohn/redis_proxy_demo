package proxy

import (
	"fmt"
	"time"

	"redis-proxy-demo/lib/logger"
	"redis-proxy-demo/lib/pool"
	"redis-proxy-demo/redis/client"
)

// RedisPoolConfig represents Redis connection pool configuration
type RedisPoolConfig struct {
	RedisAddr      string        // Redis server address
	RedisPassword  string        // Redis server password
	MaxIdle        uint          // Maximum idle connections
	MaxActive      uint          // Maximum active connections
	IdleTimeout    time.Duration // Idle connection timeout
	ConnectTimeout time.Duration // Connection timeout
}

// DefaultRedisPoolConfig returns default pool configuration
func DefaultRedisPoolConfig(redisAddr string) RedisPoolConfig {
	return DefaultRedisPoolConfigWithAuth(redisAddr, "")
}

// DefaultRedisPoolConfigWithAuth returns default pool configuration with authentication
func DefaultRedisPoolConfigWithAuth(redisAddr, password string) RedisPoolConfig {
	return RedisPoolConfig{
		RedisAddr:      redisAddr,
		RedisPassword:  password,
		MaxIdle:        20,  // 20 idle connections
		MaxActive:      100, // 100 max active connections
		IdleTimeout:    5 * time.Minute,
		ConnectTimeout: 10 * time.Second,
	}
}

// RedisPool wraps the generic pool for Redis clients
type RedisPool struct {
	config RedisPoolConfig
	pool   *pool.Pool
}

// NewRedisPool creates a new Redis connection pool
func NewRedisPool(config RedisPoolConfig) (*RedisPool, error) {
	redisPool := &RedisPool{
		config: config,
	}

	// Create factory function for Redis clients
	factory := func() (interface{}, error) {
		if config.RedisPassword != "" {
			logger.Debug(fmt.Sprintf("创建新的Redis连接到 %s (带认证)", config.RedisAddr))
		} else {
			logger.Debug(fmt.Sprintf("创建新的Redis连接到 %s", config.RedisAddr))
		}

		// Create Redis client with authentication support
		redisClient, err := client.MakeClientWithAuth(config.RedisAddr, config.RedisPassword)
		if err != nil {
			return nil, fmt.Errorf("failed to create Redis client: %w", err)
		}

		// Start the Redis client to set status to 'running'
		redisClient.Start()

		logger.Debug("Redis连接创建并启动成功")
		return redisClient, nil
	}

	// Create finalizer function for Redis clients
	finalizer := func(x interface{}) {
		if redisClient, ok := x.(*client.Client); ok {
			logger.Debug("销毁Redis连接")
			redisClient.Close()
		}
	}

	// Create pool with configuration
	poolConfig := pool.Config{
		MaxIdle:   config.MaxIdle,
		MaxActive: config.MaxActive,
	}

	redisPool.pool = pool.New(factory, finalizer, poolConfig)
	logger.Info(fmt.Sprintf("Redis连接池已创建: MaxIdle=%d, MaxActive=%d, Target=%s",
		config.MaxIdle, config.MaxActive, config.RedisAddr))

	return redisPool, nil
}

// Get gets a Redis client from the pool
func (rp *RedisPool) Get() (*client.Client, error) {
	conn, err := rp.pool.Get()
	if err != nil {
		return nil, fmt.Errorf("failed to get Redis connection from pool: %w", err)
	}

	redisClient, ok := conn.(*client.Client)
	if !ok {
		// This should never happen
		rp.pool.Put(conn)
		return nil, fmt.Errorf("invalid connection type from pool")
	}

	logger.Debug("从连接池获取Redis连接")
	return redisClient, nil
}

// Put returns a Redis client to the pool
func (rp *RedisPool) Put(redisClient *client.Client) {
	if redisClient == nil {
		return
	}

	logger.Debug("归还Redis连接到连接池")
	rp.pool.Put(redisClient)
}

// Close closes the connection pool
func (rp *RedisPool) Close() error {
	logger.Info("关闭Redis连接池")
	rp.pool.Close()
	return nil
}

// Stats returns connection pool statistics
func (rp *RedisPool) Stats() PoolStats {
	// Note: The generic pool doesn't expose internal stats
	// We could extend it or implement basic stats here
	return PoolStats{
		RedisAddr:   rp.config.RedisAddr,
		MaxIdle:     rp.config.MaxIdle,
		MaxActive:   rp.config.MaxActive,
		IdleTimeout: rp.config.IdleTimeout,
		// ActiveCount and IdleCount would need pool internal access
		ActiveCount: 0, // TODO: expose from pool
		IdleCount:   0, // TODO: expose from pool
	}
}

// PoolStats represents connection pool statistics
type PoolStats struct {
	RedisAddr   string        `json:"redis_addr"`
	MaxIdle     uint          `json:"max_idle"`
	MaxActive   uint          `json:"max_active"`
	ActiveCount uint          `json:"active_count"`
	IdleCount   uint          `json:"idle_count"`
	IdleTimeout time.Duration `json:"idle_timeout"`
}
