package proxy

import (
	"time"
)

// Config represents proxy server configuration
type Config struct {
	Address       string // Proxy server listening address (e.g., "0.0.0.0:8080")
	RedisAddr     string // Target Redis server address (e.g., "localhost:6379")
	RedisPassword string // Redis server password (if required)
	MaxIdleTime   time.Duration

	// Connection pool configuration
	PoolMaxIdle     uint          // Maximum idle connections in pool
	PoolMaxActive   uint          // Maximum active connections in pool
	PoolConnTimeout time.Duration // Connection timeout
}
