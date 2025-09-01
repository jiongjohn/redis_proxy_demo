package proxy

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"redis-proxy-demo/interface/redis"
	"redis-proxy-demo/lib/logger"
	"redis-proxy-demo/lib/sync/atomic"
	"redis-proxy-demo/redis/client"
	"redis-proxy-demo/redis/connection"
	"redis-proxy-demo/redis/parser"
	"redis-proxy-demo/redis/protocol"
)

var (
	unknownErrReplyBytes = []byte("-ERR unknown command\r\n")
)

// ProxyHandler handles proxy connections between clients and Redis servers
type ProxyHandler struct {
	activeConn sync.Map // *connection.Connection -> placeholder

	closing atomic.Boolean // refusing new client and new request

	// Redis connection pool
	redisPool *RedisPool
}

// MakeProxyHandler creates a new proxy handler
func MakeProxyHandler(redisAddr string) (*ProxyHandler, error) {
	// Create Redis connection pool with default configuration
	poolConfig := DefaultRedisPoolConfig(redisAddr)
	redisPool, err := NewRedisPool(poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis connection pool: %w", err)
	}

	return &ProxyHandler{
		redisPool: redisPool,
	}, nil
}

// MakeProxyHandlerWithConfig creates a new proxy handler with custom pool configuration
func MakeProxyHandlerWithConfig(poolConfig RedisPoolConfig) (*ProxyHandler, error) {
	redisPool, err := NewRedisPool(poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis connection pool: %w", err)
	}

	return &ProxyHandler{
		redisPool: redisPool,
	}, nil
}

// Handle handles proxy connections
func (h *ProxyHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}

	client := connection.NewConn(conn)
	h.activeConn.Store(client, struct{}{})
	defer func() {
		_ = client.Close()
		h.activeConn.Delete(client)
	}()

	logger.Info(fmt.Sprintf("proxy connection established: %s", client.RemoteAddr()))

	ch := parser.ParseStream(conn)
	for payload := range ch {
		if payload.Err != nil {
			errMsg := payload.Err.Error()
			// 正常连接关闭场景，使用INFO级别而非ERROR
			if errMsg == "connection closed" ||
				strings.Contains(errMsg, "use of closed network connection") ||
				strings.Contains(errMsg, "EOF") {
				logger.Info("connection closed: " + errMsg + " from " + client.RemoteAddr())
			} else {
				logger.Error("parse error: " + errMsg)
			}
			if h.closing.Get() {
				logger.Info("connection closed: " + client.RemoteAddr())
				return
			}
			continue
		}
		if payload.Data == nil {
			logger.Error("empty payload")
			continue
		}

		// Get Redis client from pool
		redisClient, err := h.redisPool.Get()
		if err != nil {
			logger.Error(fmt.Sprintf("failed to get Redis client from pool: %v", err))
			_, _ = client.Write([]byte("-ERR failed to get Redis connection\r\n"))
			continue
		}

		// Forward command to Redis server
		err = h.forwardCommand(client, redisClient, payload.Data)

		// Return connection to pool
		h.redisPool.Put(redisClient)

		if err != nil {
			logger.Error(fmt.Sprintf("failed to forward command: %v", err))
			_, _ = client.Write([]byte("-ERR proxy forwarding failed\r\n"))
		}
	}
}

// GetPoolStats returns connection pool statistics
func (h *ProxyHandler) GetPoolStats() PoolStats {
	if h.redisPool == nil {
		return PoolStats{}
	}
	return h.redisPool.Stats()
}

// forwardCommand forwards a command to Redis server and returns response to client
func (h *ProxyHandler) forwardCommand(clientConn *connection.Connection, redisClient *client.Client, command redis.Reply) error {
	// Convert command to bytes for forwarding
	cmdBytes := command.ToBytes()

	logger.Debug(fmt.Sprintf("forwarding command: %s", string(cmdBytes)))

	// Check if it's a multi bulk command (normal Redis command)
	multiBulkCmd, ok := command.(*protocol.MultiBulkReply)
	if !ok {
		// Handle non-standard commands
		return h.forwardRawCommand(clientConn, redisClient, cmdBytes)
	}

	// Check if this is a QUIT command
	isQuitCommand := false
	if len(multiBulkCmd.Args) > 0 {
		cmdName := strings.ToUpper(string(multiBulkCmd.Args[0]))
		if cmdName == "QUIT" {
			isQuitCommand = true
		}
	}

	// Send command to Redis server
	result := redisClient.Send(multiBulkCmd.Args)
	if result == nil {
		logger.Error("no response from Redis server")
		_, err := clientConn.Write([]byte("-ERR no response from Redis server\r\n"))
		return err
	}

	// Forward Redis response back to client
	response := result.ToBytes()
	logger.Debug(fmt.Sprintf("forwarding response: %s", string(response)))

	_, err := clientConn.Write(response)

	// Special handling for QUIT command: close the connection after sending response
	if isQuitCommand && err == nil {
		logger.Info("QUIT command processed, closing client connection")
		clientConn.Close() // Close the TCP connection to client
		return nil
	}

	return err
}

// forwardRawCommand handles non-standard or raw commands
func (h *ProxyHandler) forwardRawCommand(clientConn *connection.Connection, redisClient *client.Client, cmdBytes []byte) error {
	// For raw commands, we'll send a simple response
	// In a real implementation, you might want to establish a raw TCP connection to Redis
	logger.Warn("raw command forwarding not fully implemented yet")
	_, err := clientConn.Write([]byte("+OK\r\n"))
	return err
}

// Close stops proxy handler
func (h *ProxyHandler) Close() error {
	logger.Info("proxy handler shutting down...")
	h.closing.Set(true)

	// Close all client connections
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*connection.Connection)
		_ = client.Close()
		return true
	})

	// Close Redis connection pool
	if h.redisPool != nil {
		return h.redisPool.Close()
	}

	return nil
}
