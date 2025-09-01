package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"redis-proxy-demo/lib/logger"
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

// Server represents the proxy server
type Server struct {
	config  Config
	handler *ProxyHandler

	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewServer creates a new proxy server
func NewServer(config Config) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create pool configuration
	poolConfig := RedisPoolConfig{
		RedisAddr:      config.RedisAddr,
		RedisPassword:  config.RedisPassword,
		MaxIdle:        config.PoolMaxIdle,
		MaxActive:      config.PoolMaxActive,
		ConnectTimeout: config.PoolConnTimeout,
		IdleTimeout:    config.MaxIdleTime,
	}

	// Set defaults if not configured
	if poolConfig.MaxIdle == 0 {
		poolConfig.MaxIdle = 20
	}
	if poolConfig.MaxActive == 0 {
		poolConfig.MaxActive = 100
	}
	if poolConfig.ConnectTimeout == 0 {
		poolConfig.ConnectTimeout = 10 * time.Second
	}

	// Create proxy handler with pool
	handler, err := MakeProxyHandlerWithConfig(poolConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create proxy handler: %w", err)
	}

	return &Server{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// ListenAndServe starts the proxy server
func (server *Server) ListenAndServe() error {
	// Start TCP server
	listener, err := net.Listen("tcp", server.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", server.config.Address, err)
	}

	server.listener = listener
	logger.Info(fmt.Sprintf("🚀 Redis Proxy Server starting on %s", server.config.Address))
	logger.Info(fmt.Sprintf("📡 Forwarding to Redis server: %s", server.config.RedisAddr))

	// Handle graceful shutdown
	server.setupGracefulShutdown()

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-server.ctx.Done():
				logger.Info("proxy server stopped accepting connections")
				return nil
			default:
				logger.Error(fmt.Sprintf("failed to accept connection: %v", err))
				continue
			}
		}

		// Handle connection in goroutine
		server.wg.Add(1)
		go func() {
			defer server.wg.Done()
			server.handleConnection(conn)
		}()
	}
}

// handleConnection handles a single client connection
func (server *Server) handleConnection(conn net.Conn) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("panic in connection handler: %v", err))
		}
	}()

	// Set connection timeout
	if server.config.MaxIdleTime > 0 {
		_ = conn.SetDeadline(time.Now().Add(server.config.MaxIdleTime))
	}

	// Handle the connection through proxy handler
	server.handler.Handle(server.ctx, conn)
}

// setupGracefulShutdown sets up graceful shutdown handling
func (server *Server) setupGracefulShutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		logger.Info(fmt.Sprintf("received signal %v, shutting down proxy server...", sig))

		// Start shutdown process
		server.Shutdown()
	}()
}

// Shutdown gracefully shuts down the proxy server
func (server *Server) Shutdown() error {
	logger.Info("starting proxy server shutdown...")

	// Cancel context to stop accepting new connections
	server.cancel()

	// Close listener
	if server.listener != nil {
		_ = server.listener.Close()
	}

	// Close proxy handler
	if server.handler != nil {
		_ = server.handler.Close()
	}

	// Wait for all connections to finish (with timeout)
	done := make(chan struct{})
	go func() {
		server.wg.Wait()
		close(done)
	}()

	// Wait for graceful shutdown or timeout
	select {
	case <-done:
		logger.Info("✅ proxy server shutdown completed")
	case <-time.After(30 * time.Second):
		logger.Warn("⚠️ proxy server shutdown timeout, forcing exit")
	}

	return nil
}

// GetAddress returns the server's listening address
func (server *Server) GetAddress() string {
	if server.listener != nil {
		return server.listener.Addr().String()
	}
	return server.config.Address
}

// GetPoolStats returns connection pool statistics
func (server *Server) GetPoolStats() PoolStats {
	if server.handler == nil {
		return PoolStats{}
	}
	return server.handler.GetPoolStats()
}
