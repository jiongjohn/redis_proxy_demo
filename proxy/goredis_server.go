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

// GoRedisServer represents a Redis proxy server using go-redis for backend
type GoRedisServer struct {
	config   Config
	handler  *GoRedisHandler
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewGoRedisServer creates a new go-redis based server
func NewGoRedisServer(config Config) (*GoRedisServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create go-redis configuration
	goredisConfig := GoRedisConfig{
		RedisAddr:      config.RedisAddr,
		RedisPassword:  config.RedisPassword,
		RedisDB:        0,  // Default database
		PoolSize:       10, // Default pool size
		MinIdleConns:   2,
		MaxConnAge:     1 * time.Hour,
		IdleTimeout:    30 * time.Minute,
		DialTimeout:    10 * time.Second,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		SessionTimeout: 1 * time.Hour,
	}

	// Override with config values if available
	if config.PoolMaxActive > 0 {
		goredisConfig.PoolSize = int(config.PoolMaxActive)
	}
	if config.PoolMaxIdle > 0 {
		goredisConfig.MinIdleConns = int(config.PoolMaxIdle)
	}
	if config.MaxIdleTime > 0 {
		goredisConfig.IdleTimeout = config.MaxIdleTime
	}
	if config.PoolConnTimeout > 0 {
		goredisConfig.DialTimeout = config.PoolConnTimeout
	}

	// Create handler
	handler := NewGoRedisHandler(goredisConfig)
	if handler == nil {
		cancel()
		return nil, fmt.Errorf("failed to create go-redis handler")
	}

	return &GoRedisServer{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// ListenAndServe starts the go-redis proxy server
func (server *GoRedisServer) ListenAndServe() error {
	// Start TCP server
	listener, err := net.Listen("tcp", server.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", server.config.Address, err)
	}

	server.listener = listener
	logger.Info(fmt.Sprintf("🚀 Redis go-redis Proxy Server starting on %s", server.config.Address))
	logger.Info(fmt.Sprintf("📡 Backend Redis: %s", server.config.RedisAddr))
	logger.Info(fmt.Sprintf("🔗 Connection Pool: Optimized for high concurrency"))
	logger.Info(fmt.Sprintf("✅ WATCH/MULTI/EXEC: Fully supported with session state"))
	logger.Info(fmt.Sprintf("🎯 Connection Management: Smart pooling"))

	// Handle graceful shutdown
	server.setupGracefulShutdown()

	// Start statistics reporting
	go server.statsReporter()

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-server.ctx.Done():
				logger.Info("go-redis proxy server stopped accepting connections")
				return nil
			default:
				logger.Error(fmt.Sprintf("failed to accept connection: %v", err))
				continue
			}
		}

		// Handle connection using go-redis handler
		server.wg.Add(1)
		go func() {
			defer server.wg.Done()
			server.handler.Handle(server.ctx, conn)
		}()
	}
}

// setupGracefulShutdown sets up graceful shutdown handling
func (server *GoRedisServer) setupGracefulShutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		logger.Info(fmt.Sprintf("received signal %v, shutting down go-redis proxy server...", sig))
		server.Shutdown()
	}()
}

// statsReporter reports server statistics
func (server *GoRedisServer) statsReporter() {
	ticker := time.NewTicker(60 * time.Second) // Report every minute
	defer ticker.Stop()

	for {
		select {
		case <-server.ctx.Done():
			return
		case <-ticker.C:
			server.reportServerStats()
		}
	}
}

// reportServerStats reports detailed server statistics
func (server *GoRedisServer) reportServerStats() {
	if server.handler == nil || server.handler.sessionManager == nil {
		return
	}

	stats := server.handler.sessionManager.GetStats()

	logger.Info(fmt.Sprintf("🌟 Server Stats - Mode: go-redis Proxy"))
	logger.Info(fmt.Sprintf("📊 Sessions - Active: %d, Total: %d",
		stats.ActiveSessions, stats.TotalSessions))
	logger.Info(fmt.Sprintf("📈 Commands - Executed: %d, Transactions: %d, WATCH: %d",
		stats.CommandsExecuted, stats.TransactionsExec, stats.WatchOperations))
	logger.Info(fmt.Sprintf("🔗 Pool - Hits: %d, Misses: %d, Ratio: %.2f%%",
		stats.PoolHits, stats.PoolMisses,
		func() float64 {
			total := stats.PoolHits + stats.PoolMisses
			if total == 0 {
				return 0
			}
			return float64(stats.PoolHits) / float64(total) * 100
		}()))

	if !stats.LastActivity.IsZero() {
		logger.Info(fmt.Sprintf("⏰ Last Activity: %v", stats.LastActivity.Format(time.RFC3339)))
	}
}

// Shutdown gracefully shuts down the go-redis proxy server
func (server *GoRedisServer) Shutdown() error {
	logger.Info("starting go-redis proxy server shutdown...")

	// Cancel context to stop accepting new connections
	server.cancel()

	// Close listener
	if server.listener != nil {
		_ = server.listener.Close()
	}

	// Close handler (this will cleanup all sessions and Redis connections)
	if server.handler != nil {
		_ = server.handler.Close()
	}

	// Wait for all connection handlers to finish (with timeout)
	done := make(chan struct{})
	go func() {
		server.wg.Wait()
		close(done)
	}()

	// Wait for graceful shutdown or timeout
	select {
	case <-done:
		logger.Info("✅ go-redis proxy server shutdown completed")
	case <-time.After(30 * time.Second):
		logger.Warn("⚠️ go-redis proxy server shutdown timeout, forcing exit")
	}

	return nil
}

// GetAddress returns the server's listening address
func (server *GoRedisServer) GetAddress() string {
	if server.listener != nil {
		return server.listener.Addr().String()
	}
	return server.config.Address
}

// GetStats returns detailed statistics
func (server *GoRedisServer) GetStats() GoRedisStats {
	if server.handler == nil || server.handler.sessionManager == nil {
		return GoRedisStats{}
	}
	return server.handler.sessionManager.GetStats()
}

// GetSessionCount returns current session count
func (server *GoRedisServer) GetSessionCount() int64 {
	stats := server.GetStats()
	return stats.ActiveSessions
}

// GetConnectionPoolStats returns connection pool statistics
func (server *GoRedisServer) GetConnectionPoolStats() (hits, misses int64) {
	stats := server.GetStats()
	return stats.PoolHits, stats.PoolMisses
}

// IsHealthy checks if server is healthy
func (server *GoRedisServer) IsHealthy() bool {
	if server.handler == nil || server.handler.sessionManager == nil {
		return false
	}

	stats := server.GetStats()
	// Consider healthy if we have reasonable activity
	return stats.TotalSessions >= 0 &&
		stats.ActiveSessions >= 0 &&
		time.Since(stats.LastActivity) < 10*time.Minute // Active within 10 minutes
}
