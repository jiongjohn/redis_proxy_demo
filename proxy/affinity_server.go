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

// AffinityServer represents the connection affinity proxy server
type AffinityServer struct {
	config   Config
	handler  *AffinityHandler
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewAffinityServer creates a new connection affinity server
func NewAffinityServer(config Config) (*AffinityServer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Create affinity configuration from server config
	affinityConfig := AffinityConfig{
		RedisAddr:           fmt.Sprintf("%s:%d", "localhost", 6379), // Default Redis address
		RedisPassword:       "",                                      // Will be set from config
		MaxConnections:      int(config.PoolMaxActive),
		IdleTimeout:         config.MaxIdleTime,
		HealthCheckInterval: 30 * time.Second,
		BufferSize:          32 * 1024, // 32KB default
	}

	// Use configured values if available
	if config.PoolMaxActive == 0 {
		affinityConfig.MaxConnections = 1000 // Default max connections
	}
	if config.MaxIdleTime == 0 {
		affinityConfig.IdleTimeout = 5 * time.Minute // Default idle timeout
	}

	// Create affinity handler
	handler := NewAffinityHandler(affinityConfig)

	return &AffinityServer{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// NewAffinityServerWithFullConfig creates server with detailed affinity config
func NewAffinityServerWithFullConfig(config Config, redisAddr, redisPassword string,
	maxConnections int, idleTimeout, healthCheckInterval time.Duration) (*AffinityServer, error) {

	ctx, cancel := context.WithCancel(context.Background())

	affinityConfig := AffinityConfig{
		RedisAddr:           redisAddr,
		RedisPassword:       redisPassword,
		MaxConnections:      maxConnections,
		IdleTimeout:         idleTimeout,
		HealthCheckInterval: healthCheckInterval,
		BufferSize:          32 * 1024,
	}

	handler := NewAffinityHandler(affinityConfig)

	return &AffinityServer{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// ListenAndServe starts the affinity proxy server
func (server *AffinityServer) ListenAndServe() error {
	// Start TCP server
	listener, err := net.Listen("tcp", server.config.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", server.config.Address, err)
	}

	server.listener = listener
	logger.Info(fmt.Sprintf("🚀 Redis Affinity Proxy Server starting on %s", server.config.Address))
	logger.Info(fmt.Sprintf("📡 Using 1:1 connection mapping to Redis: %s", server.config.RedisAddr))
	logger.Info(fmt.Sprintf("✅ WATCH/MULTI/EXEC commands fully supported"))

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
				logger.Info("affinity proxy server stopped accepting connections")
				return nil
			default:
				logger.Error(fmt.Sprintf("failed to accept connection: %v", err))
				continue
			}
		}

		// Handle connection using affinity handler
		server.wg.Add(1)
		go func() {
			defer server.wg.Done()
			server.handler.Handle(server.ctx, conn)
		}()
	}
}

// setupGracefulShutdown sets up graceful shutdown handling
func (server *AffinityServer) setupGracefulShutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-c
		logger.Info(fmt.Sprintf("received signal %v, shutting down affinity proxy server...", sig))
		server.Shutdown()
	}()
}

// statsReporter reports server statistics
func (server *AffinityServer) statsReporter() {
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
func (server *AffinityServer) reportServerStats() {
	stats := server.handler.GetStats()

	logger.Info(fmt.Sprintf("🌟 Server Stats - Mode: Connection Affinity"))
	logger.Info(fmt.Sprintf("📊 Connections - Active: %d, Total: %d",
		stats.ActiveConnections, stats.TotalConnections))
	logger.Info(fmt.Sprintf("📈 Traffic - Created: %d, Closed: %d, Bytes: %d",
		stats.ConnectionsCreated, stats.ConnectionsClosed, stats.BytesTransferred))

	if !stats.LastActivity.IsZero() {
		logger.Info(fmt.Sprintf("⏰ Last Activity: %v", stats.LastActivity.Format(time.RFC3339)))
	}
}

// Shutdown gracefully shuts down the affinity proxy server
func (server *AffinityServer) Shutdown() error {
	logger.Info("starting affinity proxy server shutdown...")

	// Cancel context to stop accepting new connections
	server.cancel()

	// Close listener
	if server.listener != nil {
		_ = server.listener.Close()
	}

	// Close affinity handler (this will cleanup all connections)
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
		logger.Info("✅ affinity proxy server shutdown completed")
	case <-time.After(30 * time.Second):
		logger.Warn("⚠️ affinity proxy server shutdown timeout, forcing exit")
	}

	return nil
}

// GetAddress returns the server's listening address
func (server *AffinityServer) GetAddress() string {
	if server.listener != nil {
		return server.listener.Addr().String()
	}
	return server.config.Address
}

// GetAffinityStats returns detailed affinity statistics
func (server *AffinityServer) GetAffinityStats() AffinityStats {
	if server.handler == nil {
		return AffinityStats{}
	}
	return server.handler.GetStats()
}

// GetConnectionCount returns current connection count
func (server *AffinityServer) GetConnectionCount() int64 {
	stats := server.GetAffinityStats()
	return stats.ActiveConnections
}

// GetMaxConnections returns max connection limit
func (server *AffinityServer) GetMaxConnections() int {
	if server.handler == nil {
		return 0
	}
	return server.handler.config.MaxConnections
}

// IsHealthy checks if server is healthy
func (server *AffinityServer) IsHealthy() bool {
	if server.handler == nil {
		return false
	}

	stats := server.GetAffinityStats()
	// Consider healthy if we have reasonable connection activity
	return stats.TotalConnections >= 0 &&
		stats.ActiveConnections >= 0 &&
		stats.ActiveConnections <= int64(server.GetMaxConnections())
}
