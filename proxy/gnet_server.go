package proxy

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"redis-proxy-demo/lib/logger"

	"github.com/panjf2000/gnet/v2"
)

// GNetServer is a high-performance proxy server based on gnet
type GNetServer struct {
	gnet.BuiltinEventEngine

	config  Config
	handler *ProxyHandler

	eng    gnet.Engine
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Connection management
	connections sync.Map // gnet.Conn -> *ConnectionState
}

// ConnectionState holds the state for each client connection
type ConnectionState struct {
	lastSeen time.Time
}

// NewGNetServer creates a new gnet-based proxy server
func NewGNetServer(config Config) (*GNetServer, error) {
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

	return &GNetServer{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// OnBoot fires when the engine is ready for accepting connections
func (s *GNetServer) OnBoot(eng gnet.Engine) gnet.Action {
	s.eng = eng
	logger.Info(fmt.Sprintf("🚀 gnet Redis Proxy Server starting on %s", s.config.Address))
	logger.Info(fmt.Sprintf("📡 Forwarding to Redis server: %s", s.config.RedisAddr))

	// Setup graceful shutdown in a separate goroutine
	go s.setupGracefulShutdown()

	return gnet.None
}

// OnShutdown fires when the engine is being shut down
func (s *GNetServer) OnShutdown(eng gnet.Engine) {
	logger.Info("gnet engine shutting down...")

	// Close all connections
	s.connections.Range(func(key, value interface{}) bool {
		conn := key.(gnet.Conn)
		conn.Close()
		return true
	})

	// Close proxy handler
	if s.handler != nil {
		_ = s.handler.Close()
	}

	logger.Info("✅ gnet proxy server shutdown completed")
}

// OnOpen fires when a new connection is established
func (s *GNetServer) OnOpen(c gnet.Conn) ([]byte, gnet.Action) {
	logger.Info(fmt.Sprintf("proxy connection established: %s", c.RemoteAddr().String()))

	// Create connection state
	state := &ConnectionState{
		lastSeen: time.Now(),
	}

	// Store connection state
	s.connections.Store(c, state)

	return nil, gnet.None
}

// OnClose fires when a connection is closed
func (s *GNetServer) OnClose(c gnet.Conn, err error) gnet.Action {
	logger.Info(fmt.Sprintf("connection closed: %s", c.RemoteAddr().String()))
	if err != nil {
		logger.Error(fmt.Sprintf("connection error: %v", err))
	}

	// Clean up connection state
	s.connections.Delete(c)

	return gnet.None
}

// OnTraffic fires when data arrives on a connection
func (s *GNetServer) OnTraffic(c gnet.Conn) gnet.Action {
	// Get connection state
	stateVal, exists := s.connections.Load(c)
	if !exists {
		logger.Error("connection state not found")
		return gnet.Close
	}

	state := stateVal.(*ConnectionState)
	state.lastSeen = time.Now()

	// Read all available data from connection
	data, err := c.Peek(-1)
	if err != nil {
		logger.Error(fmt.Sprintf("failed to peek data: %v", err))
		return gnet.Close
	}

	// Discard the data we just read
	_, _ = c.Discard(-1)

	// Create adapter to make gnet.Conn compatible with net.Conn
	adapter := NewGnetConnAdapter(c, data)

	// Use the existing ProxyHandler.Handle method - same logic as traditional server!
	s.handler.Handle(s.ctx, adapter)

	return gnet.None
}

// ListenAndServe starts the gnet proxy server
func (s *GNetServer) ListenAndServe() error {
	// Configure gnet options
	options := []gnet.Option{
		gnet.WithMulticore(true),
		gnet.WithTCPKeepAlive(time.Minute * 5),
		gnet.WithTCPNoDelay(gnet.TCPNoDelay),
		gnet.WithReusePort(true),
	}

	// Start the gnet server
	// gnet expects network://address format for TCP
	address := "tcp://" + s.config.Address
	logger.Info(fmt.Sprintf("starting gnet server on %s", address))
	err := gnet.Run(s, address, options...)
	if err != nil {
		return fmt.Errorf("failed to start gnet server: %w", err)
	}

	return nil
}

// setupGracefulShutdown sets up graceful shutdown handling
func (s *GNetServer) setupGracefulShutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	sig := <-c
	logger.Info(fmt.Sprintf("received signal %v, shutting down gnet server...", sig))

	// Shutdown the gnet engine
	_ = s.eng.Stop(context.Background())
}

// GetAddress returns the server's listening address
func (s *GNetServer) GetAddress() string {
	return s.config.Address
}

// GetPoolStats returns connection pool statistics
func (s *GNetServer) GetPoolStats() PoolStats {
	if s.handler == nil {
		return PoolStats{}
	}
	return s.handler.GetPoolStats()
}
