package proxy

import (
	"context"
	"fmt"
	"io"
	"net"
	"redis-proxy-demo/pool"
	"sync"
	"time"

	"redis-proxy-demo/lib/logger"
)

// AffinityHandler implements connection affinity proxy
// Each client connection gets a dedicated Redis connection
type AffinityHandler struct {
	config     AffinityConfig
	clientConn sync.Map // net.Conn -> *AffinityConnection
	stats      *AffinityStats
	closing    chan struct{}
	wg         sync.WaitGroup

	// Connection tracking
	connectionCount int64
	mu              sync.RWMutex
}

// AffinityConfig represents configuration for connection affinity
type AffinityConfig struct {
	RedisAddr           string        // Redis server address
	RedisPassword       string        // Redis password (if required)
	MaxConnections      int           // Maximum concurrent connections
	IdleTimeout         time.Duration // Idle connection timeout
	HealthCheckInterval time.Duration // Health check interval
	BufferSize          int           // Buffer size for data forwarding
}

// AffinityConnection represents a client-redis connection pair
type AffinityConnection struct {
	clientConn net.Conn      // Client connection
	redisConn  net.Conn      // Redis connection
	lastActive time.Time     // Last activity time
	closing    chan struct{} // Closing signal
	closed     bool          // Connection closed flag
	mu         sync.RWMutex
}

// AffinityStats holds connection statistics
type AffinityStats struct {
	ActiveConnections  int64     `json:"active_connections"`
	TotalConnections   int64     `json:"total_connections"`
	BytesTransferred   int64     `json:"bytes_transferred"`
	ConnectionsCreated int64     `json:"connections_created"`
	ConnectionsClosed  int64     `json:"connections_closed"`
	LastActivity       time.Time `json:"last_activity"`
	mu                 sync.RWMutex
}

// NewAffinityHandler creates a new connection affinity handler
func NewAffinityHandler(config AffinityConfig) *AffinityHandler {
	if config.BufferSize == 0 {
		config.BufferSize = 32 * 1024 // 32KB default buffer
	}
	if config.IdleTimeout == 0 {
		config.IdleTimeout = 5 * time.Minute
	}
	if config.HealthCheckInterval == 0 {
		config.HealthCheckInterval = 30 * time.Second
	}
	if config.MaxConnections == 0 {
		config.MaxConnections = 1000
	}

	handler := &AffinityHandler{
		config:  config,
		stats:   &AffinityStats{},
		closing: make(chan struct{}),
	}

	// Start background tasks
	go handler.healthCheckLoop()
	go handler.idleConnectionCleanup()
	go handler.statsReporter()

	return handler
}

// Handle handles a client connection with connection affinity
func (h *AffinityHandler) Handle(ctx context.Context, clientConn net.Conn) {
	// Check connection limits
	if !h.checkConnectionLimit() {
		logger.Warn(fmt.Sprintf("Connection limit exceeded, rejecting client: %s", clientConn.RemoteAddr()))
		clientConn.Close()
		return
	}

	h.wg.Add(1)
	defer h.wg.Done()

	logger.Info(fmt.Sprintf("🔗 New client connection (affinity mode): %s", clientConn.RemoteAddr()))

	// Create Redis connection for this client
	redisConn, err := h.createRedisConnection()
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to create Redis connection: %v", err))
		clientConn.Close()
		return
	}

	// Create affinity connection
	affinityConn := &AffinityConnection{
		clientConn: clientConn,
		redisConn:  redisConn,
		lastActive: time.Now(),
		closing:    make(chan struct{}),
	}

	// Store the connection
	h.clientConn.Store(clientConn, affinityConn)
	h.incrementConnectionCount()

	// Update stats
	h.stats.mu.Lock()
	h.stats.ActiveConnections++
	h.stats.TotalConnections++
	h.stats.ConnectionsCreated++
	h.stats.LastActivity = time.Now()
	h.stats.mu.Unlock()

	logger.Info(fmt.Sprintf("✅ Redis connection established for client %s -> %s",
		clientConn.RemoteAddr(), redisConn.RemoteAddr()))

	// Start bidirectional data forwarding
	go h.forwardData(affinityConn, clientConn, redisConn, "Client->Redis")
	go h.forwardData(affinityConn, redisConn, clientConn, "Redis->Client")

	// Wait for connection to close
	h.waitForConnectionClose(affinityConn)

	// Cleanup
	h.cleanupConnection(affinityConn)
}

// createRedisConnection creates a new Redis connection
func (h *AffinityHandler) createRedisConnection() (net.Conn, error) {
	conn, err := pool.CreateConnection(&pool.RedisConnConfig{
		Addr:     h.config.RedisAddr,
		Password: h.config.RedisPassword,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return conn, nil
}

// forwardData forwards data between connections
func (h *AffinityHandler) forwardData(affinityConn *AffinityConnection, src, dst net.Conn, direction string) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error(fmt.Sprintf("Data forwarding panic [%s]: %v", direction, r))
		}
		affinityConn.signalClose()
	}()

	buffer := make([]byte, h.config.BufferSize)

	for {
		select {
		case <-affinityConn.closing:
			return
		default:
		}

		// Set read timeout
		src.SetReadDeadline(time.Now().Add(30 * time.Second))

		n, err := src.Read(buffer)
		if err != nil {
			if err != io.EOF && !isConnectionClosed(err) {
				logger.Debug(fmt.Sprintf("Read error [%s]: %v", direction, err))
			}
			return
		}

		if n > 0 {
			// Update activity timestamp
			affinityConn.updateActivity()

			// Forward data
			_, err = dst.Write(buffer[:n])
			if err != nil {
				logger.Debug(fmt.Sprintf("Write error [%s]: %v", direction, err))
				return
			}

			// Update stats
			h.stats.mu.Lock()
			h.stats.BytesTransferred += int64(n)
			h.stats.LastActivity = time.Now()
			h.stats.mu.Unlock()

			logger.Debug(fmt.Sprintf("📡 Forwarded %d bytes [%s]", n, direction))
		}
	}
}

// waitForConnectionClose waits for connection to close
func (h *AffinityHandler) waitForConnectionClose(affinityConn *AffinityConnection) {
	<-affinityConn.closing
	logger.Debug(fmt.Sprintf("Connection closing detected: %s", affinityConn.clientConn.RemoteAddr()))
}

// cleanupConnection cleans up an affinity connection
func (h *AffinityHandler) cleanupConnection(affinityConn *AffinityConnection) {
	affinityConn.mu.Lock()
	defer affinityConn.mu.Unlock()

	if affinityConn.closed {
		return
	}
	affinityConn.closed = true

	logger.Info(fmt.Sprintf("🧹 Cleaning up connection: %s", affinityConn.clientConn.RemoteAddr()))

	// Close connections
	if affinityConn.clientConn != nil {
		affinityConn.clientConn.Close()
	}
	if affinityConn.redisConn != nil {
		affinityConn.redisConn.Close()
	}

	// Remove from map
	h.clientConn.Delete(affinityConn.clientConn)
	h.decrementConnectionCount()

	// Update stats
	h.stats.mu.Lock()
	h.stats.ActiveConnections--
	h.stats.ConnectionsClosed++
	h.stats.mu.Unlock()

	logger.Debug("✅ Connection cleanup completed")
}

// checkConnectionLimit checks if we can accept new connections
func (h *AffinityHandler) checkConnectionLimit() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.connectionCount < int64(h.config.MaxConnections)
}

// incrementConnectionCount increments connection count
func (h *AffinityHandler) incrementConnectionCount() {
	h.mu.Lock()
	h.connectionCount++
	h.mu.Unlock()
}

// decrementConnectionCount decrements connection count
func (h *AffinityHandler) decrementConnectionCount() {
	h.mu.Lock()
	if h.connectionCount > 0 {
		h.connectionCount--
	}
	h.mu.Unlock()
}

// signalClose signals connection closure
func (ac *AffinityConnection) signalClose() {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if !ac.closed {
		close(ac.closing)
	}
}

// updateActivity updates last activity timestamp
func (ac *AffinityConnection) updateActivity() {
	ac.mu.Lock()
	ac.lastActive = time.Now()
	ac.mu.Unlock()
}

// isIdle checks if connection is idle
func (ac *AffinityConnection) isIdle(timeout time.Duration) bool {
	ac.mu.RLock()
	defer ac.mu.RUnlock()
	return time.Since(ac.lastActive) > timeout
}

// Close gracefully closes the handler
func (h *AffinityHandler) Close() error {
	logger.Info("Shutting down affinity handler...")

	close(h.closing)

	// Close all connections
	h.clientConn.Range(func(key, value interface{}) bool {
		if affinityConn, ok := value.(*AffinityConnection); ok {
			h.cleanupConnection(affinityConn)
		}
		return true
	})

	// Wait for all goroutines to finish
	h.wg.Wait()

	logger.Info("✅ Affinity handler shutdown completed")
	return nil
}

// GetStats returns current statistics
func (h *AffinityHandler) GetStats() AffinityStats {
	h.stats.mu.RLock()
	defer h.stats.mu.RUnlock()

	return AffinityStats{
		ActiveConnections:  h.stats.ActiveConnections,
		TotalConnections:   h.stats.TotalConnections,
		BytesTransferred:   h.stats.BytesTransferred,
		ConnectionsCreated: h.stats.ConnectionsCreated,
		ConnectionsClosed:  h.stats.ConnectionsClosed,
		LastActivity:       h.stats.LastActivity,
	}
}

// Background tasks

// healthCheckLoop performs periodic health checks
func (h *AffinityHandler) healthCheckLoop() {
	ticker := time.NewTicker(h.config.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.performHealthCheck()
		}
	}
}

// performHealthCheck checks health of all connections
func (h *AffinityHandler) performHealthCheck() {
	var unhealthyConnections []net.Conn

	h.clientConn.Range(func(key, value interface{}) bool {
		clientConn := key.(net.Conn)
		affinityConn := value.(*AffinityConnection)

		if !h.isConnectionHealthy(affinityConn) {
			unhealthyConnections = append(unhealthyConnections, clientConn)
		}
		return true
	})

	// Cleanup unhealthy connections
	for _, clientConn := range unhealthyConnections {
		if value, ok := h.clientConn.Load(clientConn); ok {
			if affinityConn, ok := value.(*AffinityConnection); ok {
				logger.Debug(fmt.Sprintf("Cleaning up unhealthy connection: %s", clientConn.RemoteAddr()))
				h.cleanupConnection(affinityConn)
			}
		}
	}

	if len(unhealthyConnections) > 0 {
		logger.Info(fmt.Sprintf("Health check completed, cleaned up %d unhealthy connections", len(unhealthyConnections)))
	}
}

// isConnectionHealthy checks if a connection is healthy
func (h *AffinityHandler) isConnectionHealthy(affinityConn *AffinityConnection) bool {
	affinityConn.mu.RLock()
	defer affinityConn.mu.RUnlock()

	// Check if connection is closed
	if affinityConn.closed {
		return false
	}

	// Simple connectivity check - try to set deadline
	if affinityConn.clientConn != nil {
		err := affinityConn.clientConn.SetDeadline(time.Now().Add(1 * time.Millisecond))
		if err != nil {
			return false
		}
		// Reset deadline
		affinityConn.clientConn.SetDeadline(time.Time{})
	}

	if affinityConn.redisConn != nil {
		err := affinityConn.redisConn.SetDeadline(time.Now().Add(1 * time.Millisecond))
		if err != nil {
			return false
		}
		// Reset deadline
		affinityConn.redisConn.SetDeadline(time.Time{})
	}

	return true
}

// idleConnectionCleanup cleans up idle connections
func (h *AffinityHandler) idleConnectionCleanup() {
	ticker := time.NewTicker(h.config.IdleTimeout / 2) // Check twice per idle timeout
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.cleanupIdleConnections()
		}
	}
}

// cleanupIdleConnections removes idle connections
func (h *AffinityHandler) cleanupIdleConnections() {
	var idleConnections []net.Conn

	h.clientConn.Range(func(key, value interface{}) bool {
		clientConn := key.(net.Conn)
		affinityConn := value.(*AffinityConnection)

		if affinityConn.isIdle(h.config.IdleTimeout) {
			idleConnections = append(idleConnections, clientConn)
		}
		return true
	})

	// Cleanup idle connections
	for _, clientConn := range idleConnections {
		if value, ok := h.clientConn.Load(clientConn); ok {
			if affinityConn, ok := value.(*AffinityConnection); ok {
				logger.Debug(fmt.Sprintf("Cleaning up idle connection: %s (idle for %v)",
					clientConn.RemoteAddr(), time.Since(affinityConn.lastActive)))
				h.cleanupConnection(affinityConn)
			}
		}
	}

	if len(idleConnections) > 0 {
		logger.Info(fmt.Sprintf("Idle cleanup completed, removed %d idle connections", len(idleConnections)))
	}
}

// statsReporter reports statistics periodically
func (h *AffinityHandler) statsReporter() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.reportStats()
		}
	}
}

// reportStats logs current statistics
func (h *AffinityHandler) reportStats() {
	stats := h.GetStats()
	logger.Info(fmt.Sprintf("📊 Affinity Stats - Active: %d, Total: %d, Created: %d, Closed: %d, Bytes: %d",
		stats.ActiveConnections, stats.TotalConnections,
		stats.ConnectionsCreated, stats.ConnectionsClosed, stats.BytesTransferred))
}

// Helper functions

func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}

func isConnectionClosed(err error) bool {
	return err != nil && (err == io.EOF ||
		err.Error() == "use of closed network connection" ||
		err.Error() == "connection reset by peer")
}
