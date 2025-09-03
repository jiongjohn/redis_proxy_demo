package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"redis-proxy-demo/lib/logger"

	"github.com/redis/go-redis/v9"
)

// ClientSession represents a client connection session with state
type ClientSession struct {
	ID           string        // Session ID
	ClientConn   net.Conn      // Client connection
	WatchedKeys  []string      // Keys being watched
	TxQueue      []redis.Cmder // Transaction command queue
	SelectedDB   int           // Currently selected database
	IsInTx       bool          // Whether in transaction
	LastActivity time.Time     // Last activity timestamp
	mu           sync.RWMutex  // Protects session state

	// Redis client - could be shared or per-session
	rdb *redis.Client
}

// ClientSessionManager manages client sessions
type ClientSessionManager struct {
	sessions sync.Map      // net.Conn -> *ClientSession
	rdb      *redis.Client // Shared Redis client
	nextID   int64         // Next session ID
	mu       sync.Mutex    // Protects ID generation

	// Configuration
	config GoRedisConfig

	// Statistics
	stats GoRedisStats
}

// GoRedisConfig represents configuration for go-redis approach
type GoRedisConfig struct {
	RedisAddr      string        `json:"redis_addr"`
	RedisPassword  string        `json:"redis_password"`
	RedisDB        int           `json:"redis_db"`
	PoolSize       int           `json:"pool_size,default=10"`
	MinIdleConns   int           `json:"min_idle_conns,default=2"`
	MaxConnAge     time.Duration `json:"max_conn_age,default=1h"`
	IdleTimeout    time.Duration `json:"idle_timeout,default=30m"`
	DialTimeout    time.Duration `json:"dial_timeout,default=10s"`
	ReadTimeout    time.Duration `json:"read_timeout,default=5s"`
	WriteTimeout   time.Duration `json:"write_timeout,default=5s"`
	SessionTimeout time.Duration `json:"session_timeout,default=1h"`
}

// GoRedisStats holds statistics for go-redis approach
type GoRedisStats struct {
	ActiveSessions   int64     `json:"active_sessions"`
	TotalSessions    int64     `json:"total_sessions"`
	CommandsExecuted int64     `json:"commands_executed"`
	TransactionsExec int64     `json:"transactions_executed"`
	WatchOperations  int64     `json:"watch_operations"`
	PoolHits         int64     `json:"pool_hits"`
	PoolMisses       int64     `json:"pool_misses"`
	LastActivity     time.Time `json:"last_activity"`
	mu               sync.RWMutex
}

// NewClientSessionManager creates a new session manager
func NewClientSessionManager(config GoRedisConfig) *ClientSessionManager {
	// Create Redis client with connection pooling
	rdb := redis.NewClient(&redis.Options{
		Addr:            config.RedisAddr,
		Password:        config.RedisPassword,
		DB:              config.RedisDB,
		PoolSize:        config.PoolSize,
		MinIdleConns:    config.MinIdleConns,
		ConnMaxLifetime: config.MaxConnAge,
		ConnMaxIdleTime: config.IdleTimeout,
		DialTimeout:     config.DialTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := rdb.Ping(ctx).Result()
	if err != nil {
		logger.Error("Failed to connect to Redis: " + err.Error())
		return nil
	}

	manager := &ClientSessionManager{
		rdb:    rdb,
		config: config,
		stats:  GoRedisStats{},
	}

	// Start background cleanup
	go manager.sessionCleanup()
	go manager.statsReporter()

	logger.Info("✅ go-redis session manager initialized")
	logger.Info("📊 Connection pool: " +
		fmt.Sprintf("size=%d, min_idle=%d", config.PoolSize, config.MinIdleConns))

	return manager
}

// CreateSession creates a new client session
func (m *ClientSessionManager) CreateSession(clientConn net.Conn) *ClientSession {
	m.mu.Lock()
	m.nextID++
	sessionID := fmt.Sprintf("session_%d", m.nextID)
	m.mu.Unlock()

	session := &ClientSession{
		ID:           sessionID,
		ClientConn:   clientConn,
		WatchedKeys:  make([]string, 0),
		TxQueue:      make([]redis.Cmder, 0),
		SelectedDB:   0, // Default database
		IsInTx:       false,
		LastActivity: time.Now(),
		rdb:          m.rdb,
	}

	// Store session
	m.sessions.Store(clientConn, session)

	// Update stats
	m.stats.mu.Lock()
	m.stats.ActiveSessions++
	m.stats.TotalSessions++
	m.stats.LastActivity = time.Now()
	m.stats.mu.Unlock()

	logger.Info(fmt.Sprintf("📝 Created session %s for %s", sessionID, clientConn.RemoteAddr()))
	return session
}

// GetSession retrieves session for a client connection
func (m *ClientSessionManager) GetSession(clientConn net.Conn) (*ClientSession, bool) {
	if value, ok := m.sessions.Load(clientConn); ok {
		if session, ok := value.(*ClientSession); ok {
			// Update last activity
			session.mu.Lock()
			session.LastActivity = time.Now()
			session.mu.Unlock()
			return session, true
		}
	}
	return nil, false
}

// RemoveSession removes a client session
func (m *ClientSessionManager) RemoveSession(clientConn net.Conn) {
	if value, ok := m.sessions.LoadAndDelete(clientConn); ok {
		if session, ok := value.(*ClientSession); ok {
			// Cleanup session state
			session.cleanup()

			// Update stats
			m.stats.mu.Lock()
			m.stats.ActiveSessions--
			m.stats.mu.Unlock()

			logger.Info(fmt.Sprintf("🗑️ Removed session %s", session.ID))
		}
	}
}

// Close closes the session manager
func (m *ClientSessionManager) Close() error {
	logger.Info("Closing go-redis session manager...")

	// Close all sessions
	m.sessions.Range(func(key, value interface{}) bool {
		if session, ok := value.(*ClientSession); ok {
			session.cleanup()
		}
		m.sessions.Delete(key)
		return true
	})

	// Close Redis client
	if m.rdb != nil {
		err := m.rdb.Close()
		if err != nil {
			logger.Error("Error closing Redis client: " + err.Error())
		}
	}

	logger.Info("✅ go-redis session manager closed")
	return nil
}

// GetStats returns current statistics
func (m *ClientSessionManager) GetStats() GoRedisStats {
	m.stats.mu.RLock()
	defer m.stats.mu.RUnlock()

	// Get pool stats
	poolStats := m.rdb.PoolStats()

	return GoRedisStats{
		ActiveSessions:   m.stats.ActiveSessions,
		TotalSessions:    m.stats.TotalSessions,
		CommandsExecuted: m.stats.CommandsExecuted,
		TransactionsExec: m.stats.TransactionsExec,
		WatchOperations:  m.stats.WatchOperations,
		PoolHits:         int64(poolStats.Hits),
		PoolMisses:       int64(poolStats.Misses),
		LastActivity:     m.stats.LastActivity,
	}
}

// UpdateStats updates command execution statistics
func (m *ClientSessionManager) UpdateStats(isTransaction bool, isWatch bool) {
	m.stats.mu.Lock()
	defer m.stats.mu.Unlock()

	m.stats.CommandsExecuted++
	m.stats.LastActivity = time.Now()

	if isTransaction {
		m.stats.TransactionsExec++
	}
	if isWatch {
		m.stats.WatchOperations++
	}
}

// Session Methods

// StartWatch starts watching keys
func (s *ClientSession) StartWatch(keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add keys to watched list
	s.WatchedKeys = append(s.WatchedKeys, keys...)

	logger.Debug(fmt.Sprintf("👁️ Session %s watching keys: %v", s.ID, keys))
	return nil
}

// ClearWatch clears all watched keys
func (s *ClientSession) ClearWatch() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.WatchedKeys = s.WatchedKeys[:0]
	logger.Debug(fmt.Sprintf("👁️ Session %s cleared watch", s.ID))
}

// StartTransaction starts a transaction
func (s *ClientSession) StartTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.IsInTx = true
	s.TxQueue = s.TxQueue[:0] // Clear queue

	logger.Debug(fmt.Sprintf("🔄 Session %s started transaction", s.ID))
}

// AddToTransaction adds command to transaction queue
func (s *ClientSession) AddToTransaction(cmd redis.Cmder) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.IsInTx {
		s.TxQueue = append(s.TxQueue, cmd)
		logger.Debug(fmt.Sprintf("📝 Session %s queued command: %v", s.ID, cmd.Args()))
	}
}

// ExecuteTransaction executes the transaction
func (s *ClientSession) ExecuteTransaction(ctx context.Context) ([]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.IsInTx {
		return nil, fmt.Errorf("not in transaction")
	}

	defer s.clearTransaction()

	// If no watched keys, execute directly
	if len(s.WatchedKeys) == 0 {
		return s.executePipeline(ctx)
	}

	// Execute with WATCH
	return s.executeWatchedTransaction(ctx)
}

// DiscardTransaction discards the current transaction
func (s *ClientSession) DiscardTransaction() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clearTransaction()
	logger.Debug(fmt.Sprintf("❌ Session %s discarded transaction", s.ID))
}

// SelectDatabase selects a database
func (s *ClientSession) SelectDatabase(db int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.SelectedDB = db
	logger.Debug(fmt.Sprintf("🎯 Session %s selected database %d", s.ID, db))
	return nil
}

// Private methods

func (s *ClientSession) clearTransaction() {
	s.IsInTx = false
	s.TxQueue = s.TxQueue[:0]
	s.WatchedKeys = s.WatchedKeys[:0]
}

func (s *ClientSession) executePipeline(ctx context.Context) ([]interface{}, error) {
	pipe := s.rdb.Pipeline()

	// Add all commands to pipeline
	for _, cmd := range s.TxQueue {
		pipe.Process(ctx, cmd)
	}

	// Execute pipeline
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}

	// Extract results
	results := make([]interface{}, len(cmds))
	for i, cmd := range cmds {
		results[i] = cmd
	}

	return results, nil
}

func (s *ClientSession) executeWatchedTransaction(ctx context.Context) ([]interface{}, error) {
	// Execute transaction with WATCH
	var results []interface{}
	err := s.rdb.Watch(ctx, func(tx *redis.Tx) error {
		// Execute all commands in transaction
		pipe := tx.TxPipeline()

		for _, cmd := range s.TxQueue {
			pipe.Process(ctx, cmd)
		}

		cmds, err := pipe.Exec(ctx)
		if err != nil {
			return err
		}

		// Extract results
		results = make([]interface{}, len(cmds))
		for i, cmd := range cmds {
			results[i] = cmd
		}

		return nil
	}, s.WatchedKeys...)

	return results, err
}

func (s *ClientSession) cleanup() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear all state
	s.WatchedKeys = s.WatchedKeys[:0]
	s.TxQueue = s.TxQueue[:0]
	s.IsInTx = false
}

// Background tasks

func (m *ClientSessionManager) sessionCleanup() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.cleanupExpiredSessions()
		}
	}
}

func (m *ClientSessionManager) cleanupExpiredSessions() {
	var expiredSessions []net.Conn
	timeout := m.config.SessionTimeout

	m.sessions.Range(func(key, value interface{}) bool {
		clientConn := key.(net.Conn)
		session := value.(*ClientSession)

		session.mu.RLock()
		expired := time.Since(session.LastActivity) > timeout
		session.mu.RUnlock()

		if expired {
			expiredSessions = append(expiredSessions, clientConn)
		}
		return true
	})

	for _, conn := range expiredSessions {
		logger.Debug("🧹 Cleaning up expired session")
		m.RemoveSession(conn)
	}
}

func (m *ClientSessionManager) statsReporter() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			stats := m.GetStats()
			logger.Info(fmt.Sprintf("📊 go-redis Stats: Sessions=%d, Commands=%d, Tx=%d, Pool=%d/%d",
				stats.ActiveSessions, stats.CommandsExecuted, stats.TransactionsExec,
				stats.PoolHits, stats.PoolMisses))
		}
	}
}
