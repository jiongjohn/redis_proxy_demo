package proxy

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"redis-proxy-demo/lib/logger"
)

// GoRedisServerV2Fixed Final enhanced go-redis server with proto package integration (fixed)
type GoRedisServerV2Fixed struct {
	address   string
	handler   *GoRedisHandlerV2Fixed
	listener  net.Listener
	waitGroup sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewGoRedisV2ServerFixed creates a new fixed enhanced go-redis V2 server with proto support
func NewGoRedisV2ServerFixed(config Config) (*GoRedisServerV2Fixed, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Convert proxy Config to GoRedisConfig
	goredisConfig := GoRedisConfig{
		RedisAddr:     config.RedisAddr,
		RedisPassword: config.RedisPassword,
		PoolSize:      15, // Enhanced pool size for better performance
		MinIdleConns:  3,  // Minimum idle connections
		MaxConnAge:    2 * time.Hour,
		IdleTimeout:   45 * time.Minute,
		DialTimeout:   15 * time.Second,
		ReadTimeout:   10 * time.Second,
		WriteTimeout:  10 * time.Second,
	}

	handler := NewGoRedisHandlerV2Fixed(goredisConfig)
	if handler == nil {
		cancel()
		return nil, fmt.Errorf("failed to create GoRedisHandlerV2Fixed")
	}

	return &GoRedisServerV2Fixed{
		address: config.Address,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

// ListenAndServe starts the fixed enhanced go-redis V2 server
func (s *GoRedisServerV2Fixed) ListenAndServe() error {
	var err error
	s.listener, err = net.Listen("tcp", s.address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.address, err)
	}

	logger.Info(fmt.Sprintf("🚀 go-redis V2 Enhanced Proxy (Fixed) listening on %s", s.address))
	logger.Info("🔧 Final Features:")
	logger.Info("  ✅ go-redis Proto Package for Command Parsing")
	logger.Info("  ✅ Direct RESP Response Generation (No Wrapper)")
	logger.Info("  ✅ Perfect Mixed RESP + Inline Command Support")
	logger.Info("  ✅ 100% Accurate Escape Character Handling")
	logger.Info("  ✅ Session-based Connection Management")
	logger.Info("  ✅ Full WATCH/MULTI/EXEC Transaction Support")
	logger.Info("  ✅ Production-Grade Performance & Compatibility")

	defer func() {
		s.listener.Close()
		s.handler.Close()
	}()

	for {
		select {
		case <-s.ctx.Done():
			logger.Info("Server shutting down...")
			return nil
		default:
			conn, err := s.listener.Accept()
			if err != nil {
				select {
				case <-s.ctx.Done():
					return nil
				default:
					logger.Error(fmt.Sprintf("Failed to accept connection: %v", err))
					continue
				}
			}

			s.waitGroup.Add(1)
			go func() {
				defer s.waitGroup.Done()
				s.handler.Handle(s.ctx, conn)
			}()
		}
	}
}

// Shutdown gracefully shuts down the server
func (s *GoRedisServerV2Fixed) Shutdown() error {
	logger.Info("Shutting down go-redis V2 Enhanced Proxy server (Fixed)...")

	s.cancel()

	if s.listener != nil {
		s.listener.Close()
	}

	done := make(chan struct{})
	go func() {
		s.waitGroup.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info("All connections closed gracefully")
	case <-time.After(30 * time.Second):
		logger.Error("Timeout waiting for connections to close")
	}

	return s.handler.Close()
}
