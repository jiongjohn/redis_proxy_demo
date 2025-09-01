package tcp

import (
	"context"
	"net"
)

// Handler represents a TCP connection handler
type Handler interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}

// ListenAndServeWithSignal starts TCP server with signal handling
func ListenAndServeWithSignal(cfg *Config, handler Handler) error {
	// Placeholder implementation
	return nil
}

// Config represents TCP server configuration
type Config struct {
	Address string
}
