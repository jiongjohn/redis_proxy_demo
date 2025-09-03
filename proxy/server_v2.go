package proxy

import (
	"log"
	"net"
)

// ServerV2 基于proto包的代理服务器
type ServerV2 struct {
	Address string
	Handler *ProxyHandlerV2
}

// NewServerV2 创建新的V2服务器
func NewServerV2(address string, handler *ProxyHandlerV2) *ServerV2 {
	return &ServerV2{
		Address: address,
		Handler: handler,
	}
}

// Start 启动服务器
func (s *ServerV2) Start() error {
	listener, err := net.Listen("tcp", s.Address)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("[INFO][server_v2] 🚀 Redis Proxy V2 (with go-redis proto) listening on %s", s.Address)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[ERROR][server_v2] Failed to accept connection: %v", err)
			continue
		}

		go func() {
			if err := s.Handler.Handle(conn); err != nil {
				log.Printf("[ERROR][server_v2] Handler error: %v", err)
			}
		}()
	}
}
