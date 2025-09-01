package proxy

import (
	"io"
	"net"
	"time"

	"github.com/panjf2000/gnet/v2"
)

// GnetConnAdapter adapts gnet.Conn to net.Conn interface
// This allows us to reuse the existing ProxyHandler.Handle method
type GnetConnAdapter struct {
	gnetConn gnet.Conn
	readBuf  []byte
	readPos  int
}

// NewGnetConnAdapter creates a new adapter
func NewGnetConnAdapter(gnetConn gnet.Conn, data []byte) *GnetConnAdapter {
	return &GnetConnAdapter{
		gnetConn: gnetConn,
		readBuf:  data,
		readPos:  0,
	}
}

// Read implements net.Conn.Read
func (g *GnetConnAdapter) Read(b []byte) (n int, err error) {
	// First read from buffered data
	if g.readPos < len(g.readBuf) {
		n = copy(b, g.readBuf[g.readPos:])
		g.readPos += n
		return n, nil
	}

	// No more buffered data, return EOF to indicate end of current packet
	return 0, io.EOF
}

// Write implements net.Conn.Write
func (g *GnetConnAdapter) Write(b []byte) (n int, err error) {
	return g.gnetConn.Write(b)
}

// Close implements net.Conn.Close
func (g *GnetConnAdapter) Close() error {
	return g.gnetConn.Close()
}

// LocalAddr implements net.Conn.LocalAddr
func (g *GnetConnAdapter) LocalAddr() net.Addr {
	return g.gnetConn.LocalAddr()
}

// RemoteAddr implements net.Conn.RemoteAddr
func (g *GnetConnAdapter) RemoteAddr() net.Addr {
	return g.gnetConn.RemoteAddr()
}

// SetDeadline implements net.Conn.SetDeadline
func (g *GnetConnAdapter) SetDeadline(t time.Time) error {
	// gnet doesn't support deadlines in the same way
	return nil
}

// SetReadDeadline implements net.Conn.SetReadDeadline
func (g *GnetConnAdapter) SetReadDeadline(t time.Time) error {
	return nil
}

// SetWriteDeadline implements net.Conn.SetWriteDeadline
func (g *GnetConnAdapter) SetWriteDeadline(t time.Time) error {
	return nil
}
