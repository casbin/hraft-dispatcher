package store

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// StreamLayer implements the raft.StreamLayer interface base on TCP.
type TCPStreamLayer struct {
	ln        net.Listener
	tlsConfig *tls.Config
}

// NewStreamLayer returns a StreamLayer.
func NewTCPStreamLayer(address string, tlsConfig *tls.Config) (*TCPStreamLayer, error) {
	ln, err := tls.Listen("tcp", address, tlsConfig)
	if err != nil {
		return nil, err
	}

	layer := &TCPStreamLayer{
		ln:        ln,
		tlsConfig: tlsConfig,
	}
	return layer, nil
}

// Dial implements the StreamLayer interface.
func (t *TCPStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: timeout,
	}
	return tls.DialWithDialer(dialer, "tcp", string(address), t.tlsConfig)
}

// Accept implements the net.Listener interface.
func (t *TCPStreamLayer) Accept() (c net.Conn, err error) {
	return t.ln.Accept()
}

// Close implements the net.Listener interface.
func (t *TCPStreamLayer) Close() (err error) {
	return t.ln.Close()
}

// Addr implements the net.Listener interface.
func (t *TCPStreamLayer) Addr() net.Addr {
	return t.ln.Addr()
}
