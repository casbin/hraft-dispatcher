package store

import (
	"crypto/tls"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTCPStreamLayer(t *testing.T) {
	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	ln, err := tls.Listen("tcp", "0.0.0.0:0", ts.TLS)
	assert.NoError(t, err)

	layer, err := NewTCPStreamLayer(ln, ts.TLS)
	assert.NoError(t, err)
	defer layer.Close()
}
