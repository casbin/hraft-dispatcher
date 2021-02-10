package raft

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTCPStreamLayer(t *testing.T) {
	ts := httptest.NewUnstartedServer(nil)
	ts.EnableHTTP2 = true
	ts.StartTLS()
	defer ts.Close()

	layer, err := NewTCPStreamLayer("0.0.0.0:0", ts.TLS)
	assert.NoError(t, err)
	defer layer.Close()
}
