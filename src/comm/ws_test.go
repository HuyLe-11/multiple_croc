package comm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeAndBuildWebSocketURL(t *testing.T) {
	assert.True(t, IsWebSocketAddress("ws://127.0.0.1:8080"))
	assert.True(t, IsWebSocketAddress("  WSS://relay.trycloudflare.com/ws "))
	assert.False(t, IsWebSocketAddress("127.0.0.1:9009"))
	assert.False(t, IsWebSocketAddress("http://127.0.0.1:8080"))

	norm, err := NormalizeWebSocketURL("wss://relay.trycloudflare.com")
	require.NoError(t, err)
	assert.Equal(t, "wss://relay.trycloudflare.com/ws", norm)

	normCustom, err := NormalizeWebSocketURL("ws://127.0.0.1:8080/custom")
	require.NoError(t, err)
	assert.Equal(t, "ws://127.0.0.1:8080/custom", normCustom)

	withPort, err := BuildWebSocketPortURL("wss://relay.trycloudflare.com", "9010")
	require.NoError(t, err)
	assert.Equal(t, "wss://relay.trycloudflare.com/ws?port=9010", withPort)

	joinedWS, err := JoinRelayAddressPort("wss://relay.trycloudflare.com", "9011")
	require.NoError(t, err)
	assert.Equal(t, "wss://relay.trycloudflare.com/ws?port=9011", joinedWS)

	joinedTCP, err := JoinRelayAddressPort("10.0.0.5:9009", "9012")
	require.NoError(t, err)
	assert.Equal(t, "10.0.0.5:9012", joinedTCP)
}
