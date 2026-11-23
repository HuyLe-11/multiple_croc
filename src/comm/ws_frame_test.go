package comm

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Each croc frame written to a WebSocket relay must travel as one WebSocket
// message. net.Buffers.WriteTo falls back to one Write per buffer on non-TCP
// connections, which used to send the 8-byte header and the payload as two
// messages (two frames, two flushes and two TLS records per chunk).
func TestWebSocketCommSendsOneMessagePerFrame(t *testing.T) {
	type received struct {
		typ  websocket.MessageType
		data []byte
	}
	messages := make(chan received, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		socket, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer socket.CloseNow()
		socket.SetReadLimit(maxWSFrameLimit)
		for {
			typ, data, err := socket.Read(r.Context())
			if err != nil {
				close(messages)
				return
			}
			messages <- received{typ: typ, data: data}
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := dialWebSocketContext(ctx, "ws://"+strings.TrimPrefix(server.URL, "http://")+"/ws")
	require.NoError(t, err)
	c := New(conn)

	payload := bytes.Repeat([]byte{0xab}, 32*1024+16)
	require.NoError(t, c.Send(payload))
	require.NoError(t, c.Send([]byte("ack")))

	for _, want := range [][]byte{payload, []byte("ack")} {
		select {
		case msg := <-messages:
			assert.Equal(t, websocket.MessageBinary, msg.typ)
			require.Len(t, msg.data, 8+len(want), "header and payload must share one WebSocket message")
			assert.Equal(t, MAGIC_BYTES, msg.data[:4])
			assert.Equal(t, uint32(len(want)), binary.LittleEndian.Uint32(msg.data[4:8]))
			assert.Equal(t, want, msg.data[8:])
		case <-ctx.Done():
			t.Fatal("timed out waiting for WebSocket message")
		}
	}
	c.Close()
}
