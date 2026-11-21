package tcp

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/crypt"
	"github.com/schollz/pake/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startPhase1TestRelay(t *testing.T, peers []string) (controlAddr string, allPorts []string, cancel context.CancelFunc) {
	t.Helper()
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)

	listeners := make([]net.Listener, 3)
	ports := make([]string, 3)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = l
		_, port, err := net.SplitHostPort(l.Addr().String())
		require.NoError(t, err)
		ports[i] = port
	}

	capSet, err := NewRelayCapabilitySet(2)
	require.NoError(t, err)
	dataBanner := strings.Join(ports[1:], ",")

	for i := 1; i < len(listeners); i++ {
		l := listeners[i]
		go func(listener net.Listener) {
			_ = RunWithListener(
				listener,
				"pass123",
				WithLogLevel("warn"),
				WithFastAdmission(capSet),
				WithCtx(ctx),
			)
		}(l)
	}
	go func(listener net.Listener) {
		_ = RunWithListener(
			listener,
			"pass123",
			WithBanner(dataBanner),
			WithLogLevel("warn"),
			WithFastAdmission(capSet),
			WithPeers(peers),
			WithCtx(ctx),
		)
	}(listeners[0])

	// Wait until all ports are ready.
	for _, p := range ports {
		require.Eventually(t, func() bool {
			_, err := MeasureServerLatency("127.0.0.1:"+p, 250*time.Millisecond)
			return err == nil
		}, 3*time.Second, 20*time.Millisecond)
	}
	return "127.0.0.1:" + ports[0], ports, cancelFn
}

func TestRelayMetaBackwardsCompatibility(t *testing.T) {
	// Zero metadata should produce an empty string so default relays stay unchanged.
	encodedZero, err := EncodeRelayMeta(RelayMeta{})
	require.NoError(t, err)
	assert.Equal(t, "", encodedZero)

	peers := []string{" wss://peer-b.trycloudflare.com ", "10.0.0.2:9009, 10.0.0.2:9009"}
	controlAddr, ports, _ := startPhase1TestRelay(t, peers)

	// 1. Upgraded client reading metadata via ConnectToTCPServerControlMetaContext.
	c1, banner1, ip1, cap1, meta1, err := ConnectToTCPServerControlMetaContext(
		context.Background(),
		controlAddr,
		"pass123",
		"meta-room-1",
		2*time.Second,
	)
	require.NoError(t, err)
	defer c1.Close()
	assert.Equal(t, strings.Join(ports[1:], ","), banner1)
	assert.NotEmpty(t, ip1)
	assert.NotEmpty(t, cap1)
	assert.Equal(t, []string{"wss://peer-b.trycloudflare.com", "10.0.0.2:9009"}, meta1.Peers)

	// 2. Legacy client calling ConnectToTCPServerControlContext ignores parts[3].
	c2, banner2, ip2, cap2, err := ConnectToTCPServerControlContext(
		context.Background(),
		controlAddr,
		"pass123",
		"meta-room-1",
		2*time.Second,
	)
	require.NoError(t, err)
	defer c2.Close()
	assert.Equal(t, banner1, banner2)
	assert.NotEmpty(t, ip2)
	assert.NotEmpty(t, cap2)

	// Verify the paired control room works end-to-end.
	require.NoError(t, c1.Send([]byte("hello-from-c1")))
	for {
		msg, recvErr := c2.ReceiveWithDeadline(time.Now().Add(2 * time.Second))
		require.NoError(t, recvErr)
		if bytes.Equal(msg, []byte{1}) {
			continue
		}
		assert.Equal(t, []byte("hello-from-c1"), msg)
		break
	}
}

// legacySlowHandshake simulates a legacy client that waits for the banner
// before sending the room name (causing the relay's 5ms earlyRoom read deadline
// to time out). This verifies both that parts[2] is empty (not confused with
// parts[3] metadata) and that over WebSocket the 5ms timeout does not kill the
// WebSocket connection.
func legacySlowHandshake(t *testing.T, address, password, room string) (conn *comm.Comm, parts []string) {
	t.Helper()
	c, err := comm.NewConnectionContext(context.Background(), address, 3*time.Second)
	require.NoError(t, err)

	A, err := pake.InitCurve(weakKey, 0, "siec")
	require.NoError(t, err)
	require.NoError(t, c.Send(A.Bytes()))
	Bbytes, err := c.Receive()
	require.NoError(t, err)
	require.NoError(t, A.Update(Bbytes))
	strongKey, err := A.SessionKey()
	require.NoError(t, err)
	strongKeyForEncryption, salt, err := crypt.New(strongKey, nil)
	require.NoError(t, err)
	require.NoError(t, c.Send(salt))

	bPass, err := crypt.Encrypt([]byte(password), strongKeyForEncryption)
	require.NoError(t, err)
	require.NoError(t, c.Send(bPass))

	// Do NOT pipeline the room frame; wait >5ms for the relay's banner first.
	encBanner, err := c.Receive()
	require.NoError(t, err)
	rawBanner, err := crypt.Decrypt(encBanner, strongKeyForEncryption)
	require.NoError(t, err)
	parts = strings.Split(string(rawBanner), "|||")

	bRoom, err := crypt.Encrypt([]byte(room), strongKeyForEncryption)
	require.NoError(t, err)
	require.NoError(t, c.Send(bRoom))

	encOK, err := c.Receive()
	require.NoError(t, err)
	rawOK, err := crypt.Decrypt(encOK, strongKeyForEncryption)
	require.NoError(t, err)
	require.Equal(t, "ok", string(rawOK))

	return c, parts
}

func TestWebSocketBridgeAndClientDial(t *testing.T) {
	peers := []string{"wss://backup.trycloudflare.com"}
	_, ports, _ := startPhase1TestRelay(t, peers)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bridge, err := StartWebSocketBridge(ctx, "127.0.0.1:0", "127.0.0.1", ports)
	require.NoError(t, err)
	defer bridge.Close()

	// 1. Health check endpoint works.
	resp, err := http.Get(fmt.Sprintf("http://%s/healthz", bridge.Addr().String()))
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// 2. Disallowed port is rejected with HTTP 403 (causing WS dial failure).
	badPortURL, err := comm.BuildWebSocketPortURL(bridge.URL(), "65530")
	require.NoError(t, err)
	_, err = comm.NewConnectionContext(ctx, badPortURL, time.Second)
	require.Error(t, err)

	// 3. Ping/pong latency measurement works over bare ws://host:port (no /ws or ?port).
	bareWS := "ws://" + bridge.Addr().String()
	rtt, err := MeasureServerLatencyContext(ctx, bareWS, 2*time.Second)
	require.NoError(t, err)
	assert.Greater(t, rtt, time.Duration(0))

	// 4. Non-pipelining legacy client over WebSocket survives the 5ms earlyRoom timeout
	// and gets parts[2] == "" and parts[3] == "meta-v1:...".
	slowConn, slowParts := legacySlowHandshake(t, bareWS, "pass123", "ws-slow-room")
	defer slowConn.Close()
	require.Len(t, slowParts, 4)
	assert.Equal(t, strings.Join(ports[1:], ","), slowParts[0])
	assert.Equal(t, "", slowParts[2], "parts[2] capability must be empty when room was not pipelined")
	parsedMeta, ok := ParseRelayMeta(slowParts[3])
	require.True(t, ok)
	assert.Equal(t, peers, parsedMeta.Peers)

	// 5. Full control + fast-admission data channel pairing over WebSocket bridge.
	senderCtrl, bannerS, _, capS, metaS, err := ConnectToTCPServerControlMetaContext(ctx, bareWS, "pass123", "ws-room-pair", 3*time.Second)
	require.NoError(t, err)
	defer senderCtrl.Close()
	assert.Equal(t, peers, metaS.Peers)
	require.NotEmpty(t, capS)

	recvCtrl, bannerR, _, capR, metaR, err := ConnectToTCPServerControlMetaContext(ctx, bareWS, "pass123", "ws-room-pair", 3*time.Second)
	require.NoError(t, err)
	defer recvCtrl.Close()
	assert.Equal(t, bannerS, bannerR)
	assert.Equal(t, peers, metaR.Peers)
	require.NotEmpty(t, capR)

	dataPort := strings.Split(bannerS, ",")[0]
	wsDataURL, err := comm.JoinRelayAddressPort(bareWS, dataPort)
	require.NoError(t, err)

	type dataConnectResult struct {
		conn *comm.Comm
		fast bool
		err  error
	}
	senderDataCh := make(chan dataConnectResult, 1)
	go func() {
		c, _, _, fast, connErr := ConnectToTCPServerWithCapabilityContext(ctx, wsDataURL, "pass123", "ws-room-pair-0", capS, 3*time.Second)
		senderDataCh <- dataConnectResult{conn: c, fast: fast, err: connErr}
	}()
	recvData, _, _, fastR, err := ConnectToTCPServerWithCapabilityContext(ctx, wsDataURL, "pass123", "ws-room-pair-0", capR, 3*time.Second)
	require.NoError(t, err)
	defer recvData.Close()
	senderRes := <-senderDataCh
	require.NoError(t, senderRes.err)
	defer senderRes.conn.Close()

	assert.True(t, senderRes.fast, "expected fast admission over WebSocket data channel")
	assert.True(t, fastR, "expected fast admission over WebSocket data channel")

	payload := bytes.Repeat([]byte("ws-data-chunk-"), 2048)
	require.NoError(t, senderRes.conn.Send(payload))
	for {
		got, recvErr := recvData.ReceiveWithDeadline(time.Now().Add(3 * time.Second))
		require.NoError(t, recvErr)
		if bytes.Equal(got, []byte{1}) {
			continue
		}
		assert.Equal(t, payload, got)
		break
	}
}
