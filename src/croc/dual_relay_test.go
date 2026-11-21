package croc

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/tcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChunkBitmapAndSidecarPortability(t *testing.T) {
	const totalChunks = 150
	const chunkSize = 32 * 1024
	const fileSize = int64(totalChunks)*chunkSize - 500
	fileHash := []byte("0123456789abcdef")

	bm := newChunkBitmap(totalChunks, fileSize, chunkSize)
	assert.Equal(t, 0, bm.Count())
	assert.False(t, bm.AllMarked())

	// First mark succeeds; duplicate mark returns false.
	assert.True(t, bm.Mark(0))
	assert.False(t, bm.Mark(0))
	assert.True(t, bm.Mark(1))
	assert.True(t, bm.Mark(3))
	assert.Equal(t, 3, bm.Count())

	ack := bm.BuildAck(2, -1)
	assert.Equal(t, 2, ack.FileIndex)
	assert.Equal(t, uint32(2), ack.CumChunk)
	// Chunk 3 is at offset CumChunk+1 (bit 0 of Bitmask).
	assert.NotZero(t, ack.Bitmask&1)

	sidecarPath := filepath.Join(t.TempDir(), "test.partial.crocmap")
	require.NoError(t, bm.SaveSidecar(sidecarPath, fileHash))

	loaded, err := LoadChunkBitmapSidecar(sidecarPath, fileSize, chunkSize, fileHash)
	require.NoError(t, err)
	assert.Equal(t, bm.Count(), loaded.Count())
	assert.True(t, loaded.Has(0))
	assert.True(t, loaded.Has(1))
	assert.False(t, loaded.Has(2))
	assert.True(t, loaded.Has(3))

	// Wrong file hash must be rejected.
	_, err = LoadChunkBitmapSidecar(sidecarPath, fileSize, chunkSize, []byte("wrong-hash"))
	require.Error(t, err)

	missing := loaded.MissingRanges()
	require.NotEmpty(t, missing)
	assert.Equal(t, int64(chunkSize), missing[0])
}

func TestReliableChunkSchedulerDynamicAndParity(t *testing.T) {
	const chunkSize = int64(32 * 1024)
	const totalChunks = 20
	const fileSize = chunkSize * totalChunks

	// 1. Parity scheduling: Relay 0 claims even chunks, Relay 1 claims odd chunks.
	doneCalled := false
	sched := newReliableChunkScheduler(nil, fileSize, chunkSize, ChunkPolicyParity, 2, func() {
		doneCalled = true
	})

	ctx := context.Background()
	var evenClaimed, oddClaimed []int
	for i := 0; i < totalChunks/2; i++ {
		_, idx0, ok0 := sched.Claim(ctx, 0, 1)
		require.True(t, ok0)
		assert.Equal(t, 0, idx0%2, "relay 0 should claim even chunk indices in parity mode")
		evenClaimed = append(evenClaimed, idx0)

		_, idx1, ok1 := sched.Claim(ctx, 1, 2)
		require.True(t, ok1)
		assert.Equal(t, 1, idx1%2, "relay 1 should claim odd chunk indices in parity mode")
		oddClaimed = append(oddClaimed, idx1)
	}
	assert.Len(t, evenClaimed, 10)
	assert.Len(t, oddClaimed, 10)

	// Simulate Relay 0 ACKing all its even chunks while Relay 1 dies before ACKing odd chunks.
	var ackedEven []uint32
	for _, idx := range evenClaimed {
		ackedEven = append(ackedEven, uint32(idx))
	}
	assert.False(t, sched.ApplyAck(ChunkAckPayload{AckedList: ackedEven, FailedRelay: -1}))
	requeued := sched.RequeueRelay(1)
	assert.Equal(t, 10, requeued, "all 10 unacknowledged odd chunks on relay 1 must be requeued")

	// Surviving Relay 0 can now claim all 10 requeued odd chunks.
	for i := 0; i < 10; i++ {
		_, idx, ok := sched.Claim(ctx, 0, 1)
		require.True(t, ok)
		assert.Equal(t, 1, idx%2)
	}
	assert.True(t, sched.ApplyAck(ChunkAckPayload{CumChunk: totalChunks, Finished: true}))
	assert.True(t, doneCalled)
}

type controllableTestRelay struct {
	address     string
	controlPort string
	dataPorts   []string
	allPorts    []string
	wsBridge    *tcp.WebSocketBridge
	cancel      context.CancelFunc
	listeners   []net.Listener
	mu          sync.Mutex
	activeConns []net.Conn
}

type trackingListener struct {
	net.Listener
	owner *controllableTestRelay
}

func (l *trackingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil && conn != nil {
		l.owner.mu.Lock()
		l.owner.activeConns = append(l.owner.activeConns, conn)
		l.owner.mu.Unlock()
	}
	return conn, err
}

func (r *controllableTestRelay) Kill() {
	r.cancel()
	if r.wsBridge != nil {
		_ = r.wsBridge.Close()
	}
	for _, l := range r.listeners {
		_ = l.Close()
	}
	r.mu.Lock()
	conns := append([]net.Conn(nil), r.activeConns...)
	r.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func startControllableRelay(t *testing.T, dataPortCount int, peers []string, withWS bool) *controllableTestRelay {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &controllableTestRelay{cancel: cancel}
	t.Cleanup(r.Kill)

	count := dataPortCount + 1
	ports := make([]string, count)
	for i := 0; i < count; i++ {
		rawL, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		tl := &trackingListener{Listener: rawL, owner: r}
		r.listeners = append(r.listeners, tl)
		_, port, err := net.SplitHostPort(rawL.Addr().String())
		require.NoError(t, err)
		ports[i] = port
	}
	r.controlPort = ports[0]
	r.dataPorts = ports[1:]
	r.allPorts = ports
	r.address = net.JoinHostPort("127.0.0.1", r.controlPort)

	capSet, err := tcp.NewRelayCapabilitySet(dataPortCount)
	require.NoError(t, err)
	banner := strings.Join(r.dataPorts, ",")

	for i := 1; i < count; i++ {
		l := r.listeners[i]
		go func(listener net.Listener) {
			_ = tcp.RunWithListener(
				listener,
				"pass123",
				tcp.WithLogLevel("warn"),
				tcp.WithFastAdmission(capSet),
				tcp.WithCtx(ctx),
			)
		}(l)
	}
	go func(listener net.Listener) {
		_ = tcp.RunWithListener(
			listener,
			"pass123",
			tcp.WithBanner(banner),
			tcp.WithLogLevel("warn"),
			tcp.WithFastAdmission(capSet),
			tcp.WithPeers(peers),
			tcp.WithCtx(ctx),
		)
	}(r.listeners[0])

	waitForRelayPorts(t, ports)

	if withWS {
		bridge, err := tcp.StartWebSocketBridge(ctx, "127.0.0.1:0", "127.0.0.1", ports)
		require.NoError(t, err)
		r.wsBridge = bridge
	}
	return r
}

func runDualRelayTransferTest(t *testing.T, relayAAddr, relay2Flag, policy, throttle string, fileSize int, hook func(sender, receiver *Client)) (sender, receiver *Client) {
	t.Helper()
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	payload := make([]byte, fileSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	srcSum := sha256.Sum256(payload)

	srcFile := filepath.Join(srcDir, "payload.bin")
	require.NoError(t, os.WriteFile(srcFile, payload, 0o600))

	secret := "dual-relay-test-secret"
	sender, err = New(Options{
		IsSender:         true,
		SharedSecret:     secret,
		RelayAddress:     relayAAddr,
		RelayPassword:    "pass123",
		DisableLocal:     true,
		NoPrompt:         true,
		NoCompress:       true,
		Quiet:            true,
		DisableClipboard: true,
		Transport:        TransportRelay,
		DualRelay:        true,
		SecondaryRelay:   relay2Flag,
		ChunkPolicy:      policy,
		ThrottleUpload:   throttle,
		Curve:            "p256",
		HashAlgorithm:    "xxhash",
	})
	require.NoError(t, err)

	receiver, err = New(Options{
		IsSender:         false,
		SharedSecret:     secret,
		RelayAddress:     relayAAddr,
		RelayPassword:    "pass123",
		DisableLocal:     true,
		NoPrompt:         true,
		Overwrite:        true,
		Quiet:            true,
		DisableClipboard: true,
		Curve:            "p256",
		HashAlgorithm:    "xxhash",
	})
	require.NoError(t, err)
	t.Chdir(dstDir)

	filesInfo, emptyFolders, totalFolders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
	require.NoError(t, err)

	results := make(chan error, 2)
	go func() {
		results <- sender.Send(filesInfo, emptyFolders, totalFolders)
	}()
	time.Sleep(80 * time.Millisecond)
	go func() {
		results <- receiver.Receive()
	}()

	if hook != nil {
		go hook(sender, receiver)
	}

	waitForTransferResults(t, results, 2, 25*time.Second, sender, receiver)

	receivedBytes, err := os.ReadFile(filepath.Join(dstDir, "payload.bin"))
	require.NoError(t, err)
	gotSum := sha256.Sum256(receivedBytes)
	require.True(t, bytes.Equal(srcSum[:], gotSum[:]), "SHA-256 of received file must match source file")
	return sender, receiver
}

func TestDualRelayDynamicAndParityTransfer(t *testing.T) {
	const fileSize = 64 * int(models.TCP_BUFFER_SIZE/2) // 64 chunks = 2 MiB

	t.Run("dynamic-via-banner-peers-and-websocket-relayB", func(t *testing.T) {
		relayB := startControllableRelay(t, 2, nil, true)
		// Relay A advertises Relay B's WebSocket URL in --peers (simulating Mode b Cloudflare Tunnel).
		relayA := startControllableRelay(t, 2, []string{relayB.wsBridge.URL()}, false)

		sender, receiver := runDualRelayTransferTest(t, relayA.address, "", ChunkPolicyDynamic, "", fileSize, nil)
		assert.True(t, sender.DualRelayActive())
		assert.True(t, receiver.DualRelayActive())
		assert.Greater(t, sender.DualRelayChunksSent(0), int64(0), "primary relay A must send chunks")
		assert.Greater(t, sender.DualRelayChunksSent(1), int64(0), "secondary WS relay B must send chunks")
		assert.Equal(t, int64(64), receiver.DualRelayChunksReceived(0)+receiver.DualRelayChunksReceived(1))
	})

	t.Run("parity-even-odd-split-across-two-tcp-relays", func(t *testing.T) {
		relayB := startControllableRelay(t, 2, nil, false)
		relayA := startControllableRelay(t, 2, []string{relayB.address}, false)

		sender, receiver := runDualRelayTransferTest(t, relayA.address, "", ChunkPolicyParity, "", fileSize, nil)
		assert.True(t, sender.DualRelayActive())
		assert.True(t, receiver.DualRelayActive())
		assert.Equal(t, int64(32), sender.DualRelayChunksSent(0), "relay A must send 32 even chunks")
		assert.Equal(t, int64(32), sender.DualRelayChunksSent(1), "relay B must send 32 odd chunks")
		assert.Equal(t, int64(32), receiver.DualRelayChunksReceived(0))
		assert.Equal(t, int64(32), receiver.DualRelayChunksReceived(1))
	})
}

func TestDualRelayFallbackWhenPeerUnavailable(t *testing.T) {
	const fileSize = 24 * int(models.TCP_BUFFER_SIZE/2)
	// Relay A advertises an unreachable peer address.
	relayA := startControllableRelay(t, 2, []string{"127.0.0.1:1"}, false)

	sender, receiver := runDualRelayTransferTest(t, relayA.address, "", ChunkPolicyDynamic, "", fileSize, nil)
	assert.False(t, sender.DualRelayActive(), "should fall back to single relay when peer is unreachable")
	assert.False(t, receiver.DualRelayActive())
	assert.Equal(t, int64(24), sender.DualRelayChunksSent(0))
	assert.Equal(t, int64(0), sender.DualRelayChunksSent(1))
}

func TestDualRelayFailoverSecondaryDiesMidTransfer(t *testing.T) {
	const fileSize = 96 * int(models.TCP_BUFFER_SIZE/2) // 3 MiB = 96 chunks
	relayB := startControllableRelay(t, 2, nil, true)
	relayA := startControllableRelay(t, 2, []string{relayB.wsBridge.URL()}, false)

	killed := make(chan struct{})
	sender, _ := runDualRelayTransferTest(t, relayA.address, "", ChunkPolicyParity, "1500k", fileSize, func(s, _ *Client) {
		defer close(killed)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if s.DualRelayChunksSent(1) >= 8 {
				relayB.Kill()
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	})
	<-killed
	assert.GreaterOrEqual(t, sender.DualRelayChunksSent(1), int64(8))
	assert.Greater(t, sender.DualRelayChunksSent(0), int64(48), "surviving relay A must take over relay B's remaining odd chunks")
}

func TestDualRelayFailoverPrimaryControlDiesMidTransfer(t *testing.T) {
	const fileSize = 96 * int(models.TCP_BUFFER_SIZE/2) // 3 MiB = 96 chunks
	relayB := startControllableRelay(t, 2, nil, false)
	relayA := startControllableRelay(t, 2, []string{relayB.address}, false)

	killed := make(chan struct{})
	sender, _ := runDualRelayTransferTest(t, relayA.address, "", ChunkPolicyDynamic, "1500k", fileSize, func(s, _ *Client) {
		defer close(killed)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			if s.DualRelayChunksSent(0) >= 6 && s.DualRelayChunksSent(1) >= 6 {
				// Kill Primary Relay A (both control and data ports)!
				relayA.Kill()
				return
			}
			time.Sleep(15 * time.Millisecond)
		}
	})
	<-killed
	assert.True(t, sender.dualRelay.controlPromoted.Load(), "standby control on relay B must be promoted when primary relay A dies")
	assert.Greater(t, sender.DualRelayChunksSent(1), int64(48), "surviving relay B must finish remaining chunks after relay A dies")
}
