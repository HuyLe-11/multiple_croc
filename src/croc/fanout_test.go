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
	"github.com/schollz/croc/v11/src/pakekey"
	"github.com/schollz/croc/v11/src/tcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testFanoutRelay struct {
	address string
	ports   []string
	manager *tcp.FanoutManager
	cancel  context.CancelFunc
}

func startTestFanoutRelay(t *testing.T, cfg tcp.FanoutConfig, peers ...string) *testFanoutRelay {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	listeners := make([]net.Listener, 2)
	ports := make([]string, 2)
	for i := range listeners {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		listeners[i] = l
		_, p, err := net.SplitHostPort(l.Addr().String())
		require.NoError(t, err)
		ports[i] = p
	}

	fm := tcp.NewFanoutManager(cfg)
	capSet, err := tcp.NewRelayCapabilitySet(1)
	require.NoError(t, err)

	go func() {
		_ = tcp.RunWithListener(
			listeners[1],
			"pass123",
			tcp.WithLogLevel("warn"),
			tcp.WithFastAdmission(capSet),
			tcp.WithFanoutManager(fm),
			tcp.WithPeers(peers),
			tcp.WithCtx(ctx),
		)
	}()
	go func() {
		_ = tcp.RunWithListener(
			listeners[0],
			"pass123",
			tcp.WithBanner(ports[1]),
			tcp.WithLogLevel("warn"),
			tcp.WithFastAdmission(capSet),
			tcp.WithFanoutManager(fm),
			tcp.WithPeers(peers),
			tcp.WithCtx(ctx),
		)
	}()

	waitForRelayPorts(t, ports)
	return &testFanoutRelay{
		address: net.JoinHostPort("127.0.0.1", ports[0]),
		ports:   ports,
		manager: fm,
		cancel:  cancel,
	}
}

func newTestFanoutSender(t *testing.T, relayAddr, secret string, receivers, minReceivers int, waitTimeout time.Duration) *Client {
	t.Helper()
	c, err := New(Options{
		IsSender:           true,
		SharedSecret:       secret,
		RelayAddress:       relayAddr,
		RelayPassword:      "pass123",
		DisableLocal:       true,
		NoPrompt:           true,
		NoCompress:         true,
		Quiet:              true,
		DisableClipboard:   true,
		Curve:              "p256",
		HashAlgorithm:      "xxhash",
		FanoutReceivers:    receivers,
		FanoutMinReceivers: minReceivers,
		FanoutWaitTimeout:  waitTimeout,
		FanoutMode:         true,
	})
	require.NoError(t, err)
	return c
}

func newTestFanoutReceiver(t *testing.T, relayAddr, secret, outDir string, simulatedRate int64) *Client {
	t.Helper()
	c, err := New(Options{
		IsSender:                 false,
		SharedSecret:             secret,
		RelayAddress:             relayAddr,
		RelayPassword:            "pass123",
		DisableLocal:             true,
		NoPrompt:                 true,
		Overwrite:                true,
		Quiet:                    true,
		DisableClipboard:         true,
		Curve:                    "p256",
		HashAlgorithm:            "xxhash",
		FanoutMode:               true,
		OutputDir:                outDir,
		SimulatedReceiverRateBps: simulatedRate,
	})
	require.NoError(t, err)
	return c
}

func TestFanoutAllFastReceivers(t *testing.T) {
	relay := startTestFanoutRelay(t, tcp.FanoutConfig{
		MaxBufferBytes: 2 << 20,
		MaxMemoryBytes: 16 << 20,
		MaxReceivers:   4,
		MinRateBps:     64 << 10,
		RateWindow:     2 * time.Second,
		StallTimeout:   5 * time.Second,
	})

	srcDir := t.TempDir()
	file1Data := make([]byte, 24*int(models.TCP_BUFFER_SIZE/2)) // 768 KiB
	file2Data := make([]byte, 12*int(models.TCP_BUFFER_SIZE/2)) // 384 KiB
	_, err := rand.Read(file1Data)
	require.NoError(t, err)
	_, err = rand.Read(file2Data)
	require.NoError(t, err)

	f1Path := filepath.Join(srcDir, "alpha.bin")
	f2Path := filepath.Join(srcDir, "beta.bin")
	require.NoError(t, os.WriteFile(f1Path, file1Data, 0o600))
	require.NoError(t, os.WriteFile(f2Path, file2Data, 0o600))

	secret := "fanout-fast-multi-code"
	sender := newTestFanoutSender(t, relay.address, secret, 3, 3, 5*time.Second)
	filesInfo, emptyFolders, totalFolders, err := GetFilesInfo([]string{f1Path, f2Path}, false, false, nil)
	require.NoError(t, err)

	outDirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	receivers := make([]*Client, 3)
	for i := range receivers {
		receivers[i] = newTestFanoutReceiver(t, relay.address, secret, outDirs[i], 0)
	}

	errCh := make(chan error, 4)
	go func() {
		errCh <- sender.Send(filesInfo, emptyFolders, totalFolders)
	}()
	time.Sleep(60 * time.Millisecond)
	for _, r := range receivers {
		go func(rc *Client) {
			errCh <- rc.Receive()
		}(r)
	}

	for range 4 {
		require.NoError(t, <-errCh)
	}

	require.Len(t, sender.FanoutResults, 3)
	for i, res := range sender.FanoutResults {
		assert.Equal(t, "completed", res.Status)
		got1, err := os.ReadFile(filepath.Join(outDirs[i], "alpha.bin"))
		require.NoError(t, err)
		assert.Equal(t, sha256.Sum256(file1Data), sha256.Sum256(got1))

		got2, err := os.ReadFile(filepath.Join(outDirs[i], "beta.bin"))
		require.NoError(t, err)
		assert.Equal(t, sha256.Sum256(file2Data), sha256.Sum256(got2))
	}
}

func TestFanoutBoundedBufferBackpressure(t *testing.T) {
	// Buffer is capped at 256 KiB (8 chunks), while the file is 1 MiB (32 chunks).
	// Receiver #2 runs at 400 KiB/s (above the 100 KiB/s kick threshold).
	const maxBuf = int64(256 * 1024)
	const fileSize = 32 * int(models.TCP_BUFFER_SIZE/2) // 1 MiB

	relay := startTestFanoutRelay(t, tcp.FanoutConfig{
		MaxBufferBytes: maxBuf,
		MaxMemoryBytes: 8 << 20,
		MaxReceivers:   4,
		MinRateBps:     100 * 1024,
		RateWindow:     800 * time.Millisecond,
		StallTimeout:   5 * time.Second,
	})

	srcDir := t.TempDir()
	payload := make([]byte, fileSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	srcFile := filepath.Join(srcDir, "backpressure.bin")
	require.NoError(t, os.WriteFile(srcFile, payload, 0o600))

	secret := "fanout-backpressure-code"
	sender := newTestFanoutSender(t, relay.address, secret, 2, 2, 5*time.Second)
	filesInfo, emptyFolders, totalFolders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
	require.NoError(t, err)

	out1, out2 := t.TempDir(), t.TempDir()
	recvFast := newTestFanoutReceiver(t, relay.address, secret, out1, 0)
	recvSlowAboveThreshold := newTestFanoutReceiver(t, relay.address, secret, out2, 450*1024)

	var peakObserved int64
	stopMonitor := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMonitor:
				return
			case <-ticker.C:
				p := relay.manager.PeakRoomBufferBytes(sender.Options.RoomName)
				if p > peakObserved {
					peakObserved = p
				}
			}
		}
	}()

	errCh := make(chan error, 3)
	go func() { errCh <- sender.Send(filesInfo, emptyFolders, totalFolders) }()
	time.Sleep(60 * time.Millisecond)
	go func() { errCh <- recvFast.Receive() }()
	go func() { errCh <- recvSlowAboveThreshold.Receive() }()

	for range 3 {
		require.NoError(t, <-errCh)
	}
	close(stopMonitor)

	assert.Greater(t, peakObserved, int64(0))
	assert.LessOrEqual(t, peakObserved, maxBuf, "relay ring buffer must never exceed MaxBufferBytes (256 KiB) during 1 MiB transfer")

	for _, dir := range []string{out1, out2} {
		got, err := os.ReadFile(filepath.Join(dir, "backpressure.bin"))
		require.NoError(t, err)
		assert.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
	}
}

func TestFanoutKickSlowReceiverAndResume1to1(t *testing.T) {
	// File is 48 chunks (1.5 MiB); relay kicks any receiver < 350 KiB/s over 350ms window.
	// Receiver #1 & #2 are fast; Receiver #3 is throttled to 75 KiB/s (< 350 KiB/s).
	const chunkSize = int(models.TCP_BUFFER_SIZE / 2)
	const fileSize = 48 * chunkSize

	relay := startTestFanoutRelay(t, tcp.FanoutConfig{
		MaxBufferBytes: 256 * 1024,
		MaxMemoryBytes: 8 << 20,
		MaxReceivers:   4,
		MinRateBps:     350 * 1024,
		RateWindow:     350 * time.Millisecond,
		StallTimeout:   5 * time.Second,
	})

	srcDir := t.TempDir()
	payload := make([]byte, fileSize)
	_, err := rand.Read(payload)
	require.NoError(t, err)
	// Put an explicit all-zero 32 KiB chunk at chunk index 0 so we also verify
	// that .crocmap sidecar resume preserves legitimate all-zero blocks!
	clear(payload[:chunkSize])

	srcFile := filepath.Join(srcDir, "resume-target.bin")
	require.NoError(t, os.WriteFile(srcFile, payload, 0o600))

	secret := "fanout-kick-resume-code"
	sender := newTestFanoutSender(t, relay.address, secret, 3, 3, 5*time.Second)
	filesInfo, emptyFolders, totalFolders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
	require.NoError(t, err)

	out1, out2, out3 := t.TempDir(), t.TempDir(), t.TempDir()
	recv1 := newTestFanoutReceiver(t, relay.address, secret, out1, 0)
	recv2 := newTestFanoutReceiver(t, relay.address, secret, out2, 0)
	recv3Slow := newTestFanoutReceiver(t, relay.address, secret, out3, 75*1024)

	var wg sync.WaitGroup
	var errSend, errR1, errR2, errR3 error
	wg.Add(4)
	go func() { defer wg.Done(); errSend = sender.Send(filesInfo, emptyFolders, totalFolders) }()
	time.Sleep(60 * time.Millisecond)
	go func() { defer wg.Done(); errR1 = recv1.Receive() }()
	go func() { defer wg.Done(); errR2 = recv2.Receive() }()
	go func() { defer wg.Done(); errR3 = recv3Slow.Receive() }()
	wg.Wait()

	require.NoError(t, errSend)
	require.NoError(t, errR1)
	require.NoError(t, errR2)
	require.Error(t, errR3)
	assert.ErrorIs(t, errR3, ErrFanoutKicked)
	assert.True(t, recv3Slow.FanoutKicked)
	assert.Contains(t, recv3Slow.FanoutKickReason, "below threshold")

	// Verify fast receivers #1 and #2 received the exact file.
	for _, dir := range []string{out1, out2} {
		got, readErr := os.ReadFile(filepath.Join(dir, "resume-target.bin"))
		require.NoError(t, readErr)
		assert.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
	}

	// Verify kicked receiver #3 preserved .partial and .partial.crocmap on disk.
	partialFile := filepath.Join(out3, "resume-target.bin.partial")
	crocmapFile := partialFile + ".crocmap"
	_, statErr := os.Stat(partialFile)
	require.NoError(t, statErr, "kicked receiver must preserve .partial file")
	_, mapStatErr := os.Stat(crocmapFile)
	require.NoError(t, mapStatErr, "kicked receiver must preserve .partial.crocmap bitmap")

	// Now run a standard 1-on-1 croc transfer to out3 to resume the remaining chunks!
	resumeSecret := "one-to-one-resume-code"
	sender1to1, err := New(Options{
		IsSender:         true,
		SharedSecret:     resumeSecret,
		RelayAddress:     relay.address,
		RelayPassword:    "pass123",
		DisableLocal:     true,
		NoPrompt:         true,
		NoCompress:       true,
		Quiet:            true,
		DisableClipboard: true,
		Transport:        TransportRelay,
		Curve:            "p256",
		HashAlgorithm:    "xxhash",
	})
	require.NoError(t, err)

	recv1to1, err := New(Options{
		IsSender:         false,
		SharedSecret:     resumeSecret,
		RelayAddress:     relay.address,
		RelayPassword:    "pass123",
		DisableLocal:     true,
		NoPrompt:         true,
		Overwrite:        true,
		Quiet:            true,
		DisableClipboard: true,
		Curve:            "p256",
		HashAlgorithm:    "xxhash",
		OutputDir:        out3,
	})
	require.NoError(t, err)

	filesInfo2, empty2, folders2, err := GetFilesInfo([]string{srcFile}, false, false, nil)
	require.NoError(t, err)

	resCh := make(chan error, 2)
	go func() { resCh <- sender1to1.Send(filesInfo2, empty2, folders2) }()
	time.Sleep(60 * time.Millisecond)
	go func() { resCh <- recv1to1.Receive() }()
	waitForTransferResults(t, resCh, 2, 10*time.Second, sender1to1, recv1to1)

	// Confirm that 1-to-1 resume only sent the missing chunks (< full fileSize),
	// cleaned up .partial/.crocmap, and produced an identical SHA-256!
	assert.Less(t, sender1to1.TotalSent, int64(fileSize), "1-on-1 resume must only transfer missing chunks")
	_, err = os.Stat(crocmapFile)
	assert.True(t, os.IsNotExist(err), ".crocmap sidecar must be removed after resume completes")

	resumedBytes, err := os.ReadFile(filepath.Join(out3, "resume-target.bin"))
	require.NoError(t, err)
	assert.Equal(t, sha256.Sum256(payload), sha256.Sum256(resumedBytes))
}

func TestFanoutExceptions(t *testing.T) {
	const chunkSize = int(models.TCP_BUFFER_SIZE / 2)

	t.Run("E6_late_joiner_and_room_full_rejected", func(t *testing.T) {
		relay := startTestFanoutRelay(t, tcp.FanoutConfig{
			MaxBufferBytes: 1 << 20,
			MaxMemoryBytes: 8 << 20,
			MaxReceivers:   1,
		})
		srcDir := t.TempDir()
		srcFile := filepath.Join(srcDir, "small.bin")
		require.NoError(t, os.WriteFile(srcFile, bytes.Repeat([]byte("x"), 16*chunkSize), 0o600))

		secret := "fanout-e6-secret-code"
		sender := newTestFanoutSender(t, relay.address, secret, 1, 1, 5*time.Second)
		sender.Options.ThrottleUpload = "200k"
		sender.limiter = nil
		var err error
		sender, err = New(sender.Options)
		require.NoError(t, err)

		filesInfo, empty, folders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
		require.NoError(t, err)

		sendDone := make(chan error, 1)
		go func() { sendDone <- sender.Send(filesInfo, empty, folders) }()
		time.Sleep(60 * time.Millisecond)

		recv1 := newTestFanoutReceiver(t, relay.address, secret, t.TempDir(), 0)
		recv1Done := make(chan error, 1)
		go func() { recv1Done <- recv1.Receive() }()

		// Wait until transfer has started and lobby is locked, then try a late joiner.
		time.Sleep(250 * time.Millisecond)
		lateRecv := newTestFanoutReceiver(t, relay.address, secret, t.TempDir(), 0)
		lateErr := lateRecv.Receive()
		require.Error(t, lateErr)
		assert.True(t, strings.Contains(lateErr.Error(), "already started") || strings.Contains(lateErr.Error(), "room full"), "got: %v", lateErr)

		require.NoError(t, <-recv1Done)
		require.NoError(t, <-sendDone)
	})

	t.Run("E7_wait_timeout_min_receivers_policy", func(t *testing.T) {
		relay := startTestFanoutRelay(t, tcp.FanoutConfig{
			MaxBufferBytes: 1 << 20,
			MaxMemoryBytes: 8 << 20,
			MaxReceivers:   4,
		})
		srcDir := t.TempDir()
		srcFile := filepath.Join(srcDir, "timeout.bin")
		require.NoError(t, os.WriteFile(srcFile, bytes.Repeat([]byte("t"), 4*chunkSize), 0o600))
		filesInfo, empty, folders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
		require.NoError(t, err)

		// Case 1: target=3, min=2, only 1 joins before 300ms timeout -> must abort!
		secretAbort := "fanout-e7-abort-code"
		senderAbort := newTestFanoutSender(t, relay.address, secretAbort, 3, 2, 300*time.Millisecond)
		sendErrCh := make(chan error, 1)
		go func() { sendErrCh <- senderAbort.Send(filesInfo, empty, folders) }()
		time.Sleep(50 * time.Millisecond)
		r1 := newTestFanoutReceiver(t, relay.address, secretAbort, t.TempDir(), 0)
		r1ErrCh := make(chan error, 1)
		go func() { r1ErrCh <- r1.Receive() }()

		sErr := <-sendErrCh
		require.Error(t, sErr)
		assert.Contains(t, sErr.Error(), "minimum receivers ready")
		_ = <-r1ErrCh

		// Case 2: target=3, min=1, 1 joins before 250ms timeout -> must proceed and complete!
		secretProceed := "fanout-e7-proceed-code"
		senderProceed := newTestFanoutSender(t, relay.address, secretProceed, 3, 1, 250*time.Millisecond)
		outDir := t.TempDir()
		rProceed := newTestFanoutReceiver(t, relay.address, secretProceed, outDir, 0)
		go func() { sendErrCh <- senderProceed.Send(filesInfo, empty, folders) }()
		time.Sleep(50 * time.Millisecond)
		go func() { r1ErrCh <- rProceed.Receive() }()
		require.NoError(t, <-r1ErrCh)
		require.NoError(t, <-sendErrCh)
	})

	t.Run("E8_wrong_code_pake_failure_isolated_from_healthy_peer", func(t *testing.T) {
		relay := startTestFanoutRelay(t, tcp.FanoutConfig{
			MaxBufferBytes: 1 << 20,
			MaxMemoryBytes: 8 << 20,
			MaxReceivers:   4,
		})
		srcDir := t.TempDir()
		srcFile := filepath.Join(srcDir, "e8.bin")
		require.NoError(t, os.WriteFile(srcFile, bytes.Repeat([]byte("8"), 4*chunkSize), 0o600))
		filesInfo, empty, folders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
		require.NoError(t, err)

		// Same room prefix (first 4 chars of codephrase room), different PAKE passphrase?
		// Note: codephrase derives RoomName from first 4 chars if not EFF words, wait:
		// Let's check codephrase.Parse: we can override c.pakePassphrase on badRecv while keeping c.Options.RoomName identical!
		secret := "fanout-e8-good-code"
		sender := newTestFanoutSender(t, relay.address, secret, 1, 1, 3*time.Second)
		badPake := newTestFanoutReceiver(t, relay.address, secret, t.TempDir(), 0)
		badPake.pakePassphrase = "wrong-passphrase"
		badPake.Pake, err = pakekey.Init(
			[]byte("wrong-passphrase"),
			0,
			badPake.Options.Curve,
			pakekey.PurposeTransfer,
			sender.Options.RoomName,
		)
		require.NoError(t, err)

		sendCh := make(chan error, 1)
		go func() { sendCh <- sender.Send(filesInfo, empty, folders) }()
		time.Sleep(50 * time.Millisecond)

		// Bad receiver connects to the same room with wrong PAKE password and gets rejected.
		badErr := badPake.Receive()
		require.Error(t, badErr)

		// Good receiver connects and completes cleanly.
		goodOut := t.TempDir()
		goodRecv := newTestFanoutReceiver(t, relay.address, secret, goodOut, 0)
		require.NoError(t, goodRecv.Receive())
		require.NoError(t, <-sendCh)
	})

	t.Run("E9_global_memory_budget_enforced_and_released", func(t *testing.T) {
		// Global memory budget is 512 KiB, each room reserves 512 KiB -> only 1 room allowed at a time.
		relay := startTestFanoutRelay(t, tcp.FanoutConfig{
			MaxBufferBytes: 512 * 1024,
			MaxMemoryBytes: 512 * 1024,
			MaxReceivers:   2,
		})
		srcDir := t.TempDir()
		srcFile := filepath.Join(srcDir, "e9.bin")
		require.NoError(t, os.WriteFile(srcFile, bytes.Repeat([]byte("9"), 2*chunkSize), 0o600))
		filesInfo, empty, folders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
		require.NoError(t, err)

		s1 := newTestFanoutSender(t, relay.address, "fanout-e9-room-1", 1, 1, 2*time.Second)
		s1Ch := make(chan error, 1)
		go func() { s1Ch <- s1.Send(filesInfo, empty, folders) }()
		time.Sleep(80 * time.Millisecond)
		assert.Equal(t, int64(512*1024), relay.manager.MemoryBudget().Reserved())

		// Second concurrent room must be rejected with memory budget exceeded.
		s2 := newTestFanoutSender(t, relay.address, "fanout-e9-room-2", 1, 1, time.Second)
		err2 := s2.Send(filesInfo, empty, folders)
		require.Error(t, err2)
		assert.Contains(t, err2.Error(), "memory budget exceeded")

		// Finish room 1 and verify memory budget returns to 0.
		r1 := newTestFanoutReceiver(t, relay.address, "fanout-e9-room-1", t.TempDir(), 0)
		require.NoError(t, r1.Receive())
		require.NoError(t, <-s1Ch)
		require.Eventually(t, func() bool {
			return relay.manager.MemoryBudget().Reserved() == 0
		}, 2*time.Second, 20*time.Millisecond)
	})

	t.Run("E10_hash_mismatch_isolated_to_one_receiver", func(t *testing.T) {
		relay := startTestFanoutRelay(t, tcp.FanoutConfig{
			MaxBufferBytes: 1 << 20,
			MaxMemoryBytes: 8 << 20,
			MaxReceivers:   4,
		})
		srcDir := t.TempDir()
		srcFile := filepath.Join(srcDir, "e10.bin")
		require.NoError(t, os.WriteFile(srcFile, bytes.Repeat([]byte("h"), 4*chunkSize), 0o600))
		filesInfo, empty, folders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
		require.NoError(t, err)

		secret := "fanout-e10-hash-code"
		sender := newTestFanoutSender(t, relay.address, secret, 2, 2, 5*time.Second)
		badOut := t.TempDir()
		goodOut := t.TempDir()

		origHashFn := receiveFileHash
		t.Cleanup(func() { receiveFileHash = origHashFn })
		receiveFileHash = func(ctx context.Context, fname, algorithm string, showProgress ...bool) ([]byte, error) {
			if strings.HasPrefix(fname, badOut) {
				return []byte("corrupt!"), nil
			}
			return origHashFn(ctx, fname, algorithm, showProgress...)
		}

		rBad := newTestFanoutReceiver(t, relay.address, secret, badOut, 0)
		rGood := newTestFanoutReceiver(t, relay.address, secret, goodOut, 0)

		var wg sync.WaitGroup
		var sErr, badErr, goodErr error
		wg.Add(3)
		go func() { defer wg.Done(); sErr = sender.Send(filesInfo, empty, folders) }()
		time.Sleep(60 * time.Millisecond)
		go func() { defer wg.Done(); badErr = rBad.Receive() }()
		go func() { defer wg.Done(); goodErr = rGood.Receive() }()
		wg.Wait()

		require.NoError(t, sErr, "sender must succeed because rGood completed")
		require.NoError(t, goodErr, "healthy receiver must complete despite peer's hash failure")
		require.Error(t, badErr)
		assert.Contains(t, badErr.Error(), "hash mismatch")
	})
}

func TestCombinedDualRelayFanout(t *testing.T) {
	const chunkSize = int(models.TCP_BUFFER_SIZE / 2)

	t.Run("parallel_distribution_and_synchronized_kick", func(t *testing.T) {
		const fileSize = 48 * chunkSize // 1.5 MiB
		cfg := tcp.FanoutConfig{
			MaxBufferBytes: 256 * 1024,
			MaxMemoryBytes: 8 << 20,
			MaxReceivers:   4,
			MinRateBps:     350 * 1024,
			RateWindow:     350 * time.Millisecond,
			StallTimeout:   5 * time.Second,
		}
		relayB := startTestFanoutRelay(t, cfg)
		relayA := startTestFanoutRelay(t, cfg, relayB.address)

		srcDir := t.TempDir()
		payload := make([]byte, fileSize)
		_, err := rand.Read(payload)
		require.NoError(t, err)
		srcFile := filepath.Join(srcDir, "combined-dual-fanout.bin")
		require.NoError(t, os.WriteFile(srcFile, payload, 0o600))

		secret := "combined-dual-fanout-code"
		sender := newTestFanoutSender(t, relayA.address, secret, 3, 3, 5*time.Second)
		sender.Options.DualRelay = true
		sender.Options.ChunkPolicy = ChunkPolicyParity

		filesInfo, empty, folders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
		require.NoError(t, err)

		out1, out2, out3 := t.TempDir(), t.TempDir(), t.TempDir()
		recv1 := newTestFanoutReceiver(t, relayA.address, secret, out1, 0)
		recv2 := newTestFanoutReceiver(t, relayA.address, secret, out2, 0)
		recv3Slow := newTestFanoutReceiver(t, relayA.address, secret, out3, 60*1024)

		var wg sync.WaitGroup
		var errSend, errR1, errR2, errR3 error
		wg.Add(4)
		go func() { defer wg.Done(); errSend = sender.Send(filesInfo, empty, folders) }()
		time.Sleep(60 * time.Millisecond)
		go func() { defer wg.Done(); errR1 = recv1.Receive() }()
		go func() { defer wg.Done(); errR2 = recv2.Receive() }()
		go func() { defer wg.Done(); errR3 = recv3Slow.Receive() }()
		wg.Wait()

		require.NoError(t, errSend)
		require.NoError(t, errR1)
		require.NoError(t, errR2)
		require.Error(t, errR3)
		assert.ErrorIs(t, errR3, ErrFanoutKicked)
		assert.True(t, sender.DualRelayActive(), "dual-relay fanout should be active")
		assert.Greater(t, sender.DualRelayChunksSent(0), int64(0), "Relay A must send chunks")
		assert.Greater(t, sender.DualRelayChunksSent(1), int64(0), "Relay B must send chunks")

		for _, dir := range []string{out1, out2} {
			got, readErr := os.ReadFile(filepath.Join(dir, "combined-dual-fanout.bin"))
			require.NoError(t, readErr)
			assert.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
		}
		_, statErr := os.Stat(filepath.Join(out3, "combined-dual-fanout.bin.partial"))
		require.NoError(t, statErr, "kicked receiver must save .partial file")
	})

	t.Run("relay_b_dies_mid_transfer_plus_slow_receiver_kick", func(t *testing.T) {
		const fileSize = 48 * chunkSize // 1.5 MiB
		cfg := tcp.FanoutConfig{
			MaxBufferBytes: 512 * 1024,
			MaxMemoryBytes: 8 << 20,
			MaxReceivers:   4,
			MinRateBps:     220 * 1024,
			RateWindow:     350 * time.Millisecond,
			StallTimeout:   5 * time.Second,
		}
		relayB := startTestFanoutRelay(t, cfg)
		relayA := startTestFanoutRelay(t, cfg, relayB.address)

		srcDir := t.TempDir()
		payload := make([]byte, fileSize)
		_, err := rand.Read(payload)
		require.NoError(t, err)
		srcFile := filepath.Join(srcDir, "combined-failover-fanout.bin")
		require.NoError(t, os.WriteFile(srcFile, payload, 0o600))

		secret := "combined-failover-fanout-code"
		sender, err := New(Options{
			IsSender:           true,
			SharedSecret:       secret,
			RelayAddress:       relayA.address,
			RelayPassword:      "pass123",
			DisableLocal:       true,
			NoPrompt:           true,
			NoCompress:         true,
			Quiet:              true,
			DisableClipboard:   true,
			Curve:              "p256",
			HashAlgorithm:      "xxhash",
			FanoutReceivers:    3,
			FanoutMinReceivers: 3,
			FanoutWaitTimeout:  5 * time.Second,
			FanoutMode:         true,
			DualRelay:          true,
			ChunkPolicy:        ChunkPolicyDynamic,
			ThrottleUpload:     "1200k",
		})
		require.NoError(t, err)

		filesInfo, empty, folders, err := GetFilesInfo([]string{srcFile}, false, false, nil)
		require.NoError(t, err)

		out1, out2, out3 := t.TempDir(), t.TempDir(), t.TempDir()
		recv1 := newTestFanoutReceiver(t, relayA.address, secret, out1, 0)
		recv2 := newTestFanoutReceiver(t, relayA.address, secret, out2, 0)
		recv3Slow := newTestFanoutReceiver(t, relayA.address, secret, out3, 50*1024)

		var wg sync.WaitGroup
		var errSend, errR1, errR2, errR3 error
		wg.Add(4)
		go func() { defer wg.Done(); errSend = sender.Send(filesInfo, empty, folders) }()
		time.Sleep(60 * time.Millisecond)
		go func() { defer wg.Done(); errR1 = recv1.Receive() }()
		go func() { defer wg.Done(); errR2 = recv2.Receive() }()
		go func() { defer wg.Done(); errR3 = recv3Slow.Receive() }()

		// Wait until Relay B has carried at least 3 chunks, then kill Relay B mid-transfer!
		require.Eventually(t, func() bool {
			return sender.DualRelayChunksSent(1) >= 3
		}, 5*time.Second, 20*time.Millisecond)
		relayB.cancel()

		wg.Wait()

		require.NoError(t, errSend)
		require.NoError(t, errR1)
		require.NoError(t, errR2)
		require.Error(t, errR3)
		assert.ErrorIs(t, errR3, ErrFanoutKicked)

		for _, dir := range []string{out1, out2} {
			got, readErr := os.ReadFile(filepath.Join(dir, "combined-failover-fanout.bin"))
			require.NoError(t, readErr)
			assert.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
		}
	})
}
