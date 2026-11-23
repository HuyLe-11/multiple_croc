package croc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The dynamic window must be sized from each relay's own estimates. A shared
// estimator let the low-RTT relay shrink the window of a high-RTT tunnel relay,
// and the old 256-chunk ceiling capped a relay at 8 MiB per ACK round trip
// (about 11 MB/s at a 700 ms tunnel RTT).
func TestDynamicWindowUsesPerRelayEstimates(t *testing.T) {
	const chunkSize = int64(32 * 1024)
	sched := newReliableChunkScheduler(nil, chunkSize*1000, chunkSize, ChunkPolicyDynamic, 2, nil)
	sched.mu.Lock()
	defer sched.mu.Unlock()

	for r := range sched.est {
		assert.Equal(t, initDynamicInFlightPerRelay, sched.dynamicInFlightLimitLocked(r))
	}

	fast := &sched.est[0]
	fast.maxRate, fast.minRTT, fast.srtt, fast.rateCount = 5e6, 20*time.Millisecond, 30*time.Millisecond, rateFilterLen
	sched.updateWindowLocked(fast)

	tunnel := &sched.est[1]
	tunnel.maxRate, tunnel.minRTT, tunnel.srtt, tunnel.rateCount = 15e6, 600*time.Millisecond, 700*time.Millisecond, rateFilterLen
	sched.updateWindowLocked(tunnel)

	// Fast LAN relay: 5 MB/s * 1 s delay budget.
	assert.Equal(t, 153, sched.dynamicInFlightLimitLocked(0))
	// Tunnel relay: 15 MB/s * 2 * 600 ms = 18 MB, far above the old 256-chunk cap.
	assert.Equal(t, 550, sched.dynamicInFlightLimitLocked(1))

	// A queue-inflated sRTT keeps the window neutral instead of shrinking it.
	tunnel.srtt = 3 * time.Second
	sched.updateWindowLocked(tunnel)
	assert.Equal(t, 1374, sched.dynamicInFlightLimitLocked(1))

	// Bounds.
	tunnel.maxRate = 1e9
	sched.updateWindowLocked(tunnel)
	assert.Equal(t, maxDynamicInFlightPerRelay, sched.dynamicInFlightLimitLocked(1))
	fast.maxRate = 1
	sched.updateWindowLocked(fast)
	assert.Equal(t, minDynamicInFlightPerRelay, sched.dynamicInFlightLimitLocked(0))
}

func TestSchedulerInFlightCountersStayConsistent(t *testing.T) {
	const chunkSize = int64(32 * 1024)
	const total = 64
	sched := newReliableChunkScheduler(nil, chunkSize*total, chunkSize, ChunkPolicyDynamic, 2, nil)
	ctx := context.Background()

	var claimed [2][]int
	for i := 0; i < 20; i++ {
		r := i % 2
		_, idx, ok := sched.Claim(ctx, r, r)
		require.True(t, ok)
		sched.MarkSentFirstTime(idx)
		claimed[r] = append(claimed[r], idx)
	}
	check := func() {
		t.Helper()
		sched.mu.Lock()
		defer sched.mu.Unlock()
		var want [2]int
		for _, e := range sched.inFlight {
			want[e.relayID&1]++
		}
		assert.Equal(t, want[0], sched.inFlightForRelayLocked(0))
		assert.Equal(t, want[1], sched.inFlightForRelayLocked(1))
	}
	check()

	sched.RequeueChunk(claimed[0][0])
	check()
	assert.False(t, sched.ApplyAck(ChunkAckPayload{AckedList: []uint32{uint32(claimed[1][0]), uint32(claimed[1][1])}, FailedRelay: -1}))
	check()
	assert.Equal(t, len(claimed[1])-2, sched.RequeueRelay(1))
	check()
	sched.mu.Lock()
	assert.Equal(t, 0, sched.inFlightForRelayLocked(1))
	sched.mu.Unlock()
}

// Cumulative ACKs must only visit chunks the previous ACK did not cover, and
// must work with resume ranges that skip already-received chunks.
func TestApplyAckCumulativeIsIncrementalWithResumeRanges(t *testing.T) {
	const chunkSize = int64(32 * 1024)
	// Request chunks 0-9 and 20-29 only (resume of a partial file).
	ranges := []int64{chunkSize, 0, 10, 20 * chunkSize, 10}
	sched := newReliableChunkScheduler(ranges, chunkSize*40, chunkSize, ChunkPolicyDynamic, 2, nil)
	require.Equal(t, 20, sched.total)
	require.True(t, sched.sortedOffsets)
	require.Nil(t, sched.offsetToIndex, "sorted offsets must not allocate the offset map")

	assert.False(t, sched.ApplyAck(ChunkAckPayload{CumChunk: 5, FailedRelay: -1}))
	assert.Equal(t, 5, sched.ackedCount)
	assert.Equal(t, 5, sched.cumApplied)

	// Chunks 10-19 are not part of the request; the frontier passes them.
	assert.False(t, sched.ApplyAck(ChunkAckPayload{CumChunk: 25, FailedRelay: -1}))
	assert.Equal(t, 15, sched.ackedCount)

	// A stale (lower) cumulative ACK is a no-op.
	assert.False(t, sched.ApplyAck(ChunkAckPayload{CumChunk: 3, FailedRelay: -1}))
	assert.Equal(t, 15, sched.ackedCount)

	assert.True(t, sched.ApplyAck(ChunkAckPayload{CumChunk: 30, FailedRelay: -1}))
	assert.Equal(t, 20, sched.ackedCount)
}

// The tail stealer must not duplicate chunks that the other relay is about to
// deliver, but must take chunks from a relay that is much slower.
func TestStealTailOnlyTakesChunksThatFinishSooner(t *testing.T) {
	const chunkSize = int64(32 * 1024)
	sched := newReliableChunkScheduler(nil, chunkSize*4, chunkSize, ChunkPolicyDynamic, 2, nil)
	sched.mu.Lock()
	defer sched.mu.Unlock()
	now := time.Now()

	sched.est[0].srtt = 100 * time.Millisecond // stealer
	sched.est[1].srtt = 400 * time.Millisecond // owner of the pending chunks

	// Sent 350 ms ago on a 400 ms relay: arrives in ~50 ms, cheaper to wait.
	sched.putInFlightLocked(0, inFlightChunk{relayID: 1, sentAt: now.Add(-350 * time.Millisecond), sent: true})
	_, stolen := sched.stealTailFromSlowRelayLocked(0, 7)
	assert.False(t, stolen)

	// Sent just now: the owner needs ~400 ms, the stealer ~100 ms.
	sched.putInFlightLocked(1, inFlightChunk{relayID: 1, sentAt: now, sent: true})
	idx, stolen := sched.stealTailFromSlowRelayLocked(0, 7)
	require.True(t, stolen)
	assert.Equal(t, 1, idx)
	assert.Equal(t, 0, sched.inFlight[1].relayID)
	assert.True(t, sched.inFlight[1].retrans)
	assert.Equal(t, 1, sched.inFlightForRelayLocked(0))
	assert.Equal(t, 1, sched.inFlightForRelayLocked(1))

	// Overdue (more than twice the owner's sRTT) is always stolen.
	sched.putInFlightLocked(2, inFlightChunk{relayID: 1, sentAt: now.Add(-time.Second), sent: true})
	idx, stolen = sched.stealTailFromSlowRelayLocked(0, 7)
	require.True(t, stolen)
	assert.Equal(t, 2, idx)
}

func TestObserveDeliverySkipsRetransmissionRTT(t *testing.T) {
	const chunkSize = int64(32 * 1024)
	sched := newReliableChunkScheduler(nil, chunkSize*8, chunkSize, ChunkPolicyDynamic, 2, nil)
	sched.mu.Lock()
	defer sched.mu.Unlock()
	now := time.Now()

	sched.observeDeliveryLocked(inFlightChunk{relayID: 1, sentAt: now.Add(-5 * time.Second), sent: true, retrans: true}, 0, now)
	assert.Zero(t, sched.est[1].srtt, "retransmissions are ambiguous and must not produce RTT samples")

	sched.observeDeliveryLocked(inFlightChunk{relayID: 1, sentAt: now.Add(-200 * time.Millisecond), sent: true}, 1, now)
	assert.Equal(t, 200*time.Millisecond, sched.est[1].srtt)
	assert.Equal(t, 200*time.Millisecond, sched.est[1].minRTT)
	assert.Zero(t, sched.est[0].srtt, "relay 0 estimates are independent")
}

// More than maxRecentAcked out-of-order arrivals between two ACKs used to be
// silently dropped (cap 256). The sender then kept those chunks "in flight",
// blocking the faster relay's window until the cumulative frontier caught up.
func TestChunkBitmapAckReportsAllOutOfOrderChunksOnOverflow(t *testing.T) {
	const total = maxRecentAcked + 2000
	bm := newChunkBitmap(total, int64(total)*32*1024, 32*1024)
	// Chunk 0 is still missing (slow relay); everything else arrived.
	for idx := 1; idx < total; idx++ {
		require.True(t, bm.Mark(idx))
	}
	ack := bm.BuildAck(0, -1)
	assert.Equal(t, uint32(0), ack.CumChunk)
	assert.Equal(t, ^uint64(0), ack.Bitmask)
	assert.Len(t, ack.AckedList, total-1-64)

	sched := newReliableChunkScheduler(nil, int64(total)*32*1024, 32*1024, ChunkPolicyDynamic, 2, nil)
	sched.ApplyAck(ack)
	assert.Equal(t, total-1, sched.ackedCount)

	// The next ACK only carries what arrived since.
	require.True(t, bm.Mark(0))
	next := bm.BuildAck(0, -1)
	assert.Equal(t, uint32(total), next.CumChunk)
	assert.Empty(t, next.AckedList)
}

// A dual-relay sender never advertises Tailcat, so it must not contact the
// public Tailscale DERP infrastructure to prepare it.
func TestDualRelaySenderSkipsTailcatPreparation(t *testing.T) {
	for _, opts := range []Options{
		{IsSender: true, Transport: TransportAuto, DualRelay: true},
		{IsSender: true, Transport: TransportAuto, SecondaryRelay: "wss://relay-b.example/ws"},
	} {
		provider := &preparingTailcatTransport{fakeTailcatTransport: &fakeTailcatTransport{available: true}}
		client := &Client{
			Options: opts,
			stop:    newStop(context.Background()),
			tailcat: tailcatClientState{transport: provider},
		}
		client.tailcat.transferBytes.Store(autoTailcatThresholdBytes)
		client.startTailcatPreparation()
		assert.Nil(t, client.tailcat.prepareReady)
		assert.Zero(t, provider.prepareCalls.Load())
		assert.False(t, client.localTailcatSupported())
	}
}

func BenchmarkApplyAckCumulativeLargeFile(b *testing.B) {
	const chunkSize = int64(32 * 1024)
	const total = 65536 // 2 GiB
	for b.Loop() {
		sched := newReliableChunkScheduler(nil, chunkSize*total, chunkSize, ChunkPolicyDynamic, 2, nil)
		for cum := 4; cum <= total; cum += 4 {
			sched.ApplyAck(ChunkAckPayload{CumChunk: uint32(cum), FailedRelay: -1})
		}
	}
}

// Losing one data connection requeues only that connection's chunks and keeps
// the relay alive for its other connections.
func TestRequeueConnKeepsRelayAlive(t *testing.T) {
	const chunkSize = int64(32 * 1024)
	sched := newReliableChunkScheduler(nil, chunkSize*16, chunkSize, ChunkPolicyDynamic, 2, nil)
	ctx := context.Background()
	var onConn [3][]int
	for i := 0; i < 9; i++ {
		conn := i % 3
		_, idx, ok := sched.Claim(ctx, 1, conn)
		require.True(t, ok)
		onConn[conn] = append(onConn[conn], idx)
	}
	assert.Equal(t, 3, sched.RequeueConn(1, 2))
	sched.mu.Lock()
	assert.False(t, sched.relayDead[1])
	assert.Equal(t, 6, sched.inFlightForRelayLocked(1))
	assert.ElementsMatch(t, onConn[2], sched.retryQueue)
	sched.mu.Unlock()

	// The requeued chunks are handed out again before new ones.
	for range onConn[2] {
		_, idx, ok := sched.Claim(ctx, 1, 0)
		require.True(t, ok)
		assert.Contains(t, onConn[2], idx)
	}
}
