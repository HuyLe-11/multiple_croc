package croc

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/schollz/croc/v11/src/utils"
)

const (
	// ChunkPolicyDynamic distributes chunks dynamically from a shared work
	// queue so faster relays automatically process more chunks.
	ChunkPolicyDynamic = "dynamic"
	// ChunkPolicyParity assigns even requested chunks to relay 0 and odd
	// requested chunks to relay 1 while both relays are healthy, merging
	// seamlessly if either relay fails.
	ChunkPolicyParity = "parity"

	// Dynamic mode bounds the unacknowledged chunks each relay may hold so a
	// slow relay cannot hoard megabytes of chunks in kernel/tunnel buffers at
	// the end of a transfer. The bound is computed per relay from that relay's
	// own delivery rate and RTT (see updateWindowLocked):
	//
	//   window = deliveryRate * max(dynamicDelayBudget, 2*minRTT, sRTT)
	//
	// The 2*minRTT term keeps the link saturated (2*BDP, as in BBR), the sRTT
	// term makes the window neutral (never shrinking) when the ACK loop is
	// inflated by queueing on another relay's path, and the delay budget lets a
	// relay hold at most ~1 s of data in normal conditions.
	minDynamicInFlightPerRelay  = 32   // 1 MiB floor
	initDynamicInFlightPerRelay = 256  // 8 MiB until the first estimates exist
	maxDynamicInFlightPerRelay  = 2048 // 64 MiB ceiling for long fat tunnels
	dynamicDelayBudget          = time.Second
	rateSampleMinInterval       = 100 * time.Millisecond
	rateFilterLen               = 10
	minRTTExpiry                = 10 * time.Second
	minStealAge                 = 50 * time.Millisecond
)

// relayEstimator tracks per-relay in-flight counts and path estimates used by
// dynamic scheduling. Each relay has its own RTT and delivery rate; mixing the
// samples of a fast and a slow relay into one estimator sizes both windows
// wrongly.
type relayEstimator struct {
	inFlight    int
	srtt        time.Duration
	minRTT      time.Duration
	minRTTAt    time.Time
	sampleStart time.Time
	sampleBytes int64
	rates       [rateFilterLen]float64 // bytes/sec samples (windowed max filter)
	rateNext    int
	rateCount   int
	maxRate     float64
	window      int
}

// ParseChunkPolicy validates a chunk scheduling policy name.
func ParseChunkPolicy(value string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))
	switch normalized {
	case "", ChunkPolicyDynamic:
		return ChunkPolicyDynamic, nil
	case ChunkPolicyParity:
		return ChunkPolicyParity, nil
	default:
		return "", fmt.Errorf("invalid chunk policy %q (must be %q or %q)", value, ChunkPolicyDynamic, ChunkPolicyParity)
	}
}

type inFlightChunk struct {
	chunkIdx uint32
	offset   int64
	relayID  int
	connID   int
	sentAt   time.Time
	sent     bool // the transmission finished (sentAt is the send completion time)
	retrans  bool // a retransmission: excluded from RTT sampling (Karn's algorithm)
	stolen   bool
}

// reliableChunkScheduler extends requestedChunkQueue with in-flight tracking by
// relayID, dynamic or even/odd parity scheduling, receiver ACK tracking, and
// zero-downtime requeueing when one relay fails mid-transfer.
type reliableChunkScheduler struct {
	mu        sync.Mutex
	cond      *sync.Cond
	policy    string
	chunkSize int64
	fileSize  int64
	offsets   []int64
	// offsetToIndex is only built when offsets are not strictly increasing;
	// otherwise lookups use binary search, which avoids a map entry (~40 B)
	// per chunk on large files.
	offsetToIndex map[int64]int
	sortedOffsets bool
	total         int

	// Scheduling state
	nextDynamic     int
	nextParity      [2]int
	retryQueue      []int
	inFlight        map[int]inFlightChunk
	acked           []bool
	progressCounted []bool
	ackedCount      int
	cumApplied      int // file chunk index below which cumulative ACKs were applied
	lastAckAt       time.Time
	est             [2]relayEstimator
	sendCount       int64 // every transmission, including retransmissions
	firstSendCount  int64
	relayDead       map[int]bool
	activeCount     int
	closed          bool

	doneOnce sync.Once
	onDone   func()
}

func newReliableChunkScheduler(ranges []int64, fileSize, chunkSize int64, policy string, relayCount int, onDone func()) *reliableChunkScheduler {
	if chunkSize <= 0 {
		chunkSize = 32 * 1024
	}
	parsedPolicy, err := ParseChunkPolicy(policy)
	if err != nil {
		parsedPolicy = ChunkPolicyDynamic
	}
	if len(ranges) == 0 {
		count := utils.ChunkRangesCount(nil, fileSize, chunkSize)
		ranges = []int64{chunkSize, 0, int64(count)}
	}
	stepSize := ranges[0]
	if stepSize <= 0 {
		stepSize = chunkSize
	}
	totalOffsets := int64(0)
	for i := 2; i < len(ranges); i += 2 {
		totalOffsets += max(ranges[i], 0)
	}
	offsets := make([]int64, 0, totalOffsets)
	for i := 1; i+1 < len(ranges); i += 2 {
		start := ranges[i]
		count := ranges[i+1]
		for k := int64(0); k < count; k++ {
			offsets = append(offsets, start+k*stepSize)
		}
	}
	sortedOffsets := true
	for i := 1; i < len(offsets); i++ {
		if offsets[i] <= offsets[i-1] {
			sortedOffsets = false
			break
		}
	}
	var offsetToIndex map[int64]int
	if !sortedOffsets {
		offsetToIndex = make(map[int64]int, len(offsets))
		for i, off := range offsets {
			offsetToIndex[off] = i
		}
	}
	if relayCount <= 0 {
		relayCount = 1
	}
	s := &reliableChunkScheduler{
		policy:          parsedPolicy,
		chunkSize:       stepSize,
		fileSize:        fileSize,
		offsets:         offsets,
		offsetToIndex:   offsetToIndex,
		sortedOffsets:   sortedOffsets,
		total:           len(offsets),
		nextParity:      [2]int{0, 1},
		inFlight:        make(map[int]inFlightChunk),
		acked:           make([]bool, len(offsets)),
		progressCounted: make([]bool, len(offsets)),
		relayDead:       make(map[int]bool),
		activeCount:     relayCount,
		onDone:          onDone,
	}
	for r := range s.est {
		s.est[r].window = initDynamicInFlightPerRelay
	}
	s.cond = sync.NewCond(&s.mu)
	if s.total == 0 {
		s.closed = true
		s.signalDone()
	}
	return s
}

func (s *reliableChunkScheduler) signalDone() {
	s.doneOnce.Do(func() {
		if s.onDone != nil {
			s.onDone()
		}
	})
}

func (s *reliableChunkScheduler) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.cond.Broadcast()
	s.mu.Unlock()
}

func (s *reliableChunkScheduler) aliveRelayCountLocked() int {
	dead := len(s.relayDead)
	alive := s.activeCount - dead
	if alive < 1 {
		return 1
	}
	return alive
}

// dynamicInFlightLimitLocked returns the current in-flight window of relayID.
func (s *reliableChunkScheduler) dynamicInFlightLimitLocked(relayID int) int {
	return s.est[relayID&1].window
}

// updateWindowLocked recomputes the dynamic window of one relay from its own
// delivery rate and RTT estimates.
func (s *reliableChunkScheduler) updateWindowLocked(e *relayEstimator) {
	if e.maxRate <= 0 || e.minRTT <= 0 {
		e.window = initDynamicInFlightPerRelay
		return
	}
	horizon := max(dynamicDelayBudget, 2*e.minRTT, e.srtt)
	window := int(math.Ceil(e.maxRate * horizon.Seconds() / float64(s.chunkSize)))
	if e.rateCount < rateFilterLen {
		// Start-up: early samples under-estimate the rate while the pipe fills,
		// so do not shrink below the initial window yet.
		window = max(window, initDynamicInFlightPerRelay)
	}
	e.window = min(max(window, minDynamicInFlightPerRelay), maxDynamicInFlightPerRelay)
}

// inFlightForRelayLocked returns the number of unacknowledged chunks currently
// assigned to relayID in O(1).
func (s *reliableChunkScheduler) inFlightForRelayLocked(relayID int) int {
	return s.est[relayID&1].inFlight
}

// putInFlightLocked records (or replaces) the in-flight entry of reqIdx and
// keeps the per-relay counters consistent.
func (s *reliableChunkScheduler) putInFlightLocked(reqIdx int, entry inFlightChunk) {
	if old, ok := s.inFlight[reqIdx]; ok {
		s.est[old.relayID&1].inFlight--
	}
	s.inFlight[reqIdx] = entry
	s.est[entry.relayID&1].inFlight++
}

// dropInFlightLocked removes reqIdx from the in-flight set.
func (s *reliableChunkScheduler) dropInFlightLocked(reqIdx int) (inFlightChunk, bool) {
	entry, ok := s.inFlight[reqIdx]
	if ok {
		delete(s.inFlight, reqIdx)
		s.est[entry.relayID&1].inFlight--
	}
	return entry, ok
}

func (s *reliableChunkScheduler) clearInFlightLocked() {
	clear(s.inFlight)
	for r := range s.est {
		s.est[r].inFlight = 0
	}
}

func (s *reliableChunkScheduler) newInFlightLocked(reqIdx, relayID, connID int, retrans bool) {
	off := s.offsets[reqIdx]
	s.putInFlightLocked(reqIdx, inFlightChunk{
		chunkIdx: uint32(off / s.chunkSize),
		offset:   off,
		relayID:  relayID,
		connID:   connID,
		sentAt:   time.Now(),
		retrans:  retrans,
	})
}

// stealTailFromSlowRelayLocked lets a relay that ran out of unclaimed chunks
// retransmit a chunk still pending on the other relay, but only when that is
// expected to finish sooner: either the owner relay still needs longer than
// the stealer's own RTT to deliver it (owner sRTT - age > stealer sRTT), or
// the chunk is overdue (age > 2 * owner sRTT). Stealing chunks that are about
// to arrive anyway only duplicates traffic.
func (s *reliableChunkScheduler) stealTailFromSlowRelayLocked(relayID, connID int) (int, bool) {
	if s.policy != ChunkPolicyDynamic || s.aliveRelayCountLocked() <= 1 {
		return 0, false
	}
	stealerRTT := max(s.est[relayID&1].srtt, minStealAge)
	now := time.Now()
	bestIdx := -1
	var oldestTime time.Time
	for reqIdx, entry := range s.inFlight {
		if s.acked[reqIdx] || entry.relayID == relayID || entry.stolen {
			continue
		}
		age := now.Sub(entry.sentAt)
		if ownerRTT := s.est[entry.relayID&1].srtt; ownerRTT > 0 {
			if ownerRTT-age <= stealerRTT && age <= 2*ownerRTT {
				continue
			}
		} else if age < stealerRTT {
			continue
		}
		if bestIdx == -1 || entry.sentAt.Before(oldestTime) {
			bestIdx = reqIdx
			oldestTime = entry.sentAt
		}
	}
	if bestIdx >= 0 {
		entry := s.inFlight[bestIdx]
		entry.relayID = relayID
		entry.connID = connID
		entry.stolen = true
		entry.retrans = true
		entry.sent = false
		entry.sentAt = now
		s.putInFlightLocked(bestIdx, entry)
		return bestIdx, true
	}
	return 0, false
}

// Claim returns the next file offset and logical request index for a worker on
// relayID. When all chunks have been claimed but some remain unacknowledged on
// another relay, Claim waits so that if the other relay fails, this worker can
// immediately transmit the requeued chunks.
func (s *reliableChunkScheduler) Claim(ctx context.Context, relayID, connID int) (offset int64, reqIdx int, ok bool) {
	if s == nil {
		return 0, 0, false
	}
	stopWake := func() bool { return true }
	if ctx != nil {
		stopWake = context.AfterFunc(ctx, func() {
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		})
	}
	defer stopWake()

	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.closed || s.ackedCount >= s.total || (ctx != nil && ctx.Err() != nil) {
			return 0, 0, false
		}
		if s.relayDead[relayID] {
			return 0, 0, false
		}

		// Anti-bufferbloat window: in dynamic dual-relay mode, prevent a slow
		// relay from hoarding unacked chunks in kernel socket buffers while
		// still scaling with RTT so high-latency WAN links stay saturated.
		if s.policy == ChunkPolicyDynamic && s.aliveRelayCountLocked() > 1 &&
			s.inFlightForRelayLocked(relayID) >= s.dynamicInFlightLimitLocked(relayID) {
			s.cond.Wait()
			continue
		}

		if idx, found := s.popRetryLocked(relayID); found {
			s.newInFlightLocked(idx, relayID, connID, true)
			return s.offsets[idx], idx, true
		}

		if idx, found := s.claimNextLocked(relayID); found {
			s.newInFlightLocked(idx, relayID, connID, false)
			return s.offsets[idx], idx, true
		}

		// All primary chunks have been claimed! In dynamic mode, if this faster
		// relay has spare in-flight capacity while a slower relay still holds
		// unacknowledged tail chunks, work-steal those tail chunks so the transfer
		// finishes at the fast relay's speed.
		if idx, found := s.stealTailFromSlowRelayLocked(relayID, connID); found {
			off := s.offsets[idx]
			return off, idx, true
		}

		// No unclaimed chunks remain right now, but not all chunks are ACKed.
		// Wait in case another relay fails and its in-flight chunks are requeued.
		s.cond.Wait()
	}
}

func (s *reliableChunkScheduler) popRetryLocked(relayID int) (int, bool) {
	useParity := s.policy == ChunkPolicyParity && s.aliveRelayCountLocked() > 1
	for i := 0; i < len(s.retryQueue); i++ {
		candidate := s.retryQueue[i]
		if s.acked[candidate] {
			s.retryQueue = slices.Delete(s.retryQueue, i, i+1)
			i--
			continue
		}
		if _, alreadyInFlight := s.inFlight[candidate]; alreadyInFlight {
			s.retryQueue = slices.Delete(s.retryQueue, i, i+1)
			i--
			continue
		}
		if useParity && (candidate%2 != (relayID & 1)) {
			continue
		}
		s.retryQueue = slices.Delete(s.retryQueue, i, i+1)
		return candidate, true
	}
	return 0, false
}

func (s *reliableChunkScheduler) claimNextLocked(relayID int) (int, bool) {
	if s.policy == ChunkPolicyParity && s.aliveRelayCountLocked() > 1 {
		slot := relayID & 1
		for s.nextParity[slot] < s.total {
			idx := s.nextParity[slot]
			s.nextParity[slot] += 2
			if s.acked[idx] {
				continue
			}
			if _, inFlight := s.inFlight[idx]; inFlight {
				continue
			}
			return idx, true
		}
		return 0, false
	}

	// Dynamic mode (or parity fallback after one relay died): advance through
	// any unclaimed chunk regardless of parity.
	for s.nextDynamic < s.total {
		idx := s.nextDynamic
		s.nextDynamic++
		if s.acked[idx] {
			continue
		}
		if _, inFlight := s.inFlight[idx]; inFlight {
			continue
		}
		return idx, true
	}
	// Also drain any leftover parity slots if we switched from parity to dynamic.
	for slot := 0; slot < 2; slot++ {
		for s.nextParity[slot] < s.total {
			idx := s.nextParity[slot]
			s.nextParity[slot] += 2
			if s.acked[idx] {
				continue
			}
			if _, inFlight := s.inFlight[idx]; inFlight {
				continue
			}
			return idx, true
		}
	}
	return 0, false
}

// RequeueChunk puts a single failed send back into retryQueue immediately.
func (s *reliableChunkScheduler) RequeueChunk(reqIdx int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.dropInFlightLocked(reqIdx)
	if reqIdx >= 0 && reqIdx < s.total && !s.acked[reqIdx] {
		if !slices.Contains(s.retryQueue, reqIdx) {
			s.retryQueue = append(s.retryQueue, reqIdx)
		}
	}
	s.cond.Broadcast()
	s.mu.Unlock()
}

// RequeueRelay marks relayID as dead and moves all unacknowledged chunks that
// were in-flight on relayID back into retryQueue so surviving relays transmit
// them without interrupting the transfer.
func (s *reliableChunkScheduler) RequeueRelay(relayID int) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	requeued := s.failRelayLocked(relayID)
	s.cond.Broadcast()
	s.mu.Unlock()
	return requeued
}

// RequeueConn moves every unacknowledged chunk sent on one data connection
// (relayID, connID) back to the retry queue without declaring the whole relay
// dead. A single tunnel WebSocket can drop (e.g. during a Cloudflare edge
// restart) while the relay's other connections keep working.
func (s *reliableChunkScheduler) RequeueConn(relayID, connID int) int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	queued := make(map[int]struct{}, len(s.retryQueue))
	for _, idx := range s.retryQueue {
		queued[idx] = struct{}{}
	}
	requeued := 0
	for reqIdx, entry := range s.inFlight {
		if entry.relayID != relayID || entry.connID != connID {
			continue
		}
		s.dropInFlightLocked(reqIdx)
		if _, exists := queued[reqIdx]; !s.acked[reqIdx] && !exists {
			s.retryQueue = append(s.retryQueue, reqIdx)
			queued[reqIdx] = struct{}{}
			requeued++
		}
	}
	slices.Sort(s.retryQueue)
	s.cond.Broadcast()
	return requeued
}

// failRelayLocked marks relayID dead and moves its unacknowledged in-flight
// chunks to the retry queue.
func (s *reliableChunkScheduler) failRelayLocked(relayID int) int {
	s.relayDead[relayID] = true
	requeued := 0
	queued := make(map[int]struct{}, len(s.retryQueue))
	for _, idx := range s.retryQueue {
		queued[idx] = struct{}{}
	}
	for reqIdx, entry := range s.inFlight {
		if entry.relayID != relayID {
			continue
		}
		s.dropInFlightLocked(reqIdx)
		if _, exists := queued[reqIdx]; !s.acked[reqIdx] && !exists {
			s.retryQueue = append(s.retryQueue, reqIdx)
			queued[reqIdx] = struct{}{}
			requeued++
		}
	}
	slices.Sort(s.retryQueue)
	return requeued
}

// reqIdxForFileChunkLocked maps a file chunk index from a receiver ACK to the
// scheduler's logical request index.
func (s *reliableChunkScheduler) reqIdxForFileChunkLocked(fileChunkIdx int) (int, bool) {
	if fileChunkIdx < 0 {
		return 0, false
	}
	off := int64(fileChunkIdx) * s.chunkSize
	if s.sortedOffsets {
		return slices.BinarySearch(s.offsets, off)
	}
	idx, ok := s.offsetToIndex[off]
	return idx, ok
}

// applyCumulativeAckLocked marks every chunk below cumChunk as acknowledged,
// visiting only chunks that the previous cumulative ACK did not cover. The
// previous implementation rescanned from chunk 0 on every ACK, which is
// quadratic in the file size and held the scheduler lock for milliseconds per
// ACK on multi-GiB files.
func (s *reliableChunkScheduler) applyCumulativeAckLocked(cumChunk int) {
	if cumChunk <= s.cumApplied {
		return
	}
	if s.sortedOffsets {
		start, _ := slices.BinarySearch(s.offsets, int64(s.cumApplied)*s.chunkSize)
		limit := int64(cumChunk) * s.chunkSize
		for reqIdx := start; reqIdx < s.total && s.offsets[reqIdx] < limit; reqIdx++ {
			s.markAckedLocked(reqIdx)
		}
	} else {
		for i := s.cumApplied; i < cumChunk; i++ {
			if reqIdx, exists := s.reqIdxForFileChunkLocked(i); exists {
				s.markAckedLocked(reqIdx)
			}
		}
	}
	s.cumApplied = cumChunk
}

// ApplyAck processes a receiver's ChunkAckPayload, marking confirmed chunks and
// requeueing any in-flight chunks from a relay reported dead by the receiver.
func (s *reliableChunkScheduler) ApplyAck(ack ChunkAckPayload) (allAcked bool) {
	if s == nil {
		return true
	}
	s.mu.Lock()
	s.lastAckAt = time.Now()
	if ack.Finished {
		s.markAllAckedLocked()
		s.mu.Unlock()
		s.signalDone()
		return true
	}

	// 1. Mark all chunks with file chunkIdx < ack.CumChunk.
	s.applyCumulativeAckLocked(int(ack.CumChunk))
	// 2. Mark selective bitmask chunks [CumChunk+1 .. CumChunk+64].
	for bit := 0; bit < 64; bit++ {
		if (ack.Bitmask & (uint64(1) << uint(bit))) != 0 {
			if reqIdx, exists := s.reqIdxForFileChunkLocked(int(ack.CumChunk) + 1 + bit); exists {
				s.markAckedLocked(reqIdx)
			}
		}
	}
	// 3. Mark explicit AckedList chunks.
	for _, fileChunkIdx := range ack.AckedList {
		if reqIdx, exists := s.reqIdxForFileChunkLocked(int(fileChunkIdx)); exists {
			s.markAckedLocked(reqIdx)
		}
	}

	if ack.FailedRelay >= 0 {
		s.failRelayLocked(ack.FailedRelay)
	}

	finished := s.ackedCount >= s.total
	if finished {
		s.closed = true
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	if finished {
		s.signalDone()
	}
	return finished
}

// AckSingle marks a single logical request index as acknowledged/completed.
func (s *reliableChunkScheduler) AckSingle(reqIdx int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.markAckedLocked(reqIdx)
	finished := s.ackedCount >= s.total
	if finished {
		s.closed = true
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	if finished {
		s.signalDone()
	}
}

// MarkAllAcked is called when TypeCloseSender arrives from the receiver,
// confirming that the receiver has written and closed the entire file.
func (s *reliableChunkScheduler) MarkAllAcked() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.markAllAckedLocked()
	s.mu.Unlock()
	s.signalDone()
}

// MarkSentFirstTime returns true only the first time reqIdx is transmitted so
// retransmissions during failover do not double-count sender progress.
func (s *reliableChunkScheduler) MarkSentFirstTime(reqIdx int) bool {
	if s == nil {
		return true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if reqIdx < 0 || reqIdx >= s.total {
		return false
	}
	s.sendCount++
	// Stamp the send completion time: RTT samples then measure the network and
	// ACK path rather than time spent reading/encrypting the chunk locally.
	if entry, ok := s.inFlight[reqIdx]; ok && !entry.sent {
		entry.sent = true
		entry.sentAt = time.Now()
		s.inFlight[reqIdx] = entry
	}
	if s.progressCounted[reqIdx] {
		return false
	}
	s.progressCounted[reqIdx] = true
	s.firstSendCount++
	return true
}

// SchedulerStats summarizes a finished or running transfer for diagnostics.
type SchedulerStats struct {
	Chunks          int
	Transmissions   int64
	Retransmissions int64
	Window          [2]int
	MinRTT          [2]time.Duration
	SRTT            [2]time.Duration
	RateBps         [2]float64
}

// Stats returns a snapshot of scheduler counters and per-relay estimates.
func (s *reliableChunkScheduler) Stats() SchedulerStats {
	if s == nil {
		return SchedulerStats{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := SchedulerStats{
		Chunks:          s.total,
		Transmissions:   s.sendCount,
		Retransmissions: s.sendCount - s.firstSendCount,
	}
	for r := range s.est {
		st.Window[r] = s.est[r].window
		st.MinRTT[r] = s.est[r].minRTT
		st.SRTT[r] = s.est[r].srtt
		st.RateBps[r] = s.est[r].maxRate
	}
	return st
}

// StartWatchdog periodically checks for unacknowledged in-flight chunks that
// have stalled beyond staleAfter (e.g., sitting in a dead socket buffer) once
// all primary offsets have been claimed, requeueing them onto surviving workers.
func (s *reliableChunkScheduler) StartWatchdog(ctx context.Context, staleAfter time.Duration) {
	if s == nil || staleAfter <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(staleAfter / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				s.mu.Lock()
				if s.closed || s.ackedCount >= s.total {
					s.mu.Unlock()
					return
				}
				allClaimed := s.nextDynamic >= s.total ||
					(s.nextParity[0] >= s.total && s.nextParity[1] >= s.total) ||
					len(s.relayDead) > 0
				ackStalled := s.lastAckAt.IsZero() || now.Sub(s.lastAckAt) >= staleAfter
				requeued := false
				if allClaimed {
					for reqIdx, entry := range s.inFlight {
						if s.relayDead[entry.relayID] || (ackStalled && now.Sub(entry.sentAt) >= staleAfter) {
							s.dropInFlightLocked(reqIdx)
							if !s.acked[reqIdx] && !slices.Contains(s.retryQueue, reqIdx) {
								s.retryQueue = append(s.retryQueue, reqIdx)
								requeued = true
							}
						}
					}
				}
				if requeued {
					slices.Sort(s.retryQueue)
					s.cond.Broadcast()
				}
				s.mu.Unlock()
			}
		}
	}()
}

func (s *reliableChunkScheduler) markAckedLocked(reqIdx int) {
	if reqIdx < 0 || reqIdx >= s.total {
		return
	}
	if entry, ok := s.dropInFlightLocked(reqIdx); ok && !s.acked[reqIdx] {
		s.observeDeliveryLocked(entry, reqIdx, time.Now())
	}
	if !s.acked[reqIdx] {
		s.acked[reqIdx] = true
		s.ackedCount++
	}
}

// chunkBytesLocked returns the payload size of a logical request index.
func (s *reliableChunkScheduler) chunkBytesLocked(reqIdx int) int64 {
	off := s.offsets[reqIdx]
	if s.fileSize > 0 && off+s.chunkSize > s.fileSize {
		return max(s.fileSize-off, 0)
	}
	return s.chunkSize
}

// observeDeliveryLocked feeds one acknowledged chunk into the estimator of the
// relay that carried it: an RTT sample (only for first transmissions whose
// send completed, per Karn's algorithm) and the delivered-bytes rate sample.
func (s *reliableChunkScheduler) observeDeliveryLocked(entry inFlightChunk, reqIdx int, now time.Time) {
	e := &s.est[entry.relayID&1]
	if entry.sent && !entry.retrans {
		if sample := now.Sub(entry.sentAt); sample > 0 && sample < 30*time.Second {
			if e.srtt == 0 {
				e.srtt = sample
			} else {
				e.srtt = (7*e.srtt + sample) / 8
			}
			if e.minRTT == 0 || sample < e.minRTT || now.Sub(e.minRTTAt) > minRTTExpiry {
				e.minRTT = sample
				e.minRTTAt = now
			}
		}
	}
	if e.sampleStart.IsZero() {
		e.sampleStart = now
	}
	e.sampleBytes += s.chunkBytesLocked(reqIdx)
	// Measure delivery over at least one RTT: ACKs arrive in bursts, and short
	// intervals over-estimate the rate (and therefore the window) severalfold.
	elapsed := now.Sub(e.sampleStart)
	if elapsed < max(rateSampleMinInterval, e.minRTT) {
		return
	}
	e.rates[e.rateNext] = float64(e.sampleBytes) / elapsed.Seconds()
	e.rateNext = (e.rateNext + 1) % rateFilterLen
	e.rateCount++
	e.maxRate = 0
	for i := 0; i < min(e.rateCount, rateFilterLen); i++ {
		e.maxRate = max(e.maxRate, e.rates[i])
	}
	e.sampleStart = now
	e.sampleBytes = 0
	s.updateWindowLocked(e)
}

func (s *reliableChunkScheduler) markAllAckedLocked() {
	for i := range s.acked {
		s.acked[i] = true
	}
	s.ackedCount = s.total
	s.clearInFlightLocked()
	s.retryQueue = nil
	s.closed = true
	s.cond.Broadcast()
}
