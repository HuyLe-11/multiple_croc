package croc

import (
	"context"
	"fmt"
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

	// minDynamicInFlightPerRelay caps unacknowledged chunks per relay on low-RTT
	// LAN links (16 chunks = 512 KiB) to prevent kernel TCP bufferbloat on a slow
	// relay, while scaling adaptively with observed RTT up to
	// maxDynamicInFlightPerRelay (256 chunks = 8 MiB) on high-RTT WAN/Cloudflare links.
	minDynamicInFlightPerRelay = 16
	maxDynamicInFlightPerRelay = 256
)

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
	stolen   bool
}

// reliableChunkScheduler extends requestedChunkQueue with in-flight tracking by
// relayID, dynamic or even/odd parity scheduling, receiver ACK tracking, and
// zero-downtime requeueing when one relay fails mid-transfer.
type reliableChunkScheduler struct {
	mu            sync.Mutex
	cond          *sync.Cond
	policy        string
	chunkSize     int64
	fileSize      int64
	offsets       []int64
	offsetToIndex map[int64]int
	total         int

	// Scheduling state
	nextDynamic     int
	nextParity      [2]int
	retryQueue      []int
	inFlight        map[int]inFlightChunk
	acked           []bool
	progressCounted []bool
	ackedCount      int
	lastAckAt       time.Time
	smoothedRTT     time.Duration
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
	var offsets []int64
	for i := 1; i+1 < len(ranges); i += 2 {
		start := ranges[i]
		count := ranges[i+1]
		for k := int64(0); k < count; k++ {
			offsets = append(offsets, start+k*stepSize)
		}
	}
	offsetToIndex := make(map[int64]int, len(offsets))
	for i, off := range offsets {
		offsetToIndex[off] = i
	}
	if relayCount <= 0 {
		relayCount = 1
	}
	s := &reliableChunkScheduler{
		policy:        parsedPolicy,
		chunkSize:     stepSize,
		fileSize:      fileSize,
		offsets:       offsets,
		offsetToIndex: offsetToIndex,
		total:         len(offsets),
		nextParity:    [2]int{0, 1},
		inFlight:        make(map[int]inFlightChunk),
		acked:           make([]bool, len(offsets)),
		progressCounted: make([]bool, len(offsets)),
		relayDead:       make(map[int]bool),
		activeCount:   relayCount,
		onDone:        onDone,
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

func (s *reliableChunkScheduler) dynamicInFlightLimitLocked() int {
	if s.smoothedRTT <= 25*time.Millisecond {
		return minDynamicInFlightPerRelay
	}
	// Scale window linearly with RTT (~2 chunks per ms) up to 256 chunks (8 MiB)
	// so high-RTT WAN/Cloudflare tunnels stay saturated without stalling on ACKs.
	limit := int(s.smoothedRTT/time.Millisecond) * 2
	if limit < minDynamicInFlightPerRelay {
		return minDynamicInFlightPerRelay
	}
	if limit > maxDynamicInFlightPerRelay {
		return maxDynamicInFlightPerRelay
	}
	return limit
}

func (s *reliableChunkScheduler) inFlightForRelayLocked(relayID int) int {
	count := 0
	for _, entry := range s.inFlight {
		if entry.relayID == relayID {
			count++
		}
	}
	return count
}

func (s *reliableChunkScheduler) stealTailFromSlowRelayLocked(relayID int) (int, bool) {
	if s.policy != ChunkPolicyDynamic || s.aliveRelayCountLocked() <= 1 {
		return 0, false
	}
	bestIdx := -1
	var oldestTime time.Time
	for reqIdx, entry := range s.inFlight {
		if s.acked[reqIdx] || entry.relayID == relayID || entry.stolen {
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
		entry.stolen = true
		entry.sentAt = time.Now()
		s.inFlight[bestIdx] = entry
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
			s.inFlightForRelayLocked(relayID) >= s.dynamicInFlightLimitLocked() {
			s.cond.Wait()
			continue
		}

		if idx, found := s.popRetryLocked(relayID); found {
			off := s.offsets[idx]
			s.inFlight[idx] = inFlightChunk{
				chunkIdx: uint32(off / s.chunkSize),
				offset:   off,
				relayID:  relayID,
				connID:   connID,
				sentAt:   time.Now(),
			}
			return off, idx, true
		}

		if idx, found := s.claimNextLocked(relayID); found {
			off := s.offsets[idx]
			s.inFlight[idx] = inFlightChunk{
				chunkIdx: uint32(off / s.chunkSize),
				offset:   off,
				relayID:  relayID,
				connID:   connID,
				sentAt:   time.Now(),
			}
			return off, idx, true
		}

		// All primary chunks have been claimed! In dynamic mode, if this faster
		// relay has spare in-flight capacity while a slower relay still holds
		// unacknowledged tail chunks, work-steal those tail chunks so the transfer
		// finishes at the fast relay's speed.
		if idx, found := s.stealTailFromSlowRelayLocked(relayID); found {
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
	delete(s.inFlight, reqIdx)
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
	s.relayDead[relayID] = true
	requeued := 0
	for reqIdx, entry := range s.inFlight {
		if entry.relayID == relayID {
			delete(s.inFlight, reqIdx)
			if !s.acked[reqIdx] && !slices.Contains(s.retryQueue, reqIdx) {
				s.retryQueue = append(s.retryQueue, reqIdx)
				requeued++
			}
		}
	}
	slices.Sort(s.retryQueue)
	s.cond.Broadcast()
	s.mu.Unlock()
	return requeued
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
	for i := 0; i < int(ack.CumChunk); i++ {
		off := int64(i) * s.chunkSize
		if reqIdx, exists := s.offsetToIndex[off]; exists {
			s.markAckedLocked(reqIdx)
		}
	}
	// 2. Mark selective bitmask chunks [CumChunk+1 .. CumChunk+64].
	for bit := 0; bit < 64; bit++ {
		if (ack.Bitmask & (uint64(1) << uint(bit))) != 0 {
			fileChunkIdx := int(ack.CumChunk) + 1 + bit
			off := int64(fileChunkIdx) * s.chunkSize
			if reqIdx, exists := s.offsetToIndex[off]; exists {
				s.markAckedLocked(reqIdx)
			}
		}
	}
	// 3. Mark explicit AckedList chunks.
	for _, fileChunkIdx := range ack.AckedList {
		off := int64(fileChunkIdx) * s.chunkSize
		if reqIdx, exists := s.offsetToIndex[off]; exists {
			s.markAckedLocked(reqIdx)
		}
	}

	if ack.FailedRelay >= 0 {
		s.relayDead[ack.FailedRelay] = true
		for reqIdx, entry := range s.inFlight {
			if entry.relayID == ack.FailedRelay {
				delete(s.inFlight, reqIdx)
				if !s.acked[reqIdx] && !slices.Contains(s.retryQueue, reqIdx) {
					s.retryQueue = append(s.retryQueue, reqIdx)
				}
			}
		}
		slices.Sort(s.retryQueue)
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
	if reqIdx < 0 || reqIdx >= s.total || s.progressCounted[reqIdx] {
		return false
	}
	s.progressCounted[reqIdx] = true
	return true
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
							delete(s.inFlight, reqIdx)
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
	if entry, ok := s.inFlight[reqIdx]; ok {
		sample := time.Since(entry.sentAt)
		if sample > 0 && sample < 10*time.Second {
			if s.smoothedRTT == 0 {
				s.smoothedRTT = sample
			} else {
				s.smoothedRTT = (7*s.smoothedRTT + sample) / 8
			}
		}
		delete(s.inFlight, reqIdx)
	}
	if !s.acked[reqIdx] {
		s.acked[reqIdx] = true
		s.ackedCount++
	}
}

func (s *reliableChunkScheduler) markAllAckedLocked() {
	for i := range s.acked {
		s.acked[i] = true
	}
	s.ackedCount = s.total
	clear(s.inFlight)
	s.retryQueue = nil
	s.closed = true
	s.cond.Broadcast()
}
