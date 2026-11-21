package tcp

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/croc/v11/src/utils"
)

var (
	// ErrFanoutMemoryLimit is returned when a relay's global fan-out memory
	// budget cannot accommodate a new fan-out room.
	ErrFanoutMemoryLimit = errors.New("relay fanout memory budget exceeded")
	// ErrNoActiveReceivers is returned when all subscribers have left or been
	// kicked from a fan-out session.
	ErrNoActiveReceivers = errors.New("no active fanout receivers remaining")
	// ErrFanoutClosed indicates that a fan-out buffer has been closed.
	ErrFanoutClosed = errors.New("fanout ring buffer closed")
)

// FanoutConfig defines relay-enforced limits and defaults for 1-to-N fan-out rooms.
type FanoutConfig struct {
	MaxBufferBytes int64
	MaxMemoryBytes int64
	MaxReceivers   int
	MinRateBps     int64
	RateWindow     time.Duration
	StallTimeout   time.Duration
	StrictMinRate  bool
}

func defaultFanoutConfig() FanoutConfig {
	return FanoutConfig{
		MaxBufferBytes: DEFAULT_FANOUT_BUFFER,
		MaxMemoryBytes: DEFAULT_FANOUT_MAX_MEMORY,
		MaxReceivers:   DEFAULT_FANOUT_MAX_RECEIVERS,
		MinRateBps:     DEFAULT_FANOUT_MIN_RATE,
		RateWindow:     DEFAULT_FANOUT_RATE_WINDOW,
		StallTimeout:   DEFAULT_FANOUT_STALL_TIMEOUT,
	}
}

// FanoutMemoryBudget enforces an atomic global memory cap across all active
// fan-out rooms on a relay process so concurrent rooms cannot cause an OOM.
type FanoutMemoryBudget struct {
	maxBytes int64
	reserved atomic.Int64
}

// NewFanoutMemoryBudget creates a global memory budget with the given cap.
func NewFanoutMemoryBudget(maxBytes int64) *FanoutMemoryBudget {
	if maxBytes <= 0 {
		maxBytes = DEFAULT_FANOUT_MAX_MEMORY
	}
	return &FanoutMemoryBudget{maxBytes: maxBytes}
}

// Reserve attempts to reserve bytes from the global budget.
func (b *FanoutMemoryBudget) Reserve(bytes int64) bool {
	if b == nil || bytes <= 0 {
		return true
	}
	for {
		cur := b.reserved.Load()
		if cur+bytes > b.maxBytes {
			return false
		}
		if b.reserved.CompareAndSwap(cur, cur+bytes) {
			return true
		}
	}
}

// Release returns bytes to the global budget.
func (b *FanoutMemoryBudget) Release(bytes int64) {
	if b == nil || bytes <= 0 {
		return
	}
	for {
		cur := b.reserved.Load()
		next := max(int64(0), cur-bytes)
		if b.reserved.CompareAndSwap(cur, next) {
			return
		}
	}
}

// Reserved returns the currently reserved bytes across all rooms.
func (b *FanoutMemoryBudget) Reserved() int64 {
	if b == nil {
		return 0
	}
	return b.reserved.Load()
}

type rateSample struct {
	at    time.Time
	bytes int64
}

// slidingRateWindow computes a receiver's rolling average receive rate over a
// configurable time window.
type slidingRateWindow struct {
	mu      sync.Mutex
	window  time.Duration
	samples []rateSample
	total   int64
}

func newSlidingRateWindow(window time.Duration) *slidingRateWindow {
	if window <= 0 {
		window = DEFAULT_FANOUT_RATE_WINDOW
	}
	return &slidingRateWindow{window: window}
}

func (w *slidingRateWindow) Add(now time.Time, delta int64) {
	if delta <= 0 {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneLocked(now)
	w.samples = append(w.samples, rateSample{at: now, bytes: delta})
	w.total += delta
}

func (w *slidingRateWindow) RateBytesPerSec(now time.Time) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.pruneLocked(now)
	if w.window <= 0 {
		return 0
	}
	return int64(float64(w.total) / w.window.Seconds())
}

func (w *slidingRateWindow) pruneLocked(now time.Time) {
	cutoff := now.Add(-w.window)
	idx := 0
	for idx < len(w.samples) && !w.samples[idx].at.After(cutoff) {
		w.total -= w.samples[idx].bytes
		idx++
	}
	if idx > 0 {
		w.samples = append(w.samples[:0], w.samples[idx:]...)
	}
	if w.total < 0 {
		w.total = 0
	}
}

type fanoutSlot struct {
	seq     uint64
	payload []byte
	size    int64
}

type fanoutSubscriber struct {
	id           uint16
	nextReadSeq  uint64
	ackedSeq     uint64
	ackedBytes   int64
	joinedAt     time.Time
	lastAckAt    time.Time
	rate         *slidingRateWindow
	eofDelivered bool
	kicked       bool
	kickReason   string
	removed      bool
}

// FanoutKickCallback is invoked when the watchdog kicks a slow or stalled receiver.
type FanoutKickCallback func(peerID uint16, reason string, ackedBytes int64)

// FanoutRingBuffer holds in-flight encrypted chunks for a 1-to-N fan-out room
// within a strict per-room memory cap. It applies backpressure to the sender
// when full and kicks receivers whose rolling throughput falls below minRateBps.
type FanoutRingBuffer struct {
	mu            sync.Mutex
	notFull       *sync.Cond
	notEmpty      *sync.Cond
	maxBytes      int64
	minRateBps    int64
	rateWindow    time.Duration
	stallTimeout  time.Duration
	strictMinRate bool
	onKick        FanoutKickCallback

	slots         []fanoutSlot
	headSeq       uint64
	nextSeq       uint64
	usedBytes     int64
	peakUsedBytes atomic.Int64
	firstDataAt   time.Time
	streamEOF     bool
	aborted       bool
	closed        bool
	subscribers   map[uint16]*fanoutSubscriber
}

// NewFanoutRingBuffer creates a bounded ring buffer for one fan-out room.
func NewFanoutRingBuffer(cfg FanoutConfig, onKick FanoutKickCallback) *FanoutRingBuffer {
	if cfg.MaxBufferBytes <= 0 {
		cfg.MaxBufferBytes = DEFAULT_FANOUT_BUFFER
	}
	if cfg.RateWindow <= 0 {
		cfg.RateWindow = DEFAULT_FANOUT_RATE_WINDOW
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = DEFAULT_FANOUT_STALL_TIMEOUT
	}
	rb := &FanoutRingBuffer{
		maxBytes:      cfg.MaxBufferBytes,
		minRateBps:    cfg.MinRateBps,
		rateWindow:    cfg.RateWindow,
		stallTimeout:  cfg.StallTimeout,
		strictMinRate: cfg.StrictMinRate,
		onKick:        onKick,
		subscribers:   make(map[uint16]*fanoutSubscriber),
	}
	rb.notFull = sync.NewCond(&rb.mu)
	rb.notEmpty = sync.NewCond(&rb.mu)
	return rb
}

// PeakUsedBytes returns the highest buffer occupancy observed so far.
func (b *FanoutRingBuffer) PeakUsedBytes() int64 {
	return b.peakUsedBytes.Load()
}

// UsedBytes returns the current buffer occupancy in bytes.
func (b *FanoutRingBuffer) UsedBytes() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usedBytes
}

// ActiveSubscriberCount returns the number of currently active subscribers.
func (b *FanoutRingBuffer) ActiveSubscriberCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.subscribers)
}

// AddSubscriber registers a receiver before or at stream start.
func (b *FanoutRingBuffer) AddSubscriber(peerID uint16) {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.subscribers[peerID]; exists {
		return
	}
	b.subscribers[peerID] = &fanoutSubscriber{
		id:          peerID,
		nextReadSeq: b.headSeq,
		ackedSeq:    b.headSeq,
		joinedAt:    now,
		lastAckAt:   now,
		rate:        newSlidingRateWindow(b.rateWindow),
	}
}

// RemoveSubscriber removes a disconnected or kicked receiver and immediately
// evicts any slots that were only held back by that receiver.
func (b *FanoutRingBuffer) RemoveSubscriber(peerID uint16) {
	b.mu.Lock()
	sub, exists := b.subscribers[peerID]
	if exists {
		sub.removed = true
		delete(b.subscribers, peerID)
		b.evictLocked()
		b.notFull.Broadcast()
		b.notEmpty.Broadcast()
	}
	b.mu.Unlock()
}

// ResetForNextFile clears buffered state before starting the next file in a
// multi-file fan-out session while preserving remaining active subscribers.
func (b *FanoutRingBuffer) ResetForNextFile() {
	now := time.Now()
	b.mu.Lock()
	b.slots = nil
	b.headSeq = 0
	b.nextSeq = 0
	b.usedBytes = 0
	b.firstDataAt = time.Time{}
	b.streamEOF = false
	for _, sub := range b.subscribers {
		sub.nextReadSeq = 0
		sub.ackedSeq = 0
		sub.eofDelivered = false
		sub.lastAckAt = now
		sub.rate = newSlidingRateWindow(b.rateWindow)
	}
	b.notFull.Broadcast()
	b.notEmpty.Broadcast()
	b.mu.Unlock()
}

// Push appends an encrypted chunk frame to the bounded buffer, blocking when
// usedBytes + len(payload) would exceed maxBytes (backpressure).
func (b *FanoutRingBuffer) Push(ctx context.Context, payload []byte) (uint64, error) {
	stopWake := func() bool { return true }
	if ctx != nil {
		stopWake = context.AfterFunc(ctx, func() {
			b.mu.Lock()
			b.notFull.Broadcast()
			b.mu.Unlock()
		})
	}
	defer stopWake()

	frameSize := int64(len(payload))
	copied := append([]byte(nil), payload...)

	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		if b.closed || b.aborted {
			return 0, ErrFanoutClosed
		}
		if ctx != nil && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if len(b.subscribers) == 0 {
			return 0, ErrNoActiveReceivers
		}
		// Allow a single frame larger than maxBytes only when the buffer is empty.
		if b.usedBytes == 0 || b.usedBytes+frameSize <= b.maxBytes {
			break
		}
		b.notFull.Wait()
	}

	now := time.Now()
	if b.firstDataAt.IsZero() {
		b.firstDataAt = now
		for _, sub := range b.subscribers {
			sub.lastAckAt = now
		}
	}
	if b.streamEOF {
		b.streamEOF = false
		for _, sub := range b.subscribers {
			sub.eofDelivered = false
		}
	}
	seq := b.nextSeq
	b.nextSeq++
	b.slots = append(b.slots, fanoutSlot{
		seq:     seq,
		payload: copied,
		size:    frameSize,
	})
	b.usedBytes += frameSize
	for {
		prevPeak := b.peakUsedBytes.Load()
		if b.usedBytes <= prevPeak || b.peakUsedBytes.CompareAndSwap(prevPeak, b.usedBytes) {
			break
		}
	}
	b.notEmpty.Broadcast()
	return seq, nil
}

// MarkStreamEOF signals that the sender has pushed all chunks for the current file.
func (b *FanoutRingBuffer) MarkStreamEOF() {
	b.mu.Lock()
	b.streamEOF = true
	b.notEmpty.Broadcast()
	b.notFull.Broadcast()
	b.mu.Unlock()
}

// Abort marks the stream as abnormally terminated (e.g., sender disconnected).
func (b *FanoutRingBuffer) Abort() {
	b.mu.Lock()
	b.aborted = true
	b.notEmpty.Broadcast()
	b.notFull.Broadcast()
	b.mu.Unlock()
}

// Close permanently shuts down the ring buffer.
func (b *FanoutRingBuffer) Close() {
	b.mu.Lock()
	b.closed = true
	b.slots = nil
	b.usedBytes = 0
	b.notEmpty.Broadcast()
	b.notFull.Broadcast()
	b.mu.Unlock()
}

// NextForSubscriber blocks until the next frame for peerID is available, EOF is
// reached, or the subscriber is kicked/removed.
func (b *FanoutRingBuffer) NextForSubscriber(ctx context.Context, peerID uint16) (seq uint64, payload []byte, eof bool, err error) {
	stopWake := func() bool { return true }
	if ctx != nil {
		stopWake = context.AfterFunc(ctx, func() {
			b.mu.Lock()
			b.notEmpty.Broadcast()
			b.mu.Unlock()
		})
	}
	defer stopWake()

	b.mu.Lock()
	defer b.mu.Unlock()

	for {
		if b.closed {
			return 0, nil, false, ErrFanoutClosed
		}
		if ctx != nil && ctx.Err() != nil {
			return 0, nil, false, ctx.Err()
		}
		sub, exists := b.subscribers[peerID]
		if !exists || sub.removed {
			if sub != nil && sub.kicked {
				return 0, nil, false, fmt.Errorf("kicked: %s", sub.kickReason)
			}
			return 0, nil, false, ErrFanoutClosed
		}
		if sub.nextReadSeq < b.headSeq {
			return 0, nil, false, errors.New("subscriber fell behind ring buffer head")
		}
		if sub.nextReadSeq < b.nextSeq {
			idx := int(sub.nextReadSeq - b.headSeq)
			slot := b.slots[idx]
			sub.nextReadSeq++
			return slot.seq, slot.payload, false, nil
		}
		if b.aborted {
			return 0, nil, false, errors.New("sender disconnected before transfer completed")
		}
		if b.streamEOF && !sub.eofDelivered {
			sub.eofDelivered = true
			return b.nextSeq, nil, true, nil
		}
		b.notEmpty.Wait()
	}
}

// Ack records that peerID has received and written all frames up to ackedSeq
// (exclusive) and adds bytesDelta to its sliding rate window.
func (b *FanoutRingBuffer) Ack(peerID uint16, ackedSeq uint64, bytesDelta int64) {
	now := time.Now()
	b.mu.Lock()
	sub, exists := b.subscribers[peerID]
	if !exists || sub.removed {
		b.mu.Unlock()
		return
	}
	if ackedSeq > sub.ackedSeq {
		sub.ackedSeq = ackedSeq
		sub.lastAckAt = now
	}
	if bytesDelta > 0 {
		sub.ackedBytes += bytesDelta
		sub.lastAckAt = now
		sub.rate.Add(now, bytesDelta)
	}
	b.evictLocked()
	b.notFull.Broadcast()
	b.mu.Unlock()
}

// evictLocked frees all slots from the head of the ring buffer that have been
// both read and acknowledged by every active subscriber.
func (b *FanoutRingBuffer) evictLocked() {
	if len(b.subscribers) == 0 || len(b.slots) == 0 {
		return
	}
	minSeq := ^uint64(0)
	for _, sub := range b.subscribers {
		effective := min(sub.nextReadSeq, sub.ackedSeq)
		if effective < minSeq {
			minSeq = effective
		}
	}
	if minSeq <= b.headSeq {
		return
	}
	dropCount := int(min(minSeq, b.nextSeq) - b.headSeq)
	if dropCount <= 0 {
		return
	}
	if dropCount > len(b.slots) {
		dropCount = len(b.slots)
	}
	for i := 0; i < dropCount; i++ {
		b.usedBytes -= b.slots[i].size
		b.slots[i].payload = nil
	}
	if b.usedBytes < 0 {
		b.usedBytes = 0
	}
	b.slots = append(b.slots[:0], b.slots[dropCount:]...)
	b.headSeq += uint64(dropCount)
}

// StartWatchdog periodically evaluates all active subscribers and kicks any
// subscriber that stalls beyond stallTimeout or whose rolling throughput over
// rateWindow falls below minRateBps (when >1 subscriber is active or strictMinRate is set).
func (b *FanoutRingBuffer) StartWatchdog(ctx context.Context) {
	interval := min(500*time.Millisecond, max(b.rateWindow/5, 40*time.Millisecond))
	if b.stallTimeout > 0 && b.stallTimeout/4 < interval {
		interval = max(b.stallTimeout/4, 30*time.Millisecond)
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				b.evaluateWatchdog(now)
			}
		}
	}()
}

type kickedPeerInfo struct {
	peerID     uint16
	reason     string
	ackedBytes int64
}

func (b *FanoutRingBuffer) evaluateWatchdog(now time.Time) {
	var kickedList []kickedPeerInfo

	b.mu.Lock()
	if b.closed || b.aborted || b.firstDataAt.IsZero() || len(b.subscribers) == 0 {
		b.mu.Unlock()
		return
	}
	activeCount := len(b.subscribers)
	elapsedSinceFirstData := now.Sub(b.firstDataAt)

	for id, sub := range b.subscribers {
		// If subscriber has no buffered backlog waiting for it (caught up with the
		// sender or at most 1 frame in flight before streamEOF), it is keeping up
		// with the stream and must not be penalized for sender backpressure pauses.
		hasBufferedBacklog := (b.nextSeq > sub.ackedSeq && (b.nextSeq-sub.ackedSeq >= 2 || b.streamEOF))
		if !hasBufferedBacklog {
			sub.lastAckAt = now
			continue
		}

		// 1. Stall timeout check (applies even to the last remaining receiver).
		if b.stallTimeout > 0 && !sub.lastAckAt.IsZero() && now.Sub(sub.lastAckAt) >= b.stallTimeout {
			reason := fmt.Sprintf(
				"receiver stalled for %s (threshold %s)",
				now.Sub(sub.lastAckAt).Round(10*time.Millisecond),
				b.stallTimeout,
			)
			sub.kicked = true
			sub.kickReason = reason
			sub.removed = true
			delete(b.subscribers, id)
			kickedList = append(kickedList, kickedPeerInfo{
				peerID:     id,
				reason:     reason,
				ackedBytes: sub.ackedBytes,
			})
			activeCount--
			continue
		}

		// 2. Minimum rolling speed threshold check.
		// By policy, do not kick the last remaining receiver for low speed
		// unless strictMinRate is explicitly enabled.
		if b.minRateBps > 0 && elapsedSinceFirstData >= b.rateWindow && (activeCount > 1 || b.strictMinRate) {
			rate := sub.rate.RateBytesPerSec(now)
			if rate < b.minRateBps {
				reason := fmt.Sprintf(
					"transfer speed (%s/s) fell below threshold (%s/s)",
					utils.ByteCountDecimal(rate),
					utils.ByteCountDecimal(b.minRateBps),
				)
				sub.kicked = true
				sub.kickReason = reason
				sub.removed = true
				delete(b.subscribers, id)
				kickedList = append(kickedList, kickedPeerInfo{
					peerID:     id,
					reason:     reason,
					ackedBytes: sub.ackedBytes,
				})
				activeCount--
			}
		}
	}

	if len(kickedList) > 0 {
		b.evictLocked()
		b.notFull.Broadcast()
		b.notEmpty.Broadcast()
	}
	onKick := b.onKick
	b.mu.Unlock()

	if onKick != nil {
		for _, k := range kickedList {
			onKick(k.peerID, k.reason, k.ackedBytes)
		}
	}
}
