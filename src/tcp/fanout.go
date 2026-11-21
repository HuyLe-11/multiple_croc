package tcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/crypt"
	log "github.com/schollz/croc/v11/src/logger"
)

const (
	// FanoutRoomPrefix identifies rooms managed by the 1-to-N fan-out engine.
	FanoutRoomPrefix = "fanout:v1:"

	// FanoutRelayEventPrefix marks unencrypted relay event notices delivered
	// directly to a receiver's control connection.
	FanoutRelayEventPrefix = "croc-fanout-event-v1:"

	// Multiplexed control operations between Sender Control <-> Relay.
	FanoutOpPeerJoined      uint8 = 0x01
	FanoutOpPeerLeft        uint8 = 0x02
	FanoutOpPeerKicked      uint8 = 0x03
	FanoutOpPeerFrame       uint8 = 0x04
	FanoutOpLockLobby       uint8 = 0x05
	FanoutOpKickPeer        uint8 = 0x06
	FanoutOpResetDataStream uint8 = 0x07

	// Data-plane frame markers on Fan-out Data ports.
	FanoutDataFrameChunk uint8 = 0x01
	FanoutDataFrameEOF   uint8 = 0x02
	FanoutDataFrameAck   uint8 = 0x03
)

// FanoutRoomParams carries sender-requested fan-out parameters when opening a
// fan-out control room. All values are clamped by the relay's operator policy.
type FanoutRoomParams struct {
	MaxReceivers   int   `json:"maxReceivers,omitempty"`
	BufferBytes    int64 `json:"bufferBytes,omitempty"`
	MinRateBps     int64 `json:"minRateBps,omitempty"`
	RateWindowMs   int64 `json:"rateWindowMs,omitempty"`
	StallTimeoutMs int64 `json:"stallTimeoutMs,omitempty"`
	StrictMinRate  bool  `json:"strictMinRate,omitempty"`
}

// FanoutRelayEvent describes an out-of-band relay notice sent to a receiver
// (e.g., slow-receiver kick or unexpected sender disconnect) or sender.
type FanoutRelayEvent struct {
	Type       string `json:"type"` // "kicked" or "sender-disconnected"
	PeerID     uint16 `json:"peerId,omitempty"`
	Reason     string `json:"reason"`
	AckedBytes int64  `json:"ackedBytes,omitempty"`
}

// EncodeFanoutMuxFrame packs a 4-byte LittleEndian mux header ([op][0][peerID LE])
// followed by payload.
func EncodeFanoutMuxFrame(op uint8, peerID uint16, payload []byte) []byte {
	frame := make([]byte, 4+len(payload))
	frame[0] = op
	frame[1] = 0
	binary.LittleEndian.PutUint16(frame[2:4], peerID)
	copy(frame[4:], payload)
	return frame
}

// DecodeFanoutMuxFrame unpacks a 4-byte LittleEndian mux header.
func DecodeFanoutMuxFrame(raw []byte) (op uint8, peerID uint16, payload []byte, err error) {
	if len(raw) < 4 {
		return 0, 0, nil, errors.New("fanout mux frame too short")
	}
	op = raw[0]
	peerID = binary.LittleEndian.Uint16(raw[2:4])
	payload = raw[4:]
	return op, peerID, payload, nil
}

// FormatFanoutSenderControlRoom formats the room string used by a fan-out sender
// on the relay's control port.
func FormatFanoutSenderControlRoom(baseRoom string, params FanoutRoomParams) (string, error) {
	raw, err := json.Marshal(params)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	return fmt.Sprintf("%sctrl:sender:%s:%s", FanoutRoomPrefix, baseRoom, encoded), nil
}

// FormatFanoutReceiverControlRoom formats the room string used by a fan-out
// receiver on the relay's control port.
func FormatFanoutReceiverControlRoom(baseRoom string) string {
	return fmt.Sprintf("%sctrl:recv:%s", FanoutRoomPrefix, baseRoom)
}

// FormatFanoutSenderDataRoom formats the room string used by a fan-out sender
// on a relay data port.
func FormatFanoutSenderDataRoom(baseRoom string) string {
	return fmt.Sprintf("%sdata:sender:%s", FanoutRoomPrefix, baseRoom)
}

// FormatFanoutReceiverDataRoom formats the room string used by a fan-out
// receiver on a relay data port.
func FormatFanoutReceiverDataRoom(baseRoom string, peerID uint16) string {
	return fmt.Sprintf("%sdata:recv:%s:%d", FanoutRoomPrefix, baseRoom, peerID)
}

type fanoutPeerConn struct {
	id       uint16
	ctrlConn *comm.Comm
	dataMu   sync.Mutex
	dataConn *comm.Comm
}

type fanoutRoom struct {
	baseRoom      string
	cfg           FanoutConfig
	reservedBytes int64
	senderCtrl    *comm.Comm
	ringBuffer    *FanoutRingBuffer
	ctx           context.Context
	cancel        context.CancelFunc

	mu          sync.Mutex
	nextPeerID  uint16
	lobbyLocked bool
	peers       map[uint16]*fanoutPeerConn
	closeOnce   sync.Once
}

// FanoutManager coordinates 1-to-N fan-out rooms across a relay process's
// control and data ports while enforcing a shared memory budget.
type FanoutManager struct {
	cfg    FanoutConfig
	budget *FanoutMemoryBudget
	mu     sync.RWMutex
	rooms  map[string]*fanoutRoom
}

// NewFanoutManager creates a FanoutManager shared across all ports of a relay.
func NewFanoutManager(cfg FanoutConfig) *FanoutManager {
	def := defaultFanoutConfig()
	if cfg.MaxBufferBytes <= 0 {
		cfg.MaxBufferBytes = def.MaxBufferBytes
	}
	if cfg.MaxMemoryBytes <= 0 {
		cfg.MaxMemoryBytes = def.MaxMemoryBytes
	}
	if cfg.MaxReceivers <= 0 {
		cfg.MaxReceivers = def.MaxReceivers
	}
	if cfg.MinRateBps < 0 {
		cfg.MinRateBps = 0
	}
	if cfg.RateWindow <= 0 {
		cfg.RateWindow = def.RateWindow
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = def.StallTimeout
	}
	return &FanoutManager{
		cfg:    cfg,
		budget: NewFanoutMemoryBudget(cfg.MaxMemoryBytes),
		rooms:  make(map[string]*fanoutRoom),
	}
}

// Config returns the manager's effective configuration.
func (m *FanoutManager) Config() FanoutConfig {
	if m == nil {
		return defaultFanoutConfig()
	}
	return m.cfg
}

// MemoryBudget returns the manager's global memory budget tracker.
func (m *FanoutManager) MemoryBudget() *FanoutMemoryBudget {
	if m == nil {
		return nil
	}
	return m.budget
}

// PeakRoomBufferBytes returns the peak buffer usage of an active room (for testing/telemetry).
func (m *FanoutManager) PeakRoomBufferBytes(baseRoom string) int64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	room := m.rooms[baseRoom]
	m.mu.RUnlock()
	if room == nil || room.ringBuffer == nil {
		return 0
	}
	return room.ringBuffer.PeakUsedBytes()
}

// IsFanoutRoom reports whether room uses the fanout:v1: prefix.
func IsFanoutRoom(room string) bool {
	return strings.HasPrefix(room, FanoutRoomPrefix)
}

func (m *FanoutManager) handleClient(serverCtx context.Context, c *comm.Comm, handshake handshakeResult) error {
	defer c.Close()
	sendAdmission := func(payload string) error {
		if handshake.fast {
			return c.Send([]byte(payload))
		}
		encrypted, err := crypt.Encrypt([]byte(payload), handshake.strongKeyForEncryption)
		if err != nil {
			return err
		}
		return c.Send(encrypted)
	}

	rest := strings.TrimPrefix(handshake.room, FanoutRoomPrefix)
	parts := strings.Split(rest, ":")
	if len(parts) < 3 {
		_ = sendAdmission("invalid fanout room format")
		return errors.New("invalid fanout room format")
	}
	plane, role, baseRoom := parts[0], parts[1], parts[2]
	if baseRoom == "" {
		_ = sendAdmission("empty fanout room name")
		return errors.New("empty fanout room name")
	}

	switch plane + ":" + role {
	case "ctrl:sender":
		var params FanoutRoomParams
		if len(parts) >= 4 && parts[3] != "" {
			rawJSON, err := base64.RawURLEncoding.DecodeString(parts[3])
			if err != nil {
				_ = sendAdmission("invalid fanout parameters")
				return err
			}
			if err := json.Unmarshal(rawJSON, &params); err != nil {
				_ = sendAdmission("invalid fanout parameters")
				return err
			}
		}
		return m.runSenderControl(serverCtx, c, baseRoom, params, sendAdmission)

	case "ctrl:recv":
		return m.runReceiverControl(serverCtx, c, baseRoom, sendAdmission)

	case "data:sender":
		return m.runSenderData(serverCtx, c, baseRoom, sendAdmission)

	case "data:recv":
		if len(parts) < 4 {
			_ = sendAdmission("missing fanout receiver id")
			return errors.New("missing fanout receiver id")
		}
		id64, err := strconv.ParseUint(parts[3], 10, 16)
		if err != nil || id64 == 0 {
			_ = sendAdmission("invalid fanout receiver id")
			return errors.New("invalid fanout receiver id")
		}
		return m.runReceiverData(serverCtx, c, baseRoom, uint16(id64), sendAdmission)

	default:
		_ = sendAdmission("unknown fanout role")
		return fmt.Errorf("unknown fanout role %s:%s", plane, role)
	}
}

func (m *FanoutManager) waitForRoom(ctx context.Context, baseRoom string, wait time.Duration) *fanoutRoom {
	deadline := time.Now().Add(wait)
	for {
		m.mu.RLock()
		room := m.rooms[baseRoom]
		m.mu.RUnlock()
		if room != nil {
			return room
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (m *FanoutManager) runSenderControl(serverCtx context.Context, c *comm.Comm, baseRoom string, params FanoutRoomParams, sendAdmission func(string) error) error {
	roomCfg := m.cfg
	if params.MaxReceivers > 0 && params.MaxReceivers <= m.cfg.MaxReceivers {
		roomCfg.MaxReceivers = params.MaxReceivers
	} else if params.MaxReceivers > m.cfg.MaxReceivers {
		_ = sendAdmission("fanout receiver limit exceeded")
		return errors.New("fanout receiver limit exceeded")
	}
	if params.BufferBytes > 0 {
		if params.BufferBytes > m.cfg.MaxBufferBytes {
			roomCfg.MaxBufferBytes = m.cfg.MaxBufferBytes
		} else {
			roomCfg.MaxBufferBytes = params.BufferBytes
		}
	}
	if params.MinRateBps > 0 {
		roomCfg.MinRateBps = params.MinRateBps
	} else if params.MinRateBps < 0 {
		roomCfg.MinRateBps = 0
	}
	if params.RateWindowMs > 0 {
		roomCfg.RateWindow = time.Duration(params.RateWindowMs) * time.Millisecond
	}
	if params.StallTimeoutMs > 0 {
		roomCfg.StallTimeout = time.Duration(params.StallTimeoutMs) * time.Millisecond
	}
	if params.StrictMinRate {
		roomCfg.StrictMinRate = true
	}

	if !m.budget.Reserve(roomCfg.MaxBufferBytes) {
		_ = sendAdmission(ErrFanoutMemoryLimit.Error())
		return ErrFanoutMemoryLimit
	}

	roomCtx, cancelRoom := context.WithCancel(serverCtx)
	room := &fanoutRoom{
		baseRoom:      baseRoom,
		cfg:           roomCfg,
		reservedBytes: roomCfg.MaxBufferBytes,
		senderCtrl:    c,
		ctx:           roomCtx,
		cancel:        cancelRoom,
		peers:         make(map[uint16]*fanoutPeerConn),
	}
	room.ringBuffer = NewFanoutRingBuffer(roomCfg, func(peerID uint16, reason string, ackedBytes int64) {
		room.kickPeer(peerID, reason, ackedBytes, true)
	})

	m.mu.Lock()
	if existing, exists := m.rooms[baseRoom]; exists {
		m.mu.Unlock()
		cancelRoom()
		m.budget.Release(roomCfg.MaxBufferBytes)
		_ = existing
		_ = sendAdmission("room full")
		return errors.New("fanout room already exists")
	}
	m.rooms[baseRoom] = room
	m.mu.Unlock()

	var senderFinishedNormally atomic.Bool
	defer func() {
		room.close(!senderFinishedNormally.Load())
		m.mu.Lock()
		if m.rooms[baseRoom] == room {
			delete(m.rooms, baseRoom)
		}
		m.mu.Unlock()
		m.budget.Release(room.reservedBytes)
	}()

	if err := sendAdmission("ok"); err != nil {
		return err
	}
	room.ringBuffer.StartWatchdog(roomCtx)

	for {
		raw, err := c.Receive()
		if err != nil {
			return nil
		}
		if bytes.Equal(raw, []byte{1}) {
			continue
		}
		op, peerID, payload, decErr := DecodeFanoutMuxFrame(raw)
		if decErr != nil {
			return decErr
		}
		switch op {
		case FanoutOpLockLobby:
			room.mu.Lock()
			room.lobbyLocked = true
			room.mu.Unlock()
		case FanoutOpKickPeer:
			reason := string(payload)
			if reason == "" {
				reason = "removed by sender"
			}
			room.kickPeer(peerID, reason, 0, false)
		case FanoutOpResetDataStream:
			room.ringBuffer.ResetForNextFile()
		case FanoutOpPeerFrame:
			room.mu.Lock()
			peer := room.peers[peerID]
			room.mu.Unlock()
			if peer != nil && peer.ctrlConn != nil {
				if sendErr := peer.ctrlConn.Send(payload); sendErr != nil {
					room.removePeer(peerID, sendErr.Error())
				}
			}
		case FanoutOpPeerLeft:
			// Sender indicates clean completion of the fan-out session.
			senderFinishedNormally.Store(true)
			return nil
		}
	}
}

func (m *FanoutManager) runReceiverControl(serverCtx context.Context, c *comm.Comm, baseRoom string, sendAdmission func(string) error) error {
	room := m.waitForRoom(serverCtx, baseRoom, 2*time.Second)
	if room == nil {
		_ = sendAdmission("fanout room not found")
		return errors.New("fanout room not found")
	}

	room.mu.Lock()
	if room.lobbyLocked {
		room.mu.Unlock()
		_ = sendAdmission("fanout session already started")
		return errors.New("fanout session already started")
	}
	if len(room.peers) >= room.cfg.MaxReceivers {
		room.mu.Unlock()
		_ = sendAdmission("fanout room full")
		return errors.New("fanout room full")
	}
	room.nextPeerID++
	peerID := room.nextPeerID
	peer := &fanoutPeerConn{
		id:       peerID,
		ctrlConn: c,
	}
	room.peers[peerID] = peer
	room.mu.Unlock()

	if err := sendAdmission(fmt.Sprintf("ok:%d", peerID)); err != nil {
		room.removePeer(peerID, err.Error())
		return err
	}

	// Notify sender that peerID joined.
	if err := room.senderCtrl.Send(EncodeFanoutMuxFrame(FanoutOpPeerJoined, peerID, nil)); err != nil {
		return err
	}

	defer func() {
		room.removePeer(peerID, "receiver disconnected")
	}()

	for {
		select {
		case <-room.ctx.Done():
			return nil
		default:
		}
		raw, err := c.Receive()
		if err != nil {
			return nil
		}
		if bytes.Equal(raw, []byte{1}) {
			continue
		}
		if sendErr := room.senderCtrl.Send(EncodeFanoutMuxFrame(FanoutOpPeerFrame, peerID, raw)); sendErr != nil {
			return sendErr
		}
	}
}

func (m *FanoutManager) runSenderData(serverCtx context.Context, c *comm.Comm, baseRoom string, sendAdmission func(string) error) error {
	room := m.waitForRoom(serverCtx, baseRoom, 2*time.Second)
	if room == nil {
		_ = sendAdmission("fanout room not found")
		return errors.New("fanout room not found")
	}
	if err := sendAdmission("ok"); err != nil {
		return err
	}

	for {
		raw, err := c.Receive()
		if err != nil {
			room.ringBuffer.Abort()
			return nil
		}
		if bytes.Equal(raw, []byte{1}) || len(raw) == 0 {
			continue
		}
		kind := raw[0]
		switch kind {
		case FanoutDataFrameChunk:
			if len(raw) < 9 {
				return errors.New("fanout data chunk frame too short")
			}
			// raw[1:9] is sender seq; raw[9:] is encrypted chunk payload.
			if _, pushErr := room.ringBuffer.Push(room.ctx, raw[9:]); pushErr != nil {
				return pushErr
			}
		case FanoutDataFrameEOF:
			room.ringBuffer.MarkStreamEOF()
		}
	}
}

func (m *FanoutManager) runReceiverData(serverCtx context.Context, c *comm.Comm, baseRoom string, peerID uint16, sendAdmission func(string) error) error {
	room := m.waitForRoom(serverCtx, baseRoom, 2*time.Second)
	if room == nil {
		_ = sendAdmission("fanout room not found")
		return errors.New("fanout room not found")
	}
	room.mu.Lock()
	peer, exists := room.peers[peerID]
	if !exists {
		if room.lobbyLocked || len(room.peers) >= room.cfg.MaxReceivers {
			room.mu.Unlock()
			_ = sendAdmission("fanout receiver not registered")
			return errors.New("fanout receiver not registered")
		}
		// Allow secondary dual-relay fan-out data registration before lobby lock.
		peer = &fanoutPeerConn{id: peerID}
		room.peers[peerID] = peer
	}
	room.mu.Unlock()

	peer.dataMu.Lock()
	peer.dataConn = c
	peer.dataMu.Unlock()
	room.ringBuffer.AddSubscriber(peerID)

	if err := sendAdmission("ok"); err != nil {
		room.ringBuffer.RemoveSubscriber(peerID)
		return err
	}

	streamCtx, cancelStream := context.WithCancel(room.ctx)
	defer func() {
		cancelStream()
		room.ringBuffer.RemoveSubscriber(peerID)
		room.mu.Lock()
		if p, ok := room.peers[peerID]; ok && p.ctrlConn == nil {
			delete(room.peers, peerID)
		}
		room.mu.Unlock()
	}()

	// Reader goroutine: consumes DataAck frames from the receiver.
	ackDone := make(chan struct{})
	go func() {
		defer close(ackDone)
		for {
			raw, err := c.Receive()
			if err != nil {
				cancelStream()
				return
			}
			if len(raw) >= 13 && raw[0] == FanoutDataFrameAck {
				ackedSeq := binary.LittleEndian.Uint64(raw[1:9])
				bytesDelta := int64(binary.LittleEndian.Uint32(raw[9:13]))
				room.ringBuffer.Ack(peerID, ackedSeq, bytesDelta)
			}
		}
	}()

	// Writer loop: streams chunks from FanoutRingBuffer to receiver's data socket.
	for {
		seq, payload, eof, err := room.ringBuffer.NextForSubscriber(streamCtx, peerID)
		if err != nil {
			return nil
		}
		if eof {
			var eofFrame [9]byte
			eofFrame[0] = FanoutDataFrameEOF
			binary.LittleEndian.PutUint64(eofFrame[1:9], seq)
			if sendErr := c.Send(eofFrame[:]); sendErr != nil {
				return nil
			}
			continue
		}

		frame := make([]byte, 9+len(payload))
		frame[0] = FanoutDataFrameChunk
		binary.LittleEndian.PutUint64(frame[1:9], seq)
		copy(frame[9:], payload)
		if sendErr := c.Send(frame); sendErr != nil {
			return nil
		}
	}
}

func (r *fanoutRoom) kickPeer(peerID uint16, reason string, ackedBytes int64, notifySender bool) {
	r.mu.Lock()
	peer, exists := r.peers[peerID]
	if exists {
		delete(r.peers, peerID)
	}
	r.mu.Unlock()
	r.ringBuffer.RemoveSubscriber(peerID)

	event := FanoutRelayEvent{
		Type:       "kicked",
		PeerID:     peerID,
		Reason:     reason,
		AckedBytes: ackedBytes,
	}
	eventJSON, _ := json.Marshal(event)

	if exists && peer != nil {
		if peer.ctrlConn != nil {
			notice := append([]byte(FanoutRelayEventPrefix), eventJSON...)
			_ = peer.ctrlConn.Send(notice)
			peer.ctrlConn.Close()
		}
		peer.dataMu.Lock()
		dConn := peer.dataConn
		peer.dataConn = nil
		peer.dataMu.Unlock()
		if dConn != nil {
			dConn.Close()
		}
	}

	if notifySender && r.senderCtrl != nil {
		log.Infof("fanout room %s: kicking receiver #%d (%s)", r.baseRoom, peerID, reason)
		_ = r.senderCtrl.Send(EncodeFanoutMuxFrame(FanoutOpPeerKicked, peerID, eventJSON))
	}
}

func (r *fanoutRoom) removePeer(peerID uint16, reason string) {
	r.mu.Lock()
	peer, exists := r.peers[peerID]
	if exists {
		delete(r.peers, peerID)
	}
	r.mu.Unlock()
	if !exists {
		return
	}
	r.ringBuffer.RemoveSubscriber(peerID)
	if peer.ctrlConn != nil {
		peer.ctrlConn.Close()
	}
	peer.dataMu.Lock()
	dConn := peer.dataConn
	peer.dataConn = nil
	peer.dataMu.Unlock()
	if dConn != nil {
		dConn.Close()
	}
	if r.senderCtrl != nil {
		_ = r.senderCtrl.Send(EncodeFanoutMuxFrame(FanoutOpPeerLeft, peerID, []byte(reason)))
	}
}

func (r *fanoutRoom) close(notifyAborted bool) {
	r.closeOnce.Do(func() {
		r.mu.Lock()
		peers := make([]*fanoutPeerConn, 0, len(r.peers))
		for _, p := range r.peers {
			peers = append(peers, p)
		}
		r.peers = make(map[uint16]*fanoutPeerConn)
		r.mu.Unlock()

		if notifyAborted && len(peers) > 0 {
			eventJSON, _ := json.Marshal(FanoutRelayEvent{
				Type:   "sender-disconnected",
				Reason: "sender disconnected before transfer completed",
			})
			notice := append([]byte(FanoutRelayEventPrefix), eventJSON...)
			for _, p := range peers {
				if p.ctrlConn != nil {
					_ = p.ctrlConn.Send(notice)
				}
			}
		}

		r.cancel()
		r.ringBuffer.Close()
		for _, p := range peers {
			if p.ctrlConn != nil {
				p.ctrlConn.Close()
			}
			p.dataMu.Lock()
			dConn := p.dataConn
			p.dataConn = nil
			p.dataMu.Unlock()
			if dConn != nil {
				dConn.Close()
			}
		}
	})
}
