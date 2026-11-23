package croc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/compress"
	"github.com/schollz/croc/v11/src/crypt"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/tcp"
)

const (
	dualRelayFeature        = "dual-relay-v1"
	dualRelayProbeTimeout   = 2 * time.Second
	dualRelayConnectTimeout = 5 * time.Second
	// WebSocket relays (typically behind a Cloudflare tunnel) need DNS, TCP,
	// TLS, the HTTP upgrade and the tunnel's own origin hop before the relay
	// handshake even starts. With 200-400 ms tunnel RTTs the TCP timeouts
	// above silently disabled the secondary relay.
	dualRelayWSProbeTimeout   = 6 * time.Second
	dualRelayWSConnectTimeout = 20 * time.Second
	chunkAckBatchThreshold    = 4
	chunkAckMinInterval       = 30 * time.Millisecond
	inFlightWatchdogThreshold = 1200 * time.Millisecond
	controlFailoverDomain     = "croc-control-failover-v1"
)

// MultiRelayOffer is sent by the sender over the encrypted control channel when
// a healthy secondary relay has been probed and prepared.
type MultiRelayOffer struct {
	Address     string   `json:"address"`
	Password    string   `json:"password,omitempty"`
	Room        string   `json:"room"`
	Ports       []string `json:"ports"`
	ChunkPolicy string   `json:"chunkPolicy"`
	StandbyCtrl bool     `json:"standbyCtrl"`
}

// MultiRelayAck is sent by the receiver in response to MultiRelayOffer.
type MultiRelayAck struct {
	Accepted     bool   `json:"accepted"`
	DataChannels int    `json:"dataChannels,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

// ControlResumePayload authenticates promotion of the standby control channel
// on Relay B after Primary Relay A fails mid-transfer.
type ControlResumePayload struct {
	Room        string `json:"room"`
	FailedRelay int    `json:"failedRelay"`
	MAC         []byte `json:"mac"`
}

type dualRelayDataConn struct {
	conn    *comm.Comm
	relayID int
	portIdx int
}

type controlFrameResult struct {
	data []byte
	err  error
}

type dualRelayClientState struct {
	mu               sync.Mutex
	active           atomic.Bool
	peerSupported    bool
	negotiated       bool
	pending          bool
	policy           string
	secondaryAddress string
	secondaryRoom    string
	secondaryPorts   []string
	secondaryCap     string
	secondaryControl *comm.Comm
	standbyFrames    chan controlFrameResult
	controlPromoted  atomic.Bool
	promoteOnce      sync.Once
	dataConns        []dualRelayDataConn
	relayDead        [2]atomic.Bool
	relayWorkerCount [2]atomic.Int32
	relayChunksSent  [2]atomic.Int64
	relayChunksRecv  [2]atomic.Int64
	scheduler        *reliableChunkScheduler
	receiveBitmap    *chunkBitmap
	unackedCount     atomic.Int32
	lastAckNano      atomic.Int64
	senderOpenCh     chan error
}

func (c *Client) dualRelayRequested() bool {
	return c.Options.DualRelay || strings.TrimSpace(c.Options.SecondaryRelay) != ""
}

func dualRelayProbeTimeoutFor(address string) time.Duration {
	if comm.IsWebSocketAddress(address) {
		return dualRelayWSProbeTimeout
	}
	return dualRelayProbeTimeout
}

func dualRelayConnectTimeoutFor(address string) time.Duration {
	if comm.IsWebSocketAddress(address) {
		return dualRelayWSConnectTimeout
	}
	return dualRelayConnectTimeout
}

// warnDualRelayFallback reports that a requested dual-relay transfer is
// continuing over one relay. Clients run at warn level by default, so the
// previous Info-level messages made this fallback invisible to users.
func (c *Client) warnDualRelayFallback(format string, args ...any) {
	c.dualRelay.mu.Lock()
	c.dualRelay.negotiated = true
	c.dualRelay.mu.Unlock()
	log.Warnf("dual-relay: "+format+"; continuing with a single relay", args...)
}

// DualRelayActive reports whether two relays are actively used for this session.
func (c *Client) DualRelayActive() bool {
	return c.dualRelay.active.Load()
}

// DualRelayChunksSent returns the number of data chunks transmitted over
// relayID (0 for primary Relay A, 1 for secondary Relay B).
func (c *Client) DualRelayChunksSent(relayID int) int64 {
	if relayID < 0 || relayID >= len(c.dualRelay.relayChunksSent) {
		return 0
	}
	return c.dualRelay.relayChunksSent[relayID].Load()
}

// DualRelayChunksReceived returns the number of unique data chunks received
// over relayID (0 for primary Relay A, 1 for secondary Relay B).
func (c *Client) DualRelayChunksReceived(relayID int) int64 {
	if relayID < 0 || relayID >= len(c.dualRelay.relayChunksRecv) {
		return 0
	}
	return c.dualRelay.relayChunksRecv[relayID].Load()
}

func (c *Client) closeDualRelayConnections() {
	c.dualRelay.mu.Lock()
	secondaryCtrl := c.dualRelay.secondaryControl
	c.dualRelay.secondaryControl = nil
	conns := c.dualRelay.dataConns
	c.dualRelay.dataConns = nil
	sched := c.dualRelay.scheduler
	c.dualRelay.scheduler = nil
	c.dualRelay.mu.Unlock()

	if secondaryCtrl != nil {
		secondaryCtrl.Close()
	}
	for _, dc := range conns {
		if dc.conn != nil && dc.relayID == 1 {
			dc.conn.Close()
		}
	}
	if sched != nil {
		sched.Close()
	}
}

func (c *Client) resetDualRelayState() {
	c.closeDualRelayConnections()
	c.dualRelay.mu.Lock()
	c.dualRelay.active.Store(false)
	c.dualRelay.peerSupported = false
	c.dualRelay.negotiated = false
	c.dualRelay.pending = false
	c.dualRelay.policy = ""
	c.dualRelay.secondaryAddress = ""
	c.dualRelay.secondaryRoom = ""
	c.dualRelay.secondaryPorts = nil
	c.dualRelay.secondaryCap = ""
	c.dualRelay.standbyFrames = nil
	c.dualRelay.controlPromoted.Store(false)
	c.dualRelay.promoteOnce = sync.Once{}
	for i := 0; i < 2; i++ {
		c.dualRelay.relayDead[i].Store(false)
		c.dualRelay.relayWorkerCount[i].Store(0)
		c.dualRelay.relayChunksSent[i].Store(0)
		c.dualRelay.relayChunksRecv[i].Store(0)
	}
	c.dualRelay.receiveBitmap = nil
	c.dualRelay.unackedCount.Store(0)
	c.dualRelay.lastAckNano.Store(0)
	c.dualRelay.senderOpenCh = nil
	c.dualRelay.mu.Unlock()
}

func (c *Client) registerDualRelayDataConn(relayID, portIdx int, conn *comm.Comm) {
	if conn == nil {
		return
	}
	c.dualRelay.mu.Lock()
	c.dualRelay.dataConns = append(c.dualRelay.dataConns, dualRelayDataConn{
		conn:    conn,
		relayID: relayID,
		portIdx: portIdx,
	})
	c.dualRelay.relayWorkerCount[relayID&1].Add(1)
	c.dualRelay.mu.Unlock()
}

func (c *Client) dualRelayDataConnsSnapshot() []dualRelayDataConn {
	c.dualRelay.mu.Lock()
	defer c.dualRelay.mu.Unlock()
	out := make([]dualRelayDataConn, len(c.dualRelay.dataConns))
	copy(out, c.dualRelay.dataConns)
	return out
}

func (c *Client) candidateSecondaryRelays() []string {
	primaryAddr, _ := c.currentRelayControlRoute()
	if primaryAddr == "" {
		primaryAddr = c.Options.RelayAddress
	}
	primaryNorm := normalizeRelayAddress(primaryAddr)

	var raw []string
	if strings.TrimSpace(c.Options.SecondaryRelay) != "" {
		raw = append(raw, c.Options.SecondaryRelay)
	}
	meta := c.currentRelayControlMeta()
	raw = append(raw, meta.Peers...)

	var candidates []string
	for _, entry := range tcp.NormalizePeerList(raw) {
		norm := normalizeRelayAddress(entry)
		if norm == "" || norm == primaryNorm {
			continue
		}
		if !slices.Contains(candidates, norm) {
			candidates = append(candidates, norm)
		}
	}
	return candidates
}

type peerProbeResult struct {
	address string
	rtt     time.Duration
	err     error
}

func (c *Client) selectBestSecondaryRelay(ctx context.Context) (string, time.Duration, error) {
	candidates := c.candidateSecondaryRelays()
	if len(candidates) == 0 {
		return "", 0, errors.New("no secondary relay candidates configured or advertised")
	}
	results := make(chan peerProbeResult, len(candidates))
	for _, addr := range candidates {
		go func(candidate string) {
			timeout := dualRelayProbeTimeoutFor(candidate)
			probeCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			rtt, err := tcp.MeasureServerLatencyContext(probeCtx, candidate, timeout)
			results <- peerProbeResult{address: candidate, rtt: rtt, err: err}
		}(addr)
	}

	var bestAddr string
	var bestRTT time.Duration
	var probeErrors []string
	for range candidates {
		res := <-results
		if res.err == nil {
			if bestAddr == "" || res.rtt < bestRTT {
				bestAddr = res.address
				bestRTT = res.rtt
			}
		} else {
			log.Debugf("dual-relay: probe to %s failed: %v", res.address, res.err)
			probeErrors = append(probeErrors, fmt.Sprintf("%s: %v", res.address, res.err))
		}
	}
	if bestAddr == "" {
		return "", 0, fmt.Errorf("no secondary relay responded to ping probe (%s)", strings.Join(probeErrors, "; "))
	}
	return bestAddr, bestRTT, nil
}

// initiateDualRelayOffer is called on the sender after primary Relay A data
// channels are opened. If a secondary relay is reachable, it sends
// TypeMultiRelayOffer and opens Relay B data channels asynchronously while
// waiting for TypeMultiRelayAck in the control loop.
func (c *Client) initiateDualRelayOffer(attempt *transferAttemptState) error {
	if !c.Options.IsSender || !c.dualRelayRequested() {
		return nil
	}
	if !c.dualRelay.peerSupported {
		c.warnDualRelayFallback("receiver does not support %s", dualRelayFeature)
		return nil
	}

	policy, err := ParseChunkPolicy(c.Options.ChunkPolicy)
	if err != nil {
		return err
	}

	secondaryAddr, rtt, probeErr := c.selectBestSecondaryRelay(c.stop.ctx)
	if probeErr != nil {
		c.warnDualRelayFallback("%v", probeErr)
		return nil
	}
	log.Infof("dual-relay: selected secondary relay %s (RTT %s)", secondaryAddr, rtt)

	secondaryRoom, err := generateReconnectRoom()
	if err != nil {
		return err
	}

	ctrlConn, bannerB, _, capB, _, ctrlErr := tcp.ConnectToTCPServerControlMetaContext(
		c.stop.ctx,
		secondaryAddr,
		c.Options.RelayPassword,
		secondaryRoom,
		dualRelayConnectTimeoutFor(secondaryAddr),
	)
	if ctrlErr != nil {
		c.warnDualRelayFallback("could not open control room on %s (%v)", secondaryAddr, ctrlErr)
		return nil
	}

	portsB := strings.Split(bannerB, ",")
	var cleanPorts []string
	for _, p := range portsB {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			cleanPorts = append(cleanPorts, trimmed)
		}
	}
	if len(cleanPorts) == 0 {
		ctrlConn.Close()
		c.warnDualRelayFallback("secondary relay %s advertised no data ports", secondaryAddr)
		return nil
	}
	if c.Options.NoMultiplexing {
		cleanPorts = cleanPorts[:1]
	} else if len(cleanPorts) > 4 {
		cleanPorts = cleanPorts[:4]
	}

	openCh := make(chan error, 1)
	c.dualRelay.mu.Lock()
	c.dualRelay.policy = policy
	c.dualRelay.secondaryAddress = secondaryAddr
	c.dualRelay.secondaryRoom = secondaryRoom
	c.dualRelay.secondaryPorts = cleanPorts
	c.dualRelay.secondaryCap = capB
	c.dualRelay.secondaryControl = ctrlConn
	c.dualRelay.pending = true
	c.dualRelay.senderOpenCh = openCh
	c.dualRelay.mu.Unlock()

	// Start connecting sender data channels to Relay B in the background while
	// the receiver processes TypeMultiRelayOffer.
	go func() {
		openCh <- c.openSecondaryRelayDataChannels(attempt, secondaryAddr, secondaryRoom, capB, cleanPorts, false)
	}()

	offerBytes, err := json.Marshal(MultiRelayOffer{
		Address:     secondaryAddr,
		Password:    c.Options.RelayPassword,
		Room:        secondaryRoom,
		Ports:       cleanPorts,
		ChunkPolicy: policy,
		StandbyCtrl: true,
	})
	if err != nil {
		return err
	}
	if err := c.sendControlMessage(message.Message{
		Type:  message.TypeMultiRelayOffer,
		Bytes: offerBytes,
	}); err != nil {
		return err
	}
	return nil
}

func (c *Client) openSecondaryRelayDataChannels(attempt *transferAttemptState, relayAddr, baseRoom, capability string, ports []string, startReceive bool) error {
	var wg sync.WaitGroup
	type openRes struct {
		portIdx int
		conn    *comm.Comm
		err     error
	}
	results := make(chan openRes, len(ports))
	wg.Add(len(ports))
	for idx, portStr := range ports {
		go func(j int, port string) {
			defer wg.Done()
			target, addrErr := comm.JoinRelayAddressPort(relayAddr, port)
			if addrErr != nil {
				results <- openRes{portIdx: j, err: addrErr}
				return
			}
			room := fmt.Sprintf("%s-%d", baseRoom, j)
			conn, _, _, _, connErr := tcp.ConnectToTCPServerWithCapabilityContext(
				c.stop.ctx,
				target,
				c.Options.RelayPassword,
				room,
				capability,
				dualRelayConnectTimeoutFor(relayAddr),
			)
			results <- openRes{portIdx: j, conn: conn, err: connErr}
		}(idx, portStr)
	}
	wg.Wait()
	close(results)

	var opened []openRes
	var firstErr error
	for res := range results {
		if res.err != nil {
			if firstErr == nil {
				firstErr = res.err
			}
			if res.conn != nil {
				res.conn.Close()
			}
			continue
		}
		opened = append(opened, res)
	}
	if firstErr != nil {
		for _, okRes := range opened {
			okRes.conn.Close()
		}
		return firstErr
	}

	attempt.dataMu.Lock()
	if attempt.dataClosed {
		attempt.dataMu.Unlock()
		for _, okRes := range opened {
			okRes.conn.Close()
		}
		return errors.New("transfer attempt closed")
	}
	for _, okRes := range opened {
		c.registerDualRelayDataConn(1, okRes.portIdx, okRes.conn)
	}
	attempt.dataMu.Unlock()

	if startReceive && !c.Options.IsSender {
		for _, okRes := range opened {
			c.startReceiveDataForRelay(1, okRes.portIdx, okRes.conn, attempt)
		}
	}
	return nil
}

func (c *Client) processMultiRelayOffer(m message.Message, attempt *transferAttemptState) error {
	if c.Options.IsSender {
		return errors.New("sender received unexpected multi-relay offer")
	}
	var offer MultiRelayOffer
	if err := json.Unmarshal(m.Bytes, &offer); err != nil {
		return fmt.Errorf("decode multi-relay offer: %w", err)
	}
	sendReject := func(reason string) error {
		log.Warnf("dual-relay: rejecting secondary relay %s (%s); continuing with a single relay", offer.Address, reason)
		ackBytes, _ := json.Marshal(MultiRelayAck{Accepted: false, Reason: reason})
		return c.sendControlMessage(message.Message{
			Type:  message.TypeMultiRelayAck,
			Bytes: ackBytes,
		})
	}

	if offer.Address == "" || offer.Room == "" || len(offer.Ports) == 0 {
		return sendReject("incomplete multi-relay offer")
	}
	pass := offer.Password
	if pass == "" {
		pass = c.Options.RelayPassword
	}

	var ctrlConn *comm.Comm
	var capB string
	if offer.StandbyCtrl {
		var ctrlErr error
		ctrlConn, _, _, capB, _, ctrlErr = tcp.ConnectToTCPServerControlMetaContext(
			c.stop.ctx,
			offer.Address,
			pass,
			offer.Room,
			dualRelayConnectTimeoutFor(offer.Address),
		)
		if ctrlErr != nil {
			return sendReject(ctrlErr.Error())
		}
	}

	if err := c.openSecondaryRelayDataChannels(attempt, offer.Address, offer.Room, capB, offer.Ports, true); err != nil {
		if ctrlConn != nil {
			ctrlConn.Close()
		}
		return sendReject(err.Error())
	}

	policy, err := ParseChunkPolicy(offer.ChunkPolicy)
	if err != nil {
		policy = ChunkPolicyDynamic
	}
	c.dualRelay.mu.Lock()
	c.dualRelay.policy = policy
	c.dualRelay.secondaryAddress = offer.Address
	c.dualRelay.secondaryRoom = offer.Room
	c.dualRelay.secondaryPorts = offer.Ports
	c.dualRelay.secondaryCap = capB
	c.dualRelay.secondaryControl = ctrlConn
	c.dualRelay.negotiated = true
	c.dualRelay.mu.Unlock()
	c.dualRelay.active.Store(true)

	if ctrlConn != nil {
		c.startStandbyControlWatcher(ctrlConn, attempt)
	}

	ackBytes, err := json.Marshal(MultiRelayAck{
		Accepted:     true,
		DataChannels: len(offer.Ports),
	})
	if err != nil {
		return err
	}
	log.Infof("dual-relay: receiver connected to secondary relay %s (%d data channels, policy=%s)", offer.Address, len(offer.Ports), policy)
	return c.sendControlMessage(message.Message{
		Type:  message.TypeMultiRelayAck,
		Bytes: ackBytes,
	})
}

func (c *Client) processMultiRelayAck(m message.Message, attempt *transferAttemptState) error {
	if !c.Options.IsSender {
		return errors.New("receiver received unexpected multi-relay ack")
	}
	var ack MultiRelayAck
	if err := json.Unmarshal(m.Bytes, &ack); err != nil {
		return fmt.Errorf("decode multi-relay ack: %w", err)
	}

	c.dualRelay.mu.Lock()
	openCh := c.dualRelay.senderOpenCh
	ctrlConn := c.dualRelay.secondaryControl
	secondaryAddr := c.dualRelay.secondaryAddress
	c.dualRelay.pending = false
	c.dualRelay.negotiated = true
	c.dualRelay.mu.Unlock()

	cleanupSecondary := func() {
		if ctrlConn != nil {
			ctrlConn.Close()
		}
		c.dualRelay.mu.Lock()
		c.dualRelay.secondaryControl = nil
		var kept []dualRelayDataConn
		for _, dc := range c.dualRelay.dataConns {
			if dc.relayID == 1 {
				if dc.conn != nil {
					dc.conn.Close()
				}
			} else {
				kept = append(kept, dc)
			}
		}
		c.dualRelay.dataConns = kept
		c.dualRelay.relayWorkerCount[1].Store(0)
		c.dualRelay.mu.Unlock()
	}

	if !ack.Accepted {
		log.Warnf("dual-relay: receiver declined secondary relay %s (%s); continuing with a single relay", secondaryAddr, ack.Reason)
		cleanupSecondary()
		return nil
	}

	var openErr error
	if openCh != nil {
		select {
		case openErr = <-openCh:
		case <-time.After(dualRelayConnectTimeoutFor(secondaryAddr)):
			openErr = errors.New("timed out opening sender secondary data channels")
		case <-c.stop.ctx.Done():
			openErr = c.stop.ctx.Err()
		}
	}
	if openErr != nil {
		log.Warnf("dual-relay: sender failed to open secondary data channels on %s (%v); continuing with a single relay", secondaryAddr, openErr)
		cleanupSecondary()
		return nil
	}

	c.dualRelay.active.Store(true)
	if ctrlConn != nil {
		c.startStandbyControlWatcher(ctrlConn, attempt)
	}
	log.Infof("dual-relay: active across primary relay and secondary relay %s (policy=%s)", secondaryAddr, c.dualRelay.policy)
	return nil
}

func (c *Client) startStandbyControlWatcher(ctrlConn *comm.Comm, attempt *transferAttemptState) {
	frames := make(chan controlFrameResult, 16)
	c.dualRelay.mu.Lock()
	c.dualRelay.standbyFrames = frames
	c.dualRelay.mu.Unlock()

	go func() {
		defer close(frames)
		for {
			data, err := ctrlConn.Receive()
			if err != nil {
				select {
				case frames <- controlFrameResult{err: err}:
				case <-c.stop.ctx.Done():
				}
				return
			}
			// Ignore 1-byte relay waiting-room keepalives.
			if bytes.Equal(data, []byte{1}) {
				continue
			}
			// Any real application frame arriving on the standby control room
			// means the peer has failed over to Relay B. Promote immediately.
			c.promoteSecondaryControl(attempt, false)
			select {
			case frames <- controlFrameResult{data: data}:
			case <-c.stop.ctx.Done():
				return
			}
		}
	}()
}

func (c *Client) controlFailoverMAC(room string, failedRelay int) []byte {
	mac := hmac.New(sha256.New, c.Key)
	_, _ = mac.Write([]byte(controlFailoverDomain))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(room))
	_, _ = mac.Write([]byte{byte(failedRelay)})
	return mac.Sum(nil)
}

// promoteSecondaryControl atomically switches the active control connection
// from Primary Relay A (relayID 0) to Standby Relay B (relayID 1) when Relay A
// fails mid-transfer.
func (c *Client) promoteSecondaryControl(attempt *transferAttemptState, announce bool) bool {
	if !c.dualRelay.active.Load() {
		return false
	}
	c.dualRelay.mu.Lock()
	secondaryCtrl := c.dualRelay.secondaryControl
	secondaryRoom := c.dualRelay.secondaryRoom
	standbyFrames := c.dualRelay.standbyFrames
	c.dualRelay.mu.Unlock()

	if secondaryCtrl == nil || standbyFrames == nil || c.dualRelay.relayDead[1].Load() {
		return false
	}

	promotedNow := false
	c.dualRelay.promoteOnce.Do(func() {
		promotedNow = true
		c.dualRelay.controlPromoted.Store(true)
		log.Infof("dual-relay: primary relay control failed; failing over control channel to secondary relay")

		oldCtrl := c.connection(0)
		c.setConnection(0, secondaryCtrl)
		if oldCtrl != nil && oldCtrl != secondaryCtrl {
			oldCtrl.Close()
		}
		c.markRelayDead(0, attempt)
	})

	if promotedNow && announce && len(c.Key) > 0 {
		payload, err := json.Marshal(ControlResumePayload{
			Room:        secondaryRoom,
			FailedRelay: 0,
			MAC:         c.controlFailoverMAC(secondaryRoom, 0),
		})
		if err == nil {
			_ = message.Send(secondaryCtrl, c.Key, message.Message{
				Type:  message.TypeControlResume,
				Bytes: payload,
			})
		}
		if !c.Options.IsSender {
			c.sendChunkAckNow(0)
		}
	}
	return c.dualRelay.controlPromoted.Load()
}

func (c *Client) processControlResume(m message.Message, attempt *transferAttemptState) error {
	var resume ControlResumePayload
	if err := json.Unmarshal(m.Bytes, &resume); err != nil {
		return fmt.Errorf("decode control-resume: %w", err)
	}
	expected := c.controlFailoverMAC(resume.Room, resume.FailedRelay)
	if !hmac.Equal(expected, resume.MAC) {
		return errors.New("control failover MAC verification failed")
	}
	c.promoteSecondaryControl(attempt, false)
	c.markRelayDead(resume.FailedRelay, attempt)
	if !c.Options.IsSender {
		c.sendChunkAckNow(resume.FailedRelay)
	}
	return nil
}

// receiveControlFrameWithFailover reads the next control frame from the primary
// control connection, seamlessly failing over to the standby control connection
// on Relay B if Primary Relay A drops mid-transfer.
func (c *Client) receiveControlFrameWithFailover(attempt *transferAttemptState) ([]byte, error) {
	if !c.dualRelay.controlPromoted.Load() {
		data, err := attempt.control.Receive()
		if err == nil {
			return data, nil
		}
		if !c.promoteSecondaryControl(attempt, true) {
			return nil, err
		}
	}

	c.dualRelay.mu.Lock()
	standbyFrames := c.dualRelay.standbyFrames
	c.dualRelay.mu.Unlock()
	if standbyFrames == nil {
		return nil, errors.New("standby control channel is unavailable")
	}
	select {
	case frame, ok := <-standbyFrames:
		if !ok {
			return nil, errors.New("standby control channel closed")
		}
		return frame.data, frame.err
	case <-c.stop.ctx.Done():
		return nil, c.stop.ctx.Err()
	}
}

// sendControlMessage sends an encrypted control message over c.connection(0),
// automatically promoting the standby control connection on Relay B and
// retrying once if Primary Relay A has failed.
func (c *Client) sendControlMessage(m message.Message) error {
	err := message.Send(c.connection(0), c.Key, m)
	if err == nil {
		return nil
	}
	if c.promoteSecondaryControl(nil, true) {
		return message.Send(c.connection(0), c.Key, m)
	}
	return err
}

func (c *Client) markRelayDead(relayID int, attempt *transferAttemptState) {
	if relayID < 0 || relayID > 1 {
		return
	}
	wasDead := c.dualRelay.relayDead[relayID].Swap(true)
	c.dualRelay.mu.Lock()
	sched := c.dualRelay.scheduler
	var toClose []*comm.Comm
	for _, dc := range c.dualRelay.dataConns {
		if dc.relayID == relayID && dc.conn != nil {
			toClose = append(toClose, dc.conn)
		}
	}
	c.dualRelay.mu.Unlock()

	for _, conn := range toClose {
		conn.Close()
	}
	if sched != nil {
		requeued := sched.RequeueRelay(relayID)
		if !wasDead {
			log.Infof("dual-relay: relay %d marked dead; requeued %d in-flight chunks to surviving relay", relayID, requeued)
		}
	}
}

func (c *Client) hasSurvivingRelay(failedRelayID int) bool {
	if !c.dualRelay.active.Load() {
		return false
	}
	other := 1 - (failedRelayID & 1)
	return !c.dualRelay.relayDead[other].Load()
}

func (c *Client) maybeSendChunkAck(finished bool) {
	if !c.dualRelay.active.Load() && !c.dualRelay.peerSupported {
		return
	}
	count := c.dualRelay.unackedCount.Add(1)
	now := time.Now().UnixNano()
	last := c.dualRelay.lastAckNano.Load()
	if !finished && count < chunkAckBatchThreshold && time.Duration(now-last) < chunkAckMinInterval {
		return
	}
	c.sendChunkAckNow(-1)
}

func (c *Client) sendChunkAckNow(failedRelay int) {
	c.dualRelay.mu.Lock()
	bm := c.dualRelay.receiveBitmap
	c.dualRelay.mu.Unlock()
	if bm == nil {
		return
	}
	c.dualRelay.unackedCount.Store(0)
	c.dualRelay.lastAckNano.Store(time.Now().UnixNano())

	c.receiveMutex.Lock()
	fileIdx := c.FilesToTransferCurrentNum
	c.receiveMutex.Unlock()

	ack := bm.BuildAck(fileIdx, failedRelay)
	ackBytes, err := json.Marshal(ack)
	if err != nil {
		return
	}
	_ = c.sendControlMessage(message.Message{
		Type:  message.TypeChunkAck,
		Bytes: ackBytes,
	})
}

func (c *Client) processChunkAck(m message.Message, attempt *transferAttemptState) error {
	if !c.Options.IsSender {
		return nil
	}
	var ack ChunkAckPayload
	if err := json.Unmarshal(m.Bytes, &ack); err != nil {
		return fmt.Errorf("decode chunk-ack: %w", err)
	}
	if ack.FailedRelay >= 0 && c.hasSurvivingRelay(ack.FailedRelay) {
		c.markRelayDead(ack.FailedRelay, attempt)
	}
	c.dualRelay.mu.Lock()
	sched := c.dualRelay.scheduler
	c.dualRelay.mu.Unlock()
	if sched != nil && ack.FileIndex == c.FilesToTransferCurrentNum {
		sched.ApplyAck(ack)
	}
	return nil
}

func (c *Client) startReceiveDataForRelay(relayID, index int, connection *comm.Comm, attempt *transferAttemptState) {
	attempt.dataMu.Lock()
	if attempt.dataClosed {
		attempt.dataMu.Unlock()
		connection.Close()
		return
	}
	attempt.dataWorkers.Add(1)
	attempt.dataMu.Unlock()
	go func() {
		defer attempt.dataWorkers.Done()
		c.receiveDataWithRelay(relayID, index, connection, attempt)
	}()
}

// startDualRelaySenderUnit starts reliable chunk scheduling across all active
// data connections on Relay A (relayID 0) and Relay B (relayID 1).
func (c *Client) startDualRelaySenderUnit(attempt *transferAttemptState, file *os.File, size int64, compressed bool, ranges []int64, progress func(int64, int)) {
	relayCount := 1
	if c.dualRelay.active.Load() {
		relayCount = 2
	}
	var sched *reliableChunkScheduler
	started := time.Now()
	sentBefore := [2]int64{c.dualRelay.relayChunksSent[0].Load(), c.dualRelay.relayChunksSent[1].Load()}
	sched = newReliableChunkScheduler(
		ranges,
		size,
		models.TCP_BUFFER_SIZE/2,
		c.dualRelay.policy,
		relayCount,
		func() {
			log.Debug("closing file (reliable scheduler)")
			if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				log.Errorf("error closing file: %v", err)
			}
			c.logDualRelayStats(sched, started, sentBefore)
		},
	)
	for r := 0; r < 2; r++ {
		if c.dualRelay.relayDead[r].Load() {
			sched.RequeueRelay(r)
		}
	}
	sched.StartWatchdog(attempt.ctx, inFlightWatchdogThreshold)

	c.dualRelay.mu.Lock()
	c.dualRelay.scheduler = sched
	conns := make([]dualRelayDataConn, len(c.dualRelay.dataConns))
	copy(conns, c.dualRelay.dataConns)
	c.dualRelay.mu.Unlock()

	queue := &requestedChunkQueue{
		compressed: compressed,
		progress:   progress,
	}
	c.senderDataMu.Lock()
	c.senderChunkQueue = queue
	c.senderDataAttempt = attempt
	c.senderDataFile = file
	c.senderDataWorkers = make(map[*comm.Comm]struct{})
	c.senderWorkerSequence = 0
	c.senderDataMu.Unlock()

	for _, dc := range conns {
		if dc.conn == nil || c.dualRelay.relayDead[dc.relayID&1].Load() {
			continue
		}
		c.startDualRelaySenderWorker(dc.relayID, dc.conn, file, queue, sched, attempt)
	}
}

// logDualRelayStats reports per-relay distribution and estimator state for one
// finished file (visible with LOGGER=info or --debug).
func (c *Client) logDualRelayStats(sched *reliableChunkScheduler, started time.Time, sentBefore [2]int64) {
	if sched == nil {
		return
	}
	st := sched.Stats()
	elapsed := time.Since(started)
	log.Infof(
		"dual-relay stats: policy=%s chunks=%d relayA=%d relayB=%d retransmissions=%d elapsed=%s window=%d/%d minRTT=%s/%s srtt=%s/%s rate=%.1f/%.1fMB/s",
		c.dualRelay.policy,
		st.Chunks,
		c.dualRelay.relayChunksSent[0].Load()-sentBefore[0],
		c.dualRelay.relayChunksSent[1].Load()-sentBefore[1],
		st.Retransmissions,
		elapsed.Round(time.Millisecond),
		st.Window[0], st.Window[1],
		st.MinRTT[0].Round(time.Millisecond), st.MinRTT[1].Round(time.Millisecond),
		st.SRTT[0].Round(time.Millisecond), st.SRTT[1].Round(time.Millisecond),
		st.RateBps[0]/1e6, st.RateBps[1]/1e6,
	)
}

func (c *Client) startDualRelaySenderWorker(relayID int, dataConn *comm.Comm, file *os.File, queue *requestedChunkQueue, sched *reliableChunkScheduler, expectedAttempt *transferAttemptState) {
	if dataConn == nil || sched == nil {
		return
	}
	c.senderDataMu.Lock()
	if c.senderDataAttempt != expectedAttempt {
		c.senderDataMu.Unlock()
		return
	}
	if _, exists := c.senderDataWorkers[dataConn]; exists {
		c.senderDataMu.Unlock()
		return
	}
	c.senderDataWorkers[dataConn] = struct{}{}
	workerID := c.senderWorkerSequence
	c.senderWorkerSequence++
	queue.workers.Add(1)
	c.senderDataMu.Unlock()

	go func() {
		defer queue.workers.Done()
		c.sendDataReliable(workerID, relayID, dataConn, file, queue, sched, expectedAttempt)
	}()
}

func (c *Client) isDualRelayNegotiationPending() bool {
	c.dualRelay.mu.Lock()
	defer c.dualRelay.mu.Unlock()
	return c.dualRelay.pending
}

func (c *Client) markCurrentFileAllAcked() {
	c.dualRelay.mu.Lock()
	sched := c.dualRelay.scheduler
	c.dualRelay.mu.Unlock()
	if sched != nil {
		sched.MarkAllAcked()
	}
}

func (c *Client) setCurrentReceiveBitmap(bm *chunkBitmap) {
	c.dualRelay.mu.Lock()
	c.dualRelay.receiveBitmap = bm
	c.dualRelay.mu.Unlock()
	c.dualRelay.unackedCount.Store(0)
	c.dualRelay.lastAckNano.Store(0)
}

// markReceivedChunkInBitmap records chunkIdx in the current file's receive
// bitmap. It returns false if the chunk was already received earlier (duplicate
// retransmission during failover), preventing double counting.
func (c *Client) markReceivedChunkInBitmap(chunkIdx int) bool {
	c.dualRelay.mu.Lock()
	bm := c.dualRelay.receiveBitmap
	c.dualRelay.mu.Unlock()
	if bm == nil {
		return true
	}
	return bm.Mark(chunkIdx)
}

func (c *Client) onReceiverDataConnError(relayID int, err error, attempt *transferAttemptState) (handledByFailover bool) {
	slot := relayID & 1
	remaining := c.dualRelay.relayWorkerCount[slot].Add(-1)
	if !c.dualRelay.active.Load() {
		return false
	}
	// Only report the relay as failed once its last data connection is gone.
	// Reporting it on the first closed connection made the sender abandon a
	// relay whose remaining connections were still healthy.
	failedRelay := -1
	if remaining <= 0 {
		c.dualRelay.relayDead[slot].Store(true)
		failedRelay = slot
	}
	c.sendChunkAckNow(failedRelay)
	other := 1 - slot
	otherAlive := !c.dualRelay.relayDead[other].Load() && c.dualRelay.relayWorkerCount[other].Load() > 0
	if otherAlive || remaining > 0 {
		log.Infof("dual-relay: receiver data channel on relay %d closed (%v); continuing on surviving relay", slot, err)
		return true
	}
	return false
}

func (c *Client) sendDataReliable(workerID, relayID int, dataConn *comm.Comm, fread *os.File, queue *requestedChunkQueue, sched *reliableChunkScheduler, attempt *transferAttemptState) {
	defer func() {
		if r := recover(); r != nil {
			attempt.report(fmt.Errorf("send data reliable panic: %v", r))
		}
		log.Debugf("finished reliable worker %d (relay %d)", workerID, relayID)
	}()

	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	payload := make([]byte, 8+chunkSize)
	var encryptedBuffer []byte
	var compressedBuffer []byte

	for {
		if err := c.ctxErr(); err != nil {
			return
		}
		if attempt.ctx != nil && attempt.ctx.Err() != nil {
			return
		}
		readingPos, reqIdx, ok := sched.Claim(attempt.ctx, relayID, workerID)
		if !ok {
			return
		}

		n, errRead := fread.ReadAt(payload[8:], readingPos)
		if n == 0 {
			if attempt.ctx != nil && attempt.ctx.Err() != nil {
				return
			}
			if errRead == nil {
				errRead = errors.New("unexpected zero-byte read")
			}
			attempt.report(errRead)
			return
		}
		if c.limiter != nil {
			r := c.limiter.ReserveN(time.Now(), n)
			timer := time.NewTimer(r.Delay())
			select {
			case <-timer.C:
			case <-c.stop.ctx.Done():
				r.Cancel()
				timer.Stop()
				return
			case <-attempt.contextDone():
				r.Cancel()
				timer.Stop()
				return
			}
		}

		binary.LittleEndian.PutUint64(payload[:8], uint64(readingPos))
		plain := payload[:8+n]
		var dataToSend []byte
		var err error
		if queue.compressed {
			compressedBuffer = compress.CompressTo(compressedBuffer, plain)
			dataToSend, err = crypt.EncryptAEADTo(encryptedBuffer, compressedBuffer, c.dataAEAD)
		} else {
			dataToSend, err = crypt.EncryptAEADTo(encryptedBuffer, plain, c.dataAEAD)
		}
		if err != nil {
			attempt.report(err)
			return
		}
		encryptedBuffer = dataToSend

		if err = dataConn.Send(dataToSend); err != nil {
			sched.RequeueChunk(reqIdx)
			// Chunks already written to this connection may never arrive.
			requeued := sched.RequeueConn(relayID, workerID)
			slot := relayID & 1
			remaining := c.dualRelay.relayWorkerCount[slot].Add(-1)
			if remaining > 0 && c.dualRelay.active.Load() {
				// Losing one tunnel WebSocket must not disable the whole
				// relay while its other data connections still work.
				log.Warnf("dual-relay: data connection on relay %d failed (%v); requeued %d chunks, %d connection(s) remain on that relay", slot, err, requeued+1, remaining)
				dataConn.Close()
				return
			}
			if c.hasSurvivingRelay(slot) {
				c.markRelayDead(slot, attempt)
				return
			}
			if c.ctxErr() == nil {
				attempt.report(transferDisconnectError{err: err})
			}
			return
		}

		c.dualRelay.relayChunksSent[relayID&1].Add(1)
		if sched.MarkSentFirstTime(reqIdx) {
			if queue.progress != nil {
				queue.progress(readingPos, n)
			} else {
				c.addProgress(int64(n))
			}
			c.mutex.Lock()
			c.TotalSent += int64(n)
			c.mutex.Unlock()
		}
	}
}
