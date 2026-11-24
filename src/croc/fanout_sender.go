package croc

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/compress"
	"github.com/schollz/croc/v11/src/crypt"
	"github.com/schollz/croc/v11/src/lobby"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/pakekey"
	"github.com/schollz/croc/v11/src/tcp"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/schollz/croc/v11/src/utils"
	"github.com/schollz/pake/v3"
)

const (
	defaultFanoutWaitTimeout = 60 * time.Second
	defaultFanoutFileBarrier = 30 * time.Second
)

// GroupKeyPayload distributes the common 256-bit content encryption key to an
// authenticated receiver over its pairwise PAKE-encrypted control channel,
// along with optional secondary relay parameters when Dual-Relay Fan-out is enabled.
type GroupKeyPayload struct {
	ContentKey        []byte `json:"contentKey"`
	PeerID            uint16 `json:"peerId"`
	SecondaryRelay    string `json:"secondaryRelay,omitempty"`
	SecondaryPassword string `json:"secondaryPassword,omitempty"`
	SecondaryDataAddr string `json:"secondaryDataAddr,omitempty"`
	ChunkPolicy       string `json:"chunkPolicy,omitempty"`
}

// FanoutPeerSummary records the final outcome for one receiver in a 1-to-N
// fan-out transfer session.
type FanoutPeerSummary struct {
	PeerID     uint16 `json:"peerId"`
	Status     string `json:"status"` // "completed", "kicked", "disconnected", "refused", "error"
	BytesAcked int64  `json:"bytesAcked"`
	Reason     string `json:"reason,omitempty"`
}

type fanoutSenderPeer struct {
	id             uint16
	status         string
	reason         string
	bytesAcked     int64
	pake           *pake.Pake
	pakeCurve      string
	pakeInitiator  []byte
	pakeResponder  []byte
	pakeKeys       pakekey.Keys
	pairKey        []byte
	readyInLobby   bool
	dualRelayReady bool
	ackedBitmap    *chunkBitmap
	readyForFile   int
	finishedFile   int
	completedAll   bool
}

type fanoutMuxEvent struct {
	relayID int
	op      uint8
	peerID  uint16
	payload []byte
	err     error
}

func (c *Client) sendFanout(filesInfo []FileInfo, emptyFoldersToTransfer []FileInfo, totalNumberFolders int) (err error) {
	defer func() { err = c.redactError(err) }()
	go c.stop.done()
	defer c.stop.Cancel()

	c.EmptyFoldersToTransfer = emptyFoldersToTransfer
	c.TotalNumberFolders = totalNumberFolders
	c.TotalNumberOfContents = len(filesInfo)
	c.FilesToTransfer = filesInfo

	if prepareErr := c.sendCollectFiles(filesInfo); prepareErr != nil {
		c.finishFilePreparation(prepareErr)
		return prepareErr
	}
	c.finishFilePreparation(nil)

	targetReceivers := max(c.Options.FanoutReceivers, 1)
	minReceivers := max(c.Options.FanoutMinReceivers, 1)
	if minReceivers > targetReceivers {
		minReceivers = targetReceivers
	}
	waitTimeout := c.Options.FanoutWaitTimeout
	if waitTimeout <= 0 {
		waitTimeout = defaultFanoutWaitTimeout
	}

	params := tcp.FanoutRoomParams{
		MaxReceivers:  targetReceivers,
		BufferBytes:   c.Options.FanoutBufferBytes,
		MinRateBps:    c.Options.FanoutMinRateBps,
		StrictMinRate: c.Options.FanoutStrictRate,
	}
	if c.Options.FanoutRateWindow > 0 {
		params.RateWindowMs = c.Options.FanoutRateWindow.Milliseconds()
	}
	if c.Options.FanoutStallTimeout > 0 {
		params.StallTimeoutMs = c.Options.FanoutStallTimeout.Milliseconds()
	}

	ctrlRoom, err := tcp.FormatFanoutSenderControlRoom(c.Options.RoomName, params)
	if err != nil {
		return err
	}
	relayAddr := normalizeRelayAddress(c.Options.RelayAddress)
	senderCtrl, banner, _, primaryCap, primaryMeta, err := tcp.ConnectToTCPServerControlMetaContext(
		c.stop.ctx,
		relayAddr,
		c.Options.RelayPassword,
		ctrlRoom,
		10*time.Second,
	)
	if err != nil {
		return fmt.Errorf("%w: could not open fanout control room on %s: %v", ErrRelayConnection, relayAddr, err)
	}
	defer senderCtrl.Close()
	c.setRelayControlRoute(relayAddr, primaryCap, false)
	c.setRelayControlMeta(primaryMeta)

	dataPorts := strings.Split(banner, ",")
	if len(dataPorts) == 0 || strings.TrimSpace(dataPorts[0]) == "" {
		return fmt.Errorf("%w: relay %s advertised no data ports for fanout", ErrRelayConnection, relayAddr)
	}
	dataAddr, err := comm.JoinRelayAddressPort(relayAddr, strings.TrimSpace(dataPorts[0]))
	if err != nil {
		return err
	}
	senderData, _, _, _, _, err := tcp.ConnectToTCPServerControlMetaContext(
		c.stop.ctx,
		dataAddr,
		c.Options.RelayPassword,
		tcp.FormatFanoutSenderDataRoom(c.Options.RoomName),
		10*time.Second,
	)
	if err != nil {
		return fmt.Errorf("%w: could not open fanout data room on %s: %v", ErrRelayConnection, dataAddr, err)
	}
	defer senderData.Close()

	// Optional Dual-Relay Fan-out setup (Phase 4: Part 1 + Part 2 combined).
	var secondaryCtrl *comm.Comm
	var secondaryData *comm.Comm
	var secondaryRelayAddr string
	var secondaryDataAddr string
	var secondaryDead atomic.Bool
	if c.dualRelayRequested() {
		candAddr, _, probeErr := c.selectBestSecondaryRelay(c.stop.ctx)
		if probeErr == nil && candAddr != "" {
			secCtrl, secBanner, _, _, _, secErr := tcp.ConnectToTCPServerControlMetaContext(
				c.stop.ctx,
				candAddr,
				c.Options.RelayPassword,
				ctrlRoom,
				dualRelayConnectTimeout,
			)
			if secErr == nil {
				secPorts := strings.Split(secBanner, ",")
				if len(secPorts) > 0 && strings.TrimSpace(secPorts[0]) != "" {
					secDAddr, joinErr := comm.JoinRelayAddressPort(candAddr, strings.TrimSpace(secPorts[0]))
					if joinErr == nil {
						secData, _, _, _, _, dErr := tcp.ConnectToTCPServerControlMetaContext(
							c.stop.ctx,
							secDAddr,
							c.Options.RelayPassword,
							tcp.FormatFanoutSenderDataRoom(c.Options.RoomName),
							dualRelayConnectTimeout,
						)
						if dErr == nil {
							secondaryCtrl = secCtrl
							secondaryData = secData
							secondaryRelayAddr = candAddr
							secondaryDataAddr = secDAddr
							log.Infof("fanout: secondary relay %s ready for dual-relay fan-out", candAddr)
						} else {
							secCtrl.Close()
						}
					} else {
						secCtrl.Close()
					}
				} else {
					secCtrl.Close()
				}
			}
		}
	}
	defer func() {
		if secondaryData != nil {
			secondaryData.Close()
		}
		if secondaryCtrl != nil {
			secondaryCtrl.Close()
		}
	}()

	contentKey := make([]byte, 32)
	if _, err := rand.Read(contentKey); err != nil {
		return err
	}
	contentAEAD, err := crypt.NewAESGCM(contentKey)
	if err != nil {
		return err
	}

	receiverCommand := fmt.Sprintf("croc --relay %s --fanout --out . %s", relayAddr, c.Options.SharedSecret)
	if runtime.GOOS == "windows" {
		receiverCommand = fmt.Sprintf(".\\croc.exe --relay %s --fanout --out . %s", relayAddr, c.Options.SharedSecret)
	}

	var lobbyURL string
	var lobbyServer *lobby.Server
	if c.Options.ChatLobby {
		bindAddress := strings.TrimSpace(c.Options.ChatBind)
		if bindAddress == "" {
			bindAddress = "0.0.0.0:0"
		}
		var lobbyErr error
		lobbyServer, lobbyURL, lobbyErr = lobby.Start(bindAddress, c.Options.SharedSecret, receiverCommand)
		if lobbyErr != nil {
			return lobbyErr
		}
		defer lobbyServer.Close()
	}

	if !c.Options.Quiet {
		output, _ := termui.Output(os.Stderr)
		fmt.Fprintf(output, "Fan-out session ready on %s (waiting up to %s for %d receivers, min %d)\nCode is: %s\nOn receivers run:\n  %s\n",
			relayAddr, waitTimeout, targetReceivers, minReceivers, c.Options.SharedSecret, receiverCommand)
		if lobbyURL != "" {
			fmt.Fprintf(output, "Chat and receive lobby: %s\n", lobbyURL)
		}
	}

	muxEvents := make(chan fanoutMuxEvent, 128)
	readMuxLoop := func(rID int, ctrl *comm.Comm) {
		for {
			raw, recvErr := ctrl.Receive()
			if recvErr != nil {
				select {
				case muxEvents <- fanoutMuxEvent{relayID: rID, err: recvErr}:
				case <-c.stop.ctx.Done():
				}
				return
			}
			if bytes.Equal(raw, []byte{1}) {
				continue
			}
			op, peerID, payload, decErr := tcp.DecodeFanoutMuxFrame(raw)
			if decErr != nil {
				select {
				case muxEvents <- fanoutMuxEvent{relayID: rID, err: decErr}:
				case <-c.stop.ctx.Done():
				}
				return
			}
			select {
			case muxEvents <- fanoutMuxEvent{relayID: rID, op: op, peerID: peerID, payload: payload}:
			case <-c.stop.ctx.Done():
				return
			}
		}
	}
	go readMuxLoop(0, senderCtrl)
	if secondaryCtrl != nil {
		go readMuxLoop(1, secondaryCtrl)
	}

	peers := make(map[uint16]*fanoutSenderPeer)
	var peerOrder []uint16
	chunkAckNotifyCh := make(chan struct{}, 16)

	sendToPeer := func(peer *fanoutSenderPeer, key []byte, m message.Message) error {
		encoded, encErr := message.Encode(key, m)
		if encErr != nil {
			return encErr
		}
		return senderCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpPeerFrame, peer.id, encoded))
	}

	kickPeer := func(peer *fanoutSenderPeer, status, reason string) {
		peer.status = status
		peer.reason = reason
		_ = senderCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpKickPeer, peer.id, []byte(reason)))
		if secondaryCtrl != nil && !secondaryDead.Load() {
			_ = secondaryCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpKickPeer, peer.id, []byte(reason)))
		}
	}

	countActivePeers := func() int {
		count := 0
		for _, p := range peers {
			if p.status == "active" || p.status == "completed" {
				count++
			}
		}
		return count
	}

	handleLobbyOrControlEvent := func(ev fanoutMuxEvent) error {
		if ev.err != nil {
			if ev.relayID == 1 {
				// Secondary relay control failed; mark secondary dead and trigger failover.
				secondaryDead.Store(true)
				select {
				case chunkAckNotifyCh <- struct{}{}:
				default:
				}
				return nil
			}
			return ev.err
		}
		switch ev.op {
		case tcp.FanoutOpPeerJoined:
			if ev.relayID != 0 {
				return nil
			}
			if _, exists := peers[ev.peerID]; !exists {
				peers[ev.peerID] = &fanoutSenderPeer{
					id:           ev.peerID,
					status:       "handshaking",
					readyForFile: -1,
					finishedFile: -1,
				}
				peerOrder = append(peerOrder, ev.peerID)
				log.Infof("fanout: receiver #%d joined lobby", ev.peerID)
			}
		case tcp.FanoutOpPeerLeft:
			if ev.relayID != 0 {
				return nil
			}
			if p, exists := peers[ev.peerID]; exists {
				if !p.completedAll && (p.status == "active" || p.status == "handshaking") {
					p.status = "disconnected"
					p.reason = string(ev.payload)
					if p.reason == "" {
						p.reason = "receiver disconnected"
					}
					if secondaryCtrl != nil && !secondaryDead.Load() {
						_ = secondaryCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpKickPeer, ev.peerID, []byte(p.reason)))
					}
				}
			}
		case tcp.FanoutOpPeerKicked:
			if p, exists := peers[ev.peerID]; exists {
				var notice tcp.FanoutRelayEvent
				_ = json.Unmarshal(ev.payload, &notice)
				p.status = "kicked"
				p.reason = notice.Reason
				if notice.AckedBytes > p.bytesAcked {
					p.bytesAcked = notice.AckedBytes
				}
				// Synchronize slow-receiver kick to the other relay immediately.
				if ev.relayID == 0 && secondaryCtrl != nil && !secondaryDead.Load() {
					_ = secondaryCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpKickPeer, ev.peerID, []byte(notice.Reason)))
				} else if ev.relayID == 1 && senderCtrl != nil {
					_ = senderCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpKickPeer, ev.peerID, []byte(notice.Reason)))
				}
			}
		case tcp.FanoutOpPeerFrame:
			p, exists := peers[ev.peerID]
			if !exists || (p.status != "handshaking" && p.status != "active") {
				return nil
			}
			m, decErr := message.Decode(p.pairKey, ev.payload)
			if decErr != nil {
				kickPeer(p, "error", "failed to decode control frame: "+decErr.Error())
				return nil
			}
			switch m.Type {
			case message.TypePAKE:
				if p.pairKey != nil {
					kickPeer(p, "error", "duplicate PAKE message")
					return nil
				}
				if m.Version != pakekey.ProtocolVersion {
					kickPeer(p, "error", fmt.Sprintf("unsupported PAKE version %d", m.Version))
					return nil
				}
				curve := string(m.Bytes2)
				pakeInst, initErr := pakekey.Init(
					[]byte(c.pakePassphrase),
					1,
					curve,
					pakekey.PurposeTransfer,
					c.Options.RoomName,
				)
				if initErr != nil {
					kickPeer(p, "error", initErr.Error())
					return nil
				}
				if updErr := pakeInst.Update(m.Bytes); updErr != nil {
					kickPeer(p, "error", updErr.Error())
					return nil
				}
				sharedKey, keyErr := pakeInst.SessionKey()
				if keyErr != nil {
					kickPeer(p, "error", keyErr.Error())
					return nil
				}
				salt := make([]byte, pakekey.SaltSize)
				if _, rErr := rand.Read(salt); rErr != nil {
					return rErr
				}
				responderBytes := append([]byte(nil), pakeInst.Bytes()...)
				derived, deriveErr := pakekey.Derive(sharedKey, pakekey.Context{
					Purpose:   pakekey.PurposeTransfer,
					Room:      c.Options.RoomName,
					Curve:     curve,
					Initiator: m.Bytes,
					Responder: responderBytes,
					Salt:      salt,
				})
				if deriveErr != nil {
					kickPeer(p, "error", deriveErr.Error())
					return nil
				}
				p.pake = pakeInst
				p.pakeCurve = curve
				p.pakeInitiator = append([]byte(nil), m.Bytes...)
				p.pakeResponder = responderBytes
				p.pakeKeys = derived
				if sendErr := sendToPeer(p, nil, message.Message{
					Type:    message.TypePAKE,
					Version: pakekey.ProtocolVersion,
					Bytes:   responderBytes,
					Bytes2:  salt,
				}); sendErr != nil {
					kickPeer(p, "disconnected", sendErr.Error())
				}

			case message.TypePAKEConfirm:
				if p.pairKey != nil || len(p.pakeKeys.EncryptionKey) == 0 {
					kickPeer(p, "error", "unexpected PAKE confirm")
					return nil
				}
				if !pakekey.Confirm(p.pakeKeys.ConfirmationA, m.Bytes) {
					kickPeer(p, "error", "PAKE key confirmation failed (wrong code phrase)")
					return nil
				}
				if sendErr := sendToPeer(p, nil, message.Message{
					Type:    message.TypePAKEConfirm,
					Version: pakekey.ProtocolVersion,
					Bytes:   p.pakeKeys.ConfirmationB,
				}); sendErr != nil {
					kickPeer(p, "disconnected", sendErr.Error())
					return nil
				}
				p.pairKey = append([]byte(nil), p.pakeKeys.EncryptionKey...)
				p.pakeKeys = pakekey.Keys{}

				gkBytes, _ := json.Marshal(GroupKeyPayload{
					ContentKey:        contentKey,
					PeerID:            p.id,
					SecondaryRelay:    secondaryRelayAddr,
					SecondaryPassword: c.Options.RelayPassword,
					SecondaryDataAddr: secondaryDataAddr,
					ChunkPolicy:       c.Options.ChunkPolicy,
				})
				if sendErr := sendToPeer(p, p.pairKey, message.Message{
					Type:  message.TypeGroupKey,
					Bytes: gkBytes,
				}); sendErr != nil {
					kickPeer(p, "disconnected", sendErr.Error())
					return nil
				}

				infoBytes, _ := json.Marshal(SenderInfo{
					FilesToTransfer:        c.FilesToTransfer,
					EmptyFoldersToTransfer: c.EmptyFoldersToTransfer,
					TotalNumberFolders:     c.TotalNumberFolders,
					SendingText:            c.Options.SendingText,
					NoCompress:             c.Options.NoCompress,
					HashAlgorithm:          c.Options.HashAlgorithm,
				})
				if sendErr := sendToPeer(p, p.pairKey, message.Message{
					Type:  message.TypeFileInfo,
					Bytes: infoBytes,
				}); sendErr != nil {
					kickPeer(p, "disconnected", sendErr.Error())
					return nil
				}

			case message.TypeRecipientReady:
				var req RemoteFileRequest
				if jsonErr := json.Unmarshal(m.Bytes, &req); jsonErr != nil {
					kickPeer(p, "error", "invalid recipient ready payload")
					return nil
				}
				p.readyForFile = req.FilesToTransferCurrentNum
				if !p.readyInLobby {
					p.readyInLobby = true
					p.dualRelayReady = supportsFeature(req.Features, dualRelayFeature)
					p.status = "active"
					log.Infof("fanout: receiver #%d authenticated and ready (%d/%d, dualRelay=%v)", p.id, countActivePeers(), targetReceivers, p.dualRelayReady)
				}

			case message.TypeChunkAck:
				var ack ChunkAckPayload
				if jsonErr := json.Unmarshal(m.Bytes, &ack); jsonErr == nil {
					if ack.FailedRelay == 1 {
						secondaryDead.Store(true)
					}
					if p.ackedBitmap != nil && ack.FileIndex == c.FilesToTransferCurrentNum {
						p.ackedBitmap.ApplyAck(ack)
						select {
						case chunkAckNotifyCh <- struct{}{}:
						default:
						}
					}
				}

			case message.TypeCloseSender:
				p.finishedFile = m.Num
				if m.Num >= 0 && m.Num < len(c.FilesToTransfer) {
					p.bytesAcked += c.FilesToTransfer[m.Num].Size
				}

			case message.TypeFinished:
				p.completedAll = true
				p.status = "completed"
				p.reason = ""
				_ = sendToPeer(p, p.pairKey, message.Message{
					Type: message.TypeFinished,
				})

			case message.TypeError:
				status := "error"
				if strings.Contains(strings.ToLower(m.Message), "refus") {
					status = "refused"
				}
				kickPeer(p, status, m.Message)
			}
		}
		return nil
	}

	// 1. Lobby Phase: wait until targetReceivers are ready or waitTimeout expires.
	lobbyTimer := time.NewTimer(waitTimeout)
	defer lobbyTimer.Stop()
lobbyLoop:
	for countActivePeers() < targetReceivers {
		select {
		case <-c.stop.ctx.Done():
			return c.stop.ctx.Err()
		case <-lobbyTimer.C:
			break lobbyLoop
		case ev, ok := <-muxEvents:
			if !ok {
				return errors.New("fanout control connection closed during lobby")
			}
			if err := handleLobbyOrControlEvent(ev); err != nil {
				return err
			}
		}
	}

	// Remove any half-handshaked peers that did not become ready in time.
	for _, id := range peerOrder {
		p := peers[id]
		if p.status == "handshaking" {
			kickPeer(p, "disconnected", "handshake timed out before fanout start")
		}
	}

	if countActivePeers() < minReceivers {
		for _, id := range peerOrder {
			p := peers[id]
			if p.status == "active" && p.pairKey != nil {
				_ = sendToPeer(p, p.pairKey, message.Message{
					Type:    message.TypeError,
					Message: fmt.Sprintf("not enough receivers ready (%d < %d minimum)", countActivePeers(), minReceivers),
				})
			}
		}
		c.recordFanoutSummary(peers, peerOrder)
		return fmt.Errorf("fanout aborted: only %d of %d minimum receivers ready before timeout", countActivePeers(), minReceivers)
	}

	// Lock the lobby so late joiners are rejected with "fanout session already started".
	if err := senderCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpLockLobby, 0, nil)); err != nil {
		return err
	}
	if secondaryCtrl != nil && !secondaryDead.Load() {
		_ = secondaryCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpLockLobby, 0, nil))
	}

	dualActive := secondaryData != nil && !secondaryDead.Load()
	if dualActive {
		for _, p := range peers {
			if p.status == "active" && !p.dualRelayReady {
				dualActive = false
				break
			}
		}
	}
	if dualActive {
		c.dualRelay.active.Store(true)
	} else if secondaryCtrl != nil {
		secondaryDead.Store(true)
		secondaryData.Close()
		secondaryCtrl.Close()
	}

	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)

	// 2. Data Streaming Phase: stream each file sequentially.
	for fileIdx, fileInfo := range c.FilesToTransfer {
		c.FilesToTransferCurrentNum = fileIdx
		if fileInfo.Size == 0 || fileInfo.Symlink != "" {
			continue
		}

		totalChunks := utils.ChunkRangesCount(nil, fileInfo.Size, chunkSize)
		for _, p := range peers {
			if p.status == "active" {
				p.ackedBitmap = newChunkBitmap(totalChunks, fileInfo.Size, chunkSize)
			}
		}

		if fileIdx > 0 {
			// Wait for all remaining active peers to request fileIdx.
			barrierDeadline := time.NewTimer(defaultFanoutFileBarrier)
			for {
				allReady := true
				for _, p := range peers {
					if p.status == "active" && p.readyForFile < fileIdx {
						allReady = false
						break
					}
				}
				if allReady || countActivePeers() == 0 {
					break
				}
				select {
				case <-c.stop.ctx.Done():
					barrierDeadline.Stop()
					return c.stop.ctx.Err()
				case <-barrierDeadline.C:
					for _, p := range peers {
						if p.status == "active" && p.readyForFile < fileIdx {
							kickPeer(p, "disconnected", "timed out preparing next file")
						}
					}
				case ev, ok := <-muxEvents:
					if !ok {
						barrierDeadline.Stop()
						return errors.New("fanout control closed")
					}
					if err := handleLobbyOrControlEvent(ev); err != nil {
						barrierDeadline.Stop()
						return err
					}
				}
			}
			barrierDeadline.Stop()
			if countActivePeers() == 0 {
				c.recordFanoutSummary(peers, peerOrder)
				return tcp.ErrNoActiveReceivers
			}
			if err := senderCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpResetDataStream, 0, nil)); err != nil {
				return err
			}
			if dualActive && secondaryCtrl != nil && !secondaryDead.Load() {
				_ = secondaryCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpResetDataStream, 0, nil))
			}
			// Notify active peers that fileIdx stream has been reset and is starting.
			for _, p := range peers {
				if p.status == "active" {
					_ = sendToPeer(p, p.pairKey, message.Message{
						Type: message.TypeFanoutStatus,
						Num:  fileIdx,
					})
				}
			}
		}

		pathToFile := path.Join(fileInfo.FolderSource, fileInfo.Name)
		streamErrCh := make(chan error, 1)
		stopStream := make(chan struct{})
		sentOnRelay1 := make([]atomic.Bool, totalChunks)
		requeuedToRelay0 := make([]bool, totalChunks)

		go func(fPath string, fInfo FileInfo, useDual bool) {
			if useDual && !secondaryDead.Load() {
				streamErrCh <- c.streamDualFanoutFileData(senderData, secondaryData, &secondaryDead, sentOnRelay1, contentAEAD, fPath, fInfo, stopStream)
			} else {
				streamErrCh <- c.streamFanoutFileData(senderData, contentAEAD, fPath, fInfo, stopStream)
			}
		}(pathToFile, fileInfo, dualActive)

		resendMissingRelay1Chunks := func() {
			if !dualActive {
				return
			}
			var toResend []int
			for idx := 0; idx < totalChunks; idx++ {
				if !sentOnRelay1[idx].Load() || requeuedToRelay0[idx] {
					continue
				}
				missingOnAny := false
				for _, p := range peers {
					if p.status == "active" && p.finishedFile < fileIdx {
						if p.ackedBitmap == nil || !p.ackedBitmap.Has(idx) {
							missingOnAny = true
							break
						}
					}
				}
				if missingOnAny {
					requeuedToRelay0[idx] = true
					toResend = append(toResend, idx)
				}
			}
			if len(toResend) > 0 {
				log.Infof("fanout failover: re-pushing %d chunks from failed Relay B over Relay A", len(toResend))
				_ = c.pushSpecificFanoutChunks(senderData, contentAEAD, pathToFile, fileInfo, toResend, true)
			}
		}

		streamDone := false
		var streamErr error
		for {
			allFinishedFile := true
			for _, p := range peers {
				if p.status == "active" && p.finishedFile < fileIdx {
					allFinishedFile = false
					break
				}
			}
			if countActivePeers() == 0 {
				close(stopStream)
				c.recordFanoutSummary(peers, peerOrder)
				return tcp.ErrNoActiveReceivers
			}
			if streamDone && allFinishedFile {
				break
			}

			select {
			case <-c.stop.ctx.Done():
				close(stopStream)
				return c.stop.ctx.Err()
			case sErr := <-streamErrCh:
				streamDone = true
				streamErr = sErr
				streamErrCh = nil
				if secondaryDead.Load() {
					resendMissingRelay1Chunks()
				}
			case <-chunkAckNotifyCh:
				if streamDone && secondaryDead.Load() {
					resendMissingRelay1Chunks()
				}
			case ev, ok := <-muxEvents:
				if !ok {
					close(stopStream)
					c.recordFanoutSummary(peers, peerOrder)
					return errors.New("fanout control channel closed mid-transfer")
				}
				if err := handleLobbyOrControlEvent(ev); err != nil {
					close(stopStream)
					return err
				}
				if streamDone && secondaryDead.Load() {
					resendMissingRelay1Chunks()
				}
			}
		}
		if streamErr != nil && countActivePeers() == 0 {
			c.recordFanoutSummary(peers, peerOrder)
			return streamErr
		}
	}

	// 3. Final handshake: wait for remaining active peers to send TypeFinished.
	finishTimer := time.NewTimer(15 * time.Second)
	defer finishTimer.Stop()
	for {
		allCompleted := true
		for _, p := range peers {
			if p.status == "active" && !p.completedAll {
				allCompleted = false
				break
			}
		}
		if allCompleted || countActivePeers() == 0 {
			break
		}
		select {
		case <-c.stop.ctx.Done():
			return c.stop.ctx.Err()
		case <-finishTimer.C:
			for _, p := range peers {
				if p.status == "active" && !p.completedAll {
					p.status = "disconnected"
					p.reason = "timed out waiting for final confirmation"
				}
			}
		case ev, ok := <-muxEvents:
			if !ok {
				break
			}
			_ = handleLobbyOrControlEvent(ev)
		}
	}

	for _, p := range peers {
		if p.completedAll {
			p.status = "completed"
			p.reason = ""
		}
	}
	_ = senderCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpPeerLeft, 0, nil))
	if secondaryCtrl != nil && !secondaryDead.Load() {
		_ = secondaryCtrl.Send(tcp.EncodeFanoutMuxFrame(tcp.FanoutOpPeerLeft, 0, nil))
	}
	c.recordFanoutSummary(peers, peerOrder)

	completedCount := 0
	for _, s := range c.FanoutResults {
		if s.Status == "completed" {
			completedCount++
		}
	}
	if completedCount == 0 {
		return tcp.ErrNoActiveReceivers
	}
	c.updateLifecycle(func(state *transferLifecycle) { state.Successful = true })
	return nil
}

func (c *Client) streamDualFanoutFileData(
	primaryData *comm.Comm,
	secondaryData *comm.Comm,
	secondaryDead *atomic.Bool,
	sentOnRelay1 []atomic.Bool,
	aead cipher.AEAD,
	filePath string,
	fileInfo FileInfo,
	stop <-chan struct{},
) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	totalChunks := utils.ChunkRangesCount(nil, fileInfo.Size, chunkSize)
	sched := newReliableChunkScheduler(nil, fileInfo.Size, chunkSize, c.Options.ChunkPolicy, 2, nil)
	defer sched.Close()

	var sendMu [2]sync.Mutex
	conns := [2]*comm.Comm{primaryData, secondaryData}

	worker := func(relayID int) error {
		plainBuf := make([]byte, 8+chunkSize)
		var compressedBuf []byte
		var encryptedBuf []byte
		var seq uint64

		for {
			select {
			case <-stop:
				return errors.New("fanout stream stopped")
			case <-c.stop.ctx.Done():
				return c.stop.ctx.Err()
			default:
			}
			if relayID == 1 && secondaryDead.Load() {
				return nil
			}
			_, chunkIdx, ok := sched.Claim(c.stop.ctx, relayID, relayID)
			if !ok {
				return nil
			}
			offset := int64(chunkIdx) * chunkSize
			toRead := min(chunkSize, fileInfo.Size-offset)
			n, readErr := f.ReadAt(plainBuf[8:8+toRead], offset)
			if n == 0 {
				if readErr != nil && !errors.Is(readErr, io.EOF) {
					return readErr
				}
				sched.AckSingle(chunkIdx)
				continue
			}

			if c.limiter != nil {
				r := c.limiter.ReserveN(time.Now(), n)
				timer := time.NewTimer(r.Delay())
				select {
				case <-timer.C:
				case <-stop:
					r.Cancel()
					timer.Stop()
					return errors.New("fanout stream stopped")
				case <-c.stop.ctx.Done():
					r.Cancel()
					timer.Stop()
					return c.stop.ctx.Err()
				}
			}

			binary.LittleEndian.PutUint64(plainBuf[:8], uint64(offset))
			plain := plainBuf[:8+n]
			var ciphertext []byte
			var encErr error
			if !c.Options.NoCompress && fileInfo.IsCompressed {
				compressedBuf = compress.CompressTo(compressedBuf, plain)
				ciphertext, encErr = crypt.EncryptAEADTo(encryptedBuf, compressedBuf, aead)
			} else {
				ciphertext, encErr = crypt.EncryptAEADTo(encryptedBuf, plain, aead)
			}
			if encErr != nil {
				return encErr
			}
			encryptedBuf = ciphertext

			frame := make([]byte, 9+len(ciphertext))
			frame[0] = tcp.FanoutDataFrameChunk
			binary.LittleEndian.PutUint64(frame[1:9], seq)
			copy(frame[9:], ciphertext)

			sendMu[relayID].Lock()
			sendErr := conns[relayID].Send(frame)
			sendMu[relayID].Unlock()
			if sendErr != nil {
				if relayID == 1 {
					secondaryDead.Store(true)
					sched.RequeueChunk(chunkIdx)
					sched.RequeueRelay(1)
					return nil
				}
				return sendErr
			}

			if relayID == 1 && chunkIdx >= 0 && chunkIdx < totalChunks {
				sentOnRelay1[chunkIdx].Store(true)
			}
			sched.AckSingle(chunkIdx)
			c.dualRelay.relayChunksSent[relayID].Add(1)
			if sched.MarkSentFirstTime(chunkIdx) {
				c.addProgress(int64(n))
				c.mutex.Lock()
				c.TotalSent += int64(n)
				c.mutex.Unlock()
			}
			seq++
		}
	}

	var wg sync.WaitGroup
	var err0 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		err0 = worker(0)
	}()
	go func() {
		defer wg.Done()
		_ = worker(1)
	}()
	wg.Wait()
	if err0 != nil {
		return err0
	}

	var eofFrame [9]byte
	eofFrame[0] = tcp.FanoutDataFrameEOF
	if !secondaryDead.Load() && secondaryData != nil {
		sendMu[1].Lock()
		if err := secondaryData.Send(eofFrame[:]); err != nil {
			secondaryDead.Store(true)
		}
		sendMu[1].Unlock()
	}
	sendMu[0].Lock()
	err = primaryData.Send(eofFrame[:])
	sendMu[0].Unlock()
	return err
}

func (c *Client) pushSpecificFanoutChunks(
	primaryData *comm.Comm,
	aead cipher.AEAD,
	filePath string,
	fileInfo FileInfo,
	chunkIndices []int,
	sendEOF bool,
) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	plainBuf := make([]byte, 8+chunkSize)
	var compressedBuf []byte
	var encryptedBuf []byte

	for i, chunkIdx := range chunkIndices {
		offset := int64(chunkIdx) * chunkSize
		toRead := min(chunkSize, fileInfo.Size-offset)
		n, readErr := f.ReadAt(plainBuf[8:8+toRead], offset)
		if n == 0 {
			if readErr != nil && !errors.Is(readErr, io.EOF) {
				return readErr
			}
			continue
		}
		binary.LittleEndian.PutUint64(plainBuf[:8], uint64(offset))
		plain := plainBuf[:8+n]
		var ciphertext []byte
		var encErr error
		if !c.Options.NoCompress && fileInfo.IsCompressed {
			compressedBuf = compress.CompressTo(compressedBuf, plain)
			ciphertext, encErr = crypt.EncryptAEADTo(encryptedBuf, compressedBuf, aead)
		} else {
			ciphertext, encErr = crypt.EncryptAEADTo(encryptedBuf, plain, aead)
		}
		if encErr != nil {
			return encErr
		}
		encryptedBuf = ciphertext

		frame := make([]byte, 9+len(ciphertext))
		frame[0] = tcp.FanoutDataFrameChunk
		binary.LittleEndian.PutUint64(frame[1:9], uint64(i))
		copy(frame[9:], ciphertext)

		if sendErr := primaryData.Send(frame); sendErr != nil {
			return sendErr
		}
		c.dualRelay.relayChunksSent[0].Add(1)
	}
	if sendEOF {
		var eofFrame [9]byte
		eofFrame[0] = tcp.FanoutDataFrameEOF
		return primaryData.Send(eofFrame[:])
	}
	return nil
}

func (c *Client) streamFanoutFileData(senderData *comm.Comm, aead cipher.AEAD, filePath string, fileInfo FileInfo, stop <-chan struct{}) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	plainBuf := make([]byte, 8+chunkSize)
	var compressedBuf []byte
	var encryptedBuf []byte

	var seq uint64
	var offset int64
	for offset < fileInfo.Size {
		select {
		case <-stop:
			return errors.New("fanout stream stopped")
		case <-c.stop.ctx.Done():
			return c.stop.ctx.Err()
		default:
		}

		toRead := min(chunkSize, fileInfo.Size-offset)
		n, readErr := io.ReadFull(f, plainBuf[8:8+toRead])
		if n == 0 {
			if readErr != nil {
				return readErr
			}
			break
		}
		if c.limiter != nil {
			r := c.limiter.ReserveN(time.Now(), n)
			timer := time.NewTimer(r.Delay())
			select {
			case <-timer.C:
			case <-stop:
				r.Cancel()
				timer.Stop()
				return errors.New("fanout stream stopped")
			case <-c.stop.ctx.Done():
				r.Cancel()
				timer.Stop()
				return c.stop.ctx.Err()
			}
		}

		binary.LittleEndian.PutUint64(plainBuf[:8], uint64(offset))
		plain := plainBuf[:8+n]
		var ciphertext []byte
		var encErr error
		if !c.Options.NoCompress && fileInfo.IsCompressed {
			compressedBuf = compress.CompressTo(compressedBuf, plain)
			ciphertext, encErr = crypt.EncryptAEADTo(encryptedBuf, compressedBuf, aead)
		} else {
			ciphertext, encErr = crypt.EncryptAEADTo(encryptedBuf, plain, aead)
		}
		if encErr != nil {
			return encErr
		}
		encryptedBuf = ciphertext

		frame := make([]byte, 9+len(ciphertext))
		frame[0] = tcp.FanoutDataFrameChunk
		binary.LittleEndian.PutUint64(frame[1:9], seq)
		copy(frame[9:], ciphertext)

		if sendErr := senderData.Send(frame); sendErr != nil {
			return sendErr
		}
		c.addProgress(int64(n))
		c.mutex.Lock()
		c.TotalSent += int64(n)
		c.mutex.Unlock()

		offset += int64(n)
		seq++
	}

	var eofFrame [9]byte
	eofFrame[0] = tcp.FanoutDataFrameEOF
	binary.LittleEndian.PutUint64(eofFrame[1:9], seq)
	return senderData.Send(eofFrame[:])
}

func (c *Client) recordFanoutSummary(peers map[uint16]*fanoutSenderPeer, order []uint16) {
	slices.Sort(order)
	results := make([]FanoutPeerSummary, 0, len(order))
	for _, id := range order {
		p := peers[id]
		if p == nil {
			continue
		}
		results = append(results, FanoutPeerSummary{
			PeerID:     p.id,
			Status:     p.status,
			BytesAcked: p.bytesAcked,
			Reason:     p.reason,
		})
	}
	c.mutex.Lock()
	c.FanoutResults = results
	c.mutex.Unlock()

	if !c.Options.Quiet && len(results) > 0 {
		output, _ := termui.Output(os.Stderr)
		fmt.Fprintln(output, "\nFan-out Receiver Summary:")
		for _, r := range results {
			if r.Reason != "" {
				fmt.Fprintf(output, "  Receiver #%d: %s (%s)\n", r.PeerID, r.Status, r.Reason)
			} else {
				fmt.Fprintf(output, "  Receiver #%d: %s\n", r.PeerID, r.Status)
			}
		}
	}
}
