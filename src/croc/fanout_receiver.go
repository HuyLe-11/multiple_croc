package croc

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/schollz/croc/v11/src/comm"
	"github.com/schollz/croc/v11/src/compress"
	"github.com/schollz/croc/v11/src/crypt"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/message"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/pakekey"
	"github.com/schollz/croc/v11/src/tcp"
	"github.com/schollz/croc/v11/src/termui"
	"github.com/schollz/croc/v11/src/utils"
)

// ErrFanoutKicked is returned when the relay kicks a receiver whose transfer
// rate fell below the configured minimum threshold or stalled.
var ErrFanoutKicked = errors.New("kicked from fanout session by relay")

func parseFanoutRelayNotice(raw []byte) (tcp.FanoutRelayEvent, bool) {
	if !bytes.HasPrefix(raw, []byte(tcp.FanoutRelayEventPrefix)) {
		return tcp.FanoutRelayEvent{}, false
	}
	var ev tcp.FanoutRelayEvent
	if err := json.Unmarshal(raw[len(tcp.FanoutRelayEventPrefix):], &ev); err != nil {
		return tcp.FanoutRelayEvent{}, false
	}
	return ev, true
}

func (c *Client) receiveFanout() (err error) {
	defer func() { err = c.redactError(err) }()
	go c.stop.done()
	defer c.stop.Cancel()

	root, err := c.receiveFilesystem()
	if err != nil {
		return err
	}
	defer c.closeReceiveFilesystem()

	relayAddr := normalizeRelayAddress(c.Options.RelayAddress)
	ctrlRoom := tcp.FormatFanoutReceiverControlRoom(c.Options.RoomName)
	recvCtrl, banner, _, _, meta, err := tcp.ConnectToTCPServerControlMetaContext(
		c.stop.ctx,
		relayAddr,
		c.Options.RelayPassword,
		ctrlRoom,
		10*time.Second,
	)
	if err != nil {
		return fmt.Errorf("could not join fanout session on %s: %w", relayAddr, err)
	}
	defer recvCtrl.Close()
	c.setConnection(0, recvCtrl)

	var peerID uint16
	if strings.HasPrefix(meta.WSPublicURL, "ok:") {
		parsedID, parseErr := strconv.ParseUint(strings.TrimPrefix(meta.WSPublicURL, "ok:"), 10, 16)
		if parseErr == nil {
			peerID = uint16(parsedID)
		}
	}
	if peerID == 0 {
		return errors.New("relay did not assign a valid fanout receiver ID")
	}

	dataPorts := strings.Split(banner, ",")
	if len(dataPorts) == 0 || strings.TrimSpace(dataPorts[0]) == "" {
		return errors.New("relay advertised no data ports for fanout")
	}
	dataAddr, err := comm.JoinRelayAddressPort(relayAddr, strings.TrimSpace(dataPorts[0]))
	if err != nil {
		return err
	}

	readControlMsg := func(key []byte) (message.Message, error) {
		for {
			raw, recvErr := recvCtrl.Receive()
			if recvErr != nil {
				return message.Message{}, recvErr
			}
			if bytes.Equal(raw, []byte{1}) {
				continue
			}
			if ev, ok := parseFanoutRelayNotice(raw); ok {
				if ev.Type == "kicked" {
					c.FanoutKicked = true
					c.FanoutKickReason = ev.Reason
					return message.Message{}, fmt.Errorf("%w: %s", ErrFanoutKicked, ev.Reason)
				}
				return message.Message{}, fmt.Errorf("fanout session ended by relay: %s", ev.Reason)
			}
			return message.Decode(key, raw)
		}
	}

	// 1. Pairwise PAKE v2 with the sender.
	c.pakeInitiator = append([]byte(nil), c.Pake.Bytes()...)
	c.pakeCurve = c.Options.Curve
	if err := message.Send(recvCtrl, nil, message.Message{
		Type:    message.TypePAKE,
		Version: pakekey.ProtocolVersion,
		Bytes:   c.pakeInitiator,
		Bytes2:  []byte(c.Options.Curve),
	}); err != nil {
		return err
	}

	pakeResp, err := readControlMsg(nil)
	if err != nil {
		return err
	}
	if pakeResp.Type == message.TypeError {
		return fmt.Errorf("sender error: %s", pakeResp.Message)
	}
	if pakeResp.Type != message.TypePAKE || len(pakeResp.Bytes2) != pakekey.SaltSize {
		return errors.New("invalid PAKE response from fanout sender")
	}
	c.pakeResponder = append([]byte(nil), pakeResp.Bytes...)
	if err := c.Pake.Update(pakeResp.Bytes); err != nil {
		return fmt.Errorf("pake not successful: %w", err)
	}
	if err := c.derivePakeKeys(pakeResp.Bytes2); err != nil {
		return fmt.Errorf("pake not successful: %w", err)
	}
	if err := message.Send(recvCtrl, nil, message.Message{
		Type:    message.TypePAKEConfirm,
		Version: pakekey.ProtocolVersion,
		Bytes:   c.pakeKeys.ConfirmationA,
	}); err != nil {
		return err
	}

	confirmResp, err := readControlMsg(nil)
	if err != nil {
		return err
	}
	if confirmResp.Type != message.TypePAKEConfirm || !pakekey.Confirm(c.pakeKeys.ConfirmationB, confirmResp.Bytes) {
		return errors.New("pake not successful: sender PAKE confirmation failed")
	}
	c.Key = append([]byte(nil), c.pakeKeys.EncryptionKey...)
	c.pakeKeys = pakekey.Keys{}

	// 2. Receive GroupKeyPayload (common contentKey).
	groupMsg, err := readControlMsg(c.Key)
	if err != nil {
		return err
	}
	if groupMsg.Type == message.TypeError {
		return fmt.Errorf("peer error: %s", groupMsg.Message)
	}
	if groupMsg.Type != message.TypeGroupKey {
		return fmt.Errorf("expected group-key message, got %s", groupMsg.Type)
	}
	var gk GroupKeyPayload
	if err := json.Unmarshal(groupMsg.Bytes, &gk); err != nil || len(gk.ContentKey) != 32 {
		return errors.New("invalid fanout group content key")
	}
	c.dataAEAD, err = crypt.NewAESGCM(gk.ContentKey)
	if err != nil {
		return err
	}

	// 3. Receive SenderInfo manifest.
	infoMsg, err := readControlMsg(c.Key)
	if err != nil {
		return err
	}
	if infoMsg.Type == message.TypeError {
		return fmt.Errorf("peer error: %s", infoMsg.Message)
	}
	if infoMsg.Type != message.TypeFileInfo {
		return fmt.Errorf("expected fileinfo message, got %s", infoMsg.Type)
	}
	var senderInfo SenderInfo
	if err := json.Unmarshal(infoMsg.Bytes, &senderInfo); err != nil {
		return err
	}
	if done, infoErr := c.processSenderInfo(senderInfo); infoErr != nil || done {
		return infoErr
	}

	// 4. Open Receiver Data channel BEFORE sending RecipientReady so this
	// receiver is registered in FanoutRingBuffer before streaming starts.
	recvData, _, _, _, _, err := tcp.ConnectToTCPServerControlMetaContext(
		c.stop.ctx,
		dataAddr,
		c.Options.RelayPassword,
		tcp.FormatFanoutReceiverDataRoom(c.Options.RoomName, peerID),
		10*time.Second,
	)
	if err != nil {
		return fmt.Errorf("could not open fanout data channel: %w", err)
	}
	defer recvData.Close()

	// Optional secondary data channel when sender offers Dual-Relay Fan-out (Phase 4).
	var recvData2 *comm.Comm
	if strings.TrimSpace(gk.SecondaryDataAddr) != "" {
		secPass := gk.SecondaryPassword
		if secPass == "" {
			secPass = c.Options.RelayPassword
		}
		secConn, _, _, _, _, secErr := tcp.ConnectToTCPServerControlMetaContext(
			c.stop.ctx,
			gk.SecondaryDataAddr,
			secPass,
			tcp.FormatFanoutReceiverDataRoom(c.Options.RoomName, peerID),
			dualRelayConnectTimeout,
		)
		if secErr == nil {
			recvData2 = secConn
			c.dualRelay.active.Store(true)
			defer recvData2.Close()
		} else {
			log.Debugf("fanout: secondary relay data channel %s unavailable (%v), falling back to primary", gk.SecondaryDataAddr, secErr)
		}
	}

	type fanoutRecvFrame struct {
		relayID int
		raw     []byte
		err     error
	}
	framesCh := make(chan fanoutRecvFrame, 64)
	startDataReader := func(rID int, conn *comm.Comm) {
		go func() {
			for {
				raw, rErr := conn.Receive()
				if rErr != nil {
					select {
					case framesCh <- fanoutRecvFrame{relayID: rID, err: rErr}:
					case <-c.stop.ctx.Done():
					}
					return
				}
				select {
				case framesCh <- fanoutRecvFrame{relayID: rID, raw: raw}:
				case <-c.stop.ctx.Done():
					return
				}
			}
		}()
	}
	startDataReader(0, recvData)
	if recvData2 != nil {
		startDataReader(1, recvData2)
	}

	// Watch recvCtrl in the background for relay kick events or multi-file status
	// notifications while the receiver is reading data frames.
	var eventMu sync.Mutex
	var asyncRelayEvent *tcp.FanoutRelayEvent
	nextFileStatusCh := make(chan int, 8)
	finishedConfirmCh := make(chan struct{}, 1)
	ctrlErrCh := make(chan error, 1)

	go func() {
		for {
			raw, recvErr := recvCtrl.Receive()
			if recvErr != nil {
				select {
				case ctrlErrCh <- recvErr:
				default:
				}
				return
			}
			if bytes.Equal(raw, []byte{1}) {
				continue
			}
			if ev, ok := parseFanoutRelayNotice(raw); ok {
				eventMu.Lock()
				asyncRelayEvent = &ev
				eventMu.Unlock()
				recvData.Close()
				if recvData2 != nil {
					recvData2.Close()
				}
				return
			}
			m, decErr := message.Decode(c.Key, raw)
			if decErr != nil {
				continue
			}
			switch m.Type {
			case message.TypeFanoutStatus:
				select {
				case nextFileStatusCh <- m.Num:
				default:
				}
			case message.TypeFinished:
				select {
				case finishedConfirmCh <- struct{}{}:
				default:
				}
				return
			case message.TypeError:
				eventMu.Lock()
				asyncRelayEvent = &tcp.FanoutRelayEvent{
					Type:   "sender-error",
					Reason: m.Message,
				}
				eventMu.Unlock()
				recvData.Close()
				if recvData2 != nil {
					recvData2.Close()
				}
				return
			}
		}
	}()

	checkAsyncEvent := func() *tcp.FanoutRelayEvent {
		eventMu.Lock()
		defer eventMu.Unlock()
		return asyncRelayEvent
	}

	chunkSize := int64(models.TCP_BUFFER_SIZE / 2)
	secondaryAlive := recvData2 != nil

	// 5. Receive each file into <dest>.partial + <dest>.partial.crocmap.
	for fileIdx := range c.FilesToTransfer {
		c.FilesToTransferCurrentNum = fileIdx
		fileInfo := c.FilesToTransfer[fileIdx]

		folderRemote, pathToFile, pathErr := normalizeReceiveFilePath(fileInfo.FolderRemote, fileInfo.Name)
		if pathErr != nil {
			return pathErr
		}
		c.FilesToTransfer[fileIdx].FolderRemote = folderRemote
		c.FilesToTransfer[fileIdx].Name = path.Base(pathToFile)
		fileInfo = c.FilesToTransfer[fileIdx]

		if fileInfo.Size == 0 || fileInfo.Symlink != "" {
			if err := c.createEmptyFileAndFinish(fileInfo, fileIdx); err != nil {
				return err
			}
			c.FilesHasFinished[fileIdx] = struct{}{}
			c.numberOfTransferredFiles++
			continue
		}

		dir, _ := filepath.Split(pathToFile)
		if dir != "" && dir != "." {
			if err := root.MkdirAll(dir, os.ModePerm); err != nil {
				return err
			}
		}

		partialRelPath := pathToFile + ".partial"
		crocmapRelPath := partialRelPath + ".crocmap"
		partialAbsPath := filepath.Join(root.Name(), filepath.FromSlash(partialRelPath))
		crocmapAbsPath := filepath.Join(root.Name(), filepath.FromSlash(crocmapRelPath))

		partialFile, openErr := root.OpenFile(partialRelPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if openErr != nil {
			return openErr
		}
		if err := partialFile.Truncate(fileInfo.Size); err != nil {
			partialFile.Close()
			return err
		}
		c.markFileWrittenByReceiver(fileIdx)

		totalChunks := utils.ChunkRangesCount(nil, fileInfo.Size, chunkSize)
		bm := newChunkBitmap(totalChunks, fileInfo.Size, chunkSize)

		var reqFeatures []string
		if secondaryAlive {
			reqFeatures = []string{dualRelayFeature}
		}
		reqBytes, _ := json.Marshal(RemoteFileRequest{
			FilesToTransferCurrentNum: fileIdx,
			Features:                  reqFeatures,
		})
		if err := message.Send(recvCtrl, c.Key, message.Message{
			Type:  message.TypeRecipientReady,
			Bytes: reqBytes,
		}); err != nil {
			partialFile.Close()
			return err
		}

		if fileIdx > 0 {
			// Wait for sender's stream reset confirmation for fileIdx.
			select {
			case <-nextFileStatusCh:
			case <-time.After(15 * time.Second):
				partialFile.Close()
				return errors.New("timed out waiting for sender to start next fanout file")
			case <-c.stop.ctx.Done():
				partialFile.Close()
				return c.stop.ctx.Err()
			}
		}

		savePartialAndFormatError := func(dataErr error) error {
			_ = partialFile.Sync()
			_ = partialFile.Close()
			_ = bm.SaveSidecar(crocmapAbsPath, fileInfo.Hash)
			c.FanoutPartialPath = partialAbsPath
			pct := 0.0
			if totalChunks > 0 {
				pct = float64(bm.Count()) / float64(totalChunks) * 100.0
			}

			ev := checkAsyncEvent()
			if ev != nil && ev.Type == "kicked" {
				c.FanoutKicked = true
				c.FanoutKickReason = ev.Reason
				if !c.Options.Quiet {
					output, _ := termui.Output(os.Stderr)
					fmt.Fprintf(output, "\nDisconnected by relay: %s.\nPartial file saved at %q (%.1f%% complete).\nRun a 1-on-1 croc receive to resume the remaining chunks.\n",
						ev.Reason, partialRelPath, pct)
				}
				return fmt.Errorf("%w: %s (partial file saved at %s, %.1f%% complete)", ErrFanoutKicked, ev.Reason, partialRelPath, pct)
			}
			reason := "connection closed before transfer completed"
			if ev != nil && ev.Reason != "" {
				reason = ev.Reason
			} else if dataErr != nil {
				reason = dataErr.Error()
			}
			return fmt.Errorf("fanout transfer interrupted: %s (partial file saved at %s, %.1f%% complete)", reason, partialRelPath, pct)
		}

		var decompressedBuf []byte
		var eofSeen [2]bool
		var chunksSinceCtrlAck int

		sendDataAckOn := func(conn *comm.Comm, ackedSeq uint64, bytesDelta int64) error {
			if conn == nil {
				return nil
			}
			var ackFrame [13]byte
			ackFrame[0] = tcp.FanoutDataFrameAck
			binary.LittleEndian.PutUint64(ackFrame[1:9], ackedSeq)
			binary.LittleEndian.PutUint32(ackFrame[9:13], uint32(max(int64(0), bytesDelta)))
			return conn.Send(ackFrame[:])
		}

		sendCtrlChunkAck := func(failedRelay int) {
			ackBytes, ackErr := json.Marshal(bm.BuildAck(fileIdx, failedRelay))
			if ackErr == nil {
				_ = message.Send(recvCtrl, c.Key, message.Message{
					Type:  message.TypeChunkAck,
					Bytes: ackBytes,
				})
			}
		}

		isMultiFileBeforeLast := fileIdx < len(c.FilesToTransfer)-1

		for {
			var frame fanoutRecvFrame
			select {
			case frame = <-framesCh:
			case <-c.stop.ctx.Done():
				return savePartialAndFormatError(c.stop.ctx.Err())
			}

			if frame.err != nil {
				if frame.relayID == 1 && checkAsyncEvent() == nil {
					// Secondary Relay B disconnected mid-transfer: notify sender of our
					// current chunk bitmap so any missing Relay B chunks are re-pushed
					// over Primary Relay A!
					secondaryAlive = false
					sendCtrlChunkAck(1)
					if bm.AllMarked() && (!isMultiFileBeforeLast || eofSeen[0]) {
						break
					}
					continue
				}
				return savePartialAndFormatError(frame.err)
			}

			raw := frame.raw
			if bytes.Equal(raw, []byte{1}) || len(raw) == 0 {
				continue
			}

			kind := raw[0]
			if kind == tcp.FanoutDataFrameEOF {
				if bm.Count() == 0 && fileInfo.Size > 0 {
					// Ignore trailing EOF left over from a previous stream or failover.
					continue
				}
				eofSeen[frame.relayID&1] = true
				if bm.AllMarked() {
					if !isMultiFileBeforeLast || (eofSeen[0] && (!secondaryAlive || eofSeen[1])) {
						break
					}
				}
				if !secondaryAlive && frame.relayID == 0 && !bm.AllMarked() {
					// Request any remaining chunks from sender before failing.
					sendCtrlChunkAck(1)
					continue
				}
				continue
			}
			if kind != tcp.FanoutDataFrameChunk || len(raw) < 10 {
				return savePartialAndFormatError(errors.New("invalid fanout data frame"))
			}

			seq := binary.LittleEndian.Uint64(raw[1:9])
			decrypted, decErr := crypt.DecryptAEADInPlace(raw[9:], c.dataAEAD)
			if decErr != nil {
				return savePartialAndFormatError(decErr)
			}
			if !c.Options.NoCompress && fileInfo.IsCompressed {
				var decompErr error
				decrypted, decompErr = compress.DecompressTo(decompressedBuf, decrypted, maxDecompressedChunkSize)
				if decompErr != nil {
					return savePartialAndFormatError(decompErr)
				}
				decompressedBuf = decrypted
			}
			if len(decrypted) < 9 {
				return savePartialAndFormatError(errors.New("decrypted chunk too short"))
			}
			pos := binary.LittleEndian.Uint64(decrypted[:8])
			payload := decrypted[8:]
			if fitErr := chunkFits(fileInfo.Size, pos, len(payload)); fitErr != nil {
				return savePartialAndFormatError(fitErr)
			}

			if _, writeErr := partialFile.WriteAt(payload, int64(pos)); writeErr != nil {
				return savePartialAndFormatError(writeErr)
			}
			chunkIdx := int(int64(pos) / chunkSize)
			newlyMarked := bm.Mark(chunkIdx)
			if newlyMarked {
				c.addProgress(int64(len(payload)))
				c.dualRelay.relayChunksRecv[frame.relayID&1].Add(1)
			}

			if c.Options.SimulatedReceiverRateBps > 0 {
				delay := time.Duration(float64(len(payload)) / float64(c.Options.SimulatedReceiverRateBps) * float64(time.Second))
				if delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-timer.C:
					case <-c.stop.ctx.Done():
						timer.Stop()
						return savePartialAndFormatError(c.stop.ctx.Err())
					}
					if ev := checkAsyncEvent(); ev != nil {
						return savePartialAndFormatError(errors.New(ev.Reason))
					}
				}
			}

			bytesDelta := int64(len(payload))
			if frame.relayID == 0 {
				if ackErr := sendDataAckOn(recvData, seq+1, bytesDelta); ackErr != nil {
					return savePartialAndFormatError(ackErr)
				}
				// Credit aggregate throughput to secondary relay's rate window too.
				if secondaryAlive && newlyMarked {
					_ = sendDataAckOn(recvData2, 0, bytesDelta)
				}
			} else {
				if ackErr := sendDataAckOn(recvData2, seq+1, bytesDelta); ackErr != nil {
					secondaryAlive = false
					sendCtrlChunkAck(1)
				}
				// Credit aggregate throughput to primary relay's rate window too.
				if newlyMarked {
					_ = sendDataAckOn(recvData, 0, bytesDelta)
				}
			}

			if newlyMarked && recvData2 != nil {
				chunksSinceCtrlAck++
				if chunksSinceCtrlAck >= 8 || bm.AllMarked() {
					chunksSinceCtrlAck = 0
					failed := -1
					if !secondaryAlive {
						failed = 1
					}
					sendCtrlChunkAck(failed)
				}
			}

			if bm.AllMarked() && !isMultiFileBeforeLast {
				break
			}
		}

		if err := partialFile.Close(); err != nil {
			return err
		}

		// Verify file hash before promoting <dest>.partial -> <dest>.
		if len(fileInfo.Hash) > 0 {
			gotHash, hashErr := receiveFileHash(c.clientContext(), partialAbsPath, c.Options.HashAlgorithm, false)
			if hashErr != nil || !bytes.Equal(gotHash, fileInfo.Hash) {
				_ = message.Send(recvCtrl, c.Key, message.Message{
					Type:    message.TypeError,
					Message: fmt.Sprintf("hash mismatch for %s", fileInfo.Name),
				})
				return fmt.Errorf("file hash mismatch for %s", fileInfo.Name)
			}
		}

		if err := root.Rename(partialRelPath, pathToFile); err != nil {
			return err
		}
		_ = os.Remove(crocmapAbsPath)
		c.FilesHasFinished[fileIdx] = struct{}{}
		c.numberOfTransferredFiles++

		if err := message.Send(recvCtrl, c.Key, message.Message{
			Type: message.TypeCloseSender,
			Num:  fileIdx,
		}); err != nil {
			return err
		}
	}

	if err := message.Send(recvCtrl, c.Key, message.Message{
		Type: message.TypeFinished,
	}); err != nil {
		return err
	}
	select {
	case <-finishedConfirmCh:
	case <-time.After(5 * time.Second):
	case <-c.stop.ctx.Done():
		return c.stop.ctx.Err()
	}

	c.updateLifecycle(func(state *transferLifecycle) { state.Successful = true })
	log.Debug("fanout receive completed successfully")
	return nil
}
