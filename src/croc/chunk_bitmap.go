package croc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"sync"
)

var crocMapMagic = [8]byte{'C', 'R', 'O', 'C', 'M', 'A', 'P', '1'}

const (
	// maxRecentAcked bounds the out-of-order indices remembered between two
	// ACKs. On overflow BuildAck rescans the bitmap instead of silently
	// dropping indices: a dropped index stays "in flight" on the sender until
	// the cumulative frontier passes it, which stalls the faster relay behind
	// the slower one in dynamic mode.
	maxRecentAcked = 4096
	maxAckListLen  = 16384
)

// ChunkAckPayload carries cumulative and selective chunk acknowledgements
// from a receiver to a sender, plus an optional failed-relay notification.
type ChunkAckPayload struct {
	FileIndex   int      `json:"fi"`
	CumChunk    uint32   `json:"cc"`
	Bitmask     uint64   `json:"bm,omitempty"`
	AckedList   []uint32 `json:"al,omitempty"`
	Finished    bool     `json:"fn,omitempty"`
	FailedRelay int      `json:"fr"` // -1 when no relay failure is being reported
}

// chunkBitmap tracks received chunks for one file using 1 bit per chunk.
// All multi-byte fields written to disk use explicit LittleEndian byte order so
// .crocmap files are portable across arm64 and x86 architectures.
type chunkBitmap struct {
	mu          sync.Mutex
	totalChunks int
	fileSize    int64
	chunkSize   int64
	words       []uint64
	setCount    int
	cumChunk    int
	recentAcked []uint32
	ackOverflow bool
	highest     int
}

func newChunkBitmap(totalChunks int, fileSize, chunkSize int64) *chunkBitmap {
	if totalChunks < 0 {
		totalChunks = 0
	}
	wordCount := (totalChunks + 63) / 64
	return &chunkBitmap{
		totalChunks: totalChunks,
		fileSize:    fileSize,
		chunkSize:   chunkSize,
		words:       make([]uint64, wordCount),
	}
}

// Mark sets the bit for chunkIdx and returns true if the chunk was not
// previously marked. It returns false for out-of-range or duplicate chunks.
func (b *chunkBitmap) Mark(chunkIdx int) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if chunkIdx < 0 || chunkIdx >= b.totalChunks {
		return false
	}
	wordIdx := chunkIdx / 64
	bitMask := uint64(1) << uint(chunkIdx%64)
	if b.words[wordIdx]&bitMask != 0 {
		return false
	}
	b.words[wordIdx] |= bitMask
	b.setCount++
	b.highest = max(b.highest, chunkIdx)
	b.advanceCumLocked()
	if chunkIdx > b.cumChunk+64 {
		if len(b.recentAcked) < maxRecentAcked {
			b.recentAcked = append(b.recentAcked, uint32(chunkIdx))
		} else {
			b.ackOverflow = true
		}
	}
	return true
}

// Has reports whether chunkIdx is marked.
func (b *chunkBitmap) Has(chunkIdx int) bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if chunkIdx < 0 || chunkIdx >= b.totalChunks {
		return false
	}
	return (b.words[chunkIdx/64] & (uint64(1) << uint(chunkIdx%64))) != 0
}

// ApplyAck updates the bitmap from a receiver's ChunkAckPayload.
func (b *chunkBitmap) ApplyAck(ack ChunkAckPayload) {
	if b == nil {
		return
	}
	if ack.Finished {
		for idx := 0; idx < b.totalChunks; idx++ {
			b.Mark(idx)
		}
		return
	}
	for idx := 0; idx < min(int(ack.CumChunk), b.totalChunks); idx++ {
		b.Mark(idx)
	}
	for bit := 0; bit < 64; bit++ {
		if (ack.Bitmask & (uint64(1) << uint(bit))) != 0 {
			b.Mark(int(ack.CumChunk) + 1 + bit)
		}
	}
	for _, idx := range ack.AckedList {
		b.Mark(int(idx))
	}
}

// Count returns the total number of unique marked chunks.
func (b *chunkBitmap) Count() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.setCount
}

// Total returns the total number of chunks in the file.
func (b *chunkBitmap) Total() int {
	if b == nil {
		return 0
	}
	return b.totalChunks
}

// AllMarked reports whether every chunk in [0, totalChunks) is marked.
func (b *chunkBitmap) AllMarked() bool {
	if b == nil {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.setCount >= b.totalChunks
}

func (b *chunkBitmap) advanceCumLocked() {
	for b.cumChunk < b.totalChunks {
		wordIdx := b.cumChunk / 64
		bitOffset := uint(b.cumChunk % 64)
		if bitOffset == 0 && b.words[wordIdx] == ^uint64(0) {
			b.cumChunk = min(b.totalChunks, (wordIdx+1)*64)
			continue
		}
		if (b.words[wordIdx] & (uint64(1) << bitOffset)) == 0 {
			break
		}
		b.cumChunk++
	}
}

// PreMarkPresent marks every chunk NOT listed in missingRanges as already
// present on disk (used when resuming a partially received file).
func (b *chunkBitmap) PreMarkPresent(missingRanges []int64) {
	if b == nil || len(missingRanges) == 0 {
		return
	}
	chunkSize := missingRanges[0]
	if chunkSize <= 0 {
		chunkSize = b.chunkSize
	}
	if chunkSize <= 0 {
		return
	}
	missing := make([]bool, b.totalChunks)
	for i := 1; i+1 < len(missingRanges); i += 2 {
		startOffset := missingRanges[i]
		count := missingRanges[i+1]
		if startOffset < 0 || count <= 0 {
			continue
		}
		startIdx := int(startOffset / chunkSize)
		for k := 0; k < int(count); k++ {
			idx := startIdx + k
			if idx >= 0 && idx < b.totalChunks {
				missing[idx] = true
			}
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for idx := 0; idx < b.totalChunks; idx++ {
		if !missing[idx] {
			wordIdx := idx / 64
			bitMask := uint64(1) << uint(idx%64)
			if b.words[wordIdx]&bitMask == 0 {
				b.words[wordIdx] |= bitMask
				b.setCount++
			}
		}
	}
	b.advanceCumLocked()
}

// BuildAck snapshots the current cumulative ACK frontier, a 64-bit selective
// ACK bitmask starting at CumChunk+1, and any higher out-of-order chunk indices.
func (b *chunkBitmap) BuildAck(fileIndex int, failedRelay int) ChunkAckPayload {
	if b == nil {
		return ChunkAckPayload{FileIndex: fileIndex, FailedRelay: failedRelay}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.advanceCumLocked()
	var mask uint64
	for i := 0; i < 64; i++ {
		idx := b.cumChunk + 1 + i
		if idx >= b.totalChunks {
			break
		}
		if (b.words[idx/64] & (uint64(1) << uint(idx%64))) != 0 {
			mask |= uint64(1) << uint(i)
		}
	}
	var extra []uint32
	if b.ackOverflow {
		// Too many out-of-order arrivals since the last ACK: report every
		// marked chunk beyond the bitmask window directly from the bitmap.
		for idx := b.cumChunk + 65; idx <= b.highest && idx < b.totalChunks && len(extra) < maxAckListLen; idx++ {
			if (b.words[idx/64] & (uint64(1) << uint(idx%64))) != 0 {
				extra = append(extra, uint32(idx))
			}
		}
		b.ackOverflow = false
	} else {
		for _, idx := range b.recentAcked {
			if int(idx) > b.cumChunk+64 {
				extra = append(extra, idx)
			}
		}
	}
	b.recentAcked = b.recentAcked[:0]
	return ChunkAckPayload{
		FileIndex:   fileIndex,
		CumChunk:    uint32(b.cumChunk),
		Bitmask:     mask,
		AckedList:   extra,
		Finished:    b.setCount >= b.totalChunks,
		FailedRelay: failedRelay,
	}
}

// MissingRanges returns a croc-compatible chunk-range slice ([chunkSize,
// startOffset1, count1, ...]) for all unmarked chunks. When every chunk is
// marked it returns nil.
func (b *chunkBitmap) MissingRanges() []int64 {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.setCount >= b.totalChunks {
		return nil
	}
	var ranges []int64
	var runStart, runCount int64
	flushRun := func() {
		if runCount == 0 {
			return
		}
		if len(ranges) == 0 {
			ranges = append(ranges, b.chunkSize)
		}
		ranges = append(ranges, runStart, runCount)
		runCount = 0
	}
	for idx := 0; idx < b.totalChunks; idx++ {
		marked := (b.words[idx/64] & (uint64(1) << uint(idx%64))) != 0
		if !marked {
			if runCount == 0 {
				runStart = int64(idx) * b.chunkSize
			}
			runCount++
		} else {
			flushRun()
		}
	}
	flushRun()
	return ranges
}

// SaveSidecar writes the bitmap and file identity metadata to path using an
// architecture-independent LittleEndian binary layout:
// [8B magic "CROCMAP1"][8B fileSize][8B chunkSize][4B totalChunks][4B hashLen][hashBytes][8B*wordCount words].
func (b *chunkBitmap) SaveSidecar(path string, fileHash []byte) error {
	if b == nil {
		return errors.New("chunk bitmap is nil")
	}
	b.mu.Lock()
	totalChunks := b.totalChunks
	fileSize := b.fileSize
	chunkSize := b.chunkSize
	wordsCopy := append([]uint64(nil), b.words...)
	b.mu.Unlock()

	headerSize := 8 + 8 + 8 + 4 + 4 + len(fileHash)
	buf := make([]byte, headerSize+len(wordsCopy)*8)
	copy(buf[:8], crocMapMagic[:])
	binary.LittleEndian.PutUint64(buf[8:16], uint64(fileSize))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(chunkSize))
	binary.LittleEndian.PutUint32(buf[24:28], uint32(totalChunks))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(len(fileHash)))
	copy(buf[32:headerSize], fileHash)
	offset := headerSize
	for _, w := range wordsCopy {
		binary.LittleEndian.PutUint64(buf[offset:offset+8], w)
		offset += 8
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, buf, 0o600); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

// LoadChunkBitmapSidecar loads and validates a .crocmap sidecar against the
// expected file size, chunk size, and file hash.
func LoadChunkBitmapSidecar(path string, expectedFileSize, expectedChunkSize int64, expectedHash []byte) (*chunkBitmap, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(raw) < 32 {
		return nil, errors.New("crocmap file too short")
	}
	if !bytes.Equal(raw[:8], crocMapMagic[:]) {
		return nil, errors.New("invalid crocmap magic")
	}
	fileSize := int64(binary.LittleEndian.Uint64(raw[8:16]))
	chunkSize := int64(binary.LittleEndian.Uint64(raw[16:24]))
	totalChunks := int(binary.LittleEndian.Uint32(raw[24:28]))
	hashLen := int(binary.LittleEndian.Uint32(raw[28:32]))
	if fileSize != expectedFileSize || chunkSize != expectedChunkSize || totalChunks < 0 || hashLen < 0 {
		return nil, fmt.Errorf("crocmap metadata mismatch (got size=%d chunk=%d)", fileSize, chunkSize)
	}
	headerSize := 32 + hashLen
	wordCount := (totalChunks + 63) / 64
	if len(raw) != headerSize+wordCount*8 {
		return nil, errors.New("crocmap size mismatch")
	}
	storedHash := raw[32:headerSize]
	if len(expectedHash) > 0 && len(storedHash) > 0 && !bytes.Equal(storedHash, expectedHash) {
		return nil, errors.New("crocmap file hash mismatch")
	}
	bm := newChunkBitmap(totalChunks, fileSize, chunkSize)
	offset := headerSize
	setCount := 0
	for i := 0; i < wordCount; i++ {
		w := binary.LittleEndian.Uint64(raw[offset : offset+8])
		if i == wordCount-1 && totalChunks%64 != 0 {
			validMask := (uint64(1) << uint(totalChunks%64)) - 1
			w &= validMask
		}
		bm.words[i] = w
		setCount += bits.OnesCount64(w)
		offset += 8
	}
	bm.setCount = setCount
	bm.advanceCumLocked()
	return bm, nil
}
