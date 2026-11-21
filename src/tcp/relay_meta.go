package tcp

import (
	"encoding/json"
	"slices"
	"strings"
)

// RelayMetaPrefix marks the fourth "|||"-delimited field in a relay handshake
// response as structured extension metadata.
const RelayMetaPrefix = "meta-v1:"

// RelayMeta carries backward-compatible relay capabilities and static peer
// relay addresses in the fourth field of the relay authentication banner.
type RelayMeta struct {
	Peers              []string `json:"peers,omitempty"`
	Fanout             bool     `json:"fanout,omitempty"`
	FanoutMaxReceivers int      `json:"fanoutMaxReceivers,omitempty"`
	FanoutBufferBytes  int64    `json:"fanoutBufferBytes,omitempty"`
	WSPublicURL        string   `json:"wsPublicURL,omitempty"`
}

// IsZero reports whether the metadata carries no extension fields. When true,
// the relay omits the fourth banner segment so the wire format is identical to
// legacy croc relays.
func (m RelayMeta) IsZero() bool {
	return len(m.Peers) == 0 &&
		!m.Fanout &&
		m.FanoutMaxReceivers == 0 &&
		m.FanoutBufferBytes == 0 &&
		m.WSPublicURL == ""
}

// NormalizePeerList splits comma-separated peer entries, trims whitespace,
// drops empty items, and deduplicates while preserving order.
func NormalizePeerList(peers []string) []string {
	var normalized []string
	for _, entry := range peers {
		for part := range strings.SplitSeq(entry, ",") {
			candidate := strings.TrimSpace(part)
			if candidate == "" {
				continue
			}
			if !slices.Contains(normalized, candidate) {
				normalized = append(normalized, candidate)
			}
		}
	}
	return normalized
}

// EncodeRelayMeta serializes non-empty metadata with the "meta-v1:" prefix.
func EncodeRelayMeta(meta RelayMeta) (string, error) {
	meta.Peers = NormalizePeerList(meta.Peers)
	if meta.IsZero() {
		return "", nil
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	return RelayMetaPrefix + string(encoded), nil
}

// ParseRelayMeta parses a "meta-v1:<json>" banner field.
func ParseRelayMeta(part string) (RelayMeta, bool) {
	trimmed := strings.TrimSpace(part)
	if !strings.HasPrefix(trimmed, RelayMetaPrefix) {
		return RelayMeta{}, false
	}
	rawJSON := strings.TrimPrefix(trimmed, RelayMetaPrefix)
	var meta RelayMeta
	if err := json.Unmarshal([]byte(rawJSON), &meta); err != nil {
		return RelayMeta{}, false
	}
	meta.Peers = NormalizePeerList(meta.Peers)
	return meta, true
}
