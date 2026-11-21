package tcp

import "time"

const (
	DEFAULT_LOG_LEVEL                         = "debug"
	DEFAULT_MAX_PENDING_HANDSHAKES            = 64
	DEFAULT_MAX_PENDING_HANDSHAKES_PER_SOURCE = 32
	DEFAULT_FIRST_MESSAGE_TIMEOUT             = time.Minute
	DEFAULT_HANDSHAKE_TIMEOUT                 = 5 * time.Minute
	DEFAULT_MAX_ROOMS_OPEN                    = 128
	DEFAULT_ROOM_CLEANUP_INTERVAL             = 10 * time.Minute
	DEFAULT_ROOM_TTL                          = 3 * time.Hour
	DEFAULT_SOURCE_JOIN_LIMIT                 = 30
	DEFAULT_ROOM_JOIN_LIMIT                   = 30
	DEFAULT_JOIN_LIMIT_WINDOW                 = time.Minute
	DEFAULT_FANOUT_BUFFER                     = int64(16 << 20)  // 16 MiB per fan-out room
	DEFAULT_FANOUT_MAX_MEMORY                 = int64(256 << 20) // 256 MiB total across all fan-out rooms
	DEFAULT_FANOUT_MAX_RECEIVERS              = 8
	DEFAULT_FANOUT_MIN_RATE                   = int64(256 << 10) // 256 KiB/s minimum sliding rate
	DEFAULT_FANOUT_RATE_WINDOW                = 10 * time.Second
	DEFAULT_FANOUT_STALL_TIMEOUT              = 15 * time.Second
)
