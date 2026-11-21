# Nhật ký quá trình – Multi-relay & Multi-receiver cho croc

> File này ghi lại toàn bộ quá trình (khảo sát → hỏi đáp → lập kế hoạch → cài đặt → kiểm thử).
> Mỗi mục có thời gian (giờ máy, +07:00).

## 2026-11-22 00:01 – Khởi động nhiệm vụ (giai đoạn LẬP KẾ HOẠCH, chưa sửa code)

Yêu cầu:
1. Hai croc relay tự host chạy song song, biết nhau; sender tìm được 2 relay sẵn sàng thì dùng 2, không thì dùng 1 (ý tưởng: chia chunk chẵn/lẻ).
2. Một sender → nhiều receiver, relay có buffer giới hạn (không chứa đủ toàn bộ file); xử lý exception.

Phạm vi: CLI (`croc`) và Web app (`web/` + `croc-web`).

## 2026-11-22 00:02 – Khảo sát repository (bước 1–2 của /plan)

- Module: `github.com/schollz/croc/v11`, Go 1.27.3 (bản fork mở rộng: web, store, tailcat/DERP, ssh).
- HEAD: `67e2b110`.

### Thành phần liên quan

| Thành phần | File | Vai trò hiện tại |
|---|---|---|
| Relay TCP | `src/tcp/tcp.go` | Mỗi port = 1 server; `roomInfo{first, second}` – **chỉ 2 peer/room**; sau khi đủ 2 peer thì `pipeWithProtocol` copy 2 chiều, **không có buffer store-and-forward**. |
| Khung tin | `src/comm/comm.go` | Frame `"croc" + uint32 LE length + body`, tối đa 64 MiB/frame. |
| Lệnh relay | `src/cli/cli.go` `relay()` (L1164) | `--ports` mặc định `9009..9013`; port đầu = control, các port sau = data. Yêu cầu ≥2 port. |
| Client | `src/croc/croc.go` | `Send()` (L1787) kết nối control → `senderWaitForHandshake` → banner chứa danh sách data port → `RelayPorts`. |
| Mở kênh data | `croc.go` `openRelayDataChannels` (L2930) | Mỗi data port mở 1 kết nối, room = `<room>-<j>`, **tối đa 8 kênh**, tất cả trên **cùng 1 relay host**. |
| Chia chunk | `src/croc/chunk_queue.go` | `requestedChunkQueue.claim()` – hàng đợi chung, mỗi worker (mỗi kết nối) tự lấy chunk kế tiếp ⇒ **cân bằng tải động**, không phải chẵn/lẻ cố định. Chunk = `TCP_BUFFER_SIZE/2` = 32 KiB, header 8 byte offset. |
| Nhận chunk | `croc.go` `receiveData` (L3660) | `WriteAt(offset)` ⇒ chunk đến **không cần đúng thứ tự** – thuận lợi cho đa relay. |
| Public relay pool | `src/publicrelay/publicrelay.go` | `1..4.getcroc.com:9009`; code phrase băm ra relay index (`codephrase.RelayIndex`). Đây là cơ chế "relay biết nhau" sẵn có dạng danh sách tĩnh. |
| Store | `src/store/service.go` | Dịch vụ lưu trữ tạm (HTTP, mã hoá, giới hạn số lần tải `MaxDownloads`). Liên quan phần 2 (store-and-forward). |
| Web | `web/src/protocol/*.ts`, `src/webrelay/webrelay.go` | Trình duyệt dùng WebSocket → `croc-web` bridge → TCP relay; giao thức croc cài lại bằng TypeScript. |

### Quan sát quan trọng
1. croc **đã** chạy song song nhiều kết nối (multiplexing) nhưng **chỉ trên 1 relay**. Phần 1 = mở rộng sang nhiều relay host.
2. Mã hoá end-to-end bằng PAKE **theo cặp** (sender↔receiver). Với 1→N, mỗi receiver có khoá phiên riêng ⇒ relay không thể fan-out cùng một ciphertext nếu không thay đổi mô hình khoá.
3. Relay hiện tại là "pipe" thuần, không giữ dữ liệu ⇒ "buffer giới hạn ở relay" là tính năng mới hoàn toàn.

## 2026-11-22 00:03 – Vòng hỏi đáp 1 (người dùng trả lời 00:09)

| # | Câu hỏi | Trả lời |
|---|---|---|
| 1 | Deploy 2 relay | **2 mode**: (a) 2 máy khác nhau, IP public khác nhau; (b) chính máy người gửi là 1 relay + 1 relay khác có "IP public" dùng **Cloudflare free** |
| 2 | Relay biết nhau | Danh sách peer tĩnh `--peers`, quảng bá trong banner, client tự probe ping/pong |
| 3 | Chia chunk | Hàng đợi động dùng chung (mở rộng `requestedChunkQueue`) |
| 4 | Relay chết giữa chừng | Tự chuyển chunk chưa xác nhận sang relay còn lại, không gián đoạn |
| 5 | Nhiều receiver | Đồng thời: chờ đủ N (hoặc timeout) rồi phát; đến muộn bị từ chối |
| 6 | Buffer đầy | **Threshold**: receiver chậm dưới ngưỡng → kick; chưa dưới ngưỡng → cả nhóm đi theo receiver chậm nhất (backpressure) |
| 7 | Khoá | 1 code chung, PAKE riêng từng receiver, sender phát 1 content key chung (bọc bằng khoá PAKE) để relay fan-out cùng ciphertext |
| 8 | Web app | **Chỉ CLI**, Web để sau |
| 9 | Kết hợp phần 1 + 2 | Thiết kế kết hợp được, cài & test riêng trước |
| 10 | Bối cảnh | Đồ án môn Lập trình mạng: cần tài liệu, sơ đồ, demo, benchmark |

## 2026-11-22 00:10 – Nghiên cứu Cloudflare (do câu trả lời #1b)

| Nguồn chính thức | Thông tin | Áp dụng |
|---|---|---|
| Cloudflare Spectrum – plans: https://developers.cloudflare.com/spectrum/reference/limitations/ (và trang "About Spectrum") | Spectrum (proxy TCP thuần) **không có ở gói Free**; TCP tuỳ ý cần Enterprise | Không thể đưa relay TCP croc (port 9009) public qua Cloudflare proxy với gói Free |
| Cloudflare Tunnel – Arbitrary TCP: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/use-cases/ (mục "Arbitrary TCP" / "client-side cloudflared") | TCP tuỳ ý qua Tunnel được đóng gói trong WebSocket; **client phải cài `cloudflared`** và chạy `cloudflared access tcp --hostname H --url localhost:P` | Phương án B: receiver cài thêm cloudflared – không cần sửa code nhưng UX kém |
| Cloudflare WebSockets: https://developers.cloudflare.com/network/websockets/ | WebSocket proxy được hỗ trợ **trên mọi gói kể cả Free** | Phương án A: thêm transport `ws(s)://` cho relay croc → receiver không cần cài gì thêm |
| Quick Tunnels: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/do-more-with-tunnels/trycloudflare/ | `trycloudflare.com` miễn phí, không cần domain, URL ngẫu nhiên đổi mỗi lần chạy, tối đa 200 request đồng thời, không SSE, không bảo đảm uptime | Phù hợp demo đồ án; URL phải được quảng bá động (qua control channel) |
| Service-Specific Terms (CDN/Application Services): https://www.cloudflare.com/service-specific-terms-application-services/ | Gói không phải Enterprise không được dùng CDN để phục vụ chủ yếu nội dung lớn/không phải HTML | **Rủi ro**: truyền file lớn qua proxy Cloudflare free có thể vi phạm điều khoản – chỉ dùng cho demo/benchmark nhỏ |

Trong repo: `croc-web` (`src/webrelay/webrelay.go` `websocket()`) đã có **bridge WebSocket → TCP relay** dùng `github.com/coder/websocket v1.8.15`; nhưng **client Go CLI hiện chưa quay số WebSocket** (`comm.NewConnectionContext` chỉ dial TCP/SOCKS5/HTTP CONNECT).

Nhận xét băng thông mode (b): relay trên máy sender → receiver vẫn đi qua **uplink của sender**; relay B cũng nhận dữ liệu từ uplink của sender ⇒ hai đường dùng chung uplink. Lợi ích chỉ có khi nút cổ chai là đường đi/relay chứ không phải uplink. Benchmark phải làm rõ điều này.

## 2026-11-22 00:12 – Vòng hỏi đáp 2 (người dùng trả lời 00:15)

| # | Câu hỏi | Trả lời |
|---|---|---|
| 1 | "Đồ free Cloudflare" | Cloudflare Tunnel (Quick hoặc named) expose relay qua **WebSocket**; thêm transport `ws(s)://` vào CLI (dùng `coder/websocket` sẵn có) |
| 2 | Mode b – vị trí | Relay trên **máy sender có IP public/port-forward**; relay còn lại **nằm sau Cloudflare Tunnel** |
| 3 | Tiêu chí kick | Tốc độ tối thiểu (vd < 256 KB/s trung bình trượt 10 s) |
| 4 | Receiver bị kick | Thông báo rõ, giữ file `.partial`, nhận tiếp sau bằng phiên 1-1 (resume croc) |
| 5 | Bắt đầu phát | `--receivers N`, `--wait-timeout`, `--min-receivers` (mặc định 1) |
| 6 | Buffer relay | `--fanout-buffer` (mặc định 16 MiB, RAM), `--fanout-max-memory`, `--fanout-max-receivers` (mặc định 8) |
| 7 | Benchmark | Docker Compose + `tc netem` giới hạn băng thông; thêm 1 lần chạy thật |
| 8 | Ngôn ngữ | Tài liệu tiếng Việt (thuật ngữ tiếng Anh), comment code tiếng Anh |

## 2026-11-22 00:16 – Nghiên cứu chuyên sâu (hoàn tất 00:22)
- Lưu ý: 3 subagent ban đầu bị gián đoạn bởi lỗi quota 429; agent chính đã thực hiện lại toàn bộ công việc nghiên cứu trực tiếp và đầy đủ.
- Đã đọc và truy vết chi tiết: `README.md`, `src/tcp/*`, `src/comm/comm.go`, `src/croc/croc.go`, `src/croc/chunk_queue.go`, `src/croc/client_state.go`, `src/croc/relay_dial.go`, `src/croc/local_relay.go`, `src/croc/staged_transport.go`, `src/croc/tailcat_negotiation.go`, `src/croc/receive_preflight.go`, `src/croc/bundle.go`, `src/pakekey/pakekey.go`, `src/message/message.go`, `src/crypt/crypt.go`, `src/utils/utils.go`, `src/cli/cli.go`, `src/webrelay/webrelay.go`, `web/src/protocol/relay.ts`, `Dockerfile`, `Makefile`.
- Đã kiểm chứng tài liệu chính thức: `schollz/croc` (README, design post, issues #441/#481/#510/#1040/#1269), `schollz/pake`, `coder/websocket` (phát hiện quan trọng: `NetConn` đóng kết nối khi `SetReadDeadline` hết hạn → phải dùng WS→loopback TCP bridge trên relay!), `golang.org/x/time/rate`, Cloudflare Spectrum/Tunnel/WebSockets/Quick Tunnels/ToS, `tc-tbf`/`tc-netem` & Docker `cap_add: NET_ADMIN`.
- Đã kiểm tra công cụ máy host: `docker 29.6.2` ✓, `cloudflared 2026.10.1` ✓, `go` chưa có trên PATH host (có thể dùng `golang:1.27.3-alpine` hoặc cài thêm).

## 2026-11-22 00:23 – Hoàn tất bộ tài liệu & Kế hoạch cài đặt (chưa sửa code)
Các tài liệu đã tạo trong `/Users/huyle/Documents/dev/LTM/croc/croc/multiple_croc_agent_doc/`:
1. `00_process_log.md` — Nhật ký quá trình (file này).
2. `01_official_research.md` — Báo cáo nghiên cứu tài liệu chính thức & kiểm chứng dependency (kèm URL).
3. `02_codebase_analysis.md` — Phân tích cấu trúc codebase, truy vết luồng thực thi & danh sách 10 bẫy kỹ thuật/tương thích.
4. `03_implementation_plan.md` — Kế hoạch thiết kế & triển khai chi tiết 5 giai đoạn (đồng bộ với Implementation Plan artifact để người dùng duyệt).

**Trạng thái hiện tại**: Dừng ở giai đoạn lập kế hoạch, chờ người dùng xem xét & phê duyệt trước khi bắt đầu cài đặt.

## 2026-11-22 00:35 – Cập nhật kế hoạch theo góp ý của người dùng
Người dùng góp ý trực tiếp trên `implementation_plan.md`:
1. Đã cài Go trên macOS Apple Silicon — đã kiểm tra xác nhận `/opt/homebrew/bin/go` (`go1.27.4 darwin/arm64`). Yêu cầu các relay phải chạy được trên cả **x86** (bên cạnh Apple Silicon `arm64`).
2. Xác nhận **giữ cả 2 mode** `--chunk-policy dynamic|parity`.
3. Đã kiểm tra thêm build tag `croc_no_tailcat` (`src/cli/relay_only_build_test.go`, `src/croc/relay_only_build_test.go`).
4. Đã cập nhật đồng bộ `implementation_plan.md`, `01_official_research.md`, `03_implementation_plan.md` với ràng buộc thuần Go (`CGO_ENABLED=0`), tường minh byte-order, typed `sync/atomic`, danh sách lệnh cross-compile (`darwin/arm64`, `linux/amd64`, `linux/arm64`, `linux/386`, `windows/amd64`, `croc_no_tailcat`) và Docker multi-arch (`linux/amd64,linux/arm64`).

**Trạng thái hiện tại**: Kế hoạch đã chốt mọi yêu cầu, chờ người dùng nhấn phê duyệt / xác nhận để bắt đầu Phase 0 (chưa sửa code).

## 2026-11-22 00:49 – 00:58: Thực hiện & Hoàn tất Phase 0 (Nền tảng & Môi trường)
- Tạo hồ sơ bàn giao liên tục [04_implementation_handover.md](file:///Users/huyle/Documents/dev/LTM/croc/croc/multiple_croc_agent_doc/04_implementation_handover.md) để mọi coding agent độc lập đều có thể tiếp quản ngay nếu hết token.
- Chạy `/opt/homebrew/bin/go mod download` tải đầy đủ dependencies vào `/Users/huyle/go/pkg/mod`.
- Kiểm chứng trực tiếp `go doc github.com/coder/websocket.NetConn` từ module cache cục bộ.
- Kiểm chứng biên dịch chéo thuần Go (`CGO_ENABLED=0`) trên máy macOS Apple Silicon (`go1.27.4 darwin/arm64`):
  - `GOOS=darwin GOARCH=arm64` → `/tmp/croc-phase0-darwin-arm64` (31 MB) — PASS.
  - `GOOS=linux GOARCH=amd64` → `/tmp/croc-phase0-linux-amd64` (33 MB) — PASS.
- Chạy baseline unit tests xanh 100%:
  - `ok github.com/schollz/croc/v11/src/comm 1.112s`
  - `ok github.com/schollz/croc/v11/src/tcp 5.709s`
  - `ok github.com/schollz/croc/v11/src/message 0.752s`
- **Dừng lại sau Phase 0** theo đúng chỉ thị của người dùng, chờ lệnh bắt đầu **Phase 1 (WebSocket Transport & Peer Metadata)**.

## 2026-11-22 00:59 – 01:10: Thực hiện & Hoàn tất Phase 1 (WebSocket Transport & Peer Metadata)
- Đã tạo mới:
  - [ws.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/comm/ws.go) & [ws_test.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/comm/ws_test.go) — client WebSocket dialer (`ws://`, `wss://`, proxy support, Ping 30 s, tách `streamCtx` khỏi `dialCtx`, `JoinRelayAddressPort`).
  - [relay_meta.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/relay_meta.go) — `RelayMeta` (`meta-v1:<json>`) ở `parts[3]` của relay handshake banner.
  - [ws_bridge.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/ws_bridge.go) & [ws_bridge_test.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/ws_bridge_test.go) — in-process HTTP/WebSocket→loopback TCP bridge (`/healthz`, `/ws?port=<port|control>`) cho `croc relay --ws-listen`.
- Đã cập nhật:
  - [comm.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/comm/comm.go), [options.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/options.go), [tcp.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/tcp.go), [relay_dial.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/relay_dial.go), [croc.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/croc.go), [cli.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/cli/cli.go).
- Kiểm chứng biên dịch đa kiến trúc (`CGO_ENABLED=0`):
  - `darwin/arm64` (31 MB) — PASS
  - `linux/amd64` (34 MB) — PASS
  - `linux/amd64 -tags croc_no_tailcat` (15 MB) — PASS
- Kiểm thử tự động (`-race` & regression):
  - `TestNormalizeAndBuildWebSocketURL` — PASS
  - `TestRelayMetaBackwardsCompatibility` — PASS (`0.08s`)
  - `TestWebSocketBridgeAndClientDial` (gồm test `earlyRoom` 5 ms không ngắt WS và fast-admission qua WS) — PASS (`0.12s`)
  - Regression `src/comm` & `src/tcp` — PASS (`6.149s`)
- **Dừng lại sau Phase 1** theo đúng chỉ thị của người dùng, chờ lệnh bắt đầu **Phase 2 (Dual-Relay Parallel Transfer + Dynamic/Parity Queue + Failover)**.

## 2026-11-22 01:12 – 01:33: Thực hiện & Hoàn tất Phase 2 (Dual-Relay Parallel Transfer + Dynamic/Parity Queue + Failover)
- Đã tạo mới:
  - [chunk_bitmap.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/chunk_bitmap.go) — `chunkBitmap` thread-safe (1 bit/chunk), chống đếm trùng chunk khi failover, tạo `ChunkAckPayload`, lưu/đọc `.crocmap` sidecar với `binary.LittleEndian` (tương thích đa kiến trúc `arm64`/`x86`).
  - [chunk_scheduler.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/chunk_scheduler.go) — `reliableChunkScheduler` hỗ trợ cả 2 chế độ `dynamic` và `parity` (chẵn/lẻ), theo dõi `inFlight` theo `relayID`, `ApplyAck`, `RequeueRelay`, `StartWatchdog`.
  - [dual_relay.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/dual_relay.go) & [dual_relay_test.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/dual_relay_test.go) — tự khám phá peer từ `--relay2` và `RelayMeta.Peers`, probe ping/pong song song, đàm phán `TypeMultiRelayOffer`/`TypeMultiRelayAck`, truyền song song qua 2 relay (cả TCP và WebSocket), Data Failover khi Relay B chết và Control+Data Failover (`TypeControlResume` + HMAC) khi Primary Relay A chết.
- Đã cập nhật:
  - [message.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/message/message.go), [chunk_queue.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/chunk_queue.go), [tailcat_negotiation.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/tailcat_negotiation.go), [bundle.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/bundle.go), [croc.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/croc.go), [cli.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/cli/cli.go) (thêm `--dual-relay`, `--relay2`, `--chunk-policy`).
- Phát hiện & sửa lỗi trong quá trình test:
  - Sửa `resetDualRelayState` tách riêng `closeDualRelayConnections()` để không ghi đè struct chứa `sync.Mutex` đang khoá và bảo toàn số liệu thống kê `DualRelayActive()`/`DualRelayChunksSent()` sau khi đóng kết nối.
- Kết quả kiểm thử `-race` (`5.395s`):
  - `TestChunkBitmapAndSidecarPortability` — PASS
  - `TestReliableChunkSchedulerDynamicAndParity` — PASS
  - `TestDualRelayDynamicAndParityTransfer/dynamic-via-banner-peers-and-websocket-relayB` — PASS (`0.26s`)
  - `TestDualRelayDynamicAndParityTransfer/parity-even-odd-split-across-two-tcp-relays` — PASS (`0.25s`)
  - `TestDualRelayFallbackWhenPeerUnavailable` — PASS (`0.17s`)
  - `TestDualRelayFailoverSecondaryDiesMidTransfer` — PASS (`1.38s`)
  - `TestDualRelayFailoverPrimaryControlDiesMidTransfer` — PASS (`1.39s`)
- **Dừng lại sau Phase 2** theo đúng chỉ thị của người dùng, chờ lệnh bắt đầu **Phase 3 (Phần 2: 1→N Fan-out + Bounded Buffer + Kick & Resume `.partial`)**.




## 2026-11-22 01:36 – 02:20: Thực hiện & Hoàn tất Phase 3 (Phần 2: 1→N Fan-out + Bounded Buffer + Kick & Resume `.partial`)
- Đã tạo mới:
  - [fanout_buffer.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/fanout_buffer.go) — `FanoutMemoryBudget` (quản lý RAM toàn cục `--fanout-max-memory`), `FanoutRingBuffer` (bộ đệm vòng giới hạn `--fanout-buffer`, backpressure `sync.Cond`, thu hồi slot khi mọi subscriber còn sống đã ACK), `slidingRateWindow` & `StartWatchdog` (kick receiver dưới ngưỡng tốc độ `--fanout-min-rate` trong cửa sổ `--fanout-rate-window` hoặc treo quá `--fanout-stall-timeout`, miễn trừ ngưỡng tốc độ cho receiver cuối cùng trừ khi bật `--strict-min-rate`).
  - [fanout.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/fanout.go) — `FanoutManager` điều phối phòng `fanout:v1:*`, Mux Control 4-byte header LittleEndian, phát thông báo `croc-fanout-event-v1:` khi kick receiver hoặc khi sender đứt kết nối.
  - [fanout_sender.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/fanout_sender.go) & [fanout_receiver.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/fanout_receiver.go) — PAKE v2 theo cặp qua Mux Control, phân phối `contentKey` 256-bit qua `TypeGroupKey`, lobby `--receivers N` / `--min-receivers` / `--wait-timeout`, ghi `<file>.partial` + lưu `.partial.crocmap` khi bị kick kèm thông báo chi tiết.
  - [fanout_test.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/fanout_test.go) — Bộ test tích hợp cho 1→N fan-out, kiểm chứng RAM đỉnh $\le$ `--fanout-buffer`, kick receiver chậm + resume 1-1 từ `.partial`/`.crocmap` (kể cả block toàn `0`), và các kịch bản ngoại lệ E6–E10.
- Đã cập nhật:
  - [defaults.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/defaults.go), [options.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/options.go), [tcp.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/tcp.go), [receive_preflight.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/receive_preflight.go), [croc.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/croc.go), [cli.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/cli/cli.go).
- Phát hiện & sửa lỗi trong quá trình test:
  - `internal/cli` sử dụng `cli.IntFlag` thay vì `cli.Int64Flag` → chuyển sang `cli.IntFlag` và ép kiểu `int64(c.Int(...))` tương thích cả hệ 32-bit (`386`) lẫn 64-bit (`amd64`, `arm64`).
  - Trong `FanoutRingBuffer.evaluateWatchdog`, chỉ đánh giá các subscriber đang có backlog tồn đọng trong buffer (`b.nextSeq-sub.ackedSeq >= 2 || b.streamEOF`) và gửi `DataAck` mỗi chunk để các receiver nhanh đã bắt kịp đầu luồng không bị phạt nhầm bởi thời gian sender tạm dừng do backpressure của receiver chậm.
  - Trong `fanout_sender.go`, đánh dấu `p.completedAll = true` và `p.status = "completed"` ngay khi nhận `TypeFinished` để sự kiện đóng socket `FanoutOpPeerLeft` sau đó không ghi đè trạng thái thành `"disconnected"`.
- Kết quả kiểm thử `-race` (`8.169s`) & cross-compile:
  - `TestFanoutAllFastReceivers` — PASS (`0.17s`)
  - `TestFanoutBoundedBufferBackpressure` — PASS (`2.46s`)
  - `TestFanoutKickSlowReceiverAndResume1to1` — PASS (`0.75s`)
  - `TestFanoutExceptions` (`E6`, `E7`, `E8`, `E9`, `E10`) — PASS (`2.94s`)
  - Cross-compile `darwin/arm64`, `linux/amd64`, `linux/amd64 -tags croc_no_tailcat` — PASS.
- **Dừng lại sau Phase 3** theo đúng chỉ thị của người dùng, chờ lệnh bắt đầu **Phase 4 (Tích hợp Phần 1 Dual-Relay + Phần 2 Fan-out 1→N)**.
## 2026-11-22 02:22 – 02:38: Thực hiện & Hoàn tất Phase 4 (Tích hợp Phần 1 Dual-Relay + Phần 2 Fan-out 1→N)
- Đã cập nhật:
  - [fanout.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/fanout.go) & [fanout_buffer.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/tcp/fanout_buffer.go) — Cho phép đăng ký kênh data fan-out phụ trên Relay B trước khi khoá lobby và mở lại `streamEOF = false` trên Relay A khi đẩy bù các chunk failover từ Relay B.
  - [chunk_bitmap.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/chunk_bitmap.go) & [chunk_scheduler.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/chunk_scheduler.go) — Bổ sung `chunkBitmap.ApplyAck` và `reliableChunkScheduler.AckSingle`.
  - [fanout_sender.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/fanout_sender.go) & [fanout_receiver.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/fanout_receiver.go) — Hỗ trợ `croc send --receivers N --dual-relay`: tự động probe Relay B từ `--relay2` / `RelayMeta.Peers`, chia tải chunk mã hoá bằng `contentKey` qua 2 relay fan-out theo `--chunk-policy dynamic|parity`, cộng tín dụng tốc độ tổng hợp giữa 2 relay, đồng bộ sự kiện kick receiver chậm trên cả 2 relay, và tự động đẩy bù chunk thiếu qua Relay A khi Relay B chết giữa chừng (`pushSpecificFanoutChunks`).
  - [fanout_test.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/croc/fanout_test.go) — Bổ sung `TestCombinedDualRelayFanout` với 2 kịch bản E2E phức tạp.
- Kết quả kiểm thử `-race` (`3.575s`) & cross-compile:
  - `TestCombinedDualRelayFanout/parallel_distribution_and_synchronized_kick` — PASS (`0.82s`)
  - `TestCombinedDualRelayFanout/relay_b_dies_mid_transfer_plus_slow_receiver_kick` — PASS (`0.92s`)
  - Cross-compile `darwin/arm64`, `linux/amd64`, `linux/amd64 -tags croc_no_tailcat` — PASS.
- **Dừng lại sau Phase 4** theo đúng chỉ thị của người dùng, chờ lệnh bắt đầu **Phase 5 (Cross-Arch Build, Docker + `tc` Benchmark, Demo Cloudflare & Hoàn thiện Báo cáo)**.
## 2026-11-22 02:55 – 03:40: Thực hiện & Hoàn tất Phase 5 (Cross-Arch Verification, Docker Simulation & 6 Kịch bản Thực nghiệm)
- Đã kiểm chứng biên dịch thuần Go (`CGO_ENABLED=0 -tags netgo,osusergo`) cho toàn bộ 6 tổ hợp hệ điều hành/kiến trúc (`darwin/arm64`, `linux/arm64`, `linux/amd64`, `linux/386`, `windows/amd64`, `linux/amd64 -tags croc_no_tailcat`) — tất cả PASS.
- Đã xây dựng bộ hạ tầng mô phỏng Docker + Native macOS CLI trong `/Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/`:
  - [Dockerfile](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/Dockerfile), [docker-compose.yml](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/docker-compose.yml), [entrypoint.sh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/entrypoint.sh), [tc_shape.sh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/tc_shape.sh), [build_and_up.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/build_and_up.zsh), [mac_cli.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/mac_cli.zsh).
- Đã xây dựng và chạy kiểm nghiệm thành công 100% cả 6 kịch bản trong `/Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/`:
  - [01_dual_relay_bandwidth.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/01_dual_relay_bandwidth.zsh) (Single vs Dual Symmetric & Asymmetric `dynamic` vs `parity` với Linux `tc` HTB) — PASS.
  - [02_dual_relay_failover.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/02_dual_relay_failover.zsh) (Tắt nóng `croc-sim-relay-b` giữa chừng, failover không gián đoạn) — PASS.
  - [03_cloudflare_tunnel_mode_b.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/03_cloudflare_tunnel_mode_b.zsh) (Primary Relay A chạy trực tiếp trên máy Mac thật + Secondary Relay B qua Cloudflare Quick Tunnel thật `wss://*.trycloudflare.com/ws`) — PASS.
  - [04_fanout_bounded_buffer.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/04_fanout_bounded_buffer.zsh) (1→4 Fan-out file 16 MiB với 4 MiB bounded relay buffer) — PASS.
  - [05_fanout_kick_and_resume.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/05_fanout_kick_and_resume.zsh) (Kick receiver chậm < 256 KiB/s, giữ `.partial` + `.crocmap`, resume phiên 1-1 khớp 100% SHA-256) — PASS.
  - [06_combined_dual_relay_fanout.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/06_combined_dual_relay_fanout.zsh) (Kết hợp Dual-Relay + 1→3 Fan-out + tắt nóng Relay B + kick Receiver #3 chậm) — PASS.
  - [run_all_scenarios.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/run_all_scenarios.zsh) & [README.md](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/README.md).
- Đã tạo báo cáo tổng kết thực nghiệm & hướng dẫn vận hành [05_benchmark_and_walkthrough.md](file:///Users/huyle/Documents/dev/LTM/croc/croc/multiple_croc_agent_doc/05_benchmark_and_walkthrough.md).
