# Báo cáo nghiên cứu tài liệu chính thức & kiểm chứng dependency

> Ngày thực hiện: 2026-11-22  
> Mục tiêu: Xác minh kiến trúc `croc v11`, hành vi thực tế của các thư viện trong `go.mod`, giới hạn của Cloudflare Free/Tunnel, và công cụ benchmark `tc`/Docker từ tài liệu chính thức trước khi lập kế hoạch.

---

## 1. Kiến trúc `croc` chính thức & quan điểm upstream

### 1.1. Tổng quan giao thức `croc` (Relay, PAKE, Multiplexing, Resume)
- **Nguồn chính thức**:
  - Upstream repository & README nội bộ: https://github.com/schollz/croc ([README.md](file:///Users/huyle/Documents/dev/LTM/croc/croc/README.md))
  - Bài viết thiết kế chính thức của tác giả Zack Scholl (được dẫn trực tiếp tại `README.md:26`): https://schollz.com/tinker/croc6/
- **Thông tin tìm thấy**:
  - `croc` thiết kế theo mô hình **1-to-1** (1 sender ↔ 1 receiver) đi qua một relay trung gian để vượt NAT mà không cần port-forwarding.
  - Relay lắng nghe dải cổng TCP liên tiếp (mặc định `9009–9013`, tối thiểu 2 cổng): cổng đầu tiên (`9009`) làm **control channel** (tín hiệu, PAKE, metadata, danh sách cổng data trong `banner`), các cổng còn lại (`9010–9013`) làm **data channel** song song (multiplexing).
  - Bảo mật end-to-end (E2EE) dựa trên **PAKE** từ code phrase: relay chỉ ghép cặp hai socket trong cùng phòng (`room`), **không thấy** khoá phiên hay nội dung file.
  - Tự host relay bằng `croc relay` (có các giới hạn chống DoS: `max-pending-handshakes`, `max-pending-handshakes-per-source`, `max-rooms-open`, `source-join-limit`, `room-join-limit`).
  - Hỗ trợ **fast-admission** trên bản v11: cổng control cấp token HMAC có thời hạn 30 giây (`croc-fast-admission-v1:`) giúp các cổng data bỏ qua bước PAKE relay lặp lại; tự động fallback về PAKE relay cũ nếu relay không hỗ trợ (`src/tcp/capability.go`, `src/tcp/tcp.go:900-942`).
  - Hỗ trợ **data transport auto/Tailcat/DERP**: với file $\ge 128\text{ MiB}$ (`autoTailcatThresholdBytes`), chế độ `--transport auto` ưu tiên thử Tailcat/DERP trước khi fallback về relay (`README.md:483-496`, `src/croc/tailcat_negotiation.go:26`).
- **Áp dụng vào codebase**:
  - Mọi chế độ mới (Dual-Relay, Fan-out 1→N) phải **ép dùng transport relay** (tắt Tailcat/DERP cho phiên đó) để lưu lượng thực sự đi qua các relay tự host.
  - Phải giữ nguyên hành vi mặc định cho phiên 1-1 hiện có (tính năng mới là opt-in qua flag CLI).

### 1.2. Quan điểm upstream về Multi-Relay & Multi-Receiver
- **Nguồn chính thức (GitHub Issues `schollz/croc`)**:
  - Issue #441 (*Public Relay List* / thảo luận nhiều relay & song song): https://github.com/schollz/croc/issues/441
  - Issue #1040 (*Hosting and Relay Discussion*): https://github.com/schollz/croc/issues/1040
  - Issue #1269 (*croc needs your help* – giới thiệu pool public relay): https://github.com/schollz/croc/issues/1269
  - Issue #481 (hạn chế kiến trúc 1-to-1 / nhu cầu gửi nhiều người nhận): https://github.com/schollz/croc/issues/481
  - Issue #510 (xung đột cổng khi gửi song song nhiều phiên trên 1 máy): https://github.com/schollz/croc/issues/510
- **Thông tin tìm thấy**:
  - `croc v11` đã bổ sung pool 4 public relay (`1..4.getcroc.com:9009` tại `src/publicrelay/publicrelay.go`), nhưng **mỗi phiên chỉ chọn 1 relay duy nhất** dựa trên `SHA-256(code) mod len(relays)`. Chưa có cơ chế truyền 1 file đồng thời qua 2 relay.
  - `croc` gốc là 1-to-1 (phòng relay chỉ có `first` và `second`, client thứ 3 bị từ chối `"room full"` tại `src/tcp/tcp.go:122-124`). Để gửi nhiều người theo cách bất đồng bộ, v11 thêm `croc store` (lưu toàn bộ ciphertext lên đĩa HTTP server), **chưa có** chế độ live streaming 1→N với bounded RAM buffer trên relay.
- **Áp dụng vào codebase**:
  - Hai tính năng của đồ án là mở rộng giao thức mới, không xung đột với `publicrelay` hay `store`.

---

## 2. Thư viện Go đã cài đặt trong `go.mod`

### 2.1. `github.com/schollz/pake/v3` (`v3.2.1`) & `src/pakekey`
- **Nguồn chính thức**:
  - Upstream repo: https://github.com/schollz/pake
  - Code tích hợp trong repo: [pakekey.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/pakekey/pakekey.go)
- **Thông tin tìm thấy**:
  - `croc v11` không gọi `pake.InitCurve` trực tiếp cho peer-to-peer nữa mà gọi `pake.InitCurveWithIdentities(password, role, curve, idA, idB)` qua `pakekey.Init(...)`:
    - `role = 0` luôn là **receiver** (Party A, khởi xướng).
    - `role = 1` luôn là **sender** (Party B, phản hồi).
    - `idA` và `idB` được gắn ngữ cảnh `(version=2, purpose, room, "receiver"/"sender")` và trộn vào dẫn xuất khoá phiên SHA-256 (`github.com/schollz/pake/v3/identity-bound-session-key/v1`).
  - Sau khi `Update()` và `SessionKey()`, `pakekey.Derive(...)` dùng HKDF-SHA-256 mở rộng thành `EncryptionKey` (32 B), `ConfirmationA` (32 B), `ConfirmationB` (32 B) ràng buộc vào toàn bộ transcript `(Initiator, Responder, Salt)`.
  - Mỗi đối tượng `*pake.Pake` là **stateful 2 bên** — không thể tái sử dụng một instance cho nhiều receiver.
- **Áp dụng vào codebase**:
  - Với Phần 2 (1 sender → N receiver): Sender tạo **N instance `pakekey.Init` độc lập** (1 cho mỗi `receiverID = 1..N`), có thể dùng `room = "<baseRoom>:r<id>"` hoặc dùng chung `baseRoom` vì mỗi phiên có `(Initiator_k, Responder_k, Salt_k)` riêng biệt dẫn xuất ra `pairKey[k]` hoàn toàn độc lập! Sau khi xác nhận `pairKey[k]`, sender mã hoá `contentKey` (32 B ngẫu nhiên chung của nhóm) bằng `pairKey[k]` gửi cho từng receiver.

### 2.2. `github.com/coder/websocket` (`v1.8.15`)
- **Nguồn chính thức**:
  - Go package docs: https://pkg.go.dev/github.com/coder/websocket
  - Upstream source (`netconn.go`, `conn.go`): https://github.com/coder/websocket
  - Cách dùng sẵn có trong repo: [webrelay.go](file:///Users/huyle/Documents/dev/LTM/croc/croc/src/webrelay/webrelay.go#L329-L394)
- **Thông tin tìm thấy**:
  - `websocket.Dial(ctx, urlStr, opts *DialOptions) (*Conn, *http.Response, error)`: hỗ trợ `DialOptions.HTTPClient` (có thể gán proxy `SOCKS5` / `HTTP_PROXY`!), `HTTPHeader`, `Subprotocols`.
  - `websocket.Accept(w, r, opts *AcceptOptions) (*Conn, error)`: nâng cấp HTTP lên WebSocket; `Conn.SetReadLimit(limit)` mặc định là **32,768 bytes (32 KiB)** — nếu frame lớn hơn sẽ đóng kết nối với `StatusMessageTooBig`! (`webrelay.go:33` đặt `maxWebSocketMessage = 65 * 1024 * 1024`).
  - `websocket.NetConn(ctx, c, websocket.MessageBinary) net.Conn`: tự đặt `SetReadLimit(-1)` và bọc `*websocket.Conn` thành `net.Conn`.
  - **CẢNH BÁO HÀNH VI ĐẶC BIỆT (CRITICAL)** từ tài liệu chính thức `coder/websocket` (`netconn.go`):
    > *"When a deadline is hit and there is an active read or write goroutine, the connection will be closed. This is different from most `net.Conn` implementations where only the reading/writing goroutines are interrupted but the connection is kept alive."*
    - Ngoài ra, `ctx` truyền vào `websocket.NetConn(ctx, ...)` quyết định **toàn bộ vòng đời** kết nối: nếu truyền `dialCtx` (hết hạn sau 30 s) hoặc `r.Context()` của HTTP request thì kết nối sẽ bị ngắt sớm!
  - `Conn.Ping(ctx)`: gửi WebSocket Ping control frame và chờ Pong (dùng làm keepalive chống idle timeout).
- **Áp dụng vào codebase**:
  - **Phía Relay server (`--ws-listen`)**: KHÔNG đưa trực tiếp `websocket.NetConn` vào `tcp.server.serve()`! Lý do: `src/tcp/tcp.go:521-533` dùng `ReceiveWithDeadlineLimit(5ms)` để thăm dò `earlyRoom` (timeout là hành vi bình thường cho legacy client). Nếu đưa `websocket.NetConn` trực tiếp vào đó, bất kỳ client nào chậm > 5 ms sẽ bị `coder/websocket` **giết vĩnh viễn kết nối**. Thay vào đó, HTTP/WS handler trên relay sẽ **bridge** `websocket.NetConn(streamCtx, socket, MessageBinary)` ↔ kết nối TCP loopback tới cổng TCP của chính relay đó (y hệt mẫu đã kiểm chứng của `src/webrelay/webrelay.go:362-393`).
  - **Phía Client CLI (`comm.NewConnectionContext`)**: khi địa chỉ có scheme `ws://` hoặc `wss://`:
    1. Dùng `dialCtx` (có timeout) cho `websocket.Dial`, nhưng dùng **`streamCtx := context.Background()`** (chỉ cancel khi `Close()`) cho `websocket.NetConn`.
    2. Chạy goroutine gửi `socket.Ping` mỗi **30 giây** để giữ kết nối sống qua Cloudflare proxy.
    3. Không gọi timeout ngắn không gây tử vong (non-fatal `SetReadDeadline`) trên kết nối WebSocket của client (kiểm tra đã xác nhận: `MeasureServerLatencyContext` và `receiveControlFrame` đều coi timeout là lỗi kết thúc kết nối đó).

### 2.3. `golang.org/x/time/rate` (`v0.16.0`) & Go Toolchain (`1.27.3`)
- **Nguồn chính thức**:
  - `x/time/rate`: https://pkg.go.dev/golang.org/x/time/rate
  - `go.mod` & `Dockerfile` trong repo (`go 1.27.3`, `golang:1.27.3-alpine`).
- **Thông tin tìm thấy**:
  - `rate.NewLimiter`, `ReserveN`, `WaitN` tồn tại ổn định trong `v0.16.0` và đã được dùng trong `src/croc/croc.go:468,3889` cho `--throttleUpload`.
  - Codebase dùng tính năng Go 1.25+ (`sync.WaitGroup.Go`, `strings.SplitSeq`, `errors.AsType`, `min`/`max` builtin).
  - **Môi trường máy host (cập nhật 00:35)**: Đã xác minh `/opt/homebrew/bin/go` (`go version go1.27.4 darwin/arm64`), `docker` (`29.6.2`), và `cloudflared` (`2026.10.1`) trên macOS Apple Silicon (`arm64`). Module cache cục bộ chưa tải trước (`go mod download` sẽ chạy ở Phase 0). Vì các relay cần chạy được trên cả **x86 (`linux/amd64`, tùy chọn `linux/386`/`windows/amd64`)** lẫn **ARM64 (`darwin/arm64`, `linux/arm64`)**, toàn bộ code mới giữ thuần Go (`CGO_ENABLED=0`, `-tags netgo,osusergo` và tương thích build tag `croc_no_tailcat`), tường minh endianness trên wire/sidecar, và kiểm chứng cross-compile + Docker multi-arch.

---

## 3. Cloudflare Free & Cloudflare Tunnel cho Mode (b)

| Hạng mục | Nguồn chính thức | Thông tin tìm thấy | Áp dụng vào thiết kế |
|---|---|---|---|
| **TCP proxy (Spectrum)** | https://developers.cloudflare.com/spectrum/ | Spectrum **không có trên gói Free**; proxy TCP tuỳ ý yêu cầu gói **Enterprise**. | Không thể expose cổng TCP `9009–9013` trực tiếp qua Cloudflare proxy miễn phí. |
| **Tunnel Arbitrary TCP** | https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/use-cases/ | Tunnel TCP (`tcp://`) yêu cầu **client cũng phải cài `cloudflared`** và chạy `cloudflared access tcp`. | Bị loại vì UX kém cho receiver (đã thống nhất chọn WebSocket ở vòng hỏi đáp 2). |
| **WebSockets trên Cloudflare** | https://developers.cloudflare.com/network/websockets/ | WebSockets được hỗ trợ trên **mọi gói, kể cả Free**, cho cả proxy DNS và Cloudflare Tunnel. | Expose relay B qua WebSocket (`wss://<host>/ws?port=<p>`), client CLI dùng `coder/websocket` quay số trực tiếp — **receiver không cần cài `cloudflared`**. |
| **Quick Tunnels (`trycloudflare.com`)** | https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/do-more-with-tunnels/trycloudflare/ | Chạy bằng `cloudflared tunnel --url http://localhost:PORT`; miễn phí, không cần tài khoản; hỗ trợ WebSocket; giới hạn **200 in-flight requests** (vượt trả `429`), không SSE, URL ngẫu nhiên đổi mỗi lần chạy. | Phù hợp cho demo đồ án: relay B chạy với `--ws-listen 127.0.0.1:8080`, mở 1 quick tunnel ra `https://xxx.trycloudflare.com`, đưa `wss://xxx.trycloudflare.com` vào `--peers` của Relay A (hoặc `--relay2`). Mỗi phiên transfer chỉ mở ~4–5 kết nối WS, rất xa ngưỡng 200. |
| **Idle Timeout (~100 s)** | https://developers.cloudflare.com/fundamentals/reference/http-request-headers/ & tài liệu proxy timeout Cloudflare | Kết nối WebSocket qua proxy Cloudflare bị ngắt nếu **idle ~100 giây** không có frame nào (không chỉnh được ở gói Free). | Bắt buộc có **WebSocket Ping mỗi 30 giây** trên cả client và relay WS bridge (đặc biệt quan trọng khi lobby chờ N receiver hoặc khi receiver chờ sender chuẩn bị hash). |
| **Giới hạn kích thước & Điều khoản (ToS)** | https://www.cloudflare.com/service-specific-terms-application-services/ | Giới hạn upload HTTP thường 100 MB trên Free/Pro; khung WS của croc chỉ ~32 KiB nên không đụng giới hạn frame. Tuy nhiên điều khoản CDN/Application Services hạn chế dùng gói Free để phân phối lượng lớn nội dung không phải HTML. | Ghi chú rõ trong tài liệu đồ án: chỉ dùng Cloudflare Free Tunnel cho **demo/benchmark quy mô nhỏ (vài chục–vài trăm MB)**, không dùng cho production CDN. |

---

## 4. Công cụ Benchmark tái lập (`tc` + Docker Compose)

- **Nguồn chính thức**:
  - Linux `tc-tbf(8)`: https://man7.org/linux/man-pages/man8/tc-tbf.8.html
  - Linux `tc-netem(8)`: https://man7.org/linux/man-pages/man8/tc-netem.8.html
  - Docker Compose `cap_add`: https://docs.docker.com/reference/compose-file/container-spec/
- **Thông tin tìm thấy**:
  - `tc qdisc add dev eth0 root tbf rate 10mbit burst 32kbit latency 400ms` giới hạn chính xác băng thông egress của từng container relay hoặc receiver (Token Bucket Filter).
  - `tc qdisc add dev eth0 root netem delay 20ms rate 10mbit` cho phép mô phỏng đồng thời độ trễ WAN + giới hạn băng thông.
  - Để chạy `tc` bên trong container Docker mà không cần `--privileged`, chỉ cần cấp `cap_add: [NET_ADMIN]` trong `docker-compose.yml` và cài gói `iproute2` trong image benchmark.
- **Áp dụng vào thiết kế**:
  - Tạo bộ `bench/docker-compose.yml` riêng (không làm phình `Dockerfile` chính) mô phỏng:
    - **Thí nghiệm 1 (Dual-Relay)**: Relay A bị giới hạn 10 Mbps, Relay B bị giới hạn 10 Mbps → so sánh 1 relay (~10 Mbps) vs 2 relay động (~19 Mbps) vs 2 relay chẵn/lẻ khi bất đối xứng (A = 20 Mbps, B = 5 Mbps) và khi kill Relay B giữa chừng!
    - **Thí nghiệm 2 (Fan-out 1→N)**: 1 sender → 4 receiver, buffer relay 16 MiB, 1 receiver bị bóp tốc độ bằng `tc tbf`: (a) trên ngưỡng (500 KB/s > 256 KB/s) để chứng minh backpressure + RAM ổn định ≤ 16 MiB; (b) dưới ngưỡng (100 KB/s < 256 KB/s) để chứng minh kick đúng sau ~10 s + 3 receiver còn lại tăng tốc ngay + receiver bị kick resume 1-1 thành công!

---

## 5. Tài liệu không tìm thấy / Điểm chưa thể xác minh trực tiếp

Theo yêu cầu nghiên cứu trung thực (không tự bịa hành vi khi thiếu tài liệu):
1. **Giới hạn số kết nối WebSocket đồng thời trên Cloudflare Free**: Tài liệu Cloudflare không công bố con số trần cứng công khai (chỉ nêu "guidelines" thay vì hard block). Với đồ án (tối đa vài chục kết nối WS), hoàn toàn nằm trong mức bình thường.
2. **Kích thước tối đa 1 WebSocket message qua Cloudflare Tunnel (không qua Workers)**: Tài liệu chính thức ghi rõ giới hạn 32 MiB cho Workers/Durable Objects và 100 MB cho HTTP request body; không có số riêng cho Tunnel proxy thuần. Thiết kế của ta gửi các chunk ~32 KiB (tối đa 64 KiB/frame) nên an toàn tuyệt đối.
3. **Go module cache cục bộ trên macOS host**: Người dùng đã cài `go1.27.4 darwin/arm64` tại `/opt/homebrew/bin/go`, nhưng chưa chạy `go mod download` vào `/Users/huyle/go/pkg/mod` (sandbox không có mạng để tải module trong bước lập kế hoạch); mọi chữ ký hàm của `coder/websocket` v1.8.15, `schollz/pake/v3` v3.2.1 và `golang.org/x/time/rate` v0.16.0 đều đã được xác minh qua cả **code đang gọi trực tiếp trong repo** (`src/webrelay/webrelay.go`, `src/pakekey/pakekey.go`, `src/croc/croc.go`) lẫn **tài liệu chính thức trên `pkg.go.dev` và GitHub upstream**, và sẽ kiểm tra biên dịch trực tiếp ở Phase 0 sau `go mod download`.
4. **Đặc tả upstream cho Multi-Relay & Fan-out**: `schollz/croc` không có đặc tả chính thức nào cho 2 tính năng này; toàn bộ định dạng khung tin và thông điệp mở rộng trong kế hoạch là thiết kế mới cho đồ án, có namespace/version rõ ràng và tương thích ngược.
