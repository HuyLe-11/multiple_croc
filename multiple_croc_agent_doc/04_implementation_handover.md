# Trạng thái Cài đặt & Hồ sơ Bàn giao Agent (Implementation Handover & Checkpoint)

> **Mục đích**: File này được cập nhật **ngay trước và ngay sau mỗi bước cài đặt** để nếu phiên làm việc bị ngắt (hết token/quota), bất kỳ coding agent độc lập nào đọc file này cùng `00_process_log.md` và `03_implementation_plan.md` đều biết chính xác đang ở đâu, file nào đã sửa, test nào đã chạy, và bước tiếp theo là gì.

---

## 1. Bảng tiến độ tổng thể các Phase

| Phase | Mô tả | Trạng thái | Commit / File đã chạm | Kết quả Test |
|---|---|---|---|---|
| **Phase 0** | Nền tảng & Môi trường (`go mod download`, baseline tests, cross-compile `darwin/arm64` & `linux/amd64`) | ✅ `COMPLETED` | Chưa sửa code nguồn | `src/comm` (`1.112s`), `src/tcp` (`5.709s`), `src/message` (`0.752s`) PASS; build `darwin/arm64` (31 MB) & `linux/amd64` (33 MB) PASS |
| **Phase 1** | WebSocket Transport (`src/comm/ws.go`, `src/tcp/ws_bridge.go`) & Peer Metadata (`src/tcp/relay_meta.go`, `parts[3]`, `--peers`, `--ws-listen`) | ✅ `COMPLETED` | `[NEW]` `src/comm/ws.go`, `src/comm/ws_test.go`, `src/tcp/relay_meta.go`, `src/tcp/ws_bridge.go`, `src/tcp/ws_bridge_test.go`; `[MODIFY]` `src/comm/comm.go`, `src/tcp/options.go`, `src/tcp/tcp.go`, `src/croc/relay_dial.go`, `src/croc/croc.go`, `src/cli/cli.go` | `TestNormalizeAndBuildWebSocketURL`, `TestRelayMetaBackwardsCompatibility`, `TestWebSocketBridgeAndClientDial` (`-race`) PASS; cross-build `darwin/arm64` & `linux/amd64` PASS |
| **Phase 2** | Phần 1: Dual-Relay Parallel Transfer (`chunk_bitmap.go`, `chunk_scheduler.go` dynamic+parity, `dual_relay.go`, Control Failover) | ✅ `COMPLETED` | `[NEW]` `src/croc/chunk_bitmap.go`, `src/croc/chunk_scheduler.go`, `src/croc/dual_relay.go`, `src/croc/dual_relay_test.go`; `[MODIFY]` `src/message/message.go`, `src/croc/chunk_queue.go`, `src/croc/tailcat_negotiation.go`, `src/croc/bundle.go`, `src/croc/croc.go`, `src/cli/cli.go` | Toàn bộ 6 bộ test `-race` của Phase 2 (`TestChunkBitmapAndSidecarPortability`, `TestReliableChunkSchedulerDynamicAndParity`, `TestDualRelayDynamicAndParityTransfer`, `TestDualRelayFallbackWhenPeerUnavailable`, `TestDualRelayFailoverSecondaryDiesMidTransfer`, `TestDualRelayFailoverPrimaryControlDiesMidTransfer`) PASS; cross-build `darwin/arm64` & `linux/amd64` PASS |
| **Phase 3** | Phần 2: 1→N Fan-out (`fanout_buffer.go`, `fanout.go`, `fanout_sender.go`, `fanout_receiver.go`, `.partial` + `.crocmap` resume 1-1) | ✅ `COMPLETED` | `[NEW]` `src/tcp/fanout_buffer.go`, `src/tcp/fanout.go`, `src/croc/fanout_sender.go`, `src/croc/fanout_receiver.go`, `src/croc/fanout_test.go`; `[MODIFY]` `src/tcp/defaults.go`, `src/tcp/options.go`, `src/tcp/tcp.go`, `src/croc/receive_preflight.go`, `src/croc/croc.go`, `src/cli/cli.go` | Toàn bộ bộ test `-race` của Phase 3 (`TestFanoutAllFastReceivers`, `TestFanoutBoundedBufferBackpressure`, `TestFanoutKickSlowReceiverAndResume1to1`, `TestFanoutExceptions/E6..E10`) PASS (`8.169s`); cross-build `darwin/arm64`, `linux/amd64`, `croc_no_tailcat` PASS |
| **Phase 4** | Tích hợp Phần 1 + Phần 2 (1→N qua 2 Relays + đồng bộ kick + failover) | ✅ `COMPLETED` | `[MODIFY]` `src/tcp/fanout.go`, `src/tcp/fanout_buffer.go`, `src/croc/chunk_bitmap.go`, `src/croc/chunk_scheduler.go`, `src/croc/fanout_sender.go`, `src/croc/fanout_receiver.go`, `src/croc/fanout_test.go` | `TestCombinedDualRelayFanout/parallel_distribution_and_synchronized_kick` (`0.82s`) & `TestCombinedDualRelayFanout/relay_b_dies_mid_transfer_plus_slow_receiver_kick` (`0.92s`) (`-race`) PASS; cross-build `darwin/arm64`, `linux/amd64`, `croc_no_tailcat` PASS |
| **Phase 5** | Cross-Arch Verification (`x86` & `arm64`), Docker Simulation (`test/simulation/docker` + `test/simulation/scenarios`), Demo Cloudflare Mode (b), hoàn thiện báo cáo | ✅ `COMPLETED` | `[NEW]` `test/simulation/docker/*`, `test/simulation/scenarios/*`, `multiple_croc_agent_doc/05_benchmark_and_walkthrough.md`; `[MODIFY]` `src/croc/chunk_scheduler.go`, `src/cli/cli.go`, `.gitignore` | Toàn bộ 6 tổ hợp cross-compile (`darwin/arm64`, `linux/arm64`, `linux/amd64`, `linux/386`, `windows/amd64`, `croc_no_tailcat`) PASS; toàn bộ 6 kịch bản mô phỏng Docker + `tc` + Cloudflare Quick Tunnel thật PASS 100% |

---

## 2. Quy tắc bắt buộc khi triển khai (Policy Checklist)

1. **Thực hiện từng Phase**: Kết thúc mỗi phase phải chạy test xác minh, cập nhật tài liệu handover, rồi **DỪNG LẠI** đợi chỉ thị của người dùng trước khi sang phase kế tiếp.
2. **Không làm bừa khi gặp vướng mắc**: Nếu gặp bất kỳ xung đột thiết kế hoặc lỗi không rõ ràng, phải dừng lại hỏi người dùng và chờ xác nhận.
3. **Đa kiến trúc (`darwin/arm64` + `linux/amd64` / `x86`)**:
   - Toolchain trên máy host: `/opt/homebrew/bin/go` (`go1.27.4 darwin/arm64`).
   - Toàn bộ code mới thuần Go (`CGO_ENABLED=0`), tường minh byte-order (`binary.LittleEndian` / `binary.BigEndian`), dùng typed atomics (`atomic.Int64`, `atomic.Uint64`, `atomic.Bool`), giữ tương thích build tag `-tags netgo,osusergo` và `-tags croc_no_tailcat`.
4. **Tương thích ngược 100%**: Không làm hỏng các test hiện có của `croc v11`.
5. **Comment trong code bằng tiếng Anh**, tài liệu trong `multiple_croc_agent_doc/` bằng tiếng Việt (giữ thuật ngữ kỹ thuật tiếng Anh).

---

## 3. Chi tiết bước đang thực hiện (Live Checkpoint)

- **Thời điểm**: 2026-11-22 03:40 (+07:00)
- **Trạng thái tổng thể**: **HOÀN TẤT TOÀN BỘ 5 PHASE (Phase 0 → Phase 5)** ✅
  1. **Cross-Architecture Verification (`CGO_ENABLED=0`)**:
     - Biên dịch thành công toàn bộ 6 tổ hợp: `darwin/arm64`, `linux/arm64`, `linux/amd64`, `linux/386`, `windows/amd64`, và `linux/amd64 -tags croc_no_tailcat`.
  2. **Hạ tầng Mô phỏng Docker + Native macOS CLI (`test/simulation/docker/`)**:
     - [Dockerfile](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/Dockerfile), [docker-compose.yml](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/docker-compose.yml), [entrypoint.sh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/entrypoint.sh), [tc_shape.sh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/tc_shape.sh), [build_and_up.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/build_and_up.zsh), [mac_cli.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/docker/mac_cli.zsh).
     - Cho phép máy Mac thật đóng vai **Sender**, **Receiver**, hoặc **Relay** tương tác trực tiếp với cụm 7 container (`croc-sim-relay-a`, `croc-sim-relay-b`, `croc-sim-sender`, `croc-sim-receiver-1..4`) với cùng bộ địa chỉ `127.0.0.1:9009`, `127.0.0.1:9019`, `ws://127.0.0.1:8080/ws`.
  3. **6 Kịch bản Thực nghiệm & Benchmark (`test/simulation/scenarios/`)**:
     - [01_dual_relay_bandwidth.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/01_dual_relay_bandwidth.zsh) — So sánh Single Relay vs Dual Relay đối xứng (`12+12 Mbps`) và bất đối xứng (`24+6 Mbps`, `dynamic` vs `parity` chẵn/lẻ) với Linux `tc` HTB — **PASS**.
     - [02_dual_relay_failover.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/02_dual_relay_failover.zsh) — Tắt nóng `croc-sim-relay-b` giữa chừng, tự động failover sang Relay A không đứt phiên — **PASS**.
     - [03_cloudflare_tunnel_mode_b.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/03_cloudflare_tunnel_mode_b.zsh) — Primary Relay A chạy trực tiếp trên máy Mac thật + Secondary Relay B đi qua Cloudflare Quick Tunnel thật (`wss://*.trycloudflare.com/ws`) — **PASS**.
     - [04_fanout_bounded_buffer.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/04_fanout_bounded_buffer.zsh) — 1→4 Fan-out file 16 MiB qua relay có buffer giới hạn 4 MiB, kiểm chứng backpressure giữ RAM ổn định — **PASS**.
     - [05_fanout_kick_and_resume.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/05_fanout_kick_and_resume.zsh) — Kick receiver chậm dưới ngưỡng `256 KiB/s`, bảo toàn `.partial` + `.crocmap`, và resume 1-1 hoàn tất 100% SHA-256 — **PASS**.
     - [06_combined_dual_relay_fanout.zsh](file:///Users/huyle/Documents/dev/LTM/croc/croc/test/simulation/scenarios/06_combined_dual_relay_fanout.zsh) — Kết hợp 1→3 Fan-out qua 2 Relay + tắt nóng Relay B giữa chừng + kick Receiver #3 chậm — **PASS**.
  4. **Báo cáo tổng kết**:
     - [05_benchmark_and_walkthrough.md](file:///Users/huyle/Documents/dev/LTM/croc/croc/multiple_croc_agent_doc/05_benchmark_and_walkthrough.md).
- **Lưu ý môi trường thực thi cho các Agent tiếp theo**:
  - `go build` chạy được trong sandbox với `export GOCACHE=/tmp/croc-go-build`.
  - `go test` của các gói có mở socket loopback `127.0.0.1` (`src/comm`, `src/tcp`, `src/croc`) cần chạy với `BypassSandbox: true` vì sandbox chặn `connect`/`listen` trên `127.0.0.1`.
