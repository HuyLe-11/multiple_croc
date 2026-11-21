# Hướng dẫn Mô phỏng & Thực nghiệm Hệ thống Multi-Relay & 1-to-N Fan-out (`test/simulation`)

Thư mục này cung cấp môi trường giả lập sát thực tế nhất bằng **Docker + Linux Traffic Control (`tc`) + Native macOS CLI**, cho phép chiếc máy Mac thật của bạn đóng bất kỳ vai trò nào (**Sender**, **Receiver**, hoặc **Relay**) và tương tác trực tiếp với các container Docker đóng vai các nút mạng còn lại.

---

## 1. Cấu trúc thư mục

- `test/simulation/docker/`:
  - `Dockerfile`: Alpine Linux nhẹ có sẵn `iproute2` (`tc`), `socat`, `zsh`, `bash`, `curl`, `procps` và binary `croc` Linux.
  - `docker-compose.yml`: Gồm 7 container:
    - `croc-sim-relay-a`: Primary Relay A (`127.0.0.1:9009..9013`, quảng bá peer `127.0.0.1:9019,ws://127.0.0.1:8080/ws`).
    - `croc-sim-relay-b`: Secondary Relay B (`127.0.0.1:9019..9023` + WebSocket Bridge `ws://127.0.0.1:8080/ws`).
    - `croc-sim-sender` & `croc-sim-receiver-1..4`: Các nút mạng độc lập có `cap_add: [NET_ADMIN]` để áp đặt giới hạn băng thông/độ trễ bằng `tc`.
  - `build_and_up.zsh`: Biên dịch nhanh `croc-mac` (`darwin/arm64`), `croc-linux-arm64`, `croc-linux-amd64` trong ~2–3 giây và bật cụm Docker.
  - `mac_cli.zsh`: Công cụ giúp bạn chạy `croc` trực tiếp trên terminal Mac thật ở vai trò **Relay A**, **Relay B**, **Sender**, hoặc **Receiver**.
- `test/simulation/scenarios/`:
  - `01_dual_relay_bandwidth.zsh`: So sánh 1 Relay vs 2 Relay (`dynamic` vs `parity` chẵn/lẻ) dưới giới hạn băng thông đối xứng (`12+12 Mbps`) và bất đối xứng (`24+6 Mbps`).
  - `02_dual_relay_failover.zsh`: Đang truyền qua 2 relay thì tắt nóng `croc-sim-relay-b` (`docker stop`), kiểm chứng tự động chuyển mạch không đứt phiên.
  - `03_cloudflare_tunnel_mode_b.zsh`: Chạy **Primary Relay A trực tiếp trên máy Mac thật** + **Secondary Relay B đi qua Cloudflare Quick Tunnel thật (`wss://....trycloudflare.com/ws`)**.
  - `04_fanout_bounded_buffer.zsh`: Truyền 1→4 receivers với file `16 MiB` qua relay có buffer giới hạn `4 MiB`, kiểm chứng backpressure giữ RAM relay không vượt ngưỡng.
  - `05_fanout_kick_and_resume.zsh`: Bóp băng thông `receiver-3` xuống `600 kbit/s` (`~75 KiB/s < 256 KiB/s`) để quan sát relay tự động **kick** `receiver-3`, giữ lại `<file>.partial` + `<file>.partial.crocmap`, sau đó mở khoá băng thông và chạy phiên 1-1 resume hoàn tất 100%.
  - `06_combined_dual_relay_fanout.zsh`: Kết hợp đồng thời 1→3 receivers qua 2 Relay + tắt nóng Relay B giữa chừng + kick Receiver #3 chậm.
  - `run_all_scenarios.zsh`: Chạy tự động toàn bộ 6 kịch bản hoặc mở menu tương tác (`./run_all_scenarios.zsh --menu`).

---

## 2. Khởi động nhanh

### Bước 1: Build binary (Mac + Linux) và bật cụm Docker
```zsh
./test/simulation/docker/build_and_up.zsh
```
*(Nếu bạn muốn **máy Mac thật** tự chạy Primary Relay A trên terminal Mac thay vì dùng container `relay-a`, chạy:)*
```zsh
./test/simulation/docker/build_and_up.zsh --mac-relay
./test/simulation/docker/mac_cli.zsh relay-a
```

### Bước 2: Chạy thử các kịch bản ở chế độ Tự động (`--auto`) hoặc Tự gõ lệnh tay (`--interactive`)
```zsh
# Mở menu chọn kịch bản và chế độ (Auto hoặc Interactive):
./test/simulation/scenarios/run_all_scenarios.zsh --menu

# Hoặc chạy từng kịch bản cụ thể:
./test/simulation/scenarios/01_dual_relay_bandwidth.zsh --auto
./test/simulation/scenarios/01_dual_relay_bandwidth.zsh --interactive
```

---

## 3. Tự tay đóng vai Sender / Receiver / Relay trên terminal Mac thật

Nhờ thiết kế cầu nối đồng nhất địa chỉ trong `entrypoint.sh`, mọi địa chỉ relay (`127.0.0.1:9009`, `127.0.0.1:9019`, `ws://127.0.0.1:8080/ws`) hoạt động **giống hệt nhau** dù bạn gõ lệnh trên **terminal Mac thật** hay bên trong **bất kỳ container Docker nào**!

### Ví dụ A: Máy Mac làm Sender (gửi qua 2 Relay song song) → Docker `receiver-1` làm Receiver
- **Cửa sổ Terminal 1 (trên Mac thật — đóng vai Sender)**:
  ```zsh
  ./test/simulation/docker/mac_cli.zsh send --dual-relay --chunk-policy dynamic --code my-test-code /path/to/file
  ```
- **Cửa sổ Terminal 2 (đóng vai Receiver trong Docker `receiver-1`)**:
  ```zsh
  docker exec -it croc-sim-receiver-1 sh -c 'cd /shared/receiver-1 && CROC_SECRET=my-test-code croc --no-local --yes --overwrite --relay 127.0.0.1:9009'
  ```

### Ví dụ B: Docker `sender` làm Sender → Máy Mac thật làm Receiver #1 + Docker `receiver-2` làm Receiver #2 (1→2 Fan-out)
- **Cửa sổ Terminal 1 (Sender trong Docker)**:
  ```zsh
  docker exec -it croc-sim-sender sh -c 'croc --no-local --relay 127.0.0.1:9009 send --dual-relay --receivers 2 --code group-code /shared/sender/scenario1_payload.bin'
  ```
- **Cửa sổ Terminal 2 (trên Mac thật — đóng vai Receiver #1)**:
  ```zsh
  ./test/simulation/docker/mac_cli.zsh recv group-code --fanout
  ```
- **Cửa sổ Terminal 3 (Receiver #2 trong Docker)**:
  ```zsh
  docker exec -it croc-sim-receiver-2 sh -c 'cd /shared/receiver-2 && CROC_SECRET=group-code croc --no-local --yes --overwrite --fanout --relay 127.0.0.1:9009'
  ```
