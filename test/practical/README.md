# Hướng dẫn Test Thực tế trên 3 Máy Thật (`test/practical`)

Thư mục này chứa bộ công cụ và script để bạn chạy thử nghiệm trực tiếp trên **3 máy thật** (hỗ trợ hỗn hợp **macOS Apple Silicon/Intel**, **Linux x86_64/ARM64**, **Windows**, hoặc **Android Termux**).

---

## Bước 0: Build binary và phân phối sang 2 máy còn lại (chỉ làm 1 lần)

1. **Build toàn bộ binary đa nền tảng** (trên máy Mac này):
   ```zsh
   ./test/practical/build_all.zsh
   ```
2. **Phát binary qua HTTP trên mạng LAN/Wi-Fi** để 2 máy kia tải về trong 2 giây:
   ```zsh
   ./test/practical/serve_binaries.zsh
   ```
   Script sẽ tự in sẵn câu lệnh `curl -LO http://<IP_MAC>:8888/croc-...` tương ứng cho Linux, macOS, Android Termux và Windows. Tải xong trên 2 máy kia, nhấn `Ctrl+C` để tắt HTTP server.
   *(Lưu ý: Trên các máy Linux/macOS, bạn có thể copy kèm các file `.sh` trong `test/practical/` hoặc gõ trực tiếp lệnh bên dưới).*

---

## Kịch bản A: Dual-Relay trên 3 máy thật cùng mạng Wi-Fi / LAN (hoặc 2 VPS)

Phân vai 3 máy:
- **Máy 1 (`IP_1`, ví dụ máy Mac của bạn)**: Chạy **Primary Relay A** + **Sender**
- **Máy 2 (`IP_2`, máy Linux/Mac/VPS thứ 2)**: Chạy **Secondary Relay B**
- **Máy 3 (máy thứ 3)**: Chạy **Receiver**

### 1. Trên Máy 2 (Secondary Relay B):
```bash
./machine2_relay_b.sh direct
# (Hoặc chạy trực tiếp: ./croc relay --host 0.0.0.0 --ports 9019,9020,9021,9022,9023 --ws-listen 0.0.0.0:8080)
```

### 2. Trên Máy 1 (Primary Relay A — Terminal 1):
Truyền địa chỉ của Máy 2 vào `--peers`:
```bash
./machine1_relay_a.sh <IP_MAY_2>:9019
# (Hoặc chạy trực tiếp: ./croc relay --host 0.0.0.0 --ports 9009,9010,9011,9012,9013 --peers <IP_MAY_2>:9019)
```

### 3. Trên Máy 1 (Sender — Terminal 2):
Gửi file `50 MiB` (hoặc truyền đường dẫn file thật thay cho `50`):
```bash
./sender_run.sh 127.0.0.1:9009 demo-3-may 50 dual dynamic
# Để so sánh với chế độ chia chẵn/lẻ (parity):
./sender_run.sh 127.0.0.1:9009 demo-3-may 50 dual parity
```

### 4. Trên Máy 3 (Receiver):
```bash
./receiver_run.sh <IP_MAY_1>:9009 demo-3-may normal
```
> **Test Failover trên 3 máy thật**: Trong lúc Máy 3 đang nhận file ở ~40%, hãy nhấn `Ctrl+C` trên **Máy 2 (Relay B)**. Quan sát Máy 3 vẫn nhận đủ 100% file qua Relay A trên Máy 1 và khớp SHA-256!

---

## Kịch bản B: Mode (b) Khác mạng Internet — Relay B sau NAT qua Cloudflare Free Tunnel

Phù hợp khi **Máy 2 nằm sau NAT/Router gia đình không mở port** và **Máy 3 dùng 4G/5G hoặc mạng khác**:
1. **Trên Máy 2** (cần cài `cloudflared`):
   ```bash
   ./machine2_relay_b.sh cloudflare
   ```
   Script sẽ in ra địa chỉ công cộng dạng: `wss://xxxx-yyyy.trycloudflare.com/ws`.
2. **Trên Máy 1 (Primary Relay A)** — quảng bá URL Cloudflare đó làm peer:
   ```bash
   ./machine1_relay_a.sh wss://xxxx-yyyy.trycloudflare.com/ws
   ```
3. **Trên Máy 1 (Sender)** & **Máy 3 (Receiver)**: Chạy giống hệt Kịch bản A! Nếu Máy 3 ở mạng ngoài (4G), nhớ mở port `9009..9013` trên router cho Máy 1 (hoặc dùng thêm Cloudflare Tunnel cho Máy 1).

---

## Kịch bản C: Fan-out 1→2 + Bounded Buffer + Kick Receiver chậm & Resume 1-1 trên 3 máy thật

Phân vai 3 máy:
- **Máy 1 (`IP_1`)**: **Primary Relay A** + **Sender (`--receivers 2`)**
- **Máy 2**: **Receiver #1 (Nhanh)**
- **Máy 3**: **Receiver #2 (Chậm — giả lập bị nghẽn xuống `76.8 KB/s < 256 KB/s`)**

1. **Trên Máy 1 (Terminal 1 — Relay A)**:
   ```bash
   ./machine1_relay_a.sh ""
   ```
2. **Trên Máy 1 (Terminal 2 — Sender phát 1 lần cho 2 máy nhận)**:
   ```bash
   ./sender_run.sh 127.0.0.1:9009 fanout-3-may 30 fanout dynamic 2
   ```
3. **Trên Máy 2 (Receiver #1 — nhanh)**:
   ```bash
   ./receiver_run.sh <IP_MAY_1>:9009 fanout-3-may fanout 0
   ```
4. **Trên Máy 3 (Receiver #2 — chậm `76800 B/s`)**:
   ```bash
   ./receiver_run.sh <IP_MAY_1>:9009 fanout-3-may fanout 76800
   ```
   - **Quan sát**: Sau ~10 giây, Relay A trên Máy 1 tự động **kick** Máy 3; Máy 2 nhận xong 100% file; Máy 3 giữ lại file `.partial` + `.crocmap` trên đĩa.
   - **Resume 1-1 cho Máy 3**: Chạy lại Sender ở chế độ thường (`single`) trên Máy 1 và Receiver ở chế độ `normal` trên Máy 3 với tốc độ bình thường (`0`) — Máy 3 sẽ tải bù phần còn thiếu và khớp 100% SHA-256!
