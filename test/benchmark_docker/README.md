# Benchmark Docker: so sánh 2 phiên bản croc

Chạy lại các kịch bản `test/simulation/scenarios` (01–06) cho **bản cũ** và **bản mới** với nhiều kích thước file, đo metrics từ bên ngoài tiến trình (công bằng cho cả 2 bản), rồi xuất trang HTML so sánh.

## Thành phần

| File | Vai trò |
|---|---|
| `build.sh` | Build 2 binary Linux (`croc-base` = bản cũ, mặc định `58a317db`; `croc-fix` = bản mới, mặc định `18a1b529`, có thể kèm 1 file patch) và image `croc-bench:1`. |
| `ctx/Dockerfile` | Alpine + `iproute2` (`tc`, `ss`), `ping`, `curl`, 2 binary, `tc_shape.sh` (chép từ `test/simulation/docker/`), `sampler.sh`. |
| `ctx/sampler.sh` | Chạy trong container: byte receiver nhận được mỗi 250 ms; RSS/CPU/byte eth0/socket buffer của relay và sender mỗi 500 ms; TCP RTT/retrans (`ss -ti`) mỗi 1 s. |
| `bench.py` | Tạo mạng `cb-net` + 7 container (`cb-relay-a`, `cb-relay-b`, `cb-sender`, `cb-r1..4`), sinh payload, đặt `tc`, chạy từng kịch bản × kích thước × phiên bản, kiểm SHA-256, tính metrics, ghi `results/results.jsonl`. Tự chạy tiếp từ chỗ dừng. |
| `report.py` | Đọc `results/results.jsonl` → `results/croc_benchmark_report.html` (Chart.js). |

## Kịch bản (giữ tỉ lệ băng thông gốc, trần 1 Gbit/s)

| Kịch bản | Cấu hình |
|---|---|
| 01 | 1A single 500 Mbit; 1B dual 500+500 dynamic; 1C dual 1000+250 parity; 1D dual 1000+250 dynamic (trễ 5 ms) |
| 02 | dual 833+833 dynamic, `docker stop` relay B khi receiver đã nhận 30% |
| 03 | relay B qua Cloudflare Quick Tunnel thật (container `cloudflared`), dynamic và parity |
| 04 | fan-out 1→4, buffer relay 4 MiB, receiver 4 giới hạn 250 Mbit |
| 05 | fan-out 1→3, receiver 3 giới hạn 600 kbit/s (bị kick) → resume 1-1 |
| 06 | dual 833+833 + fan-out 1→3 + kill relay B @30% + receiver 3 bị kick |

## Chạy

Yêu cầu: Docker đang chạy, Go, Python 3, Git Bash (Windows) hoặc bash.

```bash
# 1. Build 2 phiên bản + image
test/benchmark_docker/build.sh                          # cũ = 58a317db, mới = 18a1b529
# mới = 18a1b529 + patch (ví dụ bản vá cửa sổ 2xBDP chưa commit):
#   test/benchmark_docker/build.sh 58a317db 18a1b529 test/benchmark_docker/patches/bdp-window.patch
# mới = working tree hiện tại:
#   test/benchmark_docker/build.sh 58a317db WORKTREE

# 2. Chạy: <kịch bản> <kích thước MiB> <số lần lặp cho file <= 1 GiB>
cd test/benchmark_docker
python -u bench.py 01,02,04,05,06,03 100,500,1024,2048 3 | tee results/run.log

# 3. Dựng trang HTML
python report.py
# -> results/croc_benchmark_report.html
```

- File lớn hơn 1 GiB và kịch bản 03 chỉ chạy 1 lần mỗi cấu hình.
- Bị ngắt giữa chừng thì chạy lại đúng lệnh bước 2: các lần đã có kết quả hợp lệ được bỏ qua.
- Trên Windows, để không bị dừng khi đóng terminal:
  `Start-Process python -ArgumentList "-u","bench.py","01,02,04,05,06,03","100,500,1024,2048","3" -RedirectStandardOutput results\run.log -WindowStyle Hidden`
- Kịch bản 03 gửi dữ liệu thử (file ngẫu nhiên) qua Internet và Cloudflare.
- Dọn dẹp: `docker rm -f cb-relay-a cb-relay-b cb-sender cb-r1 cb-r2 cb-r3 cb-r4 cb-cf; docker network rm cb-net`

## Metrics

Thời gian handshake (tới byte đầu), tổng thời gian, goodput, throughput p5/p50/p95/p99 (mẫu 250 ms), TCP RTT và retransmit sender→relay A/B, byte relay gửi / byte payload, tỉ lệ byte qua relay B, RSS/CPU đỉnh của relay A/B và sender, socket send-buffer (kernel), failover stall/recovery (02, 06), chênh lệch hoàn tất giữa receiver, thời điểm kick, thời gian resume (05), SHA-256.
