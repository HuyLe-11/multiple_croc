"""Docker benchmark: old (58a317db) vs new (18a1b529) croc across the
test/simulation scenarios 01-06, file sizes 100 MiB .. 5 GiB.

Bandwidths are the original scenario tc rates scaled so the fastest link is
1 Gbit/s (ratios preserved). The slow fan-out receiver keeps 600 kbit/s
because it must stay below the relay kick threshold (256 KiB/s).
"""
import json
import os
import re
import statistics
import subprocess
import sys
import time
import uuid

IMAGE = "croc-bench:1"
NET = "cb-net"
VERSIONS = ["base", "fix"]
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
NODES = ["relay-a", "relay-b", "sender", "r1", "r2", "r3", "r4"]
FANOUT_FLAGS = ("--fanout-buffer 4194304 --fanout-max-memory 67108864 --fanout-max-receivers 8 "
                "--fanout-min-rate 262144 --fanout-rate-window 3s --fanout-stall-timeout 8s "
                "--source-join-limit 100000 --room-join-limit 100000")
CF_IMAGE = "cloudflare/cloudflared:2026.11.0"


def sh(args, check=True, timeout=None, capture=True):
    r = subprocess.run(args, capture_output=capture, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f"{args} -> {r.returncode}: {r.stderr.strip()[:400]}")
    return r.stdout if capture else ""


def dexec(node, cmd, check=True, timeout=None, env=None, detach=False):
    args = ["docker", "exec"]
    if detach:
        args.append("-d")
    for k, v in (env or {}).items():
        args += ["-e", f"{k}={v}"]
    args += [f"cb-{node}", "sh", "-c", cmd]
    return sh(args, check=check, timeout=timeout)


def popen_exec(node, cmd, env=None):
    args = ["docker", "exec"]
    for k, v in (env or {}).items():
        args += ["-e", f"{k}={v}"]
    args += [f"cb-{node}", "sh", "-c", cmd]
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def now_ms(node="sender"):
    return int(dexec(node, "date +%s%3N").strip())


# ---------------------------------------------------------------- cluster
def cluster_up():
    if subprocess.run(["docker", "network", "inspect", NET], capture_output=True).returncode != 0:
        sh(["docker", "network", "create", NET])
    for n in NODES:
        name = f"cb-{n}"
        state = subprocess.run(["docker", "inspect", "-f", "{{.State.Running}}", name],
                               capture_output=True, text=True)
        if state.returncode == 0 and state.stdout.strip() == "true":
            continue
        if state.returncode == 0:
            sh(["docker", "start", name])
            continue
        sh(["docker", "run", "-d", "--name", name, "--hostname", n, "--network", NET,
            "--network-alias", n, "--cap-add", "NET_ADMIN", IMAGE])


def clear_tc():
    for n in NODES:
        dexec(n, "tc_shape.sh clear", check=False)


def set_tc(node, rate, delay="5ms"):
    dexec(node, f"tc_shape.sh set {rate} {delay}")


def start_relays(ver, peers_a="relay-b:9019"):
    for n in ("relay-a", "relay-b"):
        dexec(n, "pkill -9 croc- ; sleep 0.3; rm -f /tmp/relay.log", check=False)
    dexec("relay-a", f"croc-{ver} relay --host 0.0.0.0 --ports 9009,9010,9011,9012,9013 "
          f"--peers {peers_a} {FANOUT_FLAGS} > /tmp/relay.log 2>&1", detach=True)
    dexec("relay-b", f"croc-{ver} relay --host 0.0.0.0 --ports 9019,9020,9021,9022,9023 "
          f"--ws-listen 0.0.0.0:8080 --peers relay-a:9009 {FANOUT_FLAGS} > /tmp/relay.log 2>&1", detach=True)
    for n, port in (("relay-a", 9013), ("relay-b", 8080)):
        for _ in range(50):
            if f":{port} " in dexec(n, "ss -ltnH", check=False):
                break
            time.sleep(0.1)


def ensure_payloads(sizes):
    hashes = {}
    path = os.path.join(OUT_DIR, "payload_hashes.json")
    if os.path.exists(path):
        hashes = json.load(open(path))
    for mib in sizes:
        name = f"payload_{mib}MiB.bin"
        if name in hashes and dexec("sender", f"test -f /data/{name} && echo y", check=False).strip() == "y":
            continue
        print(f"  generating {name} ...", flush=True)
        dexec("sender", f"dd if=/dev/urandom of=/data/{name} bs=1M count={mib} status=none", timeout=3600)
        hashes[name] = dexec("sender", f"sha256sum /data/{name}", timeout=3600).split()[0]
        json.dump(hashes, open(path, "w"), indent=1)
    return hashes


# ---------------------------------------------------------------- samplers
def start_samplers(fname, receivers):
    for n in NODES:
        dexec(n, "rm -f /tmp/sampler.stop /tmp/s_*.log", check=False)
    for r in receivers:
        dexec(r, f"sampler.sh recv /data/{fname} /tmp/s_recv.log", detach=True)
    for n in ("relay-a", "relay-b", "sender"):
        dexec(n, "sampler.sh proc /tmp/s_proc.log", detach=True)
    dexec("sender", "sampler.sh tcp /tmp/s_tcp.log", detach=True)


def stop_samplers():
    for n in NODES:
        dexec(n, "touch /tmp/sampler.stop", check=False)
    time.sleep(1.2)


def read_log(node, path):
    return dexec(node, f"cat {path} 2>/dev/null", check=False)


# ---------------------------------------------------------------- one run
def run_transfer(ver, fname, size, receivers, send_flags, recv_flags=None, chaos=None):
    """Runs sender + receivers. recv_flags: dict receiver -> extra flags."""
    code = f"cb-{uuid.uuid4().hex[:12]}"
    env = {"CROC_SECRET": code, "LOGGER": "info"}
    for r in receivers:
        dexec(r, f"rm -rf /data/* /tmp/r_*", check=False)
    dexec("sender", "rm -f /tmp/x_*", check=False)
    start_samplers(fname, receivers)
    sender = popen_exec("sender", (
        f"date +%s%3N > /tmp/x_start; cd /data && croc-{ver} --no-local --no-update-check --no-compress "
        f"--relay relay-a:9009 send {send_flags} {fname} > /tmp/x_send.log 2>&1; echo $? > /tmp/x_rc; "
        f"date +%s%3N > /tmp/x_end"), env)
    time.sleep(0.4)
    procs = {}
    for r in receivers:
        extra = (recv_flags or {}).get(r, "")
        procs[r] = popen_exec(r, (
            f"date +%s%3N > /tmp/r_start; cd /data && croc-{ver} --no-local --no-update-check --yes --overwrite "
            f"{extra} --relay relay-a:9009 > /tmp/r_recv.log 2>&1; echo $? > /tmp/r_rc; date +%s%3N > /tmp/r_end"), env)
    kill_ts = None
    if chaos:
        watch, frac = chaos
        target = int(size * frac)
        deadline = time.time() + 1800
        while time.time() < deadline and procs[watch].poll() is None:
            got = dexec(watch, f"tail -n1 /tmp/s_recv.log 2>/dev/null", check=False).split()
            if len(got) == 2 and int(got[1]) >= target:
                kill_ts = now_ms()
                sh(["docker", "stop", "-t", "0", "cb-relay-b"])
                break
            time.sleep(0.3)
    limit = max(900, size / 2e6)
    t0 = time.time()
    for p in list(procs.values()) + [sender]:
        try:
            p.wait(timeout=max(5, limit - (time.time() - t0)))
        except subprocess.TimeoutExpired:
            p.kill()
    stop_samplers()
    if chaos and kill_ts is not None:
        sh(["docker", "start", "cb-relay-b"])
    raw = {"kill_ts": kill_ts, "receivers": {}, "nodes": {}}
    for r in receivers:
        raw["receivers"][r] = {
            "start": int(read_log(r, "/tmp/r_start").strip() or 0),
            "end": int(read_log(r, "/tmp/r_end").strip() or 0),
            "rc": int((read_log(r, "/tmp/r_rc").strip() or "-1")),
            "recv": read_log(r, "/tmp/s_recv.log"),
            "log": read_log(r, "/tmp/r_recv.log")[-3000:],
            "sha": dexec(r, f"sha256sum /data/{fname} 2>/dev/null", check=False, timeout=3600).split()[:1],
            "partial": dexec(r, f"ls /data/ 2>/dev/null", check=False).split(),
        }
    for n in ("relay-a", "relay-b", "sender"):
        raw["nodes"][n] = {"proc": read_log(n, "/tmp/s_proc.log")}
    raw["tcp"] = read_log("sender", "/tmp/s_tcp.log")
    raw["sender"] = {
        "start": int(read_log("sender", "/tmp/x_start").strip() or 0),
        "end": int(read_log("sender", "/tmp/x_end").strip() or 0),
        "rc": int(read_log("sender", "/tmp/x_rc").strip() or "-1"),
        "log": dexec("sender", r"grep -a -E 'dual-relay|\[warn\]|\[error\]' /tmp/x_send.log | tail -n 60", check=False),
    }
    return raw


def ping_rtt(dst):
    out = dexec("sender", f"ping -c 5 -i 0.2 -q {dst}", check=False)
    m = re.search(r"= ([\d.]+)/([\d.]+)/([\d.]+)", out)
    return float(m.group(2)) if m else None


# ---------------------------------------------------------------- metrics
def pct(values, p):
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p / 100.0
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


def recv_metrics(rec, size, kill_ts=None):
    pts = [tuple(map(int, l.split())) for l in rec["recv"].splitlines() if len(l.split()) == 2]
    m = {"rc": rec["rc"], "total_ms": rec["end"] - rec["start"] if rec["end"] else None}
    if not pts:
        return m
    first = next((t for t, b in pts if b > 0), None)
    done = next((t for t, b in pts if b >= size), None)
    m["handshake_ms"] = first - rec["start"] if first else None
    m["final_bytes"] = pts[-1][1]
    if first:
        end_t = done or pts[-1][0]
        m["transfer_ms"] = end_t - first
        got = size if done else pts[-1][1]
        m["goodput_MBps"] = got / 1e6 / max((end_t - first) / 1000, 1e-3)
        rates = []
        for (t1, b1), (t2, b2) in zip(pts, pts[1:]):
            if t1 >= first and t2 <= end_t and t2 > t1:
                rates.append((b2 - b1) / 1e6 / ((t2 - t1) / 1000))
        m["rate_series"] = [[round((t - rec["start"]) / 1000, 2), round(b / 1e6, 2)] for t, b in pts[::max(1, len(pts) // 400)]]
        if rates:
            m.update({"tput_mean": statistics.fmean(rates), "tput_p5": pct(rates, 5), "tput_p50": pct(rates, 50),
                      "tput_p95": pct(rates, 95), "tput_p99": pct(rates, 99),
                      "tput_std": statistics.pstdev(rates)})
        if kill_ts and first and kill_ts > first:
            before, after = [], []
            for (t1, b1), (t2, b2) in zip(pts, pts[1:]):
                if t2 <= t1:
                    continue
                r = (b2 - b1) / 1e6 / ((t2 - t1) / 1000)
                (before if t2 <= kill_ts else after).append((t1, t2, r))
            med = pct([r for _, _, r in before], 50) or 0
            stall, cur = 0, 0
            for t1, t2, r in after:
                if r < 0.1 * med:
                    cur += t2 - t1
                    stall = max(stall, cur)
                else:
                    cur = 0
            rec_t = next((t2 for t1, t2, r in after if r >= 0.5 * med), None)
            m["failover_stall_ms"] = stall
            m["failover_recovery_ms"] = (rec_t - kill_ts) if rec_t else None
            m["pre_kill_MBps"] = med
            m["post_kill_MBps"] = pct([r for _, _, r in after], 50)
    return m


def proc_metrics(text):
    pts = [list(map(int, l.split())) for l in text.splitlines() if len(l.split()) == 7]
    if len(pts) < 2:
        return {}
    active = [p for p in pts if p[1] > 0]
    cpu = None
    if len(active) >= 2:
        dt = (active[-1][0] - active[0][0]) / 1000
        cpu = (active[-1][2] - active[0][2]) / 100 / max(dt, 1e-3) * 100
    return {"rss_peak_MiB": max(p[1] for p in pts) / 1024, "cpu_pct": cpu,
            "tx_MB": (pts[-1][3] - pts[0][3]) / 1e6, "rx_MB": (pts[-1][4] - pts[0][4]) / 1e6,
            "sock_rmem_peak_MiB": max(p[5] for p in pts) / 2**20,
            "sock_wmem_peak_MiB": max(p[6] for p in pts) / 2**20}


def tcp_metrics(text):
    by = {"relay-a": [], "relay-b": []}
    retr = {}
    for l in text.splitlines():
        f = l.split()
        if len(f) < 4:
            continue
        port = int(f[1].rsplit(":", 1)[-1]) if ":" in f[1] else 0
        key = "relay-a" if 9009 <= port <= 9013 else "relay-b" if (9019 <= port <= 9023 or port in (8080, 443)) else None
        if not key:
            continue
        by[key].append(float(f[2]))
        retr[(key, f[1])] = max(retr.get((key, f[1]), 0), int(f[3]))
    out = {}
    for k, v in by.items():
        if v:
            out[f"rtt_{k}_mean_ms"] = statistics.fmean(v)
            out[f"rtt_{k}_p95_ms"] = pct(v, 95)
            out[f"tcp_retrans_{k}"] = sum(c for (kk, _), c in retr.items() if kk == k)
    return out


def summarize(raw, size, hashes_ok, kill=False):
    res = {"receivers": {}}
    for r, rec in raw["receivers"].items():
        res["receivers"][r] = recv_metrics(rec, size, raw["kill_ts"] if kill else None)
        res["receivers"][r]["sha_ok"] = hashes_ok(rec)
    for n, d in raw["nodes"].items():
        res[n] = proc_metrics(d["proc"])
    res.update(tcp_metrics(raw["tcp"]))
    stats = re.findall(r"dual-relay stats:.*", raw["sender"]["log"])
    res["dual_active"] = "dual-relay: active" in raw["sender"]["log"]
    res["fallback_warn"] = "continuing with a single relay" in raw["sender"]["log"] or \
                           "falling back to single relay" in raw["sender"]["log"]
    res["fix_stats"] = stats[-1] if stats else None
    txa = res.get("relay-a", {}).get("tx_MB", 0) or 0
    txb = res.get("relay-b", {}).get("tx_MB", 0) or 0
    n = len(raw["receivers"])
    res["relay_bytes_overhead"] = (txa + txb) / (size / 1e6 * n) if size else None
    res["relay_b_share"] = txb / (txa + txb) if (txa + txb) else None
    res["sender_rc"] = raw["sender"]["rc"]
    return res


# ---------------------------------------------------------------- scenarios
def scen_cases(cf_url=None):
    # name, tc dict, receivers, send flags, recv flags, chaos, peers for relay A
    return {
        "01": [
            ("1A single", {"relay-a": "500mbit", "relay-b": "500mbit"}, ["r1"], "--transport relay", None, None),
            ("1B dual sym dynamic", {"relay-a": "500mbit", "relay-b": "500mbit"}, ["r1"],
             "--transport relay --dual-relay --chunk-policy dynamic", None, None),
            ("1C dual asym parity", {"relay-a": "1000mbit", "relay-b": "250mbit"}, ["r1"],
             "--transport relay --dual-relay --chunk-policy parity", None, None),
            ("1D dual asym dynamic", {"relay-a": "1000mbit", "relay-b": "250mbit"}, ["r1"],
             "--transport relay --dual-relay --chunk-policy dynamic", None, None),
        ],
        "02": [("2 failover relay-b @30%", {"relay-a": "833mbit", "relay-b": "833mbit"}, ["r1"],
                "--transport relay --dual-relay --chunk-policy dynamic", None, ("r1", 0.30))],
        "03": [("3 cloudflare relay-b dynamic", {}, ["r1"],
                "--transport relay --dual-relay --chunk-policy dynamic", None, None),
               ("3 cloudflare relay-b parity", {}, ["r1"],
                "--transport relay --dual-relay --chunk-policy parity", None, None)],
        "04": [("4 fanout 1->4 buffer 4MiB", {"r4": "250mbit"}, ["r1", "r2", "r3", "r4"],
                "--transport relay --receivers 4 --min-receivers 4 --wait-timeout 60s",
                {r: "--fanout" for r in ("r1", "r2", "r3", "r4")}, None)],
        "05": [("5 fanout kick + resume", {"r3": ("600kbit", "10ms")}, ["r1", "r2", "r3"],
                "--transport relay --receivers 3 --min-receivers 3 --wait-timeout 60s",
                {"r1": "--fanout", "r2": "--fanout", "r3": "--fanout --sim-recv-rate 76800"}, None)],
        "06": [("6 dual fanout + kill B + kick", {"relay-a": "833mbit", "relay-b": "833mbit", "r3": ("600kbit", "10ms")},
                ["r1", "r2", "r3"],
                "--transport relay --dual-relay --chunk-policy dynamic --receivers 3 --min-receivers 3 --wait-timeout 60s",
                {"r1": "--fanout", "r2": "--fanout", "r3": "--fanout --sim-recv-rate 76800"}, ("r1", 0.30))],
    }


def start_cloudflare():
    subprocess.run(["docker", "rm", "-f", "cb-cf"], capture_output=True)
    sh(["docker", "run", "-d", "--name", "cb-cf", "--network", NET, CF_IMAGE,
        "tunnel", "--no-autoupdate", "--url", "http://relay-b:8080"])
    url = None
    for _ in range(120):
        logs = subprocess.run(["docker", "logs", "cb-cf"], capture_output=True, text=True)
        m = re.search(r"https://[a-z0-9-]+\.trycloudflare\.com", logs.stdout + logs.stderr)
        if m:
            url = m.group(0)
            break
        time.sleep(1)
    if not url:
        raise RuntimeError("cloudflared did not print a trycloudflare URL")
    for _ in range(90):
        if dexec("sender", f"curl -fsS --max-time 5 {url}/healthz", check=False).strip() == "ok":
            return url.replace("https://", "wss://") + "/ws"
        time.sleep(2)
    raise RuntimeError(f"tunnel {url} never became healthy")


def main():
    scenarios = sys.argv[1].split(",") if len(sys.argv) > 1 else ["01", "02", "03", "04", "05", "06"]
    sizes = [int(x) for x in sys.argv[2].split(",")] if len(sys.argv) > 2 else [100, 500, 1024, 2048, 5120]
    reps = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "results.jsonl")
    done = set()
    if os.path.exists(out_path):
        for line in open(out_path, encoding="utf-8"):
            if line.strip():
                d = json.loads(line)
                if "error" not in d:
                    done.add((d["scenario"], d["case"], d["version"], d["size_MiB"], d["rep"]))
    print(f"  resuming: {len(done)} runs already recorded", flush=True)
    cluster_up()
    hashes = ensure_payloads(sizes)
    cf_url = None
    for sc in scenarios:
        peers_a = "relay-b:9019"
        if sc == "03":
            # relay-b must be up (and serving /healthz on :8080) before the tunnel check;
            # each run restarts the relays with the version under test anyway.
            subprocess.run(["docker", "start", "cb-relay-b"], capture_output=True)
            start_relays("fix")
            cf_url = start_cloudflare()
            peers_a = cf_url
            print(f"  cloudflare relay-b: {cf_url}", flush=True)
        for case in scen_cases(cf_url)[sc]:
            name, tcs, receivers, send_flags, recv_flags, chaos = case
            for mib in sizes:
                size = mib * 2**20
                fname = f"payload_{mib}MiB.bin"
                for rep in range(reps if mib <= 1024 and sc != "03" else 1):
                    for ver in VERSIONS:
                        if (sc, name, ver, mib, rep) in done:
                            continue
                        try:
                            one_run(sc, name, tcs, receivers, send_flags, recv_flags, chaos, ver, mib, rep,
                                    peers_a, hashes, out_path)
                        except Exception as exc:  # keep the matrix going
                            print(f"[{time.strftime('%H:%M:%S')}] {sc} {name} {mib}MiB {ver}: ERROR {exc}", flush=True)
                            with open(out_path, "a") as f:
                                f.write(json.dumps({"scenario": sc, "case": name, "version": ver, "size_MiB": mib,
                                                    "rep": rep, "error": str(exc)}) + "\n")
                            subprocess.run(["docker", "start", "cb-relay-b"], capture_output=True)
                        for r in receivers:
                            dexec(r, "rm -rf /data/*", check=False)
        if sc == "03":
            subprocess.run(["docker", "rm", "-f", "cb-cf"], capture_output=True)
    clear_tc()


def one_run(sc, name, tcs, receivers, send_flags, recv_flags, chaos, ver, mib, rep, peers_a, hashes, out_path):
    size = mib * 2**20
    fname = f"payload_{mib}MiB.bin"
    clear_tc()
    for node, spec in tcs.items():
        rate, delay = spec if isinstance(spec, tuple) else (spec, "5ms")
        set_tc(node, rate, delay)
    start_relays(ver, peers_a)
    rtt_a, rtt_b = ping_rtt("relay-a"), ping_rtt("relay-b")
    t = time.time()
    raw = run_transfer(ver, fname, size, receivers, send_flags, recv_flags, chaos)
    ok = lambda rec: bool(rec["sha"]) and rec["sha"][0] == hashes[fname]
    res = summarize(raw, size, ok, kill=bool(chaos))
    if sc == "05":
        clear_tc()
        start_relays(ver, peers_a)
        res["resume"] = resume_r3(ver, fname, size, hashes[fname])
    res.update({"scenario": sc, "case": name, "version": ver, "size_MiB": mib, "rep": rep,
                "ping_rtt_relay_a_ms": rtt_a, "ping_rtt_relay_b_ms": rtt_b, "wall_s": time.time() - t})
    with open(out_path, "a") as f:
        f.write(json.dumps(res) + "\n")
    rr = res["receivers"]
    extra = f" resume={res['resume']['resume_ms']}ms sha={res['resume']['sha_ok']}" if "resume" in res else ""
    print(f"[{time.strftime('%H:%M:%S')}] {sc} {name} {mib}MiB rep{rep} {ver}: " + " ".join(
        f"{k}:{(v.get('goodput_MBps') or 0):.1f}MB/s sha={'OK' if v.get('sha_ok') else 'no'} rc={v.get('rc')}"
        for k, v in rr.items()) + f" dual={res['dual_active']} warn={res['fallback_warn']}{extra}", flush=True)


def resume_r3(ver, fname, size, want):
    """Scenario 05 step 4: 1-on-1 resume of the kicked receiver's .partial."""
    code = f"cb-{uuid.uuid4().hex[:12]}"
    env = {"CROC_SECRET": code, "LOGGER": "info"}
    before = dexec("r3", f"stat -c '%b %B' /data/{fname}.partial 2>/dev/null | awk '{{print $1*$2}}'", check=False).strip()
    sender = popen_exec("sender", f"cd /data && croc-{ver} --no-local --no-update-check --no-compress "
                                  f"--relay relay-a:9009 send --transport relay {fname} > /tmp/x_resume.log 2>&1", env)
    time.sleep(0.4)
    t0 = now_ms("r3")
    rc = subprocess.run(["docker", "exec", "-e", f"CROC_SECRET={code}", "cb-r3", "sh", "-c",
                         f"cd /data && croc-{ver} --no-local --no-update-check --yes --overwrite "
                         f"--relay relay-a:9009 > /tmp/r_resume.log 2>&1"], timeout=3600).returncode
    t1 = now_ms("r3")
    try:
        sender.wait(timeout=60)
    except subprocess.TimeoutExpired:
        sender.kill()
    got = dexec("r3", f"sha256sum /data/{fname} 2>/dev/null", check=False, timeout=3600).split()[:1]
    return {"rc": rc, "resume_ms": t1 - t0, "partial_bytes_before": int(before or 0),
            "sha_ok": bool(got) and got[0] == want}


if __name__ == "__main__":
    main()
