#!/usr/bin/env python3
"""Verifies the runner end-to-end. `python selftest.py --backend docker` (real) or `--backend process`."""
import argparse, json, os, subprocess, sys, tempfile, time, urllib.request, urllib.error
ap = argparse.ArgumentParser(); ap.add_argument("--backend", default="docker"); ap.add_argument("--port", type=int, default=8765); a = ap.parse_args()
tmp = tempfile.mkdtemp(); sec = os.path.join(tmp, "s.env"); open(sec, "w").write("SELFTEST_SECRET=abc\nSELFTEST_UNDECLARED=xyz\n")
env = {**os.environ, "ACTORHUB_BACKEND": a.backend, "ACTORHUB_SECRETS": sec, "ACTORHUB_RUNS": tmp + "/runs", "ACTORHUB_DB": tmp + "/t.db"}
os.makedirs(tmp + "/runs"); B = f"http://127.0.0.1:{a.port}"
srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--port", str(a.port), "--log-level", "warning"], env=env, cwd=os.path.dirname(os.path.abspath(__file__)))
def call(path, body=None):
    req = urllib.request.Request(B + path, data=json.dumps(body).encode() if body is not None else None, headers={"Content-Type": "application/json"})
    try: r = urllib.request.urlopen(req, timeout=30); return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e: return e.code, None
def go(actor, inp, wait=900):
    s, d = call(f"/api/actors/{actor}/runs", inp); assert s == 200, (s, d); t0 = time.time()
    while time.time() - t0 < wait:
        s, r = call("/api/runs/" + d["id"])
        if r["status"] != "RUNNING": return r, time.time() - t0
        time.sleep(.4)
    raise TimeoutError("run never finished")
res = []
def check(name, cond, extra=""): res.append(cond); print(("PASS " if cond else "FAIL ") + name, extra)
try:
    for _ in range(50):
        try: call("/api/actors"); break
        except Exception: time.sleep(.3)
    r, _ = go("_selftest", {"sleep": 0})
    check("run succeeds, exit 0", r["status"] == "SUCCEEDED" and r["exit_code"] == 0, r["status"])
    check("declared secret reaches actor", r["items"][0]["declared_secret_seen"] is True)
    check("undeclared secret does NOT reach actor", r["items"][0]["undeclared_secret_seen"] is False)
    check("output dataset + log captured", r["count"] == 2 and "selftest end" in r["log"])
    r, _ = go("_selftest", {"fail": 1})
    check("failing actor -> FAILED, exit 1, traceback in log", r["status"] == "FAILED" and r["exit_code"] == 1 and "selftest requested failure" in r["log"])
    r, dt = go("_selftest", {"sleep": 30})
    check("timeout (3s) kills the run", r["status"] == "TIMEOUT" and dt < 10, f"{dt:.1f}s")
    left = subprocess.run(["docker", "ps", "-q", "--filter", "name=ah-"], capture_output=True, text=True).stdout.strip() if a.backend == "docker" else subprocess.run(["pgrep", "-P", str(srv.pid)], capture_output=True, text=True).stdout.strip()
    check("nothing left running after timeout", left == "", left)
    check("unknown actor -> 404", call("/api/actors/nope/runs", {})[0] == 404)
    check("missing required input -> 422", call("/api/actors/youtube-transcript/runs", {"urls": ""})[0] == 422)
    check("path traversal on run id -> 404", call("/api/runs/..%2F..%2Fetc")[0] == 404)
    r, _ = go("youtube-transcript", {"urls": "not a url"})
    check("real actor: all-rows-failed -> FAILED (not fake success)", r["status"] == "FAILED" and r["exit_code"] == 1, r["status"])
finally:
    srv.terminate()
print(f"\n{sum(res)}/{len(res)} passed on backend={a.backend}"); sys.exit(0 if all(res) else 1)
