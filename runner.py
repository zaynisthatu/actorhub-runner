"""Runs one actor: backend 'docker' (real isolation, default) or 'process' (dev/tests, same contract)."""
import os, signal, subprocess, sys
from pathlib import Path
ROOT = Path(__file__).parent

def load_secrets():
    p = Path(os.environ.get("ACTORHUB_SECRETS", ROOT / "secrets.env")); d = {}
    if p.exists():
        for l in p.read_text().splitlines():
            if "=" in l and not l.strip().startswith("#"): k, v = l.split("=", 1); d[k.strip()] = v.strip()
    return d

def _docker(args, logf, check=False):
    return subprocess.run(["docker", *args], stdout=logf, stderr=subprocess.STDOUT, check=check)

def ensure_images(name, logf):
    for img, ctx in (("actorhub/py-base", ROOT / "base/py-base"), (f"actorhub/{name}", ROOT / "actors" / name)):
        if _docker(["image", "inspect", img], subprocess.DEVNULL).returncode != 0:
            logf.write(f"[hub] building {img}\n".encode()); logf.flush(); _docker(["build", "-t", img, str(ctx)], logf, check=True)

def run(name, meta, run_dir, backend):
    """-> (status, exit_code). Only secrets declared in actor.json reach the actor."""
    timeout, sec, env = meta.get("timeout_sec", 600), load_secrets(), os.environ.copy()
    declared = [k for k in meta.get("secrets", []) if k in sec]; env.update({k: sec[k] for k in declared})
    with open(run_dir / "run.log", "ab") as lf:
        cname = f"ah-{run_dir.name}"
        if backend == "docker":
            ensure_images(name, lf)
            cmd = ["docker", "run", "--rm", "--name", cname, "--memory", meta.get("memory", "512m"), "--cpus", "1",
                   "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{run_dir.resolve()}:/data", *[x for k in declared for x in ("-e", k)], f"actorhub/{name}"]
            cwd = None
        else:
            env.update(ACTORHUB_DATA=str(run_dir.resolve()), PYTHONPATH=str(ROOT / "base/py-base"))
            for k in list(env):                       # process backend: strip other declared-elsewhere secrets
                if k in sec and k not in declared: del env[k]
            cmd, cwd = [sys.executable, "src/main.py"], ROOT / "actors" / name
        p = subprocess.Popen(cmd, stdout=lf, stderr=subprocess.STDOUT, env=env, cwd=cwd, start_new_session=True)
        try: rc = p.wait(timeout)
        except subprocess.TimeoutExpired:
            if backend == "docker": subprocess.run(["docker", "kill", cname], capture_output=True)
            os.killpg(p.pid, signal.SIGKILL); p.wait(); return "TIMEOUT", None
    return ("SUCCEEDED" if rc == 0 else "FAILED"), rc
