import glob, json, os, subprocess, sys
from actorhub_sdk import get_input, push_data, log, D
i = get_input(); here = os.path.dirname(os.path.abspath(__file__)); work = os.path.join(D, "work"); os.makedirs(work, exist_ok=True)
cmd = [sys.executable, os.path.join(here, "pipeline.py"), i["topic"], "--skip-media", "--no-retry"]
if i.get("skip"): cmd += ["--skip", i["skip"]]
if int(i.get("niche_only") or 0): cmd.append("--niche-only")
rc = subprocess.run(cmd, cwd=work).returncode; log("pipeline exit", rc)
fs = glob.glob(os.path.join(work, "Research_Output", "*", "*_master.json")); n = 0
for f in fs:
    for it in json.load(open(f, encoding="utf-8")).get("items", []): push_data(it); n += 1
log("items:", n)
if n == 0: sys.exit(1)
