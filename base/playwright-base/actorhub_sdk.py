"""Actor contract: input /data/input.json, output /data/output/, logs on stdout, exit 0 = success."""
import json, os, time
D = os.environ.get("ACTORHUB_DATA", "/data")
def get_input(): return json.load(open(f"{D}/input.json"))
def push_data(row):
    os.makedirs(f"{D}/output", exist_ok=True)
    with open(f"{D}/output/dataset.jsonl", "a", encoding="utf-8") as f: f.write(json.dumps(row, ensure_ascii=False) + "\n")
def log(*a): print(time.strftime("%H:%M:%S"), *a, flush=True)
