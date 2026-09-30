import csv, io, json, os, re, sqlite3, threading, time, uuid
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, PlainTextResponse
import runner

ROOT, BACKEND = Path(__file__).parent, os.environ.get("ACTORHUB_BACKEND", "docker")
RUNS = Path(os.environ.get("ACTORHUB_RUNS", ROOT / "runs")); RUNS.mkdir(exist_ok=True)
DB = os.environ.get("ACTORHUB_DB", str(ROOT / "actorhub.db"))
app = FastAPI(title="Actor Hub")

def q(sql, args=()):
    c = sqlite3.connect(DB); c.row_factory = sqlite3.Row
    try:
        r = c.execute(sql, args).fetchall(); c.commit(); return [dict(x) for x in r]
    finally: c.close()
q("create table if not exists runs(id text primary key, actor text, status text, input text, started real, finished real, exit_code int)")
q("update runs set status='FAILED' where status='RUNNING'")          # leftovers from a server restart

def actors():
    out = {}
    for f in sorted((ROOT / "actors").glob("*/actor.json")): m = json.loads(f.read_text()); out[m["name"]] = m
    return out

@app.get("/api/actors")
def list_actors(): return [m for n, m in actors().items() if not n.startswith("_")]

def _work(rid, name, meta):
    try: status, rc = runner.run(name, meta, RUNS / rid, BACKEND)
    except Exception as e:
        (RUNS / rid / "run.log").open("a").write(f"[hub] runner error: {type(e).__name__}: {e}\n"); status, rc = "FAILED", None
    q("update runs set status=?, exit_code=?, finished=? where id=?", (status, rc, time.time(), rid))

@app.post("/api/actors/{name}/runs")
def start(name: str, inp: dict):
    meta = actors().get(name)
    if not meta: raise HTTPException(404, "unknown actor")
    full = {}
    for k, f in meta["input_schema"].items():
        v = inp.get(k, f.get("default"))
        if f.get("required") and v in (None, ""): raise HTTPException(422, f"'{k}' is required")
        full[k] = int(v) if f["type"] == "integer" and v not in (None, "") else v
    rid = uuid.uuid4().hex[:10]; (RUNS / rid).mkdir(); (RUNS / rid / "input.json").write_text(json.dumps(full))
    q("insert into runs values(?,?,?,?,?,?,?)", (rid, name, "RUNNING", json.dumps(full), time.time(), None, None))
    threading.Thread(target=_work, args=(rid, name, meta), daemon=True).start()
    return {"id": rid}

def _run(rid):
    if not re.fullmatch(r"[0-9a-f]{10}", rid): raise HTTPException(404)
    r = q("select * from runs where id=?", (rid,))
    if not r: raise HTTPException(404)
    return r[0]

def _items(rid):
    p = RUNS / rid / "output" / "dataset.jsonl"
    return [json.loads(l) for l in p.read_text(encoding="utf-8").splitlines() if l.strip()] if p.exists() else []

@app.get("/api/runs")
def runs(): return q("select id, actor, status, started, finished from runs order by started desc limit 30")

@app.get("/api/runs/{rid}")
def run(rid: str):
    r = _run(rid); lg = RUNS / rid / "run.log"; it = _items(rid)
    return {**r, "count": len(it), "items": it[:100], "log": lg.read_text(errors="replace")[-6000:] if lg.exists() else ""}

@app.get("/api/runs/{rid}/dataset.{fmt}")
def dataset(rid: str, fmt: str):
    _run(rid); it = _items(rid)
    if fmt == "json": return it
    if fmt != "csv": raise HTTPException(404)
    cols = list(dict.fromkeys(k for i in it for k in i)); b = io.StringIO(); w = csv.DictWriter(b, cols); w.writeheader(); w.writerows(it)
    return PlainTextResponse(b.getvalue(), media_type="text/csv")

@app.get("/")
def index(): return FileResponse(ROOT / "static/index.html")
