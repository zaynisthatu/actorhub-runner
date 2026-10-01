import json, subprocess, sys
from actorhub_sdk import get_input, push_data, log
def parse(msgs):
    """gallery-dl -j output: [[2, dirmeta], [3, url, meta], ...]; type 3 = file."""
    for m in msgs:
        if isinstance(m, list) and m and m[0] == 3 and len(m) >= 3:
            k = m[2] or {}
            yield {"file_url": m[1], "filename": k.get("filename"), "extension": k.get("extension"), "category": k.get("category"), "subcategory": k.get("subcategory"), "title": k.get("title") or k.get("description"), "id": k.get("id")}
if __name__ == "__main__":
    i = get_input(); n = int(i.get("max_items", 50))
    p = subprocess.run([sys.executable, "-m", "gallery_dl", "-j", "--range", f"1-{n}", i["url"]], capture_output=True, text=True)
    log((p.stderr or "")[-500:])
    try: rows = list(parse(json.loads(p.stdout)))
    except Exception: rows = []
    for r in rows: push_data(r)
    log("files:", len(rows))
    if not rows: sys.exit(1)
