import sys
from actorhub_sdk import get_input, push_data, log
def to_row(e, src):
    return {"source_url": src, "id": e.get("id"), "title": e.get("title"), "uploader": e.get("uploader") or e.get("channel"), "url": e.get("webpage_url") or e.get("url"),
            "duration": e.get("duration"), "views": e.get("view_count"), "upload_date": e.get("upload_date")}
if __name__ == "__main__":
    import yt_dlp
    i = get_input(); n = int(i.get("max_items", 50)); ok = bad = 0
    opts = {"quiet": True, "skip_download": True, "extract_flat": "in_playlist", "playlistend": n, "ignoreerrors": False}
    for u in [x.strip() for x in i["urls"].splitlines() if x.strip()]:
        try:
            with yt_dlp.YoutubeDL(opts) as y: info = y.extract_info(u, download=False)
            for e in (info.get("entries") or [info])[:n]:
                if e: push_data(to_row(e, u)); ok += 1
        except Exception as ex: push_data({"source_url": u, "error": type(ex).__name__ + ": " + str(ex)[:160]}); bad += 1; log("failed", u)
    log(f"rows ok={ok} urls failed={bad}")
    if ok == 0: sys.exit(1)
