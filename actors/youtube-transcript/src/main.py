import re, sys
from youtube_transcript_api import YouTubeTranscriptApi
from actorhub_sdk import get_input, push_data, log
VID = re.compile(r"(?:youtube\.com/watch\?v=|youtu\.be/)([A-Za-z0-9_-]{11})")
i = get_input(); langs = [x.strip() for x in i.get("languages", "en,hi,ur").split(",")]; api = YouTubeTranscriptApi(); ok = bad = 0
for u in [x.strip() for x in i["urls"].splitlines() if x.strip()]:
    m = VID.search(u)
    if not m: push_data({"url": u, "error": "not a YouTube video URL"}); bad += 1; continue
    try:
        f = api.fetch(m.group(1), languages=langs); push_data({"video_id": m.group(1), "url": u, "language": f.language_code, "text": "\n".join(s.text for s in f.snippets)}); ok += 1
    except Exception as e: push_data({"video_id": m.group(1), "url": u, "error": type(e).__name__}); bad += 1; log("failed", m.group(1), type(e).__name__)
log(f"ok={ok} failed={bad}")
if ok == 0: sys.exit(1)
