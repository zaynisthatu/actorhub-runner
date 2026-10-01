"""Offline unit tests for actor parsers (no network)."""
import importlib.util, sys
sys.path.insert(0, "base/py-base")
def load(p):
    s = importlib.util.spec_from_file_location("m", p); m = importlib.util.module_from_spec(s); s.loader.exec_module(m); return m
y = load("actors/ytdlp-info/src/main.py").to_row({"id": "x", "title": "T", "channel": "C", "webpage_url": "https://u", "duration": 5, "view_count": 9}, "src")
assert y["uploader"] == "C" and y["url"] == "https://u" and y["views"] == 9, y
g = list(load("actors/gallerydl-info/src/main.py").parse([[2, {"category": "x"}], [3, "https://f/1.jpg", {"filename": "1", "extension": "jpg", "category": "pinterest", "id": 7}], [6, "https://q", {}]]))
assert len(g) == 1 and g[0]["file_url"] == "https://f/1.jpg" and g[0]["id"] == 7, g
r = load("actors/reddit-scraper/src/main.py").parse_comment_tree({"data": {"children": [{"kind": "t1", "data": {"author": "a", "body": "x &amp; y", "score": 1, "created_utc": 1, "replies": {"data": {"children": [{"kind": "t1", "data": {"author": "b", "body": "z", "replies": ""}}]}}}}]}}, "T")
assert [c["author"] for c in r] == ["a", "b"] and r[0]["body"] == "x & y", r
print("PASS offline parser tests: ytdlp-info, gallerydl-info, reddit comment tree")
