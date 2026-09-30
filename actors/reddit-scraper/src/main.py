import html
from curl_cffi import requests
from actorhub_sdk import get_input, push_data, log
def clean_text(t): return html.unescape((t or "").replace("&#x200B;", "\n")).strip()
def get_json(url):
    r = requests.get(url, impersonate="chrome120", timeout=20)
    if r.status_code != 200: raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.json()
def parse_comment_tree(replies, title=""):
    out = []
    if not replies: return out
    for rp in replies.get("data", {}).get("children", []):
        if rp.get("kind") == "t1":
            c = rp["data"]
            out.append({"post_title": title, "author": c.get("author", "[deleted]"), "body": clean_text(c.get("body")), "upvotes": c.get("score", 0), "created_utc": c.get("created_utc")})
            out.extend(parse_comment_tree(c.get("replies"), title))
    return out
if __name__ == "__main__":
    i = get_input(); n = int(i.get("max_items", 50)); url = (i.get("thread_url") or "").strip(); got = 0
    if url:
        d = get_json(url.rstrip("/") + ".json"); title = d[0]["data"]["children"][0]["data"]["title"]
        for c in parse_comment_tree(d[1], title)[:n]: push_data(c); got += 1
    else:
        after = None
        while got < n:
            d = get_json(f"https://www.reddit.com/r/{i['subreddit'].strip()}/new.json?limit=100" + (f"&after={after}" if after else ""))
            for ch in d["data"]["children"]:
                p = ch["data"]; push_data({"title": p["title"], "author": p.get("author"), "score": p.get("score"), "comments": p.get("num_comments"), "url": "https://reddit.com" + p["permalink"], "created_utc": p.get("created_utc"), "text": clean_text(p.get("selftext"))}); got += 1
                if got >= n: break
            after = d["data"].get("after")
            if not after: break
    log("done, items:", got)
