"""
=================================================================
  UNIVERSAL RESEARCH PIPELINE  (v4 -- streaming writes + 3 new sources)
  Ek topic do -> sab free platforms search hote hain -> raw dump +
  cleaned master JSON + readable Markdown summary, sab disk pe save.

  NO API KEYS. NO PAID SERVICES. NO AI CALLS during collection.

  ─── SOURCES (11 total, all free, no key) ────────────────────
    1.  Reddit       -> site-wide search + niche subreddits (curl_cffi)
    2.  Hacker News  -> Algolia search API (stories + comments)
    3.  arXiv        -> official query API (papers + abstracts)
    4.  GitHub       -> repo search API
    5.  Google News  -> RSS multi-publisher search (no key)
    6.  StackOverflow-> Stack Exchange advanced search API
    7.  Medium       -> tag RSS (best-effort)
    8.  DuckDuckGo   -> HTML web search (general web)
    9.  Dev.to       -> FREE JSON API (/api/articles?tag=X)  [NEW]
   10.  Lobsters     -> lobste.rs search JSON  [NEW]
   11.  Niche RSS    -> per-niche curated feeds (TechCrunch/VentureBeat/etc) [NEW]

  ─── v4.1 FIXES (this update) ────────────────────────────────
    CRASH FIX: TOP PICKS sort in build_markdown crashed with
      "'<' not supported between instances of 'dict' and 'dict'"
      whenever two items tied on engagement score -- this silently
      broke the .md flush on every run with 100+ items, while
      master.json kept working fine (which is why JSON updated but
      MD looked frozen/stale). Fixed: sort now uses key=, never
      compares raw (score, dict) tuples.
    429-STORM FIX: Reddit's niche-subreddit search pool (was 10
      threads) and per-post detail-fetch pool (was 8 threads) could
      independently hammer Reddit and each 429 triggered its own
      uncoordinated backoff -- causing a dozen overlapping 12-40s
      waits to stack up and look "frozen". Fixed: a shared semaphore
      now caps concurrent reddit requests at 5, and a 429 anywhere
      triggers one shared cooldown that every thread respects,
      instead of each thread re-hammering and re-triggering its own.

  ─── KEY FEATURES vs v3 ──────────────────────────────────────
    CRITICAL FIX: master.json + summary.md written to disk after
      EVERY source completes, not just at the end. So even if you
      kill the terminal mid-run, all completed sources are saved.
    CRITICAL FIX: Ctrl+C flushes data before exiting -- no more loss.
    HN/GitHub retry: 3-level (full -> 4-word -> 1-keyword), not 2-level.
    Dev.to: free JSON API, no key, excellent for tech/fintech/AI.
    Lobsters: HN-alternative for tech -- clean JSON search endpoint.
    Niche RSS: curated per-niche feeds (TechCrunch fintech, VentureBeat AI, etc).
    TOP PICKS: MD now opens with top-15 items ranked by engagement score.
    Reddit max_workers=8 (faster per-post comment fetch).
    niche_config: expanded to 30+ subs per niche, devto_tags, rss_feeds.

  ─── INSTALL ─────────────────────────────────────────────────
    pip install curl_cffi feedparser beautifulsoup4 requests

  ─── RUN ─────────────────────────────────────────────────────
    python universal_research.py "fintech adoption trends 2026"
    python universal_research.py "ai coding assistants" --pages 5
    python universal_research.py "some topic" --skip-media
    python universal_research.py "some topic" --no-retry
    python universal_research.py "some topic" --skip medium,lobsters

  ─── FILES ───────────────────────────────────────────────────
    Keep niche_config.json in the SAME folder as this script.
=================================================================
"""

import os
import re
import sys
import json
import time
import html
import random
import signal
import argparse
import threading
from datetime import datetime, timezone
from urllib.parse import quote_plus
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
from bs4 import BeautifulSoup

try:
    from curl_cffi import requests as cf_requests
    HAVE_CURL_CFFI = True
except ImportError:
    HAVE_CURL_CFFI = False
    import requests as cf_requests

import requests


# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
HEADERS_BROWSER = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "DNT": "1",
}

REDDIT_COOKIES = {
    "over18": "1",
    "reddit_session": ""  # browser se copy karo - never commit,
}

HEADERS_API = {
    "User-Agent": "UniversalResearchPipeline/4.0 (personal research script)",
    "Accept": "application/json",
}

STOP_FLAG = False

# Shared across ALL reddit-bound threads (niche-subreddit search pool +
# per-post detail-fetch pool). Without this, every thread that hits a 429
# backs off independently, so you get a dozen overlapping 12-40s waits
# stacking up at once -- looks frozen even though no single wait is that long.
REDDIT_LOCK = threading.Lock()
REDDIT_COOLDOWN_UNTIL = 0.0
REDDIT_SEMAPHORE = threading.Semaphore(5)  # max concurrent in-flight reddit requests
REDDIT_BLOCKED_DETECTED = False  # set True when reddit fully exhausts retries on 429

MAX_TEXT_LEN = 5000

MEDIA_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".gifv", ".webp", ".mp4", ".mov")

STOPWORDS = {
    "a", "an", "the", "of", "in", "on", "for", "to", "and", "or", "is",
    "are", "vs", "versus", "with", "about", "this", "that", "how", "why",
    "what", "does", "do", "people", "general", "users", "user", "best",
    "top", "latest", "new", "using", "use", "will", "can", "from",
}

try:
    _SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
except NameError:
    _SCRIPT_DIR = os.getcwd()
NICHE_CONFIG_PATH = os.path.join(_SCRIPT_DIR, "niche_config.json")


# ─────────────────────────────────────────────────────────────
# SMALL HELPERS
# ─────────────────────────────────────────────────────────────

def now_iso():
    return datetime.now(timezone.utc).isoformat()


def clean_filename(text, max_len=60):
    clean = re.sub(r'[^A-Za-z0-9_\- ]', '', text or "").strip()
    clean = re.sub(r'\s+', '_', clean)
    return clean[:max_len] or "untitled"


ZERO_WIDTH_CHARS = re.compile(r'[\u200b\u200c\u200d\u200e\u200f\ufeff\u2060-\u2064]')


def clean_text(text):
    if not text:
        return ""
    text = html.unescape(str(text).replace("&#x200B;", "\n"))
    text = ZERO_WIDTH_CHARS.sub("", text)
    if "<" in text and ">" in text:
        try:
            text = BeautifulSoup(text, "html.parser").get_text(" ", strip=True)
        except Exception:
            pass
    return text.strip()[:MAX_TEXT_LEN]


def topic_slug(topic):
    return clean_filename(topic, max_len=40).lower()


def safe_sleep(a, b):
    time.sleep(random.uniform(a, b))


def log(msg):
    print(msg, flush=True)


# ─────────────────────────────────────────────────────────────
# NICHE CONFIG: load + match + reword
# ─────────────────────────────────────────────────────────────

def load_niche_config():
    try:
        with open(NICHE_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception as e:
        log(f"[!] Could not load niche_config.json ({e}) -- continuing without niche targeting.")
        return {}


def match_niches(topic, niche_config, max_niches=2):
    """Word-boundary keyword match so 'tech' doesn't fire inside 'fintech'."""
    topic_lower = topic.lower()
    scored = []
    for niche, data in niche_config.items():
        hits = 0
        for kw in data.get("match_keywords", []):
            pattern = r'(?<![a-z0-9])' + re.escape(kw) + r'(?![a-z0-9])'
            if re.search(pattern, topic_lower):
                hits += 1
        if hits:
            scored.append((hits, niche))
    scored.sort(reverse=True)
    return [n for _, n in scored[:max_niches]]


def niche_subreddits_for_topic(topic, niche_config, max_subs=15):
    niches = match_niches(topic, niche_config)
    subs = []
    for n in niches:
        subs.extend(niche_config[n].get("subreddits", []))
    seen, deduped = set(), []
    for s in subs:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped[:max_subs], niches


def reword_query(topic, niche_config, attempt=1):
    """
    3-level progressive shortening:
      attempt=1 -> stopword-stripped, max 4 words
      attempt=2 -> max 2 words
      attempt=3 -> single best niche keyword (e.g. 'fintech', 'machine learning')
    """
    words = [w for w in re.findall(r"[a-zA-Z0-9']+", topic.lower()) if w not in STOPWORDS]

    if attempt == 1:
        short = " ".join(words[:4])
    elif attempt == 2:
        short = " ".join(words[:2])
    else:  # attempt 3: single niche keyword
        niches = match_niches(topic, niche_config, max_niches=1)
        if niches:
            kws = niche_config[niches[0]].get("match_keywords", [])
            matched = [kw for kw in kws if kw in topic.lower()]
            if matched:
                return max(matched, key=len)
        return words[0] if words else topic

    return short.strip() or topic


# ─────────────────────────────────────────────────────────────
# OUTPUT DIRECTORY STRUCTURE
# ─────────────────────────────────────────────────────────────

def make_dirs(base_dir):
    raw_dir = os.path.join(base_dir, "raw_dumps")
    sub_dirs = {
        "reddit":        os.path.join(raw_dir, "reddit"),
        "hackernews":    os.path.join(raw_dir, "hackernews"),
        "arxiv":         os.path.join(raw_dir, "arxiv"),
        "github":        os.path.join(raw_dir, "github"),
        "google_news":   os.path.join(raw_dir, "google_news"),
        "stackoverflow": os.path.join(raw_dir, "stackoverflow"),
        "medium":        os.path.join(raw_dir, "medium"),
        "web_search":    os.path.join(raw_dir, "web_search"),
        "devto":         os.path.join(raw_dir, "devto"),
        "lobsters":      os.path.join(raw_dir, "lobsters"),
        "niche_rss":     os.path.join(raw_dir, "niche_rss"),
    }
    for d in sub_dirs.values():
        os.makedirs(d, exist_ok=True)
    media_dir = os.path.join(base_dir, "media", "reddit")
    os.makedirs(media_dir, exist_ok=True)
    return raw_dir, sub_dirs, media_dir


def dump_raw(sub_dirs, source, name, payload):
    path = os.path.join(sub_dirs[source], f"{clean_filename(name)}.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception as e:
        log(f"    [!] raw dump failed for {source}/{name}: {e}")
    return path


# ─────────────────────────────────────────────────────────────
# ENGAGEMENT SCORING (used for deduplication sort + TOP PICKS)
# ─────────────────────────────────────────────────────────────

def engagement_score(item):
    """Score that factors in both upvotes and comment depth."""
    s = float(item.get("score") or 0)
    c = float(item.get("num_comments") or 0)
    cat = item.get("category", "")
    if cat == "reddit":
        return s + c * 3.0   # reddit discussions are gold
    elif cat in ("hackernews", "lobsters"):
        return s + c * 2.0
    elif cat == "devto":
        return s + c * 1.5
    return s


# ─────────────────────────────────────────────────────────────
# MASTER JSON
# ─────────────────────────────────────────────────────────────

def build_master_json(all_items, base_dir, topic):
    seen_urls = set()
    unique = []
    for item in all_items:
        u = item.get("url", "")
        if u and u in seen_urls:
            continue
        if u:
            seen_urls.add(u)
        unique.append(item)

    unique.sort(key=engagement_score, reverse=True)

    master = {
        "topic": topic,
        "generated_at": now_iso(),
        "total_items": len(unique),
        "duplicates_removed": len(all_items) - len(unique),
        "by_source_count": {},
        "items": unique,
    }
    for item in unique:
        src = item.get("category", "unknown")
        master["by_source_count"][src] = master["by_source_count"].get(src, 0) + 1

    path = os.path.join(base_dir, f"{topic_slug(topic)}_master.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(master, f, ensure_ascii=False, indent=2, default=str)
    return path, master


# ─────────────────────────────────────────────────────────────
# MARKDOWN SUMMARY
# Leads with TOP PICKS, then per-source deep sections.
# ─────────────────────────────────────────────────────────────

def build_markdown(master, base_dir, topic):
    lines = []
    lines.append(f"# Research Dump: {topic}")
    lines.append(f"\n_Generated: {master['generated_at']}_")
    lines.append(f"\n**Total items:** {master['total_items']}  "
                 f"**Dupes removed:** {master['duplicates_removed']}\n")

    # Source breakdown
    lines.append("## Source breakdown\n")
    for src, count in sorted(master["by_source_count"].items(), key=lambda x: -x[1]):
        bar = "█" * min(count // 3, 25)
        lines.append(f"- **{src}**: {count}  {bar}")
    lines.append("")

    # ── TOP PICKS ────────────────────────────────────────────
    scored = [(engagement_score(it), it) for it in master["items"]
              if (it.get("score") or 0) > 0 or (it.get("num_comments") or 0) > 0]
    # IMPORTANT: sort by key=, never sort raw (score, dict) tuples directly --
    # on a tie, Python falls back to comparing the second element, and dicts
    # aren't orderable ('<' not supported between dict and dict). This was
    # silently breaking every .md flush once two items had equal engagement.
    top_items = [it for _, it in sorted(scored, key=lambda pair: pair[0], reverse=True)[:15]]

    if top_items:
        lines.append("\n---\n## 🏆 TOP PICKS  (highest engagement across all sources)\n")
        for i, item in enumerate(top_items, 1):
            title = item.get("title", "(no title)")
            url   = item.get("url", "")
            cat   = item.get("category", "?").upper()
            sub   = f" r/{item['subreddit']}" if item.get("subreddit") else ""
            sc    = item.get("score", "?")
            cm    = item.get("num_comments", 0)
            lines.append(f"**{i}. [{title}]({url})**")
            lines.append(f"*{cat}{sub} | ⬆{sc} 💬{cm}*\n")
            txt = (item.get("text") or "").strip()
            if len(txt) > 60:
                lines.append(f"> {txt[:500]}\n")
        lines.append("")

    # ── PER-SOURCE DEEP SECTIONS ─────────────────────────────
    by_cat = {}
    for item in master["items"]:
        by_cat.setdefault(item.get("category", "unknown"), []).append(item)

    for cat, items in by_cat.items():
        lines.append(f"\n---\n## {cat.upper()} ({len(items)} items)\n")
        for item in items:
            lines.append(f"### {item.get('title', '(no title)')}")
            meta_bits = []
            if item.get("source"):
                meta_bits.append(f"source: {item['source']}")
            if item.get("subreddit"):
                meta_bits.append(f"r/{item['subreddit']}")
            if item.get("author"):
                meta_bits.append(f"by: {item['author']}")
            if item.get("score") is not None:
                meta_bits.append(f"score: {item['score']}")
            if item.get("num_comments"):
                meta_bits.append(f"comments: {item['num_comments']}")
            if meta_bits:
                lines.append(f"*{' | '.join(meta_bits)}*")
            lines.append(f"\n{(item.get('text') or '')[:1500]}")
            lines.append(f"\n[Link]({item.get('url', '')})")

            top_comments = item.get("top_comments", [])
            if top_comments:
                lines.append("\n**Top responses:**")
                for c in top_comments[:10]:
                    body = (c.get("body") or "")[:500]
                    pts  = c.get("points", c.get("upvotes", 0))
                    lines.append(f"- ({pts}pts) {body}")

            if item.get("media"):
                lines.append("\n**Downloaded media:**")
                for m in item["media"]:
                    lines.append(f"- [{m.get('type','media')}] {m.get('local_path','')}")
            lines.append("")

    path = os.path.join(base_dir, f"{topic_slug(topic)}_summary.md")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return path


# ─────────────────────────────────────────────────────────────
# SOURCE 1 — REDDIT
# site-wide search + niche-targeted subreddit search + media download
# ─────────────────────────────────────────────────────────────

def _reddit_wait_for_cooldown():
    """If another thread recently hit a 429, wait out the shared cooldown
    before firing a new request, instead of piling on and getting 429'd too."""
    while True:
        with REDDIT_LOCK:
            remaining = REDDIT_COOLDOWN_UNTIL - time.time()
        if remaining <= 0:
            return
        time.sleep(min(remaining, 2.0))


def _reddit_trigger_cooldown(seconds):
    """One thread hit a 429 -- tell every other thread to also pause,
    instead of each one independently sleeping and re-hammering Reddit
    in an overlapping, uncoordinated way."""
    global REDDIT_COOLDOWN_UNTIL
    with REDDIT_LOCK:
        REDDIT_COOLDOWN_UNTIL = max(REDDIT_COOLDOWN_UNTIL, time.time() + seconds)


def fetch_json_cf(url, retries=3, timeout=20):
    global REDDIT_BLOCKED_DETECTED
    if STOP_FLAG:
        return None
    last_status = None
    for attempt in range(retries):
        if STOP_FLAG:
            return None
        _reddit_wait_for_cooldown()
        with REDDIT_SEMAPHORE:  # cap concurrent in-flight reddit requests
            try:
                safe_sleep(1.0, 2.5)
                r = cf_requests.get(url, impersonate="chrome120", headers=HEADERS_BROWSER,
                                     cookies=REDDIT_COOKIES, timeout=timeout)
                last_status = r.status_code
                if r.status_code == 429:
                    wait = random.uniform(15, 25) * (attempt + 1)
                    log(f"    [429] rate limited, cooling down {wait:.0f}s (all reddit threads pause)...")
                    _reddit_trigger_cooldown(wait)
                    continue
                if r.status_code == 200:
                    REDDIT_BLOCKED_DETECTED = False  # any success clears the blocked state
                    try:
                        return r.json()
                    except Exception:
                        log(f"    [!] 200 but not JSON: {r.text[:120]!r}")
                        return None
                log(f"    [!] HTTP {r.status_code} -- {url[:80]}")
                return None
            except Exception as e:
                log(f"    [!] request error: {type(e).__name__}: {e}")
    log(f"    [!] gave up after {retries} attempts, last={last_status}")
    if last_status == 429:
        # Every attempt was 429'd -- this isn't a one-off, Reddit is actively
        # blocking this session/IP right now. Callers use this to skip firing
        # MORE parallel requests (like the 15-subreddit niche search) right
        # after, which would almost certainly just get blocked again too.
        REDDIT_BLOCKED_DETECTED = True
    return None


def parse_comment_tree(replies_obj, post_title=""):
    extracted = []
    if not replies_obj or replies_obj == "":
        return extracted
    for child in replies_obj.get('data', {}).get('children', []):
        if child.get('kind') == 't1':
            c = child.get('data', {})
            extracted.append({
                "post_title": post_title,
                "author": c.get("author", "[deleted]"),
                "body": clean_text(c.get("body", "")),
                "upvotes": c.get("score", 0),
                "created_utc": c.get("created_utc"),
            })
            if c.get("replies"):
                extracted.extend(parse_comment_tree(c.get("replies"), post_title))
    return extracted


def _media_from_single_post(pd):
    found = []
    url = pd.get("url_overridden_by_dest") or pd.get("url") or ""
    if pd.get("is_video"):
        rv = ((pd.get("media") or {}).get("reddit_video") or
              (pd.get("secure_media") or {}).get("reddit_video"))
        if rv and rv.get("fallback_url"):
            found.append(("video", rv["fallback_url"].split("?")[0]))
    if pd.get("is_gallery") and pd.get("media_metadata"):
        for _, meta in pd["media_metadata"].items():
            if meta.get("status") != "valid":
                continue
            src = meta.get("s", {}) or {}
            img_url = src.get("u") or src.get("gif") or src.get("mp4")
            if img_url:
                found.append(("image", html.unescape(img_url)))
    if url:
        url = url.replace("&amp;", "&")
        if url.endswith(".gifv"):
            url = url.replace(".gifv", ".mp4")
        if (url.lower().split("?")[0].endswith(MEDIA_EXTENSIONS) or
                any(d in url.lower() for d in ("v.redd.it", "i.redd.it",
                                                "i.imgur.com", "redgifs.com"))):
            found.append(("direct", url))
    if not found:
        preview_images = (pd.get("preview") or {}).get("images", [])
        if preview_images:
            src_url = (preview_images[0].get("source") or {}).get("url")
            if src_url:
                found.append(("preview", html.unescape(src_url)))
    return found


def extract_reddit_media(post_data):
    media = _media_from_single_post(post_data)
    if not media:
        parents = post_data.get("crosspost_parent_list") or []
        if parents:
            media = _media_from_single_post(parents[0])
    seen, deduped = set(), []
    for mt, u in media:
        if u not in seen:
            seen.add(u)
            deduped.append((mt, u))
    return deduped


def download_file(url, dest_path, timeout=60):
    if STOP_FLAG or not url:
        return False
    try:
        kwargs = dict(headers=HEADERS_BROWSER, timeout=timeout)
        if HAVE_CURL_CFFI:
            kwargs["impersonate"] = "chrome120"
            kwargs["cookies"] = REDDIT_COOKIES
        r = cf_requests.get(url, **kwargs)
        if r.status_code != 200:
            return False
        with open(dest_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception:
        return False


def download_image_plain(url, dest_path, timeout=30):
    """Download any image/media URL without Reddit cookies.
    Used for Dev.to cover images, Medium og:image, RSS enclosures, etc."""
    if STOP_FLAG or not url:
        return False
    try:
        r = requests.get(url, headers=HEADERS_BROWSER, timeout=timeout, stream=True)
        if r.status_code != 200:
            return False
        content_type = r.headers.get("content-type", "")
        # Only accept image/video content types
        if not any(t in content_type for t in
                   ("image/", "video/", "application/octet")):
            return False
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return True
    except Exception:
        return False


def resolve_redgifs_url(page_url):
    """Resolve a redgifs.com page URL to a direct .mp4 download URL
    using their public API (no auth needed for public GIFs)."""
    try:
        # Extract GIF id from URL: redgifs.com/watch/GIFID or /ifr/GIFID
        gif_id = re.search(r'redgifs\.com/(?:watch|ifr)/([a-zA-Z]+)', page_url)
        if not gif_id:
            return None
        gif_id = gif_id.group(1).lower()
        api_url = f"https://api.redgifs.com/v2/gifs/{gif_id}"
        r = requests.get(api_url, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.redgifs.com/"
        }, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        urls = data.get("gif", {}).get("urls", {})
        # Prefer hd > sd > poster
        return urls.get("hd") or urls.get("sd") or urls.get("poster")
    except Exception:
        return None


def fetch_og_image(page_url, timeout=10):
    """Fetch og:image or twitter:image meta tag from any web page."""
    try:
        r = requests.get(page_url, headers=HEADERS_BROWSER, timeout=timeout)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.content[:50000], "html.parser")
        for prop in ["og:image", "twitter:image", "og:image:url"]:
            tag = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
            if tag and tag.get("content"):
                return tag["content"].strip()
        return None
    except Exception:
        return None


def download_post_media(media_items, dest_dir, post_label, max_items=10):
    if not media_items:
        return []
    os.makedirs(dest_dir, exist_ok=True)
    saved = []
    for i, (mt, url) in enumerate(media_items[:max_items]):
        actual_url = url

        # RedGifs: resolve page URL -> direct mp4 via their public API
        if "redgifs.com" in url.lower():
            resolved = resolve_redgifs_url(url)
            if resolved:
                actual_url = resolved
                mt = "video"
                log(f"    [redgifs] resolved: {url[:50]} -> mp4")
            else:
                log(f"    [redgifs] could not resolve: {url[:60]}")
                saved.append({"type": "redgifs_link", "url": url, "local_path": None})
                continue

        path = actual_url.split("?")[0].lower()
        ext = next((e for e in MEDIA_EXTENSIONS if path.endswith(e)), None)
        if not ext:
            ext = ".mp4" if mt == "video" else ".jpg"
        elif ext == ".jpeg":
            ext = ".jpg"
        fname = f"{i+1:02d}_{mt}{ext}"
        dest_path = os.path.join(dest_dir, fname)
        if download_file(actual_url, dest_path):
            log(f"    [media] saved {post_label[:35]} -> {fname}")
            saved.append({"type": mt, "url": actual_url, "local_path": dest_path})
        else:
            try:
                if os.path.exists(dest_path):
                    os.remove(dest_path)
            except Exception:
                pass
    return saved


def reddit_search_posts(topic, pages=3, sort="relevance", time_filter="year"):
    """Site-wide Reddit search. Returns ONLY posts where title/subreddit is
    actually relevant -- filters out viral unrelated posts that rank high
    purely on upvote count (relationship advice, movie reviews, etc.)."""
    results = []
    after = None

    # Build a relevance filter from topic keywords (word-boundary matched)
    topic_words = set(
        w.lower() for w in re.findall(r'[a-zA-Z]{3,}', topic)
        if w.lower() not in STOPWORDS
    )

    def _is_relevant(child):
        """Return True if this post is actually about the topic, not just popular."""
        if not topic_words:
            return True
        d = child.get('data', {})
        title = (d.get('title') or '').lower()
        sub = (d.get('subreddit') or '').lower()
        selftext = (d.get('selftext') or '').lower()[:500]
        combined = title + ' ' + sub + ' ' + selftext
        # Must match at least one topic keyword
        return any(w in combined for w in topic_words)

    for page in range(pages):
        if STOP_FLAG:
            break
        url = (f"https://www.reddit.com/search.json?q={quote_plus(topic)}"
               f"&sort={sort}&t={time_filter}&limit=100")
        if after:
            url += f"&after={after}"
        data = fetch_json_cf(url)
        if not data or 'data' not in data:
            log(f"    [reddit] page {page+1}: no data (blocked or empty)")
            break
        children = data['data'].get('children', [])
        if not children:
            break
        # Filter before adding
        relevant = [c for c in children if _is_relevant(c)]
        skipped = len(children) - len(relevant)
        results.extend(relevant)
        after = data['data'].get('after')
        log(f"    [reddit] page {page+1}: {len(children)} posts, "
            f"{len(relevant)} relevant ({skipped} irrelevant filtered) "
            f"(total {len(results)})")
        if not after:
            break
    return results


def reddit_search_subreddit(subreddit, topic, limit=25):
    url = (f"https://www.reddit.com/r/{subreddit}/search.json?q={quote_plus(topic)}"
           f"&restrict_sr=1&sort=relevance&limit={limit}")
    data = fetch_json_cf(url)
    if not data or 'data' not in data:
        return []
    return data['data'].get('children', [])


def reddit_fetch_post_detail(permalink):
    safe_url = f"https://old.reddit.com{permalink.rstrip('/')}.json?limit=500&depth=50"
    return fetch_json_cf(safe_url)


def process_reddit_post(topic, index, child, sub_dirs, media_dir=None,
                         download_media=True, max_media_per_post=10):
    if STOP_FLAG:
        return None
    post_data = child.get('data', {})
    title = post_data.get('title', f"post_{index}")
    permalink = post_data.get('permalink', "")
    if not permalink:
        return None

    log(f"  -> [reddit {index+1}] {title[:70]}")
    detail = reddit_fetch_post_detail(permalink)

    selftext = clean_text(post_data.get('selftext', ""))
    comments = []
    full_post_data = post_data

    if detail and len(detail) >= 1:
        try:
            full_post_data = detail[0]['data']['children'][0]['data']
            selftext = clean_text(full_post_data.get('selftext', "")) or selftext
        except Exception:
            pass
        if len(detail) > 1:
            comments = parse_comment_tree(detail[1], title)

    if detail:
        dump_raw(sub_dirs, "reddit", f"{index+1:03d}_{title}", detail)

    media_files = []
    if download_media and media_dir:
        media_items = extract_reddit_media(full_post_data)
        if media_items:
            post_dir = os.path.join(media_dir, f"{index+1:03d}_{clean_filename(title)}")
            media_files = download_post_media(
                media_items, post_dir, title[:40], max_items=max_media_per_post
            )

    return {
        "source": "reddit",
        "subreddit": post_data.get("subreddit", ""),
        "title": title,
        "text": selftext or title,
        "author": full_post_data.get("author", "[deleted]"),
        "url": f"https://reddit.com{permalink}",
        "score": full_post_data.get("score", 0),
        "num_comments": full_post_data.get("num_comments", len(comments)),
        "created_utc": full_post_data.get("created_utc"),
        "top_comments": sorted(comments, key=lambda c: c.get("upvotes", 0), reverse=True)[:50],
        "media": media_files,
        "category": "reddit",
        "scraped_at": now_iso(),
    }


def collect_reddit(topic, sub_dirs, niche_config, media_dir=None, pages=3,
                   max_workers=5, download_media=True, max_media_per_post=10,
                   max_niche_subs=15, allow_retry=True, niche_only=False):
    global REDDIT_BLOCKED_DETECTED
    REDDIT_BLOCKED_DETECTED = False
    log("[REDDIT] starting...")

    posts = []

    # 1. Site-wide search -- SKIPPED if niche_only=True because site-wide
    # search returns viral unrelated posts (relationship advice, movie reviews)
    # that happen to score high on upvotes regardless of query relevance.
    # Niche subreddit search (step 2) is always more accurate for technical topics.
    if not niche_only:
        posts = reddit_search_posts(topic, pages=pages)

        if not posts and REDDIT_BLOCKED_DETECTED:
            log("    [reddit] !! Blocked -- waiting ~35s then retrying once...")
            time.sleep(random.uniform(30, 40))
            posts = reddit_search_posts(topic, pages=max(1, pages // 2))
    else:
        log("    [reddit] niche-only mode -- skipping site-wide search "
            "(avoids irrelevant viral posts), using curated subreddits only")

    # 2. Niche subreddit-targeted search -- always more relevant than site-wide
    niche_subs, matched_niches = [], []
    if not REDDIT_BLOCKED_DETECTED:
        niche_subs, matched_niches = niche_subreddits_for_topic(
            topic, niche_config, max_subs=max_niche_subs
        )
    if niche_subs:
        log(f"    [reddit] niche match: {matched_niches} -> searching {len(niche_subs)} subreddits")
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(reddit_search_subreddit, sub, topic): sub for sub in niche_subs}
            seen_ids = {c.get('data', {}).get('id') for c in posts}
            for fut in as_completed(futs):
                sub = futs[fut]
                try:
                    sub_posts = fut.result()
                    new_posts = [p for p in sub_posts
                                 if p.get('data', {}).get('id') not in seen_ids]
                    for p in new_posts:
                        seen_ids.add(p.get('data', {}).get('id'))
                    posts.extend(new_posts)
                    if new_posts:
                        log(f"    [reddit] r/{sub}: +{len(new_posts)} posts")
                except Exception:
                    pass

    # 3. Auto-retry with reworded query if still 0
    if not posts and REDDIT_BLOCKED_DETECTED:
        log("    [reddit] still rate-limited -- giving up on Reddit for this run.")
    elif not posts and allow_retry and not niche_only:
        for level in [1, 2]:
            retry_q = reword_query(topic, niche_config, attempt=level)
            if retry_q.lower() != topic.lower():
                log(f"    [reddit] 0 results, retry level {level}: '{retry_q}'")
                posts = reddit_search_posts(retry_q, pages=max(1, pages // 2))
                if posts:
                    break

    if not posts:
        log("    Reddit returned nothing (rate-limited or cookie expired?)")
        return []

    dump_raw(sub_dirs, "reddit", "_search_results_raw", posts)

    items = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [
            ex.submit(process_reddit_post, topic, i, c, sub_dirs,
                      media_dir, download_media, max_media_per_post)
            for i, c in enumerate(posts)
        ]
        for fut in as_completed(futures):
            try:
                item = fut.result()
                if item:
                    items.append(item)
            except Exception:
                pass

    media_count = sum(len(it.get("media", [])) for it in items)
    log(f"    [REDDIT] OK -- {len(items)} posts (with comments)"
        + (f", {media_count} media files" if download_media and media_count else ""))
    return items


# ─────────────────────────────────────────────────────────────
# SOURCE 2 — HACKER NEWS (Algolia)
# 3-level retry: full -> 4-word -> 1-keyword
# ─────────────────────────────────────────────────────────────

def _hn_search_once(query, limit):
    url = (f"https://hn.algolia.com/api/v1/search?query={quote_plus(query)}"
           f"&hitsPerPage={limit}&tags=story")
    r = requests.get(url, headers=HEADERS_API, timeout=15)
    if r.status_code != 200:
        log(f"    FAIL -- HTTP {r.status_code}")
        return [], None
    data = r.json()
    return data.get("hits", []), data


def collect_hackernews(topic, sub_dirs, niche_config, limit=50, allow_retry=True):
    log("[HACKERNEWS] starting...")
    try:
        hits, raw = _hn_search_once(topic, limit)
        used_query = topic

        # 3-level progressive retry (HN Algolia matches poorly on long phrases)
        if allow_retry:
            for level in [1, 2, 3]:
                if hits:
                    break
                retry_q = reword_query(topic, niche_config, attempt=level)
                if retry_q.lower() != used_query.lower():
                    log(f"    [hn] 0 hits, retry {level}: '{retry_q}'")
                    hits, raw = _hn_search_once(retry_q, limit)
                    used_query = retry_q

        if raw is not None:
            dump_raw(sub_dirs, "hackernews", "_search_results_raw", raw)
        if not hits:
            log(f"    [HACKERNEWS] OK -- 0 stories (query: '{used_query}')")
            return []

        items = []
        for hit in hits:
            title = hit.get("title", "")
            if not title:
                continue
            story_id = hit.get("objectID", "")
            top_comments = []
            try:
                cr = requests.get(
                    f"https://hn.algolia.com/api/v1/items/{story_id}",
                    headers=HEADERS_API, timeout=15
                )
                if cr.status_code == 200:
                    cdata = cr.json()
                    dump_raw(sub_dirs, "hackernews", f"item_{story_id}", cdata)
                    def flatten_hn(node):
                        out = []
                        for child in node.get("children", []) or []:
                            if child.get("text"):
                                out.append({
                                    "author": child.get("author", ""),
                                    "body": clean_text(child.get("text", "")),
                                    "points": child.get("points", 0) or 0,
                                })
                            out.extend(flatten_hn(child))
                        return out
                    top_comments = sorted(flatten_hn(cdata),
                                          key=lambda c: c["points"], reverse=True)[:25]
            except Exception:
                pass

            items.append({
                "source": "hackernews",
                "title": title,
                "text": hit.get("story_text", "") or title,
                "author": hit.get("author", ""),
                "url": hit.get("url") or f"https://news.ycombinator.com/item?id={story_id}",
                "score": hit.get("points", 0),
                "num_comments": hit.get("num_comments", 0),
                "created_utc": hit.get("created_at_i"),
                "top_comments": top_comments,
                "category": "hackernews",
                "scraped_at": now_iso(),
            })
            safe_sleep(0.2, 0.5)
        log(f"    [HACKERNEWS] OK -- {len(items)} stories (query: '{used_query}')")
        return items
    except Exception as e:
        log(f"    [HACKERNEWS] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 3 — ARXIV
# ─────────────────────────────────────────────────────────────

def collect_arxiv(topic, sub_dirs, limit=40):
    log("[ARXIV] starting...")
    try:
        url = (f"http://export.arxiv.org/api/query?search_query=all:{quote_plus(topic)}"
               f"&sortBy=relevance&sortOrder=descending&max_results={limit}")
        feed = feedparser.parse(url)
        dump_raw(sub_dirs, "arxiv", "_search_results_raw",
                 {"entries": [dict(e) for e in feed.entries]})
        items = []
        for e in feed.entries:
            items.append({
                "source": "arxiv",
                "title": e.title.replace("\n", " ").strip(),
                "text": e.summary.replace("\n", " ").strip(),
                "author": ", ".join(a.get("name", "") for a in e.get("authors", [])),
                "url": e.link,
                "score": None,
                "num_comments": 0,
                "created_utc": e.get("published", ""),
                "top_comments": [],
                "category": "arxiv",
                "scraped_at": now_iso(),
            })
        log(f"    [ARXIV] OK -- {len(items)} papers")
        return items
    except Exception as e:
        log(f"    [ARXIV] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 4 — GITHUB
# ─────────────────────────────────────────────────────────────

def _github_search_once(query, limit):
    url = (f"https://api.github.com/search/repositories?q={quote_plus(query)}"
           f"&sort=stars&order=desc&per_page={limit}")
    r = requests.get(url,
                     headers={**HEADERS_API, "Accept": "application/vnd.github+json"},
                     timeout=15)
    remaining = r.headers.get("X-RateLimit-Remaining", "?")
    if r.status_code != 200:
        log(f"    FAIL -- HTTP {r.status_code} | rate-remaining: {remaining}")
        try:
            log(f"    body: {r.json().get('message', '')[:200]}")
        except Exception:
            pass
        return [], remaining
    return r.json().get("items", []), remaining


def collect_github(topic, sub_dirs, niche_config, limit=30, allow_retry=True):
    log("[GITHUB] starting...")
    try:
        repos, remaining = _github_search_once(topic, limit)
        used_query = topic

        if allow_retry:
            for level in [1, 2]:
                if repos:
                    break
                retry_q = reword_query(topic, niche_config, attempt=level)
                if retry_q.lower() != used_query.lower():
                    log(f"    [github] 0 repos, retry {level}: '{retry_q}'")
                    repos, remaining = _github_search_once(retry_q, limit)
                    used_query = retry_q

        log(f"    (rate-remaining: {remaining})")
        if repos:
            dump_raw(sub_dirs, "github", "_search_results_raw", {"items": repos})

        items = []
        for repo in repos:
            items.append({
                "source": "github",
                "title": repo.get("full_name", ""),
                "text": (f"{repo.get('description') or 'No description'} | "
                         f"Stars:{repo.get('stargazers_count', 0)} | "
                         f"Lang:{repo.get('language', 'N/A')} | "
                         f"Topics:{', '.join(repo.get('topics', []))}"),
                "author": repo.get("owner", {}).get("login", ""),
                "url": repo.get("html_url", ""),
                "score": repo.get("stargazers_count", 0),
                "num_comments": repo.get("open_issues_count", 0),
                "created_utc": repo.get("created_at", ""),
                "top_comments": [],
                "category": "github",
                "scraped_at": now_iso(),
            })
        log(f"    [GITHUB] OK -- {len(items)} repos (query: '{used_query}')")
        return items
    except Exception as e:
        log(f"    [GITHUB] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 5 — GOOGLE NEWS RSS
# ─────────────────────────────────────────────────────────────

def collect_google_news(topic, sub_dirs, limit=40):
    log("[GOOGLE_NEWS] starting...")
    try:
        url = f"https://news.google.com/rss/search?q={quote_plus(topic)}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(url)
        dump_raw(sub_dirs, "google_news", "_search_results_raw",
                 {"entries": [dict(e) for e in feed.entries]})
        items = []
        for e in feed.entries[:limit]:
            source_name = ""
            if hasattr(e, "source") and isinstance(e.source, dict):
                source_name = e.source.get("title", "")
            items.append({
                "source": f"google_news:{source_name or 'unknown'}",
                "title": e.get("title", ""),
                "text": clean_text(e.get("summary", "")) or e.get("title", ""),
                "author": source_name,
                "url": e.get("link", ""),
                "score": None,
                "num_comments": 0,
                "created_utc": e.get("published", ""),
                "top_comments": [],
                "category": "news",
                "scraped_at": now_iso(),
            })
        log(f"    [GOOGLE_NEWS] OK -- {len(items)} articles")
        return items
    except Exception as e:
        log(f"    [GOOGLE_NEWS] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 6 — STACK OVERFLOW
# ─────────────────────────────────────────────────────────────

def _so_search_once(query, limit):
    url = ("https://api.stackexchange.com/2.3/search/advanced"
           f"?order=desc&sort=relevance&q={quote_plus(query)}"
           f"&site=stackoverflow&pagesize={limit}&withbody=true")
    r = requests.get(url, headers=HEADERS_API, timeout=15)
    if r.status_code != 200:
        log(f"    FAIL -- HTTP {r.status_code}")
        return [], None
    data = r.json()
    if data.get("error_id"):
        log(f"    API ERROR -- {data.get('error_name')}: {data.get('error_message')}")
        return [], data
    return data.get("items", []), data


def collect_stackoverflow(topic, sub_dirs, niche_config, limit=30, allow_retry=True):
    log("[STACKOVERFLOW] starting...")
    try:
        questions, raw = _so_search_once(topic, limit)
        used_query = topic

        if not questions and allow_retry:
            for level in [1, 2]:
                retry_q = reword_query(topic, niche_config, attempt=level)
                if retry_q.lower() != used_query.lower():
                    log(f"    [so] 0 results, retry {level}: '{retry_q}'")
                    questions, raw = _so_search_once(retry_q, limit)
                    used_query = retry_q
                    if questions:
                        break

        if raw is not None:
            log(f"    (quota remaining: {raw.get('quota_remaining', '?')})")
            dump_raw(sub_dirs, "stackoverflow", "_search_results_raw", raw)

        items = []
        for q in questions:
            q_id = q.get("question_id")
            top_answers = []
            if q.get("answer_count", 0) > 0 and q_id:
                try:
                    safe_sleep(0.2, 0.5)
                    ar = requests.get(
                        f"https://api.stackexchange.com/2.3/questions/{q_id}/answers"
                        f"?order=desc&sort=votes&site=stackoverflow&withbody=true&pagesize=5",
                        headers=HEADERS_API, timeout=15
                    )
                    if ar.status_code == 200:
                        adata = ar.json()
                        dump_raw(sub_dirs, "stackoverflow", f"answers_{q_id}", adata)
                        for a in adata.get("items", []):
                            top_answers.append({
                                "author": a.get("owner", {}).get("display_name", ""),
                                "body": clean_text(a.get("body", "")),
                                "points": a.get("score", 0),
                                "is_accepted": a.get("is_accepted", False),
                            })
                except Exception:
                    pass

            items.append({
                "source": "stackoverflow",
                "title": q.get("title", ""),
                "text": clean_text(q.get("body", "")) or q.get("title", ""),
                "author": q.get("owner", {}).get("display_name", ""),
                "url": q.get("link", ""),
                "score": q.get("score", 0),
                "num_comments": q.get("answer_count", 0),
                "created_utc": q.get("creation_date"),
                "top_comments": top_answers,
                "category": "stackoverflow",
                "scraped_at": now_iso(),
            })
        log(f"    [STACKOVERFLOW] OK -- {len(items)} questions (query: '{used_query}')")
        return items
    except Exception as e:
        log(f"    [STACKOVERFLOW] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 7 — MEDIUM (tag RSS, best-effort)
# ─────────────────────────────────────────────────────────────

def collect_medium(topic, sub_dirs, niche_config, limit=20, allow_retry=True):
    log("[MEDIUM] starting...")
    try:
        def _fetch(q):
            tag = re.sub(r'[^a-z0-9\- ]', '', q.lower()).strip().replace(" ", "-")
            feed = feedparser.parse(f"https://medium.com/feed/tag/{tag}")
            return feed.entries, tag

        entries, tag = _fetch(topic)
        if not entries and allow_retry:
            for level in [1, 2]:
                retry_q = reword_query(topic, niche_config, attempt=level)
                entries2, tag2 = _fetch(retry_q)
                if entries2:
                    entries, tag = entries2, tag2
                    break

        if not entries:
            log(f"    SKIP -- no Medium tag matched '{tag}'")
            return []
        dump_raw(sub_dirs, "medium", "_search_results_raw",
                 {"entries": [dict(e) for e in entries]})
        items = []
        for e in entries[:limit]:
            items.append({
                "source": "medium",
                "title": e.get("title", ""),
                "text": clean_text(e.get("summary", "")) or e.get("title", ""),
                "author": e.get("author", ""),
                "url": e.get("link", ""),
                "score": None,
                "num_comments": 0,
                "created_utc": e.get("published", ""),
                "top_comments": [],
                "category": "medium",
                "scraped_at": now_iso(),
            })
        log(f"    [MEDIUM] OK -- {len(items)} articles (tag: {tag})")
        return items
    except Exception as e:
        log(f"    [MEDIUM] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 8 — DUCKDUCKGO WEB SEARCH
# HTML scrape of html.duckduckgo.com (no key needed)
# Falls back to lite.duckduckgo.com on parse failure
# ─────────────────────────────────────────────────────────────

def _ddg_search_once(query, limit):
    """Try html DDG first, fallback to lite DDG."""
    results = []
    for ddg_url, selector in [
        (f"https://html.duckduckgo.com/html/?q={quote_plus(query)}",
         {"result": "div.result", "title": "a.result__a", "snip": "a.result__snippet"}),
        (f"https://lite.duckduckgo.com/lite/?q={quote_plus(query)}",
         {"result": "tr", "title": "a.result-link", "snip": "td.result-snippet"}),
    ]:
        if results:
            break
        try:
            kwargs = dict(headers=HEADERS_BROWSER, timeout=20)
            if HAVE_CURL_CFFI:
                kwargs["impersonate"] = "chrome120"
            r = cf_requests.get(ddg_url, **kwargs)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.content, "html.parser")
            for block in soup.select(selector["result"]):
                ta = block.select_one(selector["title"])
                sa = block.select_one(selector["snip"])
                if not ta:
                    continue
                link  = ta.get("href", "")
                title = clean_text(ta.get_text(" ", strip=True))
                snip  = clean_text(sa.get_text(" ", strip=True)) if sa else ""
                if not link or not title:
                    continue
                results.append({"title": title, "url": link, "snippet": snip})
                if len(results) >= limit:
                    break
        except Exception:
            continue
    return results


def collect_web_search(topic, sub_dirs, niche_config, limit=30, allow_retry=True):
    log("[WEB_SEARCH] starting...")
    try:
        results = _ddg_search_once(topic, limit)
        used_query = topic

        if not results and allow_retry:
            for level in [1, 2]:
                retry_q = reword_query(topic, niche_config, attempt=level)
                if retry_q.lower() != used_query.lower():
                    log(f"    [ddg] 0 results, retry {level}: '{retry_q}'")
                    results = _ddg_search_once(retry_q, limit)
                    used_query = retry_q
                    if results:
                        break

        if results:
            dump_raw(sub_dirs, "web_search", "_search_results_raw", results)

        # Fetch og:image from each result page in parallel (max 5 workers,
        # skip URLs that are clearly non-blog like GitHub/SO/arxiv)
        _skip_domains = ("github.com", "stackoverflow.com", "arxiv.org",
                         "youtube.com", "twitter.com", "x.com", "reddit.com")

        def _fetch_og(r):
            url = r["url"]
            if any(d in url for d in _skip_domains):
                return r, []
            media_dir_ws = os.path.join(sub_dirs["web_search"],
                                        clean_filename(r["title"][:50]))
            img_url = fetch_og_image(url)
            if not img_url:
                return r, []
            os.makedirs(media_dir_ws, exist_ok=True)
            ext = "." + img_url.split("?")[0].rsplit(".", 1)[-1].lower()
            if ext not in MEDIA_EXTENSIONS:
                ext = ".jpg"
            dest = os.path.join(media_dir_ws, f"og{ext}")
            if download_image_plain(img_url, dest):
                log(f"    [ddg media] {r['title'][:40]} -> og{ext}")
                return r, [{"type": "og_image", "url": img_url, "local_path": dest}]
            return r, []

        result_media = {}
        with ThreadPoolExecutor(max_workers=5) as ex:
            futs = {ex.submit(_fetch_og, r): r for r in results}
            for fut in as_completed(futs):
                try:
                    r, media = fut.result()
                    result_media[r["url"]] = media
                except Exception:
                    pass

        items = [{
            "source": "web_search:duckduckgo",
            "title": r["title"],
            "text": r["snippet"] or r["title"],
            "author": "",
            "url": r["url"],
            "score": None,
            "num_comments": 0,
            "created_utc": "",
            "top_comments": [],
            "media": result_media.get(r["url"], []),
            "category": "web_search",
            "scraped_at": now_iso(),
        } for r in results]

        log(f"    [WEB_SEARCH] OK -- {len(items)} results (query: '{used_query}')")
        return items
    except Exception as e:
        log(f"    [WEB_SEARCH] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 9 — DEV.TO  [NEW]
# Free JSON API, no key. Great for tech/fintech/AI/devops topics.
# /api/articles?tag=X returns full articles with reactions + comments
# ─────────────────────────────────────────────────────────────

def collect_devto(topic, sub_dirs, niche_config, limit=30, allow_retry=True):
    log("[DEVTO] starting...")
    try:
        # Collect candidate tags: topic slug + niche devto_tags
        niches = match_niches(topic, niche_config, max_niches=1)
        niche_tags = []
        if niches:
            niche_tags = niche_config[niches[0]].get("devto_tags", [])

        # Topic -> tag slug (e.g. "fintech adoption" -> "fintech")
        topic_slug_tag = re.sub(r'[^a-z0-9]', '',
                                 topic.lower().replace(' ', '').replace('-', ''))[:25]
        topic_first_word = re.sub(r'[^a-z0-9]', '',
                                   (re.findall(r'[a-z0-9]+', topic.lower()) or [""])[0])[:20]

        tags_to_try = list(dict.fromkeys(
            [topic_slug_tag, topic_first_word] + niche_tags[:5]
        ))

        all_articles = []
        seen_ids = set()

        for tag in tags_to_try:
            if not tag or len(tag) < 2:
                continue
            for state in ["fresh", "rising"]:
                url = f"https://dev.to/api/articles?tag={tag}&per_page={limit}&state={state}"
                try:
                    r = requests.get(url, headers=HEADERS_API, timeout=15)
                    if r.status_code == 200:
                        for a in r.json():
                            if a.get("id") not in seen_ids:
                                seen_ids.add(a.get("id"))
                                all_articles.append(a)
                except Exception:
                    pass
                safe_sleep(0.15, 0.3)

        if not all_articles and allow_retry:
            retry_q = reword_query(topic, niche_config, attempt=1)
            retry_tag = re.sub(r'[^a-z0-9]', '',
                                retry_q.split()[0] if retry_q.split() else "")[:20]
            if retry_tag and retry_tag != topic_first_word:
                log(f"    [devto] 0 articles, retry tag: '{retry_tag}'")
                try:
                    r = requests.get(
                        f"https://dev.to/api/articles?tag={retry_tag}&per_page={limit}",
                        headers=HEADERS_API, timeout=15
                    )
                    if r.status_code == 200:
                        all_articles = r.json()
                except Exception:
                    pass

        if not all_articles:
            log(f"    [DEVTO] 0 articles (no matching tags for '{topic}')")
            return []

        dump_raw(sub_dirs, "devto", "_search_results_raw", all_articles[:limit])

        items = []
        for a in all_articles[:limit]:
            media = []
            cover = a.get("cover_image") or a.get("social_image")
            if cover:
                art_dir = os.path.join(sub_dirs["devto"],
                                       clean_filename(a.get("title", f"article_{a.get('id','')}")[:50]))
                os.makedirs(art_dir, exist_ok=True)
                ext = "." + cover.split("?")[0].rsplit(".", 1)[-1].lower()
                if ext not in MEDIA_EXTENSIONS:
                    ext = ".jpg"
                dest = os.path.join(art_dir, f"cover{ext}")
                if download_image_plain(cover, dest):
                    log(f"    [devto media] {a.get('title','')[:40]} -> cover{ext}")
                    media.append({"type": "cover", "url": cover, "local_path": dest})
            items.append({
                "source": "devto",
                "title": a.get("title", ""),
                "text": clean_text(a.get("description", "") or a.get("title", "")),
                "author": (a.get("user") or {}).get("name", ""),
                "url": a.get("url", ""),
                "score": a.get("public_reactions_count", 0),
                "num_comments": a.get("comments_count", 0),
                "created_utc": a.get("published_at", ""),
                "top_comments": [],
                "tags": a.get("tag_list", []),
                "media": media,
                "category": "devto",
                "scraped_at": now_iso(),
            })
        log(f"    [DEVTO] OK -- {len(items)} articles (tags tried: {tags_to_try})")
        return items
    except Exception as e:
        log(f"    [DEVTO] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 10 — LOBSTERS  [NEW]
# lobste.rs — tech-focused link aggregator, HN alternative.
# Has a clean JSON search endpoint with no auth.
# ─────────────────────────────────────────────────────────────

def collect_lobsters(topic, sub_dirs, niche_config, limit=30, allow_retry=True):
    log("[LOBSTERS] starting...")
    try:
        def _fetch(q):
            url = (f"https://lobste.rs/search.json?q={quote_plus(q)}"
                   f"&what=stories&order=relevance")
            r = requests.get(url, headers=HEADERS_API, timeout=20)
            if r.status_code != 200:
                log(f"    FAIL -- HTTP {r.status_code}")
                return []
            data = r.json()
            # lobsters returns either a list or {"hits": [...]}
            if isinstance(data, list):
                return data
            return data.get("hits", [])

        stories = _fetch(topic)
        used_query = topic

        if not stories and allow_retry:
            for level in [1, 2]:
                retry_q = reword_query(topic, niche_config, attempt=level)
                if retry_q.lower() != used_query.lower():
                    log(f"    [lobsters] 0 results, retry {level}: '{retry_q}'")
                    stories = _fetch(retry_q)
                    used_query = retry_q
                    if stories:
                        break

        if stories:
            dump_raw(sub_dirs, "lobsters", "_search_results_raw", stories)

        items = []
        for story in stories[:limit]:
            submitter = story.get("submitter_user") or {}
            items.append({
                "source": "lobsters",
                "title": story.get("title", ""),
                "text": clean_text(story.get("description", "") or story.get("title", "")),
                "author": submitter.get("username", "") if isinstance(submitter, dict) else str(submitter),
                "url": story.get("url", "") or story.get("short_id_url", ""),
                "score": story.get("score", 0),
                "num_comments": story.get("comment_count", 0),
                "created_utc": story.get("created_at", ""),
                "top_comments": [],
                "category": "lobsters",
                "scraped_at": now_iso(),
            })
        log(f"    [LOBSTERS] OK -- {len(items)} stories (query: '{used_query}')")
        return items
    except Exception as e:
        log(f"    [LOBSTERS] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# SOURCE 11 — NICHE RSS  [NEW]
# Per-niche curated RSS feeds from niche_config.json.
# Only activates when the topic matches a niche with rss_feeds.
# Filters entries by topic-word relevance before including them.
# ─────────────────────────────────────────────────────────────

def collect_niche_rss(topic, sub_dirs, niche_config, limit=40):
    log("[NICHE_RSS] starting...")
    try:
        niches = match_niches(topic, niche_config, max_niches=2)
        if not niches:
            log("    SKIP -- no niche matched topic")
            return []

        feeds_to_try = []
        for niche in niches:
            feeds_to_try.extend(niche_config[niche].get("rss_feeds", []))
        feeds_to_try = list(dict.fromkeys(feeds_to_try))  # dedup while preserving order

        if not feeds_to_try:
            log("    SKIP -- no rss_feeds in matched niches")
            return []

        # Build a relevance filter from the topic
        topic_words = set(
            w.lower() for w in re.findall(r'[a-zA-Z]{4,}', topic)
            if w.lower() not in STOPWORDS
        )

        def _relevant(entry):
            if not topic_words:
                return True
            text = ((entry.get("title") or "") + " " + (entry.get("summary") or "")).lower()
            return any(w in text for w in topic_words)

        items = []
        seen_urls = set()

        for feed_url in feeds_to_try[:10]:  # cap at 10 feeds
            try:
                feed = feedparser.parse(feed_url)
                feed_name = (feed.feed.get("title") or "") or feed_url.split("/")[2]
                added = 0
                for e in feed.entries:
                    if not _relevant(e):
                        continue
                    url = e.get("link", "")
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)

                    # Extract image from RSS enclosure, media:thumbnail, or media:content
                    img_url = None
                    enclosures = e.get("enclosures", [])
                    if enclosures:
                        for enc in enclosures:
                            mime = enc.get("type", "")
                            if "image" in mime or "video" in mime:
                                img_url = enc.get("href") or enc.get("url")
                                break
                    if not img_url:
                        media_thumb = e.get("media_thumbnail", [])
                        if media_thumb:
                            img_url = media_thumb[0].get("url")
                    if not img_url:
                        media_content = e.get("media_content", [])
                        if media_content:
                            img_url = media_content[0].get("url")

                    media = []
                    if img_url:
                        art_dir = os.path.join(sub_dirs["niche_rss"],
                                               clean_filename(e.get("title", "article")[:50]))
                        os.makedirs(art_dir, exist_ok=True)
                        ext = "." + img_url.split("?")[0].rsplit(".", 1)[-1].lower()
                        if ext not in MEDIA_EXTENSIONS:
                            ext = ".jpg"
                        dest = os.path.join(art_dir, f"cover{ext}")
                        if download_image_plain(img_url, dest):
                            log(f"    [rss media] {e.get('title','')[:40]} -> cover{ext}")
                            media.append({"type": "cover", "url": img_url, "local_path": dest})

                    items.append({
                        "source": f"rss:{feed_name}",
                        "title": clean_text(e.get("title", "")),
                        "text": clean_text(e.get("summary", "")) or clean_text(e.get("title", "")),
                        "author": e.get("author", "") or feed_name,
                        "url": url,
                        "score": None,
                        "num_comments": 0,
                        "created_utc": e.get("published", ""),
                        "top_comments": [],
                        "media": media,
                        "category": "niche_rss",
                        "scraped_at": now_iso(),
                    })
                    added += 1
                    if len(items) >= limit:
                        break
                if added:
                    log(f"    [rss] {feed_name}: +{added} articles")
            except Exception as ef:
                log(f"    [NICHE_RSS] feed failed ({feed_url[:60]}): {ef}")
            if len(items) >= limit:
                break

        if items:
            dump_raw(sub_dirs, "niche_rss", "_combined_raw",
                     {"feeds": feeds_to_try, "niches": niches, "count": len(items)})

        log(f"    [NICHE_RSS] OK -- {len(items)} articles from {len(feeds_to_try)} feeds")
        return items
    except Exception as e:
        log(f"    [NICHE_RSS] FAIL -- {e}")
        return []


# ─────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Universal free-source research pipeline v4")
    parser.add_argument("topic", type=str, help="Topic/query to research")
    parser.add_argument("--pages", type=int, default=3, help="Reddit site-wide search pages (default 3)")
    parser.add_argument("--hn-limit", type=int, default=50)
    parser.add_argument("--arxiv-limit", type=int, default=40)
    parser.add_argument("--github-limit", type=int, default=30)
    parser.add_argument("--news-limit", type=int, default=40)
    parser.add_argument("--so-limit", type=int, default=30)
    parser.add_argument("--medium-limit", type=int, default=20)
    parser.add_argument("--web-limit", type=int, default=30)
    parser.add_argument("--devto-limit", type=int, default=30)
    parser.add_argument("--lobsters-limit", type=int, default=30)
    parser.add_argument("--rss-limit", type=int, default=40)
    parser.add_argument("--niche-subs", type=int, default=15,
                         help="Max curated subreddits to search (default 15)")
    parser.add_argument("--skip", type=str, default="",
                         help="Comma-separated sources to skip: medium,github,lobsters,devto,...")
    parser.add_argument("--short-query", type=str, default="",
                         help="Manual short keyword for HN/GitHub/SO/Medium (overrides auto-retry)")
    parser.add_argument("--skip-media", action="store_true",
                         help="Skip Reddit media download (faster)")
    parser.add_argument("--media-limit", type=int, default=10)
    parser.add_argument("--no-retry", action="store_true",
                         help="Disable auto-retry-with-reworded-query")
    parser.add_argument("--niche-only", action="store_true",
                         help="Reddit: skip site-wide search, use ONLY curated niche "
                              "subreddits. Avoids irrelevant viral posts (relationship "
                              "advice, movie reviews) that rank high on upvotes but have "
                              "nothing to do with the topic. Recommended for technical "
                              "queries like DevOps, k8s, fintech etc.")
    parser.add_argument("--no-auto-skip", action="store_true",
                         help="Disable niche-based auto-skip of structurally irrelevant "
                              "sources (e.g. arxiv/github/stackoverflow for career_jobs "
                              "queries). Auto-skip is ON by default to cut noise.")
    args = parser.parse_args()

    short_q = args.short_query.strip() or args.topic
    allow_retry = not args.no_retry
    skip = set(s.strip().lower() for s in args.skip.split(",") if s.strip())

    niche_config = load_niche_config()
    matched_niches = match_niches(args.topic, niche_config)

    # Auto-skip sources that are structurally the wrong kind of content for
    # the matched niche(s) -- e.g. StackOverflow is pure programming Q&A, and
    # for a "career_jobs" query about LinkedIn profile advice it returns
    # nothing but devs debugging OAuth login buttons. Union across matched
    # niches: if ANY matched niche flags a source irrelevant, skip it (errs
    # toward less noise; --no-auto-skip disables this entirely).
    auto_skip = set()
    if not args.no_auto_skip:
        for niche in matched_niches:
            auto_skip |= set(niche_config.get(niche, {}).get("irrelevant_sources", []))
    skip |= auto_skip

    base_dir = os.path.join("Research_Output", topic_slug(args.topic))
    raw_dir, sub_dirs, media_dir = make_dirs(base_dir)

    log("=" * 65)
    log(f"  UNIVERSAL RESEARCH PIPELINE  v4.5")
    log(f"  (+ RedGifs API resolve, Dev.to/Medium/RSS/DDG media download)")
    log(f"  Topic: {args.topic}")
    log(f"  Output: {os.path.abspath(base_dir)}")
    if matched_niches:
        log(f"  Niche match: {matched_niches}")
    if auto_skip:
        log(f"  Auto-skipping (irrelevant for this niche): {sorted(auto_skip)}"
            f"  [override with --no-auto-skip]")
    log("=" * 65 + "\n")

    if not HAVE_CURL_CFFI:
        log("[!] curl_cffi not installed -- Reddit will likely fail with 403.")
        log("[!] Run: pip install curl_cffi\n")

    # ── Shared state (main-thread only after this point via as_completed) ──
    all_items = []
    source_status = {}

    # ── CRITICAL: flush MD+JSON after every source so Ctrl+C never loses data ──
    def _flush():
        try:
            mp, m = build_master_json(all_items, base_dir, args.topic)
            build_markdown(m, base_dir, args.topic)
            return mp, m
        except Exception as e:
            log(f"  [!] flush write failed: {e}")
            return None, None

    # ── SIGINT: save before dying ──
    def _sigint(sig, frame):
        global STOP_FLAG
        STOP_FLAG = True
        log("\n[STOPPED] Ctrl+C -- saving partial data...")
        mp, m = _flush()
        if mp and m:
            log(f"  {m['total_items']} items saved -> {mp}")
        sys.exit(0)

    signal.signal(signal.SIGINT, _sigint)

    # ── Build collector list ──
    collectors = []
    if "reddit" not in skip:
        collectors.append(("reddit", lambda: collect_reddit(
            args.topic, sub_dirs, niche_config, media_dir=media_dir, pages=args.pages,
            download_media=not args.skip_media, max_media_per_post=args.media_limit,
            max_niche_subs=args.niche_subs, allow_retry=allow_retry,
            niche_only=args.niche_only
        )))
    if "hackernews" not in skip and "hn" not in skip:
        collectors.append(("hackernews", lambda: collect_hackernews(
            short_q, sub_dirs, niche_config, limit=args.hn_limit, allow_retry=allow_retry)))
    if "arxiv" not in skip:
        collectors.append(("arxiv", lambda: collect_arxiv(
            args.topic, sub_dirs, limit=args.arxiv_limit)))
    if "github" not in skip:
        collectors.append(("github", lambda: collect_github(
            short_q, sub_dirs, niche_config, limit=args.github_limit, allow_retry=allow_retry)))
    if "google_news" not in skip and "news" not in skip:
        collectors.append(("google_news", lambda: collect_google_news(
            args.topic, sub_dirs, limit=args.news_limit)))
    if "stackoverflow" not in skip and "so" not in skip:
        collectors.append(("stackoverflow", lambda: collect_stackoverflow(
            short_q, sub_dirs, niche_config, limit=args.so_limit, allow_retry=allow_retry)))
    if "medium" not in skip:
        collectors.append(("medium", lambda: collect_medium(
            short_q, sub_dirs, niche_config, limit=args.medium_limit, allow_retry=allow_retry)))
    if "web_search" not in skip and "duckduckgo" not in skip:
        collectors.append(("web_search", lambda: collect_web_search(
            args.topic, sub_dirs, niche_config, limit=args.web_limit, allow_retry=allow_retry)))
    if "devto" not in skip:
        collectors.append(("devto", lambda: collect_devto(
            args.topic, sub_dirs, niche_config, limit=args.devto_limit, allow_retry=allow_retry)))
    if "lobsters" not in skip:
        collectors.append(("lobsters", lambda: collect_lobsters(
            short_q, sub_dirs, niche_config, limit=args.lobsters_limit, allow_retry=allow_retry)))
    if "niche_rss" not in skip and "rss" not in skip:
        collectors.append(("niche_rss", lambda: collect_niche_rss(
            args.topic, sub_dirs, niche_config, limit=args.rss_limit)))

    log(f"Running {len(collectors)} sources in parallel...\n")

    t0 = time.time()
    interrupted = False

    try:
        with ThreadPoolExecutor(max_workers=len(collectors)) as ex:
            future_to_name = {ex.submit(fn): name for name, fn in collectors}
            for fut in as_completed(future_to_name):
                name = future_to_name[fut]
                try:
                    items = fut.result()
                    all_items.extend(items)
                    source_status[name] = len(items)
                    log(f"\n  ✓ [{name.upper()}] done: {len(items)} items | "
                        f"total on disk: {len(all_items)} -- flushing...")
                    _flush()
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    log(f"    [!] {name} crashed: {type(e).__name__}: {e}")
                    source_status[name] = f"CRASHED: {e}"
                    _flush()  # still flush what we have

    except KeyboardInterrupt:
        global STOP_FLAG
        STOP_FLAG = True
        interrupted = True
        log("\n[STOPPED] Ctrl+C detected in main loop -- saving partial data...")

    # ── Final definitive write (always runs, even after interrupt) ──
    master_path, master = build_master_json(all_items, base_dir, args.topic)
    md_path = build_markdown(master, base_dir, args.topic)

    elapsed = round(time.time() - t0, 1)
    log("\n" + "=" * 65)
    if interrupted:
        log(f"  PARTIAL RUN ({elapsed}s) -- Ctrl+C triggered")
    else:
        log(f"  DONE in {elapsed}s")
    log(f"  Total items: {master['total_items']} ({master['duplicates_removed']} dupes removed)")
    log("")
    for src, count in sorted(master["by_source_count"].items(), key=lambda x: -x[1]):
        bar = "█" * min(int(count / max(master['total_items'], 1) * 30), 30)
        log(f"  {src:20s} {count:4d}  {bar}")

    failed = [name for name, status in source_status.items()
              if isinstance(status, str) or status == 0]
    if failed:
        log(f"\n  [!] Zero results or crashed: {failed}")

    log("")
    log(f"  Raw dumps:   {os.path.abspath(raw_dir)}/<source>/")
    log(f"  Master JSON: {os.path.abspath(master_path)}")
    log(f"  Summary MD:  {os.path.abspath(md_path)}")
    log("=" * 65)
    log("\nNext step: paste the .md file into Claude/ChatGPT/Perplexity for synthesis.")


if __name__ == "__main__":
    main()
