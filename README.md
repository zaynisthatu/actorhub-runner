# Actor Hub

An Apify-style actor runner. Each scraper or data tool is an **actor**, a folder with a manifest, and every run executes in its own Docker container with memory and time limits. A FastAPI service starts runs and keeps the history in SQLite, and one HTML page offers the actor catalog, input forms, run logs and a results table with CSV and JSON download.

## How it works

- **Actor.** `actors/<name>/` holds `actor.json` (title, description, input schema, `timeout_sec`, `memory`, declared `secrets`), a `Dockerfile`, `requirements.txt` and `src/main.py`.
- **Run.** `POST /api/actors/{name}/runs` validates the input against the schema and starts the actor in a container (`docker run --rm` with `--memory`, `--cpus 1`, the caller's `--user`, and the run folder mounted at `/data`). Status and timing are stored in SQLite.
- **Contract.** The actor reads `/data/input.json`, writes rows with `push_data(row)` to `/data/output/dataset.jsonl`, and logs to stdout. Exit code 0 is `SUCCEEDED`, any other code is `FAILED`, and a run that exceeds `timeout_sec` is killed and marked `TIMEOUT`.
- **Secrets.** Values are kept in `secrets.env`. Only the keys an actor lists under `secrets` in `actor.json` are passed into its container.
- **Backends.** `docker` (default) isolates every run. `ACTORHUB_BACKEND=process` runs actors as local subprocesses with the same contract, for development.

## Actors

| Actor | What it does |
|---|---|
| `reddit-scraper` | Posts from a subreddit, or all comments of one thread, from Reddit's public JSON |
| `youtube-transcript` | Transcripts for a list of YouTube videos with a language priority list, one row per video |
| `ytdlp-info` | Title, uploader, duration, views and date for URLs supported by yt-dlp; nothing is downloaded |
| `gallerydl-info` | Media file URLs and metadata for sites supported by gallery-dl; nothing is downloaded |
| `research-pipeline` | One topic searched across Reddit, Hacker News, arXiv, GitHub, StackOverflow, Dev.to, Lobsters, Google News, Medium, DuckDuckGo and RSS feeds, merged into one ranked list |

## Run it

```bash
pip install -r requirements.txt
python selftest.py --backend docker     # builds the images, then checks run, failure, timeout, secrets and cleanup
uvicorn app:app --port 8000             # http://localhost:8000
```

Requires Python 3 and the Docker CLI.

## Verification

- `python selftest.py --backend docker|process` starts the API and runs 13 checks: a successful run, declared and undeclared secrets, dataset and log capture, a failing actor, the timeout kill, cleanup after a timeout, an unknown actor (404), missing input (422), path traversal on run ids (404) and the failure paths of `youtube-transcript`, `ytdlp-info` and `gallerydl-info`.
- `python tests_actors.py` runs offline parser tests for `ytdlp-info`, `gallerydl-info` and the Reddit comment tree.
