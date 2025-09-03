prereqs:
  macos:  brew install ffmpeg
  python: 3.10–3.12

setup:
  python -m venv .venv
  source .venv/bin/activate
  pip install -r requirements.txt
  
env:
  cp .env.example .env
  fill all values

run:
  source .venv/bin/activate
  python main.py

purge:
  ./purge_logs.sh


watch live log in another tab

tail -f logs/run.log# cleanup_lead_gen

reset env:
  set -a; source .env; set +a  


keywords: added strict prefilter for homicide/suicide/murder, blood/bleeding, shooting/shots fired/gsw, tear gas/cs gas, unattended death/body found, decomp/decomposition.

dedup: last 5k chars tail + 15-min incident cache; per-stream dedup; env: DEDUP_TAIL_CHARS, DUPLICATE_WINDOW_SECONDS (for cutoffs + updates on long situations)

multi-stream: supports many streams (built for up to ~10); per-stream dirs/logs; semicolon-separated envs (BROADCASTIFY_STREAM_URLS, CITY_STATES, BROADCASTIFY_AUTH_HEADERS) or single-stream envs.

auth/backoff: one shared auth header ok; no empty headers; auto-restart with exponential backoff; graceful ctrl-c shutdown.

transcripts fixed: now write to transcripts/ALL_STREAMS.log + transcripts/streamN.log and per-segment files; also keep rolling/rolling.txt updated for the scanner.

demo flow (if asked): show multi-stream running → show logs updating → trigger a duplicate and show it suppressed → point to envs + logs.