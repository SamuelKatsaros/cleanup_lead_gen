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

demo flow: show multi-stream running → show logs updating → trigger a duplicate and show it suppressed → point to envs + logs.

anything in DFW

- DFW (Dallas/Fort Worth + anywhere in the metro)
- Spokane, Spokane Valley, Post Falls, Coeur d'Alene, Deer Park, Chewelah, Newport (top right hand corner of state of WA), top left corner of ID
- Atlanta, GA

Prioritize police and medical examiners
- Put all name, city, county, state, type (police, fire, medical examiner, all, etc.), notes
- Add fire/large fire-related keywords

always add the fire department - they respond to unattended death

any first responder -> add
prioritize police

fire keyword

- Put all URLs into a database (Google Sheets), name of stream, city, county, state, notes, etc.
one tab (rolling), can sort.


just got off a meeting with my boss, here's what he wants:

1. since we've scaled to handle multiple streams, we need to add those said streams. below is waht we're going to add:

note: i'm going to give you the numbers. every stream static url follows this structure: https://audio.broadcastify.com/45513.mp3. thus, when i tell you the name of a stream, i will only give you the numbers like this: 'Dallas City Fire and Rescue: 2681'. the link is then: https://audio.broadcastify.com/2681.mp3'

here are all the names and their respective numbers:
  Dallas City Fire and Rescue: 2681
  Dallas City Police - 1 Central: 5318
  Dallas City Police - North Central Dispatch: 37171
  Arlington Fire Department: 39004
  Grapevine Police, Fire and EMS: 27945
  Johnson County Fire Departments: 35960
  Inland Northwest Fire and Sheriff - Multiple Counties (Scanning Spokane County,Wa “Mutual Aid Only” Stevens County,Wa Pend Oreille County,Wa Bonners,ID Kooteney county,ID WA Department of Natural Resources, Northeast Dispatch): 29133
  Panhandle Public Safety: 38633
  Atlanta Metro Public Safety (Multi Agency Police, Sheriff, Fire, & EMS dispatch audio covering the Atlanta metropolitan area.): 40345

With these, we must track: name, city, county, state, type (police, fire, medical examiner, all, etc.), notes (aka snippet)

In addition, you must add some fire/large fire-related keywords, since we're now tracking fire in addition to police.

Lastly, with this, we must update the google sheet. we need one large tab that has all leads. thus, it's super important that we note every county, state, and if it's fire, police, in addition to what we already have. essentially, just scale the sheet since we've scaled to multiple streams, counties, states, etc..

with this scaling, we must also include the state, county, and any other information necessary for the emails. the person needs to know where it is, if it's a lead in idaho, washington, dallas, etc.

ensure this is perfect, don't overcomplicate. zero errors. 