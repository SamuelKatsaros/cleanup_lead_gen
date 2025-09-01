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
