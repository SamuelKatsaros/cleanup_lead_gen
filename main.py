import os, sys, time, subprocess, threading
from pathlib import Path
from dotenv import load_dotenv
from faster_whisper import WhisperModel
from address import extract_address
from classifier import classify, passes_prefilter
from sheets import append_lead
from emailer import send_email
from datetime import datetime

load_dotenv()

ROOT = Path.cwd()
SEG_DIR = ROOT / "audio_segments"
TXT_DIR = ROOT / "transcripts"
LOGS    = ROOT / "logs"
for d in (SEG_DIR, TXT_DIR, LOGS):
    d.mkdir(exist_ok=True)

# now safe to define these
STREAM_URL   = os.getenv("BROADCASTIFY_STREAM_URL")
SEG_SECONDS  = int(os.getenv("SEGMENT_SECONDS", "90"))
CITY_STATE   = os.getenv("CITY_STATE","Dallas, TX")
RET_HRS      = int(os.getenv("RETENTION_HOURS","48"))
AUTH_HDR     = os.getenv("BROADCASTIFY_AUTH_HEADER","")
ROLLING_FILE = Path(os.getenv("ROLLING_TRANSCRIPT","transcripts/rolling.txt"))
ROLLING_FILE.parent.mkdir(parents=True, exist_ok=True)
ROLLING_SCAN  = int(os.getenv("ROLLING_SCAN_SECONDS","3600"))
ROLLING_MAX   = int(os.getenv("ROLLING_MAX_CHARS","1200"))
OFFSET_FILE   = LOGS / "rolling.offset"
PURGE_WAV_AFTER = int(os.getenv("PURGE_WAV_AFTER_SECONDS","60"))

if not STREAM_URL:
    sys.exit("missing BROADCASTIFY_STREAM_URL")

RUNLOG = LOGS / "run.log"

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with RUNLOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

# whisper model
model_size   = os.getenv("WHISPER_MODEL","small.en")
compute_type = os.getenv("WHISPER_COMPUTE","int8")
log(f"loading whisper model={model_size} compute={compute_type}")
whisper_model = WhisperModel(model_size, device="cpu", compute_type=compute_type)

def start_ffmpeg():
    out_pat = str(SEG_DIR / "%Y%m%d-%H%M%S.wav")
    cmd = [
        "ffmpeg",
        "-hide_banner","-loglevel","error",
        "-reconnect","1","-reconnect_streamed","1","-reconnect_on_network_error","1",
        "-headers", AUTH_HDR,
        "-i", STREAM_URL,
        "-vn","-sn","-dn",
        "-ac","1",
        "-ar","16000",
        "-c:a","pcm_s16le",
        "-f","segment",
        "-segment_format","wav",
        "-segment_time", str(SEG_SECONDS),
        "-reset_timestamps","1",
        "-strftime","1",
        out_pat,
    ]
    log(f"starting ffmpeg segmenter every {SEG_SECONDS}s")
    return subprocess.Popen(cmd)

def file_is_stable(p: Path, wait_s=6) -> bool:
    s1 = p.stat().st_size
    time.sleep(wait_s)
    s2 = p.stat().st_size
    return s1 == s2 and s2 > 0

def process_snippet(snippet: str, source_tag: str):
    if not passes_prefilter(snippet):
        log(f"{source_tag} prefilter false → skip")
        return
    log(f"{source_tag} llm classify start")
    data = classify(snippet)  # {"lead":..,"crime_type":..,"confidence":..,"address_hint":..}
    log(f"{source_tag} llm classify done lead={data.get('lead')} type={data.get('crime_type')} conf={data.get('confidence')}")
    if not data.get("lead"):
        return
    addr = extract_address(snippet) or extract_address(data.get("address_hint","") or "") or data.get("address_hint","").strip()
    if not addr:
        log(f"{source_tag} lead but no address → skip")
        return
    append_lead(
        crime_type=data.get("crime_type","other_biohazard"),
        address=addr,
        city_state=CITY_STATE,
        confidence=data.get("confidence", 0),
        snippet=snippet[:900]
    )
    subject = f"crime scene lead: {data.get('crime_type','biohazard')}"
    body = (
        f"address: {addr}, {CITY_STATE}\n"
        f"type: {data.get('crime_type')}\n"
        f"confidence: {data.get('confidence')}\n\n"
        f"snippet:\n{snippet}\n"
    )
    send_email(subject, body, os.getenv("DISPATCH_EMAIL"))
    log(f"{source_tag} sheet+email sent")

def read_offset() -> int:
    try:
        return int(OFFSET_FILE.read_text().strip())
    except Exception:
        return 0

def write_offset(n: int):
    try:
        OFFSET_FILE.write_text(str(n))
    except Exception:
        pass

def chunks(s: str, n: int):
    for i in range(0, len(s), n):
        yield s[i:i+n]

def scan_rolling_loop():
    log(f"rolling sweep every {ROLLING_SCAN}s")
    while True:
        try:
            if not ROLLING_FILE.exists():
                time.sleep(ROLLING_SCAN); continue
            off = read_offset()
            data = ROLLING_FILE.read_text(encoding="utf-8")
            if len(data) <= off:
                write_offset(len(data))
                time.sleep(ROLLING_SCAN); continue
            new = data[off:]
            write_offset(len(data))
            # process in chunks to keep prompts bounded
            for i, part in enumerate(chunks(new.strip(), ROLLING_MAX), 1):
                process_snippet(part, source_tag=f"rolling[{i}]")
        except Exception as e:
            log(f"rolling sweep error: {e}")
        time.sleep(ROLLING_SCAN)

def purge_old():
    cutoff = time.time() - (RET_HRS*3600)
    removed = 0
    for d in (SEG_DIR, TXT_DIR):
        for p in d.glob("*"):
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
                    removed += 1
            except FileNotFoundError:
                pass
    if removed:
        log(f"purged {removed} old files (> {RET_HRS}h)")

def purge_loop():
    while True:
        try:
            purge_old()
        except Exception as e:
            log(f"purge_loop error: {e}")
        time.sleep(1800)

def main():
    threading.Thread(target=purge_loop, daemon=True).start()
    threading.Thread(target=scan_rolling_loop, daemon=True).start()

    # --- dummy trigger for test only ---
    if os.getenv("DUMMY_TRIGGER","0") == "1":
        snippet = "units on scene report homicide, victim deceased, large blood pool, address 123 Main St."
        log("DUMMY_TRIGGER → forcing llm classification + email")
        process_snippet(snippet, source_tag="dummy")
        sys.exit("dummy trigger complete, exiting.")
    # --- end dummy trigger ---

    proc = start_ffmpeg()
    log(f"ffmpeg pid={proc.pid}")
    processed = set()

    try:
        while True:
            if proc.poll() is not None:
                sys.exit("audio stream ended. exiting as requested.")
            for wav in sorted(SEG_DIR.glob("*.wav")):
                if wav.name in processed:
                    continue
                if not file_is_stable(wav):
                    continue
                size_kb = wav.stat().st_size // 1024
                log(f"segment ready {wav.name} size={size_kb}KB → transcribe")
                try:
                    segs, _ = whisper_model.transcribe(
                        str(wav), vad_filter=True, vad_parameters={"min_silence_duration_ms":500}
                    )
                    text = " ".join(s.text.strip() for s in segs).strip()
                except Exception as e:
                    (LOGS / "bad_segments.log").open("a").write(f"{wav.name}\t{e}\n")
                    log(f"skip bad segment {wav.name}: {e}")
                    text = ""
                if not text:
                    log(f"empty transcript {wav.name} → skip")
                    processed.add(wav.name)
                    continue
                log(f"segment {wav.stem} chars={len(text)} → classify")
                process_snippet(text[:ROLLING_MAX], source_tag=f"segment:{wav.stem}")
                processed.add(wav.name)

                # Quick purge of old wav
                try:
                    if time.time() - wav.stat().st_mtime > PURGE_WAV_AFTER:
                        wav.unlink(missing_ok=True)
                except Exception:
                    pass

            time.sleep(2)
    finally:
        try:
            proc.terminate()
        except Exception:
            pass
        log("ffmpeg terminated")

if __name__ == "__main__":
    main()
