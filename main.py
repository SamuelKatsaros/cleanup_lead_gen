import os, sys, time, subprocess, threading, json, re
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

STREAM_URL   = os.getenv("BROADCASTIFY_STREAM_URL")
SEG_SECONDS  = int(os.getenv("SEGMENT_SECONDS", "90"))
CITY_STATE   = os.getenv("CITY_STATE","Dallas, TX")
# optional granular single-stream metadata
CITY         = os.getenv("CITY", "")
COUNTY       = os.getenv("COUNTY", "")
STATE        = os.getenv("STATE", "")
STREAM_NAME  = os.getenv("STREAM_NAME", "")
STREAM_TYPE  = os.getenv("STREAM_TYPE", "")
STREAM_ID    = os.getenv("STREAM_ID", "")
RET_HRS      = int(os.getenv("RETENTION_HOURS","48"))
AUTH_HDR     = os.getenv("BROADCASTIFY_AUTH_HEADER","")
ROLLING_FILE = Path(os.getenv("ROLLING_TRANSCRIPT","rolling/rolling.txt"))
ROLLING_FILE.parent.mkdir(parents=True, exist_ok=True)
ROLLING_SCAN  = int(os.getenv("ROLLING_SCAN_SECONDS","3600"))
ROLLING_MAX   = int(os.getenv("ROLLING_MAX_CHARS","1200"))
OFFSET_FILE   = LOGS / "rolling.offset"
PURGE_WAV_AFTER = int(os.getenv("PURGE_WAV_AFTER_SECONDS","60"))
DEDUP_TAIL_CHARS = int(os.getenv("DEDUP_TAIL_CHARS","5000"))
DUP_WINDOW_SEC   = int(os.getenv("DUPLICATE_WINDOW_SECONDS","900"))
LEAD_INDEX_FILE  = LOGS / "lead_index.json"
HEARTBEAT_MIN    = int(os.getenv("HEARTBEAT_MINUTES","60"))
HEARTBEAT_ENABLED = os.getenv("HEARTBEAT_ENABLED","1") == "1"

if not STREAM_URL and not os.getenv("BROADCASTIFY_STREAM_URLS") and not os.getenv("BROADCASTIFY_STREAM_IDS"):
    sys.exit("missing BROADCASTIFY_STREAM_URL or BROADCASTIFY_STREAM_URLS or BROADCASTIFY_STREAM_IDS")

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
WHISPER_LOCK = threading.Lock()
STOP_EVENT = threading.Event()

# --- lightweight dedup helpers (single-stream path) ---
RECENT_TAIL = ""
_LEAD_INDEX_LOCK = threading.Lock()
_STATS_LOCK = threading.Lock()
STATS = {"start_ts": time.time(), "streams": {}}

def _normalize_addr(addr: str) -> str:
    return " ".join("".join(ch.lower() if ch.isalnum() or ch.isspace() else " " for ch in (addr or "")).split())

def _token_set(s: str) -> set:
    return {t for t in " ".join(ch.lower() if ch.isalnum() else " " for ch in (s or "")).split() if t}

def _jaccard(a: set, b: set) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    if inter == 0:
        return 0.0
    return inter / float(len(a | b))

def _load_lead_index(path: Path) -> dict:
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"incidents": [], "fingerprints": []}

def _save_lead_index(path: Path, data: dict):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f)
    except Exception:
        pass

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _parse_city_state(cs: str) -> tuple[str, str]:
    try:
        parts = [p.strip() for p in (cs or "").split(",")]
        if len(parts) >= 2:
            return ",".join(parts[:-1]).strip(), parts[-1]
        return cs.strip(), ""
    except Exception:
        return cs or "", ""

def _nz(x: str) -> str:
    return x or ""

def _sanitize_label(name: str) -> str:
    try:
        if not name:
            return ""
        s = re.sub(r"[\\/:*?\"<>|]+", "-", name)
        s = s.strip()
        return s or "unnamed"
    except Exception:
        return "unnamed"

def write_transcripts(text: str, dir_tag: str, display_label: str, wav_path: Path):
    try:
        TXT_DIR.mkdir(parents=True, exist_ok=True)
        per_stream_dir = TXT_DIR / dir_tag
        per_stream_dir.mkdir(parents=True, exist_ok=True)
        seg_txt = per_stream_dir / f"{wav_path.stem}.txt"
        seg_txt.write_text(text, encoding="utf-8")
        header = f"[{_ts()}] {display_label} {wav_path.name} chars={len(text)}\n"
        with (TXT_DIR / "ALL_STREAMS.log").open("a", encoding="utf-8") as f:
            f.write(header)
            f.write(text)
            f.write("\n\n")
        with (TXT_DIR / f"{dir_tag}.log").open("a", encoding="utf-8") as f:
            f.write(header)
            f.write(text)
            f.write("\n\n")
    except Exception as e:
        try:
            log(f"write_transcripts error: {e}")
        except Exception:
            pass

def append_to_tail(text: str):
    global RECENT_TAIL
    if not text:
        return
    RECENT_TAIL = (RECENT_TAIL + " " + text).strip()
    if len(RECENT_TAIL) > DEDUP_TAIL_CHARS:
        RECENT_TAIL = RECENT_TAIL[-DEDUP_TAIL_CHARS:]

def is_duplicate_lead(addr: str, crime_type: str, snippet: str, index_file: Path = LEAD_INDEX_FILE, tail_text: str = None) -> bool:
    now = time.time()
    key = f"{_normalize_addr(addr)}|{(crime_type or '').strip().lower()}"
    tokens = _token_set(snippet)
    with _LEAD_INDEX_LOCK:
        idx = _load_lead_index(index_file)
        # purge old
        cutoff = now - DUP_WINDOW_SEC
        idx["incidents"] = [i for i in idx.get("incidents", []) if i.get("ts", 0) >= cutoff]
        idx["fingerprints"] = [fp for fp in idx.get("fingerprints", []) if fp.get("ts", 0) >= cutoff]
        # direct incident key match
        for i in idx["incidents"]:
            if i.get("key") == key:
                return True
        # similarity against recent fingerprints (when address is missing/varies)
        for fp in idx["fingerprints"]:
            sim = _jaccard(tokens, set(fp.get("tokens", [])))
            if sim >= 0.85:
                return True
    # last-5k tail heuristic: if many words overlap, consider duplicate
    try:
        tail_tokens = _token_set(RECENT_TAIL if tail_text is None else tail_text)
        if _jaccard(tokens, tail_tokens) >= 0.85:
            return True
    except Exception:
        pass
    return False

def record_lead(addr: str, crime_type: str, snippet: str, index_file: Path = LEAD_INDEX_FILE):
    now = time.time()
    key = f"{_normalize_addr(addr)}|{(crime_type or '').strip().lower()}"
    tokens = list(_token_set(snippet))
    with _LEAD_INDEX_LOCK:
        idx = _load_lead_index(index_file)
        idx.setdefault("incidents", []).append({"key": key, "ts": now})
        idx.setdefault("fingerprints", []).append({"tokens": tokens, "ts": now})
        # trim to reasonable size
        idx["incidents"] = idx["incidents"][-500:]
        idx["fingerprints"] = idx["fingerprints"][-500:]
        _save_lead_index(index_file, idx)

def start_ffmpeg():
    out_pat = str(SEG_DIR / "%Y%m%d-%H%M%S.wav")
    cmd = [
        "ffmpeg",
        "-hide_banner","-loglevel","error",
        "-reconnect","1","-reconnect_streamed","1","-reconnect_on_network_error","1",
    ]
    if AUTH_HDR:
        cmd += ["-headers", AUTH_HDR]
    cmd += [
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

def _update_stream_stat(key: str, **kwargs):
    try:
        with _STATS_LOCK:
            s = STATS["streams"].setdefault(key, {})
            for k, v in kwargs.items():
                if isinstance(v, int):
                    s[k] = int(v)
                else:
                    s[k] = v
    except Exception:
        pass

def _inc_stream_counter(key: str, field: str, delta: int = 1):
    try:
        with _STATS_LOCK:
            s = STATS["streams"].setdefault(key, {})
            s[field] = int(s.get(field, 0)) + delta
    except Exception:
        pass

def _fmt_ts(ts: float) -> str:
    try:
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ""

def _uptime() -> str:
    secs = int(time.time() - STATS.get("start_ts", time.time()))
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h}h {m}m {s}s"

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
    if is_duplicate_lead(addr, data.get("crime_type",""), snippet):
        log(f"{source_tag} duplicate lead → skip")
        return
    # compose single-stream metadata
    city_val = CITY
    state_val = STATE
    if not city_val or not state_val:
        pc, ps = _parse_city_state(CITY_STATE)
        city_val = city_val or pc
        state_val = state_val or ps
    append_lead(
        stream_name=_nz(STREAM_NAME),
        stream_type=_nz(STREAM_TYPE),
        stream_id=_nz(STREAM_ID),
        city=_nz(city_val),
        county=_nz(COUNTY),
        state=_nz(state_val),
        crime_type=data.get("crime_type","other_biohazard"),
        address=addr,
        confidence=data.get("confidence", 0),
        snippet=snippet[:900]
    )
    subject = f"Lead: {data.get('crime_type','biohazard')} - {_nz(city_val)}, {_nz(state_val)}"
    body = (
        f"stream: {_nz(STREAM_NAME)} ({_nz(STREAM_TYPE)}) id={_nz(STREAM_ID)}\n"
        f"location: {_nz(city_val)}, {_nz(COUNTY)} County, {_nz(state_val)}\n"
        f"address: {addr}\n"
        f"type: {data.get('crime_type')}\n"
        f"confidence: {data.get('confidence')}\n\n"
        f"snippet:\n{snippet}\n"
    )
    send_email(subject, body, os.getenv("DISPATCH_EMAIL"))
    try:
        _inc_stream_counter("single", "emails_sent", 1)
    except Exception:
        pass
    log(f"{source_tag} sheet+email sent")
    record_lead(addr, data.get("crime_type",""), snippet)

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
            append_to_tail(new)
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

    # startup email
    try:
        to = os.getenv("DISPATCH_EMAIL")
        if to:
            subject = "Service started"
            body_lines = [
                f"uptime: just started",
                f"heartbeat: {'on' if HEARTBEAT_ENABLED else 'off'} every {HEARTBEAT_MIN}m",
            ]
            body = "\n".join(body_lines)
            send_email(subject, body, to)
    except Exception as e:
        log(f"startup email error: {e}")

    # Multi-stream support via semicolon-separated envs
    multi_urls = os.getenv("BROADCASTIFY_STREAM_URLS", "").strip()
    ids_env = os.getenv("BROADCASTIFY_STREAM_IDS", "").strip()
    if multi_urls or ids_env:
        urls = [u.strip() for u in multi_urls.split(";") if u.strip()]
        ids  = [i.strip() for i in ids_env.split(";") if i.strip()]
        if not urls and ids:
            urls = [f"https://audio.broadcastify.com/{i}.mp3" for i in ids]

        auths_env   = os.getenv("BROADCASTIFY_AUTH_HEADERS", "")
        names_env   = os.getenv("STREAM_NAMES", "")
        types_env   = os.getenv("STREAM_TYPES", "")
        cities_env  = os.getenv("STREAM_CITIES", "") or os.getenv("CITY_STATES", "")
        counties_env= os.getenv("STREAM_COUNTIES", "")
        states_env  = os.getenv("STREAM_STATES", "")

        names    = [x.strip() for x in names_env.split(";")]
        types    = [x.strip() for x in types_env.split(";")]
        cities   = [x.strip() for x in cities_env.split(";")]
        counties = [x.strip() for x in counties_env.split(";")]
        states   = [x.strip() for x in states_env.split(";")]
        auths    = [a.strip() for a in auths_env.split(";")]
        if not cities:
            cities = [CITY_STATE]
        if not auths and AUTH_HDR:
            auths = [AUTH_HDR]
        # If a single value is supplied, reuse for all streams
        if len(cities) == 1 and len(urls) > 1:
            cities = cities * len(urls)
        if len(auths) == 1 and len(urls) > 1:
            auths = auths * len(urls)
        # normalize list lengths
        def _expand(lst, n, default=""):
            lst = list(lst)
            if not lst:
                lst = [default]
            if len(lst) == 1 and n > 1:
                lst = lst * n
            while len(lst) < n:
                lst.append(lst[-1] if lst else default)
            return lst[:n]
        n = len(urls)
        ids = _expand(ids, n, "")
        names = _expand(names, n, "")
        types = _expand(types, n, "")
        cities = _expand(cities, n, CITY_STATE)
        counties = _expand(counties, n, "")
        states = _expand(states, n, "")
        auths = _expand(auths, n, AUTH_HDR)

        def worker(idx: int, url: str, auth: str, stream_id: str, name: str, s_type: str, city_label: str, county_label: str, state_label: str):
            tag = f"stream{idx+1}"
            display_label = (name.strip() or tag)
            dir_tag = (_sanitize_label(name) or tag)
            seg_dir = ROOT / "audio_segments" / dir_tag
            seg_dir.mkdir(parents=True, exist_ok=True)
            logs_dir = LOGS
            offset_file = logs_dir / f"rolling_{dir_tag}.offset"
            lead_index_file = logs_dir / f"lead_index_{dir_tag}.json"
            tail_buffer = ""

            def log2(msg: str):
                loc = city_label if city_label else CITY_STATE
                log(f"[{display_label} {loc}] {msg}")
                try:
                    _update_stream_stat(dir_tag, name=display_label, city=loc)
                except Exception:
                    pass

            def start_ffmpeg2():
                out_pat = str(seg_dir / "%Y%m%d-%H%M%S.wav")
                cmd = [
                    "ffmpeg",
                    "-hide_banner","-loglevel","error",
                    "-reconnect","1","-reconnect_streamed","1","-reconnect_on_network_error","1",
                ]
                if auth:
                    cmd += ["-headers", auth]
                cmd += [
                    "-i", url,
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
                log2(f"starting ffmpeg segmenter every {SEG_SECONDS}s")
                return subprocess.Popen(cmd)

            def file_is_stable2(p: Path, wait_s=6) -> bool:
                s1 = p.stat().st_size
                time.sleep(wait_s)
                s2 = p.stat().st_size
                return s1 == s2 and s2 > 0

            def process_snippet2(snippet: str, source_tag: str):
                if not passes_prefilter(snippet):
                    log2(f"{source_tag} prefilter false → skip")
                    return
                log2(f"{source_tag} llm classify start")
                data = classify(snippet)
                log2(f"{source_tag} llm classify done lead={data.get('lead')} type={data.get('crime_type')} conf={data.get('confidence')}")
                if not data.get("lead"):
                    return
                addr = extract_address(snippet) or extract_address(data.get("address_hint","") or "") or data.get("address_hint","").strip()
                if not addr:
                    log2(f"{source_tag} lead but no address → skip")
                    return
                if is_duplicate_lead(addr, data.get("crime_type",""), snippet, index_file=lead_index_file, tail_text=tail_buffer):
                    log2(f"{source_tag} duplicate lead → skip")
                    return
                city_only, state_only = _parse_city_state(city_label)
                append_lead(
                    stream_name=_nz(name),
                    stream_type=_nz(s_type),
                    stream_id=_nz(stream_id),
                    city=_nz(city_only or city_label),
                    county=_nz(county_label),
                    state=_nz(state_label or state_only),
                    crime_type=data.get("crime_type","other_biohazard"),
                    address=addr,
                    confidence=data.get("confidence", 0),
                    snippet=snippet[:900]
                )
                subject = f"Lead: {data.get('crime_type','biohazard')} - {_nz(city_only or city_label)}, {_nz(state_label or state_only)} ({_nz(name)})"
                body = (
                    f"stream: {_nz(name)} ({_nz(s_type)}) id={_nz(stream_id)}\n"
                    f"location: {_nz(city_only or city_label)}, {_nz(county_label)} County, {_nz(state_label or state_only)}\n"
                    f"address: {addr}\n"
                    f"type: {data.get('crime_type')}\n"
                    f"confidence: {data.get('confidence')}\n\n"
                    f"snippet:\n{snippet}\n"
                )
                send_email(subject, body, os.getenv("DISPATCH_EMAIL"))
                log2(f"{source_tag} sheet+email sent")
                record_lead(addr, data.get("crime_type",""), snippet, index_file=lead_index_file)

            backoff = 5
            while not STOP_EVENT.is_set():
                proc = start_ffmpeg2()
                log2(f"ffmpeg pid={proc.pid}")
                processed = set()
                try:
                    while not STOP_EVENT.is_set():
                        if proc.poll() is not None:
                            rc = proc.returncode
                            log2(f"ffmpeg exited rc={rc}. restart in {backoff}s")
                            time.sleep(backoff)
                            backoff = min(backoff * 2, 60)
                            break
                        for wav in sorted(seg_dir.glob("*.wav")):
                            if wav.name in processed:
                                continue
                            if not file_is_stable2(wav):
                                continue
                            size_kb = wav.stat().st_size // 1024
                            log2(f"segment ready {wav.name} size={size_kb}KB → transcribe")
                            try:
                                _inc_stream_counter(dir_tag, "segments", 1)
                                _update_stream_stat(dir_tag, last_activity_ts=time.time())
                            except Exception:
                                pass
                            try:
                                with WHISPER_LOCK:
                                    segs, _ = whisper_model.transcribe(
                                        str(wav), vad_filter=True, vad_parameters={"min_silence_duration_ms":500}
                                    )
                                text = " ".join(s.text.strip() for s in segs).strip()
                            except Exception as e:
                                (LOGS / "bad_segments.log").open("a").write(f"{dir_tag}/{wav.name}\t{e}\n")
                                log2(f"skip bad segment {wav.name}: {e}")
                                text = ""
                            if not text:
                                log2(f"empty transcript {wav.name} → skip")
                                processed.add(wav.name)
                                continue
                            log2(f"segment {wav.stem} chars={len(text)} → classify")
                            write_transcripts(text, dir_tag=dir_tag, display_label=display_label, wav_path=wav)
                            # update per-stream tail
                            tail_buffer = (tail_buffer + " " + text).strip()
                            if len(tail_buffer) > DEDUP_TAIL_CHARS:
                                tail_buffer = tail_buffer[-DEDUP_TAIL_CHARS:]
                            process_snippet2(text[:ROLLING_MAX], source_tag=f"segment:{wav.stem}")
                            processed.add(wav.name)
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
                    log2("ffmpeg terminated")
                    try:
                        _inc_stream_counter(dir_tag, "restarts", 1)
                    except Exception:
                        pass

        threads = []
        for i, u in enumerate(urls):
            t = threading.Thread(
                target=worker,
                args=(
                    i,
                    u,
                    auths[i] if i < len(auths) else AUTH_HDR,
                    ids[i] if i < len(ids) else "",
                    names[i] if i < len(names) else "",
                    types[i] if i < len(types) else "",
                    cities[i] if i < len(cities) else CITY_STATE,
                    counties[i] if i < len(counties) else "",
                    states[i] if i < len(states) else "",
                ),
                daemon=True,
            )
            t.start()
            threads.append(t)
        # Heartbeat thread (multi-stream)
        def heartbeat_worker():
            if not HEARTBEAT_ENABLED:
                return
            to = os.getenv("DISPATCH_EMAIL")
            if not to:
                return
            while not STOP_EVENT.is_set():
                try:
                    with _STATS_LOCK:
                        snapshot = json.dumps(STATS, default=str)
                        lines = [
                            f"uptime: {_uptime()}",
                            f"streams: {', '.join(sorted(STATS.get('streams',{}).keys()))}",
                            f"stats: {snapshot}",
                        ]
                    send_email("Service heartbeat", "\n".join(lines), to)
                except Exception as e:
                    log(f"heartbeat email error: {e}")
                for _ in range(HEARTBEAT_MIN*60):
                    if STOP_EVENT.is_set():
                        break
                    time.sleep(1)

        threading.Thread(target=heartbeat_worker, daemon=True).start()

        # Block main thread
        try:
            for t in threads:
                t.join()
        except KeyboardInterrupt:
            STOP_EVENT.set()
            log("shutdown requested, stopping streams…")
            for t in threads:
                t.join(timeout=5)
        return

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
                    with WHISPER_LOCK:
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
                # single-stream path uses STREAM_NAME as label and dir if provided
                display_label = (STREAM_NAME.strip() or "stream1")
                dir_tag = (_sanitize_label(STREAM_NAME) or "stream1")
                write_transcripts(text, dir_tag=dir_tag, display_label=display_label, wav_path=wav)
                append_to_tail(text)
                process_snippet(text[:ROLLING_MAX], source_tag=f"segment:{wav.stem}")
                processed.add(wav.name)

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
