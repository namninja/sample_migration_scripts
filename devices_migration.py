#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable Device Registration Migration
- Export users (paged), read each file, and for every device in each user:
  POST /api/users/registerDeviceToken
- Page flow: list ≤10 files → download → process → delete → advance cursor
- Resumable via export_state.json (per job_id)
"""

import os, sys, json, time, threading, random, functools
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Tuple
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, ReadTimeout
from urllib3.exceptions import ReadTimeoutError, ProtocolError

#################
# Configuration #
#################

iterable_base_url = "https://api.iterable.com"
export_start_endpoint = "/api/export/start"
export_files_endpoint_tmpl = "/api/export/{job_id}/files"
register_device_endpoint = "/api/users/registerDeviceToken"

# Working dir (downloads + logs + state)
dirname = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migrate_devices")

# Process at most this many export files TOTAL across all pages (None disables limit)
testSample = None  # e.g., 20

# Pagination – Iterable returns up to 10 files per page; we’ll consume what we get
MAX_FILES_PER_PAGE = 10

# Concurrency & rate limits - REDUCED to be gentler on Iterable's servers
MAX_WORKERS = 10                 # threads used to submit POSTs (reduced from 64)
MAX_INFLIGHT_POSTS = 25          # cap concurrent in-flight POSTs (reduced from 200)  
RATE_LIMIT_RPS = 100             # Iterable project limit for this endpoint (reduced from 500)

# Export job: set to an existing job_id to resume, or None to start a new export
job_id = None  # e.g., 12345678

# API keys
origin_api_key = ""
destination_api_key = ""


# Export omit fields – keep this empty to ensure we get the full `devices` objects
# (You can trim unrelated fields if you like, but do NOT omit 'devices')
omit_fields = ""  # e.g., "emailListIds,receivedSMSDisclaimer,..."

# Restrict export to a time window. Format: "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd",
# or ISO-8601 with offset e.g. "2024-06-01T00:00:00-07:00". None = no filter.
start_datetime: Optional[str] = None
end_datetime:   Optional[str] = None

# Restrict export to specific fields (comma-separated). None = all fields.
# Must include 'userId' and 'devices' so device registration works correctly.
only_fields: Optional[str] = "userId,email,devices"  # e.g., "userId,devices"

# ── IDENTITY ──────────────────────────────────────────────────────────────────

# Project kind for source and destination: "email" | "uuid" | "hybrid"
SOURCE_PROJECT_TYPE      = "email"
DESTINATION_PROJECT_TYPE = "uuid"

# Primary identifier sent at the API root for registerDeviceToken calls.
# For hybrid projects this controls which key goes at the root vs. fallback.
SOURCE_PREFERRED_IDENTIFIER      = "email"
DESTINATION_PREFERRED_IDENTIFIER = "userId"

# Hybrid Projects-only: fall back to this identifier when the preferred one is missing.
# "email" | "userId" | None (None = skip the row, same strict behaviour as uuid).
HYBRID_IDENTIFIER_FALLBACK: Optional[str] = None

# ── AUDIT MODE ────────────────────────────────────────────────────────────────
# When enabled, writes two files to the output directory:
#   audit_log.jsonl    — one row per device attempt (registered, skipped, failed, quarantined)
#   audit_manifest.json — snapshot of identity settings at run start
# These survive CLEANUP_AFTER and allow post-run verification.
AUDIT_MODE = False

# ── RUN MODE ──────────────────────────────────────────────────────────────────

# JOB_ONLY: start (or log) the export job, print the jobId, then exit immediately.
# Useful to kick off an export without waiting for it to finish.
JOB_ONLY = False

# SAMPLE_FILE: start the job (or use an existing job_id), poll until
# jobState == "completed", download a single export file to disk, print its
# path, then exit. No device registration is performed.
# JOB_ONLY takes precedence if both are True.
SAMPLE_FILE = True

# Delete only export files (logs preserved) after the WHOLE run
CLEANUP_AFTER = False

# Persist the last page cursor (last filename) here
STATE_PATH = os.path.join(dirname, "export_state.json")

# Timeouts (list, download, post). POST uses (connect, read).
LIST_TIMEOUT = 30
DOWNLOAD_TIMEOUT = 120
POST_CONNECT_TIMEOUT = 10
POST_READ_TIMEOUT = 120

# Controlled retry/backoff for our POSTs
retry_config = {
    'stop_max_attempt_number': 3,            # 3 tries total
    'wait_exponential_multiplier': 1500,     # 1.5s, 3s, 4.5s
    'wait_exponential_max': 8000             # cap ~8s
}


def retry_device_post(func):
    """
    Stdlib-only exponential-backoff retry decorator (no third-party packages).
    Reads stop_max_attempt_number, wait_exponential_multiplier (ms), and
    wait_exponential_max (ms) from retry_config.
    """
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        max_attempts = retry_config['stop_max_attempt_number']
        wm  = retry_config['wait_exponential_multiplier']
        wmax = retry_config['wait_exponential_max']
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if attempt >= max_attempts:
                    raise
                cap_ms = min(wmax, wm * (2 ** (attempt - 1)))
                wait_s = random.uniform(0, cap_ms) / 1000.0
                print(
                    f"[retry] {func.__name__} attempt {attempt}/{max_attempts} failed "
                    f"({exc}); retrying in {wait_s:.1f}s…"
                )
                time.sleep(wait_s)
    return wrapped


# Ensure working directory exists
os.makedirs(dirname, exist_ok=True)

# Diagnostics / logs
INVALID_RECORDS_PATH  = os.path.join(dirname, "invalid_records.jsonl")
ERROR_LOG_PATH        = os.path.join(dirname, "device_errors.log")
SUMMARY_PATH          = os.path.join(dirname, "run_summary.json")
QUARANTINE_PATH       = os.path.join(dirname, "timed_out_devices.jsonl")
AUDIT_LOG_PATH        = os.path.join(dirname, "audit_log.jsonl")
AUDIT_MANIFEST_PATH   = os.path.join(dirname, "audit_manifest.json")

# Counters
downloaded_files_total = 0
processed_files_total = 0
start_time = 0
end_time = 0
total_lines_parsed = 0
devices_registered_counter = 0
users_skipped_counter = 0
devices_skipped_counter = 0
unique_users_with_devices = set()  # Track unique user IDs that have had devices registered

#########################
# HTTP session (robust) #
#########################

session = requests.Session()

# Auto-retry only GETs (not POSTs)
retry_strategy = Retry(
    total=3, connect=3, read=3, status=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET"},
    backoff_factor=1.5,
    raise_on_status=False,
)
adapter = HTTPAdapter(pool_connections=256, pool_maxsize=256, max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

DEFAULT_HEADERS = {
    "Connection": "keep-alive",
    "Accept-Encoding": "gzip, deflate",
}

################################
# Rate limiter (500 requests/s) #
################################

_rl_lock = threading.Lock()
_rl_tokens = RATE_LIMIT_RPS
_rl_last_refill = time.time()

# Protects all counter increments mutated from worker threads
_counter_lock = threading.Lock()

def acquire_token():
    """Token-bucket limiter shared across threads: max RATE_LIMIT_RPS POSTs/sec."""
    global _rl_tokens, _rl_last_refill
    while True:
        with _rl_lock:
            now = time.time()
            elapsed = now - _rl_last_refill
            if elapsed >= 1.0:
                _rl_tokens = RATE_LIMIT_RPS
                _rl_last_refill = now
            if _rl_tokens > 0:
                _rl_tokens -= 1
                return
            sleep_for = max(0.0, 1.0 - elapsed)
        time.sleep(sleep_for if sleep_for > 0 else 0.005)

_inflight_sem = threading.Semaphore(MAX_INFLIGHT_POSTS)

#################
# Utilities     #
#################

def calculate_run_time():
    global end_time
    end_time = time.time()
    print(f"Execution time: {(end_time - start_time):.2f} seconds")

def load_state() -> dict:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state: dict):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_PATH)

def log_error(msg: str):
    try:
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        pass

def log_invalid_record(reason, source_user, built_payload=None, status_code=None, response_text=None):
    rec = {
        "reason": reason,
        "status": status_code,
        "response": (response_text or "")[:4000] if response_text else "",
        "source_user": source_user,
        "built_payload": built_payload
    }
    with open(INVALID_RECORDS_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

def quarantine_device(reason, source_user, device_obj, status_code=None, response_text=None):
    rec = {
        "reason": reason,
        "status": status_code,
        "response": (response_text or "")[:4000] if response_text else "",
        "userId": source_user.get("userId") or source_user.get("email"),
        "device": device_obj
    }
    with open(QUARANTINE_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")

############################
# Identity helpers         #
############################

def _normalize_preferred_identifier(value) -> str:
    raw = str(value).strip().lower()
    if raw == "email":
        return "email"
    if raw in ("userid", "user_id", "user id", "uuid"):
        return "userId"
    raise ValueError(f"preferred identifier must be 'email' or 'userId', got {value!r}")


def _destination_preferred_key() -> str:
    return _normalize_preferred_identifier(DESTINATION_PREFERRED_IDENTIFIER)


def _destination_project_type_normalized() -> str:
    return str(DESTINATION_PROJECT_TYPE).strip().lower()


def _hybrid_fallback_normalized() -> Optional[str]:
    if HYBRID_IDENTIFIER_FALLBACK is None:
        return None
    s = str(HYBRID_IDENTIFIER_FALLBACK).strip()
    if not s:
        return None
    try:
        return _normalize_preferred_identifier(s)
    except ValueError:
        return None


def _resolve_register_device_root(
    email: Optional[str], user_id: Optional[str]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Decide the root identifier key/value for registerDeviceToken.
    Returns (root_field, root_value, error_reason).
    error_reason being non-None means the row should be skipped.
    uuid:   preferred identifier required — no fallback.
    hybrid: HYBRID_IDENTIFIER_FALLBACK used when preferred is missing.
    """
    pk = _destination_preferred_key()
    dt = _destination_project_type_normalized()
    fb = _hybrid_fallback_normalized()

    if pk == "userId":
        if user_id:
            return "userId", user_id, None
        if dt == "hybrid" and fb == "email" and email:
            return "email", email, None
        return None, None, "missing_userId_for_destination"

    if email:
        return "email", email, None
    if dt == "hybrid" and fb == "userId" and user_id:
        return "userId", user_id, None
    return None, None, "missing_email_for_destination"


def _validate_identity_settings() -> None:
    try:
        _normalize_preferred_identifier(SOURCE_PREFERRED_IDENTIFIER)
        _normalize_preferred_identifier(DESTINATION_PREFERRED_IDENTIFIER)
    except ValueError as e:
        print(f"Invalid identity settings in configuration: {e}", file=sys.stderr)
        sys.exit(1)
    dt = _destination_project_type_normalized()
    if dt not in ("email", "uuid", "hybrid"):
        print(
            f"Invalid DESTINATION_PROJECT_TYPE {DESTINATION_PROJECT_TYPE!r}; use 'email', 'uuid', or 'hybrid'.",
            file=sys.stderr,
        )
        sys.exit(1)
    if dt != "hybrid" and HYBRID_IDENTIFIER_FALLBACK not in (None, ""):
        print(
            "[config] HYBRID_IDENTIFIER_FALLBACK is set but DESTINATION_PROJECT_TYPE is not 'hybrid'; fallback is ignored.",
            flush=True,
        )
    fb = _hybrid_fallback_normalized()
    pk = _destination_preferred_key()
    if fb is not None and fb == pk:
        print(
            "[config] HYBRID_IDENTIFIER_FALLBACK matches DESTINATION_PREFERRED_IDENTIFIER; fallback has no effect.",
            flush=True,
        )
    if (
        dt == "hybrid"
        and HYBRID_IDENTIFIER_FALLBACK is not None
        and str(HYBRID_IDENTIFIER_FALLBACK).strip()
        and _hybrid_fallback_normalized() is None
    ):
        print(
            "[config] HYBRID_IDENTIFIER_FALLBACK is invalid (use 'email' or 'userId'); treating as None.",
            file=sys.stderr,
            flush=True,
        )

############################
# Audit log helpers        #
############################

def _append_audit_log(record: dict) -> None:
    """Append one row to audit_log.jsonl (no-op when AUDIT_MODE is False)."""
    if not AUDIT_MODE:
        return
    try:
        r = dict(record)
        r["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except Exception as e:
        log_error(f"[audit_log] {e}")


def write_audit_manifest() -> None:
    """Write a one-time snapshot of identity settings to audit_manifest.json."""
    if not AUDIT_MODE:
        return
    manifest = {
        "written_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "job_id": job_id,
        "identity": {
            "source_project_type": SOURCE_PROJECT_TYPE,
            "source_preferred_identifier": SOURCE_PREFERRED_IDENTIFIER,
            "destination_project_type": DESTINATION_PROJECT_TYPE,
            "destination_preferred_identifier": DESTINATION_PREFERRED_IDENTIFIER,
            "hybrid_identifier_fallback": HYBRID_IDENTIFIER_FALLBACK,
        },
    }
    try:
        with open(AUDIT_MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"[audit] Manifest written to {AUDIT_MANIFEST_PATH}")
    except Exception as e:
        log_error(f"[audit_manifest] {e}")

##############################
# Export Start / List Pages  #
##############################

def _parse_export_datetime(raw: str) -> datetime:
    """Parse a config datetime string into an aware datetime (naive → UTC)."""
    s = raw.strip()
    if not s:
        raise ValueError("empty datetime string")
    if s[-1] in "Zz":
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        pass
    else:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Unparseable export datetime: {raw!r}")


def format_iterable_export_datetime(raw: Optional[str]) -> Optional[str]:
    """Convert a config value to Iterable export format: yyyy-MM-dd HH:mm:ss ±HH:MM."""
    if raw is None or not str(raw).strip():
        return None
    dt = _parse_export_datetime(str(raw))
    off = dt.utcoffset() or timedelta(0)
    secs = int(off.total_seconds())
    sign = "+" if secs >= 0 else "-"
    secs = abs(secs)
    hh, mm = secs // 3600, (secs % 3600) // 60
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f" {sign}{hh:02d}:{mm:02d}"


def start_export() -> int:
    body: Dict[str, object] = {
        "outputFormat": "application/x-json-stream",
        "dataTypeName": "user",
    }
    if omit_fields:
        body["omitFields"] = omit_fields
    # Never include startDateTime / endDateTime when null/empty.
    try:
        if start_datetime is not None:
            _sd = str(start_datetime).strip()
            if _sd:
                body["startDateTime"] = format_iterable_export_datetime(_sd)
        if end_datetime is not None:
            _ed = str(end_datetime).strip()
            if _ed:
                body["endDateTime"] = format_iterable_export_datetime(_ed)
    except ValueError as e:
        print(f"Invalid start_datetime or end_datetime in configuration: {e}", file=sys.stderr)
        raise SystemExit(1) from e
    # Never include onlyFields when null/empty.
    if only_fields is not None:
        _only = str(only_fields).strip()
        if _only:
            body["onlyFields"] = _only

    r = session.post(
        f"{iterable_base_url}{export_start_endpoint}",
        headers={"Content-Type": "application/json", "Api-Key": origin_api_key, **DEFAULT_HEADERS},
        json=body,
        timeout=LIST_TIMEOUT
    )
    if not r.ok:
        raise requests.HTTPError(f"HTTP error! Status: {r.status_code} - {r.text}")
    data = r.json()
    print("Export job started with jobId:", data["jobId"])
    return data["jobId"]

def list_files_page(job_id: int, start_after: Optional[str]):
    """
    Returns (job_state, files_list, next_cursor, export_truncated).
    files_list entries: { "file": "<name>", "url": "<presigned>" }
    next_cursor is the last filename on this page (or None).
    """
    url = f"{iterable_base_url}{export_files_endpoint_tmpl.format(job_id=job_id)}"
    params = {"startAfter": start_after} if start_after else {}
    r = session.get(url, headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS}, params=params, timeout=LIST_TIMEOUT)
    if not r.ok:
        raise requests.HTTPError(f"HTTP error! Status: {r.status_code} - {r.text}")
    data             = r.json()
    files            = data.get("files", [])[:MAX_FILES_PER_PAGE]
    job_state        = str(data.get("jobState", "")).lower()
    export_truncated = bool(data.get("exportTruncated", False))
    next_cursor      = files[-1]["file"] if files else None
    return job_state, files, next_cursor, export_truncated
    return job_state, files, next_cursor

#####################################
# Download → process → delete (page) #
#####################################

def download_page_files(files: List[Dict[str, str]]) -> List[str]:
    global downloaded_files_total
    local_paths = []
    for entry in files:
        name = entry.get("file")
        url = entry.get("url")
        if not name or not url:
            print(f"Skipping malformed file entry: {entry}")
            continue
        dest = os.path.join(dirname, name)

        for attempt in range(1, 4):
            try:
                with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT, headers=DEFAULT_HEADERS) as dl:
                    dl.raise_for_status()
                    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
                    with open(dest, "wb") as out:
                        for chunk in dl.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                out.write(chunk)
                local_paths.append(dest)
                downloaded_files_total += 1
                print(f"Downloaded: {name}")
                break
            except requests.RequestException as e:
                if attempt == 3:
                    print(f"Failed to download {name} after retries: {e}")
                time.sleep(attempt)  # 1s, 2s
    return local_paths

def delete_local_files(local_file_paths: List[str]):
    removed = 0
    for p in local_file_paths:
        try:
            os.remove(p)
            removed += 1
        except OSError:
            pass
    print(f"[page cleanup] Deleted {removed} file(s)")

##############################
# Payload builder + POST     #
##############################

REQUIRED_DEVICE_FIELDS = ("applicationName", "platform", "token")

def build_register_device_payload(u: dict, dev: dict):
    # Some export configs nest identifiers under dataFields; check both levels
    _df = u.get("dataFields") or {}
    email   = u.get("email") or _df.get("email") or None
    user_id = (
        u.get("userId") or u.get("itblUserId") or
        _df.get("userId") or _df.get("itblUserId") or None
    )
    root_field, root_value, error_reason = _resolve_register_device_root(email, user_id)
    if error_reason:
        return None, error_reason

    missing = [k for k in REQUIRED_DEVICE_FIELDS if not dev.get(k)]
    if missing:
        return None, f"device_missing_required_fields:{','.join(missing)}"

    device = {
        "applicationName": dev["applicationName"],
        "platform": dev["platform"],
        "token": dev["token"],
    }

    # Everything else goes into dataFields
    data_fields = {}
    for k, v in dev.items():
        if k not in REQUIRED_DEVICE_FIELDS:
            data_fields[k] = v
    if data_fields:
        device["dataFields"] = data_fields

    payload = {
        "device": device,
        root_field: root_value,
    }
    return payload, None

def is_timeout_exc(exc: Exception) -> bool:
    return isinstance(exc, (ReadTimeout, ReadTimeoutError, ProtocolError)) or (
        "timeout" in str(exc).lower() and "read" in str(exc).lower()
    )

def _post_register_device(payload: dict, api_key: str):
    acquire_token()
    _inflight_sem.acquire()
    try:
        return session.post(
            f"{iterable_base_url}{register_device_endpoint}",
            json=payload,
            headers={"Content-Type": "application/json", "Api-Key": api_key, **DEFAULT_HEADERS},
            timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
        )
    finally:
        _inflight_sem.release()

@retry_device_post
def send_register_device(payload: dict, src_user: dict, dev: dict):
    global devices_registered_counter, devices_skipped_counter, unique_users_with_devices
    try:
        resp = _post_register_device(payload, destination_api_key)
        status = resp.status_code

        if status == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after and retry_after.isdigit() else random.uniform(0.5, 2.0)
            print(f"[429] Sleeping {wait_s:.2f}s …")
            time.sleep(wait_s)
            raise RequestException("429 Too Many Requests")

        if status == 400:
            with _counter_lock:
                devices_skipped_counter += 1
            print(f"❌ [400 ERROR] Device registration failed for user {payload.get('userId', payload.get('email', 'unknown'))}: {resp.text[:100]}...")
            log_invalid_record("400_register_device", src_user, payload, 400, getattr(resp, "text", None))
            _append_audit_log({
                "userId": src_user.get("userId"), "email": src_user.get("email"),
                "outcome": "api_failed", "skip_reason": "400_register_device",
                "platform": dev.get("platform"), "applicationName": dev.get("applicationName"),
            })
            return

        if status == 502:
            print(f"🔄 [502] Server error, waiting 61s for Iterable timeout to clear. User: {payload.get('userId', payload.get('email', 'unknown'))}")
            time.sleep(61)  # Wait for Iterable's 60s timeout + 1s buffer
            raise RequestException("502 Bad Gateway - server timeout")

        resp.raise_for_status()
        with _counter_lock:
            devices_registered_counter += 1
            current_count = devices_registered_counter
            if user_id_str := (payload.get('userId') or payload.get('email')):
                unique_users_with_devices.add(str(user_id_str))

        user_id = payload.get('userId', payload.get('email', 'unknown'))
        platform = dev.get('platform', 'unknown')
        app_name = dev.get('applicationName', 'unknown')
        print(f"✅ [SUCCESS] Device registered for user {user_id} | {platform} | {app_name} | Status: {status}")

        _append_audit_log({
            "userId": src_user.get("userId"), "email": src_user.get("email"),
            "outcome": "registered",
            "platform": platform, "applicationName": app_name,
        })

        if current_count % 50 == 0:
            with _counter_lock:
                unique_count = len(unique_users_with_devices)
            print(f"📊 [PROGRESS] {current_count} devices registered | {unique_count} unique users updated")

        return resp.json() if "application/json" in resp.headers.get("content-type", "") else None

    except RequestException as error:
        error_str = str(error)
        if is_timeout_exc(error):
            quarantine_device("timeout_single_device", src_user, dev, None, str(error))
            _append_audit_log({
                "userId": src_user.get("userId"), "email": src_user.get("email"),
                "outcome": "quarantined", "skip_reason": "timeout_single_device",
                "platform": dev.get("platform"), "applicationName": dev.get("applicationName"),
            })
            return
        elif "Connection aborted" in error_str or "RemoteDisconnected" in error_str:
            # Connection aborts are often timeout-related, wait for server recovery
            user_id = payload.get('userId', payload.get('email', 'unknown'))
            print(f"🔄 [CONNECTION] Connection aborted, waiting 61s for server recovery. User: {user_id}")
            time.sleep(61)  # Wait for Iterable's timeout + buffer
            raise  # Re-raise to trigger retry mechanism
        log_error(f"POST failed: {error}")
        raise

#########################
# Page file processing  #
#########################

def process_json_files_page(local_file_paths: List[str]):
    global total_lines_parsed, users_skipped_counter, devices_skipped_counter
    local_file_paths = sorted(local_file_paths)
    futures = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for file_path in local_file_paths:
            with open(file_path, 'r', encoding="utf-8") as f:
                for raw in f:
                    line = raw.strip()
                    if not line:
                        continue
                    total_lines_parsed += 1
                    try:
                        u = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    devices = u.get("devices")
                    if not isinstance(devices, list) or not devices:
                        users_skipped_counter += 1
                        _append_audit_log({
                            "userId": u.get("userId"), "email": u.get("email"),
                            "outcome": "skipped", "skip_reason": "no_devices",
                        })
                        continue

                    for dev in devices:
                        if not isinstance(dev, dict):
                            devices_skipped_counter += 1
                            continue
                        payload, reason = build_register_device_payload(u, dev)
                        if payload is None:
                            devices_skipped_counter += 1
                            log_invalid_record(reason, u, {"device": dev})
                            _append_audit_log({
                                "userId": u.get("userId"), "email": u.get("email"),
                                "outcome": "skipped", "skip_reason": reason,
                                "platform": dev.get("platform"), "applicationName": dev.get("applicationName"),
                            })
                            continue
                        futures.append(executor.submit(send_register_device, payload, u, dev))

        # drain — catch per-device failures so one bad device doesn't abort the page
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as exc:
                log_error(f"Device future raised after all retries: {exc}")

###################
# Main Execution  #
###################

def main():
    global start_time, job_id, processed_files_total
    start_time = time.time()
    
    _validate_identity_settings()

    # *** ENHANCED STARTUP LOGGING ***
    print("🚀" + "="*60)
    print("🚀 ITERABLE DEVICE MIGRATION SCRIPT STARTING")
    print("🚀" + "="*60)
    print(f"📊 Job ID: {job_id}")
    print(f"📊 Origin API Key: ...{origin_api_key[-4:]}")
    print(f"📊 Destination API Key: ...{destination_api_key[-4:]}")
    print(f"📊 Max Workers: {MAX_WORKERS} (reduced for server stability)")
    print(f"📊 Max Inflight: {MAX_INFLIGHT_POSTS} (reduced for server stability)")
    print(f"📊 Rate Limit: {RATE_LIMIT_RPS} RPS (reduced for server stability)")
    print(
        f"[config] Identity: source {SOURCE_PROJECT_TYPE} ({SOURCE_PREFERRED_IDENTIFIER}) "
        f"→ destination {DESTINATION_PROJECT_TYPE} ({DESTINATION_PREFERRED_IDENTIFIER})"
    )
    _fb = _hybrid_fallback_normalized()
    if _destination_project_type_normalized() == "hybrid":
        print(
            f"[config] Hybrid fallback when preferred id missing: {_fb!r} "
            f"(use 'email' or 'userId'; None = skip row like uuid)",
            flush=True,
        )
    print(f"[config] Audit mode: {AUDIT_MODE}")
    print("🚀" + "="*60)
    print("⏳ Pausing 3 seconds before starting to be gentle on servers...")
    time.sleep(3)

    # Start or resume export
    if job_id:
        print(f"Resuming existing export with jobId: {job_id}")
    else:
        try:
            job_id = start_export()
        except requests.RequestException as e:
            print("Error starting export job:", e)
            return

    # ── JOB_ONLY: print jobId and exit ───────────────────────────────────────
    if JOB_ONLY:
        print(f"[JOB_ONLY] Export job started. jobId: {job_id}")
        print(f"[JOB_ONLY] Set job_id = {job_id} and JOB_ONLY = False to run the full migration.")
        return

    # ── SAMPLE_FILE: poll to completion, download one file, exit ─────────────
    if SAMPLE_FILE:
        print(f"[SAMPLE_FILE] Polling job {job_id} until completed…")
        files = []
        while True:
            try:
                job_state, files, _, _ = list_files_page(job_id, None)
            except requests.RequestException as e:
                print(f"[SAMPLE_FILE] Error polling: {e}"); time.sleep(5); continue
            if job_state == "failed":
                print("[SAMPLE_FILE] Export job failed."); return
            if job_state in ("cancelled", "cancelling"):
                print(f"[SAMPLE_FILE] Export job is {job_state}. Stopping."); return
            if job_state == "completed":
                break
            print(f"[SAMPLE_FILE] jobState={job_state!r} — polling again in 10s…", flush=True)
            time.sleep(10)
        if not files:
            print("[SAMPLE_FILE] Export job completed but produced no files (empty export).")
            return
        sample_entry = files[0]
        local_paths = download_page_files([sample_entry])
        if local_paths:
            print(f"[SAMPLE_FILE] Downloaded sample file: {os.path.abspath(local_paths[0])}")
        else:
            print("[SAMPLE_FILE] Sample file download failed.")
        return

    write_audit_manifest()

    # Load last cursor if present (resume per page)
    state            = load_state()
    cursor           = state.get(str(job_id))  # None on first page
    export_truncated = False

    # Page loop: list → download (≤10) → process → delete → advance
    while True:
        try:
            job_state, files, next_cursor, export_truncated = list_files_page(job_id, cursor)
        except requests.RequestException as e:
            print("Error listing files:", e)
            time.sleep(5)
            continue

        if job_state == "failed":
            print("Export job failed.")
            break

        if not files:
            if job_state == "completed":
                print("No files and job is completed. Done.")
                break
            print("No files yet; job running. Polling again in 5s...")
            time.sleep(5)
            continue

        # Respect test cap across pages
        if isinstance(testSample, int) and testSample > 0:
            remaining = testSample - processed_files_total
            if remaining <= 0:
                print(f"[test] File limit reached ({testSample}). Stopping.")
                break
            files = files[:remaining]

        print(f"Page received with {len(files)} file(s). Downloading these…")
        local_paths = download_page_files(files)

        # If any file failed to download, refresh page and retry same page (cursor unchanged)
        if len(local_paths) != len(files):
            print("Not all files downloaded; refreshing page and retrying in 5s…")
            for p in local_paths:
                try: os.remove(p)
                except OSError: pass
            time.sleep(5)
            continue

        print(f"📁 Processing {len(local_paths)} files for device registration API calls...")
        try:
            process_json_files_page(local_paths)
            processed_files_total += len(local_paths)
            print(f"✅ Completed processing {len(local_paths)} files")
        finally:
            print("Deleting this page’s files from disk…")
            delete_local_files(local_paths)

        # Advance cursor ONLY after page success
        cursor = files[-1]["file"]
        state[str(job_id)] = cursor
        save_state(state)
        print(f"[page done] Saved cursor: {cursor}")

        # If fewer than 10 files and job completed, likely last page; loop will confirm next iteration
        if len(files) < MAX_FILES_PER_PAGE and job_state == "completed":
            print("Likely last page; verifying…")

    calculate_run_time()

    if CLEANUP_AFTER:
        # optional: delete ALL export files left (should be none)
        for fn in os.listdir(dirname):
            if fn.startswith("file-") and (fn.endswith(".json") or fn.endswith(".jsonl") or fn.endswith(".txt")):
                try: os.remove(os.path.join(dirname, fn))
                except OSError: pass

    calculate_run_time()

    # Summary
    try:
        summary = {
            "job_id": job_id,
            "source_project_type": SOURCE_PROJECT_TYPE,
            "source_preferred_identifier": SOURCE_PREFERRED_IDENTIFIER,
            "destination_project_type": DESTINATION_PROJECT_TYPE,
            "destination_preferred_identifier": DESTINATION_PREFERRED_IDENTIFIER,
            "hybrid_identifier_fallback": _hybrid_fallback_normalized(),
            "audit_mode": AUDIT_MODE,
            "audit_log_path": os.path.abspath(AUDIT_LOG_PATH) if AUDIT_MODE else None,
            "audit_manifest_path": os.path.abspath(AUDIT_MANIFEST_PATH) if AUDIT_MODE else None,
            "total_lines_parsed": total_lines_parsed,
            "devices_registered": devices_registered_counter,
            "unique_users_updated": len(unique_users_with_devices),
            "users_skipped": users_skipped_counter,
            "devices_skipped": devices_skipped_counter,
            "invalid_records_log": os.path.abspath(INVALID_RECORDS_PATH),
            "timed_out_quarantine": os.path.abspath(QUARANTINE_PATH),
            "errors_log": os.path.abspath(ERROR_LOG_PATH),
            "export_dir": os.path.abspath(dirname),
            "processed_files_total": processed_files_total,
            "last_cursor": cursor,
            "export_truncated": export_truncated,
            "run_seconds": round(end_time - start_time, 2) if end_time else None
        }
        with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"[summary] Wrote {SUMMARY_PATH}")
    except Exception as e:
        print(f"[summary] Failed to write summary: {e}")

    print(f"Total lines parsed: {total_lines_parsed}")
    print(f"Devices registered: {devices_registered_counter}")
    print(f"Unique users updated: {len(unique_users_with_devices)}")
    print(f"Users skipped (no devices): {users_skipped_counter}")
    print(f"Devices skipped (missing required fields): {devices_skipped_counter}")
    print(f"Invalid records: {INVALID_RECORDS_PATH}")
    print(f"Timed-out devices quarantine: {QUARANTINE_PATH}")
    print(f"Other errors (if any): {ERROR_LOG_PATH}")
    if AUDIT_MODE:
        print(f"Audit log: {os.path.abspath(AUDIT_LOG_PATH)}")
        print(f"Audit manifest: {os.path.abspath(AUDIT_MANIFEST_PATH)}")
    if export_truncated:
        print("\n⚠ Export was truncated. Start a new export job with start_datetime set to the last processed createdAt.")
    else:
        print("\nExport was not truncated. Migration complete.")

if __name__ == "__main__":
    main()
