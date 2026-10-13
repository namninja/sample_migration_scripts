#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable Purchase Event Migration
- Exports purchase data from a source project
- Re-tracks each purchase to the destination project via /api/commerce/trackPurchase (single-event)
- Uses _id as the event id for idempotency — safe to re-run
- Resumable via export_state.json
- Tracks the last processed createdAt and writes it to the run summary as
  `next_start_datetime` for use when continuing a large/truncated export window

Note: There is no bulk API for purchase events. Rate limiting is enforced via
a token bucket (RATE_LIMIT_RPS). Keep this well below your project's limit.
"""

import functools
import os, sys, json, time, random, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

###############################################################################
# MIGRATION SETTINGS — edit the sections below
###############################################################################
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# ── WORKING DIRECTORY ─────────────────────────────────────────────────────────
dirname = os.path.join(_CONFIG_DIR, "migrate_purchase_events")

# ── API KEYS ──────────────────────────────────────────────────────────────────
origin_api_key      = ""
destination_api_key = ""

# ── API REGION (data center) ──────────────────────────────────────────────────
# Set each side to the data center where THAT project lives: "US" or "EU".
# They are independent, so US -> EU, EU -> US and same-region migrations all work.
origin_region      = "US"   # "US" or "EU" - region of the SOURCE project
destination_region = "US"   # "US" or "EU" - region of the DESTINATION project

# Derived from the two regions above. No edits needed.
REGION_URLS = {"US": "https://api.iterable.com", "EU": "https://api.eu.iterable.com"}
for _label, _value in (("origin_region", origin_region), ("destination_region", destination_region)):
    if str(_value).strip().upper() not in REGION_URLS:
        raise SystemExit(f'{_label} must be "US" or "EU" (got {_value!r}).')
origin_base_url      = REGION_URLS[origin_region.strip().upper()]
destination_base_url = REGION_URLS[destination_region.strip().upper()]

# ── EXPORT JOB ────────────────────────────────────────────────────────────────
# Set to an existing jobId to resume, or None to start a fresh export.
job_id: Optional[int] = None

# Limit total export files processed (None = process all). Useful for dry-runs.
testSample: Optional[int] = None

# Date range for the export. Keep to 1 year or less — large windows may truncate.
# Format: "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd", or ISO-8601 with offset.
start_datetime: Optional[str] = None
end_datetime:   Optional[str] = None

# ── RUN OPTIONS ───────────────────────────────────────────────────────────────

# Parallel workers for /api/commerce/trackPurchase calls.
# Each worker holds one persistent HTTP connection.  Start at 20; raise to 40
# if the destination project has no observable throttling.
WORKERS = 20

# Resume pagination from the last entry in page_log.txt.
RESUME_FROM_PAGE_LOG = True

# Jump directly to a specific page number using page_log.txt. None = normal.
START_AT_PAGE: Optional[int] = None

# Start pagination from this exact file cursor. Overrides RESUME_FROM_PAGE_LOG.
START_AFTER_FILE: Optional[str] = None

# JOB_ONLY: start the export job, print the jobId, then exit.
JOB_ONLY = False

# SAMPLE_FILE: download one export file to disk and exit without processing.
SAMPLE_FILE = False

# Delete export files after processing (logs + state are always kept).
CLEANUP_AFTER = True

# Wipe all log/state files before the run starts.
DELETE_LOGS_ON_START = True

# ── IDENTITY ──────────────────────────────────────────────────────────────────
# Project kind: "email" | "uuid" | "hybrid"
SOURCE_PROJECT_TYPE      = "email"
DESTINATION_PROJECT_TYPE = "hybrid"

# Primary identifier for destination API calls: "email" | "userId"
DESTINATION_PREFERRED_IDENTIFIER = "email"

# Hybrid projects only: fallback identifier when the preferred one is missing.
HYBRID_IDENTIFIER_FALLBACK: Optional[str] = None

retry_config = {
    "stop_max_attempt_number":     10,
    "wait_exponential_multiplier": 1000,
    "wait_exponential_max":        60000,
}

# ── TIMEOUTS ──────────────────────────────────────────────────────────────────
LIST_TIMEOUT         = 30
DOWNLOAD_TIMEOUT     = 120
POST_CONNECT_TIMEOUT = 10
POST_READ_TIMEOUT    = 30

###############################################################################
# Derived file paths — no edits needed below this line
###############################################################################

STATE_PATH           = os.path.join(dirname, "export_state.json")
PAGE_LOG_PATH        = os.path.join(dirname, "page_log.txt")
ERROR_LOG_PATH       = os.path.join(dirname, "purchase_errors.log")
INVALID_PATH         = os.path.join(dirname, "invalid_purchases.jsonl")
SKIPPED_PATH         = os.path.join(dirname, "skipped_purchases.jsonl")
SUMMARY_PATH         = os.path.join(dirname, "run_summary.json")
LAST_JOB_ID_PATH     = os.path.join(dirname, "last_export_job_id.txt")

os.makedirs(dirname, exist_ok=True)
if not os.path.exists(PAGE_LOG_PATH):
    open(PAGE_LOG_PATH, "a").close()

###############################################################################
# API endpoints
###############################################################################

export_start_endpoint      = "/api/export/start"
export_files_endpoint_tmpl = "/api/export/{job_id}/files"
track_purchase_endpoint    = "/api/commerce/trackPurchase"

###############################################################################
# Counters
###############################################################################

_current_file: str  = ""
_previous_file: str = ""
start_time = 0

total_raw_lines         = 0
total_purchases_parsed  = 0
purchases_skipped       = 0    # skipped due to identity or missing items
purchases_sent          = 0    # successfully tracked
purchases_failed        = 0    # non-2xx response from destination
downloaded_files_total  = 0
processed_files_total   = 0

_max_created_at_ms:  Optional[int] = None
_max_created_at_str: Optional[str] = None

###############################################################################
# HTTP session
###############################################################################

session = requests.Session()
_retry_strategy = Retry(
    total=3, connect=3, read=3, status=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET"},
    backoff_factor=1.5,
    raise_on_status=False,
)
_adapter = HTTPAdapter(pool_connections=WORKERS + 4, pool_maxsize=WORKERS + 4, max_retries=_retry_strategy)
session.mount("https://", _adapter)
session.mount("http://",  _adapter)
DEFAULT_HEADERS = {"Connection": "keep-alive", "Accept-Encoding": "gzip, deflate"}

_write_lock = threading.Lock()   # guards counter mutations and JSONL file writes

###############################################################################
# Rate limiter
###############################################################################


###############################################################################
# Retry decorator
###############################################################################

class NonRetryableError(Exception):
    """Raised for permanent 4xx failures that must not be retried."""


def retry_post(func):
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        max_attempts = retry_config["stop_max_attempt_number"]
        wm   = retry_config["wait_exponential_multiplier"]
        wmax = retry_config["wait_exponential_max"]
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except NonRetryableError:
                raise  # 4xx — permanent, never retry
            except Exception as exc:
                if attempt >= max_attempts:
                    raise
                cap_ms = min(wmax, wm * (2 ** (attempt - 1)))
                wait_s = random.uniform(0, cap_ms) / 1000.0
                print(
                    f"[retry] {func.__name__} attempt {attempt}/{max_attempts} failed "
                    f"({exc}); retrying in {wait_s:.1f}s…", flush=True
                )
                time.sleep(wait_s)
    return wrapped

###############################################################################
# Utilities
###############################################################################

def _parse_export_dt(raw: str) -> datetime:
    s = raw.strip()
    if s.endswith(("Z", "z")):
        s = s[:-1] + "+00:00"
    # Iterable exports produce "2023-04-14 20:40:20 +00:00" (space before offset).
    # fromisoformat() requires no space: normalise to "2023-04-14 20:40:20+00:00".
    if len(s) >= 7 and s[-7] == " " and s[-6] in "+-":
        s = s[:-7] + s[-6:]
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except ValueError:
                continue
        raise ValueError(f"Unparseable datetime: {raw!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _format_export_dt(raw: Optional[str]) -> Optional[str]:
    if not raw or not str(raw).strip():
        return None
    dt  = _parse_export_dt(str(raw))
    off = dt.utcoffset() or timedelta(0)
    s   = int(off.total_seconds())
    sgn = "+" if s >= 0 else "-"
    s   = abs(s)
    return dt.strftime("%Y-%m-%d %H:%M:%S") + f" {sgn}{s // 3600:02d}:{(s % 3600) // 60:02d}"


def parse_created_at_ms(raw: Optional[str]) -> Optional[int]:
    """Parse export createdAt string to epoch milliseconds (used for truncation-recovery comparison only)."""
    if not raw:
        return None
    try:
        dt = _parse_export_dt(raw.strip())
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _to_epoch_seconds(raw: Optional[str]) -> Optional[int]:
    """Parse export createdAt string to epoch seconds — the format the track API expects."""
    if not raw:
        return None
    try:
        dt = _parse_export_dt(str(raw).strip())
        return int(dt.timestamp())
    except Exception:
        return None


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
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        file_ctx = f"  file={_current_file or '(unknown)'}"
        if _previous_file:
            file_ctx += f"  prev={_previous_file}"
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg.rstrip()}\n{file_ctx}\n")
    except Exception:
        pass


def write_jsonl(path: str, obj: dict):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _update_max_created_at(raw_str: Optional[str], ms: Optional[int]):
    global _max_created_at_ms, _max_created_at_str
    if ms is not None and (
        _max_created_at_ms is None or ms > _max_created_at_ms
    ):
        _max_created_at_ms  = ms
        _max_created_at_str = raw_str

###############################################################################
# Identity helpers
###############################################################################

def _normalize_identifier(value) -> str:
    raw = str(value).strip().lower()
    if raw == "email":
        return "email"
    if raw in ("userid", "user_id", "uuid"):
        return "userId"
    raise ValueError(f"Identifier must be 'email' or 'userId', got {value!r}")


def _resolve_identity(
    email: Optional[str], user_id: Optional[str]
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    pk = _normalize_identifier(DESTINATION_PREFERRED_IDENTIFIER)
    dt = str(DESTINATION_PROJECT_TYPE).strip().lower()
    fb = None
    if HYBRID_IDENTIFIER_FALLBACK:
        try:
            fb = _normalize_identifier(HYBRID_IDENTIFIER_FALLBACK)
        except ValueError:
            pass

    if pk == "userId":
        if user_id:
            return "userId", user_id, None
        if dt == "hybrid" and fb == "email" and email:
            return "email", email, None
        return None, None, "missing_userId"

    if email:
        return "email", email, None
    if dt == "hybrid" and fb == "userId" and user_id:
        return "userId", user_id, None
    return None, None, "missing_email"

###############################################################################
# Export start / poll / list pages
###############################################################################

def start_export() -> int:
    body: Dict[str, Any] = {
        "dataTypeName": "purchase",
        "outputFormat": "application/x-json-stream",
    }
    sd = _format_export_dt(start_datetime)
    ed = _format_export_dt(end_datetime)
    if sd:
        body["startDateTime"] = sd
    if ed:
        body["endDateTime"] = ed

    r = session.post(
        f"{origin_base_url}{export_start_endpoint}",
        headers={"Content-Type": "application/json", "Api-Key": origin_api_key, **DEFAULT_HEADERS},
        json=body,
        timeout=LIST_TIMEOUT,
    )
    if not r.ok:
        raise requests.HTTPError(f"Export start failed: HTTP {r.status_code} — {r.text}")
    jid = int(r.json()["jobId"])
    print(f"Export job started. jobId: {jid}", flush=True)
    try:
        with open(LAST_JOB_ID_PATH, "w", encoding="utf-8") as f:
            f.write(str(jid) + "\n")
    except OSError:
        pass
    return jid


def poll_until_complete(jid: int):
    print(f"Polling export job {jid} until complete…", flush=True)
    while True:
        job_state, files, _, _ = list_files_page(jid, start_after=None)
        print(f"  jobState={job_state!r}  files_so_far={len(files)}", flush=True)
        if job_state == "completed":
            print("Export job complete.", flush=True)
            return
        if job_state in ("error", "failed"):
            raise RuntimeError(f"Export job {jid} entered state {job_state!r}")
        time.sleep(10)


def list_files_page(jid: int, start_after: Optional[str]):
    url    = f"{origin_base_url}{export_files_endpoint_tmpl.format(job_id=jid)}"
    params = {"startAfter": start_after} if start_after else {}
    r = session.get(
        url,
        headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS},
        params=params,
        timeout=LIST_TIMEOUT,
    )
    if not r.ok:
        raise requests.HTTPError(f"List files failed: HTTP {r.status_code} — {r.text}")
    data             = r.json()
    files            = data.get("files", [])
    job_state        = str(data.get("jobState", "")).lower()
    export_truncated = bool(data.get("exportTruncated", False))
    next_cursor      = files[-1]["file"] if files else None
    return job_state, files, next_cursor, export_truncated

###############################################################################
# File download
###############################################################################

def download_file(entry: Dict[str, str]) -> Optional[str]:
    global downloaded_files_total
    name = entry.get("file")
    url  = entry.get("url")
    if not name or not url:
        print(f"  [warn] Skipping malformed file entry: {entry}", flush=True)
        return None
    dest = os.path.join(dirname, name)
    try:
        with session.get(url, headers=DEFAULT_HEADERS, stream=True, timeout=DOWNLOAD_TIMEOUT) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
        downloaded_files_total += 1
        print(f"Downloaded: {name}", flush=True)
        return dest
    except Exception as exc:
        print(f"  [error] Failed to download {name}: {exc}", flush=True)
        log_error(f"Download failed for {name}: {exc}")
        return None

###############################################################################
# Payload builder
###############################################################################

# Fields that are export system fields or go to specific top-level keys in the
# trackPurchase payload — do NOT copy these into dataFields.
_PURCHASE_SYSTEM_FIELDS = frozenset({
    "_id", "email", "userId", "total", "createdAt", "sentAt",
    "eventUpdatedAt", "shoppingCartItems",
})


def build_purchase_payload(row: dict) -> Optional[Tuple[dict, str]]:
    """
    Build a single purchase payload for /api/commerce/trackPurchase.
    Returns (payload, skip_reason) where payload is None if the row must be skipped.

    trackPurchase payload shape:
    {
      "id":        "<event _id>",         # idempotency key
      "user":      { "<id_field>": "..." }, # single identifier only — no secondary
      "items":     [ { "id", "name", "price", "quantity", ... } ],
      "total":     N.NN,
      "createdAt": "<original createdAt string>",
      "dataFields": { ... }               # custom fields only; campaignId /
                                          # templateId / messageId are always dropped
    }
    """
    global purchases_skipped

    email   = row.get("email") or None
    user_id = row.get("userId") or None
    if isinstance(email,   str): email   = email.strip()   or None
    if isinstance(user_id, str): user_id = user_id.strip() or None

    id_field, id_value, id_error = _resolve_identity(email, user_id)
    if id_error:
        purchases_skipped += 1
        return None, id_error

    items = row.get("shoppingCartItems") or []
    if not isinstance(items, list) or len(items) == 0:
        purchases_skipped += 1
        return None, "missing_or_empty_shoppingCartItems"

    event_id    = row.get("_id") or None
    total       = row.get("total")
    created_at  = row.get("createdAt") or row.get("sentAt")
    created_ts  = _to_epoch_seconds(created_at)

    # Build user object — only include the primary identifier resolved by
    # _resolve_identity (which already honours DESTINATION_PREFERRED_IDENTIFIER
    # and HYBRID_IDENTIFIER_FALLBACK).  Never add the secondary identifier on
    # top: doing so in a userId-based or hybrid project causes Iterable to
    # attempt a cross-identifier merge that raises 409 ExternalKeyConflict when
    # the email is already owned by a different userId.
    user_obj: Dict[str, Any] = {id_field: id_value}

    # Build dataFields: all top-level keys that are not system fields and
    # do not contain "." (dot-notation flat copies are export artifacts).
    # campaignId / templateId / messageId are source-project attribution fields
    # with no mapping in the destination — always excluded.
    data_fields: Dict[str, Any] = {}
    for k, v in row.items():
        if k in _PURCHASE_SYSTEM_FIELDS:
            continue
        if "." in k:
            continue  # flat export artifact (e.g. "shoppingCartItems.price")
        if k in ("campaignId", "templateId", "messageId"):
            continue
        # The export sometimes wraps custom fields under a "dataFields" key.
        # Merge them directly into our dataFields dict.
        if k == "dataFields" and isinstance(v, dict):
            data_fields.update(v)
            continue
        data_fields[k] = v

    payload: Dict[str, Any] = {
        "user":  user_obj,
        "items": items,
        "total": float(total) if total is not None else 0.0,
    }
    if event_id:
        payload["id"] = event_id
    if created_ts is not None:
        payload["createdAt"] = created_ts

    if data_fields:
        payload["dataFields"] = data_fields

    return payload, ""

###############################################################################
# Single-event sender
###############################################################################

@retry_post
def send_purchase(payload: dict) -> Tuple[bool, str]:
    """
    POST one purchase to /api/commerce/trackPurchase.
    Returns (success: bool, message: str).
    trackPurchase response on success: {"msg": "...", "code": "Success"}
    """
    r = session.post(        f"{destination_base_url}{track_purchase_endpoint}",
        headers={
            "Content-Type": "application/json",
            "Api-Key":       destination_api_key,
            **DEFAULT_HEADERS,
        },
        json=payload,
        timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
    )
    if r.ok:
        return True, r.json().get("msg", "ok")
    # 4xx — permanent client/data error, do not retry
    if 400 <= r.status_code < 500:
        raise NonRetryableError(
            f"trackPurchase HTTP {r.status_code} — {r.text[:400]}"
        )
    # 5xx — transient, let retry_post handle it
    raise requests.HTTPError(
        f"trackPurchase HTTP {r.status_code} — {r.text[:400]}"
    )

###############################################################################
# File processor
###############################################################################

def _send_one(payload: dict, file_name: str) -> None:
    """Send one purchase and update global counters (called from thread pool)."""
    global purchases_sent, purchases_failed
    try:
        success, msg = send_purchase(payload)
        if success:
            with _write_lock:
                purchases_sent += 1
                if purchases_sent % 100 == 0:
                    print(f"  … {purchases_sent} sent, {purchases_failed} failed so far", flush=True)
        else:
            with _write_lock:
                purchases_failed += 1
                write_jsonl(INVALID_PATH, {"reason": msg, "source_file": file_name, "payload": payload})
            log_error(f"trackPurchase failed: {msg}  id={payload.get('id')}")
    except NonRetryableError as exc:
        with _write_lock:
            purchases_failed += 1
            write_jsonl(INVALID_PATH, {"reason": str(exc), "source_file": file_name, "payload": payload})
        log_error(f"trackPurchase non-retryable: {exc}  id={payload.get('id')}")
    except Exception as exc:
        with _write_lock:
            purchases_failed += 1
            write_jsonl(INVALID_PATH, {"reason": str(exc), "source_file": file_name, "payload": payload})
        log_error(f"trackPurchase exception: {exc}  id={payload.get('id')}")


def process_file(path: str):
    global total_raw_lines, total_purchases_parsed

    file_name = os.path.basename(path)
    payloads: List[dict] = []

    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                total_raw_lines += 1
                line = raw_line.strip()
                if not line:
                    continue

                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    log_error(f"JSON parse error: {exc} — line: {line[:200]}")
                    continue

                total_purchases_parsed += 1

                # Track createdAt for truncation recovery (parse only for comparison)
                created_raw = row.get("createdAt") or row.get("sentAt")
                _update_max_created_at(created_raw, parse_created_at_ms(created_raw))

                result = build_purchase_payload(row)
                if result is None:
                    continue
                payload, skip_reason = result

                if payload is None:
                    write_jsonl(SKIPPED_PATH, {
                        "reason":      skip_reason,
                        "source_file": file_name,
                        "row":         row,
                    })
                    continue

                payloads.append(payload)

    except (IOError, OSError) as exc:
        print(f"  [error] Could not read {path}: {exc}", flush=True)
        log_error(f"File read error: {exc}")
        return

    if not payloads:
        return

    print(f"  Sending {len(payloads)} purchase(s) with {WORKERS} workers…", flush=True)
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(_send_one, p, file_name): p for p in payloads}
        for future in as_completed(futures):
            # Exceptions are handled inside _send_one; re-raise only unexpected ones.
            exc = future.exception()
            if exc:
                log_error(f"Unexpected thread error: {exc}")

###############################################################################
# Main pagination loop
###############################################################################

def _load_start_cursor() -> Optional[str]:
    if START_AFTER_FILE:
        print(f"Starting after file: {START_AFTER_FILE}", flush=True)
        return START_AFTER_FILE

    if RESUME_FROM_PAGE_LOG and os.path.exists(PAGE_LOG_PATH):
        try:
            with open(PAGE_LOG_PATH, "r", encoding="utf-8") as f:
                lines = [l.strip() for l in f if l.strip()]
            if lines:
                if START_AT_PAGE is not None:
                    idx = START_AT_PAGE - 1
                    if 0 <= idx < len(lines):
                        cursor = lines[idx]
                        print(f"Resuming at page {START_AT_PAGE} (cursor: {cursor})", flush=True)
                        return cursor
                else:
                    cursor = lines[-1]
                    print(f"Resuming from last page log entry: {cursor}", flush=True)
                    return cursor
        except Exception:
            pass

    return None


def _append_page_log(cursor: str):
    with open(PAGE_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(cursor + "\n")


def run():
    global _current_file, _previous_file, processed_files_total, start_time

    start_time = time.time()

    if DELETE_LOGS_ON_START:
        for p in [
            ERROR_LOG_PATH, INVALID_PATH, SKIPPED_PATH,
            PAGE_LOG_PATH, STATE_PATH, SUMMARY_PATH,
        ]:
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

    jid = job_id
    if jid is None:
        jid = start_export()
        save_state({"job_id": jid, "processed_files": []})
    else:
        print(f"Using existing export job: {jid}", flush=True)

    if JOB_ONLY:
        print(f"JOB_ONLY=True — exiting. jobId: {jid}", flush=True)
        return

    poll_until_complete(jid)

    if SAMPLE_FILE:
        _, files, _, _ = list_files_page(jid, start_after=None)
        if files:
            dest = download_file(files[0])
            if dest:
                print(f"SAMPLE_FILE downloaded: {dest}", flush=True)
        print("SAMPLE_FILE=True — exiting without processing.", flush=True)
        return

    cursor         = _load_start_cursor()
    state          = load_state()
    processed_set  = set(state.get("processed_files", []))
    files_seen     = 0
    sample_reached   = False
    export_truncated = False

    while True:
        job_state, files, next_cursor, export_truncated = list_files_page(jid, start_after=cursor)

        if not files:
            print("No more files. Export complete.", flush=True)
            break

        print(
            f"\n--- Page: {len(files)} file(s) "
            f"(cursor={cursor!r}, jobState={job_state!r}) ---",
            flush=True,
        )

        print(f"[page] Downloading and processing {len(files)} file(s)…", flush=True)
        for entry in files:
            files_seen += 1

            if testSample is not None and files_seen > testSample:
                sample_reached = True
                break

            file_name = entry.get("file", "")

            if file_name and file_name in processed_set:
                print(f"  Skipping {file_name} (already processed).", flush=True)
                processed_files_total += 1
                continue

            file_path = download_file(entry)
            if not file_path:
                continue

            _previous_file = _current_file
            _current_file  = file_name

            print(f"  Processing {_current_file} …", flush=True)
            process_file(file_path)
            processed_files_total += 1

            if file_name:
                processed_set.add(file_name)
                save_state({"job_id": jid, "last_cursor": cursor, "processed_files": list(processed_set)})

            if CLEANUP_AFTER:
                try:
                    os.remove(file_path)
                except OSError:
                    pass

            if next_cursor:
                _append_page_log(next_cursor)
                save_state({"job_id": jid, "last_cursor": next_cursor, "processed_files": list(processed_set)})

        if sample_reached:
            print(f"testSample={testSample} reached — stopping.", flush=True)
            break

        if next_cursor:
            cursor = next_cursor
        else:
            print("No next_cursor — all pages consumed.", flush=True)
            break

    write_summary(jid, export_truncated)


def write_summary(jid: int, export_truncated: bool):
    elapsed = time.time() - start_time

    next_start = None
    if _max_created_at_str:
        try:
            dt = _parse_export_dt(_max_created_at_str)
            dt_next    = dt + timedelta(seconds=1)
            next_start = dt_next.strftime("%Y-%m-%d %H:%M:%S +00:00")
        except Exception:
            next_start = _max_created_at_str

    summary = {
        "job_id":                   jid,
        "start_datetime_config":    start_datetime,
        "end_datetime_config":      end_datetime,
        "elapsed_seconds":          round(elapsed, 2),
        "total_raw_lines":          total_raw_lines,
        "total_purchases_parsed":   total_purchases_parsed,
        "purchases_skipped":        purchases_skipped,
        "purchases_sent":           purchases_sent,
        "purchases_failed":         purchases_failed,
        "downloaded_files":         downloaded_files_total,
        "processed_files":          processed_files_total,
        "export_truncated":         export_truncated,
        "next_start_datetime":      next_start if export_truncated else None,
    }

    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n═══ Run Summary ═══", flush=True)
    print(f"  Elapsed:          {elapsed:.1f}s", flush=True)
    print(f"  Purchases parsed: {total_purchases_parsed}", flush=True)
    print(f"  Skipped:          {purchases_skipped}", flush=True)
    print(f"  Sent (success):   {purchases_sent}", flush=True)
    print(f"  Failed:           {purchases_failed}", flush=True)
    if export_truncated:
        print(f"\n  ⚠ Export was truncated. Start a new export job with start_datetime = {next_start!r}", flush=True)
    else:
        print("\n  Export was not truncated. Migration complete.", flush=True)
    print(f"\n  Summary: {SUMMARY_PATH}", flush=True)
    print("═══════════════════", flush=True)


###############################################################################
# Entry point
###############################################################################

if __name__ == "__main__":
    run()
