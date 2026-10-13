#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable Custom Event Migration
- Exports customEvent data from a source project
- Filters out commerce events (updateCart, etc.)
- Re-tracks each event to the destination project using /api/events/trackBulk
- Handles partial-failure responses from the bulk API (HTTP 200 with failCount > 0)
- Resumable via export_state.json
- Tracks the last processed createdAt and writes it to the run summary as
  `next_start_datetime` for use when continuing a large/truncated export window
"""

import functools
import os, sys, json, time, random, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, ReadTimeout
from urllib3.exceptions import ReadTimeoutError, ProtocolError

###############################################################################
# MIGRATION SETTINGS — edit the sections below
###############################################################################
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# ── WORKING DIRECTORY ─────────────────────────────────────────────────────────
# Where export files and logs are written (created if missing)
dirname = os.path.join(_CONFIG_DIR, "migrate_custom_events")

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

# Resume pagination from the last entry in page_log.txt.
# When False, processing always starts from the first available file.
RESUME_FROM_PAGE_LOG = False

# Jump directly to a specific page number using page_log.txt. None = normal.
START_AT_PAGE: Optional[int] = None

# Start pagination from this exact file cursor (the file *after* this one is
# the first processed). Overrides RESUME_FROM_PAGE_LOG / START_AT_PAGE.
START_AFTER_FILE: Optional[str] = None

# JOB_ONLY: start (or log) the export job, print the jobId, then exit.
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
# "email" | "userId" | None
HYBRID_IDENTIFIER_FALLBACK: Optional[str] = None

# ── EVENT SKIP LIST ───────────────────────────────────────────────────────────
# eventNames listed here are skipped entirely and never sent to the destination.
EVENTS_TO_SKIP: List[str] = [
    "updateCart",   # commerce event included in the Custom Event Export — always skip
]

# ── EVENT TRANSFORMATIONS ─────────────────────────────────────────────────────

# Rename eventName values before sending to the destination.
# Source name (as exported) → destination name.
# Events not listed here are sent with their original name.
EVENT_NAME_MAP: Dict[str, str] = {
    # "signup complete": "user_signup",
    # "app initialized": "app_open",
}

# Per-event dataField transformations.
# Outer key = source eventName (use "*" to apply to ALL events).
# Event-specific rules are applied AFTER the "*" wildcard rules.
# "rename": rename a dataField key.   { "oldKey": "newKey" }
# "recast": change a value's type.    { "key": "bool" | "int" | "float" | "str" }
EVENT_FIELD_ADJUSTMENTS: Dict[str, Dict[str, Any]] = {
    # Apply to every event:
    # "*": {
    #     "rename": { "oldFieldName": "new_field_name" },
    #     "recast": {},
    # },
    # Apply only to "signup complete" events:
    # "signup complete": {
    #     "rename": { "firstName": "first_name", "lastName": "last_name" },
    #     "recast": { "phoneNumber": "str" },
    # },
}

# Fields injected into EVERY event's dataFields (applied last — wins over
# export values if the same key is present).
# Use this to stamp migration metadata, a site_id, etc.
ADDITIONAL_EVENT_DATA_FIELDS: Dict[str, Any] = {
    # "migration": True,
    # "migrated_at": "2026-01-01",
}

# ── BATCHING ──────────────────────────────────────────────────────────────────
# Events per /api/events/trackBulk call. Max is 2000; 500 is a safe default.
BATCH_SIZE       = 500

# Workers used when a bulk batch is rejected and we fall back to per-event
# /api/events/track calls.  Keeps the fallback fast rather than sequential.
FALLBACK_WORKERS = 20

retry_config = {
    "stop_max_attempt_number":     10,
    "wait_exponential_multiplier": 1000,  # ms; doubles each retry: 1s, 2s … 60s
    "wait_exponential_max":        60000,
}

# ── TIMEOUTS ──────────────────────────────────────────────────────────────────
LIST_TIMEOUT         = 30    # GET /export/{id}/files
DOWNLOAD_TIMEOUT     = 120   # file download
POST_CONNECT_TIMEOUT = 10    # TCP connect for POST
POST_READ_TIMEOUT    = 120   # response read for POST

# ── LOGGING ───────────────────────────────────────────────────────────────────
# Preview this many invalidMessages entries per batch in the console.
bulk_api_failure_preview_limit = 5

###############################################################################
# Derived file paths — no edits needed below this line
###############################################################################

STATE_PATH           = os.path.join(dirname, "export_state.json")
PAGE_LOG_PATH        = os.path.join(dirname, "page_log.txt")
ERROR_LOG_PATH       = os.path.join(dirname, "event_errors.log")
INVALID_EVENTS_PATH  = os.path.join(dirname, "invalid_events.jsonl")
SKIPPED_EVENTS_PATH  = os.path.join(dirname, "skipped_events.jsonl")
SUMMARY_PATH         = os.path.join(dirname, "run_summary.json")
LAST_JOB_ID_PATH     = os.path.join(dirname, "last_export_job_id.txt")
BULK_API_LOG_PATH    = os.path.join(dirname, "bulk_api.jsonl")

os.makedirs(dirname, exist_ok=True)
if not os.path.exists(PAGE_LOG_PATH):
    open(PAGE_LOG_PATH, "a").close()

###############################################################################
# API endpoints
###############################################################################

export_start_endpoint         = "/api/export/start"
export_files_endpoint_tmpl    = "/api/export/{job_id}/files"
track_bulk_endpoint           = "/api/events/trackBulk"

###############################################################################
# Counters
###############################################################################

_current_file: str  = ""
_previous_file: str = ""
start_time = 0

total_raw_lines          = 0   # Physical lines read from all files
total_events_parsed      = 0   # Lines successfully parsed as JSON
events_skipped_skip_list  = 0   # Skipped because eventName is in EVENTS_TO_SKIP
events_skipped_identity  = 0   # Skipped because required identity field is missing
events_skipped_no_name   = 0   # Skipped because eventName is empty
events_sent              = 0   # Successfully sent to destination
events_failed            = 0   # Reported as failures by the bulk API
downloaded_files_total   = 0
processed_files_total    = 0

# Track the latest createdAt seen (for truncation recovery)
_max_created_at_ms: Optional[int] = None
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
_adapter = HTTPAdapter(pool_connections=FALLBACK_WORKERS + 4, pool_maxsize=FALLBACK_WORKERS + 4, max_retries=_retry_strategy)
session.mount("https://", _adapter)
session.mount("http://",  _adapter)
DEFAULT_HEADERS = {"Connection": "keep-alive", "Accept-Encoding": "gzip, deflate"}

_write_lock = threading.Lock()   # guards counter mutations and JSONL file writes

###############################################################################
# Retry decorator
###############################################################################

class NonRetryableError(Exception):
    """Raised for permanent failures (4xx) that must not be retried."""


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
                raise  # 4xx — permanent failure, never retry
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
    """Convert a config datetime string to Iterable export format."""
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
    """
    Returns (field_name, field_value, error_reason).
    error_reason is set when the row must be skipped.
    """
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
        "dataTypeName":  "customEvent",
        "outputFormat":  "application/x-json-stream",
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
    """Poll the files endpoint until jobState == 'completed'."""
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
# Event transformation helpers
###############################################################################

def _recast_value(value: Any, target_type: str) -> Any:
    """Cast a dataField value to the requested type. Returns original on failure."""
    try:
        t = str(target_type).strip().lower()
        if t == "bool":
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("1", "true", "yes")
        if t == "int":
            return int(value)
        if t == "float":
            return float(value)
        if t == "str":
            return str(value)
    except (ValueError, TypeError):
        pass
    return value


def _apply_event_transformations(
    event_name: str, data_fields: Dict[str, Any]
) -> Tuple[str, Dict[str, Any]]:
    """
    Apply EVENT_NAME_MAP, EVENT_FIELD_ADJUSTMENTS, and ADDITIONAL_EVENT_DATA_FIELDS.

    Order of operations:
      1. Rename the eventName via EVENT_NAME_MAP
      2. Apply wildcard ("*") field rules from EVENT_FIELD_ADJUSTMENTS
      3. Apply event-specific field rules from EVENT_FIELD_ADJUSTMENTS
      4. Inject ADDITIONAL_EVENT_DATA_FIELDS (always wins on key collision)

    Returns (transformed_event_name, transformed_data_fields).
    """
    # 1. eventName rename
    dest_event_name = EVENT_NAME_MAP.get(event_name, event_name)

    # Work on a shallow copy so we don't mutate the original row data
    fields = dict(data_fields)

    # Helper: apply one rename+recast block to `fields`
    def _apply_block(block: Dict[str, Any]):
        renames = block.get("rename") or {}
        recasts = block.get("recast") or {}

        # Rename first (before recasting so recast keys refer to new names)
        for old_key, new_key in renames.items():
            if old_key in fields:
                fields[new_key] = fields.pop(old_key)

        # Recast (keys are destination names, i.e. after rename)
        for key, target_type in recasts.items():
            if key in fields:
                fields[key] = _recast_value(fields[key], target_type)

    # 2. Wildcard rules (apply to all events)
    wildcard_block = EVENT_FIELD_ADJUSTMENTS.get("*")
    if wildcard_block:
        _apply_block(wildcard_block)

    # 3. Event-specific rules (use the *source* event name as the lookup key
    #    so config always matches what's in the export, not the renamed name)
    specific_block = EVENT_FIELD_ADJUSTMENTS.get(event_name)
    if specific_block:
        _apply_block(specific_block)

    # 4. Inject additional fields (overwrite on collision)
    if ADDITIONAL_EVENT_DATA_FIELDS:
        fields.update(ADDITIONAL_EVENT_DATA_FIELDS)

    return dest_event_name, fields


###############################################################################
# Payload builder
###############################################################################

# System-level fields from the export that are NOT event data.
# campaignId / templateId / messageId are source-project attribution fields
# that have no mapping in the destination — always excluded.
_SYSTEM_FIELDS = frozenset({
    "eventName", "_id", "email", "userId", "createdAt", "sentAt",
    "eventUpdatedAt", "campaignId", "templateId", "messageId",
})


def build_event_payload(row: dict) -> Optional[Tuple[dict, str]]:
    """
    Build a single event payload for /api/events/trackBulk.
    Returns (payload, skip_reason) where payload is None if the row must be skipped.
    """
    global events_skipped_skip_list, events_skipped_identity, events_skipped_no_name

    event_name  = (row.get("eventName") or "").strip()
    if not event_name:
        events_skipped_no_name += 1
        return None, "missing_eventName"

    # Skip events in the ignore list
    if event_name in EVENTS_TO_SKIP:
        events_skipped_skip_list += 1
        return None, f"skipped:{event_name}"

    email   = row.get("email") or None
    user_id = row.get("userId") or None
    if isinstance(email,   str):
        email   = email.strip()   or None
    if isinstance(user_id, str):
        user_id = user_id.strip() or None

    id_field, id_value, id_error = _resolve_identity(email, user_id)
    if id_error:
        events_skipped_identity += 1
        return None, id_error

    event_id    = row.get("_id") or None
    created_at  = row.get("createdAt") or row.get("sentAt")
    created_ts  = _to_epoch_seconds(created_at)

    # dataFields: use the nested event object (e.g. row["signup complete"])
    # This is the cleanest representation — mirrors what was originally tracked.
    data_fields = row.get(event_name)
    if not isinstance(data_fields, dict):
        # Fallback: reconstruct from non-system, non-dotted-key fields
        data_fields = {
            k: v for k, v in row.items()
            if k not in _SYSTEM_FIELDS and k != event_name and "." not in k
        }

    # Apply transformations: rename eventName, rename/recast fields, inject extras
    event_name, data_fields = _apply_event_transformations(event_name, data_fields)

    payload: Dict[str, Any] = {
        id_field:    id_value,
        "eventName": event_name,
    }
    if event_id:
        payload["id"] = event_id
    if created_ts is not None:
        payload["createdAt"] = created_ts
    if data_fields:
        payload["dataFields"] = data_fields

    # Only the single identifier resolved by _resolve_identity is included.
    # Never append the secondary identifier — sending both can cause a 409
    # ExternalKeyConflict in userId-based and hybrid projects when the email
    # is already owned by a different userId.

    return payload, ""


###############################################################################
# Bulk track sender
###############################################################################

@retry_post
def send_bulk_events(events: List[dict]) -> Tuple[int, int, List[dict]]:
    """
    POST to /api/events/trackBulk.

    Bulk API response (HTTP 200 even on partial failure):
    {
      "successCount": N,
      "failCount": M,
      "invalidMessages": [
        {"msg": "error description", "params": { <original event object> }}
      ]
    }

    Returns (success_count, fail_count, invalid_messages).
    Raises on HTTP error (non-200 status).
    """
    r = session.post(
        f"{destination_base_url}{track_bulk_endpoint}",
        headers={
            "Content-Type": "application/json",
            "Api-Key":       destination_api_key,
            **DEFAULT_HEADERS,
        },
        json={"events": events},
        timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
    )

    # Non-200 is a hard error (network/auth problem, not a validation failure)
    if r.status_code >= 400 and r.status_code < 500:
        raise NonRetryableError(
            f"trackBulk HTTP {r.status_code} — {r.text[:500]}"
        )
    if not r.ok:
        raise requests.HTTPError(
            f"trackBulk HTTP {r.status_code} — {r.text[:500]}"
        )

    body            = r.json()
    success_count   = body.get("successCount", 0)
    fail_count      = body.get("failCount", 0)
    invalid_msgs    = body.get("invalidMessages") or []

    return success_count, fail_count, invalid_msgs


@retry_post
def send_single_event(event: dict) -> None:
    """
    POST a single event to /api/events/track.
    Raises NonRetryableError on 4xx (bad data), HTTPError on 5xx (retried by decorator).
    """
    r = session.post(
        f"{destination_base_url}/api/events/track",
        headers={
            "Content-Type": "application/json",
            "Api-Key":       destination_api_key,
            **DEFAULT_HEADERS,
        },
        json=event,
        timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
    )
    if r.status_code >= 400 and r.status_code < 500:
        raise NonRetryableError(f"track HTTP {r.status_code} — {r.text[:500]}")
    if not r.ok:
        raise requests.HTTPError(f"track HTTP {r.status_code} — {r.text[:500]}")


def _fallback_send_individually(batch: List[dict], file_name: str):
    """
    Send each event in batch one-by-one via /api/events/track using a thread pool.
    Called when trackBulk rejects the whole batch with a 4xx so that valid
    events are still saved and only the bad ones are discarded.
    """
    global events_sent, events_failed

    sent = failed = 0
    total = len(batch)
    completed = 0

    def _send_one(evt):
        nonlocal completed
        try:
            send_single_event(evt)
            return True, None
        except NonRetryableError as exc:
            return False, f"non_retryable_4xx: {exc}"
        except Exception as exc:
            return False, f"hard_failure: {exc}"

    print(f"  [fallback] Sending {total} event(s) individually with {FALLBACK_WORKERS} workers…", flush=True)

    with ThreadPoolExecutor(max_workers=FALLBACK_WORKERS) as executor:
        futures = {executor.submit(_send_one, evt): evt for evt in batch}
        for future in as_completed(futures):
            evt = futures[future]
            success, reason = future.result()
            completed += 1
            if success:
                sent += 1
            else:
                failed += 1
                log_error(f"track rejected event ({reason}) — file={file_name}")
                write_jsonl(INVALID_EVENTS_PATH, {
                    "reason":      reason,
                    "source_file": file_name,
                    "event":       evt,
                })
            if completed % 100 == 0:
                print(f"  [fallback] {completed}/{total} done ({sent} sent, {failed} failed)…", flush=True)

    events_sent   += sent
    events_failed += failed
    print(f"  [fallback] complete: {sent} sent, {failed} rejected from batch of {total}.", flush=True)
    return sent, failed


def _send_batch(batch: List[dict], file_name: str):
    """
    Send a batch via trackBulk. On a 4xx rejection (bad data poisoning the whole
    batch), fall back to /api/events/track one-by-one to isolate bad events and
    save all valid ones.
    """
    global events_sent, events_failed

    if not batch:
        return 0, 0

    try:
        success_count, fail_count, invalid_msgs = send_bulk_events(batch)

        events_sent   += success_count
        events_failed += fail_count

        write_jsonl(BULK_API_LOG_PATH, {
            "ts":           datetime.now(timezone.utc).isoformat(),
            "source_file":  file_name,
            "batch_size":   len(batch),
            "successCount": success_count,
            "failCount":    fail_count,
        })

        if fail_count > 0:
            print(f"  [warn] {fail_count} event(s) rejected by destination in this batch.", flush=True)
            for entry in invalid_msgs:
                err_msg  = entry.get("msg", "(no message)")
                orig_evt = entry.get("params", {})
                write_jsonl(INVALID_EVENTS_PATH, {
                    "reason":      err_msg,
                    "source_file": file_name,
                    "event":       orig_evt,
                })

        return success_count, fail_count

    except NonRetryableError as exc:
        # Whole batch rejected — fall back to per-event calls to isolate bad ones.
        print(
            f"  [fallback] trackBulk rejected batch of {len(batch)} ({exc}) — "
            f"retrying individually via /api/events/track",
            flush=True,
        )
        return _fallback_send_individually(batch, file_name)

    except Exception as exc:
        # 5xx / network — retries exhausted; log entire batch as failed.
        log_error(f"trackBulk hard failure ({exc}) — batch of {len(batch)} events from {file_name}")
        for evt in batch:
            write_jsonl(INVALID_EVENTS_PATH, {
                "reason":      f"batch_hard_failure: {exc}",
                "source_file": file_name,
                "event":       evt,
            })
        events_failed += len(batch)
        return 0, len(batch)


def flush_batch(batch: List[dict], file_name: str):
    """Send a batch; fall back to per-event calls on 4xx rejection."""
    if not batch:
        return
    _send_batch(batch, file_name)


###############################################################################
# File processor
###############################################################################

def process_file(path: str):
    """Read a downloaded export file, build event payloads, and flush batches."""
    global total_raw_lines, total_events_parsed

    file_name = os.path.basename(path)
    batch: List[dict] = []

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

                total_events_parsed += 1

                # Track createdAt for truncation recovery (parse only for comparison)
                created_raw = row.get("createdAt") or row.get("sentAt")
                _update_max_created_at(created_raw, parse_created_at_ms(created_raw))

                result = build_event_payload(row)
                if result is None:
                    continue
                payload, skip_reason = result

                if payload is None:
                    if skip_reason and not skip_reason.startswith("skipped:"):
                        write_jsonl(SKIPPED_EVENTS_PATH, {
                            "reason":      skip_reason,
                            "source_file": file_name,
                            "row":         row,
                        })
                    continue

                batch.append(payload)

                if len(batch) >= BATCH_SIZE:
                    flush_batch(batch, file_name)
                    batch = []

        # Flush remaining
        if batch:
            flush_batch(batch, file_name)

    except (IOError, OSError) as exc:
        print(f"  [error] Could not read {path}: {exc}", flush=True)
        log_error(f"File read error: {exc}")


###############################################################################
# Main pagination loop
###############################################################################

def _load_start_cursor() -> Optional[str]:
    """Determine the pagination cursor to resume from."""
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
            ERROR_LOG_PATH, INVALID_EVENTS_PATH, SKIPPED_EVENTS_PATH,
            BULK_API_LOG_PATH, PAGE_LOG_PATH, STATE_PATH, SUMMARY_PATH,
        ]:
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

    # Resolve job ID
    jid = job_id
    if jid is None:
        jid = start_export()
        save_state({"job_id": jid, "processed_files": []})
    else:
        print(f"Using existing export job: {jid}", flush=True)

    if JOB_ONLY:
        print(f"JOB_ONLY=True — exiting. jobId: {jid}", flush=True)
        return

    # Poll until the job is complete
    poll_until_complete(jid)

    if SAMPLE_FILE:
        _, files, _, _ = list_files_page(jid, start_after=None)
        if files:
            dest = download_file(files[0])
            if dest:
                print(f"SAMPLE_FILE downloaded: {dest}", flush=True)
        print("SAMPLE_FILE=True — exiting without processing.", flush=True)
        return

    # Paginate through all files
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
        "job_id":                  jid,
        "start_datetime_config":   start_datetime,
        "end_datetime_config":     end_datetime,
        "elapsed_seconds":         round(elapsed, 2),
        "total_raw_lines":         total_raw_lines,
        "total_events_parsed":     total_events_parsed,
        "events_skipped_skip_list": events_skipped_skip_list,
        "events_skipped_identity": events_skipped_identity,
        "events_skipped_no_name":  events_skipped_no_name,
        "events_sent":             events_sent,
        "events_failed":           events_failed,
        "downloaded_files":        downloaded_files_total,
        "processed_files":         processed_files_total,
        "export_truncated":        export_truncated,
        "next_start_datetime":     next_start if export_truncated else None,
    }

    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n═══ Run Summary ═══", flush=True)
    print(f"  Elapsed:           {elapsed:.1f}s", flush=True)
    print(f"  Lines parsed:      {total_events_parsed}", flush=True)
    print(f"  Skipped (skip list):{events_skipped_skip_list}", flush=True)
    print(f"  Skipped (identity):{events_skipped_identity}", flush=True)
    print(f"  Sent (success):    {events_sent}", flush=True)
    print(f"  Failed (API):      {events_failed}", flush=True)
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
