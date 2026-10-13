#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import functools
from collections import OrderedDict
import os, sys, json, time, threading, random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException, ReadTimeout
from urllib3.exceptions import ReadTimeoutError, ProtocolError

###############################################################################
# MIGRATION SETTINGS — edit the sections below
# (all paths are anchored to this file's directory so Spyder / cwd-independent)
###############################################################################
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))
dirname = os.path.join(_CONFIG_DIR, "migrate_users")

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

# Existing export job ID to resume, or None to start a fresh export.
job_id: Optional[int] = None

# Limit processing to this many export files after the job completes (each file
# is processed in full). Useful for dry-runs. None = process every file.
testSample: Optional[int] = None

# Comma-separated fields to omit from each exported user row (reduces payload size).
omit_fields: str = (
    "emailListIds,receivedSMSDisclaimer,itblInternal,userObject,itblDS,itblUserId,"
    "userListIds,devices.email,devices.phone"
)

# Restrict export to a time window. Format: "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd",
# or ISO-8601 with offset e.g. "2024-06-01T00:00:00-07:00". None = no filter.
start_datetime: Optional[str] = None
end_datetime:   Optional[str] = None

# Restrict export to specific fields (comma-separated). None = all fields.
only_fields: Optional[str] = (
    "phoneNumber,signupDate,firstName,lastName,customField1,customField2,"
    "unsubscribedChannelIds,unsubscribedMessageTypeIds,subscribedMessageTypeIds,userId,email,devices"
)

# ── RUN OPTIONS ───────────────────────────────────────────────────────────────

# Delete downloaded export files after a successful run (logs are always kept).
CLEANUP_AFTER = True

# Resume from the last entry in page_log.txt. When False, always starts from
# the beginning of the job (ignores any saved cursor in export_state.json).
RESUME_FROM_PAGE_LOG = False

# Jump directly to a specific page number using page_log.txt. None = normal order.
START_AT_PAGE: Optional[int] = None

# Start pagination from this exact file cursor — the file *after* this one will
# be the first processed. Overrides RESUME_FROM_PAGE_LOG and START_AT_PAGE.
# Set to None to let the normal resume/start logic decide.
START_AFTER_FILE: Optional[str] = None  # e.g. "file-1234567890123.json"
# Wipe all log/state files (bulk_errors.log, invalid_users.jsonl,
# timed_out_records.jsonl, bulk_user_api.jsonl, bulk_subs_api.jsonl,
# page_log.txt, export_state.json, run_summary.json) before the run starts.
DELETE_LOGS_ON_START = False

# JOB_ONLY: start (or log) the export job, print the jobId, then exit immediately.
# Useful to kick off an export without waiting for it to finish.
JOB_ONLY = False

# SAMPLE_FILE: start the job (or use an existing job_id), poll until
# jobState == "completed", download a single export file to disk, print its
# path, then exit. No user migration is performed.
# JOB_ONLY takes precedence if both are True.
SAMPLE_FILE = False

# ── IDENTITY ──────────────────────────────────────────────────────────────────

# Project kind for source and destination: "email" | "uuid" | "hybrid"
SOURCE_PROJECT_TYPE      = "email"
DESTINATION_PROJECT_TYPE = "hybrid"

# Primary identifier used at the API root for bulk update calls.
# For hybrid projects this controls which key goes at the root vs. in dataFields.
SOURCE_PREFERRED_IDENTIFIER      = "email"
DESTINATION_PREFERRED_IDENTIFIER = "email"

# Hybrid Projects-only: fall back to this identifier when the preferred one is missing.
# "email" | "userId" | None (None = skip the row, same strict behaviour as uuid).
HYBRID_IDENTIFIER_FALLBACK: Optional[str] = None

# When the preferred identifier is "email", controls whether the secondary
# identifier (userId) is also written into dataFields so the destination
# profile has its userId set/updated.
# True (default): userId is included — Iterable will assign it to the profile.
#   If that userId is already bound to a DIFFERENT email in the destination,
#   Iterable returns conflictUserIds for that user (logged in failedUpdates).
#   Those conflicts are genuine data issues that need manual resolution.
# False: userId is never sent — destination profiles will have no userId.
#   Use only when you intentionally want an email-only destination.
INCLUDE_SECONDARY_ID_IN_DATAFIELDS: bool = True

# ── DATA MAPPING ──────────────────────────────────────────────────────────────

# Rename or recast export field values before sending to the destination.
# Only keys that are present on the export row are affected (no auto-defaults).
USER_EXPORT_FIELD_ADJUSTMENTS: Dict[str, Any] = {
    "rename": {
        # "sourceFieldName": "destinationFieldName",
    },
    "recast": {
        # "destinationFieldName": "bool" | "int" | "float" | "str"
        # Only applied when the field is present on the export row.
        
    },
}

# Fields injected into every user's dataFields on bulkUpdate (applied last;
# these win over export values if the same key exists in both).
additional_user_data_fields: Dict[str, Any] = {
    "is_migrated": True
}

# Allow Iterable to create brand-new dataField keys that don't yet exist in the
# destination schema. Set False only if you want to restrict to existing fields.
create_new_fields = True

# Skip rows whose email contains any of these substrings (case-insensitive).
SKIP_EMAIL_SUBSTRINGS: List[str] = [
    "placeholder",
    # Disposable / temporary email domains
    "cosoinan.com",
    "mailinator.com",
    "temp-mail.me",
    "temp-mail.best",
    "temp-email.io",
    "guerrillamailblock.com",
    "guerrillamail.com",
    "throwawaymail.app",
    "trash-mail.com",
    "trashmail.fr",
    "yopmail.com",
    "yopmail.fr",
    "yopmail.net",
    "disposable.com",
]

# Drop these field names from every user's dataFields before sending to the
# destination. Useful when reusing an existing jobId whose export contains fields
# you don't want to migrate (avoids needing to restart the export with omitFields).
# Reference fields by their original export name (before any renames).
DROP_FIELDS: List[str] = [
    # "someUnwantedField",
]

# Whitelist — only these fields will be included in the bulk update payload.
# All other export fields are silently dropped. Takes effect after DROP_FIELDS.
# Reference fields by their original export name (before any renames).
# Empty list or None = include all fields (no filtering).
INCLUDE_ONLY_FIELDS: List[str] = [
    # "customField1",
    # "signupDate",
]

# Skip rows where any of these fields is absent or null.
# Supports dot-notation for nested fields (e.g. "itblInternal.documentCreatedAt").
# Empty list = no required-field filtering.
REQUIRED_FIELDS_FOR_MIGRATION: List[str] = []

# Skip rows where the given field's date value is on or after the cutoff (YYYY-MM-DD).
# Supports dot-notation for nested fields (e.g. "itblInternal.documentCreatedAt").
# Empty dict = no date filtering.
FIELD_DATE_BEFORE: Dict[str, str] = {
    # "itblInternal.documentCreatedAt": "YYYY-MM-DD",
}

# Source → destination channel ID mapping for /users/bulkUpdateSubscriptions.
CHANNEL_ID_MAP: Dict[int, int] = {
    # <source channelId>: <destination channelId>,  # e.g. Email Marketing
}

# Source → destination message type ID mapping.
MESSAGE_TYPE_ID_MAP: Dict[int, int] = {
    # <source messageTypeId>: <destination messageTypeId>,  # e.g. Email Marketing
}

# ── BATCHING & RATE LIMITS ────────────────────────────────────────────────────

BATCH_SIZE             = 750      # Users per bulk API call
MAX_WORKERS            = 12       # Parallel file-download threads
MAX_INFLIGHT_BULK_POSTS = 3       # Max concurrent bulk POST requests
approx_payload_byte_limit = 5_000_000  # Split batch if body exceeds this size

RATE_LIMIT_RPS_BULK_USERS = 5    # Max bulk user update requests per second
RATE_LIMIT_RPS_BULK_SUBS  = 5    # Max bulk subscription update requests per second

retry_config = {
    "stop_max_attempt_number":     15,    # Total attempts before giving up
    "wait_exponential_multiplier": 1000,  # Base wait in ms (doubles each retry: 1s, 2s, 4s … 60s)
    "wait_exponential_max":        60000, # Cap per sleep in ms
}

# ── TIMEOUTS ──────────────────────────────────────────────────────────────────

LIST_TIMEOUT         = 30   # seconds — GET /export/{id}/files
DOWNLOAD_TIMEOUT     = 120  # seconds — file download stream
POST_CONNECT_TIMEOUT = 10   # seconds — TCP connect for bulk POST calls
POST_READ_TIMEOUT    = 300  # seconds — response read for bulk POST calls

# ── LOGGING ───────────────────────────────────────────────────────────────────

# Write one JSONL row per batch to bulk_user_api.jsonl (failures always logged).
log_bulk_user_api_jsonl              = True
# Print a redacted sample of the first user payload in each batch for debugging.
debug_log_sample_bulk_user_payload   = False
# Number of failedUpdates entries to preview in the console per batch.
bulk_user_failed_updates_preview_limit = 3
# Write one JSONL row per batch that has failures to bulk_subs_api.jsonl.
log_bulk_subs_api_jsonl              = True

# ── AUDIT MODE ────────────────────────────────────────────────────────────────
# When enabled, writes two files to the output directory:
#   audit_log.jsonl    — one row per export line (migrated, skipped, failed, quarantined)
#   audit_manifest.json — snapshot of all migration criteria at run start
# These survive CLEANUP_AFTER and allow post-run comparison against both projects.
AUDIT_MODE = False

###############################################################################
# Derived file paths — no edits needed below this line
###############################################################################

STATE_PATH            = os.path.join(dirname, "export_state.json")
PAGE_LOG_PATH         = os.path.join(dirname, "page_log.txt")
INVALID_USERS_PATH    = os.path.join(dirname, "invalid_users.jsonl")
ERROR_LOG_PATH        = os.path.join(dirname, "bulk_errors.log")
SUMMARY_PATH          = os.path.join(dirname, "run_summary.json")
QUARANTINE_PATH       = os.path.join(dirname, "timed_out_records.jsonl")
BULK_USER_API_LOG_PATH = os.path.join(dirname, "bulk_user_api.jsonl")
BULK_SUBS_API_LOG_PATH  = os.path.join(dirname, "bulk_subs_api.jsonl")
LAST_EXPORT_JOB_ID_PATH = os.path.join(dirname, "last_export_job_id.txt")
AUDIT_LOG_PATH          = os.path.join(dirname, "audit_log.jsonl")
AUDIT_MANIFEST_PATH     = os.path.join(dirname, "audit_manifest.json")

os.makedirs(dirname, exist_ok=True)
if not os.path.exists(PAGE_LOG_PATH):
    open(PAGE_LOG_PATH, "a").close()


def retry_bulk_posts(func):
    """
    Retry bulk Iterable POST helpers with exponential backoff (stdlib only; replaces retrying package).
    Honors retry_config: stop_max_attempt_number, wait_exponential_multiplier (ms), wait_exponential_max (ms).
    """

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        max_attempts = retry_config["stop_max_attempt_number"]
        wm = retry_config["wait_exponential_multiplier"]
        wmax = retry_config["wait_exponential_max"]
        for attempt in range(1, max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                if attempt >= max_attempts:
                    raise
                cap_ms = min(wmax, wm * (2 ** (attempt - 1)))
                wait_ms = random.uniform(0, cap_ms)
                print(
                    f"[retry] {func.__name__} attempt {attempt}/{max_attempts} failed "
                    f"({exc}); retrying in {wait_ms / 1000:.1f}s…"
                )
                time.sleep(wait_ms / 1000.0)

    return wrapped


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


def _resolve_bulk_update_root(email: Optional[str], user_id: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Decide Iterable /users/bulkUpdate root identifier (userId vs email).
    Returns (root_field, root_value, error_reason). error_reason set => skip row.
    uuid: preferred identifier required — no fallback.
    hybrid: optional HYBRID_IDENTIFIER_FALLBACK when preferred is missing.
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


def _individual_subscription_path(
    subscription_group: str, subscription_group_id: int, identifier: str, prefer_email: bool
) -> str:
    """
    Iterable paths for PATCH/DELETE subscriptions:
    .../user/{email} when email is the preferred identifier, else .../byUserId/{userId}
    subscription_group is e.g. messageType or messageChannel.
    """
    ident = str(identifier).strip()
    if prefer_email:
        seg = quote(ident, safe="@._+-:")
        return f"/api/subscriptions/{subscription_group}/{subscription_group_id}/user/{seg}"
    seg = quote(ident, safe="")
    return f"/api/subscriptions/{subscription_group}/{subscription_group_id}/byUserId/{seg}"

#################
# API endpoints #
#################

export_start_endpoint = "/api/export/start"
export_files_endpoint_tmpl = "/api/export/{job_id}/files"
bulk_update_endpoint = "/api/users/bulkUpdate"
bulk_update_subs_endpoint = "/api/users/bulkUpdateSubscriptions"

# Counters
downloaded_files_total = 0
processed_files_total = 0

# Tracks the export file currently being processed — included in error log entries
# so failures can be correlated back to a specific export file.
_current_file: str = ""
_previous_file: str = ""
start_time = 0
end_time = 0
total_lines_parsed = 0
# Physical lines read from export file(s); should match `wc -l` (every iteration of for line in f).
total_raw_file_lines = 0
# Lines that were empty/whitespace-only after strip (excluded from total_lines_parsed).
blank_lines_skipped = 0
user_updated_counter = 0
subs_updated_counter = 0
skipped_users_counter = 0
remap_dropped_ids = 0
# Sum of Iterable failCount across POST /api/users/bulkUpdate batches (partial failures)
bulk_user_api_reported_failures = 0
# Rows where build_user_payload returned None, by reason (see run summary)
user_payload_skip_counts: Dict[str, int] = {}
# Rows successfully turned into a bulkUpdate user object (for reconciling vs total_lines_parsed)
user_payload_rows_built = 0
# Export lines where the same destination user key was seen again (later line wins; avoids double API writes)
duplicate_user_lines_collapsed = 0

#########################
# HTTP session (robust) #
#########################

session = requests.Session()
retry_strategy = Retry(
    total=3,
    connect=3,
    read=3,
    status=3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods={"GET"},
    backoff_factor=1.5,
    raise_on_status=False,
)
adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

DEFAULT_HEADERS = {"Connection": "keep-alive", "Accept-Encoding": "gzip, deflate"}

##############################################
# Rate limiters (see RATE_LIMIT_RPS_* above)  #
##############################################

class TokenBucket:
    def __init__(self, rps: int):
        self.rps = rps
        self.tokens = rps
        self.last = time.time()
        self.lock = threading.Lock()
    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last
                if elapsed >= 1.0:
                    self.tokens = self.rps
                    self.last = now
                if self.tokens > 0:
                    self.tokens -= 1
                    return
                sleep_for = max(0.0, 1.0 - elapsed)
            time.sleep(sleep_for if sleep_for > 0 else 0.01)

bucket_users = TokenBucket(RATE_LIMIT_RPS_BULK_USERS)
bucket_subs  = TokenBucket(RATE_LIMIT_RPS_BULK_SUBS)
_inflight_sem = threading.Semaphore(MAX_INFLIGHT_BULK_POSTS)

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
        from datetime import datetime, timezone as _tz
        ts = datetime.now(_tz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
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

def log_invalid_user(reason, source_user, built_payload, status_code=None, response_text=None):
    try:
        payload_size = len(json.dumps(built_payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    except Exception:
        payload_size = 0
    record = {
        "reason": reason,
        "status": status_code,
        "response": (response_text or "")[:4000],
        "payload_size_bytes": payload_size,
        "source_file": _current_file,
        "source_user": source_user,
        "built_payload": built_payload
    }
    write_jsonl(INVALID_USERS_PATH, record)

def quarantine_single_user(reason, source_user, built_payload, status_code=None, response_text=None):
    rec = {
        "reason": reason,
        "status": status_code,
        "response": (response_text or "")[:4000] if response_text else "",
        "source_user": source_user,
        "built_payload": built_payload
    }
    write_jsonl(QUARANTINE_PATH, rec)

##############################
# Export Start / List Pages  #
##############################

def _parse_export_datetime(raw: str) -> datetime:
    """Parse a config datetime string into an aware datetime (naive → UTC)."""
    s = raw.strip()
    if not s:
        raise ValueError("empty datetime string")
    if len(s) >= 1 and s[-1] in "Zz":
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
    """
    Convert a config value to Iterable export format: yyyy-MM-dd HH:mm:ss ±HH:MM
    (timezone offset ZZ, e.g. +00:00, -07:00).
    """
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
        "omitFields": omit_fields,
    }
    # Never include startDateTime / endDateTime when null/empty (do not send the key or JSON null).
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
    # Never include onlyFields when null/empty (do not send the key or JSON null).
    if only_fields is not None:
        _only = str(only_fields).strip()
        if _only:
            body["onlyFields"] = _only

    r = session.post(
        f"{origin_base_url}{export_start_endpoint}",
        headers={"Content-Type": "application/json", "Api-Key": origin_api_key, **DEFAULT_HEADERS},
        json=body, timeout=LIST_TIMEOUT
    )
    if not r.ok:
        raise requests.HTTPError(f"HTTP error! Status: {r.status_code} - {r.text}")
    data = r.json()
    jid = int(data["jobId"])
    print("Export job started with jobId:", jid, flush=True)
    try:
        with open(LAST_EXPORT_JOB_ID_PATH, "w", encoding="utf-8") as jf:
            jf.write(str(jid) + "\n")
    except OSError:
        pass
    return jid

def list_files_page(job_id: int, start_after: Optional[str]):
    url = f"{origin_base_url}{export_files_endpoint_tmpl.format(job_id=job_id)}"
    params = {"startAfter": start_after} if start_after else {}
    r = session.get(url, headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS}, params=params, timeout=LIST_TIMEOUT)
    if not r.ok:
        raise requests.HTTPError(f"HTTP error! Status: {r.status_code} - {r.text}")
    data             = r.json()
    files            = data.get("files", [])
    job_state        = str(data.get("jobState", "")).lower()
    export_truncated = bool(data.get("exportTruncated", False))
    next_cursor      = files[-1]["file"] if files else None
    return job_state, files, next_cursor, export_truncated

#####################################
# Download → process (just this page) → delete
#####################################

def download_page_files(files: List[Dict[str, str]]) -> List[str]:
    """Download multiple files in parallel for improved performance"""
    global downloaded_files_total
    
    def download_single_file(entry):
        name = entry.get("file")
        url = entry.get("url")
        if not name or not url:
            print(f"Skipping malformed entry: {entry}")
            return None
            
        dest = os.path.join(dirname, name)
        for attempt in range(1, 4):
            try:
                with session.get(url, stream=True, timeout=DOWNLOAD_TIMEOUT, headers=DEFAULT_HEADERS) as dl:
                    dl.raise_for_status()
                    os.makedirs(os.path.dirname(dest) or ".", exist_ok=True)
                    with open(dest, "wb") as out:
                        for chunk in dl.iter_content(chunk_size=256 * 1024):  # Smaller chunks for better network efficiency
                            if chunk:
                                out.write(chunk)
                print(f"Downloaded: {name}")
                return dest
            except requests.RequestException as e:
                if attempt == 3:
                    print(f"Failed to download {name} after retries: {e}")
                    return None
                # Shorter retry delays 
                time.sleep(min(attempt * 0.5, 2))  # Max 2 second delay instead of 3
        return None
    
    # Download files in parallel
    local_paths = []
    with ThreadPoolExecutor(max_workers=min(5, len(files))) as executor:
        results = list(executor.map(download_single_file, files))
        
    # Collect successful downloads and update counter
    for result in results:
        if result:
            local_paths.append(result)
            downloaded_files_total += 1
            
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
# Mapping & payload builders #
##############################

def _ensure_int_list(vals) -> List[int]:
    out = []
    if isinstance(vals, list):
        for v in vals:
            try: out.append(int(v))
            except Exception: pass
    return out

def _remap_ids(source_ids: List[int], id_map: Dict[int, int], field_name: str, user_obj: dict) -> List[int]:
    """
    Map source IDs to destination IDs using the provided mapping.
    Returns only destination IDs that have valid mappings.
    Logs any unmapped IDs for debugging.
    """
    global remap_dropped_ids
    mapped_ids = []
    
    for src_id in source_ids:
        if src_id in id_map:
            mapped_ids.append(id_map[src_id])
        else:
            # Log unmapped ID for debugging
            remap_dropped_ids += 1
            user_id = user_obj.get('userId', 'unknown')
            print(f"[mapping] Unmapped {field_name} ID {src_id} for user {user_id}")
    
    return mapped_ids


EXCLUDE_KEYS_USER = {
    "devices",  # full object
    "subscribedMessageTypeIds",
    "unsubscribedMessageTypeIds",
    "unsubscribedChannelIds",
}


def _coerce_export_field_value(val: object, typ: str) -> object:
    t = str(typ).strip().lower()
    if t in ("bool", "boolean"):
        if val is None:
            return None
        if isinstance(val, bool):
            return val
        s = str(val).strip().lower()
        return s in ("1", "true", "yes", "on")
    if val is None and t not in ("str", "string"):
        return None
    if t in ("int", "integer"):
        if isinstance(val, bool):
            return int(val)
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            return int(val)
        s = str(val).strip()
        return int(s) if s else 0
    if t in ("float", "double"):
        return float(val)
    if t in ("str", "string"):
        return "" if val is None else str(val)
    return val


def _export_adjustment_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    adj = USER_EXPORT_FIELD_ADJUSTMENTS
    if not isinstance(adj, dict):
        return {}, {}
    rename = adj.get("rename")
    recast = adj.get("recast")
    if not isinstance(rename, dict):
        rename = {}
    if not isinstance(recast, dict):
        recast = {}
    out_r: Dict[str, str] = {}
    out_c: Dict[str, str] = {}
    for k, v in rename.items():
        out_r[str(k)] = str(v)
    for k, v in recast.items():
        out_c[str(k)] = str(v)
    return out_r, out_c


def _apply_export_field_renames(merged_df: Dict[str, object]) -> None:
    rename, _ = _export_adjustment_maps()
    if not rename:
        return
    for old_k, new_k in rename.items():
        if old_k not in merged_df:
            continue
        val = merged_df.pop(old_k)
        merged_df[new_k] = val


def _nonempty_str_or_none(val: object) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _get_field_value(row: dict, field_path: str) -> object:
    """Retrieve a value from a row using a dot-notation path (e.g. 'itblInternal.documentCreatedAt').
    Returns None if any segment is missing or not a dict."""
    parts = field_path.split(".")
    val = row
    for part in parts:
        if not isinstance(val, dict):
            return None
        val = val.get(part)
    return val


def _parse_date_str(s: object) -> Optional[datetime]:
    """Parse a date/datetime string into a datetime object. Returns None on failure."""
    if s is None:
        return None
    raw = str(s).strip()
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None


def _first_non_empty_user_id(*sources: object) -> Optional[str]:
    """Iterable exports may put userId only under dataFields; check top-level dicts in order."""
    for src in sources:
        if not isinstance(src, dict):
            continue
        for key in ("userId", "itblUserId", "itbl_user_id", "ITBL_USER_ID"):
            s = _nonempty_str_or_none(src.get(key))
            if s is not None:
                return s
    return None


def _destination_batch_key(payload: dict) -> str:
    """Stable key for deduping bulk user + subscription requests to the same destination profile."""
    v = payload.get("userId")
    if v is not None and str(v).strip():
        return str(v).strip()
    e = payload.get("email")
    return str(e).strip().lower() if e is not None and str(e).strip() else ""


def _apply_export_field_recasts(merged_df: Dict[str, object]) -> None:
    _, recast = _export_adjustment_maps()
    if not recast:
        return
    for key, typ in recast.items():
        if key not in merged_df:
            continue
        try:
            merged_df[key] = _coerce_export_field_value(merged_df[key], typ)
        except (TypeError, ValueError) as e:
            log_error(f"[export recast] field {key!r} as {typ!r}: {e}; leaving original value")


def build_user_payload(u: dict):
    """
    Shape bulk user updates for the *destination* project: preferred identifier at the root,
    the other under dataFields (per Iterable guidance for hybrid / consistent profiling).
    Export field values are passed through unless rename/recast is configured; then
    additional_user_data_fields from config are merged last.
    """
    merged_df: Dict[str, object] = {}
    src_df = u.get("dataFields")
    if isinstance(src_df, dict):
        merged_df.update(src_df)

    reserved = {"email", "userId", "itblUserId", "itbl_user_id", "ITBL_USER_ID", "dataFields"}
    for k, v in u.items():
        if k in reserved or k in EXCLUDE_KEYS_USER or k.startswith("devices."):
            continue
        merged_df[k] = v

    # Identifiers often live under dataFields only; resolve after merge (top-level wins).
    email = _nonempty_str_or_none(u.get("email")) or _nonempty_str_or_none(merged_df.get("email"))
    user_id = _first_non_empty_user_id(u, merged_df)
    if not email and not user_id:
        return None, "missing_both_email_and_userId"

    if email and SKIP_EMAIL_SUBSTRINGS:
        email_lower = email.lower()
        for sub in SKIP_EMAIL_SUBSTRINGS:
            if sub.lower() in email_lower:
                return None, f"skipped_email_substring:{sub}"

    if REQUIRED_FIELDS_FOR_MIGRATION:
        for field_path in REQUIRED_FIELDS_FOR_MIGRATION:
            val = _get_field_value(u, field_path)
            if val is None or (isinstance(val, str) and not val.strip()):
                return None, f"missing_required_field:{field_path}"

    if FIELD_DATE_BEFORE:
        for field_path, cutoff_str in FIELD_DATE_BEFORE.items():
            cutoff = _parse_date_str(cutoff_str)
            if cutoff is None:
                log_error(f"[filter] FIELD_DATE_BEFORE: could not parse cutoff {cutoff_str!r} for {field_path!r}; skipping filter")
                continue
            if cutoff.tzinfo is None:
                cutoff = cutoff.replace(tzinfo=timezone.utc)
            field_val = _get_field_value(u, field_path)
            if field_val is None:
                return None, f"missing_date_field:{field_path}"
            field_dt = _parse_date_str(field_val)
            if field_dt is None:
                return None, f"unparseable_date_field:{field_path}"
            if field_dt.tzinfo is None:
                field_dt = field_dt.replace(tzinfo=timezone.utc)
            if field_dt >= cutoff:
                return None, f"date_not_before:{field_path}"

    for k in ("email", "userId", "itblUserId", "itbl_user_id", "ITBL_USER_ID"):
        merged_df.pop(k, None)

    for k in DROP_FIELDS:
        merged_df.pop(k, None)

    if INCLUDE_ONLY_FIELDS:
        allowed = set(INCLUDE_ONLY_FIELDS)
        for k in list(merged_df.keys()):
            if k not in allowed:
                del merged_df[k]

    _apply_export_field_renames(merged_df)
    _apply_export_field_recasts(merged_df)

    root_field, root_val, root_err = _resolve_bulk_update_root(email, user_id)
    if root_err:
        return None, root_err

    payload: Dict[str, object] = {"dataFields": merged_df}
    if root_field == "userId":
        payload["userId"] = str(root_val)
        if email and INCLUDE_SECONDARY_ID_IN_DATAFIELDS:
            merged_df["email"] = email
    else:
        payload["email"] = root_val
        if user_id and INCLUDE_SECONDARY_ID_IN_DATAFIELDS:
            merged_df["userId"] = str(user_id)

    if additional_user_data_fields:
        merged_df.update(additional_user_data_fields)

    return payload, None

def build_subscription_payload(u: dict):
    """Build a bulkUpdateSubscriptions payload.

    The root identifier matches the destination project type:
      - email destination  → "email" field
      - uuid/userId destination → "userId" field (must be present in source)

    If the required identifier is missing the payload is dropped.
    """
    bag: Dict[str, object] = {}
    src_df = u.get("dataFields")
    if isinstance(src_df, dict):
        bag.update(src_df)
    email   = _nonempty_str_or_none(u.get("email")) or _nonempty_str_or_none(bag.get("email"))
    user_id = _first_non_empty_user_id(u, bag)

    src_sub_msg  = _ensure_int_list(u.get("subscribedMessageTypeIds"))
    src_unsub_msg = _ensure_int_list(u.get("unsubscribedMessageTypeIds"))
    src_unsub_ch  = _ensure_int_list(u.get("unsubscribedChannelIds"))

    dst_sub_msg   = _remap_ids(src_sub_msg,   MESSAGE_TYPE_ID_MAP, "subscribedMessageTypeIds", u)
    dst_unsub_msg = _remap_ids(src_unsub_msg, MESSAGE_TYPE_ID_MAP, "unsubscribedMessageTypeIds", u)
    dst_unsub_ch  = _remap_ids(src_unsub_ch,  CHANNEL_ID_MAP,      "unsubscribedChannelIds", u)

    if not dst_sub_msg and not dst_unsub_msg and not dst_unsub_ch:
        return None, "no_subscription_changes_after_remap"

    dst_type = _destination_project_type_normalized()
    payload: Dict[str, object] = {}

    if dst_type in ("uuid", "userid"):
        if not user_id:
            return None, "missing_userId_for_subscription_update"
        payload["userId"] = str(user_id)
    else:
        # email destination
        if not email:
            return None, "missing_email_for_subscription_update"
        payload["email"] = email

    if dst_sub_msg:
        payload["subscribedMessageTypeIds"] = dst_sub_msg
    if dst_unsub_msg:
        payload["unsubscribedMessageTypeIds"] = dst_unsub_msg
    if dst_unsub_ch:
        payload["unsubscribedChannelIds"] = dst_unsub_ch
    return payload, None

#####################################
# POST helpers (rate-limited + caps)#
#####################################

def _post_bulk_update(users_payload: List[dict], api_key: str):
    bucket_users.acquire()
    _inflight_sem.acquire()
    try:
        body = {"users": users_payload, "createNewFields": create_new_fields}
        return session.post(
            f"{destination_base_url}{bulk_update_endpoint}",
            json=body,
            headers={"Content-Type": "application/json", "Api-Key": api_key, **DEFAULT_HEADERS},
            timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
        )
    finally:
        _inflight_sem.release()

def _post_bulk_update_subscriptions(requests_list: List[dict], api_key: str):
    bucket_subs.acquire()
    _inflight_sem.acquire()
    try:
        body = {"updateSubscriptionsRequests": requests_list}
        return session.post(
            f"{destination_base_url}{bulk_update_subs_endpoint}",
            json=body,
            headers={"Content-Type": "application/json", "Api-Key": api_key, **DEFAULT_HEADERS},
            timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
        )
    finally:
        _inflight_sem.release()

def _approx_body_size(obj: dict) -> int:
    try:
        return len(json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))
    except Exception:
        return 0

def _approx_body_size_users(users_payload: List[dict]) -> int:
    return _approx_body_size({"users": users_payload, "createNewFields": create_new_fields})

def _approx_body_size_subs(requests_list: List[dict]) -> int:
    return _approx_body_size({"updateSubscriptionsRequests": requests_list})

def is_timeout_exc(exc: Exception) -> bool:
    return isinstance(exc, (ReadTimeout, ReadTimeoutError, ProtocolError)) or (
        "timeout" in str(exc).lower() and "read" in str(exc).lower()
    )


def _redact_user_payload_for_debug(u: dict) -> dict:
    """Shallow redaction for stdout debug (one sample per batch)."""
    try:
        out = json.loads(json.dumps(u))
    except Exception:
        return {"_redact_error": "could not copy payload"}
    if "email" in out and out["email"]:
        e = str(out["email"])
        if "@" in e:
            local, _, domain = e.partition("@")
            out["email"] = (local[:2] + "***@" + domain) if local else "***@" + domain
        else:
            out["email"] = "***"
    if "userId" in out and out["userId"] is not None:
        s = str(out["userId"])
        out["userId"] = ("***" + s[-4:]) if len(s) > 4 else "***"
    df = out.get("dataFields")
    if isinstance(df, dict):
        keys = sorted(df.keys())
        out["dataFields"] = {
            "_field_names_preview": keys[:25],
            "_field_count": len(keys),
        }
    return out


def _truncate_failed_updates_for_console(fu: Optional[dict], max_per: int) -> Optional[dict]:
    if not fu or not isinstance(fu, dict):
        return None
    out: Dict[str, object] = {}
    for k, v in fu.items():
        if isinstance(v, list):
            trimmed = v[:max_per]
            if len(v) > max_per:
                trimmed = trimmed + [f"... (+{len(v) - max_per} more)"]
            out[k] = trimmed
        else:
            out[k] = v
    return out


def _append_bulk_user_api_jsonl(record: Dict[str, object]) -> None:
    if not log_bulk_user_api_jsonl:
        return
    try:
        record = dict(record)
        record["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(BULK_USER_API_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log_error(f"[bulk_user_api jsonl] {e}")


def _append_bulk_subs_api_jsonl(record: Dict[str, object]) -> None:
    """Write one JSONL row for a bulk subscription batch that had failures or invalid identifiers."""
    if not log_bulk_subs_api_jsonl:
        return
    try:
        record = dict(record)
        record["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(BULK_SUBS_API_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
    except Exception as e:
        log_error(f"[bulk_subs_api jsonl] {e}")


####################
# Audit log helpers #
####################

def _append_audit_log(record: dict) -> None:
    """Append one row to audit_log.jsonl. Last row per userId/email wins on read."""
    if not AUDIT_MODE:
        return
    try:
        r = dict(record)
        r["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(AUDIT_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except Exception as e:
        log_error(f"[audit_log] {e}")


def _audit_filter_field_values(raw_row: dict) -> dict:
    """
    Capture field values from the raw export row for the audit log.
    Includes:
      - REQUIRED_FIELDS_FOR_MIGRATION  (filter criteria)
      - FIELD_DATE_BEFORE keys         (filter criteria)
      - USER_EXPORT_FIELD_ADJUSTMENTS rename source names  (so auditor can see original value)
      - USER_EXPORT_FIELD_ADJUSTMENTS recast field names   (so auditor can compare pre/post type)
    """
    values = {}
    for field_path in REQUIRED_FIELDS_FOR_MIGRATION:
        values[field_path] = _get_field_value(raw_row, field_path)
    for field_path in FIELD_DATE_BEFORE:
        if field_path not in values:
            values[field_path] = _get_field_value(raw_row, field_path)
    # Fields being renamed — capture under "src:<source_name>" so the audit log
    # shows both the original name and the value before it was renamed.
    for src_name in USER_EXPORT_FIELD_ADJUSTMENTS.get("rename", {}):
        key = f"src:{src_name}"
        if key not in values:
            values[key] = _get_field_value(raw_row, src_name)
    # Fields being recast — capture the raw (pre-recast) value so the auditor
    # can confirm the destination received the correct type.
    for field_name in USER_EXPORT_FIELD_ADJUSTMENTS.get("recast", {}):
        if field_name not in values and f"src:{field_name}" not in values:
            values[field_name] = _get_field_value(raw_row, field_name)
    return values


def write_audit_manifest() -> None:
    """Write a one-time snapshot of all migration criteria to audit_manifest.json."""
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
        "filters": {
            "SKIP_EMAIL_SUBSTRINGS": SKIP_EMAIL_SUBSTRINGS,
            "REQUIRED_FIELDS_FOR_MIGRATION": REQUIRED_FIELDS_FOR_MIGRATION,
            "FIELD_DATE_BEFORE": FIELD_DATE_BEFORE,
            "DROP_FIELDS": DROP_FIELDS,
            "INCLUDE_ONLY_FIELDS": INCLUDE_ONLY_FIELDS,
        },
        "injected_fields": additional_user_data_fields,
        "field_adjustments": USER_EXPORT_FIELD_ADJUSTMENTS,
        "create_new_fields": create_new_fields,
        # ID maps are included so the audit script can verify subscriptions
        # without needing to manually duplicate them.
        "channel_id_map": {str(k): v for k, v in CHANNEL_ID_MAP.items()},
        "message_type_id_map": {str(k): v for k, v in MESSAGE_TYPE_ID_MAP.items()},
    }
    try:
        with open(AUDIT_MANIFEST_PATH, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)
        print(f"[audit] Manifest written to {AUDIT_MANIFEST_PATH}")
    except Exception as e:
        log_error(f"[audit_manifest] {e}")


################################
# Individual subscription retry #
################################

def retry_failed_subscriptions(pairs: List[Tuple[dict, dict]], failed_user_ids: List[str], failed_emails: List[str], api_key: str):
    """Retry failed subscription updates using individual Iterable APIs with parallel processing"""
    global subs_updated_counter
    
    total_failed = len(failed_user_ids + failed_emails)
    print(f"[individual] Retrying {total_failed} failed users with individual subscription APIs...")
    
    # Collect failed user pairs for parallel processing
    failed_pairs = []
    for src_user, sub_payload in pairs:
        user_id = sub_payload.get('userId')
        email = sub_payload.get('email')
        if (user_id in failed_user_ids) or (email in failed_emails):
            failed_pairs.append((src_user, sub_payload))
    
    fixed_count = 0
    processed_count = 0
    lock = threading.Lock()
    
    def process_failed_user(src_user_payload_tuple):
        nonlocal fixed_count, processed_count
        src_user, sub_payload = src_user_payload_tuple

        prefer_email = _destination_preferred_key() == "email"
        if prefer_email:
            identifier = sub_payload.get("email")
        else:
            identifier = sub_payload.get("userId")
        if not identifier:
            log_invalid_user("individual_missing_identifier", src_user, sub_payload, None, "No email/userId for destination preferred identifier")
            with lock:
                processed_count += 1
            return

        call_failures = []

        try:
            for msg_type_id in sub_payload.get('subscribedMessageTypeIds', []):
                ok, status, body = subscribe_user_to_message_type(identifier, msg_type_id, api_key, prefer_email=prefer_email, debug=False)
                if not ok:
                    call_failures.append({"op": "subscribe_messageType", "id": msg_type_id, "status": status, "response": body})

            for channel_id in sub_payload.get('unsubscribedChannelIds', []):
                ok, status, body = unsubscribe_user_from_channel(identifier, channel_id, api_key, prefer_email=prefer_email, debug=False)
                if not ok:
                    call_failures.append({"op": "unsubscribe_channel", "id": channel_id, "status": status, "response": body})

            for msg_type_id in sub_payload.get('unsubscribedMessageTypeIds', []):
                ok, status, body = unsubscribe_user_from_message_type(identifier, msg_type_id, api_key, prefer_email=prefer_email, debug=False)
                if not ok:
                    call_failures.append({"op": "unsubscribe_messageType", "id": msg_type_id, "status": status, "response": body})

            with lock:
                if not call_failures:
                    fixed_count += 1
                    global subs_updated_counter
                    subs_updated_counter += 1
                else:
                    log_invalid_user(
                        "individual_api_failure",
                        src_user,
                        sub_payload,
                        None,
                        json.dumps(call_failures)
                    )

                processed_count += 1
                if processed_count % 50 == 0 and total_failed > 50:
                    print(f"[individual] Progress: {processed_count}/{total_failed} processed, {fixed_count} fixed")

        except Exception as e:
            with lock:
                log_invalid_user("individual_api_exception", src_user, sub_payload, None, str(e))
                processed_count += 1
    
    # Process failed users in parallel with controlled concurrency
    max_workers = min(20, total_failed)  # Don't overwhelm the API
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        list(executor.map(process_failed_user, failed_pairs))
    
    print(f"[individual] Successfully fixed {fixed_count}/{total_failed} failed subscriptions")

def subscribe_user_to_message_type(identifier: str, message_type_id: int, api_key: str, prefer_email: bool = False, debug: bool = False) -> tuple:
    """Subscribe user to message type using individual API - PATCH method. Returns (success, status_code, response_text)."""
    endpoint = _individual_subscription_path("messageType", message_type_id, identifier, prefer_email)
    full_url = f"{destination_base_url}{endpoint}"
    
    try:
        if debug:
            print(f"[DEBUG] PATCH {full_url}")
        resp = session.patch(
            full_url,
            headers={"Api-Key": api_key, **DEFAULT_HEADERS},
            timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT)
        )
        if debug:
            print(f"[DEBUG] Response: {resp.status_code} - {resp.text[:200]}")
        
        if resp.status_code in [200, 202]:
            return True, resp.status_code, None
        else:
            if debug:
                print(f"[individual] Subscribe failed for {identifier} to message type {message_type_id}: {resp.status_code}")
            return False, resp.status_code, resp.text[:500]
    except Exception as e:
        if debug:
            print(f"[individual] Subscribe exception for {identifier} to message type {message_type_id}: {e}")
        return False, None, str(e)

def unsubscribe_user_from_channel(identifier: str, channel_id: int, api_key: str, prefer_email: bool = False, debug: bool = False) -> tuple:
    """Unsubscribe user from channel using individual API - DELETE method. Returns (success, status_code, response_text)."""
    endpoint = _individual_subscription_path("messageChannel", channel_id, identifier, prefer_email)
    full_url = f"{destination_base_url}{endpoint}"
    
    try:
        if debug:
            print(f"[DEBUG] DELETE {full_url}")
        resp = session.delete(
            full_url,
            headers={"Api-Key": api_key, **DEFAULT_HEADERS},
            timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT)
        )
        if debug:
            print(f"[DEBUG] Response: {resp.status_code} - {resp.text[:200]}")
        
        if resp.status_code in [200, 202]:
            return True, resp.status_code, None
        else:
            if debug:
                print(f"[individual] Channel unsubscribe failed for {identifier} from channel {channel_id}: {resp.status_code}")
            return False, resp.status_code, resp.text[:500]
    except Exception as e:
        if debug:
            print(f"[individual] Channel unsubscribe exception for {identifier} from channel {channel_id}: {e}")
        return False, None, str(e)

def unsubscribe_user_from_message_type(identifier: str, message_type_id: int, api_key: str, prefer_email: bool = False, debug: bool = False) -> tuple:
    """Unsubscribe user from message type using individual API - DELETE method. Returns (success, status_code, response_text)."""
    endpoint = _individual_subscription_path("messageType", message_type_id, identifier, prefer_email)
    full_url = f"{destination_base_url}{endpoint}"
    
    try:
        if debug:
            print(f"[DEBUG] DELETE {full_url}")
        resp = session.delete(
            full_url,
            headers={"Api-Key": api_key, **DEFAULT_HEADERS},
            timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT)
        )
        if debug:
            print(f"[DEBUG] Response: {resp.status_code} - {resp.text[:200]}")
        
        if resp.status_code in [200, 202]:
            return True, resp.status_code, None
        else:
            if debug:
                print(f"[individual] Message type unsubscribe failed for {identifier} from message type {message_type_id}: {resp.status_code}")
            return False, resp.status_code, resp.text[:500]
    except Exception as e:
        if debug:
            print(f"[individual] Message type unsubscribe exception for {identifier} from message type {message_type_id}: {e}")
        return False, None, str(e)

################################
# Batch senders (split + retry)#
################################

@retry_bulk_posts
def send_bulk_users(pairs: List[Tuple[dict, dict]], api_key: str):
    global user_updated_counter, skipped_users_counter, bulk_user_api_reported_failures
    users_payload = [p[1] for p in pairs]
    if debug_log_sample_bulk_user_payload and users_payload:
        sample = _redact_user_payload_for_debug(users_payload[0])
        print(f"[users][debug] sample redacted: {json.dumps(sample, ensure_ascii=False)[:6000]}")
    size_bytes = _approx_body_size_users(users_payload)
    if size_bytes > approx_payload_byte_limit and len(pairs) > 1:
        mid = len(pairs) // 2
        print(f"[users] Pre-splitting large payload (~{size_bytes} bytes) into {mid} + {len(pairs)-mid}")
        send_bulk_users(pairs[:mid], api_key)
        send_bulk_users(pairs[mid:], api_key)
        return
    try:
        resp = _post_bulk_update(users_payload, api_key)
        status = resp.status_code
        n = len(users_payload)
        print(f"[users] Response: {status} — batch size {n}")
        if status == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after and retry_after.isdigit() else random.uniform(3, 8)
            print(f"[users] 429 Too Many Requests; sleeping {wait_s:.1f}s then retrying…")
            time.sleep(wait_s); raise RequestException("429 throttled")
        if status == 413:
            if len(pairs) == 1:
                skipped_users_counter += 1
                src, built = pairs[0]
                body = getattr(resp, "text", None)
                log_invalid_user("413_single_user", src, {"users":[built],"createNewFields":create_new_fields}, 413, body)
                _append_bulk_user_api_jsonl({
                    "endpoint": "POST /api/users/bulkUpdate",
                    "http_status": 413,
                    "batch_size": n,
                    "response": {"error": "413 Request Entity Too Large", "body": (body or "")[:2000]},
                })
                _append_audit_log({"userId": src.get("userId"), "email": src.get("email"), "outcome": "api_failed", "skip_reason": "413_single_user", "filter_field_values": {}})
                return
            mid = len(pairs)//2
            print(f"[users] 413; splitting into {mid} + {len(pairs)-mid}")
            send_bulk_users(pairs[:mid], api_key); send_bulk_users(pairs[mid:], api_key); return
        if status == 400:
            if len(pairs) == 1:
                skipped_users_counter += 1
                src, built = pairs[0]
                body = getattr(resp, "text", None)
                log_invalid_user("400_single_user", src, {"users":[built],"createNewFields":create_new_fields}, 400, body)
                _append_bulk_user_api_jsonl({
                    "endpoint": "POST /api/users/bulkUpdate",
                    "http_status": 400,
                    "batch_size": n,
                    "response": {"error": "400 Bad Request", "body": (body or "")[:2000]},
                })
                _append_audit_log({"userId": src.get("userId"), "email": src.get("email"), "outcome": "api_failed", "skip_reason": "400_single_user", "filter_field_values": {}})
                return
            mid = len(pairs)//2
            print(f"[users] 400; bisecting into {mid} + {len(pairs)-mid}")
            send_bulk_users(pairs[:mid], api_key); send_bulk_users(pairs[mid:], api_key); return
        resp.raise_for_status()
        try:
            data = resp.json()
        except Exception as e:
            log_error(f"[users] JSON parse error after {status}: {e}\nBody: {getattr(resp, 'text', '')[:2000]}")
            raise
        success_count = data.get("successCount")
        fail_count = data.get("failCount")
        if success_count is None:
            success_count = n
        if fail_count is None:
            fail_count = 0
        try:
            sc = int(success_count)
            fc = int(fail_count)
        except (TypeError, ValueError):
            sc, fc = n, 0
            log_error(
                f"[users] Non-integer successCount/failCount; assuming sc={sc} fc={fc}. Raw: {str(data)[:1500]}"
            )
        user_updated_counter += sc
        bulk_user_api_reported_failures += fc
        print(
            f"[users] Iterable bulkUpdate: successCount={sc} failCount={fc} "
            f"(batch had {n} user object(s))"
        )
        if fc > 0:
            fu = data.get("failedUpdates")
            preview = _truncate_failed_updates_for_console(
                fu, bulk_user_failed_updates_preview_limit
            )
            if preview:
                print(f"[users] failedUpdates preview: {json.dumps(preview, ensure_ascii=False)}")
            inv_e = data.get("invalidEmails")
            inv_u = data.get("invalidUserIds")
            if inv_e:
                print(f"[users] invalidEmails (deprecated): {inv_e[:15]}{'...' if len(inv_e) > 15 else ''}")
            if inv_u:
                print(f"[users] invalidUserIds (deprecated): {inv_u[:15]}{'...' if len(inv_u) > 15 else ''}")
        cre = data.get("createdFields")
        filt = data.get("filteredOutFields")
        if cre:
            print(f"[users] createdFields ({len(cre)}): {cre[:25]}{'...' if len(cre) > 25 else ''}")
        if filt:
            print(f"[users] filteredOutFields ({len(filt)}): {filt[:25]}{'...' if len(filt) > 25 else ''}")
        _append_bulk_user_api_jsonl(
            {
                "endpoint": "POST /api/users/bulkUpdate",
                "http_status": status,
                "batch_size": n,
                "response": data,
            }
        )
        return data
    except RequestException as error:
        if is_timeout_exc(error) and len(pairs) > 1:
            mid = len(pairs)//2
            print(f"[users] Timeout; splitting into {mid} + {len(pairs)-mid}")
            send_bulk_users(pairs[:mid], api_key); send_bulk_users(pairs[mid:], api_key); return
        if is_timeout_exc(error) and len(pairs) == 1:
            src, built = pairs[0]
            quarantine_single_user("timeout_single_user", src, {"users":[built],"createNewFields":create_new_fields}, None, str(error))
            _append_audit_log({"userId": src.get("userId"), "email": src.get("email"), "outcome": "quarantined", "skip_reason": "timeout_single_user", "filter_field_values": {}})
            print("[users] Quarantined single-user timeout; continuing…"); return
        try:
            body = resp.text[:1000] if "resp" in locals() and hasattr(resp,"text") else ""
            log_error(f"[users] Exception: {error}\nResponse body: {body}")
        except Exception: pass
        raise

@retry_bulk_posts
def send_bulk_subs(pairs: List[Tuple[dict, dict]], api_key: str):
    global subs_updated_counter, skipped_users_counter
    requests_list = [p[1] for p in pairs]
    size_bytes = _approx_body_size_subs(requests_list)
    if size_bytes > approx_payload_byte_limit and len(pairs) > 1:
        mid = len(pairs)//2
        print(f"[subs] Pre-splitting large payload (~{size_bytes} bytes) into {mid} + {len(pairs)-mid}")
        send_bulk_subs(pairs[:mid], api_key); send_bulk_subs(pairs[mid:], api_key); return
    try:
        resp = _post_bulk_update_subscriptions(requests_list, api_key)
        status = resp.status_code
        n = len(requests_list)
        print(f"[subs] Response: {status} — batch size {n}")
        if status == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after and retry_after.isdigit() else random.uniform(3, 8)
            print(f"[subs] 429 Too Many Requests; sleeping {wait_s:.1f}s then retrying…")
            time.sleep(wait_s); raise RequestException("429 throttled")
        if status == 413:
            if len(pairs) == 1:
                skipped_users_counter += 1
                src, built = pairs[0]
                log_invalid_user("413_single_request", src, {"updateSubscriptionsRequests":[built]}, 413, getattr(resp,"text",None)); return
            mid = len(pairs)//2
            print(f"[subs] 413; splitting into {mid} + {len(pairs)-mid}")
            send_bulk_subs(pairs[:mid], api_key); send_bulk_subs(pairs[mid:], api_key); return
        if status == 400:
            if len(pairs) == 1:
                skipped_users_counter += 1
                src, built = pairs[0]
                log_invalid_user("400_single_request", src, {"updateSubscriptionsRequests":[built]}, 400, getattr(resp,"text",None)); return
            mid = len(pairs)//2
            print(f"[subs] 400; bisecting into {mid} + {len(pairs)-mid}")
            send_bulk_subs(pairs[:mid], api_key); send_bulk_subs(pairs[mid:], api_key); return
        resp.raise_for_status()
        response_data = resp.json()
        sc = response_data.get("successCount", 0) or 0
        fc = response_data.get("failCount", 0) or 0
        print(
            f"[subs] Iterable bulkUpdateSubscriptions: successCount={sc} failCount={fc} "
            f"(batch had {n} subscription request(s))"
        )
        cre = response_data.get("createdFields")
        filt = response_data.get("filteredOutFields")
        if cre:
            print(f"[subs] createdFields ({len(cre)}): {cre[:25]}{'...' if len(cre) > 25 else ''}")
        if filt:
            print(f"[subs] filteredOutFields ({len(filt)}): {filt[:25]}{'...' if len(filt) > 25 else ''}")
        invalid_emails = response_data.get("invalidEmails") or []
        invalid_user_ids = response_data.get("invalidUserIds") or []
        failed_user_ids = response_data.get("validUserIdFailures") or []
        failed_emails = response_data.get("validEmailFailures") or []
        has_any_failure = fc > 0 or invalid_emails or invalid_user_ids
        if has_any_failure:
            _append_bulk_subs_api_jsonl({
                "endpoint": "POST /api/users/bulkUpdateSubscriptions",
                "http_status": status,
                "batch_size": n,
                "response": response_data,
            })
        if invalid_emails:
            print(f"[subs] invalidEmails ({len(invalid_emails)}): {invalid_emails[:5]}{'...' if len(invalid_emails) > 5 else ''}")
        if invalid_user_ids:
            print(f"[subs] invalidUserIds ({len(invalid_user_ids)}): {invalid_user_ids[:5]}{'...' if len(invalid_user_ids) > 5 else ''}")
        # Retry failures for every identifier the destination project can receive.
        # - Preferred identifier:  always retried.
        # - Fallback identifier:   only retried on hybrid projects when a fallback is configured,
        #   because some payloads in that run may have used the fallback when preferred was absent.
        # UUID / email projects only ever send one identifier type, so the other list stays empty.
        prefer_email = _destination_preferred_key() == "email"
        dst_type     = _destination_project_type_normalized()
        fallback     = _hybrid_fallback_normalized()
        include_userid_failures = not prefer_email or (dst_type == "hybrid" and fallback == "userId")
        include_email_failures  = prefer_email      or (dst_type == "hybrid" and fallback == "email")
        all_failed_user_ids = list({*invalid_user_ids, *failed_user_ids}) if include_userid_failures else []
        all_failed_emails   = list({*invalid_emails,   *failed_emails})   if include_email_failures  else []
        if all_failed_user_ids:
            print(f"[subs] Retrying {len(all_failed_user_ids)} failed userIds: {all_failed_user_ids[:5]}{'...' if len(all_failed_user_ids) > 5 else ''}")
        if all_failed_emails:
            print(f"[subs] Retrying {len(all_failed_emails)} failed emails: {all_failed_emails[:5]}{'...' if len(all_failed_emails) > 5 else ''}")
        if all_failed_user_ids or all_failed_emails:
            retry_failed_subscriptions(pairs, all_failed_user_ids, all_failed_emails, api_key)
        subs_updated_counter += sc
        return response_data
    except RequestException as error:
        if is_timeout_exc(error) and len(pairs) > 1:
            mid = len(pairs)//2
            print(f"[subs] Timeout; splitting into {mid} + {len(pairs)-mid}")
            send_bulk_subs(pairs[:mid], api_key); send_bulk_subs(pairs[mid:], api_key); return
        if is_timeout_exc(error) and len(pairs) == 1:
            src, built = pairs[0]
            quarantine_single_user("timeout_single_request", src, {"updateSubscriptionsRequests":[built]}, None, str(error))
            print("[subs] Quarantined single-user timeout; continuing…"); return
        try:
            body = resp.text[:1000] if "resp" in locals() and hasattr(resp,"text") else ""
            log_error(f"[subs] Exception: {error}\nResponse body: {body}")
        except Exception: pass
        raise

#########################
# Page file processing  #
#########################

def process_json_files_page(local_file_paths: List[str]):
    """
    Stream export lines into /users/bulkUpdate and /users/bulkUpdateSubscriptions.

    Deduplicate by destination identity (userId or email): duplicate lines for the same user
    (common in overlapping exports) collapse to a single pending row — **last line wins** — so we
    do not POST the same profile many times across batch boundaries (which can confuse counts and
    contribute to duplicate-looking profiles if identifiers differ slightly).

    Flush order: bulk users first, then subscription rows for those same keys in the same order.
    """
    global total_lines_parsed, total_raw_file_lines, blank_lines_skipped
    global user_payload_skip_counts, user_payload_rows_built
    global duplicate_user_lines_collapsed
    pending_users = OrderedDict()
    subs_by_key: Dict[str, Tuple[dict, dict]] = {}

    def flush_one_user_sub_batch() -> None:
        if not pending_users:
            return
        n = min(BATCH_SIZE, len(pending_users))
        batch_u: List[Tuple[dict, dict]] = []
        keys_order: List[str] = []
        for _ in range(n):
            k, pair = pending_users.popitem(last=False)
            keys_order.append(k)
            batch_u.append(pair)
        batch_s: List[Tuple[dict, dict]] = []
        for k in keys_order:
            if k in subs_by_key:
                batch_s.append(subs_by_key.pop(k))

        # Users first, then subscriptions. On UUID projects the user profile must be
        # indexed before bulkUpdateSubscriptions will accept it; sending both in
        # parallel causes ~99% invalidUserIds failures that fall back to slow
        # individual retries.
        send_bulk_users(batch_u, destination_api_key)
        if batch_s:
            send_bulk_subs(batch_s, destination_api_key)

    for file_path in sorted(local_file_paths):
        global _current_file, _previous_file
        _previous_file = _current_file
        _current_file = os.path.basename(file_path)
        with open(file_path, 'r', encoding="utf-8") as f:
            for line in f:
                total_raw_file_lines += 1
                stripped = line.strip()
                if not stripped:
                    blank_lines_skipped += 1
                    continue
                total_lines_parsed += 1
                line = stripped
                try:
                    obj = json.loads(line)
                    upayload, ureason = build_user_payload(obj)
                    if upayload is not None:
                        user_payload_rows_built += 1
                        uk = _destination_batch_key(upayload)
                        if not uk:
                            log_error("[batch] empty destination key; skipping user payload")
                        else:
                            if uk in pending_users:
                                duplicate_user_lines_collapsed += 1
                                del pending_users[uk]
                            pending_users[uk] = (obj, upayload)
                            _append_audit_log({
                                "userId": obj.get("userId"),
                                "email": obj.get("email"),
                                "outcome": "migrated",
                                "skip_reason": None,
                                "filter_field_values": _audit_filter_field_values(obj),
                            })
                    elif ureason:
                        user_payload_skip_counts[ureason] = user_payload_skip_counts.get(ureason, 0) + 1
                        _append_audit_log({
                            "userId": obj.get("userId"),
                            "email": obj.get("email"),
                            "outcome": "skipped",
                            "skip_reason": ureason,
                            "filter_field_values": _audit_filter_field_values(obj),
                        })
                    if upayload is not None:
                        spayload, _ = build_subscription_payload(obj)
                        if spayload is not None:
                            sk = _destination_batch_key(spayload)
                            if sk:
                                subs_by_key[sk] = (obj, spayload)

                    while len(pending_users) >= BATCH_SIZE:
                        flush_one_user_sub_batch()
                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON in {os.path.basename(file_path)}: {e}")

    while pending_users:
        flush_one_user_sub_batch()

    if subs_by_key:
        orphan = [p for _, p in subs_by_key.items()]
        subs_by_key.clear()
        for i in range(0, len(orphan), BATCH_SIZE):
            try:
                send_bulk_subs(orphan[i : i + BATCH_SIZE], destination_api_key)
            except Exception as e:
                print(f"[batch] bulkUpdateSubscriptions (subs-only remainder) failed: {e}")
                raise

    if user_payload_skip_counts:
        total_skipped = sum(user_payload_skip_counts.values())
        print(
            f"[page] {total_lines_parsed} lines parsed, "
            f"{user_payload_rows_built} built payloads, "
            f"{total_skipped} skipped: {dict(sorted(user_payload_skip_counts.items()))}",
            flush=True,
        )

###################
# Optional cleanup #
###################

def delete_export_files_only():
    removed = 0
    for filename in os.listdir(dirname):
        if filename.startswith("file-") and (filename.endswith(".json") or filename.endswith(".jsonl") or filename.endswith(".txt")):
            try:
                os.remove(os.path.join(dirname, filename)); removed += 1
                print(f"Deleted export file: {filename}")
            except OSError: pass
    print(f"[cleanup] Export files deleted: {removed}")

###################
# Page log helpers #
###################

def append_page_log(page_no: int, cursor_filename: str):
    line = f"[{page_no}]:{cursor_filename}\n"
    with open(PAGE_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(line)

def read_page_log() -> List[Tuple[int, str]]:
    entries = []
    try:
        with open(PAGE_LOG_PATH, "r", encoding="utf-8") as f:
            for ln in f:
                ln = ln.strip()
                if not ln or not ln.startswith("[") or "]:" not in ln: continue
                try:
                    num_str, fname = ln[1:].split("]:", 1)
                    entries.append((int(num_str), fname))
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    return entries

def find_cursor_for_page(page_no: int) -> Optional[str]:
    entries = read_page_log()
    for num, fname in entries:
        if num == page_no:
            return fname
    return None

def last_logged_page() -> Optional[Tuple[int, str]]:
    entries = read_page_log()
    return entries[-1] if entries else None

###################
# Main Execution  #
###################

def main():
    global start_time, job_id, processed_files_total, user_payload_skip_counts, user_payload_rows_built
    global duplicate_user_lines_collapsed, total_raw_file_lines, blank_lines_skipped
    start_time = time.time()
    user_payload_skip_counts.clear()
    user_payload_rows_built = 0
    duplicate_user_lines_collapsed = 0
    total_raw_file_lines = 0
    blank_lines_skipped = 0

    if DELETE_LOGS_ON_START and RESUME_FROM_PAGE_LOG:
        print(
            "[config] WARNING: DELETE_LOGS_ON_START=True and RESUME_FROM_PAGE_LOG=True are both set. "
            "page_log.txt and export_state.json will be deleted before the resume check runs — "
            "the run will start from the beginning of the job. Set DELETE_LOGS_ON_START=False to preserve resume state.",
            file=sys.stderr, flush=True,
        )

    if DELETE_LOGS_ON_START and (JOB_ONLY or SAMPLE_FILE):
        print(
            "[config] WARNING: DELETE_LOGS_ON_START=True with JOB_ONLY or SAMPLE_FILE will wipe all existing "
            "logs and state before exiting. Set DELETE_LOGS_ON_START=False to preserve prior run data.",
            file=sys.stderr, flush=True,
        )

    if DELETE_LOGS_ON_START:
        log_files = [
            ERROR_LOG_PATH,
            INVALID_USERS_PATH,
            QUARANTINE_PATH,
            BULK_USER_API_LOG_PATH,
            BULK_SUBS_API_LOG_PATH,
            PAGE_LOG_PATH,
            STATE_PATH,
            SUMMARY_PATH,
            AUDIT_LOG_PATH,
            AUDIT_MANIFEST_PATH,
        ]
        deleted = []
        for p in log_files:
            try:
                os.remove(p)
                deleted.append(os.path.basename(p))
            except FileNotFoundError:
                pass
        if deleted:
            print(f"[DELETE_LOGS_ON_START] Deleted: {', '.join(deleted)}")
        else:
            print("[DELETE_LOGS_ON_START] No log files found to delete.")

    if not origin_api_key or not destination_api_key:
        print(
            "Missing API keys. Set origin_api_key and destination_api_key at the top of this file.",
            file=sys.stderr,
        )
        sys.exit(1)

    _validate_identity_settings()
    _ern, _erc = _export_adjustment_maps()
    print(
        f"[config] Identity: source {SOURCE_PROJECT_TYPE} ({SOURCE_PREFERRED_IDENTIFIER}) "
        f"→ destination {DESTINATION_PROJECT_TYPE} ({DESTINATION_PREFERRED_IDENTIFIER})"
    )
    _fb = _hybrid_fallback_normalized()
    if str(DESTINATION_PROJECT_TYPE).strip().lower() == "hybrid":
        print(
            f"[config] Hybrid fallback when preferred id missing: {_fb!r} "
            f"(use 'email' or 'userId'; None = skip row like uuid)",
            flush=True,
        )
    print(
        f"[config] Flags: export_field_rename={len(_ern)} export_field_recast={len(_erc)} "
        f"additional_user_data_fields_keys={list(additional_user_data_fields.keys())} "
        f"create_new_fields={create_new_fields} testSample={testSample!r} "
        f"log_bulk_user_api_jsonl={log_bulk_user_api_jsonl} "
        f"debug_log_sample_bulk_user_payload={debug_log_sample_bulk_user_payload} "
        f"include_secondary_id_in_datafields={INCLUDE_SECONDARY_ID_IN_DATAFIELDS}"
    )
    if isinstance(testSample, int) and testSample > 0:
        print(
            f"[config] testSample={testSample}: cap is export *files* (each file processed completely, "
            "all JSONL lines). Set testSample to None to run every file in the job.",
            flush=True,
        )

    # Start or resume export
    resume_with_existing_job = False
    if job_id:
        resume_with_existing_job = True
        print(f"Resuming existing export with jobId: {job_id}", flush=True)
        try:
            with open(LAST_EXPORT_JOB_ID_PATH, "w", encoding="utf-8") as jf:
                jf.write(str(job_id) + "\n")
        except OSError:
            pass
    else:
        try:
            job_id = start_export()
        except requests.RequestException as e:
            print("Error starting export job:", e); return

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

    # Determine starting cursor + page number.
    # When RESUME_FROM_PAGE_LOG is False we always start fresh (cursor=None).
    # When True we try page_log.txt first, then fall back to export_state.json.
    # START_AFTER_FILE overrides everything when set.
    state = load_state()
    cursor           = None
    page_no          = 1
    export_truncated = False

    if START_AFTER_FILE:
        cursor = START_AFTER_FILE
        print(f"[start after] Starting from file after: {cursor}")

    elif RESUME_FROM_PAGE_LOG:
        last_entry = last_logged_page()
        if last_entry:
            # last_entry[0] is the last *completed* page; the next page to process is +1
            page_no = last_entry[0] + 1
            cursor = last_entry[1]
            print(f"[page log resume] Last completed page was {last_entry[0]}; resuming at page {page_no} (cursor={cursor})")
        else:
            saved = state.get(str(job_id))
            if saved:
                cursor = saved
                print(f"[state resume] page_log.txt empty; resuming from export_state.json (cursor={cursor})")
    else:
        print("[resume] RESUME_FROM_PAGE_LOG=False — starting from the beginning of the job.")

    # If asked to jump to a specific page (overrides resume, but not START_AFTER_FILE).
    # The stored cursor for page N is the end-of-page-N marker, so the first page
    # fetched will be page N+1.
    if not START_AFTER_FILE and isinstance(START_AT_PAGE, int):
        fname = find_cursor_for_page(START_AT_PAGE)
        if fname:
            page_no = START_AT_PAGE + 1
            cursor = fname
            print(f"[page jump] Cursor set to end of page {START_AT_PAGE}; first page to process: {page_no} (cursor={cursor})")
        else:
            print(f"[page jump] Page {START_AT_PAGE} not found in {PAGE_LOG_PATH}; starting from state/default.")

    # Phase 1 — wait for the export job to reach "completed" before touching any files.
    # Iterable may return files while the job is still "running", but we do not process
    # them until the job is fully done so the file list is stable and testSample is exact.
    print(f"[export] Waiting for job {job_id} to complete…", flush=True)
    while True:
        try:
            job_state, _, _, _ = list_files_page(job_id, None)
        except requests.RequestException as e:
            print("Error polling job state:", e); time.sleep(5); continue

        if job_state == "completed":
            print(f"[export] Job {job_id} is complete. Starting file processing.", flush=True)
            break
        if job_state == "failed":
            print("Export job failed."); return
        if job_state in ("cancelled", "cancelling"):
            print(f"Export job is {job_state}. Stopping."); return

        print(f"[export] jobState={job_state!r} — polling again in 10s…", flush=True)
        time.sleep(10)

    # Phase 2 — page loop: list → download (≤10) → process → delete → advance
    while True:
        try:
            job_state, files, _next_cursor, export_truncated = list_files_page(job_id, cursor)
        except requests.RequestException as e:
            print("Error listing files:", e); time.sleep(5); continue

        if job_state == "failed":
            print("Export job failed."); break

        if not files:
            if job_state == "completed":
                print("No files and job is completed. Done."); break
            if job_state in ("cancelled", "cancelling"):
                print(f"Export job is {job_state}. Stopping."); break
            print("No more files at this cursor. Done.", flush=True)
            break

        # testSample: cap how many export *files* we take from the job (each file processed in full).
        # Only applied once the job is fully complete — avoids cutting off mid-export
        # when files arrive incrementally while the job is still "running".
        if isinstance(testSample, int) and testSample > 0 and job_state == "completed":
            remaining = testSample - processed_files_total
            if remaining <= 0:
                print(
                    f"[test] testSample={testSample}: stopping after {processed_files_total} "
                    f"export file(s) processed in full (each file = all its JSONL lines).",
                    flush=True,
                )
                break
            files = files[:remaining]

        print(f"[page {page_no}] Received {len(files)} file(s). Downloading these...")
        local_paths = download_page_files(files)

        if len(local_paths) != len(files):
            print("[page] Not all files downloaded; refreshing page and retrying in 5s...")
            for p in local_paths:
                try: os.remove(p)
                except OSError: pass
            time.sleep(5); continue

        print(f"[page {page_no}] Processing files (users → subs per batch)...")
        try:
            process_json_files_page(local_paths)
            processed_files_total += len(local_paths)
        finally:
            print(f"[page {page_no}] Deleting files from disk...")
            delete_local_files(local_paths)

        # Advance cursor ONLY after page success
        cursor = files[-1]["file"]
        state[str(job_id)] = cursor
        save_state(state)

        # Log the pagination cursor for this page
        append_page_log(page_no, cursor)
        print(f"[page {page_no} done] Saved cursor: {cursor} (logged in {PAGE_LOG_PATH})")

        page_no += 1

        if len(files) < 10 and job_state == "completed":
            print("Likely last page; verifying...")

    if CLEANUP_AFTER:
        delete_export_files_only()

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
            "additional_user_data_fields": additional_user_data_fields,
            "USER_EXPORT_FIELD_ADJUSTMENTS": USER_EXPORT_FIELD_ADJUSTMENTS,
            "create_new_fields": create_new_fields,
            "testSample": testSample,
            "logging": {
                "log_bulk_user_api_jsonl": log_bulk_user_api_jsonl,
                "debug_log_sample_bulk_user_payload": debug_log_sample_bulk_user_payload,
                "bulk_user_api_jsonl_path": os.path.abspath(BULK_USER_API_LOG_PATH) if log_bulk_user_api_jsonl else None,
                "log_bulk_subs_api_jsonl": log_bulk_subs_api_jsonl,
                "bulk_subs_api_jsonl_path": os.path.abspath(BULK_SUBS_API_LOG_PATH) if log_bulk_subs_api_jsonl else None,
                "audit_mode": AUDIT_MODE,
                "audit_log_path": os.path.abspath(AUDIT_LOG_PATH) if AUDIT_MODE else None,
                "audit_manifest_path": os.path.abspath(AUDIT_MANIFEST_PATH) if AUDIT_MODE else None,
            },
            "total_lines_parsed": total_lines_parsed,
            "total_raw_file_lines": total_raw_file_lines,
            "blank_lines_skipped": blank_lines_skipped,
            "user_payload_rows_built": user_payload_rows_built,
            "duplicate_user_lines_collapsed": duplicate_user_lines_collapsed,
            "user_payload_skip_by_reason": dict(sorted(user_payload_skip_counts.items())),
            "user_bulk_success_count_total": user_updated_counter,
            "user_bulk_api_fail_count_total": bulk_user_api_reported_failures,
            "subscription_updates_sent": subs_updated_counter,
            "users_skipped": skipped_users_counter,
            "unmapped_ids_dropped": remap_dropped_ids,
            "invalid_users_log": os.path.abspath(INVALID_USERS_PATH),
            "timed_out_quarantine": os.path.abspath(QUARANTINE_PATH),
            "errors_log": os.path.abspath(ERROR_LOG_PATH),
            "export_dir": os.path.abspath(dirname),
            "processed_files_total": processed_files_total,
            "last_cursor": cursor,
            "export_truncated": export_truncated,
            "page_log": os.path.abspath(PAGE_LOG_PATH),
            "run_seconds": round(end_time - start_time, 2) if end_time else None
        }
        with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"[summary] Wrote {SUMMARY_PATH}")
    except Exception as e:
        print(f"[summary] Failed to write summary: {e}")

    print(
        f"Lines read from export file(s) (wc -l style): {total_raw_file_lines}"
        + (f"; blank/whitespace-only skipped: {blank_lines_skipped}" if blank_lines_skipped else "")
    )
    print(f"Non-empty lines processed as JSON: {total_lines_parsed}")
    print(f"Rows built for bulk user update: {user_payload_rows_built}")
    if duplicate_user_lines_collapsed:
        print(
            f"Duplicate export lines merged (same destination user, last line wins): {duplicate_user_lines_collapsed}"
        )
    if user_payload_skip_counts:
        print(f"Rows skipped (no user payload): {sum(user_payload_skip_counts.values())} by reason: {dict(sorted(user_payload_skip_counts.items()))}")
    print(f"Bulk USER successCount total (Iterable API): {user_updated_counter}")
    print(f"Bulk USER failCount total (Iterable API): {bulk_user_api_reported_failures}")
    if log_bulk_user_api_jsonl:
        print(f"Bulk USER API JSONL log: {os.path.abspath(BULK_USER_API_LOG_PATH)}")
    print(f"Bulk SUBS updates sent: {subs_updated_counter}")
    if log_bulk_subs_api_jsonl:
        print(f"Bulk SUBS API JSONL log (failures only): {os.path.abspath(BULK_SUBS_API_LOG_PATH)}")
    if AUDIT_MODE:
        print(f"Audit log: {os.path.abspath(AUDIT_LOG_PATH)}")
        print(f"Audit manifest: {os.path.abspath(AUDIT_MANIFEST_PATH)}")
    print(f"Users skipped (missing id / single-record errors): {skipped_users_counter}")
    print(f"Unmapped ids dropped during remap: {remap_dropped_ids}")
    print(f"Invalid user details: {INVALID_USERS_PATH}")
    print(f"Timed-out single-user quarantine: {QUARANTINE_PATH}")
    print(f"Page log: {PAGE_LOG_PATH}")
    print(f"Other errors (if any): {ERROR_LOG_PATH}")
    if export_truncated:
        print(f"\n⚠ Export was truncated. Start a new export job with start_datetime set to the last processed createdAt.")
    else:
        print("\nExport was not truncated. Migration complete.")

if __name__ == "__main__":
    main()
