#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable List Migration
=======================
Migrates Standard, Suppression, and Internal lists — and their members —
from one Iterable project to another.

Three resumable phases — re-run safely at any point:

  Phase 1 — Create lists
    GET /api/lists (source) → filter Standard/Suppression/Internal
    → POST /api/lists (dest)
    Writes migrate_lists/list_mapping.json.
    Already-created lists are skipped on re-run.

  Phase 2 — Export users
    POST /api/export/start  dataTypeName=user
        onlyFields=email,userId,userListIds
    Polls until complete. Re-uses existing job if recorded in state.

  Phase 3 — Subscribe members
    Downloads export files in batches, parses each user's userListIds
    (or emailListIds for email-based projects), maps source → dest list IDs,
    then calls POST /api/lists/subscribe in batches of SUBSCRIBE_BATCH_SIZE.
    Processed files are tracked in export_state.json so restarts don't re-send.
"""

import os, sys, json, time, random, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

###############################################################################
# MIGRATION SETTINGS — edit the sections below
###############################################################################
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# ── WORKING DIRECTORY ─────────────────────────────────────────────────────────
dirname = os.path.join(_CONFIG_DIR, "migrate_lists")

# ── API KEYS ──────────────────────────────────────────────────────────────────
origin_api_key      = ""
destination_api_key = ""

# ── PHASE CONTROL ─────────────────────────────────────────────────────────────
RUN_PHASE_1 = True  # Create destination lists + build list_mapping.json
RUN_PHASE_2 = True   # Start/poll user export job
RUN_PHASE_3 = True   # Download files and subscribe members

# ── LIST FILTER ───────────────────────────────────────────────────────────────
# List types to migrate. Standard/Suppression/Internal are the static types.
# Dynamic/Scheduled are computed and cannot be directly migrated.
LIST_TYPES: List[str] = ["Standard", "Suppression", "Internal"]

# Only these list names will be migrated (case-sensitive). Empty = migrate all.
LIST_ALLOW:  List[str] = []
# These list names will always be skipped even if in LIST_ALLOW.
LIST_BLOCK:  List[str] = []

# ── EXPORT JOB ────────────────────────────────────────────────────────────────
# Set to an existing jobId to resume Phase 2/3, or None to start fresh.
job_id: Optional[int] = None

# Limit total export files processed (None = all). Useful for dry-runs.
testSample: Optional[int] = None

# ── IDENTITY ──────────────────────────────────────────────────────────────────
# "email" or "userId" — which identifier to use in /api/lists/subscribe calls.
DESTINATION_PREFERRED_IDENTIFIER = "email"

# ── PERFORMANCE ───────────────────────────────────────────────────────────────
# Subscribers per /api/lists/subscribe call (Iterable max: 1000).
SUBSCRIBE_BATCH_SIZE = 500
# Parallel subscribe workers.
SUBSCRIBE_WORKERS   = 10
# Export files to download concurrently.
DOWNLOAD_BATCH      = 10

# ── RUN OPTIONS ───────────────────────────────────────────────────────────────
CLEANUP_AFTER        = True    # Delete export files after processing
DELETE_LOGS_ON_START = True    # Wipe error/invalid/skipped logs on start
                               # State and mapping are NOT wiped — restart is safe.

# ── TIMEOUTS ──────────────────────────────────────────────────────────────────
LIST_TIMEOUT         = 30
DOWNLOAD_TIMEOUT     = 120
POST_CONNECT_TIMEOUT = 10
POST_READ_TIMEOUT    = 30

retry_config = {
    "stop_max_attempt_number":     10,
    "wait_exponential_multiplier": 1000,
    "wait_exponential_max":        60000,
}

###############################################################################
# Derived paths
###############################################################################

LIST_MAPPING_PATH   = os.path.join(dirname, "list_mapping.json")
STATE_PATH          = os.path.join(dirname, "export_state.json")
ERROR_LOG_PATH      = os.path.join(dirname, "list_errors.log")
INVALID_PATH        = os.path.join(dirname, "invalid_subscriptions.jsonl")
SKIPPED_PATH        = os.path.join(dirname, "skipped_users.jsonl")
SUMMARY_PATH        = os.path.join(dirname, "run_summary.json")

os.makedirs(dirname, exist_ok=True)

###############################################################################
# API endpoints
###############################################################################

iterable_base_url          = "https://api.iterable.com"
lists_endpoint             = "/api/lists"
subscribe_endpoint         = "/api/lists/subscribe"
export_start_endpoint      = "/api/export/start"
export_files_endpoint_tmpl = "/api/export/{job_id}/files"

###############################################################################
# Counters
###############################################################################

start_time = 0.0

lists_created        = 0
lists_skipped        = 0
subscriptions_sent   = 0
subscriptions_failed = 0
users_skipped        = 0
downloaded_files     = 0
processed_files      = 0
export_truncated     = False

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
_adapter = HTTPAdapter(
    pool_connections=SUBSCRIBE_WORKERS + 4,
    pool_maxsize=SUBSCRIBE_WORKERS + 4,
    max_retries=_retry_strategy,
)
session.mount("https://", _adapter)
session.mount("http://",  _adapter)
DEFAULT_HEADERS = {"Connection": "keep-alive", "Accept-Encoding": "gzip, deflate"}

_write_lock = threading.Lock()

###############################################################################
# Retry decorator
###############################################################################

class NonRetryableError(Exception):
    """Raised for permanent 4xx failures — never retried."""


def retry_post(func):
    import functools
    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        max_a = retry_config["stop_max_attempt_number"]
        wm    = retry_config["wait_exponential_multiplier"]
        wmax  = retry_config["wait_exponential_max"]
        for attempt in range(1, max_a + 1):
            try:
                return func(*args, **kwargs)
            except NonRetryableError:
                raise
            except Exception as exc:
                if attempt >= max_a:
                    raise
                cap_ms = min(wmax, wm * (2 ** (attempt - 1)))
                wait_s = random.uniform(0, cap_ms) / 1000.0
                print(f"[retry] {func.__name__} attempt {attempt}/{max_a} failed ({exc}); retrying in {wait_s:.1f}s…", flush=True)
                time.sleep(wait_s)
    return wrapped

###############################################################################
# Utilities
###############################################################################

def log_error(msg: str):
    try:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg.rstrip()}\n")
    except Exception:
        pass


def write_jsonl(path: str, obj: dict):
    with _write_lock:
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


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

###############################################################################
# Phase 1 — Create destination lists
###############################################################################

def fetch_source_lists() -> List[dict]:
    """GET /api/lists from source, return only the static list types."""
    r = session.get(
        f"{iterable_base_url}{lists_endpoint}",
        headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS},
        timeout=LIST_TIMEOUT,
    )
    if not r.ok:
        raise requests.HTTPError(f"GET /api/lists failed: {r.status_code} — {r.text}")
    all_lists = r.json().get("lists", [])

    filtered = [l for l in all_lists if l.get("listType") in LIST_TYPES]

    if LIST_ALLOW:
        filtered = [l for l in filtered if l["name"] in LIST_ALLOW]
    if LIST_BLOCK:
        filtered = [l for l in filtered if l["name"] not in LIST_BLOCK]

    return filtered


@retry_post
def create_dest_list(name: str, description: str, list_type: str) -> int:
    """POST /api/lists in destination. Returns the new listId."""
    body: Dict[str, Any] = {"name": name}
    if description:
        body["description"] = description
    r = session.post(
        f"{iterable_base_url}{lists_endpoint}",
        headers={"Content-Type": "application/json", "Api-Key": destination_api_key, **DEFAULT_HEADERS},
        json=body,
        timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
    )
    if r.status_code >= 400 and r.status_code < 500:
        raise NonRetryableError(f"POST /api/lists HTTP {r.status_code} — {r.text[:400]}")
    r.raise_for_status()
    return int(r.json()["listId"])


def run_phase1() -> List[dict]:
    """
    Create all eligible lists in the destination and write list_mapping.json.
    Returns the mapping list (each entry: {name, description, listType, source_id, dest_id}).
    If list_mapping.json already exists, loads and returns it without re-creating.
    """
    global lists_created, lists_skipped

    if os.path.exists(LIST_MAPPING_PATH):
        with open(LIST_MAPPING_PATH, "r", encoding="utf-8") as f:
            mapping = json.load(f)
        print(f"[phase1] list_mapping.json already exists — {len(mapping)} list(s) loaded. Skipping creation.", flush=True)
        return mapping

    source_lists = fetch_source_lists()
    type_counts  = {}
    for l in source_lists:
        t = l.get("listType", "Unknown")
        type_counts[t] = type_counts.get(t, 0) + 1
    print(f"[phase1] Found {len(source_lists)} list(s) to migrate: {type_counts}", flush=True)

    mapping: List[dict] = []

    for lst in source_lists:
        name        = lst["name"]
        description = lst.get("description", "") or ""
        source_id   = lst["id"]
        list_type   = lst.get("listType", "Standard")

        try:
            dest_id = create_dest_list(name, description, list_type)
            print(f"  [{list_type}] Created: {name!r}  source={source_id} → dest={dest_id}", flush=True)
            lists_created += 1
        except NonRetryableError as exc:
            print(f"  [{list_type}] [warn] Failed to create {name!r}: {exc}", flush=True)
            log_error(f"create_dest_list failed for {name!r}: {exc}")
            lists_skipped += 1
            dest_id = None
        except Exception as exc:
            print(f"  [{list_type}] [error] Failed to create {name!r}: {exc}", flush=True)
            log_error(f"create_dest_list exception for {name!r}: {exc}")
            lists_skipped += 1
            dest_id = None

        mapping.append({
            "name":        name,
            "description": description,
            "listType":    list_type,
            "source_id":   source_id,
            "dest_id":     dest_id,
        })

    with open(LIST_MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2)
    print(f"[phase1] Mapping written to {LIST_MAPPING_PATH}", flush=True)

    return mapping

###############################################################################
# Phase 2 — Start/poll user export job
###############################################################################

def start_export() -> int:
    """
    Start a full user export requesting only the identifier and list-membership
    fields. In userId-based and Hybrid projects the membership field is
    userListIds; email-based projects use emailListIds. We request both so
    the same export works regardless of project type.
    """
    body = {
        "dataTypeName": "user",
        "outputFormat": "application/x-json-stream",
        "onlyFields":   "email,userId,userListIds,emailListIds",
    }
    r = session.post(
        f"{iterable_base_url}{export_start_endpoint}",
        headers={"Content-Type": "application/json", "Api-Key": origin_api_key, **DEFAULT_HEADERS},
        json=body,
        timeout=LIST_TIMEOUT,
    )
    if not r.ok:
        raise requests.HTTPError(f"Export start failed: {r.status_code} — {r.text}")
    jid = int(r.json()["jobId"])
    print(f"[phase2] Export job started. jobId: {jid}", flush=True)
    return jid


def list_files_page(jid: int, start_after: Optional[str]) -> Tuple[str, List[dict], Optional[str], bool]:
    url    = f"{iterable_base_url}{export_files_endpoint_tmpl.format(job_id=jid)}"
    params = {"startAfter": start_after} if start_after else {}
    r = session.get(
        url,
        headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS},
        params=params,
        timeout=LIST_TIMEOUT,
    )
    if not r.ok:
        raise requests.HTTPError(f"List files failed: {r.status_code} — {r.text}")
    data        = r.json()
    files       = data.get("files", [])
    job_state   = str(data.get("jobState", "")).lower()
    truncated   = bool(data.get("exportTruncated", False))
    next_cursor = files[-1]["file"] if files else None
    return job_state, files, next_cursor, truncated


def poll_until_complete(jid: int):
    print(f"[phase2] Polling export job {jid}…", flush=True)
    while True:
        job_state, files, _, _ = list_files_page(jid, start_after=None)
        print(f"  jobState={job_state!r}  files_so_far={len(files)}", flush=True)
        if job_state == "completed":
            print("[phase2] Export job complete.", flush=True)
            return
        if job_state in ("error", "failed"):
            raise RuntimeError(f"Export job {jid} entered state {job_state!r}")
        time.sleep(10)


def run_phase2(state: dict) -> int:
    """Start (or resume) the user export job. Returns the jobId."""
    jid = job_id or state.get("job_id")
    if jid:
        print(f"[phase2] Using existing export job: {jid}", flush=True)
    else:
        jid = start_export()
        state["job_id"]          = jid
        state["processed_files"] = []
        save_state(state)
    poll_until_complete(jid)
    return jid

###############################################################################
# Phase 3 — Download files and subscribe members
###############################################################################

def download_file(entry: Dict[str, str]) -> Optional[str]:
    global downloaded_files
    name = entry.get("file")
    url  = entry.get("url")
    if not name or not url:
        return None
    dest = os.path.join(dirname, name)
    try:
        with session.get(url, headers=DEFAULT_HEADERS, stream=True, timeout=DOWNLOAD_TIMEOUT) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=65536):
                    if chunk:
                        fh.write(chunk)
        with _write_lock:
            downloaded_files += 1
        print(f"  Downloaded: {name}", flush=True)
        return dest
    except Exception as exc:
        print(f"  [error] Failed to download {name}: {exc}", flush=True)
        log_error(f"Download failed for {name}: {exc}")
        return None


def _parse_list_ids(raw) -> List[int]:
    """
    Parse userListIds or emailListIds from an export row.
    Can be a JSON array, JSON string, comma-separated string, or int.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [int(x) for x in raw if x is not None]
    if isinstance(raw, int):
        return [raw]
    s = str(raw).strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [int(x) for x in parsed]
        return [int(parsed)]
    except (json.JSONDecodeError, ValueError):
        pass
    ids = []
    for part in s.split(","):
        part = part.strip().strip("[]")
        if part:
            try:
                ids.append(int(part))
            except ValueError:
                pass
    return ids


@retry_post
def send_subscribe(dest_list_id: int, subscribers: List[dict]):
    """POST /api/lists/subscribe for a batch of subscribers."""
    r = session.post(
        f"{iterable_base_url}{subscribe_endpoint}",
        headers={"Content-Type": "application/json", "Api-Key": destination_api_key, **DEFAULT_HEADERS},
        json={
            "listId":                  dest_list_id,
            "subscribers":             subscribers,
            "updateExistingUsersOnly": False,
        },
        timeout=(POST_CONNECT_TIMEOUT, POST_READ_TIMEOUT),
    )
    if r.status_code >= 400 and r.status_code < 500:
        raise NonRetryableError(f"subscribe HTTP {r.status_code} — {r.text[:400]}")
    r.raise_for_status()
    body = r.json()
    fail_details = body.get("failedUpdates") or body.get("invalidEmails") or []
    return body.get("successCount", 0), body.get("failCount", 0), fail_details


def _send_individual(dest_id: int, subscriber: dict, file_name: str) -> Tuple[int, int]:
    """Subscribe a single user and log any failure. Returns (ok, fail)."""
    try:
        ok, fail, details = send_subscribe(dest_id, [subscriber])
        if fail:
            log_error(f"individual subscribe dest_list={dest_id} failed for {subscriber} — file={file_name} details={details}")
            write_jsonl(INVALID_PATH, {
                "reason":       "user-level failure",
                "dest_list_id": dest_id,
                "subscriber":   subscriber,
                "source_file":  file_name,
                "details":      details,
            })
        return ok, fail
    except NonRetryableError as exc:
        log_error(f"individual subscribe dest_list={dest_id} non-retryable for {subscriber}: {exc}")
        write_jsonl(INVALID_PATH, {
            "reason":       str(exc),
            "dest_list_id": dest_id,
            "subscriber":   subscriber,
            "source_file":  file_name,
        })
        return 0, 1
    except Exception as exc:
        log_error(f"individual subscribe dest_list={dest_id} error for {subscriber}: {exc}")
        return 0, 1


def _flush_list_batches(pending: Dict[int, List[dict]], file_name: str):
    """
    Send all pending subscriber batches in parallel across lists.
    If a batch returns any failCount, fall back to individual sends for every
    subscriber in that batch so we can isolate bad records precisely.
    """
    global subscriptions_sent, subscriptions_failed

    tasks: List[Tuple[int, List[dict]]] = []
    for dest_id, subs in pending.items():
        for i in range(0, len(subs), SUBSCRIBE_BATCH_SIZE):
            tasks.append((dest_id, subs[i : i + SUBSCRIBE_BATCH_SIZE]))

    if not tasks:
        return

    def _send_batch_with_fallback(dest_id: int, batch: List[dict]):
        # Attempt the batch first
        try:
            ok, fail, details = send_subscribe(dest_id, batch)
            if fail == 0:
                return dest_id, ok, 0
            # Partial or total failure — fall back to individual sends
            print(
                f"    [fallback] batch of {len(batch)} had {fail} failure(s) for"
                f" list {dest_id} — retrying individually",
                flush=True,
            )
        except NonRetryableError as exc:
            log_error(f"subscribe dest_list={dest_id} non-retryable batch: {exc} — file={file_name}")
            print(f"    [fallback] non-retryable batch error for list {dest_id} — retrying individually", flush=True)
        except Exception as exc:
            log_error(f"subscribe dest_list={dest_id} batch error: {exc} — file={file_name}")
            print(f"    [fallback] batch error for list {dest_id} — retrying individually", flush=True)

        # Individual fallback
        ind_ok = ind_fail = 0
        for sub in batch:
            o, f = _send_individual(dest_id, sub, file_name)
            ind_ok  += o
            ind_fail += f
        return dest_id, ind_ok, ind_fail

    with ThreadPoolExecutor(max_workers=SUBSCRIBE_WORKERS) as executor:
        futures = {executor.submit(_send_batch_with_fallback, d, b): (d, b) for d, b in tasks}
        for future in as_completed(futures):
            dest_id, ok, fail = future.result()
            with _write_lock:
                subscriptions_sent   += ok
                subscriptions_failed += fail


def process_file(path: str, source_to_dest: Dict[int, int]):
    """
    Parse one export file and batch-subscribe each user to their destination lists.
    Reads userListIds (userId-based / Hybrid projects) with emailListIds as fallback
    (email-based projects).
    """
    global users_skipped
    file_name = os.path.basename(path)

    pending: Dict[int, List[dict]] = {}

    try:
        with open(path, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as exc:
                    log_error(f"JSON parse error in {file_name}: {exc}")
                    continue

                # Prefer the configured identifier; fall back to the other field
                # and update the key so we never submit a UUID as an email.
                if DESTINATION_PREFERRED_IDENTIFIER == "userId":
                    id_value = (row.get("userId") or "").strip()
                    subscriber_key = "userId" if id_value else "email"
                    id_value = id_value or (row.get("email") or "").strip()
                else:
                    id_value = (row.get("email") or "").strip()
                    subscriber_key = "email" if id_value else "userId"
                    id_value = id_value or (row.get("userId") or "").strip()

                if not id_value:
                    with _write_lock:
                        users_skipped += 1
                    write_jsonl(SKIPPED_PATH, {"reason": "missing_identifier", "row": row, "source_file": file_name})
                    continue

                subscriber = {subscriber_key: id_value}

                # userListIds for userId-based/Hybrid; emailListIds for email-based
                raw_ids = row.get("userListIds") or row.get("emailListIds")
                list_ids = _parse_list_ids(raw_ids)

                for src_id in list_ids:
                    dest_id = source_to_dest.get(src_id)
                    if dest_id is None:
                        continue
                    pending.setdefault(dest_id, []).append(subscriber)

    except (IOError, OSError) as exc:
        print(f"  [error] Could not read {path}: {exc}", flush=True)
        log_error(f"File read error: {exc}")
        return

    _flush_list_batches(pending, file_name)


def run_phase3(jid: int, mapping: List[dict], state: dict):
    global processed_files, export_truncated

    source_to_dest: Dict[int, int] = {
        e["source_id"]: e["dest_id"]
        for e in mapping
        if e.get("dest_id") is not None
    }

    processed_set  = set(state.get("processed_files", []))
    cursor         = state.get("last_cursor")
    files_seen     = 0
    sample_reached = False

    while True:
        job_state, files, next_cursor, trunc = list_files_page(jid, start_after=cursor)
        export_truncated = trunc

        if not files:
            print("[phase3] No more files.", flush=True)
            break

        print(f"\n--- Page: {len(files)} file(s) (cursor={cursor!r}) ---", flush=True)

        local_paths: Dict[str, Optional[str]] = {}
        with ThreadPoolExecutor(max_workers=DOWNLOAD_BATCH) as ex:
            future_map = {ex.submit(download_file, e): e.get("file", "") for e in files}
            for fut in as_completed(future_map):
                fname = future_map[fut]
                local_paths[fname] = fut.result()

        for entry in files:
            files_seen += 1
            if testSample is not None and files_seen > testSample:
                sample_reached = True
                break

            file_name = entry.get("file", "")
            if file_name and file_name in processed_set:
                print(f"  Skipping {file_name} (already processed).", flush=True)
                processed_files += 1
                continue

            local = local_paths.get(file_name)
            if not local:
                continue

            print(f"  Processing {file_name} …", flush=True)
            process_file(local, source_to_dest)
            processed_files += 1

            if file_name:
                processed_set.add(file_name)

            if CLEANUP_AFTER:
                try:
                    os.remove(local)
                except OSError:
                    pass

        state["last_cursor"]     = next_cursor or cursor
        state["processed_files"] = list(processed_set)
        save_state(state)

        if next_cursor:
            if sample_reached:
                break
            cursor = next_cursor
        else:
            break

    if sample_reached:
        print(f"testSample={testSample} reached — stopping.", flush=True)

###############################################################################
# Summary
###############################################################################

def write_summary(jid: Optional[int]):
    elapsed = time.time() - start_time
    summary = {
        "job_id":               jid,
        "elapsed_seconds":      round(elapsed, 2),
        "lists_created":        lists_created,
        "lists_skipped":        lists_skipped,
        "subscriptions_sent":   subscriptions_sent,
        "subscriptions_failed": subscriptions_failed,
        "users_skipped":        users_skipped,
        "downloaded_files":     downloaded_files,
        "processed_files":      processed_files,
        "export_truncated":     export_truncated,
    }
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n═══ Run Summary ═══", flush=True)
    print(f"  Elapsed:               {elapsed:.1f}s", flush=True)
    print(f"  Lists created:         {lists_created}", flush=True)
    print(f"  Lists skipped/failed:  {lists_skipped}", flush=True)
    print(f"  Subscriptions sent:    {subscriptions_sent}", flush=True)
    print(f"  Subscriptions failed:  {subscriptions_failed}", flush=True)
    print(f"  Users skipped:         {users_skipped}", flush=True)
    if export_truncated:
        print("\n  ⚠ Export was truncated. Re-run with job_id=None to start a fresh export.", flush=True)
    else:
        print("\n  Export was not truncated. Migration complete.", flush=True)
    print(f"\n  Summary: {SUMMARY_PATH}", flush=True)
    print("═══════════════════", flush=True)

###############################################################################
# Entry point
###############################################################################

def run():
    global start_time
    start_time = time.time()

    if DELETE_LOGS_ON_START:
        for p in [ERROR_LOG_PATH, INVALID_PATH, SKIPPED_PATH, SUMMARY_PATH]:
            try:
                os.remove(p)
            except FileNotFoundError:
                pass

    state   = load_state()
    mapping = []
    jid     = None

    # ── Phase 1 ───────────────────────────────────────────────────────────────
    if RUN_PHASE_1:
        print("\n══ Phase 1: Create destination lists ══", flush=True)
        mapping = run_phase1()
    else:
        if os.path.exists(LIST_MAPPING_PATH):
            with open(LIST_MAPPING_PATH, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            print(f"[phase1 skipped] Loaded {len(mapping)} mappings from {LIST_MAPPING_PATH}", flush=True)
        else:
            print("[error] RUN_PHASE_1=False but list_mapping.json not found. Run Phase 1 first.", flush=True)
            sys.exit(1)

    # ── Phase 2 ───────────────────────────────────────────────────────────────
    if RUN_PHASE_2:
        print("\n══ Phase 2: Export users ══", flush=True)
        jid = run_phase2(state)
    else:
        jid = job_id or state.get("job_id")
        if not jid:
            print("[error] RUN_PHASE_2=False but no job_id found. Set job_id or run Phase 2 first.", flush=True)
            sys.exit(1)
        print(f"[phase2 skipped] Using job_id={jid}", flush=True)

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    if RUN_PHASE_3:
        print("\n══ Phase 3: Subscribe members ══", flush=True)
        run_phase3(jid, mapping, state)

    write_summary(jid)


if __name__ == "__main__":
    run()
