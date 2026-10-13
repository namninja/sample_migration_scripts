#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable Snippet Migration
==========================
Copies snippets from one Iterable project to another.

    GET /api/snippets                 (source + destination: list with content)
    PUT /api/snippets/{name}          (destination: create-or-update by name)
    POST /api/snippets                (destination: only for all-digit names)

Run this BEFORE the template migrations. Templates reference snippets by name,
and a template that references a snippet missing from the destination fails to
save.

HOW TO USE (IDE, e.g. Spyder / PyCharm / VS Code — no CLI arguments)
    1. Edit the MIGRATION SETTINGS block below.
    2. Run with DRY_RUN = True first. It reads both projects and prints what
       would be created / updated / skipped, without writing anything.
    3. Set DRY_RUN = False and run again.

HOW IT WORKS
    * Idempotent: PUT-by-name creates the snippet if it is missing and updates
      it if it exists, so re-running never creates duplicates. Snippets whose
      content, description and variables already match the destination are
      skipped, so their "updated" timestamp is not touched.
    * Dependencies: a snippet can include another snippet, and the destination
      validates that on save. Failed snippets are retried in further passes
      (up to MAX_PASSES) so a snippet can succeed once the one it uses exists.
      The run stops early if a pass makes no progress.
    * Every run writes to <this folder>/migrate_snippets/:
        snippet_mapping.json   snippet name -> source ID, destination ID, result
        run_summary.json       counts and failures for the last run
        errors.log             one line per final failure

IMPORTANT
    1. The Snippets API is behind a per-project feature flag (SnippetsApi). If it
       is off for either project the API answers HTTP 403 "Snippets API not
       enabled for this project". Ask Iterable to enable it for that project.
    2. Names are copied exactly and are never renamed: a snippet name cannot be
       changed after it is created, and templates reference snippets by name.
       Valid names are up to 100 characters of letters, digits, hyphens,
       underscores and spaces.
    3. A snippet that already exists in the destination (same name) is updated
       when OVERWRITE_EXISTING is True, and left alone when it is False.
    4. The creator and created/updated timestamps are NOT copied. createdBy must
       be a member of the destination project, so by default the destination
       project creator is recorded. Set CREATED_BY_USER_ID to use someone else.
    5. Content is copied as-is. Image URLs keep pointing at the source project's
       CDN, and any catalog or data feed a snippet uses must also exist in the
       destination to render.
    6. The destination rejects invalid Handlebars and disallowed content
       (script tags with JavaScript sources, inline event handlers such as
       onload=, and javascript: URLs). Those snippets are reported as failures.
    7. The API treats an all-digit identifier as an ID, not a name. Snippets with
       all-digit names are created with POST instead of PUT-by-name.
    8. Nothing is deleted. Snippets that exist only in the destination are left
       untouched.
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

###############################################################################
# MIGRATION SETTINGS — edit the sections below
###############################################################################
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# ── WORKING DIRECTORY ─────────────────────────────────────────────────────────
dirname = os.path.join(_CONFIG_DIR, "migrate_snippets")

# ── API KEYS ──────────────────────────────────────────────────────────────────
# Server-side keys. Never commit real keys.
origin_api_key      = "YOUR_ORIGIN_API_KEY"      # example placeholder - replace
destination_api_key = "YOUR_DESTINATION_API_KEY" # example placeholder - replace

# ── SNIPPET FILTER ────────────────────────────────────────────────────────────
# Allow-list of snippet NAMES to migrate (case-sensitive). Empty = migrate all.
SNIPPET_NAMES_TO_MIGRATE: List[str] = ["Footer_Legal", "Header_Logo"]   # example names - replace (or [] for all)
# Block-list of snippet NAMES to skip, even if they are in the allow-list.
SNIPPET_NAMES_TO_SKIP: List[str] = ["Old_Test_Snippet"]   # example name - replace

# ── OPTIONS ───────────────────────────────────────────────────────────────────
# True  = update a destination snippet that already has the same name.
# False = leave existing destination snippets untouched (only create new ones).
OVERWRITE_EXISTING = True

# Optional destination project member (email) to record as creator/updater.
# None = the destination project creator. Must be a member of the destination.
CREATED_BY_USER_ID: Optional[str] = None   # example: "teammate@example.com"

# How many passes to make over failed snippets (handles snippets that include
# other snippets). A pass with no successes ends the run early.
MAX_PASSES = 5

# Read both projects and show what would happen. Writes nothing.
DRY_RUN = True

# ── HTTP ──────────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT   = (10, 60)   # (connect, read) seconds
REQUEST_DELAY_SEC = 0.2        # pause before every API call
MAX_RETRIES       = 4          # for 429 / 5xx / connection errors

###############################################################################
# Derived settings — no edits needed below this line
###############################################################################
BASE_URL        = "https://api.iterable.com"
SNIPPETS_PATH   = "/api/snippets"

MAPPING_PATH    = os.path.join(dirname, "snippet_mapping.json")
SUMMARY_PATH    = os.path.join(dirname, "run_summary.json")
ERROR_LOG_PATH  = os.path.join(dirname, "errors.log")

VALID_NAME = re.compile(r"^[A-Za-z0-9_\- ]{1,100}$")

session = requests.Session()
_retry = Retry(
    total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES, status=MAX_RETRIES,
    status_forcelist=[429, 500, 502, 503, 504],
    # PUT-by-name is idempotent. POST (all-digit names only) is safe too: a
    # repeated create is rejected because snippet names are unique.
    allowed_methods={"GET", "PUT", "POST"},
    backoff_factor=1.5, respect_retry_after_header=True, raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=_retry))


class MigrationError(Exception):
    """A per-snippet failure. The run continues with the next snippet."""


###############################################################################
# Helpers
###############################################################################

def _missing_key(k: str) -> bool:
    """True if the key is empty or still the YOUR_... example placeholder."""
    return not k or k.startswith("YOUR_")


def _headers(api_key: str) -> Dict[str, str]:
    return {"Api-Key": api_key, "Content-Type": "application/json", "Accept": "application/json"}


def _json_or_none(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return None


def api_call(method: str, path: str, api_key: str, **kwargs) -> Tuple[int, Any, str]:
    """Returns (http_status, parsed_json_or_None, raw_text)."""
    time.sleep(REQUEST_DELAY_SEC)
    resp = session.request(method, BASE_URL + path, headers=_headers(api_key),
                           timeout=REQUEST_TIMEOUT, **kwargs)
    return resp.status_code, _json_or_none(resp), resp.text


def detail(body: Any, text: str) -> str:
    if isinstance(body, dict) and body.get("msg"):
        return str(body["msg"])
    return (text or "").strip()[:300] or "(empty response)"


def load_json(path: str, default: Any) -> Any:
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)  # atomic: a crash never leaves a half-written file


def log_error(message: str) -> None:
    with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()}  {message}\n")


def fetch_snippets(api_key: str, label: str) -> List[Dict[str, Any]]:
    """All snippets in a project (the list endpoint includes content)."""
    status, body, text = api_call("GET", SNIPPETS_PATH, api_key)
    if status == 403:
        raise SystemExit(
            f"{label} project: HTTP 403 - {detail(body, text)}. "
            f"The Snippets API (SnippetsApi flag) must be enabled for that project."
        )
    if status == 401:
        raise SystemExit(f"{label} project: HTTP 401 - the API key was rejected.")
    if status != 200 or not isinstance(body, dict) or not isinstance(body.get("snippets"), list):
        raise SystemExit(f"{label} project: could not list snippets (HTTP {status}): {detail(body, text)}")
    return body["snippets"]


def _variables(snippet: Dict[str, Any]) -> List[str]:
    return list(snippet.get("variables") or [])


def is_same(src: Dict[str, Any], dst: Dict[str, Any]) -> bool:
    return (
        (src.get("content") or "") == (dst.get("content") or "")
        and (src.get("description") or "") == (dst.get("description") or "")
        and _variables(src) == _variables(dst)
    )


def build_body(src: Dict[str, Any]) -> Dict[str, Any]:
    body: Dict[str, Any] = {"content": src.get("content") or "", "variables": _variables(src)}
    if src.get("description"):
        body["description"] = src["description"]
    if CREATED_BY_USER_ID:
        body["createdByUserId"] = CREATED_BY_USER_ID
    return body


def find_snippet_id(body: Any) -> Optional[int]:
    """The snippet ID from a create/update response, wherever it is nested."""
    if isinstance(body, dict):
        for key in ("snippetId", "id"):
            if isinstance(body.get(key), int):
                return body[key]
        return find_snippet_id(body.get("params"))
    return None


###############################################################################
# Migration
###############################################################################

def plan_snippet(src: Dict[str, Any], dest_by_name: Dict[str, Dict[str, Any]]) -> str:
    """One of: create, update, unchanged, skip_existing."""
    existing = dest_by_name.get(src["name"])
    if existing is None:
        return "create"
    if not OVERWRITE_EXISTING:
        return "skip_existing"
    return "unchanged" if is_same(src, existing) else "update"


def write_snippet(src: Dict[str, Any], action: str, dest_by_name: Dict[str, Dict[str, Any]]) -> Optional[int]:
    """Create or update one snippet in the destination. Returns its destination ID."""
    name = src["name"]
    body = build_body(src)

    if action == "update":
        # Address an existing snippet by its numeric ID: unambiguous for any name.
        dest_id = dest_by_name[name]["id"]
        status, resp, text = api_call("PUT", f"{SNIPPETS_PATH}/{dest_id}", destination_api_key, json=body)
        ok = status == 200
    elif name.isdigit():
        # An all-digit identifier would be read as an ID, so create with POST.
        status, resp, text = api_call("POST", SNIPPETS_PATH, destination_api_key, json={"name": name, **body})
        ok = status == 201
    else:
        status, resp, text = api_call("PUT", f"{SNIPPETS_PATH}/{quote(name, safe='')}", destination_api_key, json=body)
        ok = status in (200, 201)

    if not ok:
        raise MigrationError(f"HTTP {status}: {detail(resp, text)}")
    return find_snippet_id(resp)


def main() -> None:
    if _missing_key(origin_api_key) or _missing_key(destination_api_key):
        print("Missing API keys. Set origin_api_key and destination_api_key at the top of this file.")
        return

    os.makedirs(dirname, exist_ok=True)
    print(f"{'DRY RUN - ' if DRY_RUN else ''}Reading snippets from both projects...")
    source_all = fetch_snippets(origin_api_key, "Origin")
    dest_by_name = {s["name"]: s for s in fetch_snippets(destination_api_key, "Destination")}

    selected: List[Dict[str, Any]] = []
    filtered_out = 0
    for snippet in sorted(source_all, key=lambda s: s["name"].lower()):
        name = snippet["name"]
        if (SNIPPET_NAMES_TO_MIGRATE and name not in SNIPPET_NAMES_TO_MIGRATE) or name in SNIPPET_NAMES_TO_SKIP:
            filtered_out += 1
            continue
        selected.append(snippet)

    missing = sorted(set(SNIPPET_NAMES_TO_MIGRATE) - {s["name"] for s in source_all})
    for name in missing:
        print(f"WARNING: '{name}' is in SNIPPET_NAMES_TO_MIGRATE but does not exist in the origin project.")

    print(f"Origin: {len(source_all)} snippet(s); selected {len(selected)}, filtered out {filtered_out}. "
          f"Destination already has {len(dest_by_name)}.\n")

    state: Dict[str, Any] = load_json(MAPPING_PATH, {})
    results: Dict[str, Dict[str, Any]] = {}
    pending: List[Tuple[Dict[str, Any], str]] = []

    for snippet in selected:
        name = snippet["name"]
        if not VALID_NAME.match(name):
            results[name] = {"srcId": snippet.get("id"), "status": "failed",
                             "error": "name is not valid in the destination (100 chars max: letters, digits, - _ and spaces)"}
            continue
        action = plan_snippet(snippet, dest_by_name)
        if action in ("unchanged", "skip_existing"):
            results[name] = {"srcId": snippet.get("id"), "destId": dest_by_name[name]["id"], "status": action}
        else:
            pending.append((snippet, action))

    if DRY_RUN:
        for snippet, action in pending:
            results[snippet["name"]] = {"srcId": snippet.get("id"), "status": f"would_{action}"}
    else:
        last_error: Dict[str, str] = {}
        for pass_no in range(1, MAX_PASSES + 1):
            if not pending:
                break
            print(f"Pass {pass_no}: {len(pending)} snippet(s)")
            still_failing: List[Tuple[Dict[str, Any], str]] = []
            for snippet, action in pending:
                name = snippet["name"]
                try:
                    dest_id = write_snippet(snippet, action, dest_by_name)
                    results[name] = {"srcId": snippet.get("id"), "destId": dest_id,
                                     "status": "created" if action == "create" else "updated"}
                    print(f"  OK      {name} ({results[name]['status']})")
                except MigrationError as exc:
                    last_error[name] = str(exc)
                    still_failing.append((snippet, action))
                except requests.RequestException as exc:
                    last_error[name] = f"network error: {exc}"
                    still_failing.append((snippet, action))
            progressed = len(still_failing) < len(pending)
            pending = still_failing
            if pending and not progressed:
                print("  No progress this pass; stopping retries.")
                break
        for snippet, _ in pending:
            name = snippet["name"]
            results[name] = {"srcId": snippet.get("id"), "status": "failed", "error": last_error.get(name, "unknown")}

        # Fill in destination IDs the create/update response did not include.
        final_dest = {s["name"]: s["id"] for s in fetch_snippets(destination_api_key, "Destination")}
        for name, entry in results.items():
            if entry["status"] in ("created", "updated") and entry.get("destId") is None:
                entry["destId"] = final_dest.get(name)

        state.update({n: {**e, "migratedAt": datetime.now(timezone.utc).isoformat()}
                      for n, e in results.items() if e["status"] in ("created", "updated", "unchanged")})
        save_json(MAPPING_PATH, state)

    for name, entry in sorted(results.items()):
        if entry["status"] == "failed":
            log_error(f"snippet '{name}': {entry['error']}")
            print(f"  FAILED  {name}: {entry['error']}")
        elif DRY_RUN:
            print(f"  {entry['status'].upper():<14} {name}")

    counts: Dict[str, int] = {}
    for entry in results.values():
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    counts["filtered_out"] = filtered_out
    save_json(SUMMARY_PATH, {
        "finishedAt": datetime.now(timezone.utc).isoformat(),
        "dryRun": DRY_RUN,
        "counts": counts,
        "results": results,
    })
    print(f"\nDone: {counts}")
    if not DRY_RUN:
        print(f"Mapping: {MAPPING_PATH}")
    print(f"Summary: {SUMMARY_PATH}")
    if counts.get("failed"):
        print(f"Failures logged in: {ERROR_LOG_PATH}")


if __name__ == "__main__":
    main()
