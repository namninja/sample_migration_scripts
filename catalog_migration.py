#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable Catalog Migration
- Lists all catalogs in the source project (or a specific allow-list)
- For each catalog:
    1. Creates the catalog in the destination project
    2. Copies field mappings (GET source → PUT destination)
    3. Paginates through all items and bulk-upserts them to the destination
- Writes a run_summary.json and error log on completion
- Resumable: already-existing catalogs and already-upserted pages are skipped
  via catalog_state.json
"""

import os, sys, json, time, threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.exceptions import RequestException

###############################################################################
# MIGRATION SETTINGS — edit the sections below
###############################################################################
_CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# ── WORKING DIRECTORY ─────────────────────────────────────────────────────────
dirname = os.path.join(_CONFIG_DIR, "migrate_catalogs")

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


# ── CATALOG FILTER ────────────────────────────────────────────────────────────
# Allow-list: migrate only these catalog names. Empty list = migrate ALL catalogs.
CATALOG_NAMES_TO_MIGRATE: List[str] = []

# Block-list: skip these catalog names even if they appear in the source.
CATALOG_NAMES_TO_SKIP: List[str] = []

# ── CATALOG RENAME MAP ────────────────────────────────────────────────────────
# Override the destination catalog name for specific source catalogs.
# Catalogs not listed here keep their original name in the destination.
CATALOG_NAME_MAP: Dict[str, str] = {
    # "SourceCatalogName": "DestinationCatalogName",
}

# ── ITEMS SETTINGS ────────────────────────────────────────────────────────────
# Items per GET /items page. Max observed to work well: 200.
ITEMS_PAGE_SIZE = 200

# Items per POST /items bulk-upsert call. Hard max is 1000.
UPSERT_BATCH_SIZE = 500

# When True, only the fields included in the upsert payload are updated on
# existing items (other fields are preserved). When False, the full item is
# overwritten. Set to True if you've already partially migrated.
REPLACE_UPLOADED_FIELDS_ONLY = False

# ── FIELD MAPPINGS ────────────────────────────────────────────────────────────
# When True, field mappings are NOT set on the destination (useful if the
# destination catalog already has correct mappings or you want to set them
# manually after reviewing).
SKIP_FIELD_MAPPINGS = False

# ── RUN OPTIONS ───────────────────────────────────────────────────────────────
# When True, all API writes (create catalog, set mappings, upsert items) are
# skipped — only reads and logging occur.
DRY_RUN = False

# Wipe catalog_state.json and error log before the run.
DELETE_STATE_ON_START = True

# ── RATE LIMITS ───────────────────────────────────────────────────────────────
# Max write requests per second (create, field mappings, upsert).
# Catalog API allows 100 rps; 10 is a safe default to avoid burst throttling.
RATE_LIMIT_RPS = 10

retry_config = {
    "stop_max_attempt_number":     8,
    "wait_exponential_multiplier": 1000,  # ms; doubles each retry
    "wait_exponential_max":        30000,
}

# ── TIMEOUTS ──────────────────────────────────────────────────────────────────
LIST_TIMEOUT   = 30
UPSERT_TIMEOUT = 60

###############################################################################
# Constants
###############################################################################

CATALOG_LIST_ENDPOINT       = "/api/catalogs"
CATALOG_CREATE_ENDPOINT     = "/api/catalogs/{catalogName}"
CATALOG_MAPPINGS_ENDPOINT   = "/api/catalogs/{catalogName}/fieldMappings"
CATALOG_ITEMS_ENDPOINT      = "/api/catalogs/{catalogName}/items"

DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "Accept":       "application/json",
}

###############################################################################
# File paths (all relative to dirname)
###############################################################################
os.makedirs(dirname, exist_ok=True)

STATE_PATH      = os.path.join(dirname, "catalog_state.json")
ERROR_LOG_PATH  = os.path.join(dirname, "catalog_errors.log")
SUMMARY_PATH    = os.path.join(dirname, "run_summary.json")

###############################################################################
# State (persisted to catalog_state.json between runs)
###############################################################################
# state[catalog_name] = {
#   "created": bool,
#   "mappings_set": bool,
#   "completed_pages": [1, 2, 3, ...],
#   "items_done": bool,
# }
_state: Dict[str, Any] = {}

def _load_state():
    global _state
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, encoding="utf-8") as f:
                _state = json.load(f)
        except Exception:
            _state = {}

def _save_state():
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(_state, f, indent=2)

def _catalog_state(name: str) -> Dict[str, Any]:
    if name not in _state:
        _state[name] = {
            "created":         False,
            "mappings_set":    False,
            "completed_pages": [],
            "items_done":      False,
        }
    return _state[name]

###############################################################################
# Logging
###############################################################################
_error_count = 0
_error_lock  = threading.Lock()

def log_error(catalog: str, msg: str):
    global _error_count
    with _error_lock:
        _error_count += 1
        ts = datetime.now(timezone.utc).isoformat()
        line = f"[{ts}] [{catalog}] {msg}\n"
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line)
        print(f"  [ERROR] {line.rstrip()}", flush=True)

###############################################################################
# HTTP session
###############################################################################
def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=retry_config["stop_max_attempt_number"],
        backoff_factor=retry_config["wait_exponential_multiplier"] / 1000,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

session = _build_session()

###############################################################################
# Rate limiter (token-bucket)
###############################################################################
_rate_lock     = threading.Lock()
_tokens        = float(RATE_LIMIT_RPS)
_last_refill   = time.monotonic()

def _acquire_token():
    global _tokens, _last_refill
    with _rate_lock:
        now = time.monotonic()
        elapsed = now - _last_refill
        _tokens = min(RATE_LIMIT_RPS, _tokens + elapsed * RATE_LIMIT_RPS)
        _last_refill = now
        if _tokens < 1:
            sleep_for = (1 - _tokens) / RATE_LIMIT_RPS
        else:
            sleep_for = 0
        _tokens -= 1
    if sleep_for > 0:
        time.sleep(sleep_for)

###############################################################################
# Catalog name helpers
###############################################################################

def _dest_name(source_name: str) -> str:
    """Return the destination catalog name for a given source catalog name."""
    return CATALOG_NAME_MAP.get(source_name, source_name)


def _label(source_name: str) -> str:
    """Return a log-friendly label showing rename when applicable."""
    dest = _dest_name(source_name)
    return f"{source_name} → {dest}" if dest != source_name else source_name


###############################################################################
# Catalog list
###############################################################################

def list_all_catalogs() -> List[str]:
    """Return all catalog names from the source project."""
    names: List[str] = []
    page = 1
    page_size = 100  # max per docs MCP schema
    while True:
        r = session.get(
            f"{origin_base_url}{CATALOG_LIST_ENDPOINT}",
            headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS},
            params={"page": page, "pageSize": page_size},
            timeout=LIST_TIMEOUT,
        )
        r.raise_for_status()
        data = r.json()
        params = data.get("params") or data  # unwrap params envelope
        batch = [c["name"] for c in params.get("catalogNames", [])]
        names.extend(batch)
        total = params.get("totalCatalogsCount", 0)
        print(f"  Listed page {page}: {len(batch)} catalogs (total={total})", flush=True)
        if len(names) >= total or not batch:
            break
        page += 1
    return names


###############################################################################
# Catalog create
###############################################################################

def create_catalog(source_name: str) -> bool:
    """
    Create the catalog in the destination using the (possibly renamed) dest name.
    Returns True on success or if it already exists, False on unrecoverable error.
    """
    name = _dest_name(source_name)
    if DRY_RUN:
        print(f"  [dry-run] Would create catalog: {_label(source_name)}", flush=True)
        return True
    _acquire_token()
    url = f"{destination_base_url}{CATALOG_CREATE_ENDPOINT.format(catalogName=name)}"
    r = session.post(
        url,
        headers={"Api-Key": destination_api_key, **DEFAULT_HEADERS},
        timeout=LIST_TIMEOUT,
    )
    if r.status_code == 201:
        print(f"  Created catalog: {_label(source_name)}", flush=True)
        return True
    if r.status_code in (400, 409):
        body = r.text or ""
        if "exist" in body.lower() or "duplicate" in body.lower() or "conflict" in body.lower():
            print(f"  Catalog already exists (skipping create): {_label(source_name)}", flush=True)
            return True
        print(f"  Catalog create returned {r.status_code} — assuming already exists: {body!r}", flush=True)
        return True
    log_error(source_name, f"create_catalog HTTP {r.status_code}: {r.text}")
    return False


###############################################################################
# Field mappings — scan-and-classify
###############################################################################

def _infer_type(value: Any) -> Optional[str]:
    """Infer an Iterable catalog field type from a Python value."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "long"
    if isinstance(value, float):
        return "double"
    if isinstance(value, str):
        return "string"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        # Find first non-None element to determine element type
        first = next((v for v in value if v is not None), None)
        if first is None:
            return None
        if isinstance(first, dict):
            return "object"
        if isinstance(first, bool):
            return "boolean"
        if isinstance(first, int):
            return "long"
        if isinstance(first, float):
            return "double"
        if isinstance(first, str):
            return "string"
    return None


def build_mappings_for_catalog(name: str) -> Optional[List[Dict[str, Any]]]:
    """
    Build the full mappingsUpdates list for a catalog by:

    1. GET /fieldMappings  → defined types (including dotted sub-field paths)
                           + undefinedFields list (seen in items, not yet typed)
    2. Separate top-level fields from dotted sub-field paths.
       Build children_map: { parentField: [{fieldName, fieldType}] } from
       one-level-deep dotted keys.
    3. Identify:
       - unresolved_objects  : top-level fields typed "object" — need plain vs array classification
       - unresolved_undefined: undefinedFields from the API response — need type inference
    4. Paginate source items, stopping as soon as both sets are empty.
       Also discovers fields not in either set (new, completely unmapped fields).
    5. Assemble mappingsUpdates:
       - Plain object  → { fieldName, fieldType: "object", children: [...] }
       - Array/unknown → { fieldName, fieldType: "object" }              (no children)
       - Primitive     → { fieldName, fieldType }
       - Inferred new  → appended at the end
    """
    # ── 1. Fetch current field mappings ───────────────────────────────────────
    url = f"{origin_base_url}{CATALOG_MAPPINGS_ENDPOINT.format(catalogName=name)}"
    r = session.get(
        url,
        headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS},
        timeout=LIST_TIMEOUT,
    )
    if r.status_code == 404:
        print(f"  No field mappings found for: {name}", flush=True)
        return []
    r.raise_for_status()
    body   = r.json()
    params = body.get("params") or body
    defined: Dict[str, str]    = params.get("definedMappings") or {}
    undefined_fields: List[str] = params.get("undefinedFields") or []

    # ── 2. Separate top-level keys from dotted sub-field paths ────────────────
    top_level: Dict[str, str] = {k: v for k, v in defined.items() if "." not in k}

    # children_map: one level deep only (e.g. "rooms.status" → parent="rooms", child="status")
    # Paths like "a.b.c" (two dots) are deeper nesting we can't set via the API — skip them.
    children_map: Dict[str, List[Dict[str, str]]] = {}
    for dotted_key, field_type in defined.items():
        if "." not in dotted_key:
            continue
        parent, child = dotted_key.split(".", 1)
        if "." in child:
            continue  # skip deeper than one level
        children_map.setdefault(parent, []).append(
            {"fieldName": child, "fieldType": field_type}
        )

    # ── 3. Identify what needs resolving ──────────────────────────────────────
    unresolved_objects: set    = {k for k, v in top_level.items() if v == "object"}
    unresolved_undefined: set  = set(undefined_fields)
    object_classification: Dict[str, str] = {}  # "plain" | "array"
    inferred_types: Dict[str, str]        = {}  # field_name → inferred type string

    needs_scan = bool(unresolved_objects or unresolved_undefined)
    if needs_scan:
        print(
            f"  Scanning items: {len(unresolved_objects)} object field(s) to classify, "
            f"{len(unresolved_undefined)} undefined field(s) to type — for: {name}",
            flush=True,
        )

    # ── 4. Paginate items until resolved ──────────────────────────────────────
    if needs_scan:
        page = 1
        while True:
            r = session.get(
                f"{origin_base_url}{CATALOG_ITEMS_ENDPOINT.format(catalogName=name)}",
                headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS},
                params={"page": page, "pageSize": ITEMS_PAGE_SIZE},
                timeout=LIST_TIMEOUT,
            )
            r.raise_for_status()
            page_data   = r.json()
            page_params = page_data.get("params") or page_data
            raw_items   = page_params.get("catalogItemsWithProperties") or []

            if not raw_items:
                break

            for item in raw_items:
                item_value = item.get("value") or {}

                # Classify unresolved object fields
                for field_name in list(unresolved_objects):
                    val = item_value.get(field_name)
                    if val is None:
                        continue
                    object_classification[field_name] = (
                        "array" if isinstance(val, list) else "plain"
                    )
                    unresolved_objects.discard(field_name)

                # Infer types for undefined fields
                for field_name in list(unresolved_undefined):
                    val = item_value.get(field_name)
                    inferred = _infer_type(val)
                    if inferred:
                        inferred_types[field_name] = inferred
                        unresolved_undefined.discard(field_name)

                # Catch completely new fields not in defined or undefined lists
                for field_name, val in item_value.items():
                    if "." in field_name:
                        continue
                    if field_name in top_level or field_name in inferred_types:
                        continue
                    if field_name in unresolved_undefined:
                        continue
                    inferred = _infer_type(val)
                    if inferred:
                        inferred_types[field_name] = inferred

                if not unresolved_objects and not unresolved_undefined:
                    break  # all resolved — stop scanning items on this page

            total          = page_params.get("totalItemsCount", 0)
            fetched_so_far = (page - 1) * ITEMS_PAGE_SIZE + len(raw_items)
            remaining_obj  = len(unresolved_objects)
            remaining_und  = len(unresolved_undefined)
            print(
                f"  Scan page {page}: {len(raw_items)} items "
                f"| still unresolved — objects: {remaining_obj}, undefined: {remaining_und}",
                flush=True,
            )

            if not unresolved_objects and not unresolved_undefined:
                break
            if fetched_so_far >= total or not page_params.get("nextPageUrl"):
                break
            page += 1

        if unresolved_objects:
            print(
                f"  [warn] {len(unresolved_objects)} object field(s) had no non-null values "
                f"across all items — submitting without children: {sorted(unresolved_objects)}",
                flush=True,
            )

    # ── 5. Build mappingsUpdates ───────────────────────────────────────────────
    mappings_updates: List[Dict[str, Any]] = []

    for field_name, field_type in top_level.items():
        if field_type == "object":
            if object_classification.get(field_name) == "plain" and field_name in children_map:
                mappings_updates.append({
                    "fieldName": field_name,
                    "fieldType": "object",
                    "children":  children_map[field_name],
                })
            else:
                # Array of objects, unknown, or no children available — no children
                mappings_updates.append({"fieldName": field_name, "fieldType": "object"})
        else:
            mappings_updates.append({"fieldName": field_name, "fieldType": field_type})

    # Append inferred types for undefined / newly discovered fields
    for field_name, field_type in inferred_types.items():
        if field_name not in top_level:
            mappings_updates.append({"fieldName": field_name, "fieldType": field_type})
            print(f"  Discovered field: {field_name} → {field_type}", flush=True)

    return mappings_updates


def set_field_mappings(source_name: str, mappings_updates: List[Dict[str, Any]]) -> bool:
    """Apply field mappings to the destination catalog."""
    dest = _dest_name(source_name)
    if not mappings_updates:
        print(f"  No mappings to set for: {_label(source_name)}", flush=True)
        return True
    if DRY_RUN:
        print(f"  [dry-run] Would set {len(mappings_updates)} field mappings for: {_label(source_name)}", flush=True)
        return True
    _acquire_token()
    url = f"{destination_base_url}{CATALOG_MAPPINGS_ENDPOINT.format(catalogName=dest)}"
    r = session.put(
        url,
        headers={"Api-Key": destination_api_key, **DEFAULT_HEADERS},
        json={"mappingsUpdates": mappings_updates},
        timeout=LIST_TIMEOUT,
    )
    if r.ok:
        print(f"  Set {len(mappings_updates)} field mappings for: {_label(source_name)}", flush=True)
        return True
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
    log_error(source_name, f"set_field_mappings HTTP {r.status_code}: {body}")
    return False


###############################################################################
# Items — paginated read
###############################################################################

def iter_item_pages(catalog_name: str):
    """
    Generator — yields (page_number, items_dict) where items_dict is:
      { itemId: { field: value, ... } }
    ready to pass directly to the bulk upsert body as `documents`.
    """
    page = 1
    while True:
        r = session.get(
            f"{origin_base_url}{CATALOG_ITEMS_ENDPOINT.format(catalogName=catalog_name)}",
            headers={"Api-Key": origin_api_key, **DEFAULT_HEADERS},
            params={"page": page, "pageSize": ITEMS_PAGE_SIZE},
            timeout=LIST_TIMEOUT,
        )
        r.raise_for_status()
        data   = r.json()
        params = data.get("params") or data  # unwrap params envelope
        raw_items = params.get("catalogItemsWithProperties") or []

        if not raw_items:
            break

        items_dict: Dict[str, Any] = {}
        for item in raw_items:
            item_id    = item.get("itemId")
            item_value = item.get("value") or {}
            if item_id:
                items_dict[item_id] = item_value

        yield page, items_dict

        total = params.get("totalItemsCount", 0)
        fetched_so_far = (page - 1) * ITEMS_PAGE_SIZE + len(raw_items)
        if fetched_so_far >= total or not params.get("nextPageUrl"):
            break
        page += 1


###############################################################################
# Items — bulk upsert
###############################################################################

def upsert_items_batch(source_name: str, documents: Dict[str, Any]) -> bool:
    """
    POST up to UPSERT_BATCH_SIZE items to the destination catalog (using dest name).
    Returns True on 202 (accepted), False otherwise.
    """
    dest = _dest_name(source_name)
    if DRY_RUN:
        print(f"    [dry-run] Would upsert {len(documents)} items to: {_label(source_name)}", flush=True)
        return True
    _acquire_token()
    url = f"{destination_base_url}{CATALOG_ITEMS_ENDPOINT.format(catalogName=dest)}"
    r = session.post(
        url,
        headers={"Api-Key": destination_api_key, **DEFAULT_HEADERS},
        json={
            "documents":                documents,
            "replaceUploadedFieldsOnly": REPLACE_UPLOADED_FIELDS_ONLY,
        },
        timeout=UPSERT_TIMEOUT,
    )
    if r.status_code == 202:
        return True
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
    log_error(source_name, f"upsert_items HTTP {r.status_code}: {body}")
    return False


def migrate_items(catalog_name: str) -> Tuple[int, int]:
    """
    Paginate through all source items and upsert them to the destination.
    Skips pages already recorded as completed in the state file.
    Returns (total_items_upserted, pages_processed).
    """
    cstate = _catalog_state(catalog_name)
    completed_pages: List[int] = cstate.get("completed_pages") or []

    total_items   = 0
    pages_done    = 0

    for page_num, items_dict in iter_item_pages(catalog_name):
        if page_num in completed_pages:
            print(f"  [skip] Page {page_num} already migrated for: {catalog_name}", flush=True)
            continue

        # Split into sub-batches of UPSERT_BATCH_SIZE (page may be larger than max)
        item_ids   = list(items_dict.keys())
        sub_batches = [
            {k: items_dict[k] for k in item_ids[i:i + UPSERT_BATCH_SIZE]}
            for i in range(0, len(item_ids), UPSERT_BATCH_SIZE)
        ]

        page_ok = True
        for batch in sub_batches:
            ok = upsert_items_batch(catalog_name, batch)
            if ok:
                total_items += len(batch)
            else:
                page_ok = False

        if page_ok:
            completed_pages.append(page_num)
            cstate["completed_pages"] = completed_pages
            _save_state()

        pages_done += 1
        print(
            f"  Page {page_num}: upserted {len(items_dict)} items "
            f"for: {catalog_name} (running total: {total_items})",
            flush=True,
        )

    return total_items, pages_done


###############################################################################
# Per-catalog migration
###############################################################################

def migrate_catalog(name: str) -> Dict[str, Any]:
    """
    Run the full migration pipeline for a single catalog.
    Returns a result dict for the summary report.
    """
    result: Dict[str, Any] = {
        "catalog":          name,
        "created":          False,
        "mappings_set":     False,
        "items_upserted":   0,
        "pages_processed":  0,
        "errors":           0,
        "skipped":          False,
    }
    cstate = _catalog_state(name)
    errors_before = _error_count

    print(f"\n{'─' * 60}", flush=True)
    print(f"Migrating catalog: {_label(name)}", flush=True)

    # ── Step 1: create catalog ─────────────────────────────────────────────
    if not cstate.get("created"):
        ok = create_catalog(name)
        if not ok:
            result["errors"] += 1
            return result
        cstate["created"] = True
        _save_state()
    else:
        print(f"  [skip] Catalog already created: {name}", flush=True)
    result["created"] = True

    # ── Step 2: field mappings ─────────────────────────────────────────────
    if not cstate.get("mappings_set") and not SKIP_FIELD_MAPPINGS:
        mappings = build_mappings_for_catalog(name)
        if mappings is None:
            log_error(name, "Failed to fetch source field mappings")
            result["errors"] += 1
            # Continue anyway — items can still be migrated without mappings
        else:
            ok = set_field_mappings(name, mappings)
            if ok:
                cstate["mappings_set"] = True
                _save_state()
    else:
        print(f"  [skip] Field mappings already set (or SKIP_FIELD_MAPPINGS=True): {name}", flush=True)
    result["mappings_set"] = cstate.get("mappings_set", False)

    # ── Step 3: items ──────────────────────────────────────────────────────
    if not cstate.get("items_done"):
        try:
            items_count, pages = migrate_items(name)
        except RequestException as e:
            log_error(name, f"items pagination error: {e}")
            result["errors"] += 1
            return result

        result["items_upserted"]  = items_count
        result["pages_processed"] = pages

        if _error_count == errors_before:
            cstate["items_done"] = True
            _save_state()
            print(f"  Items migration complete: {items_count} items across {pages} pages", flush=True)
    else:
        print(f"  [skip] Items already fully migrated for: {name}", flush=True)
        result["skipped"] = True

    result["errors"] = _error_count - errors_before
    return result


###############################################################################
# Main
###############################################################################

def main():
    global _error_count

    print("=" * 60, flush=True)
    print("Iterable Catalog Migration", flush=True)
    print(f"  DRY_RUN = {DRY_RUN}", flush=True)
    print(f"  SKIP_FIELD_MAPPINGS = {SKIP_FIELD_MAPPINGS}", flush=True)
    print(f"  REPLACE_UPLOADED_FIELDS_ONLY = {REPLACE_UPLOADED_FIELDS_ONLY}", flush=True)
    print("=" * 60, flush=True)

    if DELETE_STATE_ON_START:
        for p in (STATE_PATH, ERROR_LOG_PATH):
            if os.path.exists(p):
                os.remove(p)
                print(f"Deleted: {p}", flush=True)

    _load_state()

    # ── List all source catalogs ─────────────────────────────────────────────
    print("\nFetching catalog list from source…", flush=True)
    try:
        all_catalogs = list_all_catalogs()
    except RequestException as e:
        print(f"[FATAL] Failed to list source catalogs: {e}", flush=True)
        sys.exit(1)

    print(f"Found {len(all_catalogs)} catalogs in source project", flush=True)

    # ── Apply filters ────────────────────────────────────────────────────────
    if CATALOG_NAMES_TO_MIGRATE:
        catalogs = [c for c in all_catalogs if c in CATALOG_NAMES_TO_MIGRATE]
        print(f"Allow-list applied: {len(catalogs)} catalogs selected", flush=True)
    else:
        catalogs = all_catalogs

    if CATALOG_NAMES_TO_SKIP:
        skip_set = set(CATALOG_NAMES_TO_SKIP)
        catalogs = [c for c in catalogs if c not in skip_set]
        print(f"Block-list applied: {len(catalogs)} catalogs remaining", flush=True)

    if not catalogs:
        print("No catalogs to migrate. Exiting.", flush=True)
        return

    # ── Migrate each catalog ─────────────────────────────────────────────────
    start_time = time.monotonic()
    results    = []
    for name in catalogs:
        r = migrate_catalog(name)
        results.append(r)

    elapsed = time.monotonic() - start_time

    # ── Summary ──────────────────────────────────────────────────────────────
    total_items  = sum(r["items_upserted"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    skipped      = sum(1 for r in results if r["skipped"])

    print("\n" + "=" * 60, flush=True)
    print("Migration complete", flush=True)
    print(f"  Catalogs processed : {len(catalogs)}", flush=True)
    print(f"  Items upserted     : {total_items}", flush=True)
    print(f"  Errors             : {total_errors}", flush=True)
    print(f"  Already-done skips : {skipped}", flush=True)
    print(f"  Elapsed            : {elapsed:.1f}s", flush=True)
    if total_errors:
        print(f"  Error log          : {ERROR_LOG_PATH}", flush=True)

    summary = {
        "run_at":               datetime.now(timezone.utc).isoformat(),
        "dry_run":              DRY_RUN,
        "elapsed_seconds":      round(elapsed, 1),
        "catalogs_processed":   len(catalogs),
        "total_items_upserted": total_items,
        "total_errors":         total_errors,
        "catalogs":             results,
    }
    with open(SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print(f"  Summary            : {SUMMARY_PATH}", flush=True)


if __name__ == "__main__":
    main()
