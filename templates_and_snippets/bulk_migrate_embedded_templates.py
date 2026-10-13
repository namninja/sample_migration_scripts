#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable Embedded Template Migration
====================================
Copies Embedded message templates from one Iterable project to another.

    GET  /api/templates/embedded/get      (source)
    POST /api/templates/embedded/upsert   (destination)

HOW TO USE (IDE, e.g. Spyder / PyCharm / VS Code — no CLI arguments)
    1. Create the embedded placements in the destination project first (manual).
    2. Edit the MIGRATION SETTINGS block below, including PLACEMENT_MAP.
    3. Run with DRY_RUN = True first. It reads the source and prints what would
       be sent, with every mapping warning, without writing anything.
    4. Migrate 1-2 templates for real and check them in the destination project.
    5. Add the rest of the template IDs and run again.

HOW IT WORKS
    * Idempotency: each upsert sets clientTemplateId to the SOURCE template ID.
      Re-running updates the same destination template instead of creating a
      duplicate.
    * Every run writes to <this folder>/migrate_embedded_templates/:
        template_mapping.json  source template ID -> destination template ID(s)
        run_summary.json       counts, failures and warnings for the last run
        errors.log             one line per failure

IMPORTANT
    1. Placements are NOT migrated by this script and have different IDs in each
       project. Create them in the destination first, then map every source
       placement ID to its destination placement ID in PLACEMENT_MAP.
       The destination rejects a placementId that does not exist in its project.
       Because the API creates an empty template BEFORE it validates the
       content, a bad placement leaves a blank template behind. The script
       therefore refuses to send a template whose placement is not mapped.
    2. Migrate snippets FIRST (see bulk_migrate_snippets.py). A template that
       references a snippet missing from the destination fails to save.
    3. Message types: map each one in MESSAGE_TYPE_MAP. Use the destination's
       Embedded-channel message types. DEFAULT_MESSAGE_TYPE is a fallback that
       logs a warning. Leave it None (or set STRICT_MAPPINGS) to fail the
       template instead of guessing.
    4. The upsert overwrites title and body with whatever the request carries,
       so the script always sends both when the source has them. Elements
       (buttons, text, media, default action) and payload are copied as-is.
    5. Images (elements.mediaUrl) and button / default-action URLs are copied as
       URLs, not re-hosted. Image URLs keep pointing at the source project's CDN.
       Re-upload the images and update the URLs if the source project will be
       retired.
    6. Embedded templates cannot be attached to a data feed through the API.
       Data feed options must be set by hand in the destination template.
    7. Locales are not handled; only the default locale is copied.
    8. Catalog references are copied as-is. They render only after the same
       catalogs exist in the destination.
    9. Campaigns are not migrated. The campaigns API has no placementId field,
       so embedded campaigns must be rebuilt by hand in the destination.
"""

import json
import os
import re
import time
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
dirname = os.path.join(_CONFIG_DIR, "migrate_embedded_templates")

# ── API KEYS ──────────────────────────────────────────────────────────────────
# Server-side keys. Never commit real keys.
origin_api_key      = "YOUR_ORIGIN_API_KEY"      # example placeholder - replace
destination_api_key = "YOUR_DESTINATION_API_KEY" # example placeholder - replace

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

# ── WHAT TO MIGRATE ───────────────────────────────────────────────────────────
# Source Embedded template IDs to migrate
TEMPLATE_IDS: List[int] = [1234567, 2345678]   # example IDs - replace

# ── MAPPINGS ──────────────────────────────────────────────────────────────────
# {source placementId: destination placementId}. Create the placements in the
# destination project first. A template whose placement is not listed here
# fails before anything is sent.
PLACEMENT_MAP: Dict[int, int] = {
    1001: 2001,   # example: <source placementId>: <destination placementId>
}

# {source messageTypeId: destination messageTypeId}
MESSAGE_TYPE_MAP: Dict[int, int] = {
    111111: 222222,   # example: <source messageTypeId>: <destination messageTypeId>
}

# Destination message type used when a source message type is not in
# MESSAGE_TYPE_MAP. None = fail the template instead of guessing.
DEFAULT_MESSAGE_TYPE: Optional[int] = None

# True = any unmapped message type fails the template (nothing is sent) instead
# of falling back to DEFAULT_MESSAGE_TYPE with a warning.
# (An unmapped placement ALWAYS fails the template.)
STRICT_MAPPINGS = False

# ── OPTIONS ───────────────────────────────────────────────────────────────────
# Optional destination project member (email) to record as creator. None = default.
CREATOR_USER_ID: Optional[str] = None   # example: "teammate@example.com"

# Skip templates already marked "success" in template_mapping.json.
# Set False to re-push (update) templates you have changed in the source.
SKIP_ALREADY_MIGRATED = True

# Read the source and show what would be sent. Writes nothing to the destination.
DRY_RUN = True

# ── HTTP ──────────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT   = (10, 60)   # (connect, read) seconds
REQUEST_DELAY_SEC = 0.2        # pause before every API call
MAX_RETRIES       = 4          # for 429 / 5xx / connection errors

###############################################################################
# Derived settings — no edits needed below this line
###############################################################################
GET_PATH        = "/api/templates/embedded/get"
UPSERT_PATH     = "/api/templates/embedded/upsert"

MAPPING_PATH    = os.path.join(dirname, "template_mapping.json")
SUMMARY_PATH    = os.path.join(dirname, "run_summary.json")
ERROR_LOG_PATH  = os.path.join(dirname, "errors.log")

# Copied from source to destination when present (not None).
# title and body are always sent when the source has them: the upsert replaces
# them with whatever the request carries.
CONTENT_FIELDS = [
    "name", "title", "body", "elements", "payload", "campaignDataFields",
]

session = requests.Session()
_retry = Retry(
    total=MAX_RETRIES, connect=MAX_RETRIES, read=MAX_RETRIES, status=MAX_RETRIES,
    status_forcelist=[429, 500, 502, 503, 504],
    # POST is safe to retry here: the upsert is keyed by clientTemplateId.
    allowed_methods={"GET", "POST"},
    backoff_factor=1.5, respect_retry_after_header=True, raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=_retry))


class MigrationError(Exception):
    """A per-template failure. The run continues with the next template."""


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


def api_call(method: str, path: str, api_key: str, base_url: str, **kwargs) -> Tuple[int, Any, str]:
    """Returns (http_status, parsed_json_or_None, raw_text)."""
    time.sleep(REQUEST_DELAY_SEC)
    resp = session.request(method, base_url.rstrip("/") + path, headers=_headers(api_key),
                           timeout=REQUEST_TIMEOUT, **kwargs)
    return resp.status_code, _json_or_none(resp), resp.text


def detail(body: Any, text: str) -> str:
    if isinstance(body, dict) and body.get("msg"):
        return str(body["msg"])
    return (text or "").strip()[:300] or "(empty response)"


def parse_upsert_ids(body: Any, text: str) -> List[int]:
    """Destination IDs from an upsert response.

    The API answers {"msg": "Upserted N templates with IDs: 1, 2", ...} and, when
    a project flag is on, also {"params": {"templateIds": [1, 2]}}.
    """
    if isinstance(body, dict):
        params = body.get("params")
        if isinstance(params, dict) and params.get("templateIds"):
            return [int(i) for i in params["templateIds"]]
    msg = body.get("msg", "") if isinstance(body, dict) else (text or "")
    match = re.search(r"IDs:\s*([\d,\s]+)", msg)
    return [int(n) for n in re.findall(r"\d+", match.group(1))] if match else []


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


def copy_present(src: Dict[str, Any], dst: Dict[str, Any], keys: List[str]) -> None:
    for key in keys:
        if src.get(key) is not None:
            dst[key] = src[key]


def map_message_type(src_mt: Any, warnings: List[str]) -> int:
    if src_mt in MESSAGE_TYPE_MAP:
        return MESSAGE_TYPE_MAP[src_mt]
    if STRICT_MAPPINGS or DEFAULT_MESSAGE_TYPE is None:
        why = "STRICT_MAPPINGS is on" if STRICT_MAPPINGS else "DEFAULT_MESSAGE_TYPE is not set"
        raise MigrationError(f"source messageTypeId {src_mt} is not in MESSAGE_TYPE_MAP and {why}")
    warnings.append(f"messageTypeId {src_mt} not in MESSAGE_TYPE_MAP; used DEFAULT_MESSAGE_TYPE {DEFAULT_MESSAGE_TYPE}")
    return DEFAULT_MESSAGE_TYPE


def map_placement(src_placement: Any, warnings: List[str]) -> Optional[int]:
    """Destination placementId for a source placementId.

    A placement missing from PLACEMENT_MAP always fails: the destination checks
    the placement AFTER it has created an empty template, so sending an unknown
    placement would leave a blank template behind.
    """
    if src_placement is None:
        warnings.append("source template has no placementId; set the placement in the destination before using it")
        return None
    try:
        key = int(src_placement)
    except (TypeError, ValueError):
        raise MigrationError(f"source placementId {src_placement!r} is not a number")
    if key not in PLACEMENT_MAP:
        raise MigrationError(
            f"source placementId {key} is not in PLACEMENT_MAP. Create the placement in the destination "
            f"project, then map it. Nothing was sent, so no blank template was created"
        )
    return PLACEMENT_MAP[key]


###############################################################################
# Channel-specific payload
###############################################################################

def build_payload(template_id: int, src: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"clientTemplateId": str(template_id)}
    copy_present(src, payload, CONTENT_FIELDS)
    payload.setdefault("name", f"Migrated template {template_id}")

    # Resolve mappings before anything is sent, so a failure never leaves a blank template.
    placement = map_placement(src.get("placementId"), warnings)
    if placement is not None:
        payload["placementId"] = placement
    payload["messageTypeId"] = map_message_type(src.get("messageTypeId"), warnings)

    media_url = (payload.get("elements") or {}).get("mediaUrl")
    if media_url:
        warnings.append("elements.mediaUrl is copied as a URL and still points at the source project; re-upload the image if the source will be retired")
    if CREATOR_USER_ID:
        payload["creatorUserId"] = CREATOR_USER_ID
    return payload


###############################################################################
# Migration
###############################################################################

def migrate_one(template_id: int, state: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    prior = state.get(str(template_id))
    if SKIP_ALREADY_MIGRATED and not DRY_RUN and prior and prior.get("status") == "success":
        return "skipped", prior

    status, src, text = api_call("GET", GET_PATH, origin_api_key, origin_base_url, params={"templateId": template_id})
    if status != 200 or not isinstance(src, dict):
        raise MigrationError(f"source GET failed (HTTP {status}): {detail(src, text)}")

    warnings: List[str] = []
    payload = build_payload(template_id, src, warnings)

    if DRY_RUN:
        preview = {k: (f"<{len(v)} chars>" if isinstance(v, str) and len(v) > 80 else v)
                   for k, v in payload.items()}
        print(f"  [dry run] would upsert: {json.dumps(preview, ensure_ascii=False)}")
        return "dry_run", {"name": payload["name"], "warnings": warnings}

    status, body, text = api_call("POST", UPSERT_PATH, destination_api_key, destination_base_url, json=payload)
    if status != 200:
        raise MigrationError(
            f"upsert failed (HTTP {status}): {detail(body, text)}. A blank destination template may "
            f"have been created; fix the cause and re-run to update it in place."
        )

    dest_ids = parse_upsert_ids(body, text)
    if not dest_ids:
        warnings.append("upsert succeeded but the destination template ID could not be read from the response")
    return "success", {
        "name": payload["name"],
        "status": "success",
        "destTemplateIds": dest_ids,
        "warnings": warnings,
        "migratedAt": datetime.now(timezone.utc).isoformat(),
    }


def main() -> None:
    if _missing_key(origin_api_key) or (_missing_key(destination_api_key) and not DRY_RUN):
        print("Missing API keys. Set origin_api_key and destination_api_key at the top of this file.")
        return
    if not TEMPLATE_IDS:
        print("TEMPLATE_IDS is empty. Add the source template IDs to migrate.")
        return

    os.makedirs(dirname, exist_ok=True)
    state: Dict[str, Any] = load_json(MAPPING_PATH, {})
    results: Dict[str, List[Any]] = {"success": [], "failed": [], "skipped": [], "dry_run": []}
    ids = list(dict.fromkeys(TEMPLATE_IDS))  # de-duplicate, keep order

    print(f"{'DRY RUN - ' if DRY_RUN else ''}Migrating {len(ids)} embedded template(s)\n")
    for template_id in ids:
        try:
            outcome, entry = migrate_one(template_id, state)
        except MigrationError as exc:
            outcome, entry = "failed", {"error": str(exc)}
            log_error(f"template {template_id}: {exc}")
        except requests.RequestException as exc:
            outcome, entry = "failed", {"error": f"network error: {exc}"}
            log_error(f"template {template_id}: network error: {exc}")

        results[outcome].append({"templateId": template_id, **entry})
        if outcome == "success":
            state[str(template_id)] = entry
            save_json(MAPPING_PATH, state)  # persist after every success

        label = {"success": "OK     ", "failed": "FAILED ", "skipped": "SKIPPED", "dry_run": "DRY RUN"}[outcome]
        extra = entry.get("error") or (f"-> {entry['destTemplateIds']}" if entry.get("destTemplateIds") else "")
        print(f"{label} template {template_id}: {entry.get('name', '')} {extra}".rstrip())
        for warning in entry.get("warnings", []):
            print(f"         warning: {warning}")

    summary = {
        "finishedAt": datetime.now(timezone.utc).isoformat(),
        "dryRun": DRY_RUN,
        "counts": {k: len(v) for k, v in results.items()},
        "results": results,
    }
    save_json(SUMMARY_PATH, summary)
    print(f"\nDone: {summary['counts']}")
    print(f"Mapping: {MAPPING_PATH}\nSummary: {SUMMARY_PATH}")
    if results["failed"]:
        print(f"Failures logged in: {ERROR_LOG_PATH}")


if __name__ == "__main__":
    main()
