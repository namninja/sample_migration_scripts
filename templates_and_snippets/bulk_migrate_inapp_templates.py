#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Iterable In-App Template Migration
==================================
Copies In-App templates from one Iterable project to another as base templates.

    GET  /api/templates/inapp/get      (source)
    POST /api/templates/inapp/upsert   (destination)

HOW TO USE (IDE, e.g. Spyder / PyCharm / VS Code — no CLI arguments)
    1. Edit the MIGRATION SETTINGS block below.
    2. Run with DRY_RUN = True first. It reads the source and prints what would
       be sent, with every mapping warning, without writing anything.
    3. Migrate 1-2 templates for real and check them in the destination project.
    4. Add the rest of the template IDs and run again.

HOW IT WORKS
    * Idempotency: each upsert sets clientTemplateId to the SOURCE template ID.
      Re-running updates the same destination template instead of creating a
      duplicate. The upsert is a partial update (only the fields sent are
      changed), so it is safe to re-run after fixing a mapping.
    * Every run writes to <this folder>/migrate_inapp_templates/:
        template_mapping.json  source template ID -> destination template ID(s)
        run_summary.json       counts, failures and warnings for the last run
        errors.log             one line per failure

IMPORTANT
    1. Migrate snippets FIRST (see bulk_migrate_snippets.py). A template that
       references a snippet missing from the destination fails to save.
    2. The API creates an empty destination template and THEN applies the
       content. If the second step fails (bad message type, missing snippet...)
       a blank template is left behind. The script validates mappings before
       sending to avoid this. If it still happens, fix the cause and re-run: the
       same blank template is updated in place.
    3. Message types: map each one in MESSAGE_TYPE_MAP. DEFAULT_MESSAGE_TYPE is a
       fallback that logs a warning. Leave it None (or set STRICT_MAPPINGS) to
       fail the template instead of guessing.
    4. Images are not migrated. They keep working (they point at the source
       project's CDN URL) but are not in the destination File Manager. To reuse
       one, open the image URL and import it via File Manager > Import.
    5. These In-App editor options are NOT available through the API and must be
       set by hand in each destination template:
         a. Data feed and data feed options
         b. "Show in mobile inbox" toggle (the inbox title / subtitle / icon ARE
            copied via inboxMetadata, but the toggle itself must be switched on)
         c. Deliver silently
         d. Message format
    6. Expiration (expirationDateTime / expirationDuration) is copied when the
       source has it. An ABSOLUTE expirationDateTime in the past makes the
       migrated template expire immediately; the script warns when it sees one.
    7. Locales are not handled; only the default locale is copied.
    8. Catalog references are copied as-is. They render only after the same
       catalogs exist in the destination.
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
dirname = os.path.join(_CONFIG_DIR, "migrate_inapp_templates")

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
# Source In-App template IDs to migrate
TEMPLATE_IDS: List[int] = [1234567, 2345678]   # example IDs - replace

# ── MAPPINGS ──────────────────────────────────────────────────────────────────
# {source messageTypeId: destination messageTypeId}
MESSAGE_TYPE_MAP: Dict[int, int] = {
    111111: 222222,   # example: <source messageTypeId>: <destination messageTypeId>
}

# Destination message type used when a source message type is not in
# MESSAGE_TYPE_MAP. None = fail the template instead of guessing.
DEFAULT_MESSAGE_TYPE: Optional[int] = None

# True = any unmapped message type fails the template (nothing is sent) instead
# of falling back to DEFAULT_MESSAGE_TYPE with a warning.
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
GET_PATH        = "/api/templates/inapp/get"
UPSERT_PATH     = "/api/templates/inapp/upsert"

MAPPING_PATH    = os.path.join(dirname, "template_mapping.json")
SUMMARY_PATH    = os.path.join(dirname, "run_summary.json")
ERROR_LOG_PATH  = os.path.join(dirname, "errors.log")

# Copied from source to destination when present (not None).
CONTENT_FIELDS = [
    "name", "html", "inAppDisplaySettings", "webInAppDisplaySettings",
    "inboxMetadata", "payload", "expirationDateTime", "expirationDuration",
    "campaignDataFields",
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


def _is_past_utc(value: str) -> Optional[bool]:
    """True/False if `value` parses as 'YYYY-MM-DD HH:MM:SS' (UTC); None if it does not."""
    try:
        parsed = datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError):
        return None
    return parsed < datetime.now(timezone.utc)


###############################################################################
# Channel-specific payload
###############################################################################

def build_payload(template_id: int, src: Dict[str, Any], warnings: List[str]) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"clientTemplateId": str(template_id)}
    copy_present(src, payload, CONTENT_FIELDS)
    payload.setdefault("name", f"Migrated template {template_id}")

    expires_at = payload.get("expirationDateTime")
    if expires_at:
        if _is_past_utc(expires_at):
            warnings.append(f"expirationDateTime {expires_at} is in the past; the migrated template will be expired")
        elif _is_past_utc(expires_at) is None:
            warnings.append(f"expirationDateTime '{expires_at}' is not in 'YYYY-MM-DD HH:MM:SS' format; check it after migrating")

    payload["messageTypeId"] = map_message_type(src.get("messageTypeId"), warnings)
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

    print(f"{'DRY RUN - ' if DRY_RUN else ''}Migrating {len(ids)} in-app template(s)\n")
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
