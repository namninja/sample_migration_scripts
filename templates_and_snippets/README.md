# Template and Snippet Migration Scripts

Python scripts that copy **snippets** and **Email, SMS, Push and In-App templates** from one Iterable project to another through the public REST API.

| Script | Copies | Source read | Destination write |
|---|---|---|---|
| `bulk_migrate_snippets.py` | Snippets | `GET /api/snippets` | `PUT /api/snippets/{name}` (or `POST` for all-digit names) |
| `bulk_migrate_email_templates.py` | Email templates | `GET /api/templates/email/get` | `POST /api/templates/email/upsert` |
| `bulk_migrate_sms_templates.py` | SMS templates | `GET /api/templates/sms/get` | `POST /api/templates/sms/upsert` |
| `bulk_migrate_push_templates.py` | Push templates | `GET /api/templates/push/get` | `POST /api/templates/push/upsert` |
| `bulk_migrate_inapp_templates.py` | In-App templates | `GET /api/templates/inapp/get` | `POST /api/templates/inapp/upsert` |

> **Drag and Drop templates: read this first.** These scripts convert a Drag and Drop template to an **HTML template**. The Drag and Drop design is lost, and the template is edited in the WYSIWYG (HTML) editor afterward.
>
> - **Convert to HTML:** use these scripts. Works for the same org or a different org.
> - **Keep Drag and Drop:** do not use these scripts. Clone each template manually, project to project, in the Iterable UI. This is **only possible when the destination project is in the same org as the source project.**
>
> See the [Drag and Drop options](../README.md#drag-and-drop-templates-choose-your-option-first) in the main README. Templates built in the HTML editor are not affected.

## Requirements

- Python 3.8+
- `pip install requests`
- A server-side API key for the source project and one for the destination project.
- For snippets: the `SnippetsApi` feature flag must be on for **both** projects. If it is off, the API returns HTTP 403 `Snippets API not enabled for this project`. Ask Iterable to enable it.

## How to run

The scripts have no command-line arguments. Open the script in your IDE (Spyder, PyCharm, VS Code), edit the settings block at the top, and run it.

1. Set `origin_api_key` and `destination_api_key`. A key still set to a `YOUR_...` placeholder is treated as missing and the script stops.
2. Set what to migrate (`TEMPLATE_IDS`, or the snippet name filters).
3. Fill in the mappings (see below).
4. Run with `DRY_RUN = True` (the default). It reads the source and prints what would be sent, plus every mapping warning. Nothing is written to the destination.
5. Set `DRY_RUN = False` and migrate one or two items. Check them in the destination project.
6. Add the rest and run again.

## Recommended order

1. **Snippets** first. A template that references a snippet missing from the destination fails to save.
2. Create message types, senders, data feeds and catalogs in the destination.
3. Templates (any channel, any order).

## Settings

### Common to all template scripts

| Setting | Purpose |
|---|---|
| `TEMPLATE_IDS` | Source template IDs to migrate. Duplicates are ignored. |
| `MESSAGE_TYPE_MAP` | `{source messageTypeId: destination messageTypeId}`. Message type IDs differ between projects. |
| `DEFAULT_MESSAGE_TYPE` | Destination message type used when a source one is not in the map. Logs a warning. `None` fails the template instead. |
| `STRICT_MAPPINGS` | `True` fails any template with an unmapped message type or data feed, instead of falling back or dropping it. |
| `CREATOR_USER_ID` | Optional email of a destination project member to record as creator. `None` uses the default. |
| `SKIP_ALREADY_MIGRATED` | Skip templates already marked `success` in `template_mapping.json`. Set `False` to re-push changed templates. |
| `DRY_RUN` | Read-only preview. Default `True`. |
| `REQUEST_TIMEOUT`, `REQUEST_DELAY_SEC`, `MAX_RETRIES` | HTTP behavior. 429, 5xx and connection errors are retried with backoff. |

### Channel-specific

| Script | Setting | Purpose |
|---|---|---|
| Email, Push | `MIGRATE_DATA_FEEDS`, `DATA_FEED_MAP` | Set `True` only after the feeds exist in the destination. Unmapped feeds are dropped with a warning (or fail under `STRICT_MAPPINGS`). |
| Email | `COPY_SENDER_INFO` | Copies `fromName`, `fromEmail`, `replyToEmail`. Fails the template if the sender is not authorized in the destination. Default `False`. |
| Email, SMS | `COPY_GA_INFO`, `COPY_LINK_PARAMS` | Copies Google Analytics campaign name and link params. |
| SMS | `COPY_TRACKING_DOMAIN` | Copies the "shorten and track" domain. Default `False`, because the same domain must exist in the destination. |

### Snippets

| Setting | Purpose |
|---|---|
| `SNIPPET_NAMES_TO_MIGRATE` | Allow-list of snippet names (case-sensitive). Empty list means all. |
| `SNIPPET_NAMES_TO_SKIP` | Block-list of names to skip. |
| `OVERWRITE_EXISTING` | `True` updates a destination snippet that has the same name. `False` only creates new ones. |
| `CREATED_BY_USER_ID` | Optional email of a destination project member. `None` records the project creator. |
| `MAX_PASSES` | Number of retry passes for snippets that include other snippets. |

## How it works

- **Idempotent.** Templates are upserted with `clientTemplateId` set to the source template ID, so re-running updates the same destination template instead of duplicating it. Snippets are created or updated by name. The upsert is a partial update: only the fields sent are changed.
- **Blank-template guard.** The template API creates an empty template first and then applies and validates the content. If the second step fails, a blank template is left behind. The scripts check the message type and data feed mappings before sending, so those failures happen before any write. Other causes, such as an unauthorized sender or a missing snippet, are only found by the API. If one still occurs, fix the cause and re-run. The same template is updated in place.
- **Snippet dependencies.** A snippet can include another snippet, and the destination validates that on save. Failed snippets are retried in further passes, and the run stops early if a pass makes no progress.
- **Unchanged snippets are skipped.** If content, description and variables already match the destination, the snippet is not rewritten, so its updated timestamp is not touched.
- **Nothing is deleted.** Items that exist only in the destination are left alone.

## Output files

Each script writes to a `migrate_<type>/` folder next to the script:

| File | Content |
|---|---|
| `template_mapping.json` (or `snippet_mapping.json`) | Source ID to destination ID(s) and result |
| `run_summary.json` | Counts, failures and warnings for the last run |
| `errors.log` | One line per failure |

Dry runs write no mapping file.

## Known limitations

- **Drag and Drop is lost.** Templates written through the API are HTML templates. The Drag and Drop editor design is not preserved, and the template is edited in the WYSIWYG editor afterward. To keep a template as Drag and Drop, clone it manually in the Iterable UI instead of using these scripts. That works only when the destination project is in the same org as the source.
- **Images are not re-hosted.** Image URLs keep pointing at the source project's CDN. They keep working, but the files are not in the destination File Manager.
- **Locales are not handled.** Only the default locale is copied.
- **Catalogs and data feeds must already exist** in the destination. References are copied as-is and only render once the destination has them.
- **Not available through the API** (set these manually):
  - Email: "Use data feed alias".
  - In-App: some editor options and the "Show in mobile inbox" toggle. The inbox title, subtitle and icon are copied.
- **Push Open Action.** The script copies `deeplink`. Confirm after your first test migration that the Open Action looks right in the destination.
- **In-App expiration.** An absolute `expirationDateTime` in the past makes the migrated template expire immediately. The script warns when it sees one.
- **Toggling a `COPY_*` flag from `True` to `False`** after a test run does not clear fields already written. Delete the test templates first.
- **Snippets:**
  - Names are immutable and are never renamed.
  - A name is up to 100 characters of letters, digits, hyphens, underscores and spaces.
  - The destination rejects invalid Handlebars, `<script>` tags with JavaScript sources, inline event handlers such as `onload=`, and `javascript:` URLs. Those snippets are reported as failures.
  - Creator and created/updated timestamps are not copied.

## Security

Never commit real API keys, template IDs or exported data. The scripts ship with placeholder keys. Keep keys out of version control, and clear them before sharing a script.
