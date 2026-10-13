# sample_migration_scripts

Python scripts for migrating data between Iterable projects through the public REST API and the bulk export API. They are for large jobs that are impractical in the UI: user profiles, subscriptions, devices, event history, purchases, lists, catalogs, snippets and templates.

Every script is configured by editing variables at the top of the file in an IDE (Spyder, PyCharm, VS Code). There are no command-line arguments.

## Requirements

- Python 3.8+
- `pip install requests`
- A server-side API key for the source project and one for the destination project.

## Scripts

### Data and history (repo root)

| Script | Migrates | Key APIs |
|---|---|---|
| `user_and_subscriptions_migration.py` | User profiles, data fields, channel and message type subscriptions | Bulk user export, `/api/users/bulkUpdate`, `/api/users/bulkUpdateSubscriptions` |
| `devices_migration.py` | Android and iOS device tokens | Bulk user export, `/api/users/registerDeviceToken` |
| `custom_event_migration.py` | Custom event history | Bulk event export, `/api/events/trackBulk` |
| `purchase_migration.py` | Purchase history | Bulk export, `/api/commerce/trackPurchase` |
| `list_migration.py` | Standard, suppression and internal lists and their members | `/api/lists`, `/api/lists/subscribe` |
| `catalog_migration.py` | Catalogs, field mappings and items | Catalog API |

### Content: `templates_and_snippets/`

| Script | Migrates |
|---|---|
| `bulk_migrate_snippets.py` | Snippets |
| `bulk_migrate_email_templates.py` | Email templates |
| `bulk_migrate_sms_templates.py` | SMS templates |
| `bulk_migrate_push_templates.py` | Push templates |
| `bulk_migrate_inapp_templates.py` | In-App templates |

See [`templates_and_snippets/README.md`](templates_and_snippets/README.md) for settings, behavior and limitations.

## Recommended order

1. Create fields with the `nested` data type in the destination (manual).
2. Configure destination project settings, channels, message types and senders.
3. Users and subscriptions.
4. Custom events, then purchases (both need users to exist first).
5. Lists.
6. Catalogs.
7. Snippets.
8. Devices (needs the mobile apps and push credentials set up first).
9. Templates (need snippets, message types, senders, data feeds and catalogs).
10. Journeys and campaigns, which have no script in this repo.

## Common workflow

1. Set `origin_api_key` and `destination_api_key` at the top of the script.
2. For the user, device, event and purchase scripts, set `SOURCE_PROJECT_TYPE` and `DESTINATION_PROJECT_TYPE` (`"email"`, `"uuid"` or `"hybrid"`), and `DESTINATION_PREFERRED_IDENTIFIER`.
3. Fill in any ID maps (for example `CHANNEL_ID_MAP`, `MESSAGE_TYPE_ID_MAP`). IDs differ between projects.
4. Do a small test first: `testSample = 1` and `SAMPLE_FILE = True` for the export-based scripts, or `DRY_RUN = True` for the template and snippet scripts.
5. Run the script and watch the console and the log files in its working folder.
6. To resume an interrupted export-based run, set `job_id` to the printed job ID and `RESUME_FROM_PAGE_LOG = True`.

## Output

Each script writes its logs, state and a `run_summary.json` to a `migrate_*/` folder next to the script. These folders can hold exported customer data, so they are in `.gitignore`.

## Truncation recovery (events and purchases)

Event and purchase exports are limited to about one year. If an export is truncated, the run summary contains:

```json
{ "next_start_datetime": "2024-07-15 09:23:41 +00:00" }
```

Set `start_datetime` to that value and `job_id = None` to continue from where the run stopped.

## Limitations

| Area | Limitation |
|---|---|
| Event history window | Custom event and purchase exports cover 1 year at most. Use `next_start_datetime` for longer histories. |
| Drag and Drop templates | Templates written through the API become HTML templates and lose the Drag and Drop design. |
| Dynamic lists | Filter definitions cannot be migrated by script. Recreate them in the destination. |
| Embedded messaging | Templates migrated by script lose their placement association. |
| System events | Cannot be exported or replayed. |
| Web Push and WhatsApp | No scripted migration path. |
| Journeys and campaigns | Not covered by these scripts. |

## Security

- Never commit real API keys, project IDs, exported data or `migrate_*/` folders.
- The scripts ship with empty or placeholder keys and example IDs only. Replace them locally and clear them before sharing a copy.
- Use keys scoped to the projects you are migrating, and rotate them after the migration.
