# sample_migration_scripts

Sample Python scripts for migrating data and assets between Iterable projects using Iterable's public REST API and bulk export API.

This is a **script-only** migration. The scripts cover the data and the content that can be moved through the API. Everything else is rebuilt or cloned by hand in the Iterable UI. The [Order of operations](#order-of-operations) section tells you which is which, and in what sequence.

Every script is configured by editing variables at the top of the file in an IDE (Spyder, PyCharm, VS Code). There are no command-line arguments.

## Requirements

- Python 3.8+
- `pip install requests`
- A server-side API key for the source project and one for the destination project.

## What the scripts migrate, and what they don't

| Asset | Method |
|---|---|
| Users and subscriptions | Script |
| Custom event history (up to 1 year) | Script |
| Purchase event history (up to 1 year) | Script |
| Catalogs and catalog items | Script |
| Static lists (and suppression / internal lists) | Script |
| Device tokens | Script |
| Snippets | Script |
| Email, SMS, Push and In-App templates **built in the HTML editor** | Script |
| Drag and Drop templates you want to keep as Drag and Drop | Manual clone |
| Dynamic lists | Manual rebuild |
| Journeys | Manual rebuild |
| Campaigns | Manual rebuild |
| Journey webhooks, data feeds, images, embedded placements, project settings | Manual |
| Web Push and WhatsApp templates and campaigns | Not migratable |
| System events | Not migratable |

Anything not listed as a script here must be done manually.

## Scripts

### Data (repo root)

| Script | Migrates | Key APIs |
|---|---|---|
| `user_and_subscriptions_migration.py` | User profiles, data fields, channel and message type subscriptions | Bulk user export, `/api/users/bulkUpdate`, `/api/users/bulkUpdateSubscriptions` |
| `custom_event_migration.py` | Custom event history | Bulk event export, `/api/events/trackBulk` |
| `purchase_migration.py` | Purchase history | Bulk export, `/api/commerce/trackPurchase` |
| `catalog_migration.py` | Catalogs, field mappings and items | Catalog API |
| `list_migration.py` | Standard, suppression and internal lists and their members | `/api/lists`, `/api/lists/subscribe` |
| `devices_migration.py` | Android and iOS device tokens | Bulk user export, `/api/users/registerDeviceToken` |

### Content: `templates_and_snippets/`

| Script | Migrates |
|---|---|
| `bulk_migrate_snippets.py` | Snippets |
| `bulk_migrate_email_templates.py` | Email templates |
| `bulk_migrate_sms_templates.py` | SMS templates |
| `bulk_migrate_push_templates.py` | Push templates |
| `bulk_migrate_inapp_templates.py` | In-App templates |

See [`templates_and_snippets/README.md`](templates_and_snippets/README.md) for settings, behavior and limitations.

## Order of operations

Follow this order. Later steps depend on earlier ones.

| Step | Asset | Method | Prerequisites |
|---|---|---|---|
| 1 | Create fields that use the `nested` data type | Manual | Nested fields can't be created through the UI or API. Ask Iterable Support to create them. |
| 2 | Configure destination project settings: channels, message types, senders and domains, mobile apps, push credentials, and web push setup if used | Manual | — |
| 3 | **Users and subscriptions** | **Script** `user_and_subscriptions_migration.py` | Steps 1–2. Channel and message type IDs mapped in the script. |
| 4 | **Custom event history** (1 year max) | **Script** `custom_event_migration.py` | Step 3 (users) |
| 5 | **Purchase event history** (1 year max) | **Script** `purchase_migration.py` | Step 3 (users) |
| 6 | **Catalogs** | **Script** `catalog_migration.py` | — |
| 7 | **Static lists** | **Script** `list_migration.py` | Step 3 (users) |
| 8 | Dynamic lists | Manual rebuild | Step 3 or the data schema, so the fields they filter on exist |
| 9 | **Snippets** | **Script** `bulk_migrate_snippets.py` | `SnippetsApi` feature flag on in both projects |
| 10 | Images | Manual | Download from the source and upload to the destination File Manager |
| 11 | **Device tokens** | **Script** `devices_migration.py` | Step 2 (mobile apps and push credentials), step 3 (users) |
| 12 | Embedded placements | Manual | — |
| 13 | Data feeds | Manual | — |
| 14a | **Templates, HTML editor**: Email, SMS, Push, In-App | **Script** `bulk_migrate_*_templates.py` | Steps 2, 6, 9, 10, 13. Email also needs the sender and domain from step 2. |
| 14b | **Templates, Drag and Drop** that must stay Drag and Drop | Manual clone | Same prerequisites as 14a |
| 15 | Reconfigure migrated templates: data feed and image URLs | Manual | Step 14 |
| 16 | Embedded messaging templates | Manual | Step 12 (placements) |
| 17 | Journey webhooks | Manual | — |
| 18 | Journeys | Manual rebuild | Steps 1–17 |
| 19 | Campaigns | Manual rebuild | Steps 1–17 |
| 20 | Reconfigure campaigns and journeys: audiences, send-time optimization, webhooks, conversion goals, exit rules | Manual | Steps 17–19 |
| 21 | Embedded messaging campaigns | Manual | Steps 12 and 16 |

### Notes on specific steps

- **Step 1.** Create nested fields before any data is loaded.
- **Step 3.** Set `SOURCE_PROJECT_TYPE`, `DESTINATION_PROJECT_TYPE` and `DESTINATION_PREFERRED_IDENTIFIER` (`"email"`, `"uuid"` or `"hybrid"`) and the `CHANNEL_ID_MAP` / `MESSAGE_TYPE_ID_MAP` before running.
- **Steps 4 and 5.** The destination must already have the users, or events and purchases can't be attributed to them. Exports cover at most one year per pass. See [Truncation recovery](#truncation-recovery-events-and-purchases).
- **Step 6.** Field mappings are set before items are loaded, so item data is typed correctly.
- **Step 8.** Dynamic lists can't be migrated by script. Recreate each one from its filter definition.
- **Step 9.** Run snippets before templates. A template that references a snippet missing from the destination fails to save.
- **Step 10.** Image URLs in migrated templates keep pointing at the source project. Re-upload the images and update the URLs in step 15, especially if the source project will be retired.
- **Step 13.** The template scripts don't create data feeds. Create them first, then map the IDs in the script (`DATA_FEED_MAP`).
- **Step 14a.** A template migrated by script is saved as an HTML template. If it was built in the Drag and Drop editor, it loses that design. Decide per template whether HTML is acceptable.
- **Step 14b.** Clone Drag and Drop templates from project to project in the Iterable UI. When a cloned template references a data feed, the platform creates a new copy of the feed in the destination for each clone, so you can end up with duplicate feeds. Review and merge them afterward. Cloning between projects in different orgs may not be available. In that case, rebuild the template by hand or accept the HTML version.
- **Steps 18 to 20.** Journeys and campaigns can't be migrated by script. Rebuild them in the destination using the migrated lists, templates, snippets and catalogs. Journey webhook nodes need to be reconfigured because webhook IDs differ between projects, and conversion goals and exit rules are set again.
- **Step 21.** Embedded messaging templates and campaigns are rebuilt by hand. The campaigns API has no `placementId` field, so there is no scripted option.

## Common workflow for the data scripts

1. Set `origin_api_key` and `destination_api_key` at the top of the script.
2. Set the project types and `DESTINATION_PREFERRED_IDENTIFIER` where the script has them.
3. Fill in any ID maps. IDs differ between projects.
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
| Drag and Drop templates | Templates written through the API become HTML templates and lose the Drag and Drop design. Clone them manually to keep it. |
| Data feed duplication | Cloning a template that references a data feed creates a new copy of the feed in the destination each time. |
| Dynamic lists | Filter definitions can't be migrated by script. Rebuild them. |
| Journeys and campaigns | Not migratable by script. Rebuild them. Journey webhooks, conversion goals and exit rules are set up again by hand. |
| Embedded messaging | No scripted path for templates, placements or campaigns. |
| Web Push and WhatsApp | Templates and campaigns can't be migrated. |
| System events | Can't be exported or replayed. |

## Security

- Never commit real API keys, project IDs, exported data or `migrate_*/` folders.
- The scripts ship with empty or placeholder keys and example IDs only. Replace them locally and clear them before sharing a copy.
- Use keys scoped to the projects you are migrating, and rotate them after the migration.
