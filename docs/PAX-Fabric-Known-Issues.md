# PAX Fabric — Known Issues and Open Items

This document records unresolved product defects and unfinished work identified during the PAX Fabric deliverables review.

> [!IMPORTANT]
> Every open item in this document is owned by the **Microsoft PAX Fabric team**. The review assigns none of this open work to MAQ.

Resolved findings are excluded from the open list. X1 is not open because deleting a PAX item now retains the Variable Library, Lakehouse, Notebook, and Data Pipeline.

## Open Items from the Deliverables Review

### Summary

| ID | Severity | Category | Summary | Status |
| --- | --- | --- | --- | --- |
| X4 | Critical | Release management | Runtime code is downloaded from mutable `main` by default. | Open |
| X5 | Critical | Data accuracy | Security Copilot activity is classified as Unlicensed Chat. | Open |
| X6 | High | Data completeness | Agent 365 catalog and ownership fields are incomplete. | Open |
| X7 | High | Permissions | Agent 365 application and owner enrichment still requires a permission update. | Open |
| X8 | High | Production readiness | Production hosting identity and the permanent item type are not finalized. | Open |
| G-7 | Medium | Reporting | The AI in One template is not aligned with the planned v2.0 Fabric edition. | Open |
| R-9 | Medium | Upgrade guidance | Workload versions are not mapped to supported PAX-Fabric release tags. | Open |
| G-8 | Medium | Documentation hosting | **Getting Started with PAX Fabric - Enterprise AI Analytics Pipeline** lacks a stable Microsoft-owned short link. | Open |
| V-1–V-6 | High | Release media | Final customer-facing videos must be re-recorded and edited. | Open |

### Known Product Defects

#### X5 — Security Copilot activity is classified as Unlicensed Chat

Records whose `AppHost` begins with `SecurityCopilot-` are processed into AI in One with `Environment = Unlicensed Chat` and `License_Status = Unlicensed`. This can inflate Unlicensed Copilot Chat counts and produce misleading license-adoption conclusions.

The reviewed dataset showed Security Copilot records driving the Unlicensed Chat totals displayed in the AI in One report. The current Fabric processors do not contain Security Copilot-specific trimming or classification logic.

**Interim guidance**

- Review `AppHost` before using affected totals for licensing decisions.
- If reporting must continue, exclude `SecurityCopilot-` records through an approved downstream filter.
- Do not use affected data in final demonstrations or screenshots.

**Resolution**

1. Port Security Copilot trimming from PAX v2.0.0 to the Fabric processors.
2. Re-run PowerShell-versus-Fabric parity tests with the same input.
3. Confirm the corrected metrics and re-record affected demonstrations.

#### X6 — Agent 365 catalog fields are incomplete

The current mapping reads `Description` from `description` and `Created in` from `source`, `origin`, or `createdIn`, while the catalog supplies fields such as `shortDescription`, `longDescription`, and `platform`. Descriptions and creation platform can remain blank, newer fields such as **Created by** and **Developer Name** are missing, and agent type reporting cannot reliably distinguish platforms.

This affects the Agent 365 table and the agent-detail experiences in AI in One and ValueLens. Blank values should not be interpreted as proof that the source catalog has no description, platform, developer, or owner information.

**Interim guidance**

- Treat blank values as a known mapping limitation.
- Do not use Agent Type Combined as an authoritative platform classification.
- Enable Agent 365 collection for AI in One and ValueLens. When disabled, the empty `Agent365` table preserves only the PBIT schema contract.

**Resolution**

1. Port the Agent 365 mapping, enrichment, and retry changes from PAX v2.0.0 prerelease 09 and 10.
2. Map descriptions, platform, Created by, and Developer Name from the appropriate catalog fields.
3. Add the permission required for application and owner lookup as described in X7.
4. Revalidate AI in One and ValueLens with populated Agent 365 data.

#### X7 — Add the Agent 365 application and owner lookup permission

Current permission guidance removed `Reports.Read.All`, explains that `AuditLogsQuery.Read.All` covers the supported audit workloads, and maps each permission to its feature. The remaining work is to add `Application.Read.All` when the application and owner enrichment in X6 is implemented.

Without the permission and tenant administrator consent, the planned application and owner lookups can fail or return incomplete enrichment data.

Permission guidance is maintained in:

- **Getting Started with PAX Fabric - Enterprise AI Analytics Pipeline**
- **PAX Fabric — Setup & Execution Guide**
- **PAX Fabric Workload User Guide**
- **PAX Fabric - Manual Deploy README**

**Resolution**

1. Add `Application.Read.All` to the Entra application requirements for workload and manual-deployment customers.
2. Update setup automation and the permission sections in the four documents above.
3. Require tenant administrator consent and validate the lookup through least-privilege and end-to-end testing.

### Open Engineering and Release Items

#### X4 — Publish immutable PAX Fabric releases

The workload and manual deployment default `GitHubRef` to `main`, and every run downloads that ref. A merge can therefore reach customers without a workload release, making runs difficult to reproduce, test, or roll back.

The workload package version alone cannot identify the exact PAX runtime code executed by a customer while the ref remains mutable.

**Interim guidance:** Pin controlled environments to a reviewed commit SHA and use `main` only for development.

**Resolution**

1. Publish versioned PAX-Fabric release tags.
2. Default workload and manual deployment to the supported tag.
3. Document and test customer upgrade and rollback procedures.

#### X8 — Finalize the production workload identity

The current production guidance uses a MAQ-owned host, the item type remains `DemoPAXItem`, and Microsoft production subscription, domain, Entra tenant, DNS, and certificate ownership are not finalized. These must be settled before customers create items because item type changes can break existing items and references.

The production identity also determines Entra redirect URIs, domain verification, TLS ownership, and the item type customers persist in Fabric workspaces.

**Resolution before production publish**

1. Select the Microsoft-owned hosting subscription, resource group, service, domain, and Entra tenant.
2. Configure DNS, TLS, redirect URIs, and the frontend app registration.
3. Select and implement the permanent item type name.
4. Update manifests, environment configuration, release automation, screenshots, and:
   - **Getting Started with PAX Fabric - Enterprise AI Analytics Pipeline**
   - **PAX Fabric — Setup & Execution Guide**
   - **PAX Fabric Workload User Guide**
   - **PAX Fabric - Manual Deploy README**
5. Validate item creation, opening, scheduling, resource updates, and deletion.

#### G-7 — Align the AI in One report template

The reviewed demonstrations use the earlier AI in One page set rather than the planned v2.0 Fabric edition.

Until the supported template is published and validated, screenshots and videos can show pages or fields that do not match the intended customer release.

**Resolution**

1. Publish the supported M365 Usage, AI in One, and ValueLens template versions.
2. Align and validate AI in One v2.0 against the current Lakehouse contract.
3. Use the supported template in the four maintained documents and final videos.

#### R-9 — Define the PAX Fabric code upgrade process

The workload repository `README.md` explains how maintainers release embedded Notebook, Data Pipeline, and Variable Library definitions, but it does not map workload versions to supported PAX-Fabric runtime releases or explain how customers should update `GitHubRef`. Until X4 provides release tags, a stable compatibility mapping cannot be published.

Without a compatibility matrix, customers and support engineers cannot determine the supported runtime, reproduce a run from the workload version, or choose a validated rollback target.

**Resolution**

1. Complete X4 and map every workload package version to a supported PAX-Fabric tag.
2. Document upgrade prerequisites, validation, rollback, and when to update `GitHubRef`.
3. Coordinate runtime changes with embedded resource definitions and publish compatibility release notes.

#### G-8 — Provide a stable Getting Started link

The workload links to **Getting Started with PAX Fabric - Enterprise AI Analytics Pipeline** through a long Office viewer URL tied to a GitHub filename.

Renaming or moving the underlying file requires a workload-link update and can break previously published entry points.

**Resolution**

1. Create a stable Microsoft-owned `aka.ms` link.
2. Point the Workload Hub learning material, Product support link, and in-item **View Documentation** action to it.

### Final Video Re-recording

The reviewed videos are handoff assets, not final release media. Re-record them after X5, X8, and G-7 are resolved.

| ID | Finding | Requirement |
| --- | --- | --- |
| V-1 | Development workload, `DemoPAXItem`, MAQ test workspace, and a DevGateway error are visible. | Record on the production workload with a neutral workspace and item name. |
| V-2 | Demonstrated settings contradict the intended guidance. | Use Retention `0`, Keep Scratch off, the supported release tag, `NumberOfDays=1`, and Agent 365 collection. |
| V-3 | Security Copilot is shown as Unlicensed Chat. | Re-record the Lakehouse and dashboard after X5 and parity validation. |
| V-4 | The run window differs between the run and output videos. | Use one window or explain pre-collected data in narration. |
| V-5 | India Standard Time and `DD-MM-YYYY` are ambiguous for a US audience. | Use US English and UTC or a US time zone, or explain the format. |
| V-6 | Long deployment, refresh, and loading waits remain. | Trim or accelerate waits during editing. |

Also use final Workload Hub content, consistent sensitivity labels, blurred environment-specific endpoints, prerequisite narration without secrets, the supported AI in One template, and populated Agent 365 data.

## Issues Found During Testing

The Executive Test Summary confirms that the Fabric implementation is correct for these cases. The findings below are based on:

- **PowerShell baseline:** `PAX_Purview_Audit_Log_Processor_v1.11.16-prerelease-20260916-06.ps1`
- **PAX-Fabric parity baseline:** `v1.11.16-prerelease-20260916-06`

The open issues below are present in the PowerShell baseline used for the parity tests. They do not indicate defects in the validated Fabric implementation.

| Test | PowerShell-side finding | Fabric result | Status |
| --- | --- | --- | --- |
| 3 | Incremental M365 append loses earlier session history. | Fabric preserves the full history. | Open |
| 12 | M365 append loses prior session statistics, double-counts an overlapping day, and produces incorrect user statistics. | Fabric handles the append correctly. | Open |
| 15 | Agent 365 availability contains generic placeholders instead of actual user or group IDs for two agents. | Fabric preserves the correct IDs and types. | Open |
| 16 | The same Agent 365 availability loss remains when cached packages are reused. | Fabric cache reuse preserves the correct values. | Open |
| 19 | The PowerShell `UserIds` run fails before producing comparable output. | Fabric completes and filters correctly. | Investigation required |

### Test 3 — Incremental M365 append loses session history

**Test case:** `-Rollup -IncludeM365Usage -IncludeUserInfo -AppendFile`

PowerShell retained 335 SessionStats rows instead of the complete 816 rows preserved by Fabric. The incomplete history also shifted three Copilot percentile values. Raw data, cumulative rollup, SessionCohort, and Entra users otherwise matched.

**PowerShell resolution:** Preserve prior SessionStats during append and recompute dependent percentile values over the complete cumulative history.

### Test 12 — M365 dashboard append corrupts cumulative outputs

**Test case:** `-AppendFile` with AIO, M365, and ValueLens dashboards

AIO and ValueLens matched Fabric. In M365, PowerShell:

- Dropped earlier SessionStats rows.
- Double-counted activity and item values on the overlapping date.
- Produced incorrect UserStats as a downstream effect.

SessionCohort matched. Fabric retained earlier data and handled the overlap correctly.

**PowerShell resolution:** Correct the M365 append merge to preserve prior session data, de-duplicate overlapping dates, and regenerate UserStats from the corrected cumulative dataset.

### Test 15 — Agent 365 availability loses principal IDs

**Test case:** `-OnlyAgent365Info`

Of 14,882 agents, two PowerShell rows used generic placeholders instead of the actual availability principal. Fabric retained the correct group ID for one agent and user ID for the other.

**PowerShell resolution:** Preserve the source user or group ID and principal type when normalizing Agent 365 availability.

### Test 16 — Cached Agent 365 rerun retains the same availability defect

**Test case:** Agent 365 rerun using cached packages

Cache reuse worked, but PowerShell again replaced the actual availability values for **Change Management Agent** and **Customer Case Assistant** with generic placeholders. Fabric preserved the resource IDs and user/group types.

**PowerShell resolution:** Apply the Test 15 mapping fix to both fresh and cached Agent 365 processing paths.

### Test 19 — PowerShell `UserIds` filtering run fails

**Test case:** `-UserIds`

The PowerShell run stopped with errors before producing complete output, so parity could not be measured. Fabric completed and filtered to the requested users correctly.

**PowerShell resolution:** Investigate the failure, rerun with the same input and date range, and complete the parity comparison after PowerShell produces a full output.

## Limitations in the PAX-Fabric Solution

The following are supported operational constraints in the current PAX-Fabric solution. They should be considered when designing schemas, schedules, and reporting connections.

| ID | Constraint | Operational implication |
| --- | --- | --- |
| L-1 | UserHistory changes the Users table shape. | One Target Schema must use one consistent UserHistory mode. |
| L-2 | Changing Target Schema creates a separate dataset boundary. | Tables, report connections, and watermark state do not carry into the new schema automatically. |
| L-3 | Watermark supports one dashboard per run. | Multi-dashboard watermark runs are blocked, and empty windows do not advance the marker. |

### L-1 — UserHistory mode is fixed per Target Schema

Enabling UserHistory changes the Users table from a current-state shape to a slowly changing dimension shape that includes `EffectiveDate` and historical rows. Current-state and history-shaped Users tables cannot coexist in the same Target Schema.

This constraint applies to AI in One and ValueLens. M365 does not support UserHistory.

If a run changes UserHistory mode against an existing incompatible schema, PAX rejects the run with `USER_HISTORY_SHAPE_MISMATCH` and leaves the target unchanged.

**To change modes**

- Point Target Schema to a new, empty schema; or
- Back up and remove the existing tables in the current schema before running with the new mode.

Use one schema for one consistent UserHistory mode.

### L-2 — Changing Target Schema creates a separate dataset

Changing Target Schema does not move or continue the existing dataset. Subsequent runs create the applicable PAX tables under the new schema.

After changing the schema:

- Existing tables remain in the previous schema.
- Power BI templates and other consumers must be repointed to the new schema.
- Watermark continuity starts fresh for the new target.
- Historical data must be recollected or migrated if it is required in the new schema.

Treat a Target Schema change as creating a new dataset rather than renaming or moving the current one.

### L-3 — Watermark is limited to one dashboard per run

Watermark-based incremental collection supports one dashboard for each run. Combining Watermark with multiple dashboards in a single run is not supported and is blocked by validation.

Watermark state advances only when the run collects new data. If the resolved window contains no new data, the existing marker is retained; no coverage is lost.

Use separate single-dashboard runs or schedules when incremental collection is required for multiple dashboards.
