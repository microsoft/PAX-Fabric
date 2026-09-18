# PAX Fabric — Customer Deployment Guide

Deploy the **PAX Fabric** solution (Microsoft 365 Copilot & Entra activity analytics
pipeline) into your own Microsoft Fabric workspace.

> **Pipeline script version: v1.11.16.** This release is a **strict superset**
> of v1.11.15. All six new knobs (`Watermark`, `PurviewInputFile`,
> `UserHistory`, `HistoryEffectiveDate`, `EmitMetricsJson`, `MetricsPath`)
> default to *off* / empty, so an existing v1.11.15 deployment continues to
> behave byte-identically after upgrade until you opt in. See Section 4.5.

The deployment provisions four Fabric items:

| Item              | Purpose                                                            |
| ----------------- | ------------------------------------------------------------------ |
| Lakehouse         | Stores Delta tables (`CopilotInteraction_*`, `Entra_Users`, etc.)  |
| Variable Library  | Holds tenant / auth / date / behavior configuration                |
| Notebook          | Runs the `pax_fabric` Python package (cloned from GitHub at run)   |
| Data Pipeline     | Wraps the Notebook so you can schedule daily runs                  |

The pipeline pulls **Microsoft Graph Security Audit Log**, **M365 Usage Reports**,
and optional **Microsoft Agent 365** catalog data, normalises them, and writes
them to Delta tables in the Lakehouse.

---

## 1. What you will need

### 1.1 Software on the machine running the deployment

| Requirement                                     | Notes                                   |
| ----------------------------------------------- | --------------------------------------- |
| PowerShell **7.0+**                             | `pwsh --version`                        |
| Azure CLI **2.55+** *(or Az PowerShell module)* | For token acquisition                   |
| Network reach to `api.fabric.microsoft.com`     |                                         |

Install Az CLI: <https://learn.microsoft.com/cli/azure/install-azure-cli>

### 1.2 Microsoft Fabric

| Requirement                                                    |
| -------------------------------------------------------------- |
| A Fabric workspace on an **F**-SKU capacity                    |
| **Workspace Admin** role on that workspace (your Entra user)   |

Copy the workspace GUID from **Fabric portal → Workspace settings → About**.

### 1.3 Entra App Registration (used **by the pipeline**, not by the deploy script)

Create it once in **Entra ID → App registrations → New registration**, then:

1. **API permissions → Add → Microsoft Graph → Application permissions**:
   - `AuditLogsQuery.Read.All`
   - `AuditLogsQuery-Exchange.Read.All`
   - `AuditLogsQuery-OneDrive.Read.All`
   - `AuditLogsQuery-SharePoint.Read.All`
   - `User.Read.All`
   - `Organization.Read.All`
   - `GroupMember.Read.All`
2. **Grant admin consent** for all seven permissions.
3. **Certificates & secrets → New client secret** — copy the *value* (you will paste it into Key Vault).
4. Record **Tenant ID** and **Application (client) ID**.

### 1.4 Azure Key Vault

The pipeline reads the App Registration client secret at runtime from Azure Key
Vault. You will need:

1. A Key Vault (existing or new) in the same tenant.
2. A **secret** in that vault (e.g. `PAXClientSecret`) whose value is the client
   secret from step 1.3 (3).
3. **RBAC role assignment on the vault**: `Key Vault Secrets User` → the
   **user running this deployment** (so the deployment/verification steps and
   the `az keyvault secret ...` commands in Section 4.3 can read the secret). The
   App Registration itself does **not** need any Key Vault RBAC role — the
   pipeline reaches the vault via the Fabric workspace over the Managed
   Private Endpoint.
4. **Network access from Fabric to the vault** — a **Managed Private Endpoint
   (MPE)** created in the Fabric workspace that targets the Key Vault. Fabric
   raises a private endpoint connection request against the vault; the Key
   Vault administrator must **Approve** it before Fabric can reach the vault
   (see Section 4.2). Public network access on the vault can remain disabled.

### 1.5 GitHub repo hosting the `pax_fabric` Python package

The notebook `pip install`s the pipeline code from your repo at runtime. You need:

- Repo URL (`https://github.com/<org>/<repo>.git`) — public, or reachable from the
  Fabric Spark environment.
- Ref — use `main` for the latest code, or pin to a specific commit hash (e.g. `a1b2c3d4e5f6...`) for a reproducible production build.

---

## 2. The deployment package

```text
PAX Fabric - Manual Deploy/
├── Deploy-PaxFabric.ps1          # Main deployment script (idempotent)
├── config.template.json          # Copy → config.json → edit
├── README.md                     # This file
└── artifacts/
    ├── varlib-variables.json     # Variable Library schema (do not edit)
    ├── varlib-valueset.json      # Value set template  (do not edit)
    ├── pipeline-content.json     # Data Pipeline template
    └── pax_fabric_notebook.ipynb # Notebook shipped with this package
```


---

## 3. Deployment steps

### Step 1 — Fill in the config

```powershell
cd "PAX Fabric - Manual Deploy"
Copy-Item config.template.json config.json
notepad config.json      # or your favourite editor
```

Populate every `<...>` placeholder. Minimum required:

| Field                            | Example                                                        |
| -------------------------------- | -------------------------------------------------------------- |
| `TenantId`                       | `11111111-2222-3333-4444-555555555555`                         |
| `WorkspaceId`                    | `aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee`                         |
| `AppRegistration.TenantId`       | *(usually same as top-level TenantId)*                         |
| `AppRegistration.ClientId`       | The App Registration client ID from Section 1.3                |
| `KeyVault.KeyVaultUri`           | `https://contoso-kv.vault.azure.net/`                          |
| `KeyVault.KeyVaultSecret`        | `PAXClientSecret`                                              |
| `GitHub.GitHubRepoUrl`           | `https://github.com/contoso/pax-fabric.git`                    |
| `GitHub.GitHubRef`               | `main` or a commit hash (e.g. `a1b2c3d4e5f6...`)               |
| `Defaults.StartDate` / `EndDate` | `2026-07-01` / `2026-07-04` (initial backfill window)          |
| `Defaults.MaxConcurrency`        | Default `10`. Reduce (e.g. `2`–`5`) when pulling large windows or if you see Graph `HTTP 429` throttling in the run log. |

### Step 2 — Run the deployment

```powershell
cd "PAX Fabric - Manual Deploy"
.\Deploy-PaxFabric.ps1 -ConfigPath .\config.json
```

The script handles sign-in automatically. On first run (or when no Az CLI
session is present) it invokes `az login` — a browser window opens and you
pick the account that has **Workspace Admin** on the target Fabric workspace.
No tenant needs to be specified in the login prompt (the token is then
requested explicitly for `TenantId` from `config.json`), and subscription
selection is bypassed because Fabric API access does not require an Azure
subscription. If you are already signed in (`az account show` returns a
context), the script reuses that session.

Expected duration: **60-120 seconds**. The script prints IDs for each item it
creates or reuses. Re-running is safe — existing items are detected and reused
(idempotent).

### Step 3 — Confirm the four items appear

Open the Fabric workspace and confirm you see the four items your
`config.json` names:

- The Lakehouse         — display name from `LakehouseName` in `config.json`
- The Variable Library  — display name from `VarLibName`    in `config.json`
- The Notebook          — display name from `NotebookName`  in `config.json`
- The Data Pipeline     — display name from `PipelineName`  in `config.json`

The rest of this README refers to these items as `<LakehouseName>`,
`<VarLibName>`, `<NotebookName>`, and `<PipelineName>` — substitute the
values you set in `config.json` when you see those placeholders.

---

## 4. Manual post-deployment steps

The Fabric REST API does not yet cover every configuration action. Complete
these in the UI:

> **Note.** The deployment script pre-attaches `<LakehouseName>` as the
> default Lakehouse on `<NotebookName>` and rewires the notebook to resolve
> `<VarLibName>` by name at runtime, so **no manual Variable-Library or
> Lakehouse attachment step is required**.

### 4.1 Enable Fabric ⇒ Key Vault network path (Managed Private Endpoint)

Create a **Managed Private Endpoint (MPE)** in the Fabric workspace targeting
the Key Vault. Fabric raises a private endpoint connection request on the
vault; the Key Vault administrator must approve it before the pipeline can
read secrets.

Before you start, copy the Key Vault's **Resource ID** from the Azure portal:
**Azure portal → your Key Vault → Overview → JSON View** (top-right), then
copy the value of the `id` field. It looks like
`/subscriptions/<sub-id>/resourceGroups/<rg>/providers/Microsoft.KeyVault/vaults/<vault-name>`.

1. **Fabric portal → open the target workspace → Workspace settings**
   (gear icon, top-right of the workspace) **→ Outbound networking →
   Managed private endpoints → + Create**.
2. Fill in the **Create managed private endpoint** dialog:
   - **Managed private endpoint name**: any short name (e.g. `mpe-<vault-name>`).
   - **Resource identifier**: paste the Key Vault Resource ID you copied above
     (must start with `/subscriptions/...`).
   - **Target sub-resource**: select **`vault`** from the dropdown.
   - **Request message**: optional note for the vault approver
     (e.g. `Fabric MPE for PAX Fabric pipeline`).
   - Click **Create**. This raises a **pending** private endpoint connection
     on the Key Vault.
3. **Azure portal → Key Vault → Networking → Private endpoint connections** —
   locate the pending request from Fabric and click **Approve**. (This step
   must be done by a Key Vault administrator / owner.)
4. Back in **Fabric → Workspace settings → Outbound networking → Managed
   private endpoints**, wait until the endpoint's **Activation** shows
   *Succeeded* and **Approval** shows *Approved* (typically ~2 minutes).
5. Public network access on the vault can stay **disabled** — Fabric now
   reaches the vault over the private link.

### 4.2 Store the client secret in Key Vault

Do this in the Azure portal:

1. **Azure portal → Key Vault → your vault → Objects → Secrets → Generate/Import**.
2. **Upload options**: `Manual`.
3. **Name**: your choice — **whatever you put here must match `KeyVault.KeyVaultSecret` in `config.json`** (e.g. `PAXClientSecret`, `pax-fabric-client-secret`, etc.).
4. **Secret value**: paste the client secret value copied from the App Registration in Section 1.3 (3).
5. Leave content type / expiration as required by your organisation → **Create**.

The user performing this step needs `Key Vault Secrets Officer` (write) on the vault. The pipeline itself reads the secret via the Fabric Managed Private Endpoint approved in Section 4.1 — the App Registration does not need any Key Vault role.

### 4.3 First pipeline run (interactive)

Run the pipeline manually once — **not the notebook directly** — to verify
authentication, Key Vault access, and Graph permissions work end-to-end.
This is the same code path a scheduled run will take.

1. Open `<PipelineName>` in Fabric.
2. Click **Run** (top toolbar) and confirm.
3. Wait for the run to complete.
4. Click into the notebook activity to inspect cell output. Healthy signs:
   - `pax_fabric` package installed from your GitHub ref.
   - `KV secret retrieved: True`.
   - `Graph API probe -> beta` (v1.0 currently returns 404; this is expected —
     the pipeline auto-falls back).
   - `submitted N jobs, completed N, failures 0`.
    - `[SQLITE] Opened driver-local state ... cacheKiB=2048 freeMiB=...`.
    - `[SQLITE] Seeded namespace=... distinctKeys=... nextValue=...` for
       persisted UserKey, ThreadId, and Message_Id continuity.
    - `[SQLITE] State summary users=... threads=... messages=... rollupRows=...`
       followed by `[SQLITE] Removed driver-local state ...`.
5. Verify Delta tables in the Lakehouse Explorer, e.g.
   `CopilotInteraction_Interactions_Rollup`, `Entra_Users`.

Copilot surrogate maps, fact rollup state, and ValueLens distinct aggregates
use a temporary SQLite database on the notebook driver. The database is rebuilt
from Delta history on every run and deleted after processing. It is deliberately
not stored in OneLake or under `/lakehouse/default`, because SQLite requires
local filesystem locking and random writes. Memory use therefore stays bounded,
but the driver must have enough temporary disk for the current run.

If the run fails, see Section 5 Troubleshooting.

### 4.4 Schedule the Pipeline

Once Section 4.3 succeeds, put the pipeline on a recurring schedule:

1. Open `<PipelineName>`.
2. **Schedule** → **On** → set frequency (typically daily, 02:00 local).
3. **Save**.

The pipeline calls the notebook with parameters from `<VarLibName>`. Change
date ranges by editing **`<VarLibName>` → StartDate / EndDate** in the Fabric
UI and saving — no redeploy required.

### 4.5 v1.11.16 optional features (all default-off)

Fresh deployments already ship these six parameters wired into
`<PipelineName>` with the safe defaults shown. **You do not need to enable
any of them** — with the defaults, the pipeline behaves exactly as v1.11.15.
Enable a knob only when you need the feature it unlocks. To edit, open
`<PipelineName>` → **Parameters** (canvas level) or **RunPaxNotebook → Base
parameters** (activity level) and change the value.

| Pipeline parameter     | Type   | Default | Purpose                                                       |
| ---------------------- | ------ | ------- | ------------------------------------------------------------- |
| `Watermark`            | bool   | `false` | Incremental runs. Persists the last completed window so re-runs skip already-processed data. |
| `PurviewInputFile`     | string | `""`    | "Bring your own data" (BYOD). Bypass Graph API and read an already-exported Purview CSV from the Lakehouse `Files/` area. |
| `UserHistory`          | string | `"Off"` | Retain a rolling per-user history slice. Set to `"On"` to keep the audit history for compliance / forensic reasons. |
| `HistoryEffectiveDate` | string | `""`    | Protective floor for retention. Rows on or after this date are never dropped, even if `RetentionDays` would otherwise evict them. Format `YYYY-MM-DD`. |
| `EmitMetricsJson`      | bool   | `false` | Write a compact run-telemetry sidecar (script version, timestamps, per-stage counters). |
| `MetricsPath`          | string | `""`    | Output path for the metrics sidecar when `EmitMetricsJson=true`. Typical value: `Files/pax_metrics.json`. Ignored when `EmitMetricsJson=false`. |

**How the four features compose:**

- **Incremental (Watermark).** Set `Watermark=true`. First run bootstraps a
  state file under the Lakehouse (`.pax_watermark_state.json`); subsequent
  runs advance the window automatically. If a run covers a window already
  processed, it short-circuits with `WATERMARK: covered through …` — no
  Graph calls, no Delta writes.
- **BYOD (`PurviewInputFile`).** Point at an exported Purview audit CSV
  in your Lakehouse (`PurviewInputFile="Files/purview_export.csv"`). The
  pipeline skips Graph authentication entirely and normalises the CSV
  through the same downstream path.
- **User history + protective floor.** Set `UserHistory="On"` and
  `HistoryEffectiveDate="YYYY-MM-DD"` (e.g. the start of your
  compliance window). Rows on/after HED will not be evicted by
  `RetentionDays`.
- **Metrics sidecar.** Set `EmitMetricsJson=true` and
  `MetricsPath="Files/pax_metrics.json"`. The pipeline writes a JSON file
  with four top-level keys — `version`, `timestampUtc`, `parameters`,
  `metrics` — after each successful run.

**Backport for deployments created before v1.11.16.** Rerunning
`Deploy-PaxFabric.ps1` reuses the existing pipeline and does **not**
replace its saved definition. To surface the six new knobs on an older
deployment, open `<PipelineName>` and add them under **Parameters**:

- `Watermark` (Boolean, default `false`)
- `PurviewInputFile` (String, default empty)
- `UserHistory` (String, default `Off`)
- `HistoryEffectiveDate` (String, default empty)
- `EmitMetricsJson` (Boolean, default `false`)
- `MetricsPath` (String, default empty)

Then add matching **Base parameters** on the `RunPaxNotebook` activity
with values `@pipeline().parameters.<name>`. Fresh deployments include all
six automatically.

---

## 5. Troubleshooting

| Symptom                                                              | Likely cause & fix                                                                                                     |
| -------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `Failed to acquire Fabric access token`                              | The in-script `az login` was cancelled, or the signed-in account has no access to the tenant in `config.json`. Re-run and pick the correct account in the browser prompt. |
| `Cannot read workspace ...`                                          | You are not Workspace Admin on that workspace. Add your account under **Workspace → Manage access**.                   |
| `Failed to create Lakehouse: ... 401`                                | Capacity is not an F-SKU, or the workspace has no capacity assigned. Only F-SKU capacities are supported.              |
| Pipeline job: `KV secret retrieved: False`                           | The Managed Private Endpoint has not been approved on the vault, or the secret name in `<VarLibName>` doesn't match the one in Key Vault. See Section 4.1, Section 4.2. |
| Pipeline job: hundreds of `HTTP 429` in log, run > 2× expected time  | Graph audit-log endpoint throttling. Reduce `MaxConcurrency` (default `10`) on the pipeline — try `5`, then `2` for very large backfills.                    |
| Pipeline job: `Graph API probe: v1.0 -> 404`                         | Expected. The pipeline probes v1.0 first and automatically falls back to the `/beta` endpoint. Non-fatal.                                                    |
| Notebook cell fails with `pip: could not resolve host github.com`    | Your Fabric capacity is in a private network with no internet egress. Fork the repo into an internal-reachable mirror. |
| `Variable Library` values wrong                                      | Edit them in the Fabric UI. The deployment script only *initialises* them at create time.                              |
| `[SQLITE]` followed by `unable to open database file`                | The notebook driver cannot create local temporary files. Restart the session and confirm Python `tempfile` points to a writable local path. Do not redirect the SQLite file to OneLake or ABFS. |
| `[SQLITE]` followed by `database or disk is full`                    | Driver-local scratch space was exhausted. Reduce the processing window or use a larger Fabric Spark resource profile; the log's `freeMiB` value shows space available when processing started. |
| Run log: `WATERMARK: covered through <date>`                         | Expected when `Watermark=true` and the requested window has already been processed. The pipeline short-circuits — no Graph calls, no Delta writes. Advance `EndDate` or wait for the next scheduled run. |
| `EmitMetricsJson=true` but no file appears                           | Confirm `MetricsPath` is set (default empty) and points at a writable Lakehouse `Files/…` path. Empty `MetricsPath` is treated as "don't emit" even when the flag is on. |
| BYOD run: rows missing / schema mismatch                             | `PurviewInputFile` expects a Purview audit-export CSV. Verify the file exists at the Lakehouse-relative path and that the header row matches the exporter's schema. |

---

## 6. FAQ

**Q. How do I change the parameters the pipeline uses (dates, flags,
`MaxConcurrency`, Key Vault name, GitHub ref, …) after deployment?**

There are **two places** parameters can live, and the notebook uses them in
this order at run time:

1. **Pipeline parameters** — the Data Pipeline (`<PipelineName>`) forwards
   these to the notebook on every run. If the pipeline sends a value, the
   notebook uses it.
2. **Variable Library** (`<VarLibName>`) — the notebook falls back to these
   when the pipeline does **not** send a value (or sends an empty string).

Which place you edit depends on **which parameter** you want to change:

**Edit the pipeline** (Fabric portal → workspace → open `<PipelineName>` →
select the **`RunPaxNotebook`** activity → **Settings → Base parameters**,
or change the pipeline's default values via **Parameters** at the pipeline
canvas level) for run-tunable knobs:

- `StartDate` / `EndDate` — audit-log backfill window (leave empty to use
  the rolling default from the Variable Library).
- `Rollup`, `IncludeM365Usage`, `IncludeUserInfo`, `OnlyUserInfo`,
  `IncludeAgent365Info`, `OnlyAgent365Info`, `KeepScratch` — feature toggles.
- `GroupNames` — comma-separated Entra group display names used to scope the
  run (for example, `Finance Copilot Users,Legal Copilot Users`). Leave empty
  for no group filter.
- `TargetSchema` — Lakehouse schema to write into (default `dbo`).
- `RetentionDays` — how long processed data is kept.
- `MaxConcurrency` — default `10`. Reduce when pulling large audit-log
  windows or if the run log shows Graph `HTTP 429` throttling.
- **v1.11.16 optional knobs (all default-off):** `Watermark`,
  `PurviewInputFile`, `UserHistory`, `HistoryEffectiveDate`,
  `EmitMetricsJson`, `MetricsPath`. See Section 4.5 for what each one does
  and how the features compose.

Save the pipeline; the next run (manual or scheduled) picks up the new
values.

`IncludeAgent365Info` adds Agent 365 catalog output to the normal audit run.
`OnlyAgent365Info` skips the audit pull and retrieves only Agent 365 catalog
data. Do not enable both switches in the same run. `GroupNames` cannot be used
with `OnlyAgent365Info` because that mode does not process user-scoped audit
data.

For deployments created before these parameters were added, rerunning the
deployment script reuses the existing pipeline and does not replace its saved
definition. Add `IncludeAgent365Info` and `OnlyAgent365Info` as Boolean pipeline
parameters (default `false`) and `GroupNames` as a String parameter (default
empty), then add matching Base parameters to the `RunPaxNotebook` activity.
Fresh deployments include all three automatically.

**Edit the Variable Library** (Fabric portal → workspace → open
`<VarLibName>` → active value set → edit → **Save**) for values the
pipeline does *not* forward:

- `KeyVault.KeyVaultUri` and `KeyVault.KeyVaultSecret` — if you rotate the
  vault or rename the secret (see Section 4.2).
- `AppRegistration.TenantId` / `AppRegistration.ClientId` — if the app
  registration changes.
- `GitHub.GitHubRef` — bump to a new commit/tag to roll the pipeline code
  forward on the next run.

> **No redeploy is required** for either path. The deployment script only
> *initialises* the pipeline defaults and Variable Library values at create
> time; after that, the Fabric UI is the source of truth.

---

## 7. Uninstall / clean up

The script does not include a `-Uninstall` switch on purpose (destructive
operations should be explicit). To remove everything:

1. Fabric portal → workspace → for each of the four items → **`...`** → **Delete**.
2. Remove the `Key Vault Secrets User` role assignment on the vault
   (deployment user).
3. Fabric portal → workspace → **Workspace settings → Outbound networking →
   Managed private endpoints** — select the MPE targeting the Key Vault and
   click **Delete**. Optionally remove the (now orphaned) private endpoint
   connection on the vault side as well.
4. Optional: delete the App Registration in Entra ID.

Delta table storage under the Lakehouse OneLake path is deleted with the
Lakehouse item.
