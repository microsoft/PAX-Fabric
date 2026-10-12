# PAX Fabric — Known Issues and Open Items

This is a short list of known defects in the current release of PAX Fabric. They do not stop a run from completing. Each entry names who will fix it.

| ID | Issue | Effect | Owner |
|---|---|---|---|
| X5 | Security Copilot activity is classified as Unlicensed Chat | In the AI-in-One interactions data, Security Copilot activity (`AppHost` beginning `SecurityCopilot-`) appears with `Environment = Unlicensed Chat` and `License Status = Unlicensed`. This can inflate unlicensed Copilot Chat counts. | Microsoft PAX Fabric team |
| X6 | Agent 365 `Description` and `Created in` columns are blank | The AI-in-One and ValueLens agent pages show empty descriptions, and agents cannot be told apart by where they were created. | Microsoft PAX Fabric team |

## X5 — Security Copilot activity

- **What happens:** `pax_fabric` has no Security Copilot handling, so Security Copilot records are processed like other Copilot Chat records.
- **Fix needed:** port the Security Copilot trimming from PAX v2.0.0 to the Fabric processors, then re-validate against the same input.

## X6 — Agent 365 `Description` and `Created in`

- **What happens:** in `mod12_pax_agent365.py`, `Description` is read from a field named `description`, and `Created in` is read from `source`, `origin` or `createdIn`. The Agent 365 catalog returns `shortDescription` / `longDescription` and `platform` instead, so both columns stay blank.
- **Fix needed:** port the Agent 365 changes from PAX v2.0.0 prerelease 09 and 10 to `mod12_pax_agent365.py`. The Agent 365 lookups need `Application.Read.All`, which the Setup Guide already lists.

No code change is planned on the MAQ side for either item.
