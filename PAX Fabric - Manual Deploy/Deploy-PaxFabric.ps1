<#
.SYNOPSIS
    Deploys the PAX Fabric solution (Lakehouse + Variable Library + Notebook + Data Pipeline)
    into a customer's Microsoft Fabric workspace.

.DESCRIPTION
    Idempotent deployment script. Uses the Microsoft Fabric REST API and the caller's
    Entra identity (via Az CLI or Az PowerShell). No shared secrets stored in the script.

    Prerequisites:
      - PowerShell 7.0 or newer
      - Azure CLI (`az`) OR Az PowerShell module (`Az.Accounts`)
      - Workspace Admin on the target Fabric workspace
      - App Registration with Microsoft Graph `AuditLogsQuery.Read.All`
        and `User.Read.All` (Application, admin-consented)
      - Azure Key Vault with the App Registration's client secret stored

.PARAMETER ConfigPath
    Path to a JSON file with deployment values. Copy config.template.json and edit it.

.PARAMETER ArtifactsPath
    Folder containing the notebook, variable library, and pipeline templates.
    Defaults to `.\artifacts` relative to the script.

.EXAMPLE
    .\Deploy-PaxFabric.ps1 -ConfigPath .\config.json
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)] [string] $ConfigPath,
    [string] $ArtifactsPath = (Join-Path $PSScriptRoot 'artifacts')
)

$ErrorActionPreference = 'Stop'

# Force TLS 1.2 (Windows PowerShell 5.1 defaults to 1.0/1.1, which the Fabric REST
# endpoint rejects with a forcibly-closed transport connection). No-op on PS 7+.
[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

# ------------------------------------------------------------------------
# 1. Load & validate config
# ------------------------------------------------------------------------
if (-not (Test-Path $ConfigPath))    { throw "Config file not found: $ConfigPath" }
if (-not (Test-Path $ArtifactsPath)) { throw "Artifacts folder not found: $ArtifactsPath" }

$cfg = Get-Content $ConfigPath -Raw | ConvertFrom-Json

$requiredTop = @('TenantId', 'WorkspaceId', 'LakehouseName', 'VarLibName', 'NotebookName', 'PipelineName')
foreach ($k in $requiredTop) {
    if ([string]::IsNullOrWhiteSpace($cfg.$k) -or $cfg.$k -like '<*>') {
        throw "config.json is missing or contains a placeholder for '$k'. Edit config.json and try again."
    }
}
if (-not $cfg.AppRegistration -or [string]::IsNullOrWhiteSpace($cfg.AppRegistration.ClientId) -or $cfg.AppRegistration.ClientId -like '<*>') {
    throw "config.json is missing AppRegistration.ClientId. Edit config.json and try again."
}
if (-not $cfg.KeyVault -or [string]::IsNullOrWhiteSpace($cfg.KeyVault.KeyVaultUri) -or $cfg.KeyVault.KeyVaultUri -like '<*>') {
    throw "config.json is missing KeyVault.KeyVaultUri. Edit config.json and try again."
}

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host " PAX Fabric Deployment"                     -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "Tenant       : $($cfg.TenantId)"
Write-Host "Workspace    : $($cfg.WorkspaceId)"
Write-Host "Artifacts    : $ArtifactsPath"

$notebookFile = Join-Path $ArtifactsPath 'pax_fabric_notebook.ipynb'
if (-not (Test-Path $notebookFile)) {
    throw "Notebook file not found at '$notebookFile'. See artifacts\NOTEBOOK_PLACEHOLDER.txt for instructions."
}

# ------------------------------------------------------------------------
# 2. Sign in and acquire Fabric access token
# ------------------------------------------------------------------------
#   - Uses Az CLI as the primary path (Az PowerShell as fallback).
#   - Triggers `az login` in-script when no session is present — no need for
#     the operator to run it beforehand.
#   - `az login` is invoked WITHOUT `--tenant`: the account picker lets the
#     user select any identity they have; the token below is then requested
#     explicitly for `$cfg.TenantId`, so multi-tenant guests still work.
#   - `--allow-no-subscriptions` bypasses subscription selection entirely —
#     Fabric API access does not require an Azure subscription.
Write-Host "`nChecking Az CLI sign-in state..."

$azCmd = Get-Command az -ErrorAction SilentlyContinue
if (-not $azCmd) {
    Write-Warning "Az CLI ('az') not found on PATH. Falling back to Az PowerShell."
}

$signedInUser = $null
if ($azCmd) {
    try {
        $acctJson = az account show --output json 2>$null
        if ($LASTEXITCODE -eq 0 -and $acctJson) {
            $signedInUser = ($acctJson | ConvertFrom-Json).user.name
        }
    } catch { }

    if (-not $signedInUser) {
        Write-Host "  No active Az CLI session. Launching 'az login' - a browser window will open..." -ForegroundColor Yellow
        az login --allow-no-subscriptions --only-show-errors | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "az login failed. Re-run the script after resolving the sign-in error above."
        }
        $acctJson = az account show --output json 2>$null
        if ($LASTEXITCODE -eq 0 -and $acctJson) {
            $signedInUser = ($acctJson | ConvertFrom-Json).user.name
        }
    }
    if ($signedInUser) { Write-Host "  Signed in as: $signedInUser" }
}

Write-Host "Acquiring Fabric access token..."
$token = $null
if ($azCmd) {
    try {
        $azOutput = az account get-access-token `
            --resource "https://api.fabric.microsoft.com" `
            --tenant   $cfg.TenantId `
            --output   json 2>$null
        if ($LASTEXITCODE -eq 0 -and $azOutput) {
            $token = ($azOutput | ConvertFrom-Json).accessToken
        }
    } catch { }
}

if (-not $token) {
    Write-Host "  Falling back to Az PowerShell..."
    Import-Module Az.Accounts -ErrorAction Stop
    if (-not (Get-AzContext)) { Connect-AzAccount -TenantId $cfg.TenantId | Out-Null }
    $tokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com" -TenantId $cfg.TenantId -ErrorAction SilentlyContinue
    if (-not $tokenObj) {
        $tokenObj = Get-AzAccessToken -ResourceUrl "https://api.fabric.microsoft.com"
    }
    $token = $tokenObj.Token
}
if (-not $token) {
    throw "Failed to acquire Fabric access token. Confirm the signed-in account has access to tenant $($cfg.TenantId) and is a Workspace Admin on workspace $($cfg.WorkspaceId)."
}

$headers = @{
    "Authorization" = "Bearer $token"
    "Content-Type"  = "application/json"
}
$fabricBase = "https://api.fabric.microsoft.com/v1"
$wsUrl      = "$fabricBase/workspaces/$($cfg.WorkspaceId)"
Write-Host "  Token acquired."

# ------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------
function ConvertTo-Base64Utf8 {
    param([Parameter(Mandatory)][string] $Text)
    [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Text))
}

function Get-FabricItem {
    param([Parameter(Mandatory)][string] $Type,
          [Parameter(Mandatory)][string] $Name)
    try {
        $resp = Invoke-RestMethod -Uri "$wsUrl/items?type=$Type" -Headers $headers -Method GET
        return $resp.value | Where-Object { $_.displayName -eq $Name } | Select-Object -First 1
    } catch {
        Write-Warning "Failed to list items of type $Type : $($_.Exception.Message)"
        return $null
    }
}

function Wait-FabricOperation {
    param([Parameter(Mandatory)][string] $OperationLocation,
          [int] $TimeoutSeconds = 600)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        Start-Sleep -Seconds 3
        $op = Invoke-RestMethod -Uri $OperationLocation -Headers $headers -Method GET
        switch ($op.status) {
            'Succeeded' { return $op }
            'Failed'    { throw "Fabric operation failed: $($op.error | ConvertTo-Json -Depth 5)" }
            'Cancelled' { throw "Fabric operation cancelled." }
        }
    }
    throw "Fabric operation timed out after ${TimeoutSeconds}s: $OperationLocation"
}

function New-FabricItem {
    param(
        [Parameter(Mandatory)][string] $Type,
        [Parameter(Mandatory)][string] $Name,
        [Parameter(Mandatory)][hashtable] $Definition
    )
    $body = @{
        displayName = $Name
        type        = $Type
        definition  = $Definition
    } | ConvertTo-Json -Depth 30

    try {
        $resp = Invoke-WebRequest -Uri "$wsUrl/items" `
            -Headers $headers -Method POST -Body $body -ContentType 'application/json' `
            -UseBasicParsing -ErrorAction Stop
    } catch {
        $msg = if ($_.ErrorDetails) { $_.ErrorDetails.Message } else { $_.Exception.Message }
        throw "Failed to create $Type '$Name': $msg"
    }

    if ($resp.StatusCode -eq 202) {
        $loc = $resp.Headers['Location']
        if ($loc -is [array]) { $loc = $loc[0] }
        Wait-FabricOperation -OperationLocation $loc | Out-Null
        return (Get-FabricItem -Type $Type -Name $Name)
    }
    return $resp.Content | ConvertFrom-Json
}

# ------------------------------------------------------------------------
# 3. Verify workspace access
# ------------------------------------------------------------------------
Write-Host "`nVerifying workspace access..."
try {
    $ws = Invoke-RestMethod -Uri $wsUrl -Headers $headers -Method GET
    Write-Host "  Workspace : '$($ws.displayName)'"
    Write-Host "  Capacity  : $($ws.capacityId)"
} catch {
    throw "Cannot read workspace $($cfg.WorkspaceId). Confirm you have Admin access. Details: $($_.Exception.Message)"
}

# ------------------------------------------------------------------------
# 4. Create (or reuse) Lakehouse
# ------------------------------------------------------------------------
Write-Host "`n[1/4] Lakehouse '$($cfg.LakehouseName)'..." -ForegroundColor Cyan
$lh = Get-FabricItem -Type 'Lakehouse' -Name $cfg.LakehouseName
if ($lh) {
    Write-Host "  Exists - reusing id=$($lh.id)"
} else {
    $body = @{
        displayName     = $cfg.LakehouseName
        creationPayload = @{ enableSchemas = $true }
    } | ConvertTo-Json -Depth 5
    $lh = Invoke-RestMethod -Uri "$wsUrl/lakehouses" -Headers $headers -Method POST -Body $body
    Write-Host "  Created id=$($lh.id) (schema-enabled)"
}
$lakehouseId = $lh.id

# ------------------------------------------------------------------------
# 5. Create Variable Library
# ------------------------------------------------------------------------
Write-Host "`n[2/4] Variable Library '$($cfg.VarLibName)'..." -ForegroundColor Cyan
$varLib = Get-FabricItem -Type 'VariableLibrary' -Name $cfg.VarLibName
if ($varLib) {
    Write-Host "  Exists - reusing id=$($varLib.id)"
    Write-Host "  NOTE: to update values, delete the Variable Library in Fabric UI and re-run this script,"
    Write-Host "        or edit the values directly in the Fabric UI."
} else {
    $variables = Get-Content (Join-Path $ArtifactsPath 'varlib-variables.json') -Raw | ConvertFrom-Json

    # Inject customer values directly into the primary (Default) value set.
    # The primary value set = the `value` field on each entry in variables.json.
    # This makes the built-in "Default value set" (which is always Active) show the real values in the Fabric UI.
    # Env constants only; run-shape switches (dates, Include*/Only*, GroupNames) live on the pipeline.
    $customerValues = [ordered]@{
        TenantId       = $cfg.AppRegistration.TenantId
        ClientId       = $cfg.AppRegistration.ClientId
        KeyVaultUri    = $cfg.KeyVault.KeyVaultUri
        KeyVaultSecret = $cfg.KeyVault.KeyVaultSecret
        GitHubRepoUrl  = $cfg.GitHub.GitHubRepoUrl
        GitHubRef      = $cfg.GitHub.GitHubRef
        TargetSchema   = $cfg.Defaults.TargetSchema
        KeepScratch    = $cfg.Defaults.KeepScratch
        MaxConcurrency = $cfg.Defaults.MaxConcurrency
        PartitionHours = $cfg.Defaults.PartitionHours
        RetentionDays  = $cfg.Defaults.RetentionDays
    }
    foreach ($v in $variables.variables) {
        if ($customerValues.Contains($v.name)) {
            $v.value = $customerValues[$v.name]
        }
    }
    $variablesJson = $variables | ConvertTo-Json -Depth 10

    $settingsJson = @{
        '$schema'       = 'https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/settings/1.0.0/schema.json'
        valueSetsOrder  = @()
    } | ConvertTo-Json

    $definition = @{
        parts = @(
            @{ path = 'variables.json'; payload = ConvertTo-Base64Utf8 $variablesJson; payloadType = 'InlineBase64' }
            @{ path = 'settings.json';  payload = ConvertTo-Base64Utf8 $settingsJson;  payloadType = 'InlineBase64' }
        )
    }
    $varLib = New-FabricItem -Type 'VariableLibrary' -Name $cfg.VarLibName -Definition $definition
    Write-Host "  Created id=$($varLib.id)"
}
$varLibId = $varLib.id

# ------------------------------------------------------------------------
# 6. Create Notebook (with default lakehouse pre-attached)
# ------------------------------------------------------------------------
Write-Host "`n[3/4] Notebook '$($cfg.NotebookName)'..." -ForegroundColor Cyan
$nb = Get-FabricItem -Type 'Notebook' -Name $cfg.NotebookName
if ($nb) {
    Write-Host "  Exists - reusing id=$($nb.id)"
} else {
    $ipynb = Get-Content $notebookFile -Raw | ConvertFrom-Json

    # Overwrite the default-lakehouse metadata so the notebook opens attached
    if (-not $ipynb.metadata) {
        $ipynb | Add-Member -MemberType NoteProperty -Name metadata -Value ([pscustomobject]@{})
    }
    if (-not $ipynb.metadata.dependencies) {
        $ipynb.metadata | Add-Member -MemberType NoteProperty -Name dependencies -Value ([pscustomobject]@{}) -Force
    }
    $ipynb.metadata.dependencies | Add-Member -MemberType NoteProperty -Name lakehouse -Value ([pscustomobject]@{
        default_lakehouse              = $lakehouseId
        default_lakehouse_name         = $cfg.LakehouseName
        default_lakehouse_workspace_id = $cfg.WorkspaceId
    }) -Force

    $ipynbJson = $ipynb | ConvertTo-Json -Depth 100

    # Point the notebook at THIS deployment's Variable Library.
    # notebookutils.variableLibrary.getLibrary(...) resolves by name at runtime,
    # so no UI attachment step is needed - we just rewrite the argument.
    $ipynbJson = $ipynbJson -replace 'getLibrary\(\\"[^"]*\\"\)', ('getLibrary(\"{0}\")' -f $cfg.VarLibName)

    $definition = @{
        format = 'ipynb'
        parts  = @(
            @{ path = 'notebook-content.ipynb'; payload = ConvertTo-Base64Utf8 $ipynbJson; payloadType = 'InlineBase64' }
        )
    }
    $nb = New-FabricItem -Type 'Notebook' -Name $cfg.NotebookName -Definition $definition
    Write-Host "  Created id=$($nb.id) (default lakehouse attached)"
}
$notebookId = $nb.id

# ------------------------------------------------------------------------
# 7. Create Data Pipeline
# ------------------------------------------------------------------------
Write-Host "`n[4/4] Data Pipeline '$($cfg.PipelineName)'..." -ForegroundColor Cyan
$pl = Get-FabricItem -Type 'DataPipeline' -Name $cfg.PipelineName
if ($pl) {
    Write-Host "  Exists - reusing id=$($pl.id)"
    Write-Host "  NOTE: Existing pipeline definitions are not updated by this script." -ForegroundColor Yellow
    Write-Host "        Ensure the pipeline has a string Dashboard parameter (default empty)" -ForegroundColor Yellow
    Write-Host "        forwarded to the notebook as @pipeline().parameters.Dashboard." -ForegroundColor Yellow
} else {
    $pipelineTemplate = Get-Content (Join-Path $ArtifactsPath 'pipeline-content.json') -Raw
    $pipelineJson = $pipelineTemplate `
        -replace '__NOTEBOOK_ID__',  $notebookId `
        -replace '__WORKSPACE_ID__', $cfg.WorkspaceId

    $definition = @{
        parts = @(
            @{ path = 'pipeline-content.json'; payload = ConvertTo-Base64Utf8 $pipelineJson; payloadType = 'InlineBase64' }
        )
    }
    $pl = New-FabricItem -Type 'DataPipeline' -Name $cfg.PipelineName -Definition $definition
    Write-Host "  Created id=$($pl.id)"
}
$pipelineId = $pl.id

# ------------------------------------------------------------------------
# 8. Post-deployment summary
# ------------------------------------------------------------------------
Write-Host "`n==========================================" -ForegroundColor Green
Write-Host " Deployment complete"                        -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green
Write-Host "Workspace ID     : $($cfg.WorkspaceId)"
Write-Host "Lakehouse ID     : $lakehouseId"
Write-Host "VariableLib ID   : $varLibId"
Write-Host "Notebook ID      : $notebookId"
Write-Host "Pipeline ID      : $pipelineId"

Write-Host "`n--- Manual steps still required ---" -ForegroundColor Yellow
Write-Host "1. Enable network access from Fabric to the Key Vault via Managed Private Endpoint:"
Write-Host "   Fabric workspace -> Manage -> Managed Private Endpoints -> New -> Key Vault"
Write-Host "   Then approve the pending request in: Key Vault -> Networking -> Private endpoint connections"
Write-Host "   (The App Registration does NOT need any Key Vault RBAC role.)"
Write-Host ""
Write-Host "2. Confirm Microsoft Graph Application permissions on App Registration $($cfg.AppRegistration.ClientId):"
Write-Host "   - AuditLogsQuery.Read.All"
Write-Host "   - AuditLogsQuery-Exchange.Read.All"
Write-Host "   - AuditLogsQuery-OneDrive.Read.All"
Write-Host "   - AuditLogsQuery-SharePoint.Read.All"
Write-Host "   - User.Read.All"
Write-Host "   - Organization.Read.All"
Write-Host "   - GroupMember.Read.All"
Write-Host "   All must be Application permissions with tenant admin consent granted."
Write-Host ""
Write-Host "3. Store the App Registration client secret in Key Vault as '$($cfg.KeyVault.KeyVaultSecret)'"
Write-Host "   (the user running this step needs 'Key Vault Secrets User' on the vault)."
Write-Host ""
Write-Host "4. Open the Pipeline '$($cfg.PipelineName)' in Fabric and click Run to verify end-to-end."
Write-Host "   (Run the Pipeline, not the Notebook directly - this is the same path a scheduled run uses.)"
Write-Host "   Once the first Pipeline run succeeds, set a recurring schedule: Pipeline -> Schedule."
