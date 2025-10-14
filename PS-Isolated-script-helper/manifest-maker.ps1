# ---- Create module in the *real* Documents (handles OneDrive redirection) ----
$ModuleName = 'IsolatedScript'
$Version    = '1.0.0'
$Author     = 'Andy'

# 1) Resolve the OS "My Documents" (OneDrive if redirected)
$docs = [Environment]::GetFolderPath('MyDocuments')  # e.g. C:\Users\Andy\OneDrive - Contoso\Documents
Write-Host "My Documents resolves to: $docs"

# 2) Pick the per-user Modules path for this host
$subdir = if ($PSVersionTable.PSEdition -eq 'Desktop') { 'WindowsPowerShell\Modules' } else { 'PowerShell\Modules' }
$root   = Join-Path $docs $subdir

# 3) Create module folder
$modPath = Join-Path $root "$ModuleName\$Version"
New-Item -ItemType Directory -Path $modPath -Force | Out-Null
Write-Host "Creating module here: $modPath"

# 4) Place your module file here (paste your function into this file)
$psm1 = Join-Path $modPath "$ModuleName.psm1"
if (-not (Test-Path $psm1)) {
  Set-Content -LiteralPath $psm1 -Value @"
# $ModuleName.psm1
# Exported function(s):
# .SYNOPSIS: Run a script in a clean pwsh with deterministic module versions.
# (Paste your Invoke-IsolatedScript function below)
"@
  Write-Host "Stub created: $psm1"
}

# 5) Create manifest
$psd1 = Join-Path $modPath "$ModuleName.psd1"
if (-not (Test-Path $psd1)) {
  New-ModuleManifest -Path $psd1 `
    -RootModule "$ModuleName.psm1" `
    -ModuleVersion $Version `
    -Guid (New-Guid) `
    -Author $Author `
    -CompanyName '' `
    -Copyright "(c) $(Get-Date -f yyyy) $Author" `
    -Description 'Run scripts in a clean pwsh process with deterministic module versions.' `
    -PowerShellVersion '5.1' `
    -CompatiblePSEditions @('Core','Desktop') `
    -FunctionsToExport @('Invoke-IsolatedScript') `
    -CmdletsToExport @() -VariablesToExport @() -AliasesToExport @()
  Write-Host "Manifest created: $psd1"
}

# 6) Sanity checks
Write-Host "`nPSModulePath entries:"
$env:PSModulePath -split ';' | ForEach-Object { " - $_" }
Test-ModuleManifest $psd1 | Out-Null
Write-Host "`nModule structure ready. Try:  Import-Module $ModuleName -Force; Get-Command -Module $ModuleName"
# ---------------------------------------------------------------------------
