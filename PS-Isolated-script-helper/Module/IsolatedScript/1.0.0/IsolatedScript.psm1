<#
.SYNOPSIS
  Run PowerShell in a fresh process (no profile) with deterministic module versions.

.DESCRIPTION
  This module provides three helpers:
    • Invoke-IsolatedScript   — run a .ps1 in a NEW, clean PowerShell process.
    • Invoke-IsolatedCommand  — run ONE command in a NEW, clean PowerShell process (supports -CommandSplat).
    • Invoke-IsolatedSequence — run MULTIPLE commands in the SAME clean process (writes a temp .ps1 behind the scenes).

  Why? To eliminate "module bleed" and version conflicts (e.g., different Microsoft.Graph.* versions),
  to pin exact module versions, and to keep auth contexts (like Connect-MgGraph) scoped to the child.

  Features
    • Honors #requires -Modules (scripts).
    • Lets you add/override module requirements (exact or ranges).
    • Pins exact versions by importing the specific module manifest path.
    • Optional Install-If-Missing from PSGallery (CurrentUser).
    • VendoredModulesPath to prepend a minimal module cache.
    • PS 5.1 + PS 7+ compatible (JSON parsing fallback).
    • Pre-imports Microsoft.PowerShell.* core modules in the child so Join-Path, etc. work.

.INSTALL
  Save this file as:
    PS 7+:   $HOME\Documents\PowerShell\Modules\IsolatedScript\1.0.0\IsolatedScript.psm1
    PS 5.1:  $HOME\Documents\WindowsPowerShell\Modules\IsolatedScript\1.0.0\IsolatedScript.psm1
  Update your manifest to export:
    FunctionsToExport = 'Invoke-IsolatedScript','Invoke-IsolatedCommand','Invoke-IsolatedSequence'

.SAMPLES (copy/paste)
  # --- Run a .ps1, honoring its #requires -Modules ---
  Invoke-IsolatedScript -ScriptPath .\MyScript.ps1

  # --- Run a .ps1 but pin Graph.Auth exactly (no bleed) ---
  Invoke-IsolatedScript -ScriptPath .\MyScript.ps1 `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.31.0' })

  # --- Use a vendored modules cache (only what you saved is visible to the child) ---
  $vend = "$PSScriptRoot\Modules"
  Invoke-IsolatedScript -ScriptPath .\MyScript.ps1 -VendoredModulesPath $vend -EnableAutoload

  # --- Run a single command (no .ps1): Invoke-ZTAssessment with Graph.Auth 2.2.0 and a switch ---
  Invoke-IsolatedCommand -CommandName Invoke-ZTAssessment `
    -PreloadModules @('ZeroTrustAssessmentV2') -EnableAutoload `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.2.0' }) `
    -CommandSplat @{ Interactive = $true; Days = 1 }

  # --- Connect-MgGraph (device code), inside isolated child, then exit (single command) ---
  Invoke-IsolatedCommand -CommandName Connect-MgGraph `
    -EnableAutoload `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.2.0' }) `
    -CommandArgs @('-UseDeviceCode','-Scopes','User.Read.All','-TenantId','contoso.onmicrosoft.com','-ContextScope','Process')

  # --- Run multiple commands in the SAME child (preserve Graph context) ---
  Invoke-IsolatedSequence `
    -Statements @(
      "Connect-MgGraph -UseDeviceCode -Scopes 'User.Read.All' -TenantId 'contoso.onmicrosoft.com' -ContextScope Process",
      "Invoke-ZTAssessment -Interactive -Days 1"
    ) `
    -VendoredModulesPath "$PSScriptRoot\Modules-ZT-Graph220" `
    -PreloadModules @('ZeroTrustAssessmentV2') `
    -EnableAutoload `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.2.0' })

.NOTES
  • Each Invoke-IsolatedCommand call is a NEW process (context doesn’t persist).
    Use Invoke-IsolatedSequence to run multiple commands in ONE process.
  • If you see “parameter could not be found that accepts argument '-'”, ensure -CommandArgs is an ARRAY
    of strings (each token separate), or use -CommandSplat (recommended).
  • If you hit “assembly with the same name is already loaded”, vendor only the versions you need and pass
    -VendoredModulesPath so the child can’t see other versions.
#>

<#
.SYNOPSIS
  Run a .ps1 in a fresh PowerShell process with deterministic module versions.
.DESCRIPTION
  Launches a brand-new PowerShell host (pwsh or powershell) with -NoProfile and executes the script.
  Honors your script’s #requires -Modules and/or any -ModuleRequirement pins you pass. Supports vendored
  module path, install-if-missing, and turns off autoload by default for determinism.
.PARAMETER ScriptPath
  Path to the .ps1 file to execute.
.PARAMETER ScriptArgs
  Array of arguments to pass to your script (tokens).
.PARAMETER ModuleRequirement
  Array of hashtables describing required modules and versions (exact or ranges).
  Examples:
    @{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.2.0' }
    @{ Name='PnP.PowerShell'; MinimumVersion='2.4.0' }
.PARAMETER ConflictPolicy
  ScriptWins (default) or ExternalWins for merging #requires with ModuleRequirement.
.PARAMETER VendoredModulesPath
  Folder prepended to PSModulePath for the child (e.g., output of Save-Module).
.PARAMETER InstallIfMissing
  If set, Install-Module missing requirements to CurrentUser inside the child.
.PARAMETER EnableAutoload
  Allow automatic module autoloading in the child (default disabled).
.PARAMETER IgnoreScriptRequires
  Ignore #requires -Modules in the script and ONLY use -ModuleRequirement.
.PARAMETER PwshPath
  Host to launch. If omitted, auto-picks pwsh (7+) then powershell (5.1).
.EXAMPLE
  Invoke-IsolatedScript -ScriptPath .\Test-GraphAuth-230.ps1
.EXAMPLE
  Invoke-IsolatedScript -ScriptPath .\My.ps1 -VendoredModulesPath "$PSScriptRoot\Modules" -EnableAutoload
#>
function Invoke-IsolatedScript {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)] [string]$ScriptPath,
    [string[]] $ScriptArgs,
    [hashtable[]] $ModuleRequirement,
    [ValidateSet('ScriptWins','ExternalWins')] [string] $ConflictPolicy = 'ScriptWins',
    [string] $VendoredModulesPath,
    [switch] $InstallIfMissing,
    [switch] $EnableAutoload,
    [switch] $IgnoreScriptRequires,
    [string] $PwshPath
  )

  if (-not (Test-Path -LiteralPath $ScriptPath)) {
    throw "Script not found: $ScriptPath"
  }

  # Auto-select child host if caller didn’t specify one (pwsh → fallback to powershell 5.1)
  if (-not $PSBoundParameters.ContainsKey('PwshPath')) {
    if (Get-Command pwsh -ErrorAction SilentlyContinue) { $PwshPath = 'pwsh' } else { $PwshPath = 'powershell' }
  }

  # Parse #requires -Modules from the script
  function Get-RequiresModules {
    param([string]$Path)
    $text = Get-Content -LiteralPath $Path -Raw
    $reqs = @()

    foreach ($m in [regex]::Matches($text, '(?im)^\s*#\s*requires\s+-modules\s+(.+)$')) {
      $val = $m.Groups[1].Value.Trim()

      if ($val -like '@{*') {
        $parts = [regex]::Split($val, '\}\s*,') | ForEach-Object { $_.Trim(" ,`r`n") }
        foreach ($p in $parts) {
          if ($p -notmatch '@\{') { continue }
          $name = ([regex]::Match($p, "(?is)ModuleName\s*=\s*['""]?([^'"";]+)")).Groups[1].Value.Trim()
          $reqv = ([regex]::Match($p, "(?is)(RequiredVersion|ModuleVersion)\s*=\s*['""]?([^'"";]+)")).Groups[2].Value.Trim()
          $minv = ([regex]::Match($p, "(?is)MinimumVersion\s*=\s*['""]?([^'"";]+)")).Groups[1].Value.Trim()
          $maxv = ([regex]::Match($p, "(?is)MaximumVersion\s*=\s*['""]?([^'"";]+)")).Groups[1].Value.Trim()
          if ($name) {
            $h = @{ Name = $name }
            if ($reqv) { $h.RequiredVersion = $reqv }   # treat as exact pin
            if ($minv) { $h.MinimumVersion  = $minv }
            if ($maxv) { $h.MaximumVersion  = $maxv }
            $reqs += $h
          }
        }
      } else {
        $val -split ',' | ForEach-Object {
          $n = $_.Trim(" `'`"`t")
          if ($n) { $reqs += @{ Name = $n } }
        }
      }
    }
    ,$reqs
  }

  # 1) Collect requirements
  $scriptReqs = if ($IgnoreScriptRequires) { @() } else { Get-RequiresModules -Path $ScriptPath }

  # 2) Merge with external per ConflictPolicy
  $byName = @{}
  foreach ($r in $scriptReqs) { $byName[$r.Name] = $r }

  if ($ModuleRequirement) {
    foreach ($r in $ModuleRequirement) {
      $name = if ($r.ContainsKey('Name')) { $r['Name'] } elseif ($r.ContainsKey('ModuleName')) { $r['ModuleName'] } else { $null }
      if (-not $name) { throw "ModuleRequirement item missing 'Name'." }
      $item = @{}; foreach ($k in $r.Keys) { $item[$k] = $r[$k] }; $item['Name'] = $name
      if ($ConflictPolicy -eq 'ExternalWins' -or -not $byName.ContainsKey($name)) { $byName[$name] = $item }
    }
  }

  $finalReqs = New-Object System.Collections.Generic.List[object]
  foreach ($v in $byName.Values) { [void]$finalReqs.Add($v) }

  # 3) Build payload for the child
  $resolvedScript = (Resolve-Path -LiteralPath $ScriptPath).Path
  if ($null -eq $ScriptArgs) { $ScriptArgs = @() }

  $payload = @{
    ScriptPath    = $resolvedScript
    ScriptArgs    = $ScriptArgs
    Requirements  = $finalReqs
    Vendored      = $VendoredModulesPath
    Install       = [bool]$InstallIfMissing
    Autoload      = [bool]$EnableAutoload
  } | ConvertTo-Json -Depth 8 -Compress
  $payloadEnc = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload))

  # 4) Child bootstrap (single-quoted; replace token to avoid premature expansion)
  $child = @'
$ErrorActionPreference = 'Stop'

$raw = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('__PAYLOAD__'))

function ConvertTo-HashtableDeep($input) {
  if ($null -eq $input) { return $null }
  if ($input -is [System.Collections.IDictionary]) {
    $h=@{}; foreach ($k in $input.Keys) { $h[$k] = ConvertTo-HashtableDeep $input[$k] }; return $h
  }
  if ($input -is [System.Collections.IEnumerable] -and -not ($input -is [string])) {
    $list = @(); foreach ($i in $input) { $list += ,(ConvertTo-HashtableDeep $i) }; return $list
  }
  if ($input -is [psobject]) {
    $h=@{}; foreach ($p in $input.PSObject.Properties) { $h[$p.Name] = ConvertTo-HashtableDeep $p.Value }; return $h
  }
  return $input
}

try {
  $cfg = $raw | ConvertFrom-Json -AsHashtable
} catch {
  $cfg = ConvertTo-HashtableDeep ($raw | ConvertFrom-Json)
}

if (-not $cfg.Autoload) { $PSModuleAutoloadingPreference = 'None' }

# Ensure core cmdlets even with autoload off
try { Import-Module Microsoft.PowerShell.Management -ErrorAction Stop } catch {}
try { Import-Module Microsoft.PowerShell.Utility    -ErrorAction Stop } catch {}

if ($cfg.Vendored -and (Test-Path -LiteralPath $cfg.Vendored)) {
  $env:PSModulePath = $cfg.Vendored + ';' + $env:PSModulePath
}

function Import-Exact([hashtable]$r) {
  $name = $r.Name
  if (-not $name) { throw 'Requirement missing Name' }

  # Exact pin (RequiredVersion or ModuleVersion)
  $ver = if ($r.ContainsKey('RequiredVersion')) { $r.RequiredVersion } elseif ($r.ContainsKey('ModuleVersion')) { $r.ModuleVersion } else { $null }

  if ($ver) {
    # Import the specific manifest/module path for that version
    $target = Get-Module -ListAvailable -Name $name |
              Where-Object { $_.Version -eq [version]$ver } |
              Select-Object -First 1
    if (-not $target) { throw "Requested $name $ver not found on PSModulePath." }
    Import-Module -Name $target.Path -Force -ErrorAction Stop
  }
  elseif ($r.MinimumVersion -or $r.MaximumVersion) {
    $p = @{}
    if ($r.MinimumVersion) { $p.MinimumVersion = $r.MinimumVersion }
    if ($r.MaximumVersion) { $p.MaximumVersion = $r.MaximumVersion }
    Import-Module $name @p -ErrorAction Stop
  }
  else {
    Import-Module $name -ErrorAction Stop
  }

  # Verify the loaded version satisfies the constraint
  $loaded = Get-Module -Name $name | Select-Object -First 1
  if (-not $loaded) { throw "Failed to import module $name" }
  if ($r.ContainsKey('RequiredVersion') -and $loaded.Version -ne [version]$r.RequiredVersion) { throw "Loaded $name $($loaded.Version), wanted $($r.RequiredVersion)" }
  if ($r.ContainsKey('ModuleVersion')   -and $loaded.Version -ne [version]$r.ModuleVersion)   { throw "Loaded $name $($loaded.Version), wanted $($r.ModuleVersion)" }
  if ($r.MinimumVersion -and $loaded.Version -lt [version]$r.MinimumVersion)                   { throw "Loaded $name $($loaded.Version) < min $($r.MinimumVersion)" }
  if ($r.MaximumVersion -and $loaded.Version -gt [version]$r.MaximumVersion)                   { throw "Loaded $name $($loaded.Version) > max $($r.MaximumVersion)" }
}

# Optional install step
if ($cfg.Install -and $cfg.Requirements -and $cfg.Requirements.Count -gt 0) {
  try { Import-Module PowerShellGet -ErrorAction Stop } catch {}
  foreach ($r in $cfg.Requirements) {
    $name = $r.Name
    $need = $true
    if ($r.ContainsKey('RequiredVersion')) {
      $need = -not (Get-Module -ListAvailable -Name $name | Where-Object { $_.Version -eq [version]$r.RequiredVersion })
    } elseif ($r.ContainsKey('ModuleVersion')) {
      $need = -not (Get-Module -ListAvailable -Name $name | Where-Object { $_.Version -eq [version]$r.ModuleVersion })
    } elseif ($r.MinimumVersion -or $r.MaximumVersion) {
      $need = -not (Get-Module -ListAvailable -Name $name | Where-Object {
        $ok = $true
        if ($r.MinimumVersion) { $ok = $ok -and ($_.Version -ge [version]$r.MinimumVersion) }
        if ($r.MaximumVersion) { $ok = $ok -and ($_.Version -le [version]$r.MaximumVersion) }
        $ok
      })
    } else {
      $need = -not (Get-Module -ListAvailable -Name $name)
    }
    if ($need) {
      Install-Module -Name $name -Scope CurrentUser -Force -AllowClobber | Out-Null
    }
  }
}

# Preload & verify requirements to avoid wrong autoloads
if ($cfg.Requirements -and $cfg.Requirements.Count -gt 0) {
  foreach ($r in $cfg.Requirements) { Import-Exact $r }
}

# Run the target script
$args = if ($cfg.ScriptArgs) { $cfg.ScriptArgs } else { @() }
& $cfg.ScriptPath @args
'@

  $child = $child.Replace('__PAYLOAD__', $payloadEnc)

  # Launch child
  $childEnc = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($child))
  & $PwshPath -NoLogo -NoProfile -ExecutionPolicy Bypass -EncodedCommand $childEnc
}

<#
.SYNOPSIS
  Run ONE command (no .ps1) in a fresh, isolated process.
.DESCRIPTION
  Launches a brand-new PowerShell host (pwsh or powershell) with -NoProfile, optionally pins module versions,
  preloads modules, and then invokes the command. Use -CommandSplat for unambiguous parameter passing.
.PARAMETER CommandName
  The command to run (e.g., Connect-MgGraph, Invoke-ZTAssessment).
.PARAMETER CommandArgs
  Array of tokens (strings) to pass positionally (e.g., @('-Interactive','-Days','1')).
.PARAMETER CommandSplat
  Hashtable of parameters to splat to the command (recommended).
.PARAMETER ModuleRequirement
  Array of hashtables describing modules to pin (exact or ranges).
.PARAMETER PreloadModules
  Modules to Import-Module before running the command (load-order control).
.PARAMETER VendoredModulesPath
  Folder prepended to PSModulePath for the child (e.g., a Save-Module cache).
.PARAMETER InstallIfMissing
  Install missing requirements to CurrentUser inside the child.
.PARAMETER EnableAutoload
  Allow automatic module autoloading in the child (default disabled).
.PARAMETER PwshPath
  Host to launch. If omitted, auto-picks pwsh (7+) then powershell (5.1).
.PARAMETER WorkingDirectory
  Starting directory inside the child process.
.EXAMPLE
  Invoke-IsolatedCommand -CommandName Invoke-ZTAssessment `
    -PreloadModules @('ZeroTrustAssessmentV2') -EnableAutoload `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.2.0' }) `
    -CommandSplat @{ Interactive = $true; Days = 1 }
.EXAMPLE
  Invoke-IsolatedCommand -CommandName Get-Date -CommandArgs @('-Format','o')
#>
function Invoke-IsolatedCommand {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)] [string]$CommandName,
    [string[]] $CommandArgs,
    [hashtable] $CommandSplat,
    [hashtable[]] $ModuleRequirement,
    [string[]] $PreloadModules,
    [string] $VendoredModulesPath,
    [switch] $InstallIfMissing,
    [switch] $EnableAutoload,
    [string] $PwshPath,
    [string] $WorkingDirectory
  )

  # Auto-select child host if not specified
  if (-not $PSBoundParameters.ContainsKey('PwshPath')) {
    if (Get-Command pwsh -ErrorAction SilentlyContinue) { $PwshPath = 'pwsh' } else { $PwshPath = 'powershell' }
  }

  if ($null -eq $CommandArgs) { $CommandArgs = @() }
  if ($null -eq $ModuleRequirement) { $ModuleRequirement = @() }
  if ($null -eq $PreloadModules) { $PreloadModules = @() }

  $payload = @{
    CommandName  = $CommandName
    CommandArgs  = $CommandArgs
    CommandSplat = $CommandSplat
    Requirements = $ModuleRequirement
    PreloadMods  = $PreloadModules
    Vendored     = $VendoredModulesPath
    Install      = [bool]$InstallIfMissing
    Autoload     = [bool]$EnableAutoload
    WorkDir      = $WorkingDirectory
  } | ConvertTo-Json -Depth 8 -Compress
  $payloadEnc = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($payload))

  $child = @'
$ErrorActionPreference = 'Stop'

$raw = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('__PAYLOAD__'))

function ConvertTo-HashtableDeep($input) {
  if ($null -eq $input) { return $null }
  if ($input -is [System.Collections.IDictionary]) { $h=@{}; foreach ($k in $input.Keys) { $h[$k] = ConvertTo-HashtableDeep $input[$k] }; return $h }
  if ($input -is [System.Collections.IEnumerable] -and -not ($input -is [string])) { $list=@(); foreach ($i in $input) { $list += ,(ConvertTo-HashtableDeep $i) }; return $list }
  if ($input -is [psobject]) { $h=@{}; foreach ($p in $input.PSObject.Properties) { $h[$p.Name] = ConvertTo-HashtableDeep $p.Value }; return $h }
  return $input
}

try { $cfg = $raw | ConvertFrom-Json -AsHashtable } catch { $cfg = ConvertTo-HashtableDeep ($raw | ConvertFrom-Json) }

if (-not $cfg.Autoload) { $PSModuleAutoloadingPreference = 'None' }

# Ensure core cmdlets even with autoload off
try { Import-Module Microsoft.PowerShell.Management -ErrorAction Stop } catch {}
try { Import-Module Microsoft.PowerShell.Utility    -ErrorAction Stop } catch {}

if ($cfg.WorkDir) { try { Set-Location -LiteralPath $cfg.WorkDir } catch {} }

if ($cfg.Vendored -and (Test-Path -LiteralPath $cfg.Vendored)) {
  $env:PSModulePath = $cfg.Vendored + ';' + $env:PSModulePath
}

function Import-Exact([hashtable]$r) {
  $name = $r.Name
  if (-not $name) { throw 'Requirement missing Name' }

  $ver = if ($r.ContainsKey('RequiredVersion')) { $r.RequiredVersion } elseif ($r.ContainsKey('ModuleVersion')) { $r.ModuleVersion } else { $null }

  if ($ver) {
    $target = Get-Module -ListAvailable -Name $name |
              Where-Object { $_.Version -eq [version]$ver } |
              Select-Object -First 1
    if (-not $target) { throw "Requested $name $ver not found on PSModulePath." }
    Import-Module -Name $target.Path -Force -ErrorAction Stop
  }
  elseif ($r.MinimumVersion -or $r.MaximumVersion) {
    $p = @{}
    if ($r.MinimumVersion) { $p.MinimumVersion = $r.MinimumVersion }
    if ($r.MaximumVersion) { $p.MaximumVersion = $r.MaximumVersion }
    Import-Module $name @p -ErrorAction Stop
  }
  else {
    Import-Module $name -ErrorAction Stop
  }

  $loaded = Get-Module -Name $name | Select-Object -First 1
  if (-not $loaded) { throw "Failed to import module $name" }
  if ($r.ContainsKey('RequiredVersion') -and $loaded.Version -ne [version]$r.RequiredVersion) { throw "Loaded $name $($loaded.Version), wanted $($r.RequiredVersion)" }
  if ($r.ContainsKey('ModuleVersion')   -and $loaded.Version -ne [version]$r.ModuleVersion)   { throw "Loaded $name $($loaded.Version), wanted $($r.ModuleVersion)" }
  if ($r.MinimumVersion -and $loaded.Version -lt [version]$r.MinimumVersion)                   { throw "Loaded $name $($loaded.Version) < min $($r.MinimumVersion)" }
  if ($r.MaximumVersion -and $loaded.Version -gt [version]$r.MaximumVersion)                   { throw "Loaded $name $($loaded.Version) > max $($r.MaximumVersion)" }
}

# Optional install of missing requirements
if ($cfg.Install -and $cfg.Requirements -and $cfg.Requirements.Count -gt 0) {
  try { Import-Module PowerShellGet -ErrorAction Stop } catch {}
  foreach ($r in $cfg.Requirements) {
    $name = $r.Name
    $need = $true
    if ($r.ContainsKey('RequiredVersion')) {
      $need = -not (Get-Module -ListAvailable -Name $name | Where-Object { $_.Version -eq [version]$r.RequiredVersion })
    } elseif ($r.ContainsKey('ModuleVersion')) {
      $need = -not (Get-Module -ListAvailable -Name $name | Where-Object { $_.Version -eq [version]$r.ModuleVersion })
    } elseif ($r.MinimumVersion -or $r.MaximumVersion) {
      $need = -not (Get-Module -ListAvailable -Name $name | Where-Object {
        $ok = $true
        if ($r.MinimumVersion) { $ok = $ok -and ($_.Version -ge [version]$r.MinimumVersion) }
        if ($r.MaximumVersion) { $ok = $ok -and ($_.Version -le [version]$r.MaximumVersion) }
        $ok
      })
    } else {
      $need = -not (Get-Module -ListAvailable -Name $name)
    }
    if ($need) {
      Install-Module -Name $name -Scope CurrentUser -Force -AllowClobber | Out-Null
    }
  }
}

# Pin/import requested modules FIRST, then preload any other module(s)
if ($cfg.Requirements -and $cfg.Requirements.Count -gt 0) {
  foreach ($r in $cfg.Requirements) { Import-Exact $r }
}
if ($cfg.PreloadMods -and $cfg.PreloadMods.Count -gt 0) {
  foreach ($m in $cfg.PreloadMods) { try { Import-Module $m -ErrorAction Stop } catch {} }
}

# Finally, run the command (with sanitized splat)
$cmd   = $cfg.CommandName
$args  = if ($cfg.CommandArgs) { $cfg.CommandArgs } else { @() }
$splat = $cfg.CommandSplat

# Drop null / empty entries from the splat to avoid binding errors like: Get-Date -Date $null
$clean = @{}
if ($splat -is [System.Collections.IDictionary]) {
  foreach ($k in $splat.Keys) {
    $v = $splat[$k]
    if ($null -ne $v -and -not ($v -is [string] -and $v.Trim() -eq '')) {
      $clean[$k] = $v
    }
  }
}

if ($clean.Count -gt 0) {
  & $cmd @clean
} else {
  & $cmd @args
}
'@

  $child = $child.Replace('__PAYLOAD__', $payloadEnc)
  $childEnc = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($child))
  & $PwshPath -NoLogo -NoProfile -ExecutionPolicy Bypass -EncodedCommand $childEnc
}

<#
.SYNOPSIS
  Run MULTIPLE statements in ONE fresh, isolated process (preserves context between lines).
.DESCRIPTION
  Writes a temporary .ps1 with your statements, then calls Invoke-IsolatedScript so all version
  pinning and vendored path logic applies. Perfect for: Connect-MgGraph then Invoke-ZTAssessment.
.PARAMETER Statements
  Array of lines (strings) to execute in order in the child.
.PARAMETER ModuleRequirement
  Array of hashtables describing modules to pin (exact or ranges).
.PARAMETER PreloadModules
  Modules to Import-Module before running your statements.
.PARAMETER VendoredModulesPath
  Folder prepended to PSModulePath for the child (e.g., a Save-Module cache).
.PARAMETER InstallIfMissing
  Install missing requirements to CurrentUser inside the child.
.PARAMETER EnableAutoload
  Allow automatic module autoloading in the child (default disabled).
.PARAMETER PwshPath
  Host to launch. If omitted, auto-picks pwsh (7+) then powershell (5.1).
.PARAMETER WorkingDirectory
  Starting directory inside the child process.
.PARAMETER KeepTemp
  If set, keep the generated temp .ps1 (path is written to output).
.EXAMPLE
  Invoke-IsolatedSequence `
    -Statements @(
      "Connect-MgGraph -UseDeviceCode -Scopes 'User.Read.All' -TenantId 'contoso.onmicrosoft.com' -ContextScope Process",
      "Invoke-ZTAssessment -Interactive -Days 1"
    ) `
    -VendoredModulesPath "$PSScriptRoot\Modules-ZT-Graph220" `
    -PreloadModules @('ZeroTrustAssessmentV2') `
    -EnableAutoload `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.2.0' })
#>
function Invoke-IsolatedSequence {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory, Position=0)]
    [string[]] $Statements,

    [hashtable[]] $ModuleRequirement,
    [string[]]   $PreloadModules,
    [string]     $VendoredModulesPath,

    [switch] $InstallIfMissing,
    [switch] $EnableAutoload,
    [string] $PwshPath,
    [string] $WorkingDirectory,
    [switch] $KeepTemp
  )

  # Auto-select child host if not specified
  if (-not $PSBoundParameters.ContainsKey('PwshPath')) {
    if (Get-Command pwsh -ErrorAction SilentlyContinue) { $PwshPath = 'pwsh' } else { $PwshPath = 'powershell' }
  }

  # Build the temporary script
  $tmp = Join-Path ([IO.Path]::GetTempPath()) ("iso-seq-" + [guid]::NewGuid().ToString('N') + ".ps1")
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)

  $lines = New-Object System.Collections.Generic.List[string]
  $lines.Add("# generated by Invoke-IsolatedSequence $(Get-Date -Format o)")
  if ($WorkingDirectory) {
    $escaped = $WorkingDirectory.Replace('"','""')
    $lines.Add("Set-Location -LiteralPath `"$escaped`"")
  }
  if ($PreloadModules) {
    foreach ($m in $PreloadModules) { $lines.Add("Import-Module $m -ErrorAction Stop") }
  }
  foreach ($s in $Statements) { $lines.Add($s) }

  [IO.File]::WriteAllLines($tmp, $lines, $utf8NoBom)

  try {
    Invoke-IsolatedScript -ScriptPath $tmp `
      -ModuleRequirement $ModuleRequirement `
      -VendoredModulesPath $VendoredModulesPath `
      -InstallIfMissing:$InstallIfMissing `
      -EnableAutoload:$EnableAutoload `
      -PwshPath $PwshPath
  }
  finally {
    if (-not $KeepTemp) {
      Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    } else {
      Write-Verbose "Temp script kept at $tmp"
      $tmp
    }
  }
}

Export-ModuleMember -Function Invoke-IsolatedScript,Invoke-IsolatedCommand,Invoke-IsolatedSequence
