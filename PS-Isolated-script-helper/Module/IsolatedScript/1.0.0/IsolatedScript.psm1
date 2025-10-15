<#
.SYNOPSIS
  Run any PowerShell script in a fresh process (no profile) with deterministic module versions.

.DESCRIPTION
  Invoke-IsolatedScript launches a brand-new PowerShell host and (optionally):
    • Honors versions declared via   #requires -Modules
    • Adds/overrides module requirements you pass in
    • Prepends a “vendored” Modules folder (from Save-Module)
    • (Optional) installs missing modules from PSGallery (CurrentUser)
    • Pre-imports and verifies requested module versions
  This avoids version bleed (e.g., different Microsoft.Graph.* versions).

.INSTALL (user scope)
  Save as:
    PowerShell 7+:   $HOME\Documents\PowerShell\Modules\IsolatedScript\1.0.0\IsolatedScript.psm1
    Windows PS 5.1:  $HOME\Documents\WindowsPowerShell\Modules\IsolatedScript\1.0.0\IsolatedScript.psm1
  Create a manifest exporting Invoke-IsolatedScript and Invoke-IsolatedCommand
  (or keep Export-ModuleMember below).

.QUICK START (scripts)
  Import-Module IsolatedScript
  Invoke-IsolatedScript -ScriptPath .\MyScript.ps1                       # honor #requires
  Invoke-IsolatedScript -ScriptPath .\MyScript.ps1 `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.31.0' }) `
    -ConflictPolicy ExternalWins
  Invoke-IsolatedScript -ScriptPath .\MyScript.ps1 -VendoredModulesPath "$PSScriptRoot\Modules"

.QUICK START (commands / no .ps1)
  # Run a command like Connect-ZTAssessment in a clean child process, pinning Graph.Auth
  Invoke-IsolatedCommand -CommandName 'Connect-ZTAssessment' `
    -CommandArgs @('-TenantId','contoso.onmicrosoft.com') `
    -ModuleRequirement @(@{ Name='Microsoft.Graph.Authentication'; RequiredVersion='2.30.0' }) `
    -PreloadModules @('ZTAssessment')

.PARAMETERS (key ones)
  -ScriptPath            Path to the script you want to run.            (Invoke-IsolatedScript)
  -CommandName           Command to run (e.g., Connect-ZTAssessment).   (Invoke-IsolatedCommand)
  -ScriptArgs/CommandArgs  Array of args to pass through.
  -ModuleRequirement     Array of hashtables describing extra/override modules:
                           @{ Name='Module'; RequiredVersion='x.y.z' }
                           @{ Name='Module'; MinimumVersion='x'; MaximumVersion='y' }
  -ConflictPolicy        ScriptWins (default) | ExternalWins            (scripts only)
  -PreloadModules        Modules to import before invoking the command  (commands only)
  -VendoredModulesPath   Prepend this folder to PSModulePath in the child process.
  -InstallIfMissing      If set, install any missing requested modules from PSGallery.
  -EnableAutoload        Allow implicit autoload (default: off for determinism).
  -IgnoreScriptRequires  Ignore the script’s #requires -Modules.        (scripts only)
  -PwshPath              Host to launch. If omitted, auto-picks pwsh (7+) or powershell (5.1).

.NOTES
  The child runs with: -NoLogo -NoProfile -ExecutionPolicy Bypass
  Core modules are imported explicitly so cmdlets like Join-Path work even with autoload off.
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
    if (Get-Command pwsh -ErrorAction SilentlyContinue) {
      $PwshPath = 'pwsh'
    } else {
      $PwshPath = 'powershell'
    }
  }

  # --- Parse #requires -Modules from the script (names or @{ ModuleName='X'; ModuleVersion='1.2.3' }) ---
  function Get-RequiresModules {
    param([string]$Path)
    $text = Get-Content -LiteralPath $Path -Raw
    $reqs = @()

    foreach ($m in [regex]::Matches($text, '(?im)^\s*#\s*requires\s+-modules\s+(.+)$')) {
      $val = $m.Groups[1].Value.Trim()

      if ($val -like '@{*') {
        # Hashtable entries separated by '},'
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
    Requirements  = $finalReqs        # array of hashtables
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
  # PS7+ path
  $cfg = $raw | ConvertFrom-Json -AsHashtable
} catch {
  # PS5.1 fallback
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
    # Find the requested version on disk and import that specific manifest/module file
    $target = Get-Module -ListAvailable -Name $name |
              Where-Object { $_.Version -eq [version]$ver } |
              Select-Object -First 1
    if (-not $target) { throw "Requested $name $ver not found on PSModulePath." }

    Import-Module -Name $target.Path -Force -ErrorAction Stop
  }
  elseif ($r.MinimumVersion -or $r.MaximumVersion) {
    # Version range: let Import-Module pick a matching one, then verify
    $p = @{}
    if ($r.MinimumVersion) { $p.MinimumVersion = $r.MinimumVersion }
    if ($r.MaximumVersion) { $p.MaximumVersion = $r.MaximumVersion }
    Import-Module $name @p -ErrorAction Stop
  }
  else {
    # No version provided: import whatever is first on PSModulePath
    Import-Module $name -ErrorAction Stop
  }

  # Verify loaded version satisfies the constraint
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

# NEW: Run a single command (e.g., Connect-ZTAssessment) in a fresh child with pinned modules
function Invoke-IsolatedCommand {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)] [string]$CommandName,
    [string[]] $CommandArgs,
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

  # Exact pin (RequiredVersion or ModuleVersion)
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

  # Verify
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

# Pin/import requested modules FIRST (e.g., exact Graph.Auth), then preload any other module(s)
if ($cfg.Requirements -and $cfg.Requirements.Count -gt 0) {
  foreach ($r in $cfg.Requirements) { Import-Exact $r }
}
if ($cfg.PreloadMods -and $cfg.PreloadMods.Count -gt 0) {
  foreach ($m in $cfg.PreloadMods) { try { Import-Module $m -ErrorAction Stop } catch {} }
}

# Finally, run the command
$cmd = $cfg.CommandName
$args = if ($cfg.CommandArgs) { $cfg.CommandArgs } else { @() }
& $cmd @args
'@

  $child = $child.Replace('__PAYLOAD__', $payloadEnc)
  $childEnc = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($child))
  & $PwshPath -NoLogo -NoProfile -ExecutionPolicy Bypass -EncodedCommand $childEnc
}

Export-ModuleMember -Function Invoke-IsolatedScript,Invoke-IsolatedCommand
