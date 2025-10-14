#requires -Modules @{ ModuleName='Microsoft.Graph.Authentication'; ModuleVersion='2.31.0' }
$ErrorActionPreference = 'Stop'

Write-Host "== Test-GraphAuth-231.ps1 =="
Write-Host "PID: $PID"
Write-Host "Autoloading: $PSModuleAutoloadingPreference`n"

$loaded = Get-Module -Name Microsoft.Graph.Authentication | Select-Object -First 1
if (-not $loaded) { throw "Module didn't load. #requires should have imported it." }

"Loaded module : {0} v{1}" -f $loaded.Name, $loaded.Version
"ModuleBase    : {0}" -f $loaded.ModuleBase
""

[AppDomain]::CurrentDomain.GetAssemblies() |
  Where-Object { $_.GetName().Name -match '^(Microsoft\.Graph(\..+)?|Microsoft\.Identity\..+|Microsoft\.Graph\.Authentication)$' } |
  Sort-Object { $_.GetName().Name } |
  Select-Object @{n='Assembly';e={$_.GetName().Name}},
                @{n='Version'; e={$_.GetName().Version}},
                Location |
  Format-Table -AutoSize
