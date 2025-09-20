#Requires -Version 5.1
param([switch]$Force)
<#
OPRT Rotator — v2.6b (ASCII-safe)
- Runs hourly pipeline once, then a 48h sweet-spot analytics sweep
- Start-Process based Python launcher (no cmd /c; stderr captured)
- Treats analytics as success if a new report folder appears
- Logs: C:\OPRT\logs\rotator.log
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'

function New-Dir($p) { if ($p -and -not (Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null } }

function Get-Python {
  if (Get-Command python -ErrorAction SilentlyContinue) { return @{ exe="python"; pre=@() } }
  if (Get-Command py      -ErrorAction SilentlyContinue) { return @{ exe="py";      pre=@("-3") } }
  throw "Python not found. Install Python or add it to PATH."
}

function Invoke-Py {
  param(
    [hashtable]$py,
    [string]$scriptPath,
    [string[]]$args = @(),
    [string]$LogPath = $null
  )
  if (!(Test-Path $scriptPath)) { throw "Missing script: $scriptPath" }
  $argList = @(); $argList += $py.pre; $argList += @($scriptPath) + $args
  $argLine = ($argList -join ' ')
  Write-Host (">> " + $py.exe + " " + $argLine)

  $tmpOut = [System.IO.Path]::GetTempFileName()
  $tmpErr = [System.IO.Path]::GetTempFileName()
  try {
    $p = Start-Process -FilePath $py.exe -ArgumentList $argLine `
          -NoNewWindow -Wait -PassThru `
          -RedirectStandardOutput $tmpOut `
          -RedirectStandardError  $tmpErr
    if ($LogPath) {
      if (Test-Path $tmpOut) { Get-Content $tmpOut | Add-Content -Path $LogPath }
      if (Test-Path $tmpErr) { Get-Content $tmpErr | Add-Content -Path $LogPath }
    } else {
      if (Test-Path $tmpOut) { Get-Content $tmpOut }
      if (Test-Path $tmpErr) { Get-Content $tmpErr }
    }
    return $p.ExitCode
  } finally {
    Remove-Item $tmpOut,$tmpErr -Force -ErrorAction SilentlyContinue
  }
}

# Paths
$root    = "C:\OPRT"
$scrDir  = Join-Path $root "scripts"
$logs    = Join-Path $root "logs"
$locks   = Join-Path $root "locks"
$repDir  = Join-Path $root "reports\unified"
$varDir  = Join-Path $root "engine_variants"
New-Dir $logs; New-Dir $locks; New-Dir $repDir

$rotLog  = Join-Path $logs "rotator.log"
$hourly  = Join-Path $scrDir "oprt_hourly_run.ps1"
$analyticsPy = Join-Path $scrDir "oprt_unified_analytics.py"

$currFile = Join-Path $varDir "CURRENT.txt"
if (!(Test-Path $currFile)) { throw "Missing CURRENT.txt in $varDir" }
$experiment = (Get-Content $currFile -Raw).Trim()

("=== " + (Get-Date -Format s) + "Z START rotator Force=" + $Force.IsPresent) | Out-File $rotLog -Append

# Lock
$lockFile = Join-Path $locks "rotator.lock"
if ((Test-Path $lockFile) -and (-not $Force)) {
  $age = (Get-Date) - (Get-Item $lockFile).LastWriteTime
  if ($age.TotalMinutes -lt 55) {
    "rotator: lock present (<55m) - skipping" | Out-File $rotLog -Append
    Write-Host "rotator: lock present - skipping"
    return
  } else {
    "rotator: stale lock removed" | Out-File $rotLog -Append
    Remove-Item $lockFile -Force -ErrorAction SilentlyContinue
  }
}
New-Item -ItemType File -Path $lockFile -Force | Out-Null

try {
  $py = Get-Python
  Write-Host ("[rotator] current experiment: " + $experiment)
  ("[rotator] current experiment: " + $experiment) | Out-File $rotLog -Append

  # Run hourly pipeline once
  if (!(Test-Path $hourly)) { throw "Missing hourly runner: $hourly" }
  "[rotator] running hourly pipeline..." | Out-File $rotLog -Append
  $tmpOut = [System.IO.Path]::GetTempFileName()
  $tmpErr = [System.IO.Path]::GetTempFileName()
  try {
    $psExe  = "$env:WINDIR\System32\WindowsPowerShell\v1.0\powershell.exe"
    $psArgs = "-NoProfile -ExecutionPolicy Bypass -File `"$hourly`""
    $p = Start-Process -FilePath $psExe -ArgumentList $psArgs -NoNewWindow -Wait -PassThru `
          -RedirectStandardOutput $tmpOut -RedirectStandardError $tmpErr
    if (Test-Path $tmpOut) { Get-Content $tmpOut | Add-Content -Path $rotLog }
    if (Test-Path $tmpErr) { Get-Content $tmpErr | Add-Content -Path $rotLog }
    if ($p.ExitCode -ne 0) { throw ("hourly pipeline exit " + $p.ExitCode) }
  } finally {
    Remove-Item $tmpOut,$tmpErr -Force -ErrorAction SilentlyContinue
  }

  # 48h sweet-spot analytics
  ("[rotator] sweet-spot finder (48h) for " + $experiment) | Out-File $rotLog -Append
  $args48 = @("--since_hours","48","--tz","Europe/Brussels","--experiment_id",$experiment,"--trap_cutoff_default","0.80")
  $rcA = Invoke-Py $py $analyticsPy $args48 $rotLog

  # Artifact check
  $after = Get-ChildItem -Path $repDir -Directory -ErrorAction SilentlyContinue |
           Sort-Object LastWriteTime -Descending | Select-Object -First 1
  if ($after) {
    ("[rotator] sweet-spot written to: " + $after.FullName) | Out-File $rotLog -Append
  } else {
    ("[rotator] WARN: analytics returned " + $rcA + " and no report folder was detected.") | Out-File $rotLog -Append
    if ($rcA -ne 0) { throw "Analytics failed" }
  }

} finally {
  Remove-Item $lockFile -Force -ErrorAction SilentlyContinue
  ("=== " + (Get-Date -Format s) + "Z END rotator ===") | Out-File $rotLog -Append
}
