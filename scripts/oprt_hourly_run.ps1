#Requires -Version 5.1
param(
  [string]$Experiment = $null,     # default: read from engine_variants\CURRENT.txt
  [switch]$SkipAnalytics           # optional quick run w/o 6h analytics
)
<#
OPRT Hourly Runner — v3.2 (ASCII-safe)
- No cmd /c, no pipes; uses Start-Process with redirected stdout/stderr
- File lock to prevent double-runs (e.g., Task Scheduler overlap)
- Same Python launcher as rotator
- Optionally runs compact 6h analytics (rotator will still do the 48h sweep)
- Logs: C:\OPRT\logs\hourly_runner.log
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$PSDefaultParameterValues['Out-File:Encoding'] = 'utf8'

# ---- helpers ---------------------------------------------------------------
function New-Dir($p){ if ($p -and -not (Test-Path $p)) { New-Item -ItemType Directory -Path $p | Out-Null } }

function Get-Python {
  if (Get-Command python -ErrorAction SilentlyContinue) { return @{ exe="python"; pre=@() } }
  if (Get-Command py      -ErrorAction SilentlyContinue) { return @{ exe="py";      pre=@("-3") } }
  throw "Python not found. Install Python or add it to PATH."
}

function Invoke-Py {
  param(
    [hashtable]$py,
    [string]$ScriptPath,
    [string[]]$Args = @(),
    [string]$LogPath = $null
  )
  if (!(Test-Path $ScriptPath)) { throw "Missing script: $ScriptPath" }
  $argList = @(); $argList += $py.pre; $argList += @($ScriptPath) + $Args
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

# ---- paths -----------------------------------------------------------------
$ROOT   = "C:\OPRT"
$scripts= Join-Path $ROOT "scripts"
$data   = Join-Path $ROOT "data"
$logs   = Join-Path $ROOT "logs"
$locks  = Join-Path $ROOT "locks"
New-Dir $logs; New-Dir $locks; New-Dir $data

$LogPath        = Join-Path $logs "hourly_runner.log"
$LockFile       = Join-Path $locks "hourly.lock"
$FlowsIngest    = Join-Path $scripts "flows_ingest_btc.py"
$HeadsIngest    = Join-Path $scripts "headlines_ingest.py"
$HeadsToHDR     = Join-Path $scripts "headlines_to_sentiment_hdr.py"   # optional, if you use it
$AgentsBuild    = Join-Path $scripts "agents_build_openai.py"
$Engine         = Join-Path $scripts "mirror_loop_v0_3_plus.py"
$Analytics      = Join-Path $scripts "oprt_unified_analytics.py"

# experiment from CURRENT.txt unless provided
if (-not $Experiment) {
  $currFile = Join-Path (Join-Path $ROOT "engine_variants") "CURRENT.txt"
  if (Test-Path $currFile) { $Experiment = (Get-Content $currFile -Raw).Trim() }
}

("=== " + (Get-Date -Format s) + "Z START hourly ===") | Out-File $LogPath -Append

# ---- lock: avoid overlap ---------------------------------------------------
if (Test-Path $LockFile) {
  $age = (Get-Date) - (Get-Item $LockFile).LastWriteTime
  if ($age.TotalMinutes -lt 55) {
    "hourly: lock present (<55m) - skipping" | Out-File $LogPath -Append
    Write-Host "hourly: lock present - skipping"
    return
  } else {
    "hourly: stale lock removed" | Out-File $LogPath -Append
    Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
  }
}
New-Item -ItemType File -Path $LockFile -Force | Out-Null

try {
  $py = Get-Python

  # 1) FLOWS
  ">> python $FlowsIngest" | Out-File $LogPath -Append
  $rc = Invoke-Py $py $FlowsIngest @() $LogPath
  if ($rc -ne 0) { throw "flows_ingest_btc.py exit $rc" }

  # 2) HEADLINES (ingest + optional HDR transform)
  ">> python $HeadsIngest" | Out-File $LogPath -Append
  $rc = Invoke-Py $py $HeadsIngest @() $LogPath
  if ($rc -ne 0) { throw "headlines_ingest.py exit $rc" }

  if (Test-Path $HeadsToHDR) {
    ">> python $HeadsToHDR" | Out-File $LogPath -Append
    $null = Invoke-Py $py $HeadsToHDR @() $LogPath
  }

  # 3) BUILD AGENTS
  ">> python $AgentsBuild" | Out-File $LogPath -Append
  $rc = Invoke-Py $py $AgentsBuild @() $LogPath
  if ($rc -ne 0) { throw "agents_build_openai.py exit $rc" }

  # 4) RUN ENGINE
  $engArgs = @()
  if ($Experiment) { $engArgs += @("--experiment_id", $Experiment) }
  ">> python $Engine $($engArgs -join ' ')" | Out-File $LogPath -Append
  $rc = Invoke-Py $py $Engine $engArgs $LogPath
  if ($rc -ne 0) { throw "engine exit $rc" }

  # 5) QUICK ANALYTICS (6h), optional (rotator will still run 48h)
  if (-not $SkipAnalytics) {
    $aArgs = @("--since_hours","6","--tz","Europe/Brussels")
    if ($Experiment) { $aArgs += @("--experiment_id",$Experiment) }
    ">> python $Analytics $($aArgs -join ' ')" | Out-File $LogPath -Append
    $null = Invoke-Py $py $Analytics $aArgs $LogPath
  } else {
    "analytics skipped by switch" | Out-File $LogPath -Append
  }

} finally {
  Remove-Item $LockFile -Force -ErrorAction SilentlyContinue
  ("=== " + (Get-Date -Format s) + "Z END hourly ===") | Out-File $LogPath -Append
}
