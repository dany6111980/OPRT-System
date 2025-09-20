# OPRT Pipeline X-Ray (PS5-safe, ASCII only)
[CmdletBinding()]
param(
  [string]$Root = "C:\OPRT",
  [int]$FreshIngestMins = 90,
  [int]$FreshLogsMins   = 180,
  [int]$TailN           = 3,
  [switch]$ShowDetails
)

$ErrorActionPreference = "Stop"

# ---------- helpers ----------
$Findings = New-Object System.Collections.ArrayList
function AddFinding { param([string]$Level,[string]$Message)
  $null = $Findings.Add(@{ level=$Level; message=$Message })
  if ($Level -eq "OK")      { Write-Host ("  [OK]    " + $Message) -ForegroundColor Green }
  elseif ($Level -eq "WARN"){ Write-Host ("  [WARN]  " + $Message) -ForegroundColor Yellow }
  elseif ($Level -eq "ERROR"){Write-Host ("  [ERROR] " + $Message) -ForegroundColor Red }
  else                      { Write-Host ("  [INFO]  " + $Message) -ForegroundColor Cyan }
}
function PathAgeMins { param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { return $null }
  [int][Math]::Round((New-TimeSpan -Start (Get-Item -LiteralPath $Path).LastWriteTime -End (Get-Date)).TotalMinutes)
}
function TailFile { param([string]$Path,[int]$N)
  if (Test-Path -LiteralPath $Path) { Get-Content -LiteralPath $Path -Tail $N -ErrorAction SilentlyContinue } else { @() }
}
function ReadJson { param([string]$Path)
  try {
    if (-not (Test-Path -LiteralPath $Path)) { return $null }
    $raw = Get-Content -LiteralPath $Path -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    $raw | ConvertFrom-Json -ErrorAction Stop
  } catch { return $null }
}
function HasColumns { param([string]$Csv,[string[]]$Cols)
  if (-not (Test-Path -LiteralPath $Csv)) { return $false }
  try {
    $one = Import-Csv -Path $Csv -Delimiter "," -ErrorAction Stop | Select-Object -First 1
    if (-not $one) { return $false }
    $names = $one.PSObject.Properties.Name
    foreach ($c in $Cols) { if ($names -notcontains $c) { return $false } }
    return $true
  } catch { return $false }
}
function LatestDir { param([string]$Dir)
  if (-not (Test-Path -LiteralPath $Dir)) { return $null }
  Get-ChildItem -LiteralPath $Dir -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
}

# ---------- spine ----------
Write-Host "=== OPRT Pipeline X-Ray (PS5) ===" -ForegroundColor Cyan

$paths = [ordered]@{
  scripts = Join-Path $Root "scripts"
  data    = Join-Path $Root "data"
  agents  = Join-Path $Root "agents"
  logs    = Join-Path $Root "logs"
  reports = Join-Path $Root "reports"
}
foreach ($k in $paths.Keys) {
  $p = $paths[$k]
  if (Test-Path -LiteralPath $p) { AddFinding "OK" ($k + " folder present: " + $p) }
  else { AddFinding "ERROR" ($k + " folder missing: " + $p) }
}

# ---------- scripts ----------
Write-Host "[1/7] Script presence" -ForegroundColor Cyan
$scripts = [ordered]@{
  agents_build_openai   = Join-Path $paths.scripts "agents_build_openai.py"
  headlines_ingest      = Join-Path $paths.scripts "headlines_ingest.py"
  headlines_to_si       = Join-Path $paths.scripts "headlines_to_sentiment_hdr.py"
  flows_ingest_btc      = Join-Path $paths.scripts "flows_ingest_btc.py"
  pressure_ingest_btc   = Join-Path $paths.scripts "pressure_ingest_btc.py"
  engine                = Join-Path $paths.scripts "mirror_loop_v0_3_plus.py"
  analytics             = Join-Path $paths.scripts "oprt_unified_analytics.py"
  supervisor_eod        = Join-Path $paths.scripts "oprt_supervisor_eod.py"
  hourly_runner_ps1     = Join-Path $paths.scripts "oprt_hourly_run.ps1"
  freshness_watchdog    = Join-Path $paths.scripts "oprt_freshness_watchdog.ps1"
}
foreach ($k in $scripts.Keys) {
  $p = $scripts[$k]
  if (Test-Path -LiteralPath $p) { AddFinding "OK" ($k + " found: " + $p) }
  else { AddFinding "WARN" ($k + " missing: " + $p) }
}

# ---------- ingest ----------
Write-Host "[2/7] Ingest freshness and validity" -ForegroundColor Cyan
$ing = [ordered]@{
  headlines_csv = Join-Path $paths.data "headlines.csv"
  sentiment_txt = Join-Path $paths.data "sentiment_index.txt"
  flows_json    = Join-Path $paths.data "flows_btc.json"
  pressure_json = Join-Path $paths.data "pressure_btc.json"
}

# headlines.csv
if (Test-Path -LiteralPath $ing.headlines_csv) {
  $age = PathAgeMins $ing.headlines_csv
  if ($age -le $FreshIngestMins) { AddFinding "OK" ("headlines.csv age " + $age + "m") }
  else { AddFinding "WARN" ("headlines.csv stale (" + $age + "m)") }
} else { AddFinding "WARN" "headlines.csv missing" }

# sentiment_index.txt
if (Test-Path -LiteralPath $ing.sentiment_txt) {
  $age = PathAgeMins $ing.sentiment_txt
  $val = (Get-Content -LiteralPath $ing.sentiment_txt -Raw).Trim()
  [double]$tmp = 0
  $isNum = [double]::TryParse($val, [ref]$tmp)
  if ($isNum) {
    if ($age -le $FreshIngestMins) { AddFinding "OK" ("sentiment_index " + $val + " age " + $age + "m") }
    else { AddFinding "WARN" ("sentiment_index stale (" + $age + "m)") }
  } else { AddFinding "WARN" ("sentiment_index not numeric: " + $val) }
} else { AddFinding "WARN" "sentiment_index.txt missing" }

# flows_btc.json
if (Test-Path -LiteralPath $ing.flows_json) {
  $age = PathAgeMins $ing.flows_json
  $flows = ReadJson $ing.flows_json
  $need = @("price","vol_1h_current","vol_avg20","volume_ratio","oi","funding","liq_skew")
  $miss = @()
  foreach ($k in $need) { if (-not ($flows -and $flows.PSObject.Properties.Name -contains $k)) { $miss += $k } }
  if ($miss.Count -gt 0) { AddFinding "WARN" ("flows_btc.json missing keys: " + ($miss -join ", ")) }
  if ($age -le $FreshIngestMins) {
    $msg = "flows age " + $age + "m vol_ratio=" + $flows.volume_ratio + " oi=" + $flows.oi + " funding=" + $flows.funding + " liq_skew=" + $flows.liq_skew
    AddFinding "OK" $msg
  } else { AddFinding "WARN" ("flows_btc.json stale (" + $age + "m)") }
} else { AddFinding "WARN" "flows_btc.json missing" }

# pressure_btc.json
if (Test-Path -LiteralPath $ing.pressure_json) {
  $age = PathAgeMins $ing.pressure_json
  $pjs = ReadJson $ing.pressure_json
  $p = $null
  if ($pjs -and ($pjs.PSObject.Properties.Name -contains "pressure")) { [double]$p = $pjs.pressure }
  $ok = ($p -ne $null -and $p -ge -1 -and $p -le 1)
  if ($ok) {
    if ($age -le $FreshIngestMins) { AddFinding "OK" ("pressure " + $p + " age " + $age + "m") }
    else { AddFinding "WARN" ("pressure stale (" + $age + "m)") }
  } else { AddFinding "WARN" "pressure invalid or missing pressure field" }
} else { AddFinding "WARN" "pressure_btc.json missing" }

# ---------- agents ----------
Write-Host "[3/7] Agents A and B health" -ForegroundColor Cyan
$assets = @("BTC","ETH","SOL","SPX","NDX","DXY","GOLD","US10Y")

foreach ($a in $assets) {
  $A = Join-Path $paths.agents ($a + "_A.json")
  $B = Join-Path $paths.agents ($a + "_B.json")

  $hasA = Test-Path -LiteralPath $A
  $hasB = Test-Path -LiteralPath $B

  $ok = $false
  $phaseLen = $null
  $tf = $false
  $ind = $false

  if ($hasA -and $hasB) {
    $aj = ReadJson $A
    if ($aj -and $aj.phase_vector) { $phaseLen = $aj.phase_vector.Count }
    if ($aj -and $aj.tf_alignment -and $aj.tf_alignment.H1 -and $aj.tf_alignment.H4) { $tf = $true }
    if ($aj -and $aj.indicators -and $aj.indicators.rsi -and $aj.indicators.macd -and $aj.indicators.ema) { $ind = $true }
    if ($phaseLen -eq 5 -and $tf -and $ind) { $ok = $true }
  }

  if ($ok) {
    $msg = "Agents " + $a + " present; phase_len=" + $phaseLen + " TF=" + $tf + " IND=" + $ind
    AddFinding "OK" $msg
  } elseif ($hasA -and $hasB) {
    $msg = "Agents " + $a + " present but schema incomplete (phase_len=" + $phaseLen + ", TF=" + $tf + ", IND=" + $ind + ")"
    AddFinding "WARN" $msg
  } else {
    $msg = "Agents " + $a + " missing A or B"
    AddFinding "WARN" $msg
  }
}

# ---------- engine and logs ----------
Write-Host "[4/7] Engine and logs continuity" -ForegroundColor Cyan
$engine = $scripts.engine
if (Test-Path -LiteralPath $engine) { AddFinding "OK" ("Engine present: " + $engine) } else { AddFinding "ERROR" ("Engine missing: " + $engine) }

$csv  = Join-Path $paths.logs "mirror_loop_unified_run.csv"
$json = Join-Path $paths.logs "mirror_loop_unified_decisions.jsonl"
$needCols = @("timestamp_utc","price","C_raw","C_global","phase_angle_deg","C_eff","signal","size_band","trap_T")

if (Test-Path -LiteralPath $csv) {
  $age = PathAgeMins $csv
  if ($age -le $FreshLogsMins) { AddFinding "OK" ("CSV age " + $age + "m") } else { AddFinding "WARN" ("CSV stale (" + $age + "m)") }
  if (-not (HasColumns $csv $needCols)) { AddFinding "WARN" ("CSV missing expected columns") }
  if ($ShowDetails) {
    $tail = TailFile $csv $TailN
    if ($tail.Count -gt 0) {
      Write-Host "  CSV tail:" -ForegroundColor DarkGray
      $tail | ForEach-Object { Write-Host ("    " + $_) -ForegroundColor DarkGray }
    }
  }
} else { AddFinding "WARN" "Unified CSV not found" }

if (Test-Path -LiteralPath $json) {
  $age = PathAgeMins $json
  if ($age -le $FreshLogsMins) { AddFinding "OK" ("JSONL age " + $age + "m") } else { AddFinding "WARN" ("JSONL stale (" + $age + "m)") }
  $last = TailFile $json 1
  if ($last.Count -gt 0) {
    try {
      $row = ($last -join "") | ConvertFrom-Json
      $msg = "signal " + $row.signal + " C_eff=" + $row.C_eff + " angle=" + $row.phase_angle_deg + " vol=" + $row.volume_ratio + " trap_T=" + $row.trap_T
      AddFinding "OK" $msg
    } catch { AddFinding "WARN" "Failed to parse last JSONL line" }
  }
} else { AddFinding "WARN" "Unified JSONL not found" }

# ---------- analytics ----------
Write-Host "[5/7] Analytics artifacts" -ForegroundColor Cyan
$u = Join-Path $paths.reports "unified"
$latest = LatestDir $u
if ($latest -ne $null) {
  $age = PathAgeMins $latest.FullName
  AddFinding "OK" ("Latest unified report " + $latest.Name + " age " + $age + "m")
} else { AddFinding "WARN" ("No unified report under " + $u) }

# ---------- scheduler ----------
Write-Host "[6/7] Scheduler summary" -ForegroundColor Cyan
function GetTasksLike { param([string]$name)
  try { Get-ScheduledTask -ErrorAction Stop | Where-Object { $_.TaskName -like ("*" + $name + "*") } }
  catch { & schtasks /Query /FO LIST /V | Select-String $name }
}
$hourly = GetTasksLike "OPRT" | Where-Object { $_.TaskName -match "Hourly|Chain|Run|Loop" }
$eod    = GetTasksLike "OPRT" | Where-Object { $_.TaskName -match "EOD|Daily|End" }
if ($hourly) { foreach ($t in $hourly) { $msg = "Hourly task " + $t.TaskName + " state " + $t.State + " last " + $t.LastRunTime; AddFinding "OK" $msg } }
else { AddFinding "WARN" "No hourly OPRT task" }
if ($eod) { foreach ($t in $eod) { $msg = "EOD task " + $t.TaskName + " state " + $t.State + " last " + $t.LastRunTime; AddFinding "OK" $msg } }
else { AddFinding "WARN" "No EOD OPRT task" }

# ---------- final ----------
Write-Host "[7/7] Final status" -ForegroundColor Cyan
$errors = $Findings | Where-Object { $_.level -eq "ERROR" }
$warns  = $Findings | Where-Object { $_.level -eq "WARN" }
$status = "READY"
if ($errors.Count -gt 0) { $status = "NEEDS_FIXES" } elseif ($warns.Count -gt 0) { $status = "DEGRADED" }
if ($status -eq "READY") { Write-Host ("Overall status: " + $status) -ForegroundColor Green }
elseif ($status -eq "DEGRADED") { Write-Host ("Overall status: " + $status) -ForegroundColor Yellow }
else { Write-Host ("Overall status: " + $status) -ForegroundColor Red }

$reportDir = Join-Path $paths.logs "doctor"
New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$report = Join-Path $reportDir ("oprt_xray_" + $stamp + ".json")
(@{
  started_utc = (Get-Date).ToUniversalTime().ToString("s") + "Z"
  root=$Root; status=$status; findings=$Findings
} | ConvertTo-Json -Depth 5) | Out-File -FilePath $report -Encoding UTF8
Write-Host ("Report saved: " + $report) -ForegroundColor Cyan
Write-Host "Done." -ForegroundColor Green

