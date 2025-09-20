<# =====================================================================
 OPRT Doctor — Full Daily Loop Auditor (PS5-Stable)
 - PS5-safe: no semicolons, no -f formatting, no ternaries
 - Each logging call on its own line with prebuilt $msg
 - Traces: data ingest → agents → engine → logs → analytics → scheduler
 - Writes a JSON report under C:\OPRT\logs\doctor\
===================================================================== #>

[CmdletBinding()]
param(
  [string]$Root = "C:\OPRT",
  [switch]$RunEngineSmoke,          # optional one-shot smoke run
  [int]$FreshMinsIngest = 90,       # freshness budgets
  [int]$FreshMinsLogs   = 180,
  [int]$TailN           = 3
)

$ErrorActionPreference = "Stop"

# ---------- State ----------
$Audit = [ordered]@{
  started_utc = (Get-Date).ToUniversalTime().ToString("s") + "Z"
  root        = $Root
  env         = [ordered]@{}
  folders     = [ordered]@{}
  ingest      = [ordered]@{}
  agents      = [ordered]@{}
  engine      = [ordered]@{}
  logs        = [ordered]@{}
  analytics   = [ordered]@{}
  scheduler   = [ordered]@{}
}
$Findings = New-Object System.Collections.ArrayList

# ---------- Helpers ----------
function AddFinding {
  param([string]$Level,[string]$Message)
  $null = $Findings.Add(@{ level=$Level; message=$Message })
  if ($Level -eq "OK")      { Write-Host ("  [OK]    " + $Message) -ForegroundColor Green }
  elseif ($Level -eq "WARN"){ Write-Host ("  [WARN]  " + $Message) -ForegroundColor Yellow }
  elseif ($Level -eq "ERROR"){Write-Host ("  [ERROR] " + $Message) -ForegroundColor Red }
  else                      { Write-Host ("  [INFO]  " + $Message) -ForegroundColor Cyan }
}

function Get-AgeMins { param([string]$Path)
  if (-not (Test-Path -LiteralPath $Path)) { return $null }
  $ts = New-TimeSpan -Start (Get-Item -LiteralPath $Path).LastWriteTime -End (Get-Date)
  [int][Math]::Round($ts.TotalMinutes)
}

function TryReadJson { param([string]$Path)
  try {
    $raw = Get-Content -Raw -LiteralPath $Path -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    $obj = $raw | ConvertFrom-Json -ErrorAction Stop
    return $obj
  } catch { return $null }
}

function HasColumns { param([string]$CsvPath,[string[]]$MustCols)
  if (-not (Test-Path -LiteralPath $CsvPath)) { return $false }
  try {
    $first = Import-Csv -Path $CsvPath -Delimiter "," -ErrorAction Stop | Select-Object -First 1
    if (-not $first) { return $false }
    $cols = $first.PSObject.Properties.Name
    foreach ($c in $MustCols) {
      if ($cols -notcontains $c) { return $false }
    }
    return $true
  } catch { return $false }
}

function TailFile { param([string]$Path,[int]$N)
  if (Test-Path -LiteralPath $Path) { return Get-Content -LiteralPath $Path -Tail $N -ErrorAction SilentlyContinue }
  else { return @() }
}

function LatestSubdir { param([string]$Dir)
  if (-not (Test-Path -LiteralPath $Dir)) { return $null }
  Get-ChildItem -LiteralPath $Dir -Directory -ErrorAction SilentlyContinue |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
}

Write-Host ""
Write-Host "=== OPRT Doctor — Full Daily Loop Auditor (PS5) ===" -ForegroundColor Cyan

# ---------- 1) ENV + FOLDERS ----------
Write-Host ""
Write-Host "[1/8] Environment and Folder spine" -ForegroundColor Cyan

$paths = [ordered]@{
  scripts = Join-Path $Root "scripts"
  data    = Join-Path $Root "data"
  agents  = Join-Path $Root "agents"
  logs    = Join-Path $Root "logs"
  reports = Join-Path $Root "reports"
}

foreach ($k in $paths.Keys) {
  $p = $paths[$k]
  $exists = Test-Path -LiteralPath $p
  $Audit.folders[$k] = @{ path=$p; exists=$exists }
  if ($exists) { $msg = $k + " folder: " + $p; AddFinding "OK" $msg }
  else { $msg = $k + " folder missing: " + $p; AddFinding "ERROR" $msg }
}

# Python
$python = ""
try { $python = (& python --version) 2>$null } catch {}
$Audit.env.python = $python
if ($python) { $msg = "Python detected: " + $python; AddFinding "OK" $msg }
else { $msg = "Python not found in PATH; engine runs may fail."; AddFinding "WARN" $msg }

# ---------- 2) INGEST ----------
Write-Host ""
Write-Host "[2/8] Ingest freshness and validity" -ForegroundColor Cyan

$ing = [ordered]@{
  headlines_csv  = Join-Path $paths.data "headlines.csv"
  sentiment_txt  = Join-Path $paths.data "sentiment_index.txt"
  flows_btc_json = Join-Path $paths.data "flows_btc.json"
  pressure_json  = Join-Path $paths.data "pressure_btc.json"
}
$Audit.ingest.paths = $ing

# Headlines
if (Test-Path -LiteralPath $ing.headlines_csv) {
  $age = Get-AgeMins $ing.headlines_csv
  $Audit.ingest.headlines_age_mins = $age
  if ($age -le $FreshMinsIngest) { $msg = "headlines.csv age: " + $age + "m (<= " + $FreshMinsIngest + "m)"; AddFinding "OK" $msg }
  else { $msg = "headlines.csv age: " + $age + "m (> " + $FreshMinsIngest + "m)"; AddFinding "WARN" $msg }
} else { AddFinding "WARN" "headlines.csv missing" }

# Sentiment
if (Test-Path -LiteralPath $ing.sentiment_txt) {
  $age = Get-AgeMins $ing.sentiment_txt
  $val = (Get-Content -Raw -LiteralPath $ing.sentiment_txt).Trim()
  [double]$tmp = 0
  $isNum = [double]::TryParse($val, [ref]$tmp)
  $Audit.ingest.sentiment = @{ age_mins=$age; value=$val; numeric=$isNum }
  if ($isNum) {
    if ($age -le $FreshMinsIngest) { $msg = "sentiment_index=" + $val + " age: " + $age + "m"; AddFinding "OK" $msg }
    else { $msg = "sentiment_index=" + $val + " is stale (" + $age + "m)"; AddFinding "WARN" $msg }
  } else {
    $msg = "sentiment_index.txt not numeric: '" + $val + "'"
    AddFinding "WARN" $msg
  }
} else { AddFinding "WARN" "sentiment_index.txt missing" }

# Flows
if (Test-Path -LiteralPath $ing.flows_btc_json) {
  $age = Get-AgeMins $ing.flows_btc_json
  $flows = TryReadJson $ing.flows_btc_json
  $Audit.ingest.flows = @{
    age_mins = $age
    keys = (if ($flows) { ($flows | Get-Member -MemberType NoteProperty | % Name) } else { @() })
  }
  $need = @("price","volume_ratio","oi","funding","liq_skew")
  $missing = @()
  foreach ($k in $need) {
    if (-not ($flows -and $flows.PSObject.Properties.Name -contains $k)) { $missing += $k }
  }
  if ($missing.Count -gt 0) { $msg = "flows_btc.json missing keys: " + ($missing -join ", "); AddFinding "WARN" $msg }
  else {
    if ($age -le $FreshMinsIngest) { $msg = "flows_btc.json OK (age " + $age + "m)"; AddFinding "OK" $msg }
    else { $msg = "flows_btc.json stale (" + $age + "m)"; AddFinding "WARN" $msg }
  }
} else { AddFinding "WARN" "flows_btc.json missing" }

# Pressure
if (Test-Path -LiteralPath $ing.pressure_json) {
  $age = Get-AgeMins $ing.pressure_json
  $pjs = TryReadJson $ing.pressure_json
  $pval = $null
  if ($pjs -and ($pjs.PSObject.Properties.Name -contains "pressure")) { [double]$pval = $pjs.pressure }
  $okRange = ($pval -ne $null -and $pval -ge -1 -and $pval -le 1)
  $Audit.ingest.pressure = @{ age_mins=$age; pressure=$pval; in_range=$okRange }
  if ($okRange) {
    if ($age -le $FreshMinsIngest) { $msg = "pressure=" + $pval + " age " + $age + "m"; AddFinding "OK" $msg }
    else { $msg = "pressure stale (" + $age + "m)"; AddFinding "WARN" $msg }
  } else {
    $msg = "pressure invalid or out of range [-1,1]: '" + $pval + "'"
    AddFinding "WARN" $msg
  }
} else { AddFinding "WARN" "pressure_btc.json missing" }

# ---------- 3) AGENTS ----------
Write-Host ""
Write-Host "[3/8] Agents A/B JSON validation" -ForegroundColor Cyan
$assets = @("BTC","ETH","SOL","SPX","NDX","DXY","GOLD","US10Y")
$Audit.agents.assets = @()

foreach ($asset in $assets) {
  $aPath = Join-Path $paths.agents ($asset + "_A.json")
  $bPath = Join-Path $paths.agents ($asset + "_B.json")
  $Aexists = Test-Path -LiteralPath $aPath
  $Bexists = Test-Path -LiteralPath $bPath
  $plen = $null
  $hasTF = $false
  $hasInd = $false
  $ok = $false

  if ($Aexists -and $Bexists) {
    $aj = TryReadJson $aPath
    $bj = TryReadJson $bPath
    if ($aj -and $aj.phase_vector) { $plen = $aj.phase_vector.Count }
    if ($aj -and $aj.tf_alignment -and $aj.tf_alignment.H1 -and $aj.tf_alignment.H4) { $hasTF = $true }
    if ($aj -and $aj.indicators -and $aj.indicators.rsi -and $aj.indicators.macd -and $aj.indicators.ema) { $hasInd = $true }
    if ($plen -eq 5 -and $hasTF -and $hasInd) { $ok = $true }
    if ($ok) { $msg = "Agents for " + $asset + " — A/B present; phase_len=" + $plen + "; TF/Indicators=" + $hasTF + "/" + $hasInd; AddFinding "OK" $msg }
    else { $msg = "Agents for " + $asset + " present but schema incomplete (phase_len=" + $plen + ", TF=" + $hasTF + ", IND=" + $hasInd + ")"; AddFinding "WARN" $msg }
  } else {
    $msg = "Missing agent JSONs for " + $asset
    AddFinding "WARN" $msg
  }

  $Audit.agents.assets += @{ asset=$asset; A_exists=$Aexists; B_exists=$Bexists; ok=$ok; phase_len=$plen }
}

# ---------- 4) ENGINE ----------
Write-Host ""
Write-Host "[4/8] Engine presence and optional smoke-run" -ForegroundColor Cyan
$engine = Join-Path $paths.scripts "mirror_loop_v0_3_plus.py"
$Audit.engine.path = $engine
if (Test-Path -LiteralPath $engine) { AddFinding "OK" ("Engine found: " + $engine) }
else { AddFinding "ERROR" ("Engine missing: " + $engine) }

if ($RunEngineSmoke -and (Test-Path -LiteralPath $engine)) {
  try {
    Write-Host "  Running engine smoke test (mocks if agents incomplete)..." -ForegroundColor DarkCyan
    $csv  = Join-Path $paths.logs "mirror_loop_unified_run.csv"
    $json = Join-Path $paths.logs "mirror_loop_unified_decisions.jsonl"
    $cmd  = "python `"" + $engine + "`" --agents_dir `"" + $paths.agents + "`" --csv `"" + $csv + "`" --jsonl `"" + $json + "`""
    Write-Host ("  > " + $cmd) -ForegroundColor DarkGray
    $out = & cmd /c $cmd 2>&1
    $Audit.engine.smoke_output = ($out | Select-Object -Last 12) -join "`n"
    AddFinding "OK" "Engine smoke-run completed; tailed output captured."
  } catch {
    AddFinding "ERROR" ("Engine smoke-run failed: " + $_.Exception.Message)
  }
}

# ---------- 5) LOGS ----------
Write-Host ""
Write-Host "[5/8] Logs integrity and last signals" -ForegroundColor Cyan
$csvPath  = Join-Path $paths.logs "mirror_loop_unified_run.csv"
$jsonPath = Join-Path $paths.logs "mirror_loop_unified_decisions.jsonl"
$Audit.logs.paths = @{ csv=$csvPath; jsonl=$jsonPath }

$needCols = @("timestamp_utc","price","C_raw","C_global","phase_angle_deg","C_eff","signal","size_band","trap_T")

if (Test-Path -LiteralPath $csvPath) {
  $age = Get-AgeMins $csvPath
  $hasCols = HasColumns $csvPath $needCols
  $Audit.logs.csv = @{ age_mins=$age; has_core_columns=$hasCols }
  if ($age -le $FreshMinsLogs) { $msg = "CSV age " + $age + "m"; AddFinding "OK" $msg } else { $msg = "CSV stale (" + $age + "m)"; AddFinding "WARN" $msg }
  if (-not $hasCols) { $msg = "CSV missing expected columns: " + ($needCols -join ", "); AddFinding "WARN" $msg }
  $tailCsv = TailFile $csvPath $TailN
  if ($tailCsv.Count -gt 0) {
    Write-Host "  CSV tail:" -ForegroundColor DarkGray
    $tailCsv | ForEach-Object { Write-Host ("    " + $_) -ForegroundColor DarkGray }
    $Audit.logs.csv_tail = $tailCsv
  }
} else { AddFinding "WARN" "Unified CSV not found (no recent engine writes?)" }

if (Test-Path -LiteralPath $jsonPath) {
  $age = Get-AgeMins $jsonPath
  $tail = TailFile $jsonPath 1
  $last = $null
  try { if ($tail.Count -gt 0) { $last = ($tail -join "") | ConvertFrom-Json } } catch {}
  $Audit.logs.jsonl = @{ age_mins=$age; last=$last }
  if ($age -le $FreshMinsLogs) { $msg = "JSONL age " + $age + "m"; AddFinding "OK" $msg } else { $msg = "JSONL stale (" + $age + "m)"; AddFinding "WARN" $msg }
  if ($last) {
    $keyInfo = "C_raw=" + $last.C_raw + " C_eff=" + $last.C_eff + " angle=" + $last.phase_angle_deg + " trap_T=" + $last.trap_T + " vol_ratio=" + $last.volume_ratio + " S=" + $last.sentiment_index + " P=" + $last.pressure
    $msg = "Last signal: " + $last.signal + " [" + $keyInfo + "]"
    AddFinding "OK" $msg
  } else { AddFinding "WARN" "JSONL last line parse failed or empty." }
} else { AddFinding "WARN" "Unified JSONL not found" }

# ---------- 6) ANALYTICS ----------
Write-Host ""
Write-Host "[6/8] Analytics reports" -ForegroundColor Cyan
$unifiedDir = Join-Path $paths.reports "unified"
$latest = LatestSubdir $unifiedDir
if ($latest -ne $null) {
  $Audit.analytics.latest = @{ path=$latest.FullName; age_mins=(Get-AgeMins $latest.FullName) }
  $msg = "Latest unified report: " + $latest.Name
  AddFinding "OK" $msg
} else {
  $msg = "No unified reports found under " + $unifiedDir
  AddFinding "WARN" $msg
}

# ---------- 7) SCHEDULER ----------
Write-Host ""
Write-Host "[7/8] Task Scheduler (Hourly and EOD)" -ForegroundColor Cyan
function GetTasksLike { param([string]$namePart)
  try { Get-ScheduledTask -ErrorAction Stop | Where-Object { $_.TaskName -like ("*" + $namePart + "*") } }
  catch { & schtasks /Query /FO LIST /V | Select-String $namePart }
}

$aTasks = [ordered]@{}
$hourly = GetTasksLike "OPRT" | Where-Object { $_.TaskName -match "Hourly|Chain|Loop" }
$eod    = GetTasksLike "OPRT" | Where-Object { $_.TaskName -match "EOD|Daily|End" }

if ($hourly) {
  foreach ($t in $hourly) {
    $aTasks[$t.TaskName] = @{
      State=$t.State; LastRunTime=$t.LastRunTime; LastTaskResult=$t.LastTaskResult;
      Triggers = ($t.Triggers | ForEach-Object { $_.StartBoundary })
    }
    $msg = "Hourly task: " + $t.TaskName + " — State=" + $t.State + " LastRun=" + $t.LastRunTime
    AddFinding "OK" $msg
  }
} else { AddFinding "WARN" "No Hourly OPRT task found" }

if ($eod) {
  foreach ($t in $eod) {
    $aTasks[$t.TaskName] = @{
      State=$t.State; LastRunTime=$t.LastRunTime; LastTaskResult=$t.LastTaskResult;
      Triggers = ($t.Triggers | ForEach-Object { $_.StartBoundary })
    }
    $msg = "EOD task: " + $t.TaskName + " — State=" + $t.State + " LastRun=" + $t.LastRunTime
    AddFinding "OK" $msg
  }
} else { AddFinding "WARN" "No EOD OPRT task found" }

$Audit.scheduler = $aTasks

# ---------- 8) STATUS + REPORT ----------
Write-Host ""
Write-Host "[8/8] Final status" -ForegroundColor Cyan
$errors = $Findings | Where-Object { $_.level -eq "ERROR" }
$warns  = $Findings | Where-Object { $_.level -eq "WARN" }
$status = "READY"
if ($errors.Count -gt 0) { $status = "NEEDS_FIXES" }
elseif ($warns.Count -gt 0) { $status = "DEGRADED" }
$Audit.status = $status

if ($status -eq "READY") { Write-Host ("Overall status: " + $status) -ForegroundColor Green }
elseif ($status -eq "DEGRADED") { Write-Host ("Overall status: " + $status) -ForegroundColor Yellow }
else { Write-Host ("Overall status: " + $status) -ForegroundColor Red }

$Audit.completed_utc = (Get-Date).ToUniversalTime().ToString("s") + "Z"
$Audit.findings = $Findings

$reportDir = Join-Path $paths.logs "doctor"
New-Item -ItemType Directory -Force -Path $reportDir | Out-Null
$stamp = (Get-Date).ToString("yyyyMMdd_HHmmss")
$reportPath = Join-Path $reportDir ("oprt_doctor_" + $stamp + ".json")
($Audit | ConvertTo-Json -Depth 7) | Out-File -FilePath $reportPath -Encoding UTF8

Write-Host ("Report saved: " + $reportPath) -ForegroundColor Cyan
Write-Host "Done." -ForegroundColor Green
