<# ========================================================================
  OPRT — EOD Agent Runner (Windows PowerShell 5.1 safe)
  File:  C:\OPRT\scripts\oprt_eod_agent_run.ps1
========================================================================= #>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ---------- Paths & environment ----------
$root    = 'C:\OPRT'
$scripts = Join-Path $root 'scripts'
$data    = Join-Path $root 'data'
$logs    = Join-Path $root 'logs'
$reports = Join-Path $root 'reports'
$outRoot = Join-Path $reports 'daily'

if (-not (Test-Path -LiteralPath $outRoot)) {
    New-Item -ItemType Directory -Path $outRoot | Out-Null
}

# Python exe (let PATH resolve if not explicitly set)
$py = 'python'

# Inputs
$csvMain      = Join-Path $logs   'mirror_loop_unified_run.csv'
$jsonStrong   = Join-Path $logs   'mirror_loop_unified_decisions.jsonl'
$jsonWeak     = Join-Path $logs   'mirror_loop_unified_decisions_skipped.jsonl'
$agentsDir    = Join-Path $root   'agents'
$flowsFile    = Join-Path $data   'flows_btc.json'
$pressureFile = Join-Path $data   'pressure_btc.json'
$sentimentTxt = Join-Path $data   'sentiment_index.txt'

# Runner timestamp (for display)
$stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmm')

# ---------- LLM toggle (no ternary) ----------
$withLLM = $false
if ($env:OPENAI_API_KEY) {
    if ($env:OPENAI_API_KEY.Trim().Length -gt 0) { $withLLM = $true }
}
$llmStatus = if ($withLLM) { 'Yes' } else { 'No' }
$llmModel = 'gpt-4o-mini'
$llmMaxTokens = 900

# ---------- Build args safely ----------
$eodPy = Join-Path $scripts 'oprt_supervisor_eod.py'

$argsList = @(
    $eodPy,
    '--agents_dir',    $agentsDir,
    '--csv',           $csvMain,
    '--jsonl',         $jsonStrong,
    '--jsonl_skipped', $jsonWeak,
    '--data_dir',      $data,
    '--flows',         $flowsFile,
    '--pressure',      $pressureFile,
    '--sentiment_index', $sentimentTxt,
    '--out_root',      $outRoot
)

if ($withLLM) {
    $argsList += @('--with_llm', '--llm_model', $llmModel, '--llm_max_tokens', "$llmMaxTokens")
}

# ---------- Visibility ----------
Write-Host ("[EOD] Running supervisor at {0} UTC" -f ((Get-Date).ToUniversalTime().ToString('yyyy-MM-dd HH:mm'))) -ForegroundColor Cyan
Write-Host ("[EOD] Outputs root: {0}" -f $outRoot) -ForegroundColor DarkCyan
Write-Host ("[EOD] LLM enabled: {0}" -f $llmStatus) -ForegroundColor DarkCyan

# ---------- Execute ----------
try {
    & $py @argsList
    if ($LASTEXITCODE -ne 0) {
        throw "EOD supervisor exited with code $LASTEXITCODE"
    }
}
catch {
    Write-Host ("[EOD] ERROR: {0}" -f $_.Exception.Message) -ForegroundColor Red
    exit 1
}

# ---------- Done ----------
Write-Host ("EOD finished. Check newest folder under {0} for outputs." -f $outRoot) -ForegroundColor Green
exit 0
