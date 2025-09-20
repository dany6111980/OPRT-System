# OPRT Freshness Watchdog (PowerShell 5.1 safe)
# - Verifies freshness of last_price.txt and the tail of mirror_loop_unified_decisions.jsonl
# - Optionally flags big cadence gaps in mirror_loop_unified_run.csv
# - Appends compact lines to C:\OPRT\logs\watchdog_log.txt

$ErrorActionPreference = 'Stop'

# ----- Paths
$root    = 'C:\OPRT'
$logs    = Join-Path $root 'logs'
$data    = Join-Path $root 'data'

$lastPrice = Join-Path $data 'last_price.txt'
$jsonl     = Join-Path $logs 'mirror_loop_unified_decisions.jsonl'
$runCsv    = Join-Path $logs 'mirror_loop_unified_run.csv'
$watchLog  = Join-Path $logs 'watchdog_log.txt'

if (-not (Test-Path $logs)) { New-Item -ItemType Directory -Force -Path $logs | Out-Null }

# ----- Helpers
function Get-IntEnv {
    param([string]$Name, [int]$Default)
    try {
        $v = [Environment]::GetEnvironmentVariable($Name)
        if ([string]::IsNullOrWhiteSpace($v)) { return $Default }
        return [int]$v
    } catch { return $Default }
}

function UtcNow     { [DateTime]::UtcNow }
function UtcStamp   { (Get-Date -Date (UtcNow) -Format 'yyyy-MM-ddTHH:mm:ssZ') }

function Log-Line {
    param([string]$Message)
    $line = ('{0}  {1}' -f (UtcStamp), $Message)
    Add-Content -Path $watchLog -Value $line -Encoding UTF8
}

function File-AgeMinutes {
    param([string]$Path)
    if (-not (Test-Path $Path)) { return $null }
    $wt = (Get-Item $Path).LastWriteTimeUtc
    return ((UtcNow) - $wt).TotalMinutes
}

function Parse-IsoUtc {
    param([string]$s)
    try { return ([DateTimeOffset]::Parse($s)).UtcDateTime } catch { return $null }
}

# ----- Thresholds (env overrides)
$stalePriceMin = Get-IntEnv -Name 'OPRT_PRICE_MAX_AGE_MIN'          -Default 5
$staleJsonMin  = Get-IntEnv -Name 'OPRT_JSONL_MAX_AGE_MIN'          -Default 15
$cadenceMin    = Get-IntEnv -Name 'OPRT_RUN_EXPECTED_CADENCE_MIN'   -Default 60  # used for gap warning

# ----- Start
$bad = 0
Log-Line '=== Freshness watchdog started ==='

# == last_price.txt ==
if (Test-Path $lastPrice) {
    $priceRaw = ''
    try { $priceRaw = (Get-Content $lastPrice -ErrorAction SilentlyContinue | Select-Object -First 1).Trim() } catch {}

    $priceVal = $null
    try { if ($priceRaw -ne $null -and $priceRaw -ne '') { $priceVal = [double]$priceRaw } } catch {}

    $pAge    = File-AgeMinutes $lastPrice
    $pAgeStr = if ($pAge -ne $null) { '{0:N1}' -f $pAge } else { 'na' }
    $showVal = if ($priceVal -ne $null) { '{0:N2}' -f $priceVal } else { $priceRaw }

    Log-Line ("last_price.txt = {0} (age {1} min)" -f $showVal, $pAgeStr)

    if ($pAge -eq $null) {
        Log-Line 'WARN last_price.txt age unknown'
        $bad++
    } elseif ($pAge -gt $stalePriceMin) {
        Log-Line ("WARN last_price.txt stale (> {0} min)" -f $stalePriceMin)
        $bad++
    }
} else {
    Log-Line 'WARN last_price.txt missing'
    $bad++
}

# == decisions jsonl tail ==
if (Test-Path $jsonl) {
    $tail = ''
    try { $tail = Get-Content $jsonl -Tail 1 -ErrorAction SilentlyContinue } catch {}

    if (-not [string]::IsNullOrWhiteSpace($tail)) {
        $obj = $null
        try { $obj = $tail | ConvertFrom-Json } catch {}

        if ($obj -ne $null) {
            $tsStr = '' + $obj.timestamp_utc
            $ts    = Parse-IsoUtc $tsStr
            $price = $obj.price
            $wcode = $obj.W_code

            $ageMin = $null
            if ($ts -ne $null) { $ageMin = ((UtcNow) - $ts).TotalMinutes }
            $ageStr = if ($ageMin -ne $null) { '{0:N1}' -f $ageMin } else { 'na' }

            Log-Line ("json1 tail: ts={0} price={1} W_code={2} (age {3} min)" -f $tsStr, $price, $wcode, $ageStr)

            if ($ageMin -eq $null) {
                Log-Line 'WARN json1 age unknown'
                $bad++
            } elseif ($ageMin -gt $staleJsonMin) {
                Log-Line ("WARN json1 stale (> {0} min)" -f $staleJsonMin)
                $bad++
            }
        } else {
            Log-Line 'WARN json1 tail parse error'
            $bad++
        }
    } else {
        Log-Line 'WARN json1 tail missing (empty file?)'
        $bad++
    }
} else {
    Log-Line 'WARN decisions.jsonl missing'
    $bad++
}

# == Optional: cadence gap from run CSV ==
if (Test-Path $runCsv) {
    try {
        $rows = Import-Csv $runCsv
        if ($rows -and $rows.Count -ge 2 -and ($rows | Get-Member -Name 'timestamp_utc')) {
            $last2 =
                $rows |
                Select-Object timestamp_utc |
                ForEach-Object {
                    $t = Parse-IsoUtc $_.timestamp_utc
                    if ($t -ne $null) { [PSCustomObject]@{ ts = $t; raw = $_.timestamp_utc } }
                } |
                Sort-Object ts |
                Select-Object -Last 2

            if ($last2.Count -eq 2) {
                $gapMin = (($last2[1].ts) - ($last2[0].ts)).TotalMinutes
                if ($gapMin -gt ($cadenceMin * 1.5)) {
                    Log-Line ("[GAP] run cadence gap {0:N1} min ({1} → {2})" -f $gapMin, $last2[0].raw, $last2[1].raw)
                    $bad++
                }
            }
        }
    } catch {
        Log-Line ("WARN run.csv cadence check error: {0}" -f $_.Exception.Message)
    }
}

# == Summary ==
if ($bad -gt 0) {
    Log-Line '[BAD] Freshness failing checks.'
} else {
    Log-Line '[OK] Freshness checks passed.'
}
Log-Line '=== Freshness watchdog end ==='
