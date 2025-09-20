param(
  [string]$CocRoot = "C:\OPRT\coc",
  [string]$OutRoot = "C:\OPRT\reports\coc",
  [string]$Python  = "python",
  [string]$TZ      = "Europe/Brussels"
)

function New-Dir([string]$p) { if (!(Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null } }

$stamp  = (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmm")
$OutDir = Join-Path $OutRoot $stamp
New-Dir $OutDir

# 1) Inventory (files, sizes, hashes, lines)
Write-Host "[1/4] Inventory..."
$files = Get-ChildItem -Path $CocRoot -Recurse -File -Include *.py,*.json,*.md,*.yaml,*.yml,*.toml,*.csv
$inv = @()
foreach ($f in $files) {
  try {
    $hash = (Get-FileHash -Path $f.FullName -Algorithm SHA256).Hash
  } catch { $hash = $null }
  try {
    $lines = (Get-Content -Path $f.FullName -ErrorAction SilentlyContinue | Measure-Object -Line).Lines
  } catch { $lines = $null }
  $inv += [pscustomobject]@{
    path = $f.FullName
    rel  = $f.FullName.Substring($CocRoot.Length).TrimStart("\","/")
    ext  = $f.Extension
    bytes = $f.Length
    lines = $lines
    last_write_utc = [DateTime]::SpecifyKind($f.LastWriteTimeUtc, 'Utc').ToString("s") + "Z"
    sha256 = $hash
  }
}
$inv | Sort-Object path | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutDir "coc_inventory.csv")

# 2) Write a temporary Python AST analyzer
$PyScan = @'
import sys, json, ast, os, pathlib

root = sys.argv[1]
items = []

def stdlib_modules():
    # Minimal heuristic list (readability only)
    return set("""
abc argparse array ast asyncio base64 bisect calendar collections concurrent contextlib copy
csv ctypes dataclasses datetime decimal difflib enum errno faulthandler fnmatch functools gc getpass getopt
glob gzip hashlib heapq hmac html http importlib inspect io ipaddress itertools json linecache locale logging
lzma math mimetypes mmap modulefinder multiprocessing numbers operator os pathlib pickle pkgutil platform plistlib
pprint profile pstats pty queue random re selectors shelve shutil signal site smtpd smtplib socket sqlite3 ssl
stat statistics string subprocess sys tarfile tempfile textwrap threading time timeit traceback types typing
unicodedata urllib uuid venv warnings wave weakref webbrowser xml zipfile zlib zoneinfo
""".split())

def classify_import(mod):
    if not mod:
        return "unknown"
    m = mod.split(".")[0]
    if m in stdlib_modules():
        return "stdlib"
    if m in ("numpy","pandas","scipy","openai","requests","matplotlib","torch","transformers"):
        return "thirdparty"
    if m in ("coc","core","oprt","mirror_loop"):
        return "local"
    return "thirdparty"

for p in pathlib.Path(root).rglob("*.py"):
    rel = str(p).replace(root,"").lstrip("\\/") or p.name
    try:
        code = p.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        code = ""
    record = {"file": rel, "syntax_ok": True, "error": None, "funcs":[], "classes":[], "imports":[], "consts":[], "hints":[]}
    try:
        tree = ast.parse(code, filename=str(p))
        for n in ast.walk(tree):
            if isinstance(n, ast.Import):
                for a in n.names:
                    record["imports"].append({"module": a.name, "name": None, "kind": classify_import(a.name)})
            elif isinstance(n, ast.ImportFrom):
                mod = n.module or ""
                for a in n.names:
                    record["imports"].append({"module": mod, "name": a.name, "kind": classify_import(mod)})

        for n in tree.body:
            if isinstance(n, ast.FunctionDef):
                record["funcs"].append({"name": n.name, "args": len(n.args.args)})
            elif isinstance(n, ast.AsyncFunctionDef):
                record["funcs"].append({"name": n.name, "args": len(n.args.args), "async": True})
            elif isinstance(n, ast.ClassDef):
                methods = [m.name for m in n.body if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef))]
                record["classes"].append({"name": n.name, "methods": methods, "method_count": len(methods)})
            elif isinstance(n, ast.Assign):
                for t in n.targets:
                    if isinstance(t, ast.Name) and t.id.isupper() and len(t.id) >= 2 and t.id.replace("_","").isalpha():
                        record["consts"].append(t.id)

        names = {f["name"].lower() for f in record["funcs"]}
        if any("coc" in s for s in names): record["hints"].append("func:coc")
        if any("global" in s or "c_global" in s for s in names): record["hints"].append("func:global")
        if any("phase" in s for s in names): record["hints"].append("func:phase")
        if "CoC" in code or "COC" in record["consts"]: record["hints"].append("const:CoC")
        if "CoC_ref" in code or "COC_REF" in code: record["hints"].append("const:CoC_ref")

    except SyntaxError as e:
        record["syntax_ok"] = False
        record["error"] = f"{e.__class__.__name__}: {e.msg} at {e.lineno}:{e.offset}"
    except Exception as e:
        record["syntax_ok"] = False
        record["error"] = f"{e.__class__.__name__}: {e}"

    items.append(record)

print(json.dumps(items, ensure_ascii=False))
'@
$PyScanPath = Join-Path $OutDir "_coc_scan.py"
$PyScan | Set-Content -Encoding UTF8 $PyScanPath



# 3) Run analyzer and collect JSON
Write-Host "[2/4] Python AST scan..."
$raw = & $Python $PyScanPath $CocRoot
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($raw)) {
  Write-Host "[ERR] Python scan failed. Check Python path or syntax."
  exit 1
}
$data = $raw | ConvertFrom-Json

# 4) Emit tables
Write-Host "[3/4] Writing tables..."
# per-file summary
$sum = foreach ($r in $data) {
  [pscustomobject]@{
    file         = $r.file
    syntax_ok    = $r.syntax_ok
    error        = $r.error
    funcs        = ($r.funcs | ForEach-Object { $_.name } | Sort-Object) -join ";"
    classes      = ($r.classes | ForEach-Object { $_.name } | Sort-Object) -join ";"
    imports_std  = ($r.imports | Where-Object { $_.kind -eq "stdlib" } | ForEach-Object { if ($_.name){ "$($_.module):$($_.name)" } else { $_.module } } | Sort-Object -Unique) -join ";"
    imports_3rd  = ($r.imports | Where-Object { $_.kind -eq "thirdparty" } | ForEach-Object { if ($_.name){ "$($_.module):$($_.name)" } else { $_.module } } | Sort-Object -Unique) -join ";"
    imports_loc  = ($r.imports | Where-Object { $_.kind -eq "local" } | ForEach-Object { if ($_.name){ "$($_.module):$($_.name)" } else { $_.module } } | Sort-Object -Unique) -join ";"
    consts       = ($r.consts | Sort-Object -Unique) -join ";"
    hints        = ($r.hints | Sort-Object -Unique) -join ";"
  }
}
$sum | Sort-Object file | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutDir "coc_python_summary.csv")

# explode funcs/classes/imports to separate tables
$funcRows = foreach ($r in $data) {
  foreach ($f in $r.funcs) { [pscustomobject]@{ file=$r.file; func=$f.name; args=($f.args); async=($f.async -as [bool]) } }
}
if ($funcRows) { $funcRows | Sort-Object file,func | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutDir "coc_functions.csv") }

$classRows = foreach ($r in $data) {
  foreach ($c in $r.classes) { [pscustomobject]@{ file=$r.file; class=$c.name; method_count=$c.method_count; methods=($c.methods -join ";") } }
}
if ($classRows) { $classRows | Sort-Object file,class | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutDir "coc_classes.csv") }

$impRows = foreach ($r in $data) {
  foreach ($i in $r.imports) { [pscustomobject]@{ file=$r.file; module=$i.module; name=$i.name; kind=$i.kind } }
}
if ($impRows) { $impRows | Sort-Object file,module,name | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutDir "coc_imports.csv") }

$errRows = $data | Where-Object { -not $_.syntax_ok } | ForEach-Object {
  [pscustomobject]@{ file=$_.file; error=$_.error }
}
if ($errRows) { $errRows | Export-Csv -NoTypeInformation -Encoding UTF8 (Join-Path $OutDir "coc_syntax_errors.csv") }

# 5) Markdown summary
$invCount   = ($inv | Measure-Object).Count
$pyCount    = ($inv | Where-Object { $_.ext -eq ".py" } | Measure-Object).Count
$errCount   = ($errRows | Measure-Object).Count
$tp         = ($impRows | Where-Object { $_.kind -eq "thirdparty" } | Select-Object -ExpandProperty module -Unique | Measure-Object).Count
$loc        = ($impRows | Where-Object { $_.kind -eq "local" } | Select-Object -ExpandProperty module -Unique | Measure-Object).Count
$hintCounts = ($sum.hints | Where-Object { $_ } | ForEach-Object { $_.Split(";") } | Group-Object | Sort-Object Count -Descending | Select-Object -First 12)

$md = @()
$md += "# CoC Folder Audit"
$md += ""
$md += "*generated:* $((Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm 'UTC'"))"
$md += "*root:* $CocRoot"
$md += ""
$md += "## High-level"
$md += "- files scanned: **$invCount**  (python: **$pyCount**)"
$md += "- python syntax errors: **$errCount**"
$md += "- unique third-party modules: **$tp**"
$md += "- local modules referenced: **$loc**"
$md += ""
$md += "## Top Hints (OPRT/CoC patterns)"
if ($hintCounts) {
  foreach ($h in $hintCounts) { $md += "- $($h.Name): $($h.Count)" }
} else {
  $md += "- (no hints found)"
}
$md += ""
$md += "## Files"
$md += "See CSVs in this folder:"
$md += "- coc_inventory.csv"
$md += "- coc_python_summary.csv"
$md += "- coc_functions.csv / coc_classes.csv / coc_imports.csv"
if ($errCount -gt 0) { $md += "- coc_syntax_errors.csv" }
$md += ""
$md += "## Next steps"
$md += "1) Upload this folder (or the ZIP) so I can cross-check with the daily loop."
$md += "2) We’ll wire CoC metrics into the EOD/analytics dashboards (C_global alignment & phase-angle deltas)."
$mdText = $md -join "`r`n"
$mdText | Set-Content -Encoding UTF8 (Join-Path $OutDir "coc_audit.md")

# 6) Zip bundle for upload
$zipPath = Join-Path $OutDir ("coc_audit_" + $stamp + ".zip")
try {
  if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
  Compress-Archive -Path (Join-Path $OutDir "*") -DestinationPath $zipPath -Force
} catch {
  Write-Host "[WARN] Could not zip; continuing."
}

Write-Host "==============================================="
Write-Host "CoC audit written to: $OutDir"
Write-Host "Bundle: $zipPath"
Write-Host "Artifacts:"
Get-ChildItem $OutDir | Select-Object Name,Length | Format-Table -AutoSize
Write-Host "==============================================="
