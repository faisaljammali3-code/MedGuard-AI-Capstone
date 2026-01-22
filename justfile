# Justfile
set shell := ["powershell.exe", "-c"]

default:
    @just --list

# 1. Run Main Package
#uv run python -m src.MedGuard_AI
#uv run python src/MedGuard_AI/pipelines/etl.py

run:
    env PYTHONPATH=src uv run python -m MedGuard_AI.pipelines.etl

# 2. Context Generator (Smart Notebook Parsing)
plan:
    #!powershell
    $outFile = "CANVAS.md"
    Write-Host 'Generating AI Context file...' -ForegroundColor Cyan
    
    $sb = new-object System.Text.StringBuilder
    $null = $sb.AppendLine("# Project Context: MedGuard-AI")
    $null = $sb.AppendLine("Generated: 01/20/2026 17:30:15")
    $null = $sb.AppendLine("> NOTE: Notebook outputs are stripped for brevity.")
    $null = $sb.AppendLine("")
    
    # --- Section 1: Tree ---
    $null = $sb.AppendLine('## 1. Project Structure')
    $null = $sb.AppendLine('`	ext')
    
    $items = Get-ChildItem -Recurse | Where-Object { 
        $_.FullName -notmatch "[\\/]\.venv" -and 
        $_.FullName -notmatch "[\\/]\.git" -and 
        $_.FullName -notmatch "[\\/]__pycache__" -and
        $_.FullName -notmatch "[\\/]\.ipynb_checkpoints" -and
        $_.FullName -notmatch "[\\/]data[\\/](raw|processed|external|cache)" -and
        $_.Name -ne "CANVAS.md" -and 
        $_.Name -ne "uv.lock"
    }
    
    foreach ($i in $items) {
        $rel = $i.FullName.Substring($PWD.Path.Length + 1)
        $null = $sb.AppendLine($rel)
    }
    $null = $sb.AppendLine('`')
    $null = $sb.AppendLine("")
    
    # --- Section 2: Contents ---
    $null = $sb.AppendLine('## 2. File Contents')
    
    # Filter for code files AND notebooks
    $codeItems = $items | Where-Object { -not $_.PSIsContainer -and $_.Extension -match "\.(py|toml|md|json|yaml|yml|ipynb)$" }
    
    foreach ($i in $codeItems) {
        $rel = $i.FullName.Substring($PWD.Path.Length + 1)
        $ext = $i.Extension.Trim(".")
        $null = $sb.AppendLine("### File: $rel")
        
        if ($ext -eq "ipynb") {
            # --- INTELLIGENT NOTEBOOK PARSING ---
            $null = $sb.AppendLine('`python')
            try {
                # Read JSON and loop through cells
                $json = Get-Content $i.FullName -Raw -Encoding UTF8 | ConvertFrom-Json
                foreach ($cell in $json.cells) {
                    if ($cell.cell_type -eq "code") {
                        # Join array of strings lines
                        $src = $cell.source -join ""
                        if ($src.Trim().Length -gt 0) {
                            $null = $sb.AppendLine("# [CELL: CODE]")
                            $null = $sb.AppendLine($src)
                            $null = $sb.AppendLine("")
                        }
                    } elseif ($cell.cell_type -eq "markdown") {
                        $src = $cell.source -join ""
                        if ($src.Trim().Length -gt 0) {
                            $null = $sb.AppendLine("# [CELL: MARKDOWN]")
                            # Comment out markdown so it doesn't break python syntax highlighting
                            $commented = $src -replace "(?m)^", "# "
                            $null = $sb.AppendLine($commented)
                            $null = $sb.AppendLine("")
                        }
                    }
                }
            } catch {
                $null = $sb.AppendLine("# Error parsing notebook JSON")
            }
            $null = $sb.AppendLine('`')
        
        } else {
            # --- STANDARD TEXT FILE ---
            $null = $sb.AppendLine("`$ext")
            try {
                $text = [System.IO.File]::ReadAllText($i.FullName)
                $null = $sb.AppendLine($text)
            } catch {
                 $null = $sb.AppendLine("# Error reading file")
            }
            $null = $sb.AppendLine('`')
        }
        $null = $sb.AppendLine("")
    }
    
    $finalPath = Join-Path $PWD $outFile
    [System.IO.File]::WriteAllText($finalPath, $sb.ToString())
    
    Write-Host "Context saved to $outFile" -ForegroundColor Green
    code $outFile

# 3. Smart Script Runner
s input:
    #!powershell
    $target = "{{input}}"
    $finalPath = ""
    
    if ($target.EndsWith("x")) {
        if ($target -match "[\\/]") {
            $dir = $target.Substring(0, $target.Length - 1)
            if (-not (Test-Path $dir)) { New-Item -Path $dir -ItemType Directory -Force | Out-Null }
            $count = (Get-ChildItem $dir -Filter "module_*.py").Count + 1
            $finalPath = Join-Path $dir "module_$count.py"
        } else {
            $count = (Get-ChildItem "scripts" -Filter "script_*.py").Count + 1
            $finalPath = "scripts/script_$count.py"
        }
    } else {
        if (-not $target.EndsWith(".py")) { $target += ".py" }
        if ($target -match "[\\/]") { $finalPath = $target } else { $finalPath = "scripts/$target" }
    }

    $parentDir = Split-Path -Parent $finalPath
    if (-not (Test-Path $parentDir) -and $parentDir -ne "") {
        New-Item -Path $parentDir -ItemType Directory -Force | Out-Null
    }

    if (-not (Test-Path $finalPath)) {
        Write-Host "[CREATE] New script: $finalPath" -ForegroundColor Green
        # r string for raw python paths
        $lines = @(
            "from loguru import logger",
            "",
            "def main():",
            "    logger.info(r'Hello from MedGuard-AI! Script: $finalPath')",
            "",
            "if __name__ == '__main__':",
            "    main()"
        )
        $template = $lines -join [Environment]::NewLine
        [System.IO.File]::WriteAllText($finalPath, $template)
    }
    
    Write-Host "[OPEN] Opening in VS Code..." -ForegroundColor Cyan
    code $finalPath
    
    Write-Host "[RUN] Running script..." -ForegroundColor Cyan
    uv run python $finalPath

# 4. Smart Notebook Launcher
nb input="":
    #!powershell
    $target = "{{input}}"
    
    if (-not $target) {
        Write-Host "[OPEN] Opening notebooks folder..." -ForegroundColor Cyan
        code notebooks/
        exit
    }

    $finalName = ""
    if ($target -eq "x") {
        $count = (Get-ChildItem "notebooks" -Filter "notebook_*.ipynb").Count + 1
        $finalName = "notebook_$count.ipynb"
    } else {
        $finalName = $target
        if (-not $finalName.EndsWith(".ipynb")) { $finalName += ".ipynb" }
    }

    $path = "notebooks/$finalName"
    
    if (-not (Test-Path $path)) {
        Write-Host "[CREATE] New notebook: $path" -ForegroundColor Green
        $nbObj = @{ cells = @(); metadata = @{}; nbformat = 4; nbformat_minor = 5 }
        $minimalNb = $nbObj | ConvertTo-Json -Compress
        [System.IO.File]::WriteAllText($path, $minimalNb)
    }
    
    Write-Host "[OPEN] Opening in VS Code..." -ForegroundColor Cyan
    code $path

# Quality & Clean
fix:
    uv run ruff check . --fix
    uv run ruff format .

clean:
    Remove-Item -Recurse -Force .pytest_cache, __pycache__, .ruff_cache, .ipynb_checkpoints -ErrorAction SilentlyContinue
    Get-ChildItem -Path "data/cache/*" -Exclude ".gitkeep" -Recurse -ErrorAction SilentlyContinue | Remove-Item -Force