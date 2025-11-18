# PowerShell Script: Download Python Packages for Offline Installation
# Run this on your computer WITH internet access
# This will download all required packages to a local folder

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Download Python Packages for Offline Install" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if requirements.txt exists
if (-not (Test-Path "requirements.txt")) {
    Write-Host "❌ Error: requirements.txt not found!" -ForegroundColor Red
    Write-Host "   Make sure you run this script from the project root directory" -ForegroundColor Yellow
    exit 1
}

# Create directory for offline packages
$offlineDir = "python-packages-offline"
if (Test-Path $offlineDir) {
    Write-Host "⚠️  Directory $offlineDir already exists" -ForegroundColor Yellow
    $response = Read-Host "Delete and recreate? (y/n)"
    if ($response -eq "y" -or $response -eq "Y") {
        Remove-Item -Recurse -Force $offlineDir
        Write-Host "✅ Deleted existing directory" -ForegroundColor Green
    } else {
        Write-Host "❌ Aborted" -ForegroundColor Red
        exit 1
    }
}

New-Item -ItemType Directory -Path $offlineDir | Out-Null
Write-Host "✅ Created directory: $offlineDir" -ForegroundColor Green
Write-Host ""

# Check if Python and pip are available
Write-Host "[1/3] Checking Python and pip..." -ForegroundColor Cyan

# Use py launcher first (most reliable on Windows), then try python, then full path
$pythonCmd = $null

# Try py launcher first
if (Get-Command py -ErrorAction SilentlyContinue) {
    $pythonCmd = "py"
    Write-Host "✅ Using Python launcher (py)" -ForegroundColor Gray
    try {
        $pythonVersion = py --version 2>&1
        Write-Host "✅ Python found: $pythonVersion" -ForegroundColor Green
    } catch {
        Write-Host "❌ Error: Could not run Python launcher!" -ForegroundColor Red
        exit 1
    }
}
# Try python command (but check if it's Windows Store alias)
elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $pythonExe = (Get-Command python -ErrorAction SilentlyContinue).Source
    if ($pythonExe -like "*WindowsApps*") {
        # Windows Store alias - find actual Python
        Write-Host "⚠️  Windows Store alias detected, finding actual Python..." -ForegroundColor Yellow
        $pythonPaths = @(
            "$env:LOCALAPPDATA\Programs\Python\Python311\python.exe",
            "$env:ProgramFiles\Python311\python.exe",
            "$env:ProgramFiles(x86)\Python311\python.exe"
        )
        foreach ($path in $pythonPaths) {
            if (Test-Path $path) {
                $pythonCmd = $path
                Write-Host "✅ Found Python at: $path" -ForegroundColor Gray
                try {
                    $pythonVersion = & $path --version 2>&1
                    Write-Host "✅ Python found: $pythonVersion" -ForegroundColor Green
                } catch {
                    Write-Host "❌ Error: Could not run Python!" -ForegroundColor Red
                    exit 1
                }
                break
            }
        }
        if (-not $pythonCmd) {
            Write-Host "❌ Error: Could not find Python installation!" -ForegroundColor Red
            Write-Host "   Please install Python 3.11 from python.org" -ForegroundColor Yellow
            exit 1
        }
    } else {
        $pythonCmd = "python"
        try {
            $pythonVersion = python --version 2>&1
            Write-Host "✅ Python found: $pythonVersion" -ForegroundColor Green
        } catch {
            Write-Host "❌ Error: Could not run Python!" -ForegroundColor Red
            exit 1
        }
    }
} else {
    Write-Host "❌ Error: Python not found!" -ForegroundColor Red
    Write-Host "   Please install Python 3.11 from python.org" -ForegroundColor Yellow
    exit 1
}

# Test pip
try {
    if ($pythonCmd -eq "py") {
        $pipVersion = py -m pip --version 2>&1
    } elseif ($pythonCmd -eq "python") {
        $pipVersion = python -m pip --version 2>&1
    } else {
        # Full path
        $pipVersion = & $pythonCmd -m pip --version 2>&1
    }
    Write-Host "✅ pip found: $pipVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Error: pip not found!" -ForegroundColor Red
    Write-Host "   Make sure Python is installed with pip" -ForegroundColor Yellow
    exit 1
}
Write-Host ""

# Download packages
Write-Host "[2/3] Downloading packages from requirements.txt..." -ForegroundColor Cyan
Write-Host "   This may take several minutes depending on your internet speed..." -ForegroundColor Yellow
Write-Host ""

# Build download command based on Python command found
if ($pythonCmd -eq "py") {
    py -m pip download -r requirements.txt -d $offlineDir
} elseif ($pythonCmd -eq "python") {
    python -m pip download -r requirements.txt -d $offlineDir
} else {
    # Full path
    & $pythonCmd -m pip download -r requirements.txt -d $offlineDir
}

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "❌ Error downloading packages!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "✅ Packages downloaded successfully!" -ForegroundColor Green
Write-Host ""

# Count downloaded files
$packageCount = (Get-ChildItem -Path $offlineDir -File).Count
Write-Host "[3/3] Summary:" -ForegroundColor Cyan
Write-Host "   📦 Packages downloaded: $packageCount files" -ForegroundColor Green
Write-Host "   📁 Location: $PWD\$offlineDir" -ForegroundColor Green
Write-Host ""

# Calculate total size
$totalSize = (Get-ChildItem -Path $offlineDir -Recurse -File | Measure-Object -Property Length -Sum).Sum
$totalSizeMB = [math]::Round($totalSize / 1MB, 2)
Write-Host "   💾 Total size: $totalSizeMB MB" -ForegroundColor Green
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Copy the '$offlineDir' folder to USB drive or network share" -ForegroundColor Yellow
Write-Host "2. Transfer to bank server" -ForegroundColor Yellow
Write-Host "3. On bank server, run:" -ForegroundColor Yellow
Write-Host "   pip install --no-index --find-links . -r requirements.txt" -ForegroundColor White
Write-Host "   (from within the $offlineDir directory)" -ForegroundColor Gray
Write-Host ""

