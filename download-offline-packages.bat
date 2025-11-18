@echo off
REM ========================================
REM Download Python Packages for Offline Install
REM Run this on your computer WITH internet access
REM ========================================

echo.
echo ========================================
echo Download Python Packages for Offline Install
echo ========================================
echo.

REM Check if requirements.txt exists
if not exist "requirements.txt" (
    echo ❌ Error: requirements.txt not found!
    echo    Make sure you run this script from the project root directory
    pause
    exit /b 1
)

REM Create directory for offline packages
set OFFLINE_DIR=python-packages-offline
if exist "%OFFLINE_DIR%" (
    echo ⚠️  Directory %OFFLINE_DIR% already exists
    set /p RESPONSE="Delete and recreate? (y/n): "
    if /i "%RESPONSE%"=="y" (
        rmdir /s /q "%OFFLINE_DIR%"
        echo ✅ Deleted existing directory
    ) else (
        echo ❌ Aborted
        pause
        exit /b 1
    )
)

mkdir "%OFFLINE_DIR%"
echo ✅ Created directory: %OFFLINE_DIR%
echo.

REM Check if Python and pip are available
echo [1/3] Checking Python and pip...
python --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ❌ Error: Python not found!
    echo    Make sure Python is installed and added to PATH
    pause
    exit /b 1
)
python --version
python -m pip --version >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ❌ Error: pip not found!
    echo    Make sure Python is installed and added to PATH
    pause
    exit /b 1
)
python -m pip --version
echo ✅ Python and pip found
echo.

REM Download packages
echo [2/3] Downloading packages from requirements.txt...
echo    This may take several minutes depending on your internet speed...
echo.

python -m pip download -r requirements.txt -d "%OFFLINE_DIR%"

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ Error downloading packages!
    pause
    exit /b 1
)

echo.
echo ✅ Packages downloaded successfully!
echo.

REM Count downloaded files
for /f %%i in ('dir /b "%OFFLINE_DIR%\*.*" ^| find /c /v ""') do set COUNT=%%i
echo [3/3] Summary:
echo    📦 Packages downloaded: %COUNT% files
echo    📁 Location: %CD%\%OFFLINE_DIR%
echo.

echo ========================================
echo Next Steps:
echo ========================================
echo.
echo 1. Copy the '%OFFLINE_DIR%' folder to USB drive or network share
echo 2. Transfer to bank server
echo 3. On bank server, run:
echo    pip install --no-index --find-links . -r requirements.txt
echo    (from within the %OFFLINE_DIR% directory)
echo.

pause

