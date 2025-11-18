# Installation Guide - Offline Installation on Bank Server

## Preparation Phase (On Your Computer with Internet)

### Step 1: Download Python 3.11
- Go to: `https://www.python.org/downloads/release/python-3119/`
- Download: `python-3.11.9-amd64.exe` (~25 MB)
- Save to: `C:\temp\bank-installation\`

### Step 2: Download ODBC Driver 18
- Go to: `https://go.microsoft.com/fwlink/?linkid=2249004`
- Download: `msodbcsql_18.x.x.x_x64.msi` (~5-10 MB)
- Save to: `C:\temp\bank-installation\`

### Step 3: Download Python Packages
```powershell
cd F:\pianat\reporting_system_python
.\download-offline-packages.ps1
```
Wait 5-15 minutes (~150-200 MB)

### Step 4: Organize Files for Transfer
```powershell
mkdir C:\temp\bank-deployment-package

# Copy installers
Copy-Item C:\temp\bank-installation\* C:\temp\bank-deployment-package\

# Copy Python packages
Copy-Item -Recurse F:\pianat\reporting_system_python\python-packages-offline C:\temp\bank-deployment-package\

# Copy application files
Copy-Item -Recurse F:\pianat\reporting_system_python C:\temp\bank-deployment-package\reporting_system_python

# Remove .venv if present
Remove-Item -Recurse -Force "C:\temp\bank-deployment-package\reporting_system_python\.venv" -ErrorAction SilentlyContinue
```

**Final Structure:**
```
C:\temp\bank-deployment-package\
├── python-3.11.9-amd64.exe
├── msodbcsql_18.x.x.x_x64.msi
├── python-packages-offline\
└── reporting_system_python\
```

---

## Transfer Phase (VPN + Remote Desktop)

### Step 5: Transfer Files to Bank Server

1. Connect to VPN
2. Connect to bank server
3. Copy from: `C:\temp\bank-deployment-package\`
4. Paste to: `C:\temp\installers\` (create folder if needed)
5. Wait for copy (10-30 minutes)

**Verify:**
```powershell
cd C:\temp\installers
dir
```

---

## Installation Phase (On Bank Server)

### Step 6: Install Python 3.11
```powershell
cd C:\temp\installers
.\python-3.11.9-amd64.exe
```
- ✅ Check "Add Python 3.11 to PATH"
- ✅ Check "Install for all users"
- Close and reopen PowerShell
- Verify: `python --version`

**If not found:**
```powershell
$pythonPath = "C:\Users\grcsvc\AppData\Local\Programs\Python\Python311"
$pythonScripts = "$pythonPath\Scripts"
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";$pythonPath;$pythonScripts", "User")
```

### Step 7: Install ODBC Driver 18
```powershell
cd C:\temp\installers
msiexec /i msodbcsql_18.x.x.x_x64.msi /quiet IACCEPTMSODBCSQLLICENSETERMS=YES
```
**Verify:**
```powershell
Get-OdbcDriver | Where-Object {$_.Name -like "*ODBC Driver 18*"}
```

### Step 8: Install Python Packages
```powershell
cd C:\temp\installers\python-packages-offline
python -m pip install --no-index --find-links . -r ..\reporting_system_python\requirements.txt
```
Wait 5-15 minutes

**Verify:**
```powershell
python -c "import fastapi; import pyodbc; print('OK')"
```

### Step 9: Copy Application Files
```powershell
mkdir C:\apps\reporting_system_python
Copy-Item -Recurse C:\temp\installers\reporting_system_python\* C:\apps\reporting_system_python\
```

---

## Verification

### Step 10: Verify Installation
```powershell
python --version
pip --version
Get-OdbcDriver | Where-Object {$_.Name -like "*ODBC Driver 18*"}
python -c "import fastapi; import pyodbc; print('OK')"
cd C:\apps\reporting_system_python
Test-Path main.py
Test-Path environment.env
```

---

## Running the Application

### Step 11: Test Application
```powershell
cd C:\apps\reporting_system_python
python main.py
```
Look for: `✅ DATABASE CONNECTION: SUCCESS`
Stop: `Ctrl + C`

---

## Set Up as Windows Service (Always Running)

### Option 1: NSSM (Recommended)

1. Download NSSM: https://nssm.cc/download
2. Copy `nssm.exe` to bank server: `C:\tools\nssm\`

3. Install service:
```powershell
cd C:\tools\nssm
$pythonPath = (Get-Command python).Source
.\nssm.exe install ReportingSystemPython "$pythonPath" "C:\apps\reporting_system_python\main.py"
.\nssm.exe set ReportingSystemPython AppDirectory "C:\apps\reporting_system_python"
.\nssm.exe set ReportingSystemPython ObjectName "ADIBEG\GRCSVC" "P@ssw0rd"
mkdir C:\apps\reporting_system_python\logs -ErrorAction SilentlyContinue
.\nssm.exe set ReportingSystemPython AppStdout "C:\apps\reporting_system_python\logs\app.log"
.\nssm.exe set ReportingSystemPython AppStderr "C:\apps\reporting_system_python\logs\error.log"
.\nssm.exe set ReportingSystemPython AppRestartDelay 5000
.\nssm.exe set ReportingSystemPython AppExit Default Restart
.\nssm.exe set ReportingSystemPython Start SERVICE_AUTO_START
.\nssm.exe start ReportingSystemPython
```

**Service Commands:**
```powershell
Start-Service ReportingSystemPython
Stop-Service ReportingSystemPython
Restart-Service ReportingSystemPython
Get-Service ReportingSystemPython
Get-Content C:\apps\reporting_system_python\logs\app.log -Tail 50
```

### Option 2: Task Scheduler (Simple)

1. Open: `Windows Key + R` → `taskschd.msc`
2. Create Basic Task:
   - Name: `Reporting System Python API`
   - Trigger: "When the computer starts"
   - Action: "Start a program"
     - Program: `python.exe` (or `(Get-Command python).Source`)
     - Arguments: `C:\apps\reporting_system_python\main.py`
     - Start in: `C:\apps\reporting_system_python`
3. Properties:
   - ✅ "Run whether user is logged on or not"
   - ✅ "Run with highest privileges"
   - User: `ADIBEG\GRCSVC`
   - Settings: ✅ "If task fails, restart every: 1 minute"

---

## Firewall (If Needed)

```powershell
New-NetFirewallRule -DisplayName "Reporting System Python API" -Direction Inbound -LocalPort 8000 -Protocol TCP -Action Allow
```

---

## Final Verification

```powershell
Get-Service ReportingSystemPython  # For NSSM
# OR
Get-Process python  # For Task Scheduler

Get-Content C:\apps\reporting_system_python\logs\app.log -Tail 50
curl http://localhost:8000/docs
```

---

## Troubleshooting

### Python Not Found
```powershell
Get-ChildItem "C:\Users\$env:USERNAME\AppData\Local\Programs\Python" -ErrorAction SilentlyContinue
$pythonPath = "C:\Users\grcsvc\AppData\Local\Programs\Python\Python311"
$pythonScripts = "$pythonPath\Scripts"
[Environment]::SetEnvironmentVariable("Path", $env:Path + ";$pythonPath;$pythonScripts", "User")
```

### ODBC Driver Not Found
```powershell
cd C:\temp\installers
.\msodbcsql_18.x.x.x_x64.msi
```

### Package Installation Fails
- Verify: `python --version` (must be 3.11.x)
- Try: `python -m pip install --no-index --find-links . fastapi`

### Database Connection Fails
- Check user: `whoami` (should be ADIBEG\GRCSVC)
- Test network: `Test-NetConnection -ComputerName 10.240.10.202 -Port 5555`

### Service Won't Start
```powershell
Get-Content C:\apps\reporting_system_python\logs\error.log
cd C:\apps\reporting_system_python
python main.py
```

### Port Already in Use
```powershell
netstat -ano | findstr :8000
taskkill /PID [PID] /F
```

---

## Checklist

- [ ] Python 3.11 installed and in PATH
- [ ] pip working
- [ ] ODBC Driver 18 installed
- [ ] Python packages installed
- [ ] Application files in `C:\apps\reporting_system_python`
- [ ] `environment.env` configured
- [ ] Database connection successful
- [ ] Windows Service/Task installed
- [ ] Firewall configured (if needed)

---

## Summary

**Files Required**: ~200-300 MB total
- Python installer: ~25 MB
- ODBC Driver: ~5-10 MB
- Python packages: ~150-200 MB
- Application files: ~10-50 MB

**Time Required**: 1.5 - 2.5 hours

**Result**: Application runs automatically on server startup, restarts on crash, runs as ADIBEG\GRCSVC user.
