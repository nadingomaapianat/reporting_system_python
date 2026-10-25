#!/usr/bin/env python3
"""Fail (exit 1) when a package pinned in requirements.txt has a known advisory that is not in
.audit-allowlist.json, or when an allowlist entry has passed its expiry date.

pip-audit does not report severity, so every advisory counts. The pinned versions are checked
without installing anything (--no-deps --disable-pip). Used by the Dependency audit workflow;
run locally with: pip install pip-audit==2.9.0 && python scripts/audit_check.py
"""
import datetime
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
allow_path = os.path.join(ROOT, ".audit-allowlist.json")
allow = json.load(open(allow_path, encoding="utf-8")) if os.path.exists(allow_path) else []
today = datetime.date.today().isoformat()

proc = subprocess.run(
    [sys.executable, "-m", "pip_audit", "-r", os.path.join(ROOT, "requirements.txt"),
     "--no-deps", "--disable-pip", "--progress-spinner", "off", "-f", "json"],
    capture_output=True, text=True,
)
try:
    report = json.loads(proc.stdout)
except ValueError:
    print("Could not read the pip-audit report:\n" + proc.stdout[:2000] + proc.stderr[:2000])
    sys.exit(1)

problems = []
for dep in report.get("dependencies", []):
    for vuln in dep.get("vulns", []):
        ids = {vuln["id"], *vuln.get("aliases", [])}
        entry = next((a for a in allow if a["id"] in ids), None)
        fixes = ", ".join(vuln.get("fix_versions", [])) or "none yet"
        if entry is None:
            problems.append(f"{dep['name']} {dep['version']}: {vuln['id']} (fixed in {fixes})")
        elif entry["expires"] < today:
            problems.append(f"ALLOWLIST EXPIRED {entry['expires']}: {dep['name']} ({vuln['id']}) - upgrade it or renew the entry with a reason")
        else:
            print(f"allowed until {entry['expires']}: {dep['name']} ({vuln['id']}) - {entry['reason']}")

if problems:
    print("\nKnown vulnerabilities in pinned requirements:\n  " + "\n  ".join(problems))
    print("\nFix: pin a fixed version in requirements.txt. If it truly cannot be reached in this app,")
    print("add it to .audit-allowlist.json with a reason and an expiry date.")
    sys.exit(1)
print("No unallowed advisories in requirements.txt.")
