#!/usr/bin/env python3
"""
RPR GST + IT Automation Suite v3.5 ADVANCED PRO
Installation Verification Script
Updated: May 19, 2026 (v12 build)

Checks ALL dependencies discovered from full scan of all 32 .py files:
  TIER 1 : Core Excel + Data
  TIER 2 : Core PDF Processing
  TIER 3 : Browser Automation
  TIER 4 : Web / HTTP
  TIER 5 : Tally Integration (pyodbc, pyautogui, pygetwindow, pywin32,
            Pillow, pytesseract) + Tesseract binary check
  TIER 6 : Utilities
  TIER 7 : Optional
"""

import sys
import os
import subprocess
from pathlib import Path

# ── Terminal colour codes ────────────────────────────────────────────────────
GREEN  = '\033[92m'
RED    = '\033[91m'
YELLOW = '\033[93m'
BLUE   = '\033[94m'
CYAN   = '\033[96m'
RESET  = '\033[0m'
BOLD   = '\033[1m'
DIM    = '\033[2m'

# ── Helpers ──────────────────────────────────────────────────────────────────

def check_python_version():
    """Check Python version is 3.10+."""
    print(f"\n{BOLD}Checking Python Version...{RESET}")
    vi = sys.version_info
    ver = f"{vi.major}.{vi.minor}.{vi.micro}"
    if vi.major < 3 or (vi.major == 3 and vi.minor < 10):
        print(f"{RED}✗ Python {ver} — UPGRADE REQUIRED (need 3.10+){RESET}")
        return False
    print(f"{GREEN}✓ Python {ver} — OK{RESET}")
    return True


def check_package(package_name, import_name=None, min_version=None, extra_check=None):
    """
    Check if a package is importable and meets the minimum version.

    Returns (success: bool, version: str|None, error: str|None)
    extra_check: optional callable(module) → (ok: bool, note: str)
    """
    iname = import_name or package_name.lower().replace('-', '_')
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            module = __import__(iname)
            version = getattr(module, '__version__', None)

        # pyodbc stores version in .version not .__version__
        if version is None:
            version = getattr(module, 'version', None)
        # Fallback: importlib.metadata (works for packages that dropped __version__)
        if version is None:
            try:
                import importlib.metadata
                version = importlib.metadata.version(package_name)
            except Exception:
                version = 'unknown'
        if version is None:
            version = 'unknown'

        if min_version and version != 'unknown':
            try:
                from packaging import version as pv
                if pv.parse(version) < pv.parse(min_version):
                    return False, version, f"requires {min_version}+, have {version}"
            except ImportError:
                try:
                    inst = tuple(int(x) for x in version.split('.')[:3])
                    req  = tuple(int(x) for x in min_version.split('.')[:3])
                    if inst < req:
                        return False, version, f"requires {min_version}+, have {version}"
                except ValueError:
                    pass  # non-numeric version — skip comparison

        if extra_check:
            ok, note = extra_check(module)
            if not ok:
                return False, version, note

        return True, version, None
    except ImportError as e:
        return False, None, str(e)


def check_tesseract_binary():
    """Check whether the Tesseract OCR binary is accessible."""
    # 1. Try PATH
    try:
        result = subprocess.run(
            ['tesseract', '--version'],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            first_line = result.stdout.strip().splitlines()[0] if result.stdout else ''
            return True, first_line or 'found on PATH'
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass

    # 2. Try common Windows install paths
    common_paths = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        r"C:\Tesseract-OCR\tesseract.exe",
        r"D:\Program Files\Tesseract-OCR\tesseract.exe",
    ]
    for p in common_paths:
        if os.path.isfile(p):
            return True, f"found at {p}"

    return False, (
        "Tesseract binary NOT found.\n"
        "  Download: https://github.com/UB-Mannheim/tesseract/wiki\n"
        "  Install to: C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
    )


def check_tally_odbc_driver():
    """Check whether the TallyODBC ODBC DSN / driver is registered (Windows only)."""
    if sys.platform != 'win32':
        return False, "ODBC check only available on Windows"
    try:
        import winreg
        found_keys = []
        hives = [
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"),
            (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\ODBC\ODBC.INI\ODBC Data Sources"),
            (winreg.HKEY_CURRENT_USER,  r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"),
        ]
        for hive, path in hives:
            try:
                with winreg.OpenKey(hive, path) as key:
                    i = 0
                    while True:
                        try:
                            name, _, _ = winreg.EnumValue(key, i)
                            if 'tally' in name.lower() or 'odbc' in name.lower():
                                found_keys.append(name)
                            i += 1
                        except OSError:
                            break
            except OSError:
                pass
        if found_keys:
            return True, f"DSN(s) found: {', '.join(found_keys)}"
        return False, (
            "No TallyODBC DSN found in registry.\n"
            "  Enable in TallyPrime: Gateway of Tally > F12 > ODBC Server > Yes\n"
            "  Default port: 9000"
        )
    except Exception as e:
        return False, f"Registry check failed: {e}"


def print_tier_header(title):
    print(f"\n{BOLD}{title}{RESET}")
    print("─" * 60)


def print_result(label, success, version, error, required=True):
    """Print a single package check result line."""
    if success:
        ver_str = f"  {DIM}v{version}{RESET}" if version and version != 'unknown' else ""
        print(f"  {GREEN}✓{RESET}  {label:<26}{ver_str}")
    else:
        if required:
            icon = f"{RED}✗{RESET}"
            suffix = f"  {RED}[{error or 'NOT INSTALLED'}]{RESET}"
        else:
            icon = f"{YELLOW}○{RESET}"
            suffix = f"  {YELLOW}[{error or 'not installed'}]{RESET}"
        print(f"  {icon}  {label:<26}{suffix}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{BOLD}{'═'*60}")
    print(f"  RPR GST + IT Automation Suite v3.5 ADVANCED PRO")
    print(f"  Installation Verification Report")
    print(f"  Updated: May 19, 2026  (v12 build)")
    print(f"{'═'*60}{RESET}")

    python_ok = check_python_version()

    # ── TIER 1: Core Excel & Data ────────────────────────────────────────────
    print_tier_header("TIER 1 — Core Excel & Data Processing")
    tier1 = [
        # (display_name, import_name, min_version)
        ('pandas',          'pandas',    '2.0.0'),
        ('openpyxl',        'openpyxl',  '3.1.0'),
        ('numpy',           'numpy',     '1.24.0'),
        ('xlrd',            'xlrd',      '2.0.0'),
    ]
    t1_ok = 0
    for pkg, imp, minv in tier1:
        ok, ver, err = check_package(pkg, imp, minv)
        t1_ok += ok
        print_result(pkg, ok, ver, err, required=True)

    # ── TIER 2: PDF Processing ───────────────────────────────────────────────
    print_tier_header("TIER 2 — PDF Processing  (3-tier fallback chain)")
    tier2 = [
        ('pdfplumber',      'pdfplumber', '0.9.0'),
        ('pypdf',           'pypdf',      '3.0.0'),
        ('PyPDF2',          'PyPDF2',     '3.0.0'),
    ]
    t2_ok = 0
    for pkg, imp, minv in tier2:
        ok, ver, err = check_package(pkg, imp, minv)
        t2_ok += ok
        print_result(pkg, ok, ver, err, required=True)

    # ── TIER 3: Browser Automation ───────────────────────────────────────────
    print_tier_header("TIER 3 — Browser Automation (GST/IT Portal Downloads)")
    tier3 = [
        ('selenium',           'selenium',          '4.0.0'),
        ('webdriver-manager',  'webdriver_manager', '3.9.0'),
    ]
    t3_ok = 0
    for pkg, imp, minv in tier3:
        ok, ver, err = check_package(pkg, imp, minv)
        t3_ok += ok
        print_result(pkg, ok, ver, err, required=True)

    # ── TIER 4: Web / HTTP ───────────────────────────────────────────────────
    print_tier_header("TIER 4 — Web / HTTP Server")
    tier4 = [
        ('flask',       'flask',    '2.3.0'),
        ('requests',    'requests', '2.28.0'),
        ('werkzeug',    'werkzeug', '2.3.0'),
        ('urllib3',     'urllib3',  '1.26.0'),
        ('gunicorn',    'gunicorn', '21.0.0'),
    ]
    t4_ok = 0
    for pkg, imp, minv in tier4:
        ok, ver, err = check_package(pkg, imp, minv)
        t4_ok += ok
        print_result(pkg, ok, ver, err, required=True)

    # ── TIER 5: Tally Integration ────────────────────────────────────────────
    print_tier_header("TIER 5 — Tally Integration")
    print(f"  {DIM}Required by: tally_extract_gst.py, tally_extract_gst_v3.15_FIXED.py,{RESET}")
    print(f"  {DIM}             tally_extract_gst automated.py{RESET}")
    print()

    tally_ok = 0
    tally_total = 0

    # pyodbc
    tally_total += 1
    ok, ver, err = check_package('pyodbc', 'pyodbc', '4.0.0')
    tally_ok += ok
    print_result('pyodbc', ok, ver, err, required=True)
    if ok:
        # Check TallyODBC DSN in registry
        odbc_ok, odbc_note = check_tally_odbc_driver()
        sym = f"{GREEN}✓{RESET}" if odbc_ok else f"{YELLOW}○{RESET}"
        print(f"     {sym}  {DIM}TallyODBC DSN: {odbc_note}{RESET}")

    # pyautogui
    tally_total += 1
    ok, ver, err = check_package('pyautogui', 'pyautogui', '0.9.54')
    tally_ok += ok
    print_result('pyautogui', ok, ver, err, required=False)

    # pygetwindow
    tally_total += 1
    ok, ver, err = check_package('pygetwindow', 'pygetwindow', '0.0.9')
    tally_ok += ok
    print_result('pygetwindow', ok, ver, err, required=False)

    # pywin32 — imports as win32gui
    tally_total += 1
    ok, ver, err = check_package('pywin32', 'win32gui', None)
    tally_ok += ok
    print_result('pywin32  (win32gui)', ok, ver, err, required=False)

    # Pillow — imports as PIL
    tally_total += 1
    ok, ver, err = check_package('Pillow', 'PIL', '9.0.0')
    if ok and ver in (None, 'unknown'):
        # PIL stores version in PIL.__version__ but __import__('PIL') gives package
        try:
            import PIL
            ver = PIL.__version__
        except Exception:
            pass
    tally_ok += ok
    print_result('Pillow  (PIL)', ok, ver, err, required=False)

    # pytesseract
    tally_total += 1
    ok, ver, err = check_package('pytesseract', 'pytesseract', '0.3.10')
    tally_ok += ok
    print_result('pytesseract', ok, ver, err, required=False)

    # Tesseract OCR binary (not a pip package)
    print()
    tess_ok, tess_note = check_tesseract_binary()
    sym = f"{GREEN}✓{RESET}" if tess_ok else f"{YELLOW}○{RESET}"
    label = "Tesseract OCR binary"
    if tess_ok:
        print(f"  {sym}  {label:<26}  {DIM}{tess_note}{RESET}")
    else:
        print(f"  {sym}  {label:<26}  {YELLOW}[{tess_note}]{RESET}")

    # ── TIER 6: Utilities ────────────────────────────────────────────────────
    print_tier_header("TIER 6 — Utilities")
    tier6 = [
        ('setuptools',  'setuptools', '65.0'),
        ('packaging',   'packaging',  '21.0'),
    ]
    t6_ok = 0
    for pkg, imp, minv in tier6:
        ok, ver, err = check_package(pkg, imp, minv)
        t6_ok += ok
        print_result(pkg, ok, ver, err, required=False)

    # ── TIER 7: Optional ─────────────────────────────────────────────────────
    print_tier_header("TIER 7 — Optional")
    tier7 = [
        ('cryptography',  'cryptography', '41.0.0'),
        ('python-dotenv', 'dotenv',       '1.0.0'),
    ]
    t7_ok = 0
    for pkg, imp, minv in tier7:
        ok, ver, err = check_package(pkg, imp, minv)
        t7_ok += ok
        print_result(pkg, ok, ver, err, required=False)

    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'═'*60}")
    print(f"  INSTALLATION SUMMARY")
    print(f"{'═'*60}{RESET}\n")

    def status_str(ok, total):
        color = GREEN if ok == total else (YELLOW if ok >= total * 0.8 else RED)
        return f"{color}{ok}/{total}{RESET}"

    core_ok    = t1_ok + t2_ok + t3_ok + t4_ok
    core_total = len(tier1) + len(tier2) + len(tier3) + len(tier4)

    print(f"  Python Version     : {GREEN+'✓ OK'+RESET if python_ok else RED+'✗ UPGRADE REQUIRED'+RESET}")
    print(f"  Tier 1  Excel/Data : {status_str(t1_ok, len(tier1))}")
    print(f"  Tier 2  PDF        : {status_str(t2_ok, len(tier2))}")
    print(f"  Tier 3  Browser    : {status_str(t3_ok, len(tier3))}")
    print(f"  Tier 4  Web/HTTP   : {status_str(t4_ok, len(tier4))}")
    print(f"  Tier 5  Tally      : {status_str(tally_ok, tally_total)}  {DIM}(pyodbc required; others optional){RESET}")
    print(f"  Tier 6  Utilities  : {status_str(t6_ok, len(tier6))}")
    print(f"  Tier 7  Optional   : {status_str(t7_ok, len(tier7))}")
    print(f"  Tesseract binary   : {GREEN+'✓ Found'+RESET if tess_ok else YELLOW+'○ Not found (optional for OCR)'+RESET}")

    # Determine readiness
    if python_ok and core_ok == core_total:
        overall = f"{GREEN}✓ READY FOR USE{RESET}"
        exit_code = 0
    elif python_ok and core_ok >= core_total - 2:
        overall = f"{YELLOW}⚠ MOSTLY READY — some packages missing{RESET}"
        exit_code = 1
    else:
        overall = f"{RED}✗ NOT READY — critical packages missing{RESET}"
        exit_code = 2

    print(f"\n  Overall Status     : {overall}")

    # ── Missing package suggestions ──────────────────────────────────────────
    all_required = (
        [('Tier 1', p, i, v) for p, i, v in tier1] +
        [('Tier 2', p, i, v) for p, i, v in tier2] +
        [('Tier 3', p, i, v) for p, i, v in tier3] +
        [('Tier 4', p, i, v) for p, i, v in tier4]
    )
    missing_core = []
    for tier_label, pkg, imp, minv in all_required:
        ok, _, _ = check_package(pkg, imp, minv)
        if not ok:
            missing_core.append(pkg)

    tally_pip = [
        ('pyodbc',      'pyodbc',      '4.0.0'),
        ('pyautogui',   'pyautogui',   '0.9.54'),
        ('pygetwindow', 'pygetwindow', '0.0.9'),
        ('pywin32',     'win32gui',    None),
        ('Pillow',      'PIL',         '9.0.0'),
        ('pytesseract', 'pytesseract', '0.3.10'),
    ]
    missing_tally = []
    for pkg, imp, minv in tally_pip:
        ok, _, _ = check_package(pkg, imp, minv)
        if not ok:
            missing_tally.append(pkg)

    if missing_core:
        print(f"\n{RED}Missing core packages:{RESET}")
        for pkg in missing_core:
            print(f"    - {pkg}")
        print(f"\n{BLUE}Install core packages:{RESET}")
        print(f"  pip install {' '.join(missing_core)}")

    if missing_tally:
        print(f"\n{YELLOW}Missing Tally packages:{RESET}")
        for pkg in missing_tally:
            print(f"    - {pkg}")
        print(f"\n{BLUE}Install Tally packages:{RESET}")
        print(f"  pip install {' '.join(missing_tally)}")
        if not tess_ok:
            print(f"\n{YELLOW}Tesseract OCR binary not found.{RESET}")
            print(f"  Download: https://github.com/UB-Mannheim/tesseract/wiki")

    if not tess_ok and 'pytesseract' not in missing_tally:
        print(f"\n{YELLOW}pytesseract is installed but Tesseract binary is missing.{RESET}")
        print(f"  Download: https://github.com/UB-Mannheim/tesseract/wiki")
        print(f"  Install to: C:\\Program Files\\Tesseract-OCR\\tesseract.exe")

    print(f"\n{'═'*60}\n")
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
