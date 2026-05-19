"""
Tally GST Extractor - With Sundry Debtors & Sundry Creditors separated
DSN: TallyODBC64_9000
Run: python tally_extract_gst.py

v2.0 CHANGE:
  - Auto-detects if Tally is already running
  - If NOT running → launches Tally.EXE automatically
  - Waits for Tally to fully load before connecting ODBC
  - No need to open Tally manually!
"""

import csv, sys, os, time, subprocess

try:
    import pyodbc
except ImportError:
    print("Run:  pip install pyodbc")
    sys.exit(1)

DSN = "TallyODBC64_9000"

# ── Fallback Tally EXE paths (used only if Registry lookup fails) ─────────────
TALLY_PATHS = [
    r"C:\Program Files\TallyPrime\tally.exe",
    r"C:\Program Files (x86)\TallyPrime\tally.exe",
    r"C:\Program Files\Tally.ERP9\tally.exe",
    r"C:\Program Files (x86)\Tally.ERP9\tally.exe",
    r"C:\Tally\tally.exe",
    r"C:\TallyPrime\tally.exe",
    r"D:\TallyPrime\tally.exe",
    r"D:\Tally\tally.exe",
]

# ── Registry keys where Tally stores its install path ────────────────────────
TALLY_REGISTRY_KEYS = [
    # TallyPrime
    (r"SOFTWARE\Tally Solutions Pvt. Ltd.\TallyPrime",             "ProgramDirectory"),
    (r"SOFTWARE\WOW6432Node\Tally Solutions Pvt. Ltd.\TallyPrime", "ProgramDirectory"),
    # Tally.ERP9
    (r"SOFTWARE\Tally Solutions Pvt. Ltd.\Tally.ERP9",             "ProgramDirectory"),
    (r"SOFTWARE\WOW6432Node\Tally Solutions Pvt. Ltd.\Tally.ERP9", "ProgramDirectory"),
    # Uninstall entries (fallback)
    (r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\TallyPrime",              "InstallLocation"),
    (r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\TallyPrime",  "InstallLocation"),
    (r"SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall\Tally.ERP9",              "InstallLocation"),
    (r"SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall\Tally.ERP9",  "InstallLocation"),
]


def find_tally_exe_from_registry():
    """
    Read Tally's install directory from the Windows Registry.
    This works on any client machine regardless of where Tally is installed.
    Returns the full path to tally.exe, or None if not found.
    """
    try:
        import winreg
    except ImportError:
        return None   # Not on Windows

    for reg_path, value_name in TALLY_REGISTRY_KEYS:
        for hive in (winreg.HKEY_LOCAL_MACHINE, winreg.HKEY_CURRENT_USER):
            try:
                key = winreg.OpenKey(hive, reg_path)
                install_dir, _ = winreg.QueryValueEx(key, value_name)
                winreg.CloseKey(key)
                if install_dir:
                    candidate = os.path.join(install_dir.strip(), "tally.exe")
                    if os.path.exists(candidate):
                        print(f"  \u2713 Found Tally via Registry: {candidate}")
                        return candidate
            except (FileNotFoundError, OSError):
                continue
    return None

# ── How long to wait for Tally to load after launching (seconds) ─────────────
TALLY_LAUNCH_WAIT   = 15   # initial wait after EXE starts
TALLY_CONNECT_RETRY = 10   # how many times to retry ODBC connection
TALLY_RETRY_DELAY   = 3    # seconds between each retry


# ═════════════════════════════════════════════════════════════════════════════
def is_tally_running():
    """Check if any Tally process is currently running (Windows tasklist)."""
    try:
        output = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq tally.exe"],
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW
        ).decode("utf-8", errors="ignore").lower()
        return "tally.exe" in output
    except Exception:
        # If tasklist fails, assume not running and try to connect anyway
        return False


def find_tally_exe():
    """
    Find Tally EXE — tries Registry first (works on any client machine),
    then falls back to the hardcoded TALLY_PATHS list.
    Returns path string or None.
    """
    # 1️⃣  Registry lookup — works regardless of where client installed Tally
    reg_path = find_tally_exe_from_registry()
    if reg_path:
        return reg_path

    # 2️⃣  Fallback: scan common hardcoded paths
    print("  Registry lookup failed — scanning common paths...")
    for path in TALLY_PATHS:
        if os.path.exists(path):
            print(f"  \u2713 Found Tally at: {path}")
            return path
    return None


def launch_tally():
    """
    Launch Tally EXE in background.
    Returns True if launched successfully, False otherwise.
    """
    exe = find_tally_exe()
    if not exe:
        print("  ⚠  Tally EXE not found in standard locations.")
        print("  Please set your Tally path in TALLY_PATHS list at top of script.")
        return False

    print(f"  Launching Tally: {exe}")
    try:
        subprocess.Popen(
            [exe],
            cwd=os.path.dirname(exe),
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
        )
        return True
    except Exception as e:
        print(f"  ✗ Could not launch Tally: {e}")
        return False


def ensure_tally_running():
    """
    Main entry point:
      - If Tally already running → return immediately
      - If not → launch it, then wait for ODBC to become available
    Returns True if Tally is ready, False if all attempts failed.
    """
    if is_tally_running():
        print("  ✓ Tally is already running.\n")
        return True

    print("  Tally is NOT running — launching automatically...")
    launched = launch_tally()
    if not launched:
        return False

    print(f"  Waiting {TALLY_LAUNCH_WAIT}s for Tally to load", end="", flush=True)
    for _ in range(TALLY_LAUNCH_WAIT):
        time.sleep(1)
        print(".", end="", flush=True)
    print(" done.\n")

    return True


def connect():
    """
    Connect to Tally ODBC. Retries up to TALLY_CONNECT_RETRY times
    to handle slow Tally startup.
    """
    for attempt in range(1, TALLY_CONNECT_RETRY + 1):
        try:
            conn = pyodbc.connect(f"DSN={DSN}", autocommit=True, timeout=10)
            if attempt > 1:
                print(f"  ✓ Connected on attempt {attempt}.\n")
            return conn
        except pyodbc.Error as e:
            if attempt < TALLY_CONNECT_RETRY:
                print(f"  Attempt {attempt}/{TALLY_CONNECT_RETRY} failed — retrying in {TALLY_RETRY_DELAY}s... ({e})")
                time.sleep(TALLY_RETRY_DELAY)
            else:
                print(f"\n  ✗ Connection failed after {TALLY_CONNECT_RETRY} attempts: {e}")
                print("  Make sure Tally is fully open and a company is loaded.")
    return None


def query(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return cols, [list(r) for r in cur.fetchall()]
    except pyodbc.Error as e:
        print(f"  Query error: {e}")
        return [], []


def clean(val):
    return (val or "").strip()


def save_csv(data, filename, headers):
    if not data:
        return
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(data)
    print(f"  Saved --> {filename}")


def print_table(rows, title):
    if not rows:
        print(f"  No {title} with GSTIN found.\n")
        return
    print(f"  {title} ({len(rows)} entries)\n")
    print(f"  {'Ledger Name':<42} {'GSTIN':<20} Parent Group")
    print("  " + "-" * 90)
    for r in rows:
        print(f"  {r[0]:<42} {r[2]:<20} {r[1]}")
    print()


# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("   Tally GST Extractor  v2.0  |  DSN: TallyODBC64_9000")
    print("=" * 60)
    print()

    # ── Step 0: Make sure Tally is running ───────────────────
    print("[0] Checking Tally status...\n")
    if not ensure_tally_running():
        print("\n  Could not start Tally. Please open Tally manually and re-run.")
        sys.exit(1)

    # ── Step 1: Connect ──────────────────────────────────────
    print("[1] Connecting to Tally ODBC...\n")
    conn = connect()
    if not conn:
        input("\nPress Enter to exit...")
        sys.exit(1)
    print("  Connected!\n")

    # ── Step 2: Company Details ──────────────────────────────
    print("[2] Company Details\n")
    _, rows = query(conn,
        "SELECT [$Name], [$GSTRegistrationNumber], [$StateName] FROM Company")
    company_out = []
    for row in rows:
        name  = clean(row[0])
        gstin = clean(row[1]) if len(row) > 1 else ""
        state = clean(row[2]) if len(row) > 2 else ""
        if name:
            print(f"  Company : {name}")
            print(f"  GSTIN   : {gstin or 'Not Set'}")
            print(f"  State   : {state}\n")
            company_out.append([name, gstin, state])
    save_csv(company_out, "my_company_gst.csv", ["Company Name", "GSTIN", "State"])

    # ── Step 3: All Ledgers ──────────────────────────────────
    print("[3] Ledger GSTIN Summary\n")
    _, rows = query(conn,
        "SELECT [$Name], [$Parent], [$PartyGSTIN] FROM Ledger")

    all_ledgers    = []
    debtors        = []   # Sundry Debtors
    creditors      = []   # Sundry Creditors
    other_with_gst = []   # Other groups with GSTIN

    for row in rows:
        name   = clean(row[0])
        parent = clean(row[1]) if len(row) > 1 else ""
        gstin  = clean(row[2]) if len(row) > 2 else ""
        if not name:
            continue
        entry = [name, parent, gstin]
        all_ledgers.append(entry)

        if gstin:
            p = parent.lower()
            if "sundry debtor" in p:
                debtors.append(entry)
            elif "sundry creditor" in p:
                creditors.append(entry)
            else:
                other_with_gst.append(entry)

    total_with = len(debtors) + len(creditors) + len(other_with_gst)
    print(f"  Total Ledgers     : {len(all_ledgers)}")
    print(f"  With GSTIN        : {total_with}")
    print(f"    Sundry Debtors  : {len(debtors)}")
    print(f"    Sundry Creditors: {len(creditors)}")
    print(f"    Others          : {len(other_with_gst)}")
    print(f"  Without GSTIN     : {len(all_ledgers) - total_with}\n")

    # Print tables
    print_table(creditors,     "Sundry Creditors with GSTIN")
    print_table(debtors,       "Sundry Debtors with GSTIN")
    if other_with_gst:
        print_table(other_with_gst, "Other Ledgers with GSTIN")

    # Save CSVs
    save_csv(debtors,       "sundry_debtors_gstin.csv",
             ["Ledger Name", "Parent Group", "GSTIN"])
    save_csv(creditors,     "sundry_creditors_gstin.csv",
             ["Ledger Name", "Parent Group", "GSTIN"])
    save_csv(all_ledgers,   "all_ledgers.csv",
             ["Ledger Name", "Parent Group", "GSTIN"])

    all_with_gst = debtors + creditors + other_with_gst
    save_csv(all_with_gst,  "all_parties_with_gstin.csv",
             ["Ledger Name", "Parent Group", "GSTIN"])

    conn.close()
    print("\n  Done.")
    input("\nPress Enter to exit...")
