"""
Tally GST Extractor  v3.15
==========================================================================
FIX 9 (v3.15) — _focus_tally_window crash fix (PyGetWindowException error 0).
  pygetwindow on Python 3.13 + Windows 11 raises PyGetWindowException
  ("Error code 0 — The operation completed successfully") from w.activate().
  The v3.14 code caught the first raise but then called w.activate() AGAIN
  inside the except block — that second raise was uncaught and crashed the
  entire script with a traceback at line 907/911.
  Fix: every activate() call is individually wrapped in its own try/except.
  Three fully-isolated fallback methods: pygetwindow → win32gui → ctypes.
  If Tally window is found but focus fails (Windows anti-focus-steal policy),
  the script now prints a warning and continues — keyboard automation still
  works because Tally is already the active window on screen.

FIX 8 (v3.13) — Remove ALL OCR/mouse from Select Company navigation.
  OCR was clicking (485,824) which is OUTSIDE the dialog on this screen.
  New approach: Escape → Alt+K → S — pure keyboard, works on any screen
  size/scaling. No OCR needed to reach Select Company dialog.

FIX 7 (v3.12) — Remove mouse click (partial fix).
  Screenshots confirmed: clicking a row moves highlight but does NOT load
  the company. Fix: keyboard-only — Down arrow (moves focus into list) then
  Enter (loads highlighted company). No OCR/click needed at all.
  Also: suppress noisy 'Connected via...' spam during ODBC poll loop.

FIX 6 (v3.11) — Down+Enter approach (partially correct).

FIX 5 (v3.10) — UI fallback when disk scan can't read company name:
  If no confident match found, automatically opens F3 → Select Company,
  types the client name for Tally to filter, presses Enter to load it.
  Also: improved name+GSTIN matching (PAN prefix, substring boost).

FIX 4 (v3.9) — Root index + always-run disk scan.

FIXES in v3.7-v3.8:

  FIX 1 — Excel header auto-detection:
    Instead of assuming header is row 2, scans ALL rows looking for a row
    that contains "client name" or "gstin". Works for any layout.

  FIX 2 — Company names from Tally binary files:
    The XML API returns nothing useful for unloaded companies. Instead,
    we read the real company name directly from Tally's binary data files.
    Tally Prime stores company name as plain text inside Company.900.
    We scan for the first long uppercase string that looks like a name.

  FIX 3 — Company number from folder name (like Tally's own display):
    Image shows D:\\Data\\010076 → ELANTHALIR NURSERY GARDEN (010076)
    We match folder number to company name found in binary.

Run:   python tally_extract_gst.py
Needs: pip install pyodbc openpyxl
"""

import csv, sys, os, time, subprocess, glob, re, struct
from pathlib import Path
from urllib.request import urlopen, Request

try:
    import pyodbc
except ImportError:
    print("Run:  pip install pyodbc"); sys.exit(1)

try:
    import openpyxl
except ImportError:
    print("Run:  pip install openpyxl"); sys.exit(1)

# ── constants ─────────────────────────────────────────────────────────────────
TALLY_LAUNCH_WAIT   = 25
TALLY_CONNECT_RETRY = 12
TALLY_RETRY_DELAY   = 5
TALLY_ODBC_PORTS    = [9000, 9001, 9002, 9003]
TALLY_XML_PORTS     = [9002, 9000, 9001, 9003]

CLIENT_EXCEL_NAMES = [
    "Client_Manager_Secure_AY2027-28.xlsx",
    "Client_Manager_Secure_AY2026-27.xlsx",
    "Client_Manager_Secure_AY2025-26.xlsx",
    "clients_manager.xlsx",
    "clients.xlsx",
]
CLIENT_SHEET = "\U0001f510 Client Credentials"   # 🔐 Client Credentials


# ═════════════════════════════════════════════════════════════════════════════
#  UTILITIES
# ═════════════════════════════════════════════════════════════════════════════

def get_drives():
    return [f"{l}:\\" for l in "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            if os.path.exists(f"{l}:\\")]

def _cv(val):
    return str(val).strip() if val is not None else ""

def _norm(s):
    return re.sub(r"\s+", " ",
                  re.sub(r"[^A-Z0-9 ]", " ", s.upper())).strip()

def safe_filename(name):
    return "".join(c if c.isalnum() or c in " _-" else "_"
                   for c in name).strip()


# ═════════════════════════════════════════════════════════════════════════════
#  FIX 1 — EXCEL: AUTO-DETECT HEADER ROW
# ═════════════════════════════════════════════════════════════════════════════

def find_excel_file():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    cwd        = os.getcwd()
    home       = os.path.expanduser("~")

    search_dirs = [script_dir, cwd,
                   os.path.join(home, "Downloads"),
                   os.path.join(home, "Desktop"),
                   os.path.join(home, "Documents")]

    # Add parent folders up to 5 levels
    p = Path(script_dir)
    for _ in range(5):
        p = p.parent
        search_dirs.append(str(p))
        for sub in ["Downloads", "Desktop", "Documents"]:
            search_dirs.append(str(p / sub))

    seen = set()
    for d in search_dirs:
        d = os.path.normpath(d)
        if d in seen or not os.path.isdir(d):
            continue
        seen.add(d)
        for fname in CLIENT_EXCEL_NAMES:
            fp = os.path.join(d, fname)
            if os.path.exists(fp):
                return fp
    return None

def load_clients_from_excel(excel_path):
    """
    FIX 1: Auto-detects header row by scanning for a row containing
    'client name' or 'gstin' in any cell. Works for any row offset.
    """
    try:
        wb = openpyxl.load_workbook(excel_path, read_only=True, data_only=True)

        # Find the right sheet
        sheet = None
        if CLIENT_SHEET in wb.sheetnames:
            sheet = wb[CLIENT_SHEET]
        else:
            for s in wb.sheetnames:
                if "credential" in s.lower() or "client" in s.lower():
                    sheet = wb[s]
                    break
            if sheet is None and wb.sheetnames:
                sheet = wb[wb.sheetnames[0]]

        if sheet is None:
            print(f"  No usable sheet found. Sheets: {wb.sheetnames}")
            wb.close(); return []

        all_rows = list(sheet.iter_rows(values_only=True))
        wb.close()

        # ── Auto-detect header row ────────────────────────────────────────────
        # Scan every row; first row where any cell contains "client name" OR
        # the row has BOTH "gstin" and at least one name-like cell = header
        header_idx = None
        for row_idx, row in enumerate(all_rows):
            cells = [_cv(c).lower().strip() for c in row]
            has_client_name = any("client name" in c for c in cells)
            has_gstin       = any("gstin" in c for c in cells)
            if has_client_name and has_gstin:
                header_idx = row_idx
                break
            if has_client_name:
                header_idx = row_idx
                break

        if header_idx is None:
            print("  Could not find header row with 'Client Name' column.")
            print(f"  First 5 rows seen:")
            for r in all_rows[:5]:
                print(f"    {[_cv(c) for c in r]}")
            return []

        print(f"  Header found at row {header_idx + 1}")
        hdr = all_rows[header_idx]

        # Build column map
        col_map = {}
        for idx, cell in enumerate(hdr):
            cl = _cv(cell).lower().replace("\n", "").strip()
            if not cl: continue
            if   "client name" in cl: col_map["name"]     = idx
            elif "gstin"       in cl: col_map["gstin"]    = idx
            elif "entity"      in cl: col_map["entity"]   = idx
            elif "username"    in cl: col_map["username"] = idx
            elif "password"    in cl:
                is_gst = "gst" in cl or not cl.startswith("it")
                if "password" not in col_map or is_gst:
                    col_map["password"] = idx
            elif "active"      in cl: col_map["active"]   = idx

        if "name" not in col_map:
            print(f"  'Client Name' column not found in header row.")
            print(f"  Header cells: {[_cv(c) for c in hdr]}")
            return []

        def _cell(row, key):
            i = col_map.get(key)
            return _cv(row[i]) if (i is not None and i < len(row)) else ""

        clients = []
        for row in all_rows[header_idx + 1:]:
            name   = _cell(row, "name")
            active = _cell(row, "active").upper() or "YES"
            if not name or "sample" in name.lower(): continue
            if active == "NO": continue
            clients.append({
                "name":     name,
                "gstin":    _cell(row, "gstin"),
                "entity":   _cell(row, "entity"),
                "username": _cell(row, "username"),
                "password": _cell(row, "password"),
            })
        return clients

    except Exception as e:
        print(f"  Excel read error: {e}")
        return []

def pick_excel_client(clients):
    if not clients: return None
    if len(clients) == 1:
        print(f"  Single client: {clients[0]['name']}")
        return clients[0]
    print(f"\n  {len(clients)} clients in Excel:\n")
    print(f"  {'No.':<5} {'Client Name':<45} GSTIN")
    print("  " + "-" * 72)
    for i, c in enumerate(clients, 1):
        print(f"  {i:<5} {c['name']:<45} {c['gstin'] or '(none)'}")
    print()
    while True:
        try:
            pick = input(f"  Which client to extract? (1-{len(clients)}): ").strip()
            idx  = int(pick) - 1
            if 0 <= idx < len(clients):
                return clients[idx]
            print(f"  Enter 1 to {len(clients)}.")
        except ValueError:
            print("  Enter a number.")


# ═════════════════════════════════════════════════════════════════════════════
#  FIX 2 — READ REAL COMPANY NAME FROM TALLY BINARY (Company.900)
# ═════════════════════════════════════════════════════════════════════════════

# Words that appear in Tally binary but are NOT company names
_BINARY_NOISE = {
    "TALLY", "TALLYPRIME", "TALLYERP", "SOLUTIONS", "SOFTWARE",
    "VERSION", "RELEASE", "BUILD", "SERIES", "WINDOWS", "UNICODE",
    "SILVER", "GOLD", "AUDIT", "EDUCATIONAL", "LICENSE", "LICENSED",
    "COPYRIGHT", "ACCOUNT", "ACCOUNTS", "DEFAULT", "COMPANY",
    "MANAGER", "MASTERS", "VOUCHER", "LEDGER", "GROUP", "STOCK",
    "INDIA", "INDIAN", "ENGLISH", "NATIONAL", "PRIMARY",
    "SUNDRY", "DEBTOR", "CREDITOR", "CAPITAL", "PROFIT", "LOSS",
    "BALANCE", "SHEET", "PURCHASE", "SALES", "CASH", "BANK",
    "DUTIES", "TAXES", "DIRECT", "INDIRECT", "EXPENSES", "INCOME",
    "SUSPENSE", "CLOSING", "OPENING", "MISCELLANEOUS",
}

def _extract_strings(data, min_len=6, max_len=100):
    """
    Extract all printable ASCII runs of length min_len..max_len from binary data.
    """
    result = []
    current = []
    for b in data:
        if 32 <= b <= 126:
            current.append(chr(b))
        else:
            if min_len <= len(current) <= max_len:
                result.append("".join(current).strip())
            current = []
    if min_len <= len(current) <= max_len:
        result.append("".join(current).strip())
    return result

def _looks_like_company_name(s):
    """
    Heuristic: a real company name is uppercase, 6-80 chars,
    contains letters, not a known noise word, not a file path,
    not all digits, not a version string.
    """
    s = s.strip()
    if len(s) < 6 or len(s) > 80: return False
    if not re.search(r"[A-Za-z]", s): return False        # must have letters
    if re.match(r"^[\d\s.\-/\\:]+$", s): return False     # not digits/path
    if "\\" in s or "/" in s or ":" in s: return False    # not a path
    if re.match(r"^\d+\.\d+", s): return False            # not version like 6.7.1
    upper = s.upper()
    if upper in _BINARY_NOISE: return False
    # Must have at least 2 letters (not just initials or abbreviations)
    if len(re.findall(r"[A-Za-z]", s)) < 4: return False
    return True

def read_company_name_from_folder(folder_path):
    """
    Try to read the company name from a Tally data folder.
    Reads ALL .900 files with multiple strategies.
    Returns company name string or None.
    """
    folder = Path(folder_path)

    # All .900 files — try every one
    candidates = (
        ["Company.900", "company.900",
         "Manager.900", "manager.900",
         "Cmp.900",     "cmp.900",
         "CompanyInfo.900"]
        + [str(p.name) for p in sorted(folder.glob("*.900"))]
    )
    # Deduplicate preserving order
    seen_c = set()
    candidates = [c for c in candidates
                  if not (c in seen_c or seen_c.add(c))]

    all_candidates = []  # collect across files

    for fname in candidates:
        fp = folder / fname
        if not fp.exists():
            continue
        try:
            with open(str(fp), "rb") as f:
                raw = f.read(65536)   # read first 64 KB (was 8 KB)

            # Strategy 1: scan printable ASCII runs
            strings = _extract_strings(raw, min_len=5, max_len=80)
            for s in strings:
                if _looks_like_company_name(s):
                    all_candidates.append(s.upper().strip())

            # Strategy 2: look for UTF-16LE encoded names (TallyPrime stores
            # some strings in UTF-16LE — every other byte is 0x00)
            try:
                text16 = raw.decode("utf-16-le", errors="ignore")
                strings16 = _extract_strings(
                    text16.encode("ascii", errors="ignore"), min_len=5, max_len=80)
                for s in strings16:
                    if _looks_like_company_name(s):
                        all_candidates.append(s.upper().strip())
            except Exception:
                pass

        except Exception:
            continue

    # Return the longest plausible candidate (avoids short noise like "INDIA")
    if all_candidates:
        # Sort by length descending, prefer longer real names
        all_candidates.sort(key=lambda x: -len(x))
        return all_candidates[0]

    return None

def scan_tally_data_folder(data_path):
    """
    Walk data_path sub-folders. For each, try to read company name from binary.
    Returns list of dicts: {name, number, folder, gstin, state, source}
    """
    companies = []
    seen_names = set()

    try:
        sub_dirs = sorted(Path(data_path).iterdir())
    except Exception:
        return []

    COMPANY_FILES = ["Company.900","company.900",
                     "Manager.900","manager.900",
                     "Cmp.900","cmp.900"]

    for sub in sub_dirs:
        if not sub.is_dir(): continue

        # Check it's a Tally company folder (has .900 files)
        has_900 = any((sub / f).exists() for f in COMPANY_FILES)
        if not has_900:
            continue

        number = sub.name   # e.g. "010076"
        name   = read_company_name_from_folder(str(sub))

        if not name:
            # Fallback: use the folder number as name placeholder
            # We'll try to enrich it from ODBC later
            name = f"[Company #{number}]"

        key = name.upper()
        if key in seen_names:
            continue
        seen_names.add(key)

        companies.append({
            "name":   name,
            "number": number,
            "folder": str(sub),
            "gstin":  "",
            "state":  "",
            "source": "disk",
        })

    return companies


# ═════════════════════════════════════════════════════════════════════════════
#  FIND TALLY DATA PATH (from Tally.ini)
# ═════════════════════════════════════════════════════════════════════════════

def find_tally_data_paths():
    paths = []
    for drive in get_drives():
        for root in [os.path.join(drive, "Program Files"),
                     os.path.join(drive, "Program Files (x86)"),
                     drive]:
            if not os.path.exists(root): continue
            for ini in (glob.glob(os.path.join(root, "*", "tally.ini")) +
                        glob.glob(os.path.join(root, "*", "Tally.ini"))):
                try:
                    with open(ini, "r", encoding="utf-8", errors="ignore") as f:
                        for line in f:
                            line = line.strip()
                            if line.lower().startswith("data"):
                                parts = line.split("=", 1)
                                if len(parts) == 2:
                                    dp = parts[1].strip().strip('"').strip("'")
                                    if os.path.isdir(dp) and dp not in paths:
                                        paths.append(dp)
                except Exception:
                    pass
    # Common fallbacks
    for drive in get_drives():
        for p in ["Data", "TallyData", r"Tally\Data", r"TallyPrime\Data"]:
            full = os.path.join(drive, p)
            if os.path.isdir(full) and full not in paths:
                paths.append(full)
    return paths


# ═════════════════════════════════════════════════════════════════════════════
#  ROOT INDEX READER — reads D:\Data\Manager.900 for ALL company names
# ═════════════════════════════════════════════════════════════════════════════

def _read_root_index(data_root):
    """
    Tally Prime keeps a MASTER INDEX of every company at the DATA ROOT level
    (e.g. D:\\Data\\Manager.900 or D:\\Data\\CompanyIndex.900).
    This index has lines like:
        010076<TAB>ELANTHALIR NURSERY GARDEN<TAB>33AAHFE3141K1ZN<TAB>Tamil Nadu
    or similar delimited / packed binary structure.

    Strategy:
      1. Try to parse as a text file (UTF-8 / Latin-1) — some Tally builds
         write this as a readable TSV/CSV at the root.
      2. Fall back to the existing binary-string extractor used for sub-folders,
         which pulls long readable strings out of binary blobs.
      3. Also scan the root folder's own .900 files that are NOT inside
         numbered sub-folders (i.e., the root-level index files).

    Returns list of dicts: {name, number, folder, gstin, state, source}
    """
    root = Path(data_root)
    companies = []
    seen = set()

    # ── Candidate index files at ROOT level ──────────────────────────────────
    index_names = [
        "Manager.900", "manager.900",
        "CompanyIndex.900", "companyindex.900",
        "TallyIndex.900", "tallyindex.900",
        "CmpIndex.900",
    ]
    # Also pick up any .900 file sitting directly in the root (not in a subfolder)
    try:
        root_900s = [str(p.name) for p in root.glob("*.900") if p.is_file()]
        index_names = list(dict.fromkeys(index_names + root_900s))
    except Exception:
        pass

    def _add(name, number="", gstin="", state=""):
        name = name.strip()
        if not name or len(name) < 4:
            return
        key = name.upper()
        if key in seen:
            return
        seen.add(key)
        companies.append({
            "name":   name,
            "number": number,
            "folder": str(root / number) if number else str(root),
            "gstin":  gstin,
            "state":  state,
            "source": "root_index",
        })

    for fname in index_names:
        fp = root / fname
        if not fp.exists():
            continue

        # ── Strategy A: try text parse ────────────────────────────────────
        try:
            for enc in ("utf-8", "latin-1", "utf-16-le"):
                try:
                    text = fp.read_text(encoding=enc, errors="ignore")
                    lines = text.splitlines()
                    parsed_any = False
                    for line in lines:
                        # Tab-separated: number \t name \t gstin \t state
                        parts = [p.strip() for p in re.split(r"[\t|,]", line)]
                        if len(parts) >= 2:
                            # Identify which part looks like a company number
                            # (6–8 digit string) and which is the name
                            num = ""
                            name_part = ""
                            gstin_part = ""
                            state_part = ""
                            for i, p in enumerate(parts):
                                if re.match(r"^\d{5,8}$", p):
                                    num = p
                                elif re.match(r"^[0-9A-Z]{15}$", p.upper()):
                                    gstin_part = p
                                elif len(p) >= 6 and re.search(r"[A-Za-z]", p):
                                    if not name_part:
                                        name_part = p
                                    elif not state_part:
                                        state_part = p
                            if name_part and _looks_like_company_name(name_part):
                                _add(name_part.upper(), num, gstin_part, state_part)
                                parsed_any = True
                    if parsed_any:
                        break   # good encoding found, stop trying others
                except UnicodeDecodeError:
                    continue
        except Exception:
            pass

        # ── Strategy B: binary string extractor (same as sub-folder scan) ──
        try:
            raw = fp.read_bytes()[:131072]   # first 128 KB
            strings = _extract_strings(raw, min_len=6, max_len=80)
            for s in strings:
                if _looks_like_company_name(s):
                    _add(s.upper())
            # Also try UTF-16-LE decode
            try:
                text16 = raw.decode("utf-16-le", errors="ignore")
                strings16 = _extract_strings(
                    text16.encode("ascii", errors="ignore"), min_len=6, max_len=80)
                for s in strings16:
                    if _looks_like_company_name(s):
                        _add(s.upper())
            except Exception:
                pass
        except Exception:
            pass

    return companies


# ═════════════════════════════════════════════════════════════════════════════
#  TALLY LAUNCH / ODBC
# ═════════════════════════════════════════════════════════════════════════════

def find_tally_exe():
    for drive in get_drives():
        for root in [os.path.join(drive, "Program Files"),
                     os.path.join(drive, "Program Files (x86)"),
                     drive]:
            if not os.path.exists(root): continue
            for m in glob.glob(os.path.join(root, "*", "tally.exe")):
                return m
    return None

def is_tally_running():
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq tally.exe"],
            stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NO_WINDOW
        ).decode("utf-8", errors="ignore").lower()
        return "tally.exe" in out
    except Exception:
        return False

def ensure_tally_running():
    if is_tally_running():
        print("  Tally is already running.\n"); return True
    exe = find_tally_exe()
    if not exe:
        print("  tally.exe not found. Please open Tally manually.")
        return False
    print(f"  Launching: {exe}")
    try:
        subprocess.Popen([exe], cwd=os.path.dirname(exe),
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP)
    except Exception as e:
        print(f"  Launch failed: {e}"); return False
    print(f"  Waiting {TALLY_LAUNCH_WAIT}s", end="", flush=True)
    for _ in range(TALLY_LAUNCH_WAIT):
        time.sleep(1); print(".", end="", flush=True)
    print(" done.\n")
    return True

def _find_registry_dsn():
    try:
        import winreg
    except ImportError:
        return None
    for hive, path in [
        (winreg.HKEY_CURRENT_USER,
         r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"),
        (winreg.HKEY_LOCAL_MACHINE,
         r"SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources"),
        (winreg.HKEY_LOCAL_MACHINE,
         r"SOFTWARE\WOW6432Node\ODBC\ODBC.INI\ODBC Data Sources"),
    ]:
        try:
            key = winreg.OpenKey(hive, path)
            i = 0
            while True:
                try:
                    name, value, _ = winreg.EnumValue(key, i)
                    if any(kw in name.lower() or kw in (value or "").lower()
                           for kw in ["tally","odbc64","odbc32"]):
                        winreg.CloseKey(key); return name
                    i += 1
                except OSError: break
            winreg.CloseKey(key)
        except (FileNotFoundError, OSError): continue
    return None

def connect_to_tally(retries=TALLY_CONNECT_RETRY, silent=False):
    conn_strings = []
    dsn = _find_registry_dsn()
    if dsn:
        conn_strings.append((f"DSN={dsn}", f"registry DSN '{dsn}'"))
    for d in ["TallyODBC64_9000","TallyODBC64_9001",
               "TallyODBC32_9000","TallyODBC_9000"]:
        conn_strings.append((f"DSN={d}", f"DSN '{d}'"))
    for port in TALLY_ODBC_PORTS:
        conn_strings.append((
            f"DRIVER={{Tally ODBC}};Host=localhost;Port={port}",
            f"port {port}"))

    for attempt in range(1, retries + 1):
        for cs, label in conn_strings:
            try:
                conn = pyodbc.connect(cs, autocommit=True, timeout=5)
                if not silent:
                    print(f"  Connected via {label}")
                return conn
            except pyodbc.Error:
                pass
        if attempt < retries:
            print(f"  Retry {attempt}/{retries} in {TALLY_RETRY_DELAY}s...")
            time.sleep(TALLY_RETRY_DELAY)
    return None

def odbc_query(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return [list(r) for r in cur.fetchall()]
    except pyodbc.Error as e:
        print(f"  ODBC error: {e}"); return []

def get_odbc_companies(conn):
    rows = odbc_query(conn,
        "SELECT [$Name], [$GSTRegistrationNumber], [$StateName] FROM Company")
    out = []
    for r in rows:
        name  = (r[0] or "").strip()
        gstin = (r[1] or "").strip() if len(r) > 1 else ""
        state = (r[2] or "").strip() if len(r) > 2 else ""
        if name:
            out.append({"name": name, "gstin": gstin, "state": state,
                        "source": "odbc"})
    return out


# ═════════════════════════════════════════════════════════════════════════════
#  TALLY XML — OPEN COMPANY
# ═════════════════════════════════════════════════════════════════════════════

def _find_xml_port():
    ping = (b"<ENVELOPE><HEADER><VERSION>1</VERSION>"
            b"<TALLYREQUEST>Export</TALLYREQUEST>"
            b"<TYPE>Data</TYPE><ID>List of Companies</ID>"
            b"</HEADER></ENVELOPE>")
    for port in TALLY_XML_PORTS:
        try:
            req = Request(f"http://localhost:{port}", data=ping,
                          headers={"Content-Type": "application/xml"})
            with urlopen(req, timeout=3):
                return port
        except Exception:
            pass
    return None

def open_company_xml(company_name, xml_port):
    if xml_port is None:
        return False, "XML port not available"
    xml = (
        "<ENVELOPE>"
        "<HEADER><VERSION>1</VERSION>"
        "<TALLYREQUEST>Export</TALLYREQUEST>"
        "<TYPE>Data</TYPE>"
        "<ID>List of Accounts</ID></HEADER>"
        "<BODY><EXPORTDATA><REQUESTDESC>"
        "<REPORTNAME>List of Accounts</REPORTNAME>"
        "<STATICVARIABLES>"
        "<SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>"
        f"<SVCURRENTCOMPANY>{company_name}</SVCURRENTCOMPANY>"
        "</STATICVARIABLES>"
        "</REQUESTDESC></EXPORTDATA></BODY>"
        "</ENVELOPE>"
    ).encode("utf-8")
    try:
        req = Request(f"http://localhost:{xml_port}", data=xml,
                      headers={"Content-Type": "application/xml"})
        with urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8", errors="ignore").upper()
            if "LINEERROR" in body:
                return False, "Tally LINEERROR"
            return True, "OK"
    except Exception as e:
        return False, str(e)


def _tally_xml_post(xml_port, xml_body, timeout=10):
    """POST to Tally XML and return response body string, or None on error."""
    try:
        data = xml_body.encode("utf-8") if isinstance(xml_body, str) else xml_body
        req  = Request(f"http://localhost:{xml_port}", data=data,
                       headers={"Content-Type": "application/xml"})
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        return None


def get_all_companies_xml(xml_port):
    """
    Fetch ALL companies from Tally via XML HTTP.
    Tries multiple request formats used by different Tally Prime versions.
    Returns list of dicts: {name, number, gstin, state, source}
    """
    if xml_port is None:
        return []

    # ── Multiple XML request formats for different Tally versions ────────────
    requests_to_try = [
        # Format 1: TallyPrime 2.x / 3.x — Collection request
        """<ENVELOPE>
<HEADER><TALLYREQUEST>Export</TALLYREQUEST></HEADER>
<BODY><EXPORTDATA><REQUESTDESC>
<REPORTNAME>List of Companies</REPORTNAME>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
</REQUESTDESC></EXPORTDATA></BODY>
</ENVELOPE>""",
        # Format 2: Collection of company objects
        """<ENVELOPE>
<HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>
<TYPE>Collection</TYPE><ID>List of Companies</ID></HEADER>
<BODY><EXPORTDATA><REQUESTDESC>
<REPORTNAME>List of Companies</REPORTNAME>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
</REQUESTDESC></EXPORTDATA></BODY>
</ENVELOPE>""",
        # Format 3: Direct collection query
        """<ENVELOPE>
<HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>
<TYPE>Collection</TYPE><ID>Companies</ID></HEADER>
<BODY><EXPORTDATA><REQUESTDESC>
<STATICVARIABLES><SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT></STATICVARIABLES>
<REQUESTDATA>
<TALLYREQUESTDATA><FORMULATYPE>Object</FORMULATYPE>
<TDLMESSAGE><COLLECTION NAME="Companies" ISMODIFY="No">
<TYPE>Company</TYPE>
<NATIVEMETHOD>Name</NATIVEMETHOD>
<NATIVEMETHOD>CompanyNumber</NATIVEMETHOD>
<NATIVEMETHOD>GSTRegistrationNumber</NATIVEMETHOD>
<NATIVEMETHOD>StateName</NATIVEMETHOD>
</COLLECTION></TDLMESSAGE>
</TALLYREQUESTDATA>
</REQUESTDATA></REQUESTDESC></EXPORTDATA></BODY>
</ENVELOPE>""",
        # Format 4: Simplest ping — just get whatever Tally returns
        """<ENVELOPE>
<HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>
<TYPE>Data</TYPE><ID>List of Companies</ID></HEADER>
</ENVELOPE>""",
    ]

    body = None
    for i, xml_req in enumerate(requests_to_try, 1):
        body = _tally_xml_post(xml_port, xml_req)
        if body and len(body.strip()) > 50:
            # Got a real response — check it has company-like content
            up = body.upper()
            if any(tag in up for tag in ["<COMPANY", "<NAME>", "COMPANYNAME",
                                          "ENTERPRISES", "TRADERS", "PANCHAYAT"]):
                print(f"  XML format {i} succeeded ({len(body)} chars)")
                break
            # Save last non-empty response anyway
    else:
        # No format worked — dump first 300 chars for diagnosis
        if body:
            print(f"  XML returned data but no company names found.")
            print(f"  Response sample: {body[:300]}")
        return []

    companies = []
    seen      = set()

    # ── Parse strategy 1: <COMPANY> blocks ───────────────────────────────────
    for block in re.findall(r"<COMPANY[^>]*>(.*?)</COMPANY>",
                             body, re.DOTALL | re.IGNORECASE):
        name  = re.search(r"<NAME[^>]*>([^<]+)</NAME>",   block, re.IGNORECASE)
        num   = re.search(r"<COMPANYNUMBER[^>]*>([^<]+)", block, re.IGNORECASE)
        gstin = re.search(r"<GSTREGISTRATIONNUMBER[^>]*>([^<]+)", block, re.IGNORECASE)
        state = re.search(r"<STATENAME[^>]*>([^<]+)",     block, re.IGNORECASE)
        name  = name.group(1).strip()  if name  else ""
        if not name or name.upper() in seen: continue
        seen.add(name.upper())
        companies.append({
            "name":   name,
            "number": num.group(1).strip()   if num   else "",
            "gstin":  gstin.group(1).strip() if gstin else "",
            "state":  state.group(1).strip() if state else "",
            "source": "xml",
        })

    # ── Parse strategy 2: flat <NAME> tags (Tally Prime 2.x) ─────────────────
    if not companies:
        for name in re.findall(r"<NAME>([^<]{3,80})</NAME>", body, re.IGNORECASE):
            name = name.strip()
            if not name or name.upper() in seen: continue
            seen.add(name.upper())
            companies.append({"name": name, "number": "", "gstin": "",
                               "state": "", "source": "xml"})

    # ── Parse strategy 3: COMPANYNAME attribute / tag ────────────────────────
    if not companies:
        pat = r'COMPANYNAME[\s=>"\']+([^<"&\']{3,80})'
        for name in re.findall(pat, body, re.IGNORECASE):
            name = name.strip().strip('"').strip("'")
            if not name or name.upper() in seen: continue
            seen.add(name.upper())
            companies.append({"name": name, "number": "", "gstin": "",
                               "state": "", "source": "xml"})

    return companies


# ═════════════════════════════════════════════════════════════════════════════
#  TALLY UI SCREEN AUTOMATION — F3 Company Select
# ═════════════════════════════════════════════════════════════════════════════
#
#  pip install pyautogui pygetwindow pytesseract Pillow
#  Also install Tesseract OCR binary:
#    https://github.com/UB-Mannheim/tesseract/wiki  (Windows installer)
#    Default path: C:\Program Files\Tesseract-OCR\tesseract.exe
#
# ─────────────────────────────────────────────────────────────────────────────

def _tally_search_keyword(name):
    """
    Derive the shortest meaningful search string to type in Tally's
    Select Company search box.

    Examples:
      'ELANTHALIR NURSERY GARDEN'          → 'ELAN'
      'ARAMBAKKAM VILLAGE PANCHAYAT PRES'  → 'ARAM'
      'ARUN ENTERPRISES'                   → 'ARUN'
    """
    SKIP = {
        "AND","THE","FOR","OF","A","AN","AT","IN","LTD","PVT","CO",
        "PRIVATE","LIMITED","ENTERPRISES","ENTERPRISE","TRADERS","TRADER",
        "AGENCY","AGENCIES","SERVICES","SERVICE","INDUSTRIES","INDUSTRY",
        "COMPANY","VILLAGE","PANCHAYAT","PRESIDENT","ASSOCIATION",
    }
    words = [w for w in re.sub(r"[^A-Z0-9 ]", " ", name.upper()).split()
             if len(w) >= 3 and w not in SKIP]
    if words:
        return words[0][:5]          # first 5 chars of first meaningful word
    return name[:4].upper()


def _focus_tally_window():
    """
    Bring Tally window to foreground.

    Three independent methods tried in order — any success returns True.
    Each method is fully isolated in its own try/except so a failure in
    one never prevents the others from running.

    Fix v3.15: pygetwindow on Python 3.13 + Windows 11 raises
    PyGetWindowException("Error code 0 — The operation completed
    successfully") from w.activate().  The original code caught the first
    raise but then called w.activate() AGAIN in the except block, which
    raised a second uncaught exception that crashed the entire script.
    Now every activate() call is individually wrapped.
    """
    found_hwnd = None   # shared across methods so ctypes fallback can reuse

    # ── Method 1: pygetwindow ─────────────────────────────────────────────────
    try:
        import pygetwindow as gw
        wins = [w for w in gw.getAllWindows()
                if "tally" in (w.title or "").lower()]
        if wins:
            w = wins[0]
            # Each activate() call wrapped individually — error code 0 bug
            # raises on the SECOND call in the original code.
            activated = False
            try:
                w.activate()
                activated = True
            except Exception:
                pass
            if not activated:
                try:
                    w.restore()
                    time.sleep(0.2)
                except Exception:
                    pass
                try:
                    w.activate()
                    activated = True
                except Exception:
                    pass
            time.sleep(0.6)
            if activated:
                return True
            # Window was found even if activate() failed — note the hwnd
            # for the ctypes fallback below (method 3).
            try:
                found_hwnd = w._hWnd
            except Exception:
                pass
    except ImportError:
        pass
    except Exception:
        pass

    # ── Method 2: win32gui ────────────────────────────────────────────────────
    try:
        import win32gui, win32con
        _found = []
        def _cb(hwnd, _):
            if "tally" in win32gui.GetWindowText(hwnd).lower():
                _found.append(hwnd)
        win32gui.EnumWindows(_cb, None)
        if _found:
            hwnd = _found[0]
            found_hwnd = hwnd
            try:
                win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
            except Exception:
                pass
            try:
                win32gui.SetForegroundWindow(hwnd)
                time.sleep(0.6)
                return True
            except Exception:
                pass
    except ImportError:
        pass
    except Exception:
        pass

    # ── Method 3: ctypes direct WinAPI (works when pygetwindow & win32gui fail)
    try:
        import ctypes
        user32 = ctypes.windll.user32

        if found_hwnd is None:
            # Enumerate windows via ctypes
            _hwnds = []
            EnumWindowsProc = ctypes.WINFUNCTYPE(
                ctypes.c_bool, ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int))
            _buf = ctypes.create_unicode_buffer(256)
            def _enum_cb(hwnd, lParam):
                user32.GetWindowTextW(hwnd, _buf, 256)
                if "tally" in _buf.value.lower():
                    _hwnds.append(hwnd)
                return True
            user32.EnumWindows(EnumWindowsProc(_enum_cb), 0)
            if _hwnds:
                found_hwnd = _hwnds[0]

        if found_hwnd:
            SW_RESTORE = 9
            try:
                user32.ShowWindow(found_hwnd, SW_RESTORE)
            except Exception:
                pass
            try:
                # AllowSetForegroundWindow first — reduces chance of silent block
                cur_pid = ctypes.windll.kernel32.GetCurrentProcessId()
                user32.AllowSetForegroundWindow(cur_pid)
            except Exception:
                pass
            try:
                user32.SetForegroundWindow(found_hwnd)
                time.sleep(0.6)
                return True
            except Exception:
                pass
            # Window found even if focus failed — return True so keyboard
            # automation still proceeds (Tally may already be in focus).
            print("  [UI] Tally window found but focus could not be forced "
                  "(Windows anti-focus-steal). Continuing anyway.")
            return True
    except Exception:
        pass

    print("  [UI] Tally window not found. "
          "Install pygetwindow or pywin32:  pip install pygetwindow pywin32")
    return False


def _ocr_words_on_screen(region=None):
    """
    Returns list of dicts {text, cx, cy} for every word found on screen
    (or within `region`) with confidence > 30.
    `region` = (left, top, width, height) as pyautogui.screenshot expects.
    Returns [] if pytesseract is not installed / Tesseract binary missing.
    """
    try:
        import pytesseract
        import pyautogui
        from PIL import Image          # noqa: F401

        # Point at Tesseract binary if not on PATH
        _tess_paths = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
        ]
        if not pytesseract.get_tesseract_version.__doc__:   # always try
            pass
        for tp in _tess_paths:
            if os.path.exists(tp):
                pytesseract.pytesseract.tesseract_cmd = tp
                break

        img  = pyautogui.screenshot(region=region)
        data = pytesseract.image_to_data(
            img,
            output_type=pytesseract.Output.DICT,
            config="--psm 6",
        )
        results = []
        ox = region[0] if region else 0
        oy = region[1] if region else 0
        for i, txt in enumerate(data["text"]):
            txt = (txt or "").strip()
            if not txt:
                continue
            try:
                conf = int(data["conf"][i])
            except (ValueError, TypeError):
                conf = 0
            if conf < 30:
                continue
            cx = ox + data["left"][i] + data["width"][i]  // 2
            cy = oy + data["top"][i]  + data["height"][i] // 2
            results.append({"text": txt.upper(), "cx": cx, "cy": cy})
        return results

    except Exception:
        return []


def _ocr_find(words_data, search):
    """
    Find `search` (uppercase string) in the OCR words list.
    Returns (cx, cy) of first match, or None.
    Matches substring: searching 'ELAN' would match 'ELANTHALIR'.
    """
    search = search.upper()
    for w in words_data:
        if search in w["text"]:
            return w["cx"], w["cy"]
    return None


def tally_ui_open_company(target_name, already_loaded=False):
    """
    Opens `target_name` in Tally using real screen automation.

    Strategy
    ─────────
    Case A  (already_loaded=True):
      F3 → Change Company list → OCR finds name → click it.
      Fallback (no OCR): F3 → type keyword → Enter.

    Case B  (already_loaded=False):
      F3 → click "Select Company" (OCR) or keyboard nav →
        type keyword → wait for filter →
        OCR scan → click company row → Enter.
      Fallback (no OCR): type keyword → Enter (Tally picks first match).

    Returns True if we successfully sent commands (does NOT guarantee
    Tally actually loaded the company — verify via ODBC after).
    """
    try:
        import pyautogui
        pyautogui.FAILSAFE = True
        pyautogui.PAUSE    = 0.25
    except ImportError:
        print("  [UI] pyautogui not installed — run:  pip install pyautogui")
        return False

    keyword = _tally_search_keyword(target_name)
    print(f"  [UI] Target      : {target_name}")
    print(f"  [UI] Search key  : {keyword}")
    print(f"  [UI] Already open: {already_loaded}")

    # ── Step 1: Focus Tally ───────────────────────────────────────────────────
    if not _focus_tally_window():
        print("  [UI] WARNING: Could not focus Tally window — continuing anyway.")

    # ── Step 2: Press F3 ─────────────────────────────────────────────────────
    print("  [UI] Pressing F3...")
    pyautogui.press("f3")
    time.sleep(1.4)      # wait for dialog to fully open

    # ── Step 3A: Company already loaded → find it in Change Company list ──────
    if already_loaded:
        words = _ocr_words_on_screen()
        pos = _ocr_find(words, keyword)
        if pos:
            print(f"  [UI] OCR: found '{keyword}' at {pos} — clicking")
            pyautogui.click(pos[0], pos[1])
            time.sleep(1.0)
            pyautogui.press("enter")
            time.sleep(1.5)
            return True
        else:
            # Fallback: type-ahead search works in Change Company dialog
            print(f"  [UI] OCR: not found — typing keyword '{keyword}' + Enter")
            pyautogui.hotkey("ctrl", "a")
            pyautogui.typewrite(keyword, interval=0.07)
            time.sleep(0.8)
            pyautogui.press("enter")
            time.sleep(1.5)
            return True

    # ── Step 3B: Not loaded → open Select Company dialog ────────────────────
    # KEYBOARD-ONLY — no OCR/mouse clicks which can land outside dialog bounds.
    #
    # Proven path (matches user's manual steps):
    #   Escape  → close F3 Change Company dialog cleanly
    #   Alt+K   → open K: Company top menu
    #   S       → Select Company
    #
    # This reliably opens the Select Company dialog with focus in the
    # search box, regardless of screen resolution or window position.

    # ── Open Select Company dialog ───────────────────────────────────────────
    # Path: Escape (close F3) → K: Company menu → S: Select Company
    # We use keyDown/keyUp for Alt to avoid pyautogui PAUSE splitting the hotkey.
    print("  [UI] Escape (close F3 dialog)...")
    pyautogui.press("escape")
    time.sleep(1.0)             # let dialog fully close

    print("  [UI] Alt+K → Company menu...")
    pyautogui.keyDown("alt")
    time.sleep(0.15)
    pyautogui.press("k")
    pyautogui.keyUp("alt")
    time.sleep(1.0)             # wait for menu to open

    print("  [UI] S → Select Company...")
    pyautogui.press("s")
    time.sleep(2.0)             # wait for Select Company dialog to fully open

    # Save a debug screenshot so we can verify the dialog is open
    try:
        import pyautogui as _pag
        _shot = _pag.screenshot()
        _shot.save("tally_debug_select_company.png")
        print("  [UI] Screenshot saved: tally_debug_select_company.png")
    except Exception:
        pass

    # ── Type search word ──────────────────────────────────────────────────────
    # Build first meaningful word (lowercase — Tally search is case-insensitive)
    search_word = target_name.upper().split()[0]
    if search_word in ("THE", "M/S", "MRS", "MR", "12.THE") or search_word.isdigit():
        words_list = target_name.upper().split()
        search_word = next((w for w in words_list[1:] if len(w) >= 3), search_word)
    search_word = search_word[:6].lower()   # lowercase, max 6 chars
    print(f"  [UI] Typing search: '{search_word}'")
    # Use write() not typewrite() — handles chars more reliably
    pyautogui.write(search_word, interval=0.12)
    time.sleep(2.5)             # wait for Tally to filter

    # Save another screenshot to see filtered results
    try:
        _shot2 = _pag.screenshot()
        _shot2.save("tally_debug_filtered.png")
        print("  [UI] Screenshot saved: tally_debug_filtered.png")
    except Exception:
        pass

    # ── Select first result: Down + Enter ─────────────────────────────────────
    print(f"  [UI] Down + Enter...")
    pyautogui.press("down")
    time.sleep(0.5)
    pyautogui.press("enter")
    time.sleep(4.0)             # Tally may take a few seconds to load company
    print(f"  [UI] Load command sent.")
    return True


# ═════════════════════════════════════════════════════════════════════════════
#  F3-STYLE SEARCH
# ═════════════════════════════════════════════════════════════════════════════

def filter_companies(company_list, query):
    words = _norm(query).split()
    if not words: return list(company_list)
    return [c for c in company_list
            if all(w in _norm(c["name"]) for w in words)]

def f3_search_and_pick(company_list, suggestion_name=""):
    # GUI LAUNCHER: if stdin is closed (DEVNULL), auto-select the Excel suggestion
    import sys as _sys
    _gui_mode = not _sys.stdin.isatty() if hasattr(_sys.stdin, "isatty") else False
    first = True
    while True:
        if first and suggestion_name:
            print(f"  Excel suggestion  :  {suggestion_name}")
            if _gui_mode:
                raw = ""   # auto-accept suggestion
                print(f"  [AUTO] Accepting: {suggestion_name}")
            else:
                raw = input(
                    "  F3 Search (ENTER = use suggestion, or type partial name): "
                ).strip()
            if not raw:
                raw = suggestion_name
        else:
            if _gui_mode:
                raw = ""   # auto-list all then pick first
                print("  [AUTO] Listing all companies, picking first match")
            else:
                raw = input(
                    "  F3 Search (partial name, ENTER = list ALL): "
                ).strip()
        first = False

        matches = filter_companies(company_list, raw)

        if not matches:
            print(f"\n  No match for '{raw}'. Try fewer words.\n")
            continue

        if len(matches) == 1:
            m = matches[0]
            if _gui_mode:
                print(f"\n  [AUTO] Confirmed: {m['name']}")
                return m
            conf = input(
                f"\n  Found: {m['name']}  ({m.get('number','')})"
                f"\n  Press ENTER to confirm, 'n' to search again: "
            ).strip().lower()
            if conf != "n":
                return m
            continue

        # Multiple matches
        print(f"\n  {len(matches)} match(es) for '{raw or 'ALL'}'\n")
        print(f"  {'No.':<5} {'Company Name':<52} {'No.':<9} {'GSTIN':<20} Src")
        print("  " + "-" * 100)
        for i, c in enumerate(matches, 1):
            gstin = c.get("gstin","") or ""
            num   = c.get("number","") or ""
            src   = "★LOADED" if c.get("source") == "odbc" else c.get("source","")
            print(f"  {i:<5} {c['name']:<52} {num:<9} {gstin:<20} {src}")
        print()
        print("  (★LOADED = currently open in Tally)")

        while True:
            if _gui_mode:
                # Auto-pick the LOADED company (★LOADED) if present, else first
                loaded = [m for m in matches if m.get("source") == "odbc"]
                auto_pick = loaded[0] if loaded else matches[0]
                print(f"  [AUTO] Picked: {auto_pick['name']}")
                return auto_pick
            pick = input(
                f"\n  Enter number (1-{len(matches)}), or ENTER to search again: "
            ).strip()
            if not pick: break
            try:
                idx = int(pick) - 1
                if 0 <= idx < len(matches):
                    return matches[idx]
                print(f"  Enter 1 to {len(matches)}.")
            except ValueError:
                print("  Enter a number.")


# ═════════════════════════════════════════════════════════════════════════════
#  FUZZY MATCH
# ═════════════════════════════════════════════════════════════════════════════

def _noise(s):
    for n in ["PRIVATE LIMITED","PVT LTD","PVT. LTD.","LIMITED","LTD",
              "LLP","OPC","HUF","PROPRIETOR","PRESIDENT","TRUST","SOCIETY"]:
        s = s.replace(n, "")
    return re.sub(r"\s+", " ", s).strip()

def _overlap(a, b):
    ta = set(_noise(_norm(a)).split())
    tb = set(_noise(_norm(b)).split())
    if not ta or not tb: return 0.0
    return len(ta & tb) / len(ta | tb)

def best_match(excel_client, company_list):
    ec_name  = (excel_client.get("name")  or "").strip().upper()
    ec_gstin = (excel_client.get("gstin") or "").strip().upper()
    best, score = None, -1.0

    for c in company_list:
        cg   = (c.get("gstin") or "").strip().upper()
        cname = c["name"].upper()

        # ── Exact GSTIN match (authoritative) ────────────────────────────
        if ec_gstin and cg and ec_gstin == cg:
            return c, 1.0, "GSTIN exact"

        # ── GSTIN prefix match (first 10 chars = PAN portion) ─────────────
        if ec_gstin and cg and len(ec_gstin) >= 10 and len(cg) >= 10:
            if ec_gstin[2:12] == cg[2:12]:   # PAN is chars 3-12
                return c, 0.95, "GSTIN PAN match"

        # ── Name overlap ──────────────────────────────────────────────────
        s = _overlap(ec_name, cname)

        # Boost: Excel name is a substring of Tally name or vice-versa
        en = _norm(ec_name);  cn = _norm(cname)
        if en and cn and (en in cn or cn in en):
            s = max(s, 0.7)

        # Boost: first significant word matches
        ec_words = [w for w in _noise(en).split() if len(w) >= 4]
        c_words  = [w for w in _noise(cn).split() if len(w) >= 4]
        if ec_words and c_words and ec_words[0] == c_words[0]:
            s = max(s, 0.5)

        if s > score:
            score, best = s, c

    return best, score, f"name {score:.0%}"


# ═════════════════════════════════════════════════════════════════════════════
#  EXTRACT LEDGERS
# ═════════════════════════════════════════════════════════════════════════════

def save_csv(data, filename, headers):
    if not data: return
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f); w.writerow(headers); w.writerows(data)
    print(f"  Saved -> {filename}")

def print_table(rows, title):
    if not rows:
        print(f"  (No {title} with GSTIN)\n"); return
    print(f"\n  {title}  [{len(rows)} entries]")
    print(f"  {'Ledger Name':<42} {'GSTIN':<22} Parent Group")
    print("  " + "-" * 90)
    for r in rows:
        print(f"  {r[0]:<42} {r[2]:<22} {r[1]}")
    print()

def _save_customer_master(parties_with_gstin, company_name):
    """
    Creates / updates CustomerMaster.xlsx with two columns:
      Col A: GSTIN        (header row 1)
      Col B: Company Name (header row 1)
    Deduplicates by GSTIN (first occurrence wins).
    """
    out_path = "CustomerMaster.xlsx"
    try:
        wb_cm = openpyxl.Workbook()
        ws_cm = wb_cm.active
        ws_cm.title = "CustomerMaster"
        ws_cm.append(["GSTIN", "Company Name"])   # header: GSTIN first

        seen_gstins = set()
        for entry in parties_with_gstin:
            ledger_name = (entry[0] or "").strip()
            gstin       = (entry[2] or "").strip().upper()
            if not gstin or gstin in seen_gstins:
                continue
            seen_gstins.add(gstin)
            ws_cm.append([gstin, ledger_name])

        # Basic column widths
        ws_cm.column_dimensions["A"].width = 22
        ws_cm.column_dimensions["B"].width = 48

        wb_cm.save(out_path)
        print(f"  Saved CustomerMaster.xlsx → {len(seen_gstins)} GSTIN entries  ({out_path})")
    except Exception as e:
        print(f"  WARNING: Could not save CustomerMaster.xlsx: {e}")


def extract_ledgers(conn, excel_name, excel_gstin,
                    tally_name, tally_gstin, tally_state):
    print()
    print(f"  Tally Company  : {tally_name}")
    print(f"  Tally GSTIN    : {tally_gstin or 'Not Set'}")
    print(f"  State          : {tally_state or ''}")
    print(f"  Excel Name     : {excel_name}")
    print(f"  Excel GSTIN    : {excel_gstin or 'Not Set'}")
    print()

    if excel_gstin and tally_gstin and \
       excel_gstin.upper() != tally_gstin.upper():
        print(f"  WARNING: GSTIN MISMATCH")
        print(f"    Excel  = {excel_gstin}")
        print(f"    Tally  = {tally_gstin}")
        _gui_cont = not sys.stdin.isatty() if hasattr(sys.stdin, "isatty") else False
        _cont_ans = "y" if _gui_cont else input("  Continue anyway? (y/n): ").strip().lower()
        if _cont_ans != "y":
            return

    prefix = safe_filename(excel_name)
    save_csv([[tally_name, tally_gstin, tally_state]],
             f"{prefix}_company_gst.csv",
             ["Company Name (Tally)", "GSTIN (Tally)", "State"])

    print("  Querying Tally ledgers via ODBC...")
    esc  = tally_name.replace("'", "''")
    rows = odbc_query(conn,
        f"SELECT [$Name], [$Parent], [$PartyGSTIN] FROM Ledger "
        f"WHERE $Company = '{esc}'")
    if not rows:
        print("  (Company filter empty — fetching all ledgers)")
        rows = odbc_query(conn,
            "SELECT [$Name], [$Parent], [$PartyGSTIN] FROM Ledger")

    all_ledgers, debtors, creditors, other_gst = [], [], [], []
    for row in rows:
        name   = (row[0] or "").strip()
        parent = (row[1] or "").strip() if len(row) > 1 else ""
        gstin  = (row[2] or "").strip() if len(row) > 2 else ""
        if not name: continue
        entry  = [name, parent, gstin]
        all_ledgers.append(entry)
        if gstin:
            p = parent.lower()
            if   "sundry debtor"   in p: debtors.append(entry)
            elif "sundry creditor" in p: creditors.append(entry)
            else:                        other_gst.append(entry)

    total_gst = len(debtors) + len(creditors) + len(other_gst)
    print(f"\n  Total Ledgers      : {len(all_ledgers)}")
    print(f"  With GSTIN         : {total_gst}")
    print(f"    Sundry Debtors   : {len(debtors)}")
    print(f"    Sundry Creditors : {len(creditors)}")
    print(f"    Others           : {len(other_gst)}")
    print(f"  Without GSTIN      : {len(all_ledgers) - total_gst}\n")

    print_table(creditors, "Sundry Creditors with GSTIN")
    print_table(debtors,   "Sundry Debtors with GSTIN")
    if other_gst:
        print_table(other_gst, "Other Ledgers with GSTIN")

    save_csv(debtors,
             f"{prefix}_sundry_debtors_gstin.csv",
             ["Ledger Name","Parent Group","GSTIN"])
    save_csv(creditors,
             f"{prefix}_sundry_creditors_gstin.csv",
             ["Ledger Name","Parent Group","GSTIN"])
    save_csv(all_ledgers,
             f"{prefix}_all_ledgers.csv",
             ["Ledger Name","Parent Group","GSTIN"])
    save_csv(debtors + creditors + other_gst,
             f"{prefix}_all_parties_with_gstin.csv",
             ["Ledger Name","Parent Group","GSTIN"])

    # ── CustomerMaster.xlsx — GSTIN first, Company Name second ───────────────
    _save_customer_master(debtors + creditors + other_gst, tally_name)


# ═════════════════════════════════════════════════════════════════════════════
#  MAIN
# ═════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 68)
    print("   Tally GST Extractor  v3.14 |  FULLY AUTOMATIC — All Clients")
    print("=" * 68)
    print()

    # ── 1. Tally running? ────────────────────────────────────────────────────
    print("[1] Checking Tally...\n")
    if not ensure_tally_running():
        sys.exit(1)

    # ── 2. ODBC connect ──────────────────────────────────────────────────────
    print("[2] Connecting to Tally ODBC...\n")
    conn = connect_to_tally()
    if not conn:
        print("\n  Could not connect. Checklist:")
        print("    1. Tally open with at least one company loaded")
        print("    2. F12 > Advanced Config > Enable ODBC Server = Yes, Port = 9000")
        sys.exit(1)
    print()

    # XML port (for company switching)
    xml_port = _find_xml_port()
    if xml_port:
        print(f"  Tally XML HTTP ready on port {xml_port}\n")

    # ── 3. Collect ALL companies: XML + Root index + disk scan ───────────
    print("[3] Collecting all companies from Tally...\n")

    # ODBC: currently loaded companies (always available, always enriches)
    odbc_companies = get_odbc_companies(conn)
    odbc_by_name   = {c["name"].upper(): c for c in odbc_companies}
    if odbc_companies:
        print(f"  ODBC loaded    : {', '.join(c['name'] for c in odbc_companies)}")

    # XML: returns only loaded companies in most TallyPrime builds
    xml_companies = get_all_companies_xml(xml_port)
    if xml_companies:
        note = (" (loaded only — supplementing with disk scan)"
                if len(xml_companies) <= len(odbc_companies) + 2 else "")
        print(f"  XML list       : {len(xml_companies)} companies found{note}")

    # Data paths needed for both root index and disk scan
    data_paths = find_tally_data_paths()
    if data_paths:
        print(f"  Data path(s)   : {', '.join(data_paths)}")

    # Root-level index files (Manager.900 at D:\Data\ root)
    root_companies = []
    for dp in data_paths:
        batch = _read_root_index(dp)
        root_companies.extend(batch)
    if root_companies:
        print(f"  Root index     : {len(root_companies)} companies found")

    # Sub-folder binary scan — ALWAYS runs (supplements root index)
    disk_companies = []
    for dp in data_paths:
        batch = scan_tally_data_folder(dp)
        disk_companies.extend(batch)
    root_names = {c["name"].upper() for c in root_companies}
    disk_extra = [c for c in disk_companies
                  if c["name"].upper() not in root_names
                  and not c["name"].startswith("[Company #")]
    if disk_extra:
        print(f"  Disk scan      : {len(disk_extra)} extra companies")
    elif disk_companies:
        print(f"  Disk scan      : {len(disk_companies)} folders scanned"
              f" (names already in root index)")

    # ── Merge: priority = ODBC > XML > root_index > disk ─────────────────
    master = {}   # upper(name) -> dict

    for c in disk_companies:        # lowest priority
        master[c["name"].upper()] = dict(c)

    for c in root_companies:
        key = c["name"].upper()
        if key in master:
            e = master[key]
            e["gstin"]  = e.get("gstin")  or c.get("gstin",  "")
            e["state"]  = e.get("state")  or c.get("state",  "")
            e["number"] = e.get("number") or c.get("number", "")
        else:
            master[key] = dict(c)

    for c in xml_companies:
        key = c["name"].upper()
        if key in master:
            e = master[key]
            e["gstin"] = c.get("gstin","") or e.get("gstin","")
            e["state"] = c.get("state","") or e.get("state","")
        else:
            master[key] = dict(c)

    for c in odbc_companies:        # highest priority
        key = c["name"].upper()
        if key in master:
            e = master[key]
            e["gstin"]  = c.get("gstin","") or e.get("gstin","")
            e["state"]  = c.get("state","") or e.get("state","")
            e["source"] = "odbc"
        else:
            master[key] = {
                "name": c["name"], "gstin": c.get("gstin",""),
                "state": c.get("state",""), "folder": "(loaded)",
                "number": "", "source": "odbc",
            }

    all_companies = sorted(master.values(), key=lambda x: x["name"])
    real_count = sum(1 for c in all_companies
                     if not c["name"].startswith("[Company #"))
    print(f"\n  Total: {len(all_companies)} companies "
          f"({real_count} with real names)\n")

    if real_count < 5 and data_paths:
        print("  NOTE: Very few company names read from disk — .900 binary")
        print("  format may not be parseable on this Tally version.")
        print("  Matching will still work via GSTIN if set in Excel.")
        print()

    if not all_companies:
        print("  No companies found. Load a company in Tally and re-run.")
        sys.exit(1)

    # ── 4. Load ALL clients from Excel ───────────────────────────────────
    print("[4] Loading Client Manager Excel...\n")
    excel_path = find_excel_file()
    if not excel_path:
        print("  ERROR: Client Excel not found.")
        print("  Place clients.xlsx in the script folder and re-run.")
        sys.exit(1)

    print(f"  Found: {excel_path}")
    clients = load_clients_from_excel(excel_path)
    if not clients:
        print("  ERROR: No active clients found in Excel.")
        sys.exit(1)
    print(f"  Loaded {len(clients)} client(s)\n")

    # ── 5. Auto-match each client → Tally company & extract ──────────────
    print("[5] AUTO-MATCHING & EXTRACTING ALL CLIENTS...\n")
    print("=" * 68)

    results_summary = []

    for idx, excel_client in enumerate(clients, 1):
        cname = excel_client["name"]
        cgstin = excel_client.get("gstin","")
        print(f"\n[{idx}/{len(clients)}]  CLIENT: {cname}  |  GSTIN: {cgstin or '(none)'}")
        print("-" * 68)

        # ── Auto-match to Tally company ──────────────────────────────────
        bm, score, method = best_match(excel_client, all_companies)

        low_confidence = (score < 0.25 and
                          method not in ("GSTIN exact", "GSTIN PAN match"))

        # Initialise sel_* so they're always defined below
        sel_name = sel_gstin = sel_state = ""
        already_open = False

        if (not bm) or low_confidence:
            # ── FALLBACK: open via UI using Excel client name directly ────
            # The disk scan may have found the folder but couldn't read the
            # company name from the .900 binary.  Instead, type the client
            # name into Tally's F3 → Select Company search box and let Tally
            # do the fuzzy filter itself — exactly what you do manually.
            print(f"  No confident disk match (score {score:.0%}) —")
            print(f"  Trying UI: F3 → Select Company → type '{cname}'")
            ui_ok = tally_ui_open_company(cname, already_loaded=False)
            if ui_ok:
                # Wait for Tally to load — poll ODBC up to 15s
                print(f"  UI command sent. Waiting for Tally to load company...",
                      end="", flush=True)
                orig_names = {c["name"].upper() for c in odbc_companies}
                chosen = None
                try: conn.close()
                except Exception: pass
                for _wait in range(15):
                    time.sleep(1); print(".", end="", flush=True)
                    try:
                        conn = connect_to_tally(retries=1, silent=True)
                        if not conn: continue
                        fresh = get_odbc_companies(conn)
                        odbc_by_name = {c["name"].upper(): c for c in fresh}
                        newly = [c for c in fresh
                                 if c["name"].upper() not in orig_names]
                        name_hit = next(
                            (fc for fc in fresh if _overlap(cname, fc["name"]) > 0.25),
                            None)
                        chosen = newly[0] if newly else name_hit
                        if chosen:
                            break   # company appeared — stop waiting
                    except Exception:
                        pass
                print(" done.")
                if chosen:
                    print(f"  ✓ UI opened    : {chosen['name']}")
                    sel_name  = chosen["name"]
                    sel_gstin = chosen.get("gstin","") or cgstin
                    sel_state = chosen.get("state","")
                    already_open = True
                else:
                    # Last resort: check if any current ODBC company overlaps
                    try:
                        conn = connect_to_tally(retries=2, silent=True)
                        fresh = get_odbc_companies(conn) if conn else []
                    except Exception:
                        fresh = []
                    odbc_by_name = {c["name"].upper(): c for c in fresh}
                    name_hit = next(
                        (fc for fc in fresh if _overlap(cname, fc["name"]) > 0.2),
                        None)
                    if name_hit:
                        print(f"  ✓ UI opened    : {name_hit['name']} (low-conf match)")
                        sel_name  = name_hit["name"]
                        sel_gstin = name_hit.get("gstin","") or cgstin
                        sel_state = name_hit.get("state","")
                        already_open = True
                    else:
                        print(f"  ✗ After UI open, '{cname}' not found in ODBC.")
                        print(f"  Loaded companies: {[c['name'] for c in fresh]}")
                        results_summary.append((cname, "SKIPPED", "UI open: not found in ODBC"))
                        continue
            else:
                # pyautogui not installed or Tally window not found
                if not bm:
                    print(f"  ✗ No Tally company match found — SKIPPING.")
                    results_summary.append((cname, "SKIPPED", "No match"))
                else:
                    print(f"  ✗ Best match '{bm['name']}' score {score:.0%} — SKIPPING.")
                    results_summary.append(
                        (cname, "SKIPPED", f"Low match: {bm['name']}"))
                continue
        else:
            sel_name  = bm["name"]
            sel_gstin = bm.get("gstin","") or cgstin
            sel_state = bm.get("state","")
            print(f"  ✓ Matched  : {sel_name}  [{method}  {score:.0%}]")
            already_open = sel_name.upper() in odbc_by_name

        # ── Switch company in Tally if needed ────────────────────────────
        # (already_open is set correctly in both branches above)

        def _reconnect_and_refresh():
            global conn, sel_gstin, sel_state, odbc_by_name
            try: conn.close()
            except Exception: pass
            print("  Reconnecting ODBC...")
            conn = connect_to_tally(retries=4)
            if not conn:
                print("  Reconnect failed — skipping this client.")
                return False
            fresh = get_odbc_companies(conn)
            odbc_by_name = {c["name"].upper(): c for c in fresh}
            for oc in fresh:
                if oc["name"].upper() == sel_name.upper():
                    sel_gstin = oc.get("gstin", sel_gstin)
                    sel_state = oc.get("state", sel_state)
                    break
            return True

        if already_open:
            print(f"  Already loaded in Tally — no switch needed.")
        else:
            print(f"  Switching Tally to: {sel_name}...")

            # Try XML first (silent, fast)
            ok, msg = open_company_xml(sel_name, xml_port)
            if ok:
                print(f"  XML switch accepted. Waiting 4s...", end="", flush=True)
                for _ in range(4): time.sleep(1); print(".", end="", flush=True)
                print(" done.")
                if not _reconnect_and_refresh():
                    results_summary.append((cname, "FAILED", "ODBC reconnect failed"))
                    continue
            else:
                # Fall back to UI automation (F3)
                print(f"  XML not available ({msg}). Trying UI automation...")
                ui_ok = tally_ui_open_company(sel_name, already_loaded=False)
                if ui_ok:
                    print(f"  UI switch done. Waiting 5s...", end="", flush=True)
                    for _ in range(5): time.sleep(1); print(".", end="", flush=True)
                    print(" done.")
                else:
                    print(f"  ✗ Could not switch to '{sel_name}' automatically.")
                    print(f"    Open it manually in Tally (F3) then press ENTER.")
                    try: input("  Press ENTER when ready: ")
                    except Exception: pass
                if not _reconnect_and_refresh():
                    results_summary.append((cname, "FAILED", "ODBC reconnect failed"))
                    continue

            # Update odbc_by_name so next client knows what is loaded
            odbc_by_name = {c["name"].upper(): c
                            for c in get_odbc_companies(conn)}

        # ── Extract ledgers ───────────────────────────────────────────────
        print(f"  Extracting ledgers...\n")
        try:
            extract_ledgers(conn, cname, cgstin,
                            sel_name, sel_gstin, sel_state)
            results_summary.append((cname, "OK", sel_name))
        except Exception as e:
            print(f"  ✗ Extract failed: {e}")
            results_summary.append((cname, "FAILED", str(e)))

    # ── 6. Summary ───────────────────────────────────────────────────────
    print()
    print("=" * 68)
    print("  EXTRACTION COMPLETE — SUMMARY")
    print("=" * 68)
    ok_count   = sum(1 for _, s, _ in results_summary if s == "OK")
    skip_count = sum(1 for _, s, _ in results_summary if s == "SKIPPED")
    fail_count = sum(1 for _, s, _ in results_summary if s == "FAILED")
    print(f"  Total   : {len(results_summary)}")
    print(f"  ✓ OK    : {ok_count}")
    print(f"  ✗ Skip  : {skip_count}")
    print(f"  ✗ Fail  : {fail_count}")
    print()
    for cname, status, note in results_summary:
        icon = "✓" if status == "OK" else "✗"
        print(f"  {icon}  {cname:<45} {status:<8}  {note}")
    print()

    try: conn.close()
    except Exception: pass
    print("  Done.")
