"""
================================================================================
  RUN ALL — Unified GST + Income Tax Automation Pipeline  v10.12
================================================================================

  PIPELINE ORDER (enforced):
  ─────────────────────────────────────────────────────────────────────────────
  Step 1    Tally extract → CustomerMaster.xlsx (GSTIN→Name cache, offline)
  Step 2+3  GST CAPTCHA entered ONCE → all clients download (GSTR-1/2B/3B/1A)
            gst_suite output → MultiYear_{ts}/AY{fy_tag}/{ClientName_GSTIN}/
  Step 4+5  IT portal → 26AS / AIS / TIS per PAN   (IT_OUT_DIR passed via env)
            it_suite output  → AY{AY_LABEL}_{ts}/{ClientName_GSTIN}/
  Step 6    Master Bridge — GST ↔ IT reconciliation
  Step 6b   GST-IT Comparison Excel (TIS / AIS template)
  Step 6c   GSTR-2B Consolidated Extractor per GSTIN
  Step 6d   GSTR-1 vs 26AS Comparison per client
  Step 7    Final Consolidated 7-Sheet Report
  ─────────────────────────────────────────────────────────────────────────────

  HOW TO RUN:
    python run_all.py                  ← full run (steps 1→7, Tally first)
    python run_all.py --skip-tally     ← skip Step 1 (Tally not available)
    python run_all.py --only-gst       ← steps 2-3 only
    python run_all.py --only-it        ← steps 4-5 only
    python run_all.py --only-bridge    ← steps 6-7 only
    python run_all.py --offline        ← pick folders, run bridge (no downloads)
    python run_all.py --fy 2024-25     ← override FY
    python run_all.py --client "RAVI"  ← one client only

  KEY FIXES v10.12:
  ✓ run_bridge_step: override paths VALIDATED before use — if staging was cleaned
    up the path no longer exists, so we fall through to Option B automatically
  ✓ run_bridge_step: _resolve_gst() / _resolve_it() scan ClientName/GST Automation
    and ClientName/IT Download as first priority (always present after reorganize)
  ✓ Online mode: _best_bridge_paths() helper resolves Option B paths once and
    passes them to run_bridge_step, run_gst_it_comparison, and all sub-steps
  ✓ master_bridge.py now always receives a folder that EXISTS on disk
  ✓ build_final_consolidated v3: no longer requires FINAL_CONSOLIDATED_REPORT*.xlsx
  ✓ build_final_consolidated v3: accepts --base <BASE_DIR> --out <path> from run_all.py
  ✓ build_final_consolidated v3: auto-discovers ClientName/ folders and source Excels
  ✓ run_bridge_step: passes actual GST Automation folder (not its parent) for GST arg
  ✓ run_bridge_step: passes actual IT Download folder (not GST Automation parent) for IT
  ✓ _run_final_consolidated: always passes correct BASE_DIR (not a stub path)
  ✓ reorganize_it_output: now tries Name+GSTIN candidates (fixes IT files not moving)
  ✓ run_gst_it_comparison_step: _find_pdf scoped to current client IT Download only
  ✓ _read_it_recon / _merge_gst_data: float() uses try/except (fixes '2025-26' crash)
  ✓ Step 6d: passes client root folder so comparison finds GSTR1_FY + 26AS in subfolders
  ✓ gstr1_26as_comparison_v2: rglob fallback for GSTR1_FY_*.xlsx and 26AS*.pdf
  ✓ it_suite_v6: GST auto-detect searches Option B ClientName/GST Automation/ first
  ✓ _safe_folder() + _folder_candidates() added
  ✓ Step 7 (Final Consolidated) — runs in ALL modes including --only-bridge
  ✓ IT_OUT_DIR env var passed to it_suite so it writes to the tracked folder
  ✓ Duplicate IT recon eliminated — it_suite v6 runs recon internally
================================================================================
"""
import os, sys, re, subprocess, argparse, logging, shutil, threading, time
from pathlib import Path
from datetime import datetime

# Option B folder structure helper
try:
    from folder_structure import (
        client_root, client_raw_data, client_gst, client_it, client_downloads,
        client_bridge, client_comparison, client_26as,
        ensure_client_dirs, reorganize_gst_output, reorganize_it_output,
        print_structure,
    )
    _OPTION_B = True
except ImportError:
    _OPTION_B = False
    def client_root(b, n): return b / n.replace(' ', '_')
    def client_raw_data(b, n): return client_root(b, n) / 'GST Automation'
    def client_gst(b, n): return client_root(b, n) / 'GST Automation'
    def client_it(b, n): return client_root(b, n) / 'IT Download'
    def client_downloads(b, n): return client_root(b, n) / 'GST Automation'
    def client_bridge(b, n): return client_root(b, n) / 'IT Bridge'
    def client_comparison(b, n): return client_root(b, n) / 'GST IT Comparison'
    def client_26as(b, n): return client_root(b, n) / '26AS vs GSTR1'

SCRIPT_DIR = Path(__file__).parent.resolve()

# ─── Option B staging dirs (suites write here first, then we reorganize) ─────
# These are TEMPORARY staging locations; real output goes to per-client folders.
_GST_STAGING = None   # set dynamically after gst_suite runs
_IT_STAGING  = None   # set dynamically after it_suite runs

# ─── Change only ONE line each year ──────────────────────────────────────────
FY_LABEL   = "2025-26"
_fy_yr     = int(FY_LABEL.split("-")[0])
AY_LABEL   = f"{_fy_yr + 1}-{str(_fy_yr + 2)[2:]}"   # "2026-27"

# ─── Output base folder ───────────────────────────────────────────────────────
def _find_base_dir():
    """
    Locate working base directory.
    Priority:
      1. SCRIPT_DIR itself when it already contains known sub-folders
         (user runs scripts from their Downloads folder directly).
      2. OneDrive / Desktop OUTPUT (corporate laptops).
      3. Downloads folder (default fallback).
    """
    home = Path.home()

    # Priority 1: script's own folder already has known sub-folders
    for marker in ["GST_Automation", "IT_Automation", "GST_IT_Bridge",
                   "GST_IT_Comparison", "26AS_GSTR1_Compare",
                   "26as", "TALLY EXTRACTED"]:
        if (SCRIPT_DIR / marker).exists():
            return SCRIPT_DIR

    # Option B: script dir already has per-client folders with Raw Data subfolder
    for child in SCRIPT_DIR.iterdir():
        if child.is_dir() and ((child / "GST Automation").exists() or (child / "Raw Data").exists()):
            return SCRIPT_DIR
    # Also accept Raw Data (legacy OptionC)
    for child in SCRIPT_DIR.iterdir():
        if child.is_dir() and (child / "IT Download").exists():
            return SCRIPT_DIR

    # Priority 2: OneDrive / Desktop OUTPUT
    candidates = [
        home / "OneDrive" / "Desktop" / "OUTPUT",
        home / "OneDrive - Personal" / "Desktop" / "OUTPUT",
        *[p / "Desktop" / "OUTPUT" for p in home.glob("OneDrive*") if p.is_dir()],
        home / "Desktop" / "OUTPUT",
    ]
    for c in candidates:
        if c.exists():
            return c

    # Priority 3: Downloads (always exists on Windows)
    dl = home / "Downloads"
    if dl.exists():
        return dl

    onedrive = next((p for p in home.glob("OneDrive*") if p.is_dir()), None)
    if onedrive:
        return onedrive / "Desktop" / "OUTPUT"
    return home / "Downloads"

BASE_DIR   = _find_base_dir()
# Hidden temp staging dirs — written by gst_suite / it_suite, then
# reorganized into ClientName/Raw Data/ and deleted. Never visible in Downloads.
GST_BASE   = SCRIPT_DIR / "._gst_stage_"
IT_BASE    = SCRIPT_DIR / "._it_stage_"

# ── Alternate folder names that may exist on disk ─────────────────────────────
def _resolve_gst_base():
    for alt in [GST_BASE, BASE_DIR / "GSTAutomation", BASE_DIR / "GST"]:
        if alt.exists(): return alt
    return GST_BASE

def _resolve_it_base():
    for alt in [IT_BASE, BASE_DIR / "ITAutomation", BASE_DIR / "IT"]:
        if alt.exists(): return alt
    return IT_BASE
RUN_TS     = datetime.now().strftime("%Y%m%d_%H%M")

# Stub paths — updated to real folders after each suite finishes
GST_RUN    = GST_BASE / f"FY{FY_LABEL}_{RUN_TS}"
IT_RUN     = IT_BASE  / f"AY{AY_LABEL}_{RUN_TS}"

VARIANCE_THRESHOLD = 5000
LOG_FILE   = BASE_DIR / f"run_all_{RUN_TS}.log"

MISSING = []
try:    import pandas as pd
except: MISSING.append("pandas")
try:
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
except: MISSING.append("openpyxl")

if MISSING:
    print(f"✗ Missing: pip install {' '.join(MISSING)}")
    sys.exit(1)

# ─── Colours ─────────────────────────────────────────────────────────────────
DARK_BLUE = "1F3864"; MED_BLUE  = "2E75B6"
HDR_BG    = "1F3864"; TOT_BG    = "D6DCE4"
ALT1      = "FFFFFF"; ALT2      = "F2F2F2"
GREEN_BG  = "C6EFCE"; RED_BG    = "FFC7CE"; YELLOW_BG = "FFEB9C"
GREEN_FG  = "276221"; RED_FG    = "9C0006"
NUM_FMT   = "#,##0.00"
FY_MONTHS = ["APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC","JAN","FEB","MAR"]


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
def _setup_log():
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ]
    )
    return logging.getLogger("run_all")

log = None
def _log(msg, level="info"):
    if log: getattr(log, level)(msg)
    else:   print(msg)

def _banner(title):
    _log("=" * 72)
    _log(f"  {title}")
    _log("=" * 72)


# ═══════════════════════════════════════════════════════════════════════════════
# LIVE DOWNLOAD WATCHER
# ═══════════════════════════════════════════════════════════════════════════════
class _DownloadWatcher:
    """
    Background thread that watches one or more folders while a suite script
    (gst_suite / it_suite) is running and prints a live status line every few
    seconds so the operator can see downloads arriving in real time.

    What it shows every INTERVAL seconds:
      [DL WATCH]  GST staging: 3 file(s) | 4.2 MB  ←  latest: GSTR1_APR2025.zip (1.1 MB) ✓
      [DL WATCH]  ⏳ In progress: GSTR2B_062025_….crdownload  (2.3 MB downloading…)

    It reads:
      • All finished files (.zip, .xlsx, .pdf, .json) in the watched folders
      • Any active .crdownload / .tmp / .part files (Chrome in-progress marker)
      • File sizes — so the operator can see bytes accumulating
    """

    INTERVAL     = 4          # seconds between status prints
    IN_PROGRESS  = (".crdownload", ".tmp", ".part")
    DONE_EXT     = {".zip", ".xlsx", ".pdf", ".json"}

    def __init__(self, watch_dirs: list, label: str = "DL WATCH"):
        self._dirs   = [Path(d) for d in watch_dirs if d]
        self._label  = label
        self._stop   = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._stop.clear()
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=self.INTERVAL + 2)

    @staticmethod
    def _fmt_size(n_bytes: int) -> str:
        if n_bytes >= 1_048_576:
            return f"{n_bytes/1_048_576:.1f} MB"
        if n_bytes >= 1024:
            return f"{n_bytes/1024:.1f} KB"
        return f"{n_bytes} B"

    def _scan(self):
        done_files   = []   # (path, size)
        active_files = []   # (path, size)  — .crdownload etc.

        for watch_dir in self._dirs:
            if not watch_dir.exists():
                continue
            try:
                for f in watch_dir.rglob("*"):
                    if not f.is_file():
                        continue
                    try:
                        sz = f.stat().st_size
                    except OSError:
                        continue
                    if f.name.endswith(self.IN_PROGRESS):
                        active_files.append((f, sz))
                    elif f.suffix.lower() in self.DONE_EXT:
                        done_files.append((f, sz))
            except Exception:
                pass

        return done_files, active_files

    def _run(self):
        prev_count = -1
        while not self._stop.is_set():
            try:
                done, active = self._scan()
                total_size   = sum(sz for _, sz in done)
                count        = len(done)

                # Only reprint if something changed, or every ~30 s anyway
                changed = (count != prev_count) or active
                if changed:
                    prev_count = count

                    # Latest completed file (by mtime)
                    latest_str = ""
                    if done:
                        latest = max(done, key=lambda x: x[0].stat().st_mtime
                                     if x[0].exists() else 0)
                        latest_str = (f"  ←  latest: {latest[0].name[:50]} "
                                      f"({self._fmt_size(latest[1])}) ✓")

                    # Main status line
                    line = (f"  [{self._label}]  "
                            f"{count} file(s) | {self._fmt_size(total_size)}"
                            f"{latest_str}")
                    print(line, flush=True)

                    # Active download line(s)
                    for af, asz in active:
                        print(f"  [{self._label}]  ⏳ downloading: "
                              f"{af.name[:60]}  ({self._fmt_size(asz)}…)", flush=True)

            except Exception:
                pass

            self._stop.wait(self.INTERVAL)


def _start_gst_watcher() -> "_DownloadWatcher":
    """Start a watcher over all GST staging + per-client GST Automation dirs."""
    dirs = [GST_BASE]
    try:
        for child in BASE_DIR.iterdir():
            gd = child / "GST Automation"
            if gd.exists():
                dirs.append(gd)
            rd = child / "Raw Data"
            if rd.exists():
                dirs.append(rd)
    except Exception:
        pass
    w = _DownloadWatcher(dirs, label="GST DL")
    w.start()
    return w


def _start_it_watcher() -> "_DownloadWatcher":
    """Start a watcher over IT staging + per-client IT Download dirs."""
    dirs = [IT_BASE]
    try:
        for child in BASE_DIR.iterdir():
            it = child / "IT Download"
            if it.exists():
                dirs.append(it)
    except Exception:
        pass
    w = _DownloadWatcher(dirs, label="IT DL")
    w.start()
    return w
def _col(df, *cands):
    def _norm(s):
        s = str(s).lower().strip()
        s = re.sub(r'[\s\n\r()/\\-]+', '_', s)
        return re.sub(r'_+', '_', s).strip('_')
    norm_cols = {_norm(c): c for c in df.columns}
    for cand in cands:
        h = norm_cols.get(_norm(cand))
        if h is not None: return h
    return None

def _clean(s):
    if s is None: return ""
    s = str(s).strip()
    return "" if s.lower() in ("nan","none","") else s

def load_clients(fy_override=None, name_filter=None):
    candidates = [
        "Client_Manager_Secure_AY2025-26.xlsx",
        "clients_manager.xlsx",
        "clients.xlsx",
        "clients.csv",
    ]
    for fname in candidates:
        fpath = SCRIPT_DIR / fname
        if not fpath.exists(): continue
        _log(f"  Loading clients from: {fname}")
        try:
            if fname.endswith(".csv"):
                df = pd.read_csv(fpath, dtype=str).fillna("")
            else:
                xl  = pd.ExcelFile(fpath, engine="openpyxl")
                sht = next(
                    (s for s in xl.sheet_names if any(k in s.lower() for k in ["client","cred","🔐"])),
                    xl.sheet_names[0])
                # Some files (e.g. Client_Manager_Secure) have a merged title in row 0
                # and the real column headers in row 2. Auto-detect the header row.
                df_raw = xl.parse(sht, header=None, dtype=str).fillna("")
                hdr_row = 0
                for _ri, _row in df_raw.iterrows():
                    _rt = " ".join(str(v).lower() for v in _row if str(v).strip())
                    if any(k in _rt for k in ["client name","gstin","pan","gst portal"]):
                        hdr_row = _ri; break
                df = xl.parse(sht, header=hdr_row, dtype=str).fillna("")
            df.columns = [re.sub(r'[\n\r]+','_', str(c)).strip().lower().replace(" ","_") for c in df.columns]
            df.columns = [re.sub(r'_+','_', c).strip('_') for c in df.columns]

            c_name = _col(df,"client_name","name","company_name","client name")
            c_pan  = _col(df,"pan","pan_no","pan_number"," pan ")
            c_gst  = _col(df,"gstin","gst_number","gstin_number","gst_no")
            c_dob  = _col(df,"dob","date_of_birth","date_of_birth_(ddmmyyyy)")
            c_itu  = _col(df,"it_username","it_user","income_tax_username","it username")
            c_itp  = _col(df,"it_password","income_tax_password","it password")
            c_gstu = _col(df,"gst_username","gst_user","gst_portal_username","gst username")
            c_gstp = _col(df,"gst_password","gst_portal_password","gst password")
            c_act  = _col(df,"active","status","active_(yes/no)","active_yes/no")
            c_fy   = _col(df,"fy","financial_year","fin_year"," fy")

            clients = []
            for _, row in df.iterrows():
                name = _clean(row.get(c_name,"")) if c_name else ""
                if not name: continue
                active = _clean(row.get(c_act,"YES")).upper() if c_act else "YES"
                if active not in ("YES","Y","1","TRUE","ACTIVE",""): continue
                if name_filter and name_filter.lower() not in name.lower(): continue
                pan    = _clean(row.get(c_pan,"")).upper() if c_pan else ""
                raw_g  = _clean(row.get(c_gst,"")) if c_gst else ""
                gstins = [g.strip() for g in re.split(r"[,;/\n]",raw_g) if g.strip()]
                fy_raw = _clean(row.get(c_fy,"")) if c_fy else ""
                fy     = fy_override or (fy_raw if re.match(r"\d{4}-\d{2,4}",fy_raw) else FY_LABEL)
                clients.append({
                    "name":         name,
                    "pan":          pan,
                    "gstin":        gstins,
                    "fy":           fy,
                    "dob":          _clean(row.get(c_dob,"")) if c_dob else "",
                    "it_username":  _clean(row.get(c_itu,"")) if c_itu else "",
                    "it_password":  _clean(row.get(c_itp,"")) if c_itp else "",
                    "gst_username": _clean(row.get(c_gstu,"")) if c_gstu else "",
                    "gst_password": _clean(row.get(c_gstp,"")) if c_gstp else "",
                })
            if clients:
                _log(f"  Loaded {len(clients)} client(s)")
                return clients
        except Exception as e:
            _log(f"  ✗ Could not read {fname}: {e}", "error")

    _log("✗ No clients file found. Create clients.xlsx with columns:", "error")
    _log("  Client Name | PAN | GSTIN | DOB | IT Username | IT Password |", "error")
    _log("  GST Username | GST Password | Active | FY", "error")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════════════
# FOLDER DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════════
def _iter_gst_run_dirs():
    """
    Yield all GST run directories, newest first.
    Handles both layouts:
      run_all layout  → GST_Automation/FY2025-26_YYYYMMDD/
      gst_suite layout → GST_Automation/MultiYear_ts/AY2025_26/
    Also scans alternate folder names that users may have on disk
    (GST_IT_Bridge, 26as, etc.) so discovery always works even when
    BASE_DIR is the Downloads folder directly.
    """
    # All candidate base folders to search
    # Option B: scan each client's GST Automation subfolder first
    option_b_roots = []
    try:
        for child in BASE_DIR.iterdir():
            gst_sub = child / "Raw Data"
            if child.is_dir() and gst_sub.exists():
                option_b_roots.append(gst_sub)
    except PermissionError:
        pass

    gst_search_roots = option_b_roots + [
        GST_BASE,
        BASE_DIR / "GST_IT_Bridge",
        BASE_DIR / "GST_IT_Comparison",
        BASE_DIR / "26AS_GSTR1_Compare",
        BASE_DIR / "26as",
        BASE_DIR / "TALLY EXTRACTED",
        BASE_DIR,               # direct children of Downloads
    ]
    seen = set()
    for root in gst_search_roots:
        if not root.exists() or not root.is_dir():
            continue
        try:
            for d in sorted(root.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
                if not d.is_dir():
                    continue
                key = str(d.resolve())
                if key in seen:
                    continue
                seen.add(key)
                if d.name.startswith("MultiYear"):
                    for sub in sorted(d.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
                        if sub.is_dir():
                            sub_key = str(sub.resolve())
                            if sub_key not in seen:
                                seen.add(sub_key)
                                yield sub
                else:
                    yield d
        except PermissionError:
            pass


def _discover_real_gst_run(fy=None):
    """
    After gst_suite finishes, find the folder that actually has output.
    Priority:
      0. Option B: any ClientName/GST Automation/ that has real xlsx files
      1. ~/.gst_suite_last_run marker file (written by gst_suite after each run)
      2. Current GST_RUN if it has real files
      3. Newest MultiYear_*/AY{fy_tag}/ with ANNUAL_RECONCILIATION files
      4. Newest MultiYear_*/AY* with GSTR2B files
      5. Fallback to GST_RUN stub
    """
    fy_tag = (fy or FY_LABEL).replace("-","_")

    # Priority 0: Option B — ClientName/GST Automation/ with real xlsx
    # FIX v10.12: was looking for "Raw Data" subfolder but actual folder is "GST Automation"
    try:
        for child in sorted(BASE_DIR.iterdir(),
                            key=lambda d: d.stat().st_mtime, reverse=True):
            if not child.is_dir():
                continue
            # Check "GST Automation" first (primary Option B layout), then "Raw Data"
            for _subfolder_name in ("GST Automation", "Raw Data"):
                gst_sub = child / _subfolder_name
                if gst_sub.is_dir() and any(gst_sub.glob("*.xlsx")):
                    _log(f"  GST folder (Option B): {gst_sub}")
                    return gst_sub
    except Exception:
        pass

    # Priority 1: exact path written by gst_suite to marker file
    _marker = Path.home() / ".gst_suite_last_run"
    if _marker.exists():
        try:
            _path = Path(_marker.read_text(encoding="utf-8").strip())
            if _path.exists() and _path.is_dir():
                _log(f"  GST folder (marker): {_path}")
                return _path
        except Exception:
            pass

    if GST_RUN.exists() and any(GST_RUN.rglob("ANNUAL_RECONCILIATION*.xlsx")):
        return GST_RUN

    for myd in sorted(GST_BASE.glob("MultiYear_*"), key=lambda d: d.stat().st_mtime, reverse=True):
        if not myd.is_dir(): continue
        for sub in sorted(myd.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if sub.is_dir() and any(sub.rglob("ANNUAL_RECONCILIATION*.xlsx")):
                return sub

    for d in sorted(GST_BASE.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
        if d.is_dir() and any(d.rglob("ANNUAL_RECONCILIATION*.xlsx")):
            return d

    # Fallback 2: newest MultiYear_*/AY* with GSTR2B files
    for myd in sorted(GST_BASE.glob("MultiYear_*"), key=lambda d: d.stat().st_mtime, reverse=True):
        if not myd.is_dir(): continue
        for sub in sorted(myd.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if sub.is_dir() and any(sub.rglob("GSTR2B*.xlsx")):
                return sub

    # Last resort: newest AY* sub of any MultiYear_* regardless of content
    for myd in sorted(GST_BASE.glob("MultiYear_*"), key=lambda d: d.stat().st_mtime, reverse=True):
        if not myd.is_dir(): continue
        for sub in sorted(myd.iterdir(), key=lambda x: x.stat().st_mtime, reverse=True):
            if sub.is_dir():
                return sub

    return GST_RUN


def _discover_real_it_run():
    """
    After it_suite finishes, find the IT folder with real data.
    it_suite respects IT_OUT_DIR env var; if used correctly IT_RUN has content.
    If it ignored env var and created its own AY*_HHMM folder, discover it.

    KEY FIX (Issue 2): it_suite sometimes runs its own recon internally and
    creates a DIFFERENT AY* folder than IT_RUN. We must find the folder that
    contains the LARGEST IT_RECONCILIATION xlsx (most data), not just any folder.
    """
    # Option B check first — ClientName/IT Download/ with real xlsx
    # FIX v10.12: was looking for "Raw Data" subfolder but actual folder is "IT Download"
    try:
        for child in sorted(BASE_DIR.iterdir(),
                            key=lambda d: d.stat().st_mtime, reverse=True):
            if not child.is_dir():
                continue
            # Check "IT Download" first (primary Option B layout), then "Raw Data"
            for _subfolder_name in ("IT Download", "Raw Data"):
                it_sub = child / _subfolder_name
                if it_sub.is_dir() and any(it_sub.glob("IT_RECONCILIATION*.xlsx")):
                    _log(f"  IT folder (Option B): {it_sub}")
                    return it_sub
    except Exception:
        pass

    if not IT_BASE.exists():
        return IT_RUN

    candidates = sorted(
        [d for d in IT_BASE.iterdir() if d.is_dir()],
        key=lambda d: d.stat().st_mtime, reverse=True
    )

    # Pass 1: find the folder whose IT_RECONCILIATION is the largest (real data)
    best_folder = None
    best_size   = 0
    for d in candidates:
        for xl in d.rglob("IT_RECONCILIATION*.xlsx"):
            try:
                sz = xl.stat().st_size
                if sz > best_size:
                    best_size   = sz
                    best_folder = d
            except: pass

    if best_folder and best_size >= 25_000:
        return best_folder

    # Pass 2: folder with any PDFs (downloads happened but recon not yet built)
    for d in candidates:
        if any(d.rglob("*.pdf")):
            return d

    # Pass 3: current IT_RUN if it exists at all
    if IT_RUN.exists():
        return IT_RUN

    return IT_RUN



# ═══════════════════════════════════════════════════════════════════════════════
# FOLDER NAMING HELPERS  (must match gst_suite_v31 & it_suite_v6 exactly)
# ═══════════════════════════════════════════════════════════════════════════════
def _safe_folder(name, gstin=""):
    """
    Build the client folder name exactly as gst_suite_v31 and it_suite_v6 do:
        ClientName_GSTIN   (if GSTIN provided)
        ClientName         (if no GSTIN)
    Both suites use:
        safe = name.replace(" ","_").replace("/","_")
        safe = f"{safe}_{gstin.upper()}" if gstin else safe
    """
    safe = name.replace(" ", "_").replace("/", "_")
    if gstin:
        safe = f"{safe}_{gstin.strip().upper()}"
    return safe


def _folder_candidates(name, gstin=""):
    """
    Return ALL folder name variants to try when searching for a client's data.
    Ordered: most-specific first (v10.12 format) → legacy formats last.
    This lets run_all find folders created by older AND newer suite versions.
    """
    safe_name = name.replace(" ", "_").replace("/", "_")
    gstin_up  = (gstin or "").strip().upper()
    cands = []
    if gstin_up:
        cands.append(f"{safe_name}_{gstin_up}")           # v10.12: Name_GSTIN
        cands.append(f"{safe_name}_{gstin_up[:10]}")      # partial GSTIN match
    cands.append(safe_name)                                # legacy: Name_only
    cands.append(name)                                     # legacy: exact name
    if gstin_up:
        cands.append(gstin_up)                             # legacy: GSTIN only
    return cands


def _find_gst_excel_for_client(gstins, name, fy=None):
    """Find the best GST reconciliation Excel for a client.
    Priority: Option B ClientName/GST Automation/ → legacy staging."""
    # Option B: check ClientName/GST Automation/ first
    gst_b = client_gst(BASE_DIR, name)
    if gst_b.exists():
        excels = sorted(gst_b.glob("*.xlsx"), key=lambda p: p.stat().st_size, reverse=True)
        for xl in excels:
            if "IT_RECONCILIATION" not in xl.name.upper():
                return xl

    # Legacy staging search
    search_dirs = [GST_RUN] + list(_iter_gst_run_dirs())
    for run_dir in search_dirs:
        for gstin in gstins:
            for cand in _folder_candidates(name, gstin):
                d = run_dir / cand
                if not d.exists(): continue
                excels = sorted(d.glob("*.xlsx"), key=lambda p: p.stat().st_size, reverse=True)
                for xl in excels:
                    if "IT_RECONCILIATION" not in xl.name.upper():
                        return xl
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1 — TALLY GST EXTRACT (CustomerMaster + Supplier Name Cache)
# ═══════════════════════════════════════════════════════════════════════════════
def run_tally_extract_step():
    """
    Run tally_extract_gst.py FIRST — before any portal downloads.

    Why first:
      • Extracts GSTIN → Supplier/Buyer Name mapping directly from Tally's
        local ODBC ledger (no online lookup needed).
      • Writes CustomerMaster.xlsx (GSTIN | Company Name) that is consumed by:
          – gstr1_26as_comparison_v2.py  (26AS vs GSTR-1 — supplier name column)
          – master_bridge.py             (name enrichment for GST ↔ IT recon)
          – gstr1_fy_v5.py               (GSTR-1 FY detail sheet supplier names)
      • Running it first means every downstream step has the name cache ready
        so zero online GSTIN lookups are needed during the run.

    Behaviour:
      • If tally_extract_gst.py is not present → logs a warning and skips
        (non-fatal; downstream steps fall back to online lookup).
      • If Tally is not running → the script itself will prompt the user;
        run_all.py does NOT force-skip — the user can abort inside Tally script.
      • Output (CustomerMaster.xlsx, *_all_parties_with_gstin.csv) is written
        to SCRIPT_DIR (same folder as the .exe / .py files) so all scripts
        find it automatically via their own search logic.
    """
    _banner("STEP 1 — Tally GST Extract (Build Supplier Name Cache)")

    tally_script = SCRIPT_DIR / "tally_extract_gst.py"
    if not tally_script.exists():
        _log("  ⚠  tally_extract_gst.py not found in script folder — skipping Step 1", "warning")
        _log("     Downstream steps will fall back to online GSTIN lookup.", "warning")
        return

    _log("  Running tally_extract_gst.py ...")
    _log("  ► Tally must be open with the client company loaded.")
    _log("  ► CustomerMaster.xlsx will be written/updated in the script folder.")
    _log("  ► Output is used by 26AS vs GSTR-1, Master Bridge, and GSTR-1 FY steps.")
    _log("")

    try:
        # Run interactively — the script has interactive prompts (F3 company picker)
        r = subprocess.run(
            [sys.executable, str(tally_script)],
            timeout=600,          # 10-min cap; Tally company load can be slow
            capture_output=False, # show all output so user sees prompts
        )
        if r.returncode == 0:
            # Confirm CustomerMaster.xlsx was produced
            cm = SCRIPT_DIR / "CustomerMaster.xlsx"
            if cm.exists():
                try:
                    import openpyxl as _oxl
                    _wb = _oxl.load_workbook(str(cm), read_only=True)
                    _ws = _wb.active
                    _rows = sum(1 for _ in _ws.iter_rows()) - 1  # minus header
                    _wb.close()
                    _log(f"  ✓ CustomerMaster.xlsx updated — {_rows} GSTIN entries")
                except Exception:
                    _log(f"  ✓ CustomerMaster.xlsx written → {cm}")
            else:
                _log("  ✓ Tally extract finished (CustomerMaster.xlsx not found — check Tally output)")
        else:
            _log(f"  ✗ tally_extract_gst.py exited with code {r.returncode}", "warning")
            _log("     Continuing pipeline — downstream steps will use existing cache or online lookup.", "warning")
    except subprocess.TimeoutExpired:
        _log("  ✗ Tally extract timed out (10 min). Continuing pipeline.", "warning")
    except Exception as e:
        _log(f"  ✗ Tally extract error: {e}", "error")
        _log("     Continuing pipeline.", "warning")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2+3 — GST SUITE
# ═══════════════════════════════════════════════════════════════════════════════
def run_gst_step(clients):
    """
    Launch gst_suite_v32.py interactively.

    gst_suite has NO --gstin/--out args. It uses its own interactive main()
    that asks which returns to download, which FY range, reads clients.xlsx
    itself, and creates: GST_Automation/MultiYear_{ts}/AY{fy_tag}/{ClientName}/

    After it exits, _discover_real_gst_run() updates GST_RUN to the real folder.
    """
    global GST_RUN
    _banner("STEP 2+3 — GST Portal Download + Reconciliation Excel")
    _log("  ► Browser will open. Enter CAPTCHA ONCE — all clients processed.")
    _log(f"  ► Output: {GST_BASE}/MultiYear_*/AY{FY_LABEL.replace('-','_')}/")

    gst_script = SCRIPT_DIR / "gst_suite_v32.py"
    if not gst_script.exists():
        _log("  ✗ gst_suite_v32.py not found — skipping", "warning"); return

    # Hidden staging dir inside SCRIPT_DIR — never touches Downloads directly
    GST_BASE.mkdir(parents=True, exist_ok=True)

    # Tell gst_suite exactly where to write via env var.
    # gst_suite reads GST_OUT_DIR and uses it as session_root if set.
    fy_tag     = FY_LABEL.replace("-","_")
    gst_out    = GST_BASE / f"MultiYear_{RUN_TS}" / f"AY{fy_tag}"
    gst_out.mkdir(parents=True, exist_ok=True)
    env = {**os.environ, "GST_OUT_DIR": str(gst_out)}

    try:
        _log("  ► Live download monitor started — you will see files arrive below:")
        _watcher = _start_gst_watcher()
        result = subprocess.run(
            [sys.executable, str(gst_script)],
            timeout=7200,
            capture_output=False,
            env=env,
        )
        _watcher.stop()
        if result.returncode == 0:
            _log("  ✓ gst_suite completed")
        else:
            _log(f"  ✗ gst_suite exited {result.returncode}", "warning")
    except subprocess.TimeoutExpired:
        _watcher.stop()
        _log("  ✗ gst_suite timed out (2 hrs)", "warning")
    except Exception as e:
        try: _watcher.stop()
        except Exception: pass
        _log(f"  ✗ gst_suite error: {e}", "error")

    # Discover real output folder
    real = _discover_real_gst_run()
    if real != GST_RUN:
        _log(f"  ℹ  GST output discovered: {real}")
        GST_RUN = real
    else:
        _log(f"  ℹ  GST output: {GST_RUN}")

    # ── Option B: reorganize GST into ClientName/Raw Data/ ────────────────────
    if _OPTION_B and GST_RUN.exists():
        _log("  Reorganizing GST files into ClientName/GST Automation/ ...")
        try:
            reorganize_gst_output(BASE_DIR, GST_RUN, clients, _folder_candidates, _log)
            _log("  OK ClientName/GST Automation/ + IT Download/ updated")
        except Exception as _e:
            _log(f"  ⚠  reorganize_gst_output error: {_e}", "warning")
        # Clean up hidden staging dir so it never appears in Downloads
        try:
            if GST_BASE.exists():
                shutil.rmtree(str(GST_BASE), ignore_errors=True)
                _log("  ✓ Staging folder cleaned up")
        except Exception as _e:
            _log(f"  ⚠  Staging cleanup error: {_e}", "warning")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4+5 — IT SUITE
# ═══════════════════════════════════════════════════════════════════════════════
def run_it_step(clients):
    """
    Launch it_suite_v6.py with IT_OUT_DIR set so it writes to our tracked folder.
    it_suite v6 checks os.environ['IT_OUT_DIR'] and uses it if the path exists.
    After it exits, verify IT_RUN has content; if not, auto-discover real folder.
    Only run it_recon_engine as fallback if it_suite didn't produce IT_RECONCILIATION.xlsx.
    """
    global IT_RUN
    _banner("STEP 4+5 — IT Portal Download + IT Reconciliation Excel")

    it_script    = SCRIPT_DIR / "it_suite_v6.py"
    recon_script = SCRIPT_DIR / "it_recon_engine.py"

    if not it_script.exists():
        _log("  ✗ it_suite_v6.py not found — skipping", "warning"); return

    # Create tracked folder and pass it to it_suite via env var
    IT_RUN.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["IT_OUT_DIR"] = str(IT_RUN)
    env["FY_LABEL"]   = FY_LABEL
    env["AY_LABEL"]   = AY_LABEL

    _log(f"  ► IT_OUT_DIR = {IT_RUN}")
    _log(f"  ► Browser(s) will open. Enter OTP once per client.")

    try:
        _log("  ► Live download monitor started — you will see files arrive below:")
        _watcher = _start_it_watcher()
        result = subprocess.run(
            [sys.executable, str(it_script)],
            env=env,
            timeout=5400,
            capture_output=False,
        )
        _watcher.stop()
        if result.returncode == 0:
            _log("  ✓ it_suite completed")
        else:
            _log(f"  ✗ it_suite exited {result.returncode}", "warning")
    except subprocess.TimeoutExpired:
        _watcher.stop()
        _log("  ✗ it_suite timed out (90 min)", "warning")
    except Exception as e:
        try: _watcher.stop()
        except Exception: pass
        _log(f"  ✗ it_suite error: {e}", "error")

    # Discover real output folder (handles it_suite ignoring IT_OUT_DIR env var)
    real = _discover_real_it_run()
    if real != IT_RUN:
        _log(f"  ℹ  IT output discovered: {real}")
        IT_RUN = real
    else:
        _log(f"  ℹ  IT output: {IT_RUN}")

    # ── Option B: reorganize IT into per-client folders ──────────────────────
    if _OPTION_B and IT_RUN.exists():
        _log("  Reorganizing IT files into ClientName/IT Download/ ...")
        try:
            reorganize_it_output(BASE_DIR, IT_RUN, clients, _folder_candidates, _log)
            _log("  OK ClientName/GST Automation/ + IT Download/ updated")
        except Exception as _e:
            _log(f"  ⚠  reorganize_it_output error: {_e}", "warning")
        # Clean up hidden staging dir
        try:
            if IT_BASE.exists():
                shutil.rmtree(str(IT_BASE), ignore_errors=True)
                _log("  ✓ IT staging folder cleaned up")
        except Exception as _e:
            _log(f"  ⚠  IT staging cleanup error: {_e}", "warning")

    # ── Step 5: Fallback recon ONLY if it_suite produced NO good IT_RECON at all ─
    # CRITICAL: we check the DISCOVERED IT_RUN (not the stub), so we never
    # run a second recon that overwrites the good file with blank data.
    if not recon_script.exists():
        return

    for client in clients:
        name = client["name"]
        pan  = client["pan"]
        fy   = client["fy"]
        if not pan: continue

        # Find client subfolder under DISCOVERED IT_RUN
        it_dir = IT_RUN / name.replace(" ", "_")
        if not it_dir.exists():
            # fuzzy match: first 6 chars of name
            if IT_RUN.exists():
                for sub in IT_RUN.iterdir():
                    if sub.is_dir() and name.upper()[:6] in sub.name.upper():
                        it_dir = sub; break
            else:
                # Staging folder was cleaned up — look in reorganized client folder
                candidate = BASE_DIR / name.replace(" ", "_") / "IT Download"
                if candidate.exists():
                    it_dir = candidate

        if not it_dir.exists():
            _log(f"    ⚠  IT client folder not found for {name} — skipping fallback recon")
            continue

        # Check size of ALL IT_RECONCILIATION files for this client
        existing = list(it_dir.glob("IT_RECONCILIATION*.xlsx"))
        if any(xl.stat().st_size >= 8_000 for xl in existing):   # it_suite produces ~19KB
            _log(f"    ℹ  Good IT Recon already exists for {name} ({existing[0].stat().st_size//1024} KB) — skipping")
            continue

        # Also check parent folder (in case it_suite put it one level up)
        parent_existing = list(IT_RUN.glob("IT_RECONCILIATION*.xlsx")) if IT_RUN.exists() else []
        if any(xl.stat().st_size >= 8_000 for xl in parent_existing):  # it_suite produces ~19KB
            _log(f"    ℹ  IT Recon exists at run-level for {name} — skipping")
            continue

        _log(f"    → Fallback: running it_recon_engine for {name}")
        gst_xl = _find_gst_excel_for_client(client["gstin"], name, fy)
        # Pass GST folder (parent of Excel) so engine can read turnover
        gst_folder_arg = str(Path(gst_xl).parent) if gst_xl else None
        recon_args = [
            sys.executable, str(recon_script),
            str(it_dir), name, pan,
            ",".join(client["gstin"]) if client["gstin"] else "",
            fy,
        ]
        if gst_folder_arg: recon_args += ["--gst-excel", gst_folder_arg]
        try:
            r = subprocess.run(recon_args, timeout=300, capture_output=False)
            if r.returncode == 0: _log(f"    ✓ IT Recon built for {name}")
            else: _log(f"    ✗ Recon engine failed for {pan}", "warning")
        except Exception as e:
            _log(f"    ✗ Recon error for {pan}: {e}", "error")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6 — MASTER BRIDGE
# ═══════════════════════════════════════════════════════════════════════════════
def run_bridge_step(clients, gst_folder_override=None, it_folder_override=None):
    _banner("STEP 6 — Master Bridge: GST ↔ IT Final Reconciliation")

    global GST_RUN, IT_RUN

    # ── Resolve GST folder ────────────────────────────────────────────────────
    # Priority:
    #   1. Override path (if it actually EXISTS on disk — staging may be deleted)
    #   2. Option B: ClientName/GST Automation/ (always present after reorganize)
    #   3. Discovery fallback
    def _resolve_gst():
        # Override only accepted if path exists (staging cleanup may have deleted it)
        if gst_folder_override:
            _p = Path(gst_folder_override)
            if _p.exists():
                return _p
            _log(f"  ℹ  GST override path gone (staging cleaned) — using Option B", "warning")
        # Option B: scan per-client GST Automation folders
        if clients:
            for cl in clients:
                _g = client_gst(BASE_DIR, cl["name"])
                if _g.exists() and any(_g.glob("*.xlsx")):
                    return _g
        # Also scan BASE_DIR children for any GST Automation with xlsx files
        try:
            for child in sorted(BASE_DIR.iterdir(),
                                 key=lambda d: d.stat().st_mtime, reverse=True):
                _g = child / "GST Automation"
                if _g.is_dir() and any(_g.glob("*.xlsx")):
                    return _g
        except Exception:
            pass
        return _discover_real_gst_run()

    def _resolve_it():
        if it_folder_override:
            _p = Path(it_folder_override)
            if _p.exists():
                return _p
            _log(f"  ℹ  IT override path gone (staging cleaned) — using Option B", "warning")
        if clients:
            for cl in clients:
                _i = client_it(BASE_DIR, cl["name"])
                if _i.exists() and (any(_i.glob("*.xlsx")) or any(_i.glob("*.pdf"))):
                    return _i
        try:
            for child in sorted(BASE_DIR.iterdir(),
                                 key=lambda d: d.stat().st_mtime, reverse=True):
                _i = child / "IT Download"
                if _i.is_dir() and (any(_i.glob("*.xlsx")) or any(_i.glob("*.pdf"))):
                    return _i
        except Exception:
            pass
        return _discover_real_it_run()

    real_gst = _resolve_gst()
    real_it  = _resolve_it()

    # Update globals so Steps 6b/6c/6d also use the correct resolved paths
    if real_gst.exists(): GST_RUN = real_gst
    if real_it.exists():  IT_RUN  = real_it

    gst_arg = str(real_gst)
    it_arg  = str(real_it)
    _log(f"  GST folder → {gst_arg}")
    _log(f"  IT  folder → {it_arg}")

    bridge_script = SCRIPT_DIR / "master_bridge.py"
    if bridge_script.exists():
        try:
            r = subprocess.run(
                [sys.executable, str(bridge_script),
                 "--gst", gst_arg, "--it", it_arg, "--fy", FY_LABEL],
                timeout=600, capture_output=False,
            )
            if r.returncode == 0:
                _log("  ✓ master_bridge.py completed"); return
            _log("  ✗ master_bridge.py failed — running built-in bridge", "warning")
        except Exception as e:
            _log(f"  ✗ master_bridge.py error: {e} — running built-in bridge", "error")

    _log("  Running built-in bridge...")
    _builtin_master_bridge(clients)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6b — GST-IT COMPARISON EXCEL
# ═══════════════════════════════════════════════════════════════════════════════
def run_gst_it_comparison_step():
    _banner("STEP 6b — GST-IT Comparison Excel (TIS / AIS Template)")

    comp_script = SCRIPT_DIR / "build_gst_it_comparison.py"
    # Output goes into each client's Raw Data folder (set per-client below)
    # No separate GST_IT_Comparison folder in Downloads

    def _find_pdf(prefix, client_name=None):
        """Find the most recent PDF with given prefix.
        FIX v10.10: scope search to current client's IT Download folder first
        to avoid picking up stale PDFs from other clients in Downloads."""
        best_path, best_mtime = None, 0

        # Priority 1: current client's IT Download (if client_name given)
        if client_name:
            _it_b = client_it(BASE_DIR, client_name)
            if _it_b.exists():
                for p in _it_b.glob(f"{prefix}*.pdf"):
                    try:
                        mt = p.stat().st_mtime
                        if mt > best_mtime:
                            best_path = p; best_mtime = mt
                    except Exception:
                        pass
            if best_path:
                return best_path

        # Priority 2: IT staging (only current run, not archived runs)
        if IT_RUN.exists():
            for p in IT_RUN.rglob(f"{prefix}*.pdf"):
                try:
                    mt = p.stat().st_mtime
                    if mt > best_mtime:
                        best_path = p; best_mtime = mt
                except Exception:
                    pass
            if best_path:
                return best_path

        # Priority 3: all per-client IT Download folders (only in BASE_DIR children)
        try:
            for child in BASE_DIR.iterdir():
                if not child.is_dir():
                    continue
                it_sub = child / "IT Download"
                if not it_sub.is_dir():
                    continue
                for p in it_sub.glob(f"{prefix}*.pdf"):
                    try:
                        mt = p.stat().st_mtime
                        if mt > best_mtime:
                            best_path = p; best_mtime = mt
                    except Exception:
                        pass
        except Exception:
            pass
        return best_path

    # Determine primary client name for scoped PDF search
    _primary_client = None
    try:
        from folder_structure import client_it as _client_it
        for _ch in sorted(BASE_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if _ch.is_dir() and (_ch / "IT Download").exists():
                _primary_client = _ch.name.replace("_", " ")
                break
    except Exception:
        pass

    tis_path = _find_pdf("TIS", _primary_client)
    ais_path = _find_pdf("AIS", _primary_client)

    gst_folder = None
    # Priority 1: current GST_RUN (already discovered)
    if GST_RUN.exists() and any(GST_RUN.rglob("GSTR2B*.xlsx")):
        gst_folder = str(GST_RUN)
    # Priority 2: any MultiYear_*/AY*/ subdir that has GSTR2B files
    if not gst_folder and GST_BASE.exists():
        for myd in sorted(GST_BASE.glob("MultiYear_*"),
                          key=lambda d: d.stat().st_mtime, reverse=True):
            if not myd.is_dir(): continue
            for sub in sorted(myd.iterdir(),
                              key=lambda x: x.stat().st_mtime, reverse=True):
                if sub.is_dir() and any(sub.rglob("GSTR2B*.xlsx")):
                    gst_folder = str(sub); break
            if gst_folder: break
    # Priority 3: any direct child of GST_BASE with GSTR2B files
    if not gst_folder and GST_BASE.exists():
        for d in sorted(GST_BASE.rglob("GSTR2B*.xlsx"),
                        key=lambda p: p.stat().st_mtime, reverse=True):
            gst_folder = str(d.parent); break

    _log(f"  GST folder : {gst_folder or '(auto)'}")
    _log(f"  TIS PDF    : {tis_path or '(not found)'}")
    _log(f"  AIS PDF    : {ais_path or '(not found)'}")

    if not comp_script.exists():
        _log("  ⚠  build_gst_it_comparison.py not found — skipping", "warning"); return

    # Route output into each client's Raw Data folder (or BASE_DIR if no clients)
    _did_any = False
    for child in sorted(BASE_DIR.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
        if not child.is_dir(): continue
        # Only process client folders (they have GST Automation or IT Download)
        if not ((child / "GST Automation").exists() or (child / "IT Download").exists()):
            continue
        out_dir = child / "GST IT Comparison"
        out_dir.mkdir(parents=True, exist_ok=True)
        cmd = [sys.executable, str(comp_script), "--out", str(out_dir), "--fy", FY_LABEL]
        if gst_folder: cmd += ["--gst-folder", gst_folder]
        if tis_path:   cmd += ["--tis-pdf",    str(tis_path)]
        if ais_path:   cmd += ["--ais-pdf",    str(ais_path)]
        try:
            r = subprocess.run(cmd, timeout=180, capture_output=False)
            if r.returncode == 0:
                _log(f"  ✓ GST-IT Comparison built → {out_dir}")
                _did_any = True
        except Exception as e:
            _log(f"  ✗ GST-IT Comparison error: {e}", "error")
        break  # run once; the script uses all clients internally

    if not _did_any:
        # Fallback: single output at BASE_DIR level
        out_dir = BASE_DIR
        cmd = [sys.executable, str(comp_script), "--out", str(out_dir), "--fy", FY_LABEL]
        if gst_folder: cmd += ["--gst-folder", gst_folder]
        if tis_path:   cmd += ["--tis-pdf",    str(tis_path)]
        if ais_path:   cmd += ["--ais-pdf",    str(ais_path)]
        try:
            r = subprocess.run(cmd, timeout=180, capture_output=False)
            if r.returncode == 0: _log(f"  ✓ GST-IT Comparison built → {out_dir}")
            else: _log("  ✗ build_gst_it_comparison.py failed", "warning")
        except Exception as e:
            _log(f"  ✗ GST-IT Comparison error: {e}", "error")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6c — GSTR-2B CONSOLIDATED EXTRACTOR
# ═══════════════════════════════════════════════════════════════════════════════
def run_gstr2b_extractor_step(clients):
    _banner("STEP 6c — GSTR-2B Consolidated Extractor")

    extractor = SCRIPT_DIR / "gstr2b_extractor_v2.py"
    if not extractor.exists():
        _log("  ⚠  gstr2b_extractor_v2.py not found — skipping", "warning"); return

    processed = 0
    seen_2b_folders = set()  # prevent running extractor twice on same folder
                             # (client with multiple GSTINs stored in one ClientName/ folder)
    for client in clients:
        name   = client["name"]
        gstins = client["gstin"]
        fy     = client["fy"]

        for gstin in gstins:
            if not gstin: continue

            # Locate GSTR-2B files for this GSTIN
            gstin_dir = None

            # Option B: ClientName/GST Automation/ first
            _gst_b = client_gst(BASE_DIR, name)
            if _gst_b.exists() and list(_gst_b.glob("GSTR2B_*.xlsx")):
                gstin_dir = _gst_b

            if not gstin_dir:
                # Legacy: GST_RUN/GSTIN/
                for _cand in _folder_candidates(name, gstin):
                    _d = GST_RUN / _cand
                    if _d.exists() and list(_d.glob("GSTR2B_*.xlsx")):
                        gstin_dir = _d; break

            if not gstin_dir:
                for run_dir in _iter_gst_run_dirs():
                    for _cand in _folder_candidates(name, gstin):
                        d = run_dir / _cand
                        if d.exists() and list(d.glob("GSTR2B_*.xlsx")):
                            gstin_dir = d; break
                    if not gstin_dir:
                        try:
                            for sub in run_dir.iterdir():
                                if sub.is_dir() and list(sub.glob("GSTR2B_*.xlsx")):
                                    gstin_dir = sub; break
                        except PermissionError:
                            pass
                    if gstin_dir: break

            if not gstin_dir:
                _log(f"    ⚠  No GSTR2B_*.xlsx found for {name} ({gstin}) — skipping")
                continue

            # Skip if we already ran the extractor on this physical folder
            folder_key = str(gstin_dir.resolve())
            if folder_key in seen_2b_folders:
                _log(f"    ℹ  {gstin}: folder already processed ({gstin_dir.name}) — skipping duplicate")
                continue
            seen_2b_folders.add(folder_key)

            # Name the output after the client (not GSTIN) since it's per-folder
            safe_client = _safe_folder(name, gstin)
            # Write to ClientName/Raw Data/ if it exists, else alongside source
            gst_dir = client_gst(BASE_DIR, name)
            out_dir = gst_dir if gst_dir.exists() else gstin_dir
            out_xl = out_dir / f"GSTR2B_Consolidated_Analysis_{safe_client}.xlsx"
            try:
                r = subprocess.run(
                    [sys.executable, str(extractor),
                     "--input", str(gstin_dir), "--output", str(out_xl)],
                    timeout=300, capture_output=False,
                )
                if r.returncode == 0:
                    _log(f"    ✓ 2B Consolidated: {name} ({gstin}) → {out_xl.name}")
                    processed += 1
                else:
                    _log(f"    ✗ Extractor failed for {gstin}", "warning")
            except Exception as e:
                _log(f"    ✗ Extractor error for {gstin}: {e}", "error")

    _log(f"  Step 6c done — {processed} file(s) built")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 6d — GSTR-1 vs 26AS COMPARISON
# ═══════════════════════════════════════════════════════════════════════════════
def run_gstr1_26as_step(clients):
    _banner("STEP 6d — GSTR-1 vs 26AS Comparison")

    comp_script = SCRIPT_DIR / "gstr1_26as_comparison_v2.py"
    if not comp_script.exists():
        _log("  ⚠  gstr1_26as_comparison_v2.py not found — skipping", "warning")
        return

    processed = 0

    # All roots where GSTR1_FY_*.xlsx or 26AS*.pdf may live
    # Option B: per-client Raw Data folders come first
    _option_b_gst_roots = []
    _option_b_it_roots  = []
    try:
        for child in BASE_DIR.iterdir():
            if child.is_dir():
                g = child / "GST Automation"
                i = child / "IT Download"
                if g.exists(): _option_b_gst_roots.append(g)
                if i.exists(): _option_b_it_roots.append(i)
    except Exception:
        pass

    # Search GST Automation first (GSTR1_FY files), then IT Download (26AS PDFs)
    _extra_roots = _option_b_gst_roots + _option_b_it_roots + [
        BASE_DIR / "26as",
        BASE_DIR / "GST_IT_Bridge",
        BASE_DIR / "GST_IT_Comparison",
        BASE_DIR / "26AS_GSTR1_Compare",
        GST_RUN,
        IT_RUN,
    ]

    for client in clients:
        name   = client["name"]
        pan    = client.get("pan", "")
        gstins = client.get("gstin", [])
        fy     = client.get("fy", FY_LABEL)

        for gstin in gstins:
            if not gstin:
                continue

            client_dir = None

            # ── Pass 0 (Option B): use client root — has GST Automation + IT Download ─
            # Passing the root means the comparison script can search both subfolders
            _ob_root = client_root(BASE_DIR, name)
            if _ob_root.exists() and (
                any((_ob_root / "GST Automation").glob("GSTR1_FY_*.xlsx")) or
                any((_ob_root / "IT Download").glob("26AS*.pdf")) or
                any((_ob_root / "IT Download").glob("*26AS*.pdf"))
            ):
                client_dir = _ob_root

            if not client_dir:
                # ── Pass 1: GST Automation folder directly ──────────────────────
                _gst_b = client_gst(BASE_DIR, name)
                if _gst_b.exists() and any(_gst_b.glob("GSTR1_FY_*.xlsx")):
                    client_dir = _gst_b

            if not client_dir:
                # ── Pass 2: GST run dirs — look for GSTR1_FY_*.xlsx ─────────────
                for run_dir in _iter_gst_run_dirs():
                    for cand in _folder_candidates(name, gstin):
                        d = run_dir / cand
                        if d.exists() and any(d.glob("GSTR1_FY_*.xlsx")):
                            client_dir = d; break
                    if client_dir:
                        break

            if not client_dir:
                # ── Pass 3: extra roots — look for GSTR1_FY_*.xlsx ──────────────
                for root in _extra_roots:
                    if not root or not root.exists():
                        continue
                    for cand in _folder_candidates(name, gstin):
                        d = root / cand
                        if d.exists() and any(d.glob("GSTR1_FY_*.xlsx")):
                            client_dir = d; break
                    if not client_dir and any(root.glob("GSTR1_FY_*.xlsx")):
                        client_dir = root   # single-client flat layout
                    if client_dir:
                        break

            # ── Pass 4: fallback — any folder with a 26AS PDF ────────────────
            if not client_dir:
                for root in list(_iter_gst_run_dirs()) + _extra_roots:
                    if not root or not root.exists():
                        continue
                    for cand in _folder_candidates(name, gstin):
                        d = root / cand
                        if d.exists() and (
                            any(d.glob("26AS*.pdf")) or
                            any(d.glob("*26as*.pdf")) or
                            any(d.glob("*26AS*.pdf"))
                        ):
                            client_dir = d; break
                    if client_dir:
                        break

            if not client_dir:
                _log(
                    f"    ⚠  No GSTR1_FY_*.xlsx or 26AS PDF found for "
                    f"{name} ({gstin}) — skipping 26AS comparison"
                )
                continue

            # Always write output to ClientName/26AS vs GSTR1/ if it exists
            _26as_dir = client_26as(BASE_DIR, name)
            _26as_dir.mkdir(parents=True, exist_ok=True)
            out_path = _26as_dir / f"26AS_GSTR1_Compare_{fy.replace('/', '-')}.xlsx"
            try:
                cmd = [
                    sys.executable, str(comp_script),
                    "--folder", str(client_dir),
                    "--name",   name,
                    "--gstin",  gstin,
                    "--pan",    pan or "",
                    "--fy",     fy,
                    "--out",    str(out_path),
                ]
                r = subprocess.run(cmd, timeout=300, capture_output=False)
                if r.returncode == 0:
                    _log(f"    ✓ 26AS vs GSTR-1: {name} ({gstin}) → {out_path.name}")
                    processed += 1
                else:
                    _log(f"    ✗ 26AS comparison failed for {gstin}", "warning")
            except Exception as e:
                _log(f"    ✗ 26AS comparison error for {gstin}: {e}", "error")

    _log(f"  Step 6d done — {processed} file(s) built")


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 7 — FINAL CONSOLIDATED REPORT
# ═══════════════════════════════════════════════════════════════════════════════
def _run_final_consolidated():
    _banner("STEP 7 — Final Consolidated 7-Sheet Report")
    cons_script = SCRIPT_DIR / "build_final_consolidated.py"
    if not cons_script.exists():
        _log("  ⚠  build_final_consolidated.py not found — skipping", "warning")
        return
    # Pass BASE_DIR so the script scans all ClientName/ subfolders.
    # Also pass explicit --out path so it writes to the known location.
    out_path = BASE_DIR / f"FINAL_CONSOLIDATED_REPORT_{RUN_TS}.xlsx"
    try:
        r = subprocess.run(
            [sys.executable, str(cons_script),
             "--base", str(BASE_DIR),
             "--out",  str(out_path)],
            timeout=300, capture_output=False,
        )
        if r.returncode == 0:
            _log(f"  ✓ Final consolidated report → {out_path}")
        else:
            _log("  ✗ Final consolidated report failed", "warning")
    except Exception as e:
        _log(f"  ✗ Final consolidated report error: {e}", "error")


# ═══════════════════════════════════════════════════════════════════════════════
# BUILT-IN BRIDGE (fallback)
# ═══════════════════════════════════════════════════════════════════════════════
def _builtin_master_bridge(clients):
    # Route bridge output into first client's IT Bridge folder (or BASE_DIR)
    _bridge_out_dir = BASE_DIR
    try:
        for _bc in clients:
            _bd = client_bridge(BASE_DIR, _bc["name"])
            _bd.mkdir(parents=True, exist_ok=True)
            _bridge_out_dir = _bd
            break
    except Exception:
        pass
    out_xl = _bridge_out_dir / f"Master_Reconciliation_{FY_LABEL}_{RUN_TS}.xlsx"
    wb = Workbook()
    _build_dashboard(wb, clients)
    for client in clients:
        gst_data = _merge_gst_data(client["gstin"], client["fy"])
        it_data  = _read_it_recon(client["pan"], client["name"])
        _build_company_sheet(wb, client, gst_data, it_data)
    wb.save(out_xl)
    _log(f"\n  ✓ MASTER OUTPUT: {out_xl}")
    return out_xl


def _merge_gst_data(gstins, fy):
    merged = {"annual_turnover":0.0,"annual_purchase":0.0,"monthly":{},"gstr2b_itc":[]}
    FY_MON = {"APRIL":"APR","MAY":"MAY","JUNE":"JUN","JULY":"JUL","AUGUST":"AUG",
              "SEPTEMBER":"SEP","OCTOBER":"OCT","NOVEMBER":"NOV","DECEMBER":"DEC",
              "JANUARY":"JAN","FEBRUARY":"FEB","MARCH":"MAR"}

    for gstin in gstins:
        # Find GSTIN folder
        gstin_dir = None
        if (GST_RUN / gstin).exists():
            gstin_dir = GST_RUN / gstin
        else:
            for run_dir in _iter_gst_run_dirs():
                d = run_dir / gstin
                if d.exists(): gstin_dir = d; break
                for sub in run_dir.iterdir():
                    if sub.is_dir() and sub.name.upper().startswith(gstin[:6].upper()):
                        gstin_dir = sub; break
                if gstin_dir: break

        if not gstin_dir or not gstin_dir.exists(): continue

        for xl in gstin_dir.glob("*.xlsx"):
            if "IT_RECONCILIATION" in xl.name.upper(): continue
            try: xf = pd.ExcelFile(xl, engine="openpyxl")
            except: continue

            for sn in xf.sheet_names:
                sn_up = sn.strip().upper()
                mon_abbr = next((abbr for full,abbr in FY_MON.items()
                                 if full in sn_up or sn_up.startswith(abbr)), None)
                if not mon_abbr: continue
                try: df = xf.parse(sn, header=None, dtype=str).fillna("")
                except: continue

                for _, row in df.iterrows():
                    label = str(row.iloc[0]).lower().strip()
                    nums = []
                    for v in row.iloc[1:]:
                        try: nums.append(float(str(v).replace(",","")))
                        except: pass
                    if not nums: continue
                    m = merged["monthly"].setdefault(mon_abbr, {"r1":0.0,"r1a":0.0,"r3b":0.0})
                    if any(k in label for k in ["gstr-1 + gstr-1a","tot_r1_incl","grand total r1"]):
                        m["r1a"] = max(m["r1a"], abs(nums[0]))
                    elif any(k in label for k in ["total from gstr-1","grand total","tot_r1"]):
                        m["r1"] = max(m["r1"], abs(nums[0]))
                    elif any(k in label for k in ["3b","outward supplies"]):
                        m["r3b"] = max(m["r3b"], abs(nums[0]))

            ann_sn = next((s for s in xf.sheet_names if "annual" in s.lower()), None)
            if ann_sn:
                try:
                    adf = xf.parse(ann_sn, header=None, dtype=str).fillna("")
                    for _, row in adf.iterrows():
                        label = str(row.iloc[0]).lower()
                        nums = []
                        for v in row.iloc[1:]:
                            try:
                                nums.append(float(str(v).replace(",","")))
                            except (ValueError, TypeError):
                                pass
                        if nums and any(k in label for k in ["total taxable","grand total","tot_r1"]):
                            merged["annual_turnover"] += abs(nums[0]); break
                except: pass
    return merged


def _read_it_recon(pan, name):
    result = {"tis_turnover":0.0,"ais_purchase":0.0,"tds_total":0.0,
              "advance_tax":0.0,"monthly_ais":{},"source":""}

    # Option B: ClientName/IT Download/ first
    _it_option_b = client_it(BASE_DIR, name)
    if _it_option_b.exists() and any(_it_option_b.glob("IT_RECONCILIATION*.xlsx")):
        client_dir = _it_option_b
    else:
        # Search legacy staging folder name variants
        _it_cands = _folder_candidates(name, "")
        client_dir = None
        for _cand in _it_cands:
            _d = IT_RUN / _cand
            if _d.exists(): client_dir = _d; break
        if client_dir is None:
            client_dir = IT_RUN / name.replace(" ","_")

    if not client_dir.exists() and IT_BASE.exists():
        for run_dir in sorted(IT_BASE.iterdir(), key=lambda d: d.stat().st_mtime, reverse=True):
            if not run_dir.is_dir(): continue
            for _cand in _it_cands:
                cand = run_dir / _cand
                if cand.exists(): client_dir = cand; break
            if client_dir.exists(): break
            for sub in run_dir.iterdir():
                if sub.is_dir() and name.upper()[:6] in sub.name.upper():
                    client_dir = sub; break
            if client_dir.exists(): break

    if not client_dir.exists(): return result

    for xl in client_dir.glob("IT_RECONCILIATION*.xlsx"):
        try:
            xf = pd.ExcelFile(xl, engine="openpyxl"); result["source"] = xl.name
            for sn in xf.sheet_names:
                sn_up = sn.upper()
                if "IT_SUMMARY" in sn_up or "SUMMARY" in sn_up:
                    df = xf.parse(sn, header=None, dtype=str).fillna("")
                    for _, row in df.iterrows():
                        label = str(row.iloc[0]).lower()
                        nums = []
                        for v in row.iloc[1:]:
                            try:
                                nums.append(float(str(v).replace(",","")))
                            except (ValueError, TypeError):
                                pass
                        if not nums: continue
                        if "tis" in label and "turnover" in label: result["tis_turnover"] = abs(nums[0])
                        elif "ais" in label and "purchase" in label: result["ais_purchase"] = abs(nums[0])
                        elif "tds" in label and "total" in label: result["tds_total"] = abs(nums[0])
                        elif "advance" in label and "tax" in label: result["advance_tax"] = abs(nums[0])
                elif "MONTHLY" in sn_up:
                    df = xf.parse(sn, header=None, dtype=str).fillna("")
                    for _, row in df.iterrows():
                        lbl = str(row.iloc[0]).upper().strip()
                        if lbl[:3] in FY_MONTHS:
                            nums = []
                            for v in row.iloc[1:]:
                                try:
                                    nums.append(float(str(v).replace(",","")))
                                except (ValueError, TypeError):
                                    pass
                            if nums: result["monthly_ais"][lbl[:3]] = abs(nums[0])
            break
        except Exception as e:
            _log(f"    ⚠  IT Recon read error: {e}", "warning")
    return result


# ─── Excel helpers ────────────────────────────────────────────────────────────
def _f(h):  return PatternFill("solid", fgColor=h)
def _fn(b=False,c="000000",s=9): return Font(name="Arial",bold=b,color=c,size=s)
def _bd():
    x=Side(style="thin"); return Border(left=x,right=x,top=x,bottom=x)
def _al(h="left",w=False): return Alignment(horizontal=h,vertical="center",wrap_text=w)
def _c(ws,r,col,v,bg=ALT1,bold=False,fg="000000",align="left",numfmt=None,size=9):
    cell=ws.cell(row=r,column=col,value=v)
    cell.font=_fn(bold,fg,size); cell.fill=_f(bg); cell.alignment=_al(align); cell.border=_bd()
    if numfmt and isinstance(v,(int,float)): cell.number_format=numfmt
    elif isinstance(v,(int,float)): cell.number_format=NUM_FMT
    return cell


def _build_dashboard(wb, clients):
    ws=wb.active; ws.title="Dashboard"
    ws.merge_cells("A1:I1")
    ws["A1"].value=f"GST ↔ Income Tax Master Reconciliation — FY {FY_LABEL}"
    ws["A1"].font=_fn(True,"FFFFFF",12); ws["A1"].fill=_f(DARK_BLUE)
    ws["A1"].alignment=_al("center"); ws["A1"].border=_bd(); ws.row_dimensions[1].height=28
    hdrs=[("Client Name",22),("PAN",14),("GSTINs",20),("FY",10),
          ("GST Turnover",16),("IT TIS Turnover",16),("Diff",14),("GST→IT Match",12),("Status",10)]
    for ci,(h,w) in enumerate(hdrs,1):
        c=ws.cell(row=2,column=ci,value=h)
        c.font=_fn(True,"FFFFFF",9); c.fill=_f(HDR_BG); c.alignment=_al("center"); c.border=_bd()
        ws.column_dimensions[get_column_letter(ci)].width=w
    ws.row_dimensions[2].height=18


def _build_company_sheet(wb, client, gst_data, it_data):
    name=client["name"][:28]; pan=client["pan"]; gstins=client["gstin"]; fy=client["fy"]
    ws=wb.create_sheet(re.sub(r"[\\/*?:\[\]]","",name)[:31])
    ws.merge_cells("A1:L1")
    ws["A1"].value=f"{name} — GST ↔ IT Reconciliation  FY {fy}"
    ws["A1"].font=_fn(True,"FFFFFF",11); ws["A1"].fill=_f(DARK_BLUE)
    ws["A1"].alignment=_al("center"); ws["A1"].border=_bd(); ws.row_dimensions[1].height=26
    ri=2
    for ci,(h,v) in enumerate([("PAN",pan),("GSTINs"," / ".join(gstins)),
                                ("FY",fy),("IT Source",it_data.get("source","—"))],1):
        ws.cell(row=ri,column=ci*2-1,value=h).font=_fn(True)
        ws.cell(row=ri,column=ci*2,value=v)
    ri+=1
    ws.merge_cells(f"A{ri}:L{ri}")
    ws.cell(row=ri,column=1,value="ANNUAL SUMMARY").font=_fn(True,"FFFFFF",9)
    ws.cell(row=ri,column=1).fill=_f(MED_BLUE); ws.cell(row=ri,column=1).border=_bd(); ri+=1
    ann=[("GST GSTR-1 Turnover (B)",gst_data["annual_turnover"]),
         ("IT TIS Turnover (A)",it_data["tis_turnover"]),
         ("Difference (A−B)",round(it_data["tis_turnover"]-gst_data["annual_turnover"],2)),
         ("GST Purchases (GSTR-2B ITC)",gst_data["annual_purchase"]),
         ("IT AIS Purchases",it_data["ais_purchase"]),
         ("Purchase Difference",round(it_data["ais_purchase"]-gst_data["annual_purchase"],2)),
         ("TDS (26AS Total)",it_data["tds_total"]),
         ("Advance Tax",it_data["advance_tax"])]
    for label,val in ann:
        _c(ws,ri,1,label,bold=True); _c(ws,ri,2,val,align="right",numfmt=NUM_FMT)
        if "Diff" in label or "ifference" in label:
            ws.cell(row=ri,column=2).fill=_f(GREEN_BG if abs(val)<VARIANCE_THRESHOLD else RED_BG)
        ri+=1
    ri+=1
    ws.merge_cells(f"A{ri}:L{ri}")
    ws.cell(row=ri,column=1,value="MONTH-WISE COMPARISON").font=_fn(True,"FFFFFF",9)
    ws.cell(row=ri,column=1).fill=_f(MED_BLUE); ws.cell(row=ri,column=1).border=_bd(); ri+=1
    mh=[("Month",8),("GSTR-1 Taxable",16),("GSTR-1A Combined",16),("GSTR-3B Filed",16),
        ("AIS/TIS Turnover",16),("GST vs AIS Diff",16),("GST vs 3B Diff",16),("Status",10)]
    for ci,(h,w) in enumerate(mh,1):
        c=ws.cell(row=ri,column=ci,value=h)
        c.font=_fn(True,"FFFFFF",9); c.fill=_f(HDR_BG); c.alignment=_al("center"); c.border=_bd()
        ws.column_dimensions[get_column_letter(ci)].width=w
    ri+=1
    r1t=r1at=r3bt=aist=0.0
    for mon in FY_MONTHS:
        m=gst_data["monthly"].get(mon,{})
        r1=m.get("r1",0.0); r1a=m.get("r1a",0.0); r3b=m.get("r3b",0.0)
        ais=it_data["monthly_ais"].get(mon,0.0)
        d1=round(r1a-ais,2); d2=round(r1a-r3b,2)
        st="OK"; fbg=GREEN_BG
        if abs(d1)>VARIANCE_THRESHOLD or abs(d2)>VARIANCE_THRESHOLD: st="CHECK ⚠"; fbg=RED_BG
        elif abs(d1)>0 or abs(d2)>0: st="REVIEW"; fbg=YELLOW_BG
        rb=ALT1 if FY_MONTHS.index(mon)%2==0 else ALT2
        _c(ws,ri,1,mon,bg=rb,bold=True); _c(ws,ri,2,r1,bg=rb,align="right",numfmt=NUM_FMT)
        _c(ws,ri,3,r1a,bg=rb,align="right",numfmt=NUM_FMT); _c(ws,ri,4,r3b,bg=rb,align="right",numfmt=NUM_FMT)
        _c(ws,ri,5,ais,bg=rb,align="right",numfmt=NUM_FMT)
        _c(ws,ri,6,d1,bg=RED_BG if abs(d1)>VARIANCE_THRESHOLD else rb,align="right",numfmt=NUM_FMT)
        _c(ws,ri,7,d2,bg=RED_BG if abs(d2)>VARIANCE_THRESHOLD else rb,align="right",numfmt=NUM_FMT)
        _c(ws,ri,8,st,bg=fbg,bold=(st!="OK"),fg=RED_FG if "CHECK" in st else ("9C6500" if st=="REVIEW" else GREEN_FG))
        r1t+=r1; r1at+=r1a; r3bt+=r3b; aist+=ais; ri+=1
    tots=["TOTAL",r1t,r1at,r3bt,aist,round(r1at-aist,2),round(r1at-r3bt,2),
          "OK" if abs(r1at-aist)<VARIANCE_THRESHOLD else "CHECK ⚠"]
    for ci,v in enumerate(tots,1):
        c=ws.cell(row=ri,column=ci,value=v); c.font=_fn(True); c.fill=_f(TOT_BG); c.border=_bd()
        if isinstance(v,float): c.number_format=NUM_FMT; c.alignment=_al("right")
    ri+=2
    ws.merge_cells(f"A{ri}:L{ri}")
    ws.cell(row=ri,column=1,value="ITC vs PURCHASE RECONCILIATION").font=_fn(True,"FFFFFF",9)
    ws.cell(row=ri,column=1).fill=_f(MED_BLUE); ws.cell(row=ri,column=1).border=_bd(); ri+=1
    _c(ws,ri,1,"GSTR-2B ITC Claimed (GST)",bold=True)
    _c(ws,ri,2,gst_data.get("annual_purchase",0.0),align="right",numfmt=NUM_FMT); ri+=1
    _c(ws,ri,1,"AIS Purchases (IT Portal)",bold=True)
    _c(ws,ri,2,it_data.get("ais_purchase",0.0),align="right",numfmt=NUM_FMT); ri+=1
    diff_itc=round(gst_data.get("annual_purchase",0.0)-it_data.get("ais_purchase",0.0),2)
    _c(ws,ri,1,"Difference",bold=True)
    _c(ws,ri,2,diff_itc,align="right",numfmt=NUM_FMT,
       bg=GREEN_BG if abs(diff_itc)<VARIANCE_THRESHOLD else RED_BG)


# ═══════════════════════════════════════════════════════════════════════════════
# OFFLINE FOLDER PICKER
# ═══════════════════════════════════════════════════════════════════════════════
def _pick_existing_folder(base_path, label, expand_inner=False):
    """
    Show a numbered list of run folders.

    expand_inner=True  (GST): MultiYear_* folders are expanded to show
                               their AY* sub-folders — user picks the FY folder.
    expand_inner=False (IT):  AY* folders shown as-is — bridge needs the
                               AY* parent, NOT client children inside it.
    """
    base = Path(base_path)
    if not base.exists():
        print(f"  ✗ Base folder not found: {base}"); return None

    raw = sorted([d for d in base.iterdir() if d.is_dir()],
                 key=lambda d: d.stat().st_mtime, reverse=True)
    all_folders = []
    for f in raw:
        if expand_inner and f.name.startswith("MultiYear"):
            subs = sorted([s for s in f.iterdir() if s.is_dir()],
                          key=lambda s: s.stat().st_mtime, reverse=True)
            if subs: all_folders.extend(subs); continue
        all_folders.append(f)

    if not all_folders:
        print(f"  ✗ No folders found inside {base}"); return None

    print(f"\n  {label} — choose a folder:")
    for i, f in enumerate(all_folders, 1):
        mtime = datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        try:    display = str(f.relative_to(base))
        except: display = f.name
        print(f"    {i:2d}.  {display}   [{mtime}]")
    try:    recent = str(all_folders[0].relative_to(base))
    except: recent = all_folders[0].name
    print(f"     0.  Use most recent ({recent})")

    while True:
        raw_in = input("  Enter number (or 0 for most recent): ").strip()
        if raw_in == "0": return all_folders[0]
        try:
            idx = int(raw_in) - 1
            if 0 <= idx < len(all_folders): return all_folders[idx]
        except ValueError: pass
        print("  Invalid choice. Try again.")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    global log, GST_RUN, IT_RUN, FY_LABEL, AY_LABEL, _fy_yr

    parser = argparse.ArgumentParser(description="Run ALL — GST + IT full pipeline v10.12")
    parser.add_argument("--show-structure", action="store_true",
                        help="Print Option B folder layout and exit")
    parser.add_argument("--skip-tally",  action="store_true",
                        help="Skip Step 1 Tally extract (use when Tally is not available)")
    parser.add_argument("--only-gst",    action="store_true", help="GST steps 2-3 only")
    parser.add_argument("--only-it",     action="store_true", help="IT steps 4-5 only")
    parser.add_argument("--only-bridge", action="store_true", help="Bridge steps 6-7 only")
    parser.add_argument("--offline",     action="store_true",
                        help="Pick existing folders, run bridge (no portal downloads)")
    parser.add_argument("--gst-folder",  default=None, help="Explicit GST run folder")
    parser.add_argument("--it-folder",   default=None, help="Explicit IT run folder")
    parser.add_argument("--fy",          default=None, help="Override FY (e.g. 2024-25)")
    parser.add_argument("--client",      default=None, help="Process one client by name")
    args = parser.parse_args()

    if getattr(args, "show_structure", False):
        try:
            clients_tmp = load_clients()
            print_structure(BASE_DIR, clients_tmp)
        except Exception as e:
            print(f"  (Could not load clients: {e})")
            print_structure(BASE_DIR, [])
        return

    if args.fy:
        FY_LABEL = args.fy
        _fy_yr   = int(FY_LABEL.split("-")[0])
        AY_LABEL = f"{_fy_yr+1}-{str(_fy_yr+2)[2:]}"
        GST_RUN  = GST_BASE / f"FY{FY_LABEL}_{RUN_TS}"
        IT_RUN   = IT_BASE  / f"AY{AY_LABEL}_{RUN_TS}"

    log = _setup_log()

    # ── OFFLINE / BRIDGE-ONLY ────────────────────────────────────────────────
    if args.offline or args.only_bridge:
        _banner(f"RUN ALL v10.12 — OFFLINE MODE  FY {FY_LABEL}")
        _log("  No portal downloads. All steps work from existing files on disk.")
        _log(f"  Script dir : {SCRIPT_DIR}")
        _log(f"  Log file   : {LOG_FILE}")
        _log("")

        # ── OFFLINE MENU ──────────────────────────────────────────────────────
        print("""
╔══════════════════════════════════════════════════════════════════╗
║         OFFLINE MODE — SELECT WHAT TO RUN                       ║
╠══════════════════════════════════════════════════════════════════╣
║  INDIVIDUAL STEPS                                               ║
║  [1]  Step 1   — Tally GST Extract  (build CustomerMaster.xlsx) ║
║                  (Tally must be open — not a portal download)   ║
║  [2]  Step 2+3 — GST Reconciliation Excel from existing files   ║
║                  (Option 11 inside GST suite — no browser)      ║
║  [3]  Step 4+5 — IT Recon Excel from existing PDFs             ║
║                  (26AS + AIS + TIS already on disk)             ║
║  [4]  Step 6   — Master Bridge (GST ↔ IT reconciliation)       ║
║  [5]  Step 6b  — GST-IT Comparison Excel (TIS/AIS template)    ║
║  [6]  Step 6c  — GSTR-2B Consolidated Extractor               ║
║  [7]  Step 6d  — GSTR-1 vs 26AS Comparison                    ║
║  [8]  Step 7   — Final Consolidated 7-Sheet Report             ║
╠══════════════════════════════════════════════════════════════════╣
║  COMBO OPTIONS                                                  ║
║  [9]  Steps 6→7  — Bridge + Comparison + 2B + 26AS + Final     ║
║                    (most common offline use — pick folders)     ║
║  [10] Steps 3→7  — IT Recon + Bridge + all report steps        ║
║  [11] Steps 2→7  — GST Recon + IT Recon + Bridge + Reports     ║
║  [12] ALL STEPS  — 1+2+3+4+5+6+6b+6c+6d+7 (full offline run)  ║
╚══════════════════════════════════════════════════════════════════╝""")

        while True:
            raw = input("\n  Enter choice [1-12]: ").strip()
            if raw.isdigit() and 1 <= int(raw) <= 12:
                offline_choice = int(raw); break
            print("  Invalid choice — enter a number from 1 to 12.")

        # ── helper: pick GST + IT folders (shared by steps that need them) ──
        def _pick_gst_it_folders(need_gst=True, need_it=True):
            global GST_RUN, IT_RUN
            gst_ok = it_ok = True

            if need_gst:
                if args.gst_folder:
                    gst_offline = Path(args.gst_folder)
                else:
                    print("\n  ─── SELECT GST FOLDER ───")
                    print("  Tip: ClientName/GST Automation/ OR the old MultiYear_*/AY*/ staging folder")
                    # Try Option-B per-client folders first, then staging
                    _gst_candidates = []
                    try:
                        for _ch in sorted(BASE_DIR.iterdir()):
                            _gd = _ch / "GST Automation"
                            if _gd.exists(): _gst_candidates.append(_gd)
                    except Exception: pass
                    if _gst_candidates:
                        print("\n  Per-client GST Automation folders found:")
                        for _i, _p in enumerate(_gst_candidates, 1):
                            try: _disp = str(_p.relative_to(BASE_DIR))
                            except: _disp = _p.name
                            print(f"    {_i:2d}.  {_disp}")
                        print(f"     0.  Browse staging folders (._gst_stage_)")
                        _raw = input("  Enter number (0 to browse staging): ").strip()
                        if _raw == "0":
                            gst_offline = _pick_existing_folder(GST_BASE, "GST folder", expand_inner=True)
                        elif _raw.isdigit() and 1 <= int(_raw) <= len(_gst_candidates):
                            gst_offline = _gst_candidates[int(_raw)-1]
                        else:
                            gst_offline = _gst_candidates[0]
                    else:
                        gst_offline = _pick_existing_folder(GST_BASE, "GST folder", expand_inner=True)

                if not gst_offline or not gst_offline.exists():
                    _log(f"  ✗ GST folder not found: {gst_offline}", "error"); gst_ok = False
                else:
                    GST_RUN = gst_offline
                    _log(f"  GST folder : {gst_offline}")

            if need_it:
                if args.it_folder:
                    it_offline = Path(args.it_folder)
                else:
                    print("\n  ─── SELECT IT FOLDER ───")
                    print("  Tip: ClientName/IT Download/ OR the old ._it_stage_/AY*/ staging folder")
                    _it_candidates = []
                    try:
                        for _ch in sorted(BASE_DIR.iterdir()):
                            _id = _ch / "IT Download"
                            if _id.exists(): _it_candidates.append(_id)
                    except Exception: pass
                    if _it_candidates:
                        print("\n  Per-client IT Download folders found:")
                        for _i, _p in enumerate(_it_candidates, 1):
                            try: _disp = str(_p.relative_to(BASE_DIR))
                            except: _disp = _p.name
                            print(f"    {_i:2d}.  {_disp}")
                        print(f"     0.  Browse staging folders (._it_stage_)")
                        _raw = input("  Enter number (0 to browse staging): ").strip()
                        if _raw == "0":
                            it_offline = _pick_existing_folder(IT_BASE, "IT folder", expand_inner=False)
                        elif _raw.isdigit() and 1 <= int(_raw) <= len(_it_candidates):
                            it_offline = _it_candidates[int(_raw)-1]
                        else:
                            it_offline = _it_candidates[0]
                    else:
                        it_offline = _pick_existing_folder(IT_BASE, "IT folder", expand_inner=False)

                if not it_offline or not it_offline.exists():
                    _log(f"  ✗ IT folder not found: {it_offline}", "error"); it_ok = False
                else:
                    # Guard: if user picked a client subfolder, go up one level
                    _it_markers = ["IT_RECONCILIATION*.xlsx","26AS_*.pdf","AIS_*.pdf","TIS_*.pdf"]
                    if any(list(it_offline.glob(pat)) for pat in _it_markers):
                        _log(f"  ℹ  IT folder looks like client subfolder — using parent: {it_offline.parent}")
                        it_offline = it_offline.parent
                    IT_RUN = it_offline
                    _log(f"  IT  folder : {it_offline}")

            return gst_ok, it_ok

        # ── helper: run GST suite in offline/Option-11 mode ─────────────────
        def _run_gst_offline():
            """Launch gst_suite with GST_OFFLINE=11 env var so it auto-picks Option 11."""
            gst_script = SCRIPT_DIR / "gst_suite_v32.py"
            if not gst_script.exists():
                _log("  ✗ gst_suite_v32.py not found — skipping", "warning"); return
            _log("  Launching GST Suite — Offline / Option 11 (no browser, existing files)...")
            _log("  ► Select your downloaded files folder when prompted inside the suite.")
            env = {**os.environ, "GST_OFFLINE": "11"}
            try:
                subprocess.run([sys.executable, str(gst_script)],
                               capture_output=False, env=env)
            except Exception as _e:
                _log(f"  ✗ GST offline error: {_e}", "error")

        # ── helper: run IT recon from existing PDFs ──────────────────────────
        def _run_it_offline(clients_list):
            """Run IT recon engine from 26AS/AIS/TIS already on disk (no browser)."""
            it_script = SCRIPT_DIR / "it_suite_v6.py"
            recon_script = SCRIPT_DIR / "it_recon_engine.py"

            if it_script.exists():
                _log("  Launching IT Suite — Option 4 (IT Recon Excel from existing PDFs)...")
                _log("  ► When the suite menu appears, enter  4  to run recon-only.")
                env = {**os.environ, "IT_OFFLINE": "4"}
                try:
                    subprocess.run([sys.executable, str(it_script)],
                                   capture_output=False, env=env)
                    return
                except Exception as _e:
                    _log(f"  ✗ IT suite error: {_e} — trying direct recon engine", "warning")

            if recon_script.exists():
                _log("  Running it_recon_engine.py directly from existing files...")
                for client in clients_list:
                    name = client.get("name",""); pan = client.get("pan","")
                    fy   = client.get("fy", FY_LABEL)
                    if not pan: continue
                    # Locate IT files in IT Download or staging folder
                    it_dir = None
                    for _cand in [
                        IT_RUN / name.replace(" ","_"),
                        BASE_DIR / name.replace(" ","_") / "IT Download",
                    ]:
                        if _cand.exists(): it_dir = _cand; break
                    if not it_dir:
                        _log(f"  ⚠  No IT folder found for {name} — skipping", "warning")
                        continue
                    gst_xl = _find_gst_excel_for_client(client.get("gstin",""), name, fy)
                    gst_folder_arg = str(Path(gst_xl).parent) if gst_xl else None
                    recon_args = [sys.executable, str(recon_script),
                                  "--name", name, "--pan", pan, "--fy", fy,
                                  "--it-folder", str(it_dir)]
                    if gst_folder_arg: recon_args += ["--gst-folder", gst_folder_arg]
                    try:
                        subprocess.run(recon_args, capture_output=False)
                        _log(f"  ✓ IT Recon done for {name}")
                    except Exception as _e:
                        _log(f"  ✗ IT Recon error for {name}: {_e}", "error")
            else:
                _log("  ✗ Neither it_suite_v6.py nor it_recon_engine.py found", "error")

        # ── LOAD CLIENTS ─────────────────────────────────────────────────────
        clients = load_clients(fy_override=args.fy, name_filter=args.client)
        _log(f"\n  {len(clients)} client(s) loaded")

        # ── DISPATCH ─────────────────────────────────────────────────────────
        c = offline_choice

        if c == 1:
            # Step 1 — Tally extract (Tally must be open)
            run_tally_extract_step()

        elif c == 2:
            # Step 2+3 — GST recon from existing downloaded files
            _run_gst_offline()

        elif c == 3:
            # Step 4+5 — IT recon from existing PDFs
            _run_it_offline(clients)

        elif c == 4:
            # Step 6 — Bridge only (needs GST + IT folders)
            gst_ok, it_ok = _pick_gst_it_folders(need_gst=True, need_it=True)
            if gst_ok and it_ok:
                run_bridge_step(clients, str(GST_RUN), str(IT_RUN))

        elif c == 5:
            # Step 6b — GST-IT Comparison Excel
            _pick_gst_it_folders(need_gst=True, need_it=True)
            run_gst_it_comparison_step()

        elif c == 6:
            # Step 6c — GSTR-2B Consolidated Extractor
            _pick_gst_it_folders(need_gst=True, need_it=False)
            run_gstr2b_extractor_step(clients)

        elif c == 7:
            # Step 6d — GSTR-1 vs 26AS Comparison
            _pick_gst_it_folders(need_gst=True, need_it=True)
            run_gstr1_26as_step(clients)

        elif c == 8:
            # Step 7 — Final consolidated report
            _pick_gst_it_folders(need_gst=True, need_it=True)
            _run_final_consolidated()

        elif c == 9:
            # Steps 6→7 — Bridge + all reports (original --offline behaviour)
            gst_ok, it_ok = _pick_gst_it_folders(need_gst=True, need_it=True)
            if gst_ok and it_ok:
                run_bridge_step(clients, str(GST_RUN), str(IT_RUN))
                run_gst_it_comparison_step()
                run_gstr2b_extractor_step(clients)
                run_gstr1_26as_step(clients)
                _run_final_consolidated()

        elif c == 10:
            # Steps 3→7 — IT Recon + Bridge + Reports
            _run_it_offline(clients)
            gst_ok, it_ok = _pick_gst_it_folders(need_gst=True, need_it=True)
            if gst_ok and it_ok:
                run_bridge_step(clients, str(GST_RUN), str(IT_RUN))
                run_gst_it_comparison_step()
                run_gstr2b_extractor_step(clients)
                run_gstr1_26as_step(clients)
                _run_final_consolidated()

        elif c == 11:
            # Steps 2→7 — GST Recon + IT Recon + Bridge + Reports
            _run_gst_offline()
            _run_it_offline(clients)
            gst_ok, it_ok = _pick_gst_it_folders(need_gst=True, need_it=True)
            if gst_ok and it_ok:
                run_bridge_step(clients, str(GST_RUN), str(IT_RUN))
                run_gst_it_comparison_step()
                run_gstr2b_extractor_step(clients)
                run_gstr1_26as_step(clients)
                _run_final_consolidated()

        elif c == 12:
            # ALL STEPS — full offline pipeline
            run_tally_extract_step()
            _run_gst_offline()
            _run_it_offline(clients)
            gst_ok, it_ok = _pick_gst_it_folders(need_gst=True, need_it=True)
            if gst_ok and it_ok:
                run_bridge_step(clients, str(GST_RUN), str(IT_RUN))
                run_gst_it_comparison_step()
                run_gstr2b_extractor_step(clients)
                run_gstr1_26as_step(clients)
                _run_final_consolidated()

        _banner("OFFLINE — ALL DONE")
        _log(f"  Log: {LOG_FILE}")
        return

    # ── NORMAL (ONLINE) MODE ─────────────────────────────────────────────────
    _banner(f"RUN ALL v10.12 — GST + Income Tax Pipeline  FY {FY_LABEL}")
    _log(f"  Script dir : {SCRIPT_DIR}")
    _log(f"  Base dir   : {BASE_DIR}")
    _log(f"  Log file   : {LOG_FILE}")
    _log(f"  Variance   : ₹{VARIANCE_THRESHOLD:,.0f}")
    _log("")
    _log("  ORDER:  Step 1 Tally Extract  →  Step 2+3 GST  →  Step 4+5 IT  →  Step 6 Bridge  →  6b Comparison  →  6c 2B Extract  →  6d 26AS vs GSTR-1  →  Step 7 Final")

    clients = load_clients(fy_override=args.fy, name_filter=args.client)
    _log(f"\n  {len(clients)} client(s) loaded\n")

    run_all_flag = not (args.only_gst or args.only_it or args.only_bridge)
    do_gst    = run_all_flag or args.only_gst
    do_it     = run_all_flag or args.only_it
    do_bridge = run_all_flag or args.only_bridge

    # ── STEP 1: Tally extract (always runs in full/gst/online modes) ──────────
    skip_tally = getattr(args, "skip_tally", False)
    if run_all_flag or do_gst:
        if skip_tally:
            _log("  [Step 1] --skip-tally flag set — skipping Tally extract.")
        else:
            run_tally_extract_step()

    if do_gst:
        run_gst_step(clients)        # GST_RUN updated to real folder

    if do_it:
        run_it_step(clients)         # IT_RUN updated to real folder

    if do_bridge:
        # ── Always prefer Option B per-client folders for bridge + downstream steps ──
        # After reorganization, staging folders are deleted. We must pass the
        # real ClientName/GST Automation and ClientName/IT Download paths.
        # _best_bridge_paths() resolves this once and shares result across all steps.
        def _best_bridge_paths():
            """Return (gst_path, it_path) preferring Option B per-client folders."""
            _gst = _it = None
            for cl in clients:
                _g = client_gst(BASE_DIR, cl["name"])
                _i = client_it(BASE_DIR, cl["name"])
                if not _gst and _g.exists():
                    _gst = _g
                if not _it and _i.exists():
                    _it = _i
                if _gst and _it:
                    break
            # Fall back to discovered staging paths only if Option B not found
            return (
                str(_gst) if _gst and _gst.exists() else str(GST_RUN),
                str(_it)  if _it  and _it.exists()  else str(IT_RUN),
            )

        _gst_arg, _it_arg = _best_bridge_paths()
        run_bridge_step(clients, _gst_arg, _it_arg)
        run_gst_it_comparison_step()
        run_gstr2b_extractor_step(clients)
        run_gstr1_26as_step(clients)
        _run_final_consolidated()

    _banner("ALL DONE — Option B Structure")
    _log(f"  Base dir   : {BASE_DIR}")
    _log(f"  GST staging: {GST_RUN}")
    _log(f"  IT  staging: {IT_RUN}")
    _log(f"  Log        : {LOG_FILE}")
    _log("")
    _log("  Per-client folders (Option B):")
    try:
        for child in sorted(BASE_DIR.iterdir()):
            gst_sub = child / "GST Automation"
            it_sub  = child / "IT Download"
            dl_sub  = child / "26AS vs GSTR1"
            if child.is_dir() and (gst_sub.exists() or it_sub.exists()):
                _log(f"    📁 {child.name}/")
                for sub in [dl_sub, gst_sub, it_sub]:
                    if sub.exists():
                        cnt = len(list(sub.glob("*")))
                        _log(f"        📄 {sub.name}/ ({cnt} file(s))")
    except Exception:
        pass


if __name__ == "__main__":
    main()
