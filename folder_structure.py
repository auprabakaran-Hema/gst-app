"""
================================================================================
  FOLDER STRUCTURE — Option B Helper  v10.9-D
================================================================================

  One folder per client, SIX named subfolders inside it:

  📁 BASE_DIR/
      📁 Elanthailiar/
          📁 Raw Data/            ← JSON, ZIP, raw portal PDFs (GST + IT originals)
          📁 GST Automation/      ← GSTR1_FY.xlsx, GSTR2B.xlsx, GSTR3B.xlsx,
                                     ANNUAL_RECONCILIATION.xlsx
          📁 IT Download/         ← 26AS.pdf, AIS.pdf, TIS.pdf,
                                     IT_RECONCILIATION.xlsx
          📁 IT Bridge/           ← Master_Bridge_*.xlsx
          📁 GST IT Comparison/   ← GST_IT_Comparison_*.xlsx
          📁 26AS vs GSTR1/       ← 26AS_GSTR1_Compare_*.xlsx
      📁 Client B/
          📁 GST Automation/
          📁 IT Download/
          📁 IT Bridge/
          📁 GST IT Comparison/
          📁 26AS vs GSTR1/

  Usage (imported by run_all.py):
      from folder_structure import (
          client_root, client_gst, client_it,
          client_bridge, client_comparison, client_26as,
          ensure_client_dirs,
          reorganize_gst_output, reorganize_it_output,
          print_structure,
      )
================================================================================
"""
import shutil
import re
from pathlib import Path


# ---------------------------------------------------------------------------
# Per-client folder helpers
# ---------------------------------------------------------------------------

def client_root(base_dir: Path, name: str) -> Path:
    """BASE_DIR / ClientSafeName"""
    safe = name.strip().replace(" ", "_").replace("/", "_")
    safe = re.sub(r"[^\w\-]", "_", safe)
    return base_dir / safe


def client_gst(base_dir: Path, name: str) -> Path:
    """GST portal downloads + reconciliation Excels."""
    return client_root(base_dir, name) / "GST Automation"


def client_it(base_dir: Path, name: str) -> Path:
    """26AS / AIS / TIS PDFs + IT_RECONCILIATION.xlsx."""
    return client_root(base_dir, name) / "IT Download"


def client_bridge(base_dir: Path, name: str) -> Path:
    """Master Bridge output (GST <-> IT reconciliation)."""
    return client_root(base_dir, name) / "IT Bridge"


def client_comparison(base_dir: Path, name: str) -> Path:
    """GST-IT Comparison Excel (TIS / AIS template)."""
    return client_root(base_dir, name) / "GST IT Comparison"


def client_26as(base_dir: Path, name: str) -> Path:
    """26AS vs GSTR-1 Comparison Excel."""
    return client_root(base_dir, name) / "26AS vs GSTR1"


# Backward compat aliases
def client_downloads(base_dir: Path, name: str) -> Path:
    return client_gst(base_dir, name)

def client_raw_data(base_dir: Path, name: str) -> Path:
    """Raw JSON, ZIP, and portal PDFs — original downloads before processing."""
    return client_root(base_dir, name) / "Raw Data"


def ensure_client_dirs(base_dir: Path, name: str) -> dict:
    """Create all six subfolders for a client. Returns dict of paths."""
    dirs = {
        "root":       client_root(base_dir, name),
        "raw_data":   client_raw_data(base_dir, name),
        "gst":        client_gst(base_dir, name),
        "it":         client_it(base_dir, name),
        "bridge":     client_bridge(base_dir, name),
        "comparison": client_comparison(base_dir, name),
        "recon_26as": client_26as(base_dir, name),
        # compat keys
        "downloads":  client_gst(base_dir, name),
    }
    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)
    return dirs


# ---------------------------------------------------------------------------
# File classification helpers
# ---------------------------------------------------------------------------

_IT_PATTERNS  = ["26AS", "AIS", "TIS", "AIS_", "IT_RECONCILIATION", "IT_RECON"]
_GST_PATTERNS = ["GSTR", "GST", "ANNUAL_RECON", "GSTR1_FY", "GSTR2B",
                 "GSTR3B", "RECON"]


def _classify(filename: str) -> str:
    """
    Route a file to 'raw_data', 'gst' (GST Automation), or 'it' (IT Download).

      .json / .zip              -> raw_data  (raw GST/IT portal originals)
      .pdf  with 26AS/AIS/TIS   -> it        (IT portal PDFs)
      .pdf  other               -> raw_data  (raw GST return PDF)
      .xlsx with IT patterns    -> it        (IT_RECONCILIATION, 26AS xlsx)
      .xlsx other               -> gst       (GSTR1_FY, GSTR2B, ANNUAL_RECON, etc.)
    """
    fn  = filename.upper()
    ext = Path(filename).suffix.lower()

    if ext in {".json", ".zip"}:
        return "raw_data"

    if ext == ".pdf":
        for p in _IT_PATTERNS:
            if p in fn:
                return "it"
        return "raw_data"

    if ext in {".xlsx", ".xls", ".csv"}:
        for p in _IT_PATTERNS:
            if p in fn:
                return "it"
        return "gst"

    return "raw_data"


# ---------------------------------------------------------------------------
# Post-suite reorganization
# ---------------------------------------------------------------------------

def reorganize_gst_output(base_dir: Path, gst_run: Path, clients: list,
                          folder_candidates_fn, log_fn=print):
    """
    After gst_suite finishes, move files from staging into:
        ClientName/GST Automation/   (GSTR excels, JSON, ZIP, PDF)
        ClientName/IT Download/      (any 26AS/AIS that ended up here)
    Returns {client_name: gst_path}.
    """
    results = {}
    for client in clients:
        name   = client["name"]
        gstins = client.get("gstin", [])
        dirs   = ensure_client_dirs(base_dir, name)
        moved  = 0
        for gstin in gstins:
            for cand in folder_candidates_fn(name, gstin):
                src_dir = gst_run / cand
                if not src_dir.exists():
                    continue
                for f in src_dir.iterdir():
                    if not f.is_file():
                        continue
                    dest = dirs[_classify(f.name)] / f.name
                    try:
                        shutil.copy2(str(f), str(dest))
                        moved += 1
                    except Exception as e:
                        log_fn(f"    WARNING  Copy error {f.name}: {e}")
                break
        if moved:
            log_fn(f"  OK {name}: {moved} file(s) -> {dirs['gst'].relative_to(base_dir)}")
            results[name] = dirs["gst"]
        else:
            log_fn(f"  INFO {name}: no GST files found in staging")
            results[name] = dirs["gst"]
    return results


def reorganize_it_output(base_dir: Path, it_run: Path, clients: list,
                         folder_candidates_fn, log_fn=print):
    """
    After it_suite finishes, move files from staging into:
        ClientName/IT Download/      (26AS/AIS/TIS PDFs, IT_RECON.xlsx)
        ClientName/GST Automation/   (any stray GST files)
    Returns {client_name: it_path}.

    FIX v10.10: it_suite creates ClientName_GSTIN folders (not ClientName alone).
    We must try ALL GSTIN candidates for the client, not just the empty-GSTIN one.
    """
    results = {}
    for client in clients:
        name   = client["name"]
        gstins = client.get("gstin", [])
        dirs   = ensure_client_dirs(base_dir, name)
        moved  = 0

        # Build the full candidate list: Name+GSTIN variants first, then Name-only
        # This mirrors what gst_suite does (Name_GSTIN folder) so we always match.
        all_cands = []
        for gstin in gstins:
            all_cands.extend(folder_candidates_fn(name, gstin))
        all_cands.extend(folder_candidates_fn(name, ""))   # name-only fallback

        for cand in all_cands:
            src_dir = it_run / cand
            if not src_dir.exists():
                continue
            for f in src_dir.iterdir():
                if not f.is_file():
                    continue
                dest = dirs[_classify(f.name)] / f.name
                try:
                    shutil.copy2(str(f), str(dest))
                    moved += 1
                except Exception as e:
                    log_fn(f"    WARNING  Copy error {f.name}: {e}")
            break  # stop after first matching candidate

        if moved:
            log_fn(f"  OK {name}: {moved} file(s) -> {dirs['it'].relative_to(base_dir)}")
            results[name] = dirs["it"]
        else:
            log_fn(f"  INFO {name}: no IT files found in staging")
            results[name] = dirs["it"]
    return results


def print_structure(base_dir: Path, clients: list):
    """Print the current folder layout to stdout."""
    SUBFOLDERS = [
        ("Raw Data",          "JSON, ZIP, raw portal PDFs (originals)"),
        ("GST Automation",    "GSTR reconciliation Excels"),
        ("IT Download",       "26AS / AIS / TIS + IT recon"),
        ("IT Bridge",         "GST <-> IT master bridge"),
        ("GST IT Comparison", "GST-IT comparison Excel"),
        ("26AS vs GSTR1",     "26AS vs GSTR-1 comparison"),
    ]
    print("\n  FOLDER LAYOUT — Option B")
    print(f"  Root: {base_dir}\n")
    for client in clients:
        name = client["name"]
        root = client_root(base_dir, name)
        if root.exists():
            print(f"  [+] {root.name}/")
            for sub, desc in SUBFOLDERS:
                sd    = root / sub
                count = len(list(sd.glob("*"))) if sd.exists() else 0
                icon  = "[F]" if sd.exists() else "   "
                print(f"       {icon} {sub}/  ({count} files)  <- {desc}")
        else:
            print(f"  [ ] {name}/ (not yet created)")
    print()
