"""
================================================================================
  GSTR-1 GST NAME EXTRACTOR  v3.0  ─  Advanced Professional Edition
================================================================================

PURPOSE
-------
Run ONCE before processing GSTR-1 files to pre-populate gstin_name_cache.json
with all buyer/receiver party names.

WHAT'S NEW IN v3.0
------------------
  * Concurrent portal fetches via ThreadPoolExecutor (--workers N, default 6)
  * Exponential back-off retry per GSTIN (--max-retries N, default 3)
  * EXP section GSTINs now extracted (exporter GSTINs)
  * --verify-cache: integrity check — flags missing / stale / unnamed
  * Colour ANSI console (Windows 10+, Linux, macOS; respects NO_COLOR)
  * Live progress bar (no external deps)
  * 3-sheet Excel: Party Master · State Summary · Unnamed GSTINs (action list)
  * Auto-filter, freeze panes, alternate-row banding, Status colour column

USAGE
-----
  python extract_gst_names_once.py                       # auto-discovers ZIPs
  python extract_gst_names_once.py /path/to/gstr1/       # explicit folder
  python extract_gst_names_once.py Apr.zip May.zip ...   # specific files
  python extract_gst_names_once.py --refresh-all         # re-fetch all
  python extract_gst_names_once.py --dry-run             # preview only
  python extract_gst_names_once.py --no-export           # skip Excel
  python extract_gst_names_once.py --workers 8           # parallelism
  python extract_gst_names_once.py --verify-cache        # integrity check

================================================================================
"""

import json, sys, os, zipfile, argparse, re, time, threading
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False

try:
    from gstin_name_cache import GSTINNameCache
    CACHE_AVAILABLE = True
except ImportError:
    CACHE_AVAILABLE = False

# ── Colour helpers ────────────────────────────────────────────────────────────
def _supports_colour():
    if os.environ.get("NO_COLOR"):    return False
    if os.environ.get("FORCE_COLOR"): return True
    if sys.platform == "win32":
        try:
            import ctypes; ctypes.windll.kernel32.SetConsoleMode(
                ctypes.windll.kernel32.GetStdHandle(-11), 7)
            return True
        except Exception: return False
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

_COLOUR = _supports_colour()
def _c(code, t): return f"\033[{code}m{t}\033[0m" if _COLOUR else t
def _green(t):  return _c("92", t)
def _red(t):    return _c("91", t)
def _yellow(t): return _c("93", t)
def _cyan(t):   return _c("96", t)
def _dim(t):    return _c("2",  t)
def _bold(t):   return _c("1",  t)
def _ok(t):     return _green(f"  \u2713  {t}")
def _warn(t):   return _yellow(f"  \u26a0  {t}")
def _err(t):    return _red(f"  \u2717  {t}")
def _info(t):   return _dim(f"  \u2022  {t}")

# ── Progress bar ──────────────────────────────────────────────────────────────
class _Progress:
    def __init__(self, total, label="", width=38):
        self._total = max(total, 1); self._done = 0; self._label = label
        self._width = width; self._lock = threading.Lock()
        self._start = time.monotonic(); self._last = 0.0
    def update(self, n=1):
        with self._lock:
            self._done = min(self._done + n, self._total)
            now = time.monotonic()
            if now - self._last < 0.15 and self._done < self._total: return
            self._last = now; self._render()
    def finish(self):
        with self._lock: self._done = self._total; self._render(); print()
    def _render(self):
        frac = self._done / self._total
        filled = int(self._width * frac)
        bar = "\u2588" * filled + "\u2591" * (self._width - filled)
        elapsed = time.monotonic() - self._start
        eta = ""
        if self._done > 0 and self._done < self._total:
            eta = f"  ETA {int(elapsed/self._done*(self._total-self._done))}s"
        bar_str = f"\033[92m{bar}\033[0m" if _COLOUR else bar
        sys.stdout.write(
            f"\r  {self._label}  [{bar_str}] {frac*100:5.1f}%"
            f"  {self._done}/{self._total}{eta}   ")
        sys.stdout.flush()

# ── GSTIN / state helpers ─────────────────────────────────────────────────────
_STATE = {
    "01":"J&K","02":"HP","03":"Punjab","04":"Chandigarh","05":"Uttarakhand",
    "06":"Haryana","07":"Delhi","08":"Rajasthan","09":"UP","10":"Bihar",
    "11":"Sikkim","12":"Arunachal","13":"Nagaland","14":"Manipur",
    "15":"Mizoram","16":"Tripura","17":"Meghalaya","18":"Assam",
    "19":"West Bengal","20":"Jharkhand","21":"Odisha","22":"Chhattisgarh",
    "23":"MP","24":"Gujarat","26":"D&NH+DD","27":"Maharashtra",
    "28":"Andhra","29":"Karnataka","30":"Goa","31":"Lakshadweep",
    "32":"Kerala","33":"Tamil Nadu","34":"Puducherry","35":"A&N Islands",
    "36":"Telangana","37":"AP (new)","38":"Ladakh",
}
_STALE_DAYS = 90

def _cg(g):  return re.sub(r"[^A-Z0-9]","",str(g or "").strip().upper())
def _pan(g): g=_cg(g); return g[2:12] if len(g)==15 else ""
def _st(g):  g=_cg(g); code=g[:2] if len(g)==15 else ""; return _STATE.get(code,"")

# ── JSON extraction ───────────────────────────────────────────────────────────
def _unwrap(data):
    if not isinstance(data, dict): return {}
    for k in ("data","result","fileContent"):
        inner = data.get(k)
        if isinstance(inner, str):
            try: inner = json.loads(inner)
            except Exception: continue
        if isinstance(inner, dict):
            if "gstnDetailed" in inner: return inner["gstnDetailed"]
            if any(x in inner for x in ("b2b","cdnr","exp","b2ba")): return inner
    if any(x in data for x in ("b2b","cdnr","exp","b2ba")): return data
    return {}

def _gstins_from_dict(data):
    found = set()
    for sec in ("b2b","cdnr","b2ba","cdnra"):
        for e in data.get(sec,[]):
            g=_cg(e.get("ctin",""))
            if len(g)==15: found.add(g)
    for e in data.get("exp",[]):
        g=_cg(e.get("gstin",""))
        if len(g)==15: found.add(g)
    return found

def _gstins_from_zip(zpath):
    out = set()
    try:
        with zipfile.ZipFile(zpath,"r") as zf:
            for n in zf.namelist():
                if not n.lower().endswith(".json"): continue
                try:
                    inner = _unwrap(json.loads(zf.read(n).decode("utf-8","replace")))
                    if inner: out.update(_gstins_from_dict(inner))
                except Exception: continue
    except zipfile.BadZipFile: print(_warn(f"Bad ZIP: {Path(zpath).name}"))
    except Exception as e:     print(_warn(f"Cannot read {Path(zpath).name}: {e}"))
    return out

def _gstins_from_json(jpath):
    try:
        inner = _unwrap(json.loads(Path(jpath).read_text(encoding="utf-8",errors="replace")))
        return _gstins_from_dict(inner) if inner else set()
    except Exception as e: print(_warn(f"Cannot parse {Path(jpath).name}: {e}")); return set()

def collect_all_gstins(sources):
    res = defaultdict(set)
    for p in sources:
        p = Path(p)
        if not p.exists(): print(_warn(f"Not found: {p}")); continue
        gstins = _gstins_from_zip(p) if p.suffix.lower()==".zip" else \
                 _gstins_from_json(p) if p.suffix.lower()==".json" else set()
        for g in gstins: res[g].add(p.stem)
    return res

# ── Fetch with retry ──────────────────────────────────────────────────────────
def _fetch_retry(cache, gstin, max_retries=3, base_delay=1.5):
    for attempt in range(max_retries):
        try:
            rec = cache._fetch_one(gstin)
            if rec and (rec.get("legal_name") or rec.get("trade_name")):
                cache._data[gstin] = rec; return True
        except Exception: pass
        if attempt < max_retries-1: time.sleep(base_delay * (2**attempt))
    return False

# ── Cache integrity ───────────────────────────────────────────────────────────
def verify_cache(cache_data, gstin_sources):
    now = datetime.now()
    out = {}
    for g in gstin_sources:
        rec = cache_data.get(g)
        if not rec:                                   out[g]="missing"; continue
        if not rec.get("legal_name") and not rec.get("trade_name"):
                                                      out[g]="unnamed"; continue
        fa = rec.get("fetched_at","")
        if fa:
            try:
                if now-datetime.fromisoformat(fa[:19]) > timedelta(days=_STALE_DAYS):
                    out[g]="stale"; continue
            except Exception: pass
        out[g]="ok"
    return out

# ── Excel export ──────────────────────────────────────────────────────────────
NAVY="1F3864"; BLUE="2E75B6"; TEAL="17375E"
GRN="C6EFCE"; DKG="276221"; AMB="FFEB9C"; DKA="9C6500"
RED="FFC7CE"; DKR="9C0006"; LGR="F5F5F5"; WHT="FFFFFF"

def _xs(ws, r, c, v, *, bg=WHT, fg="000000", bold=False, sz=9, h="left",
        wrap=False, bdr=True):
    cell = ws.cell(row=r, column=c, value=v)
    cell.font      = Font(name="Arial", bold=bold, color=fg, size=sz)
    cell.fill      = PatternFill("solid", fgColor=bg)
    cell.alignment = Alignment(horizontal=h, vertical="center", wrap_text=wrap)
    if bdr:
        bd = Side(style="thin", color="D0D0D0")
        cell.border = Border(left=bd, right=bd, top=bd, bottom=bd)
    return cell

def export_party_master(gstin_sources, cache_data, out_path, statuses=None):
    if not EXCEL_AVAILABLE:
        print(_warn("openpyxl not installed — skipping Excel export")); return

    wb = openpyxl.Workbook()

    # ── Sheet 1: Party Master ─────────────────────────────────────────────────
    ws = wb.active; ws.title = "Party Master"
    ws.sheet_view.showGridLines = False; ws.freeze_panes = "A3"

    ws.merge_cells("A1:H1")
    c = ws["A1"]
    c.value = (f"GSTR-1 Party Master  \u00b7  "
               f"Generated {datetime.now().strftime('%d-%b-%Y %H:%M')}  \u00b7  "
               f"{len(gstin_sources)} GSTINs")
    c.font = Font(name="Arial",bold=True,color="FFFFFF",size=12)
    c.fill = PatternFill("solid",fgColor=NAVY)
    c.alignment = Alignment(horizontal="center",vertical="center")
    ws.row_dimensions[1].height = 28

    hdrs   = ["GSTIN","PAN","State","Party Name","Status","Source","Fetched On","Months Seen"]
    widths = [22,     13,   14,     48,          10,      18,      13,          40]
    for ci,(h,w) in enumerate(zip(hdrs,widths),1):
        _xs(ws,2,ci,h,bg=BLUE,fg="FFFFFF",bold=True,sz=9,h="center")
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[2].height = 20

    sorted_gstins = sorted(gstin_sources.keys(),
        key=lambda g: (cache_data.get(g) or {}).get("trade_name","") or
                      (cache_data.get(g) or {}).get("legal_name","") or g)

    for ri, gstin in enumerate(sorted_gstins, 3):
        rec   = cache_data.get(gstin) or {}
        legal = rec.get("legal_name",""); trade = rec.get("trade_name","")
        name  = trade or legal or ""
        src   = rec.get("source","not found")
        fetd  = (rec.get("fetched_at") or "")[:10]
        mths  = ", ".join(sorted(gstin_sources[gstin]))
        st    = (statuses or {}).get(gstin,"ok")
        alt   = LGR if ri%2==0 else WHT

        if st=="ok":      sbg,sfg,slbl = GRN,DKG,"\u2713 OK"
        elif st=="stale": sbg,sfg,slbl = AMB,DKA,"\u26a0 Stale"
        elif st=="missing":sbg,sfg,slbl= RED,DKR,"\u2717 Missing"
        elif st=="unnamed":sbg,sfg,slbl= AMB,DKA,"? Unnamed"
        else:              sbg,sfg,slbl= LGR,"444444",st

        row_vals = [gstin,_pan(gstin),_st(gstin),name,slbl,src,fetd,mths]
        for ci,v in enumerate(row_vals,1):
            if ci==5: _xs(ws,ri,ci,v,bg=sbg,fg=sfg,bold=True,sz=8,h="center")
            else:      _xs(ws,ri,ci,v,bg=alt)
        ws.row_dimensions[ri].height = 16

    last = 2+len(sorted_gstins)
    ws.auto_filter.ref = f"A2:H{last}"

    total  = len(sorted_gstins)
    named  = sum(1 for g in sorted_gstins if
                 (cache_data.get(g) or {}).get("legal_name") or
                 (cache_data.get(g) or {}).get("trade_name"))
    stale_c = sum(1 for v in (statuses or {}).values() if v=="stale")
    fr = last+2
    ws.merge_cells(f"A{fr}:H{fr}")
    c=ws[f"A{fr}"]
    c.value = (f"Total: {total}   \u00b7   Named: {named}   \u00b7   "
               f"Unnamed: {total-named}   \u00b7   Stale (>{_STALE_DAYS}d): {stale_c}")
    c.font = Font(name="Arial",bold=True,color="FFFFFF",size=9)
    c.fill = PatternFill("solid",fgColor=NAVY)
    c.alignment = Alignment(horizontal="center",vertical="center")
    ws.row_dimensions[fr].height = 20

    # ── Sheet 2: State Summary ────────────────────────────────────────────────
    ws2 = wb.create_sheet("State Summary")
    ws2.sheet_view.showGridLines = False
    ws2.merge_cells("A1:D1")
    c2=ws2["A1"]; c2.value="State-wise Party Count"
    c2.font=Font(name="Arial",bold=True,color="FFFFFF",size=11)
    c2.fill=PatternFill("solid",fgColor=TEAL)
    c2.alignment=Alignment(horizontal="center",vertical="center")
    ws2.row_dimensions[1].height=26
    for ci,(h,w) in enumerate([("State Code",12),("State Name",22),
                                ("# GSTINs",12),("# Named",12)],1):
        _xs(ws2,2,ci,h,bg=BLUE,fg="FFFFFF",bold=True,h="center")
        ws2.column_dimensions[get_column_letter(ci)].width=w
    ws2.row_dimensions[2].height=18
    sg = defaultdict(list)
    for g in sorted_gstins: sg[g[:2]].append(g)
    for ri2,(code,glist) in enumerate(sorted(sg.items()),3):
        named2=sum(1 for g in glist if (cache_data.get(g) or {}).get("legal_name") or
                   (cache_data.get(g) or {}).get("trade_name"))
        alt2=LGR if ri2%2==0 else WHT
        for ci2,v in enumerate([code,_STATE.get(code,"Unknown"),len(glist),named2],1):
            _xs(ws2,ri2,ci2,v,bg=alt2,h="center" if ci2 in (1,3,4) else "left")
        ws2.row_dimensions[ri2].height=15

    # ── Sheet 3: Unnamed (action list) ───────────────────────────────────────
    unm_list = [g for g in sorted_gstins if not
                ((cache_data.get(g) or {}).get("legal_name") or
                 (cache_data.get(g) or {}).get("trade_name"))]
    if unm_list:
        ws3 = wb.create_sheet("Unnamed GSTINs")
        ws3.sheet_view.showGridLines = False
        ws3.merge_cells("A1:E1")
        c3=ws3["A1"]
        c3.value=f"Unnamed GSTINs \u2014 portal could not resolve ({len(unm_list)} entries)"
        c3.font=Font(name="Arial",bold=True,color="FFFFFF",size=11)
        c3.fill=PatternFill("solid",fgColor="9C0006")
        c3.alignment=Alignment(horizontal="center",vertical="center")
        ws3.row_dimensions[1].height=26
        for ci3,(h,w) in enumerate([("GSTIN",22),("PAN",13),("State",14),
                                     ("Months",35),("Action",40)],1):
            _xs(ws3,2,ci3,h,bg=BLUE,fg="FFFFFF",bold=True,h="center")
            ws3.column_dimensions[get_column_letter(ci3)].width=w
        ws3.row_dimensions[2].height=18
        for ri3,g in enumerate(unm_list,3):
            action="Add manually in CustomerMaster.xlsx or run --refresh-all"
            alt3="FFF2CC" if ri3%2==0 else WHT
            for ci3,v in enumerate([g,_pan(g),_st(g),
                                     ", ".join(sorted(gstin_sources[g])),action],1):
                _xs(ws3,ri3,ci3,v,bg=alt3,wrap=(ci3==5))
            ws3.row_dimensions[ri3].height=15

    wb.save(str(out_path))
    sz=out_path.stat().st_size//1024
    print(_ok(f"Excel exported \u2192 {out_path.name}  ({total} parties, {sz} KB)"))

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(
        prog="extract_gst_names_once",
        description="GSTR-1 Party Name Extractor v3.0",
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("sources", nargs="*",
        help="GSTR-1 ZIP/JSON files or a folder. Default: current dir.")
    ap.add_argument("--refresh-all", action="store_true",
        help="Re-fetch even if already portal-verified in cache.")
    ap.add_argument("--dry-run", action="store_true",
        help="Preview what would be fetched — no portal calls.")
    ap.add_argument("--no-export", action="store_true",
        help="Skip Excel export (only update cache JSON).")
    ap.add_argument("--out", metavar="PATH",
        help="Custom output path for Excel party master.")
    ap.add_argument("--workers", type=int, default=6, metavar="N",
        help="Parallel portal workers (default: 6).")
    ap.add_argument("--verify-cache", action="store_true",
        help="Check cache integrity and flag stale/missing entries.")
    ap.add_argument("--max-retries", type=int, default=3, metavar="N",
        help="Per-GSTIN retry attempts (default: 3).")
    args = ap.parse_args()
    script_dir = Path(__file__).parent

    print()
    print(_bold("=" * 66))
    print(_bold("  GSTR-1 Party Name Extractor  v3.0"))
    print(_dim ("  GST Automation Suite \u2014 RPR"))
    print(_bold("=" * 66))
    if args.dry_run:    print(_yellow("  [DRY RUN] No cache changes will be made."))
    if args.verify_cache: print(_cyan("  [VERIFY]  Cache integrity check enabled."))
    print()

    # ── Source resolution ─────────────────────────────────────────────────────
    sources = []
    if args.sources:
        for s in args.sources:
            p = Path(s)
            if p.is_dir():
                zips  = sorted(p.glob("*.zip"))
                jsons = sorted(p.glob("*.json"))
                sources.extend(zips); sources.extend(jsons)
                if zips or jsons:
                    print(_info(f"Folder: {p}  \u2192  {len(zips)} ZIP, {len(jsons)} JSON"))
            elif p.exists(): sources.append(p)
            else: print(_warn(f"Not found: {p}"))
    else:
        for d in [Path.cwd(), script_dir]:
            zips = sorted(d.glob("*.zip"))
            if zips:
                sources = zips
                print(_info(f"Auto-discovered {len(zips)} ZIP(s) in: {d}"))
                break
        if not sources:
            print(_err("No GSTR-1 ZIP files found."))
            print("\n  Usage:  python extract_gst_names_once.py /path/to/gstr1/")
            sys.exit(0)

    print(f"  Sources   : {_bold(str(len(sources)))} file(s)")
    for s in sources[:6]: print(_dim(f"    \u2022 {Path(s).name}"))
    if len(sources) > 6:  print(_dim(f"    \u2026 and {len(sources)-6} more"))
    print()

    # ── Step 1: Extract GSTINs ────────────────────────────────────────────────
    print(_bold("  [1/3]  Scanning for buyer GSTINs \u2026"))
    t0 = time.monotonic()
    gstin_sources = collect_all_gstins(sources)
    elapsed = time.monotonic() - t0
    print(_ok(f"{len(gstin_sources)} unique GSTINs found  ({elapsed:.1f}s)"))
    if not gstin_sources:
        print(_warn("Nothing to process.")); sys.exit(0)
    print()

    # ── Step 2: Fetch names ───────────────────────────────────────────────────
    print(_bold("  [2/3]  Fetching party names \u2026"))
    cache_data = {}; statuses = {}

    if not CACHE_AVAILABLE:
        print(_err("gstin_name_cache.py not found \u2014 cannot fetch names."))
    elif args.dry_run:
        c = GSTINNameCache(auto_fetch=False)
        cache_data = dict(c._data)
        to_fetch = [(g,"not in cache") for g in gstin_sources if not c._data.get(g)]
        if args.refresh_all:
            to_fetch += [(g,f"would refresh ({c._data[g].get('source','?')})")
                         for g in gstin_sources
                         if g in c._data and c._data[g].get("source") not in
                            ("portal","portal_selenium")]
        print(_info(f"Already cached : {len(gstin_sources)-len(to_fetch)}"))
        print(_info(f"Would fetch    : {len(to_fetch)}"))
        for g,r in to_fetch[:20]: print(f"    {_cyan(g)}  {_dim(r)}")
        if len(to_fetch)>20: print(_dim(f"    \u2026 and {len(to_fetch)-20} more"))
        print(_yellow("\n  [DRY RUN] No changes made."))
        sys.exit(0)
    else:
        cache = GSTINNameCache(auto_fetch=True, prefer_portal=args.refresh_all,
                               log_fn=lambda m: None)
        all_g = list(gstin_sources.keys())
        to_fetch = [g for g in all_g if g not in cache._data or
                    (args.refresh_all and cache._data[g].get("source")
                     not in ("portal","portal_selenium"))]
        already_ok = [g for g in all_g if g not in to_fetch]
        print(_info(f"Already cached : {len(already_ok)}"))
        print(_info(f"Need portal    : {len(to_fetch)}  (workers={args.workers})"))

        fetched_ok = 0; failed_list = []
        if to_fetch:
            print()
            prog = _Progress(len(to_fetch), "Fetching")
            lock = threading.Lock()
            def _do(gstin):
                ok = _fetch_retry(cache, gstin, args.max_retries)
                return gstin, ok
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                for fut in as_completed({pool.submit(_do,g):g for g in to_fetch}):
                    g, ok = fut.result()
                    with lock:
                        if ok: fetched_ok += 1
                        else:  failed_list.append(g)
                    prog.update(1)
            prog.finish()
            print(_ok(f"Fetched: {fetched_ok}") +
                  ("  "+_warn(f"Failed: {len(failed_list)}") if failed_list else ""))
            if failed_list:
                print(_dim("  (Failed GSTINs will use CustomerMaster / Tally CSV names)"))
        else:
            print(_ok("All GSTINs already cached."))

        cache.save()
        cache_data = dict(cache._data)

    # ── Optional integrity check ──────────────────────────────────────────────
    if args.verify_cache and cache_data:
        statuses = verify_cache(cache_data, gstin_sources)
        ok_c  = sum(1 for v in statuses.values() if v=="ok")
        st_c  = sum(1 for v in statuses.values() if v=="stale")
        ms_c  = sum(1 for v in statuses.values() if v=="missing")
        un_c  = sum(1 for v in statuses.values() if v=="unnamed")
        print()
        print(_bold("  Cache integrity:"))
        print(_ok(f" OK       : {ok_c}"))
        if st_c: print(_warn(f" Stale (>{_STALE_DAYS}d) : {st_c}"))
        if ms_c: print(_err( f" Missing         : {ms_c}"))
        if un_c: print(_warn(f" Unnamed         : {un_c}"))
        if st_c or ms_c or un_c:
            print(_dim("  Tip: run --refresh-all to re-fetch stale/unnamed entries"))
    print()

    # ── Step 3: Export Excel ──────────────────────────────────────────────────
    if not args.no_export:
        print(_bold("  [3/3]  Exporting party master Excel \u2026"))
        ts = datetime.now().strftime("%Y%m%d_%H%M")
        out_path = Path(args.out) if args.out else \
                   script_dir / f"GSTR1_Party_Master_{ts}.xlsx"
        export_party_master(gstin_sources, cache_data, out_path,
                            statuses=statuses or None)
    else:
        print(_dim("  [3/3]  Excel export skipped (--no-export)"))

    # ── Summary ───────────────────────────────────────────────────────────────
    named   = sum(1 for g in gstin_sources if
                  (cache_data.get(g) or {}).get("legal_name") or
                  (cache_data.get(g) or {}).get("trade_name"))
    unnamed_c = len(gstin_sources) - named
    print()
    print(_bold("=" * 66))
    print(_bold("  COMPLETE"))
    print("=" * 66)
    print(f"  Total parties  : {_bold(str(len(gstin_sources)))}")
    print(f"  Named          : {_green(str(named))}")
    if unnamed_c:
        print(f"  Unnamed        : {_yellow(str(unnamed_c))}")
        for g in sorted(g for g in gstin_sources if not
                        ((cache_data.get(g) or {}).get("legal_name") or
                         (cache_data.get(g) or {}).get("trade_name")))[:12]:
            print(_dim(f"    {g}"))
        if unnamed_c > 12: print(_dim(f"    \u2026 and {unnamed_c-12} more"))
    print()
    if CACHE_AVAILABLE:
        print(f"  Cache file     : {_cyan('gstin_name_cache.json')}"
              f"  ({len(cache_data)} total entries)")
    print()
    print(_dim("  gstr1_fy_v5.py will now read all party names INSTANTLY."))
    print("=" * 66)
    print()

if __name__ == "__main__":
    main()
