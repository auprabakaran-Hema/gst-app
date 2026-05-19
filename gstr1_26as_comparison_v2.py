"""
================================================================================
  GSTR-1  vs  Form 26AS  COMPARISON  —  v2.0
  ============================================
  Reads the FY Excel already produced by gst_suite (via gstr1_fy_v5) and
  compares it against Form 26AS TDS data.

  WHY v2 IS BETTER THAN v1:
  ──────────────────────────
  gst_suite already parses every GSTR-1 ZIP and builds:
    • Sheet 25_GSTIN_Annual_Summary  →  FY totals per buyer GSTIN
    • Sheet 26_Master_All_Invoices   →  Every B2B invoice + credit/debit note

  So instead of re-parsing ZIPs (slow, error-prone), v2 reads directly from
  the FY Excel.  This means:
    ✓  All gst_suite logic (watermark cleanup, name cache, CDN sign) is already applied
    ✓  Buyer names from CustomerMaster.xlsx are already embedded
    ✓  Runs in seconds (Excel read vs ZIP extraction)
    ✓  Single source of truth — no discrepancy between this report and gst_suite output

  GSTR-1A NOTE:
  ─────────────
  GSTR-1A PDFs are section-level summaries only (no GSTIN/bill-wise breakdown).
  They are read separately and shown as monthly totals in a dedicated sheet.
  GSTR-1A amended B2B values are added to the company-wise summary as a lump-sum
  correction column so the full picture is visible.

  OUTPUT EXCEL — 5 Sheets:
  ──────────────────────────
  1. Company_Wise_Summary   — Per buyer GSTIN: GSTR-1 taxable | GSTR-1A correction |
                              26AS gross payment | TDS | Difference | Match Status
  2. Bill_Wise_Detail       — Every invoice / CDN from Sheet 26 of FY Excel,
                              grouped by Period → GSTIN, with 26AS match status
  3. GSTR1A_Monthly         — GSTR-1A PDF section totals month by month
  4. 26AS_TDS_Detail        — All 26AS deductors + transaction detail
  5. Reconciliation_Notes   — Mismatches, missing invoices, action checklist

  INPUT FILES (client folder):
  ─────────────────────────────
  GSTR1_FY_*.xlsx            →  FY Excel from gst_suite / gstr1_fy_v5   ← PRIMARY
  GSTR1A_<Month>_<Year>.pdf  →  Monthly GSTR-1A PDFs
  *26as*.pdf / *26AS*.pdf    →  Form 26AS from TRACES
  CustomerMaster.xlsx        →  Buyer GSTIN→Name map (fallback for missing names)

  USAGE:
  ──────
  # Standalone
  python gstr1_26as_comparison_v2.py --folder "C:/GST/Client" \
         --name "ACME PVT LTD" --gstin "33AABCA1234X1ZX" --pan "AABCA1234X" \
         --fy "2024-25"

  # From gst_suite or any script
  from gstr1_26as_comparison_v2 import run_26as_gstr1_comparison
  run_26as_gstr1_comparison(
      client_dir="...", client_name="...", gstin="...", pan="...",
      fy="2024-25", cache=cache_instance
  )
================================================================================
"""

from __future__ import annotations

import json, re, sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ── Optional deps ──────────────────────────────────────────────────────────────
MISSING = []
try:    import pdfplumber
except ImportError: MISSING.append("pdfplumber")
try:
    from openpyxl import Workbook, load_workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
except ImportError: MISSING.append("openpyxl")

if MISSING:
    print(f"Missing packages: pip install {' '.join(MISSING)}")
    sys.exit(1)

# ── Colour palette ─────────────────────────────────────────────────────────────
NAVY   = "1F3864"; BLUE  = "2E75B6"; TEAL  = "1D6A72"; PURPLE = "7030A0"
WHITE  = "FFFFFF"; LGRAY = "F2F2F2"; DGRAY = "D6DCE4"
GREEN  = "C6EFCE"; DKGRN = "276221"
AMBER  = "FFEB9C"; DKAMB = "9C6500"
RED_BG = "FFC7CE"; DKRED = "9C0006"
ORANGE = "FCE4D6"
ALT1   = "FFFFFF"; ALT2  = "F2F2F2"
R1A_BG = "D6E4F0"    # light blue — GSTR-1A columns
NUM_FMT = "#,##0.00"

_MON2NUM = {
    "january":"01","february":"02","march":"03","april":"04",
    "may":"05","june":"06","july":"07","august":"08",
    "september":"09","october":"10","november":"11","december":"12",
    "jan":"01","feb":"02","mar":"03","apr":"04","jun":"06",
    "jul":"07","aug":"08","sep":"09","oct":"10","nov":"11","dec":"12",
}
_FY_MON_ORDER = {
    "april":1,"may":2,"june":3,"july":4,"august":5,"september":6,
    "october":7,"november":8,"december":9,"january":10,"february":11,"march":12,
    "apr":1,"may":2,"jun":3,"jul":4,"aug":5,"sep":6,
    "oct":7,"nov":8,"dec":9,"jan":10,"feb":11,"mar":12,
}


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL STYLE HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _f(h):
    return PatternFill("solid", fgColor=h)

def _fn(bold=False, color="000000", size=9):
    return Font(name="Arial", bold=bold, color=color, size=size)

def _bd():
    s = Side(style="thin")
    return Border(left=s, right=s, top=s, bottom=s)

def _al(h="left", wrap=False):
    return Alignment(horizontal=h, vertical="center", wrap_text=wrap)

def _c(ws, r, col, val, bg=ALT1, bold=False, fg="000000",
       align="left", numfmt=None, size=9):
    c = ws.cell(row=r, column=col, value=val)
    c.font      = _fn(bold, fg, size)
    c.fill      = _f(bg)
    c.border    = _bd()
    c.alignment = _al(align,
                      wrap=(align == "left" and isinstance(val, str)
                            and len(str(val or "")) > 35))
    if numfmt and isinstance(val, (int, float)):
        c.number_format = numfmt
    return c

def _title(ws, txt, nc, bg=NAVY, size=11):
    ws.merge_cells(f"A1:{get_column_letter(nc)}1")
    c = ws["A1"]
    c.value     = txt
    c.font      = _fn(True, WHITE, size)
    c.fill      = _f(bg)
    c.alignment = _al("center")
    c.border    = _bd()
    ws.row_dimensions[1].height = 28

def _hdr(ws, cols, row=2, bg=NAVY):
    for ci, (h, w) in enumerate(cols, 1):
        c = ws.cell(row=row, column=ci, value=h)
        c.font      = _fn(True, WHITE, 9)
        c.fill      = _f(bg)
        c.alignment = _al("center", wrap=True)
        c.border    = _bd()
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[row].height = 24

def _sep(ws, r, lbl, nc, bg=BLUE):
    ws.merge_cells(f"A{r}:{get_column_letter(nc)}{r}")
    c = ws.cell(row=r, column=1, value=lbl)
    c.font      = _fn(True, WHITE, 9)
    c.fill      = _f(bg)
    c.alignment = _al("left")
    c.border    = _bd()
    ws.row_dimensions[r].height = 18

def _tot(ws, r, vals, bg=DGRAY, fgc="000000"):
    for ci, v in enumerate(vals, 1):
        c = ws.cell(row=r, column=ci, value=v)
        is_num = isinstance(v, (int, float))
        c.font      = _fn(True, fgc, 9)
        c.fill      = _f(bg)
        c.alignment = _al("right" if is_num else "left")
        c.border    = _bd()
        if is_num:
            c.number_format = NUM_FMT
    ws.row_dimensions[r].height = 18

def _note(ws, r, txt, nc, bg=AMBER, fg=DKAMB):
    ws.merge_cells(f"A{r}:{get_column_letter(nc)}{r}")
    c = ws.cell(row=r, column=1, value=txt)
    c.font      = _fn(False, fg, 8)
    c.fill      = _f(bg)
    c.alignment = _al("left", wrap=True)
    c.border    = _bd()
    ws.row_dimensions[r].height = 26


# ══════════════════════════════════════════════════════════════════════════════
#  DATA HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _n(v):
    try:
        return round(float(str(v or 0).replace(",", "").replace("₹", "").strip()), 2)
    except Exception:
        return 0.0

def _s(v):
    if v is None: return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "") else s

def _clean_gstin(g):
    return re.sub(r"[^A-Z0-9]", "", str(g or "").strip().upper())

def _period_sort(p: str):
    """Apr-2025 or April-2025 → FY sort key."""
    m = re.match(r"([A-Za-z]+)[-\s](\d{4})", str(p))
    if not m: return (9999, 99)
    mon = m.group(1)[:3].lower()
    yr  = int(m.group(2))
    fy  = _FY_MON_ORDER.get(mon, 99)
    return (yr, fy)

def _clean_name(nm: str) -> str:
    """Remove _x000D_ and similar Excel artefacts from names."""
    if not nm: return ""
    nm = re.sub(r"_x000D_", "", nm)
    nm = re.sub(r"\s+", " ", nm)
    return nm.strip()


# ══════════════════════════════════════════════════════════════════════════════
#  CUSTOMER MASTER LOADER  (fallback buyer names)
# ══════════════════════════════════════════════════════════════════════════════

def load_customer_master(client_dir: Path, log) -> dict[str, str]:
    """
    Load GSTIN → Name map from CustomerMaster.xlsx.
    Returns {} if file not found or missing GSTIN column.
    """
    cm_path = client_dir / "CustomerMaster.xlsx"
    if not cm_path.exists():
        for alt in ("customer_master.xlsx", "GSTIN_Names.xlsx"):
            if (client_dir / alt).exists():
                cm_path = client_dir / alt
                break
        else:
            return {}

    name_map: dict[str, str] = {}
    try:
        wb = load_workbook(str(cm_path), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
        if not rows or len(rows) < 2:
            return {}

        hdrs = [str(c or "").strip().upper() for c in rows[0]]

        def _col(*names):
            for n in names:
                if n in hdrs: return hdrs.index(n)
            return -1

        ci_g = _col("GSTIN/UIN", "GSTIN", "GST NO", "GSTIN NO")
        ci_n = _col("PARTICULARS", "NAME", "COMPANY NAME", "TRADE NAME", "LEGAL NAME")

        if ci_g == -1:
            return {}

        for row in rows[1:]:
            g = _clean_gstin(str(row[ci_g] or "") if ci_g < len(row) else "")
            n = _clean_name(str(row[ci_n] or "") if ci_n != -1 and ci_n < len(row) else "")
            if len(g) == 15 and n:
                name_map[g] = n

        log(f"  📋 CustomerMaster: {len(name_map)} GSTIN→name entries loaded")
    except Exception as e:
        log(f"  ⚠ CustomerMaster load error: {e}")

    return name_map


# ══════════════════════════════════════════════════════════════════════════════
#  FY EXCEL READER  (from gst_suite / gstr1_fy_v5 output)
# ══════════════════════════════════════════════════════════════════════════════

class BillRow:
    """One data row from Sheet 26 (Master All Invoices)."""
    __slots__ = ("period","supply_type","gstin","buyer_name",
                 "doc_no","doc_date","doc_type",
                 "taxable","igst","cgst","sgst","total_tax","inv_val",
                 "note_type","note_no","note_date")

    def __init__(self, period, supply_type, gstin, buyer_name,
                 doc_no, doc_date, doc_type,
                 taxable, igst, cgst, sgst, total_tax, inv_val,
                 note_type="", note_no="", note_date=""):
        self.period      = period
        self.supply_type = supply_type
        self.gstin       = gstin
        self.buyer_name  = buyer_name
        self.doc_no      = doc_no
        self.doc_date    = doc_date
        self.doc_type    = doc_type
        self.taxable     = taxable
        self.igst        = igst
        self.cgst        = cgst
        self.sgst        = sgst
        self.total_tax   = total_tax
        self.inv_val     = inv_val
        self.note_type   = note_type
        self.note_no     = note_no
        self.note_date   = note_date


def _is_data_row(row_vals) -> bool:
    """Return True if this row is a real data row (not a header / subtotal / separator)."""
    if not row_vals or not any(row_vals):
        return False
    first = _s(row_vals[0])
    # Skip serial-number-less rows, subtotal rows, separator rows
    if not first:
        return False
    # Subtotal / separator rows start with spaces or ✦ ★ or contain "TOTAL"
    if first.startswith(("  ", "✦", "★")) or "TOTAL" in first.upper():
        return False
    # Must have a numeric serial number in col 0
    try:
        int(float(first))
        return True
    except (ValueError, TypeError):
        return False


def read_fy_excel(client_dir: Path, log,
                  name_map: dict[str, str] | None = None,
                  cache=None) -> tuple[list[BillRow], dict[str, dict]]:
    """
    Read the FY Excel produced by gst_suite (gstr1_fy_v5).

    Returns:
        bill_rows   : list[BillRow]  — from sheet 26_Master_All_Invoices
        gstin_totals: dict[gstin → {taxable, igst, cgst, sgst, buyer_name}]
                      — from sheet 25_GSTIN_Annual_Summary  (cross-check)
    """
    # ── Locate the FY Excel ───────────────────────────────────────────────────
    fy_excels = sorted(
        list(client_dir.glob("GSTR1_FY_*.xlsx")) +
        list(client_dir.glob("GSTR1_FY_*.xlsm"))
    )
    # FIX v10.10: also search subfolders (e.g. GST Automation/) when client root is passed
    if not fy_excels:
        fy_excels = sorted(
            list(client_dir.rglob("GSTR1_FY_*.xlsx")) +
            list(client_dir.rglob("GSTR1_FY_*.xlsm"))
        )
    if not fy_excels:
        log("  ⚠ No GSTR1_FY_*.xlsx found. Run gst_suite first to generate the FY Excel.")
        return [], {}

    fy_path = fy_excels[-1]   # Latest if multiple
    log(f"  📊 Reading FY Excel: {fy_path.name}")

    try:
        wb = load_workbook(str(fy_path), read_only=True, data_only=True)
    except Exception as e:
        log(f"  ❌ Cannot open FY Excel: {e}")
        return [], {}

    sheet_names = wb.sheetnames
    log(f"  📑 Sheets available: {len(sheet_names)}")

    # ── Read Sheet 26 — Master All Invoices ───────────────────────────────────
    master_ws = None
    for sn in sheet_names:
        if "Master" in sn or "26_" in sn or "master" in sn.lower():
            master_ws = wb[sn]
            log(f"  📋 Bill-wise sheet: '{sn}'")
            break

    bill_rows: list[BillRow] = []
    buyer_name_map: dict[str, str] = {}   # gstin → name collected from excel

    if master_ws is None:
        log("  ⚠ Sheet 26_Master_All_Invoices not found — bill-wise detail will be empty")
    else:
        # Sheet 26 column layout (1-indexed Excel columns):
        # 1=Sr, 2=Period, 3=SupplyType, 4=BuyerGSTIN, 5=InvNo, 6=InvDate,
        # 7=InvType, 8=POS, 9=RevChg, 10=Rate, 11=TaxableVal,
        # 12=IGST, 13=CGST, 14=SGST, 15=TotalTax, 16=InvVal,
        # 17=NoteType, 18=NoteNo, 19=NoteDate
        skip_header = True   # skip first 3 rows (title, trader, header)
        header_rows_skipped = 0
        for row in master_ws.iter_rows(values_only=True):
            if header_rows_skipped < 3:
                header_rows_skipped += 1
                continue

            if not _is_data_row(row):
                continue

            # col indices: 0-based
            period      = _s(row[1])  if len(row) > 1  else ""
            supply_type = _s(row[2])  if len(row) > 2  else ""
            gstin       = _clean_gstin(_s(row[3]))
            doc_no      = _s(row[4])  if len(row) > 4  else ""
            doc_date    = _s(row[5])  if len(row) > 5  else ""
            doc_type    = _s(row[6])  if len(row) > 6  else ""
            taxable     = _n(row[10]) if len(row) > 10 else 0.0
            igst        = _n(row[11]) if len(row) > 11 else 0.0
            cgst        = _n(row[12]) if len(row) > 12 else 0.0
            sgst        = _n(row[13]) if len(row) > 13 else 0.0
            total_tax   = _n(row[14]) if len(row) > 14 else igst + cgst + sgst
            inv_val     = _n(row[15]) if len(row) > 15 else 0.0
            note_type   = _s(row[16]) if len(row) > 16 else ""
            note_no     = _s(row[17]) if len(row) > 17 else ""
            note_date   = _s(row[18]) if len(row) > 18 else ""

            if not gstin or (taxable == 0 and igst == 0):
                continue

            # Buyer name: from cache / name_map
            buyer_name = ""
            if cache:
                try: buyer_name = cache.get(gstin, "") or ""
                except Exception: pass
            if not buyer_name and name_map:
                buyer_name = name_map.get(gstin, "")
            if not buyer_name:
                buyer_name = buyer_name_map.get(gstin, "")

            if gstin and buyer_name:
                buyer_name_map[gstin] = buyer_name

            bill_rows.append(BillRow(
                period=period, supply_type=supply_type, gstin=gstin,
                buyer_name=buyer_name,
                doc_no=doc_no, doc_date=doc_date, doc_type=doc_type,
                taxable=taxable, igst=igst, cgst=cgst, sgst=sgst,
                total_tax=total_tax, inv_val=inv_val,
                note_type=note_type, note_no=note_no, note_date=note_date,
            ))

    log(f"  ✅ Bill rows loaded: {len(bill_rows)}")

    # ── Read Sheet 25 — GSTIN Annual Summary ─────────────────────────────────
    gstin_ws = None
    for sn in sheet_names:
        if "GSTIN" in sn.upper() and ("25" in sn or "Summary" in sn or "Annual" in sn):
            gstin_ws = wb[sn]
            log(f"  📋 GSTIN summary sheet: '{sn}'")
            break

    gstin_totals: dict[str, dict] = {}

    if gstin_ws is None:
        log("  ⚠ Sheet 25_GSTIN_Annual_Summary not found — building from bill rows")
        # Build from bill_rows as fallback
        for br in bill_rows:
            if not br.gstin: continue
            if br.gstin not in gstin_totals:
                gstin_totals[br.gstin] = {
                    "buyer_name": br.buyer_name, "taxable": 0.0,
                    "igst": 0.0, "cgst": 0.0, "sgst": 0.0,
                }
            gstin_totals[br.gstin]["taxable"] += br.taxable
            gstin_totals[br.gstin]["igst"]    += br.igst
            gstin_totals[br.gstin]["cgst"]    += br.cgst
            gstin_totals[br.gstin]["sgst"]    += br.sgst
            if not gstin_totals[br.gstin]["buyer_name"] and br.buyer_name:
                gstin_totals[br.gstin]["buyer_name"] = br.buyer_name
    else:
        # Sheet 25 layout (0-indexed from row values):
        # col 0 = BuyerGSTIN, col 1 = Jan-Mar Total,
        # cols 2-13 = monthly taxable Apr…Mar,
        # col 14 = FY Total Taxable, col 15 = FY IGST, col 16 = FY CGST,
        # col 17 = FY SGST, col 18 = FY Tax, col 19 = FY Invoice Value
        header_skipped = 0
        for row in gstin_ws.iter_rows(values_only=True):
            if header_skipped < 3:
                header_skipped += 1
                continue
            if not row or not row[0]:
                continue
            gstin_v = _clean_gstin(_s(row[0]))
            if len(gstin_v) != 15:
                continue
            fy_tv = _n(row[14]) if len(row) > 14 else 0.0
            fy_ig = _n(row[15]) if len(row) > 15 else 0.0
            fy_cg = _n(row[16]) if len(row) > 16 else 0.0
            fy_sg = _n(row[17]) if len(row) > 17 else 0.0

            buyer_name = ""
            if cache:
                try: buyer_name = cache.get(gstin_v, "") or ""
                except Exception: pass
            if not buyer_name and name_map:
                buyer_name = name_map.get(gstin_v, "")
            if not buyer_name:
                buyer_name = buyer_name_map.get(gstin_v, "")

            gstin_totals[gstin_v] = {
                "buyer_name": buyer_name,
                "taxable": fy_tv,
                "igst":    fy_ig,
                "cgst":    fy_cg,
                "sgst":    fy_sg,
            }

        log(f"  ✅ GSTIN summary rows: {len(gstin_totals)}")

    # Fill buyer names into bill_rows from gstin_totals (second pass)
    for br in bill_rows:
        if not br.buyer_name and br.gstin in gstin_totals:
            br.buyer_name = gstin_totals[br.gstin]["buyer_name"]

    wb.close()
    return bill_rows, gstin_totals


# ══════════════════════════════════════════════════════════════════════════════
#  GSTR-1A PDF READER  (section-level summary — ported from gst_suite_v31)
# ══════════════════════════════════════════════════════════════════════════════

def _gstr1a_clean(text: str) -> str:
    text = re.sub("[–—‒―‐﹘﹣－\u2013\u2014]", "-", text)
    text = re.sub(r"(\d)([DELIF])(?=[,\d])", r"\1", text)
    text = re.sub(r"(?<![A-Za-z0-9])([DELIF])(?![A-Za-z0-9])", " ", text)
    while re.search(r"\d,\d", text):
        text = re.sub(r"(\d),(\d)", r"\1\2", text)
    return text

def _5vals(line: str):
    c = _gstr1a_clean(line)
    nums = [float(t) for t in re.findall(r"-?\d+\.\d+", c)]
    if not nums:
        nums = [float(t) for t in re.findall(r"-?\d+", c) if len(t) > 0]
    if len(nums) >= 5: return tuple(nums[-5:])
    if len(nums) == 4: return (nums[0], nums[1], nums[2], nums[3], 0.0)
    if len(nums) == 3: return (nums[0], 0.0, nums[1], nums[2], 0.0)
    if len(nums) == 2: return (nums[0], nums[1], 0.0, 0.0, 0.0)
    if len(nums) == 1: return (nums[0], 0.0, 0.0, 0.0, 0.0)
    return None

def extract_gstr1a_pdf(pdf_path: str, log=None) -> dict:
    """Parse GSTR-1A PDF → section totals dict (ported from gst_suite_v31)."""
    def _log(m):
        if log: log(f"    {m}")

    R = {
        "t4a_b2b_tx":0.0,"t4a_b2b_ig":0.0,"t4a_b2b_cg":0.0,"t4a_b2b_sg":0.0,
        "t9a_b2b_tx":0.0,"t9a_b2b_ig":0.0,"t9a_b2b_cg":0.0,"t9a_b2b_sg":0.0,
        "t9b_cdnr_tx":0.0,"t9b_cdnr_ig":0.0,"t9b_cdnr_cg":0.0,"t9b_cdnr_sg":0.0,
        "t9b_cdnur_tx":0.0,"t9b_cdnur_ig":0.0,
        "t9c_cdnra_tx":0.0,"t9c_cdnra_ig":0.0,"t9c_cdnra_cg":0.0,"t9c_cdnra_sg":0.0,
        "t5_b2cl_tx":0.0,"t5_b2cl_ig":0.0,
        "t6a_exp_tx":0.0,"t6a_exp_ig":0.0,
        "t6b_sez_tx":0.0,"t6b_sez_ig":0.0,
        "t7_b2cs_tx":0.0,"t7_b2cs_ig":0.0,"t7_b2cs_cg":0.0,"t7_b2cs_sg":0.0,
        "t8_nil_tx":0.0,"t8_exempt_tx":0.0,"t8_nongst_tx":0.0,
        "total_liab_tx":0.0,"total_liab_ig":0.0,"total_liab_cg":0.0,"total_liab_sg":0.0,
        "b2b_tx":0.0,"b2b_ig":0.0,"b2b_cg":0.0,"b2b_sg":0.0,
        "cdn_cr":0.0,"cdn_ig":0.0,"cdn_cg":0.0,"cdn_sg":0.0,
    }

    path = Path(pdf_path)
    if not path.exists():
        _log(f"Not found: {path.name}"); return R

    text = ""
    for lib in ("pdfplumber", "pypdf", "PyPDF2"):
        try:
            if lib == "pdfplumber":
                with pdfplumber.open(str(path)) as doc:
                    text = "\n".join(pg.extract_text() or "" for pg in doc.pages)
            elif lib == "pypdf":
                from pypdf import PdfReader
                with open(str(path), "rb") as f:
                    text = "\n".join(pg.extract_text() or "" for pg in PdfReader(f).pages)
            else:
                import PyPDF2
                with open(str(path), "rb") as f:
                    text = "\n".join(pg.extract_text() or "" for pg in PyPDF2.PdfReader(f).pages)
            if text.strip(): break
        except Exception: pass

    if not text.strip():
        _log(f"Could not read text: {path.name}"); return R

    text  = re.sub("[–—‒―‐﹘﹣－\u2013\u2014]", "-", text)
    lines = [ln.strip() for ln in text.split("\n")]

    SECS = [
        (r"^4A\s+-",                                   "4A"),
        (r"^9A\s+-.*table 4.*B2B Regular",             "9A_b2b"),
        (r"^9A\s+-.*reverse charge",                   "9A_rcm"),
        (r"^9A\s+-.*table 5.*B2CL",                    "9A_b2cl"),
        (r"^9A\s+-.*table 6A.*EXPWP",                  "9A_exp"),
        (r"^9A\s+-.*table 6B.*SEZWP",                  "9A_sez"),
        (r"^9B\s+-\s+Credit/Debit.*Registered.*CDNR",  "9B_cdnr"),
        (r"^9B\s+-\s+Credit/Debit.*Unregistered",      "9B_cdnur"),
        (r"^9C\s+-\s+Amended.*Registered.*CDNRA",      "9C_cdnra"),
        (r"^5\s+-\s+Taxable outward inter-state",       "5"),
        (r"^6A\s",                                      "6A"),
        (r"^6B\s+-",                                    "6B"),
        (r"^7-\s+Taxable",                              "7"),
        (r"^8\s+-\s+Nil",                               "8"),
    ]

    def _ms(ls):
        for pat, n in SECS:
            if re.search(pat, ls, re.IGNORECASE): return n
        return None

    def _is_total(ls):
        return bool(re.match(r"^(Total|Amended amount\s+-\s+Total)", ls, re.IGNORECASE))

    def _store(sec, nums):
        if nums is None: return
        tx, ig, cg, sg, _ = nums
        mapping = {
            "4A":      ("t4a_b2b_tx","t4a_b2b_ig","t4a_b2b_cg","t4a_b2b_sg"),
            "9A_b2b":  ("t9a_b2b_tx","t9a_b2b_ig","t9a_b2b_cg","t9a_b2b_sg"),
            "9B_cdnr": ("t9b_cdnr_tx","t9b_cdnr_ig","t9b_cdnr_cg","t9b_cdnr_sg"),
            "9B_cdnur":("t9b_cdnur_tx","t9b_cdnur_ig",None,None),
            "9C_cdnra":("t9c_cdnra_tx","t9c_cdnra_ig","t9c_cdnra_cg","t9c_cdnra_sg"),
            "5":       ("t5_b2cl_tx","t5_b2cl_ig",None,None),
            "6A":      ("t6a_exp_tx","t6a_exp_ig",None,None),
            "6B":      ("t6b_sez_tx","t6b_sez_ig",None,None),
            "7":       ("t7_b2cs_tx","t7_b2cs_ig","t7_b2cs_cg","t7_b2cs_sg"),
        }
        m = mapping.get(sec)
        if not m: return
        for i, k in enumerate(m):
            if k: R[k] = [tx, ig, cg, sg][i]

    sec = None; stored = set(); hsn_v = None

    for idx, ls in enumerate(lines):
        if not ls or ls in ("D","E","L","I","F"): continue
        ns = _ms(ls)
        if ns: sec = ns; continue

        if "Total Liability" in ls and "Reverse charge" in ls:
            p = _5vals(ls)
            if p:
                R["total_liab_tx"]=p[0]; R["total_liab_ig"]=p[1]
                R["total_liab_cg"]=p[2]; R["total_liab_sg"]=p[3]
            continue

        if re.match(r"^Total\s+\d+\s+NA\b", ls) and hsn_v is None:
            p = _5vals(ls)
            if p and abs(p[0]) > 0: hsn_v = p

        if sec in ("9B_cdnr","9B_cdnur") and "Net off debit" in ls and sec not in stored:
            combined = ls
            if idx+1 < len(lines):
                nxt = lines[idx+1].strip()
                if nxt and not _ms(nxt) and not _is_total(nxt):
                    combined = combined + " " + nxt
            p = _5vals(combined)
            if p: _store(sec, p); stored.add(sec)
            continue

        if sec and sec not in stored and _is_total(ls):
            combined = ls
            if idx+1 < len(lines):
                nxt = lines[idx+1].strip()
                if nxt and not _ms(nxt) and not _is_total(nxt) \
                        and not nxt.startswith("Net differential"):
                    combined = combined + " " + nxt
            p = _5vals(combined)
            if p and any(abs(x) > 0 for x in p[:4]):
                _store(sec, p); stored.add(sec)

    if R["t4a_b2b_tx"] == 0 and R["t9a_b2b_tx"] == 0 and hsn_v and hsn_v[0] > 0:
        R["t4a_b2b_tx"]=hsn_v[0]; R["t4a_b2b_ig"]=abs(hsn_v[1])
        R["t4a_b2b_cg"]=abs(hsn_v[2]); R["t4a_b2b_sg"]=abs(hsn_v[3])

    R["b2b_tx"] = round(R["t9a_b2b_tx"] + R["t4a_b2b_tx"], 2)
    R["b2b_ig"] = round(R["t9a_b2b_ig"] + R["t4a_b2b_ig"], 2)
    R["b2b_cg"] = round(R["t9a_b2b_cg"] + R["t4a_b2b_cg"], 2)
    R["b2b_sg"] = round(R["t9a_b2b_sg"] + R["t4a_b2b_sg"], 2)
    R["cdn_cr"]  = abs(R["t9b_cdnr_tx"])
    R["cdn_ig"]  = abs(R["t9b_cdnr_ig"])
    R["cdn_cg"]  = abs(R["t9b_cdnr_cg"])
    R["cdn_sg"]  = abs(R["t9b_cdnr_sg"])

    _log(f"{path.name}: B2B ₹{R['b2b_tx']:,.2f}  CDNR ₹{R['cdn_cr']:,.2f}  "
         f"TotalLiab ₹{R['total_liab_tx']:,.2f}")
    return R


def read_gstr1a_pdfs(client_dir: Path, log) -> dict[str, dict]:
    """Read all GSTR1A_<Month>_<Year>.pdf files → {period: vals}."""
    pdfs = sorted(client_dir.glob("GSTR1A_*.pdf"))
    if not pdfs:
        log("  ℹ No GSTR1A_*.pdf files found")
        return {}
    log(f"  📄 Found {len(pdfs)} GSTR-1A PDF(s)")
    result = {}
    for pdf in pdfs:
        parts = pdf.stem.split("_")    # ["GSTR1A","April","2025"]
        if len(parts) < 3: continue
        mon  = parts[1][:3].capitalize()
        yr   = parts[2]
        period = f"{mon}-{yr}"
        vals = extract_gstr1a_pdf(str(pdf), log=log)
        result[period] = vals
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  FORM 26AS PDF READER
# ══════════════════════════════════════════════════════════════════════════════

class TDS26ASEntry:
    __slots__ = ("name","tan","sections","gross_paid",
                 "tds_deducted","tds_deposited","transactions")

    def __init__(self, name, tan, sections, gross_paid,
                 tds_deducted, tds_deposited, transactions):
        self.name          = name
        self.tan           = tan
        self.sections      = sections
        self.gross_paid    = gross_paid
        self.tds_deducted  = tds_deducted
        self.tds_deposited = tds_deposited
        self.transactions  = transactions    # list[dict]


def parse_26as_pdf(pdf_path: str, log) -> list[TDS26ASEntry]:
    path = Path(pdf_path)
    if not path.exists():
        log(f"  ⚠ 26AS PDF not found: {pdf_path}")
        return []

    log(f"  📄 Parsing 26AS: {path.name}")
    try:
        with pdfplumber.open(str(path)) as pdf:
            all_tables = []
            for pg in pdf.pages:
                tbls = pg.extract_tables()
                if tbls: all_tables.extend(tbls)
    except Exception as e:
        log(f"  ⚠ pdfplumber error: {e}")
        return []

    raw: list[dict] = []
    current: dict | None = None

    for table in all_tables:
        if not table: continue
        hdr    = [_s(c) for c in (table[0] or [])]
        hdr_lc = " ".join(hdr).lower()

        # ── Deductor summary table ────────────────────────────────────────────
        if ("name of deductor" in hdr_lc and
                ("total amount paid" in hdr_lc or "total tds" in hdr_lc)):
            for row in table[1:]:
                if not row: continue
                vals = [_s(c) for c in row]
                if not any(vals): continue
                if not (vals[0] and vals[0].isdigit()): continue
                has_tan = any(re.match(r"[A-Z]{4}\d{5}[A-Z]$", v) for v in vals if v)
                if not has_tan: continue
                name = tan = ""
                for v in vals:
                    if re.match(r"[A-Z]{4}\d{5}[A-Z]$", v):
                        tan = v
                    elif (len(v) > 4 and not re.match(r"[\d,\.]+$", v)
                          and v != vals[0]):
                        if not name: name = v[:80]
                nums  = [_n(v) for v in vals if _n(v) != 0]
                gross = nums[0] if nums else 0.0
                ded   = nums[1] if len(nums) > 1 else 0.0
                dep   = nums[2] if len(nums) > 2 else ded
                current = {"name":name,"tan":tan,"gross":gross,
                           "deducted":ded,"deposited":dep,
                           "transactions":[],"sections":set()}
                raw.append(current)

        # ── Transaction detail rows ───────────────────────────────────────────
        elif ("section" in hdr_lc and "transaction date" in hdr_lc):
            for row in table:
                if not row: continue
                vals = [_s(c) for c in row]
                if not any(vals): continue
                sec = vals[0] if vals else ""
                if not sec or not re.match(r"19\d|20[67]", sec): continue
                nums = [_n(v) for v in vals if _n(v) != 0]
                txn  = {
                    "section":       sec,
                    "date":          vals[1] if len(vals) > 1 else "",
                    "status":        vals[2] if len(vals) > 2 else "",
                    "gross":         nums[0] if nums else 0.0,
                    "tds_deducted":  nums[1] if len(nums) > 1 else 0.0,
                    "tds_deposited": nums[2] if len(nums) > 2 else
                                     (nums[1] if len(nums) > 1 else 0.0),
                }
                if current is not None:
                    current["transactions"].append(txn)
                    current["sections"].add(sec)

        # ── TCS table (Part VI) ───────────────────────────────────────────────
        elif ("name of collector" in hdr_lc and "tcs" in hdr_lc):
            for row in table[1:]:
                if not row: continue
                vals = [_s(c) for c in row]
                if not any(vals) or not vals[0].isdigit(): continue
                name = tan = ""
                for v in vals:
                    if re.match(r"[A-Z]{4}\d{5}[A-Z]$", v): tan = v
                    elif len(v) > 4 and not re.match(r"[\d,\.]+$", v) and v != vals[0]:
                        if not name: name = v[:80]
                nums = [_n(v) for v in vals if _n(v) != 0]
                raw.append({
                    "name":name,"tan":tan,
                    "gross":   nums[0] if nums else 0.0,
                    "deducted":nums[1] if len(nums) > 1 else 0.0,
                    "deposited":nums[2] if len(nums) > 2 else
                                (nums[1] if len(nums) > 1 else 0.0),
                    "transactions":[],"sections":{"206C"},
                })

    entries = [
        TDS26ASEntry(
            name          = e["name"],
            tan           = e["tan"],
            sections      = ", ".join(sorted(e["sections"])),
            gross_paid    = e["gross"],
            tds_deducted  = e["deducted"],
            tds_deposited = e["deposited"],
            transactions  = e["transactions"],
        )
        for e in raw
    ]
    total_tds = sum(x.tds_deposited for x in entries)
    log(f"  ✅ 26AS: {len(entries)} deductor(s) | Total TDS ₹{total_tds:,.2f}")
    return entries


# ══════════════════════════════════════════════════════════════════════════════
#  MATCHING ENGINE
#  Matches GSTR-1 buyers (by GSTIN from FY Excel) with 26AS deductors (by name)
# ══════════════════════════════════════════════════════════════════════════════

def _name_words(nm: str) -> set[str]:
    stop = {"pvt","ltd","private","limited","and","of","the","india","co",
            "company","services","enterprises","industries","solutions",
            "technologies","tech","sys","systems","works"}
    return set(re.findall(r"[a-z]+", nm.lower())) - stop

def build_company_map(
    gstin_totals: dict[str, dict],
    tds_entries:  list[TDS26ASEntry],
    log
) -> dict[str, dict]:
    """
    Merge GSTR-1 GSTIN totals with 26AS TDS entries.

    Match strategy:
      1. Exact GSTIN match (if 26AS entry has GSTIN field)
      2. Name-word overlap (≥2 significant words in common)
      3. Unmatched 26AS entries → keyed as "26AS_<TAN>"
      4. Unmatched GSTR-1 GSTINs → shown with 0 TDS

    Returns company_map: {key → rec}
    """
    company_map: dict[str, dict] = {}
    matched_gstins: set[str]   = set()
    matched_tans:   set[str]   = set()

    # Pre-build word-sets for GSTR-1 buyers
    r1_words: dict[str, set] = {
        g: _name_words(rec.get("buyer_name", ""))
        for g, rec in gstin_totals.items()
    }

    for te in tds_entries:
        ded_words = _name_words(te.name)
        best_g    = None
        best_score = 0

        if ded_words:
            for g, words in r1_words.items():
                if g in matched_gstins:
                    continue
                common = ded_words & words
                if len(common) >= 2 and len(common) > best_score:
                    best_score = len(common)
                    best_g = g

        key = best_g if best_g else f"26AS_{te.tan}"

        if key not in company_map:
            r1_rec = gstin_totals.get(key, {})
            company_map[key] = {
                "gstin":       key,
                "buyer_name":  r1_rec.get("buyer_name", "") or te.name,
                "r1_taxable":  r1_rec.get("taxable", 0.0),
                "r1_igst":     r1_rec.get("igst", 0.0),
                "r1_cgst":     r1_rec.get("cgst", 0.0),
                "r1_sgst":     r1_rec.get("sgst", 0.0),
                "gross_26as":  0.0,
                "tds_ded":     0.0,
                "tds_dep":     0.0,
                "sections":    "",
                "tan":         te.tan,
            }
            if best_g:
                matched_gstins.add(best_g)

        company_map[key]["gross_26as"] += te.gross_paid
        company_map[key]["tds_ded"]    += te.tds_deducted
        company_map[key]["tds_dep"]    += te.tds_deposited

        secs = set(filter(None, company_map[key]["sections"].split(", ")))
        secs |= set(filter(None, te.sections.split(", ")))
        company_map[key]["sections"] = ", ".join(sorted(secs))
        if not company_map[key]["tan"]:
            company_map[key]["tan"] = te.tan
        matched_tans.add(te.tan)

    # Add unmatched GSTR-1 buyers (no 26AS TDS entry)
    for g, rec in gstin_totals.items():
        if g not in matched_gstins and g not in company_map:
            company_map[g] = {
                "gstin":      g,
                "buyer_name": rec.get("buyer_name", ""),
                "r1_taxable": rec.get("taxable", 0.0),
                "r1_igst":    rec.get("igst", 0.0),
                "r1_cgst":    rec.get("cgst", 0.0),
                "r1_sgst":    rec.get("sgst", 0.0),
                "gross_26as": 0.0, "tds_ded": 0.0, "tds_dep": 0.0,
                "sections": "", "tan": "",
            }

    # Compute difference + status for each entry
    for key, rec in company_map.items():
        tv   = rec["r1_taxable"]
        g26  = rec["gross_26as"]
        diff = tv - g26
        rec["diff"] = diff

        if abs(diff) <= 1.0:
            rec["status"] = "✅ Match";             rec["sbg"] = GREEN
        elif tv == 0 and g26 > 0:
            rec["status"] = "⚠ Missing in GSTR-1"; rec["sbg"] = RED_BG
        elif g26 == 0 and tv > 0:
            rec["status"] = "ℹ No TDS Deducted";   rec["sbg"] = AMBER
        elif tv > 0 and abs(diff) / max(abs(tv), 1) < 0.05:
            rec["status"] = "≈ Near Match (<5%)";   rec["sbg"] = "E2EFDA"
        else:
            rec["status"] = "❌ Mismatch";           rec["sbg"] = ORANGE

    unmatched_26as = len([t for t in tds_entries if t.tan not in matched_tans])
    log(f"  Company groups: {len(company_map)}  |  Unmatched 26AS: {unmatched_26as}")
    return company_map


# ══════════════════════════════════════════════════════════════════════════════
#  EXCEL WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def _write_company_wise(wb, company_map, gstr1a_pdfs, client_name,
                        gstin, fy, log):
    ws = wb.create_sheet("Company_Wise_Summary")
    ws.sheet_view.showGridLines = False

    # GSTR-1A annual totals
    r1a_b2b = sum(v.get("b2b_tx", 0.0)        for v in gstr1a_pdfs.values())
    r1a_cdn = sum(v.get("cdn_cr", 0.0)         for v in gstr1a_pdfs.values())
    r1a_net = sum(v.get("total_liab_tx", 0.0)  for v in gstr1a_pdfs.values())

    COLS = [
        ("GSTIN (Buyer)",                  22),
        ("Buyer Name",                     34),
        ("TAN (26AS)",                     14),
        ("TDS Section(s)",                 16),
        ("GSTR-1 Taxable ₹\n(from FY Excel)",  20),
        ("GSTR-1 IGST ₹",                 14),
        ("GSTR-1 CGST ₹",                 14),
        ("GSTR-1 SGST ₹",                 14),
        ("GSTR-1 Total Tax ₹",            17),
        ("26AS Gross Payment ₹",           20),
        ("26AS TDS Deducted ₹",            18),
        ("26AS TDS Deposited ₹",           18),
        ("Difference ₹\n(GSTR-1 − 26AS)", 18),
        ("Match Status",                   22),
    ]
    NC = len(COLS)

    _title(ws,
           f"26AS vs GSTR-1 — Company-Wise Summary — {client_name} ({gstin}) — FY {fy}",
           NC)
    _hdr(ws, COLS, row=2)
    _note(ws, 3,
          "Source: GSTR-1 data read from FY Excel (gst_suite output). "
          "GSTR-1A totals shown separately below (PDF-only — no GSTIN-wise breakdown). "
          "Difference = GSTR-1 Taxable − 26AS Gross Payment.",
          NC)

    ri = 4
    sorted_recs = sorted(
        company_map.items(),
        key=lambda x: -(x[1]["r1_taxable"] + x[1]["gross_26as"])
    )

    gt = defaultdict(float)

    for g, rec in sorted_recs:
        bg   = ALT2 if ri % 2 == 0 else ALT1
        sbg  = rec.get("sbg", ALT1)
        tv   = rec["r1_taxable"]
        ig   = rec["r1_igst"]
        cg   = rec["r1_cgst"]
        sg   = rec["r1_sgst"]
        ttax = ig + cg + sg
        g26  = rec["gross_26as"]
        td   = rec["tds_ded"]
        tp   = rec["tds_dep"]
        dif  = rec["diff"]

        gt["tv"] += tv;  gt["ig"] += ig;  gt["cg"] += cg;  gt["sg"] += sg
        gt["ttax"] += ttax
        gt["g26"] += g26; gt["td"] += td;  gt["tp"] += tp;  gt["dif"] += dif

        disp_g = g if not g.startswith("26AS_") else "—"
        row_data = [
            (disp_g,            "left",   ALT1,  False),
            (rec["buyer_name"], "left",   ALT1,  False),
            (rec.get("tan",""), "left",   ALT1,  False),
            (rec.get("sections",""), "left", ALT1, False),
            (tv,   "right", bg,   False),
            (ig,   "right", bg,   False),
            (cg,   "right", bg,   False),
            (sg,   "right", bg,   False),
            (ttax, "right", bg,   True),
            (g26,  "right", bg,   False),
            (td,   "right", bg,   False),
            (tp,   "right", bg,   False),
            (dif,  "right", sbg,  True),
            (rec["status"], "center", sbg, True),
        ]
        for ci, (v, al, cbg, bold) in enumerate(row_data, 1):
            c = ws.cell(row=ri, column=ci, value=v)
            c.font = _fn(bold, "000000", 9)
            c.fill = _f(cbg); c.border = _bd()
            c.alignment = _al(al)
            if isinstance(v, float): c.number_format = NUM_FMT
        ws.row_dimensions[ri].height = 16
        ri += 1

    # Grand total
    _tot(ws, ri, [
        "GRAND TOTAL (GSTR-1 vs 26AS)", "", "", "",
        gt["tv"], gt["ig"], gt["cg"], gt["sg"], gt["ttax"],
        gt["g26"], gt["td"], gt["tp"], gt["dif"], "",
    ], bg=NAVY, fgc=WHITE); ri += 1

    # GSTR-1A lump-sum band
    _sep(ws, ri, "GSTR-1A ANNUAL TOTALS (PDF summary — no GSTIN-wise breakdown)", NC, bg=PURPLE)
    ri += 1
    for lbl, val in [
        ("GSTR-1A B2B Amendments (4A + 9A) ₹",         f"₹{r1a_b2b:,.2f}"),
        ("GSTR-1A CDNR Credit Notes (9B) ₹",            f"₹{r1a_cdn:,.2f}"),
        ("GSTR-1A Total Liability (Net) ₹",             f"₹{r1a_net:,.2f}"),
        ("Combined: GSTR-1 + GSTR-1A B2B − CDNR ₹",
         f"₹{gt['tv'] + r1a_b2b - r1a_cdn:,.2f}"),
    ]:
        ws.merge_cells(f"A{ri}:H{ri}")
        c = ws.cell(row=ri, column=1, value=lbl)
        c.font = _fn(True, "000000", 9); c.fill = _f(R1A_BG)
        c.alignment = _al("left"); c.border = _bd()
        ws.merge_cells(f"I{ri}:{get_column_letter(NC)}{ri}")
        c2 = ws.cell(row=ri, column=9, value=val)
        c2.font = _fn(True, NAVY, 9); c2.fill = _f(R1A_BG)
        c2.alignment = _al("right"); c2.border = _bd()
        ri += 1

    # Summary stats
    _sep(ws, ri, "MATCH STATUS SUMMARY", NC, bg=TEAL); ri += 1
    status_counts = defaultdict(int)
    for rec in company_map.values():
        status_counts[rec["status"]] += 1

    for status, count in sorted(status_counts.items()):
        ws.merge_cells(f"A{ri}:F{ri}")
        c = ws.cell(row=ri, column=1, value=status)
        c.font = _fn(False, "000000", 9); c.fill = _f(ALT2)
        c.alignment = _al("left"); c.border = _bd()
        ws.merge_cells(f"G{ri}:{get_column_letter(NC)}{ri}")
        c2 = ws.cell(row=ri, column=7, value=f"{count} company(s)")
        c2.font = _fn(True, NAVY, 9); c2.fill = _f(ALT1)
        c2.alignment = _al("center"); c2.border = _bd()
        ri += 1

    ws.freeze_panes = "A4"
    log(f"    Sheet 1 (Company_Wise_Summary): {len(company_map)} rows")


def _write_bill_wise(wb, bill_rows, company_map, client_name, gstin, fy, log):
    ws = wb.create_sheet("Bill_Wise_Detail")
    ws.sheet_view.showGridLines = False

    COLS = [
        ("Period",        11), ("Supply Type",    14), ("Buyer GSTIN",    22),
        ("Buyer Name",    32), ("Doc / Inv No.",  20), ("Date",           12),
        ("Doc Type",      16), ("Taxable ₹",      16), ("IGST ₹",         13),
        ("CGST ₹",        13), ("SGST ₹",         13), ("Total Tax ₹",    14),
        ("Inv Value ₹",   16), ("Match Status",   22),
    ]
    NC = len(COLS)

    _title(ws, f"Bill-Wise Detail (GSTR-1) — {client_name} ({gstin}) — FY {fy}", NC)
    _hdr(ws, COLS, row=2)
    _note(ws, 3,
          "Source: Sheet 26 (Master All Invoices) from GSTR1_FY_*.xlsx produced by gst_suite. "
          "Includes B2B invoices and all Credit/Debit Notes filed in GSTR-1. "
          "GSTR-1A data is summary-only — see sheet 'GSTR1A_Monthly'.",
          NC)

    sorted_rows = sorted(
        bill_rows,
        key=lambda x: (_period_sort(x.period), x.gstin, x.doc_date or "")
    )

    ri = 4
    cur_period = cur_gstin = None
    gt = defaultdict(float)

    for br in sorted_rows:
        if br.period != cur_period:
            _sep(ws, ri, f"▶  Period: {br.period}", NC, bg=TEAL)
            ri += 1; cur_period = br.period; cur_gstin = None

        if br.gstin != cur_gstin:
            nm = company_map.get(br.gstin, {}).get("buyer_name", "") or br.buyer_name
            _sep(ws, ri, f"    {br.gstin}  —  {nm}", NC, bg=BLUE)
            ri += 1; cur_gstin = br.gstin

        status = company_map.get(br.gstin, {}).get("status", "")
        sbg    = company_map.get(br.gstin, {}).get("sbg", ALT1)
        bg     = ALT2 if ri % 2 == 0 else ALT1

        gt["tv"] += br.taxable; gt["ig"] += br.igst
        gt["cg"] += br.cgst;   gt["sg"] += br.sgst
        gt["val"] += br.inv_val

        row_data = [
            (br.period,      "center", bg),
            (br.supply_type, "left",   bg),
            (br.gstin,       "left",   ALT1),
            (br.buyer_name,  "left",   ALT1),
            (br.doc_no or br.note_no, "left", ALT1),
            (br.doc_date or br.note_date, "center", bg),
            (br.doc_type or br.note_type, "left", bg),
            (br.taxable,   "right", bg),
            (br.igst,      "right", bg),
            (br.cgst,      "right", bg),
            (br.sgst,      "right", bg),
            (br.total_tax, "right", bg),
            (br.inv_val,   "right", bg),
            (status,       "center", sbg),
        ]
        for ci, (v, al, cbg) in enumerate(row_data, 1):
            c = ws.cell(row=ri, column=ci, value=v)
            c.font = _fn(False, "000000", 9)
            c.fill = _f(cbg); c.border = _bd()
            c.alignment = _al(al)
            if isinstance(v, float): c.number_format = NUM_FMT
        ws.row_dimensions[ri].height = 15
        ri += 1

    _tot(ws, ri, [
        "GRAND TOTAL", "", "", "", "", "", "",
        gt["tv"], gt["ig"], gt["cg"], gt["sg"],
        gt["ig"] + gt["cg"] + gt["sg"], gt["val"], "",
    ], bg=NAVY, fgc=WHITE)

    ws.freeze_panes = "A4"
    log(f"    Sheet 2 (Bill_Wise_Detail): {len(bill_rows)} rows")


def _write_gstr1a_monthly(wb, gstr1a_pdfs, client_name, gstin, fy, log):
    ws = wb.create_sheet("GSTR1A_Monthly")
    ws.sheet_view.showGridLines = False

    COLS = [
        ("Period",                11),
        ("4A B2B Taxable ₹",     17), ("4A B2B Tax ₹",          14),
        ("9A Amnd B2B Tax ₹",    19), ("9A Amnd B2B Tax ₹",     16),
        ("Total B2B ₹\n(4A+9A)", 16),
        ("9B CDNR ₹\n(Cr Notes)", 17),
        ("9B CDNUR ₹",            14),
        ("Total Liability ₹\n(Net Auth.)", 18),
        ("B2CS ₹",                13), ("Exports ₹",             13),
        ("SEZ ₹",                 13), ("B2CL ₹",                13),
    ]
    NC = len(COLS)

    _title(ws,
           f"GSTR-1A Monthly Totals (PDF Summary) — {client_name} ({gstin}) — FY {fy}",
           NC, bg=PURPLE)
    _hdr(ws, COLS, row=2, bg=PURPLE)
    _note(ws, 3,
          "GSTR-1A is a portal PDF — only section totals are available (no GSTIN/bill-wise). "
          "Total Liability = authoritative net value from portal. "
          "CDNR values shown as positive (absolute). "
          "4A = direct B2B filed in GSTR-1A. 9A = amendments to original GSTR-1 invoices.",
          NC, bg=R1A_BG, fg="1F3864")

    ri = 4
    ann = defaultdict(float)

    for period in sorted(gstr1a_pdfs.keys(), key=_period_sort):
        v   = gstr1a_pdfs[period]
        bg  = ALT2 if ri % 2 == 0 else ALT1
        t4a = v.get("t4a_b2b_tx", 0.0)
        t4x = v.get("t4a_b2b_ig",0.0)+v.get("t4a_b2b_cg",0.0)+v.get("t4a_b2b_sg",0.0)
        t9a = v.get("t9a_b2b_tx", 0.0)
        t9x = v.get("t9a_b2b_ig",0.0)+v.get("t9a_b2b_cg",0.0)+v.get("t9a_b2b_sg",0.0)
        b2b = v.get("b2b_tx", 0.0)
        cdn = v.get("cdn_cr", 0.0)
        cdu = v.get("t9b_cdnur_tx", 0.0)
        lib = v.get("total_liab_tx", 0.0)
        b2cs= v.get("t7_b2cs_tx", 0.0)
        exp = v.get("t6a_exp_tx", 0.0)
        sez = v.get("t6b_sez_tx", 0.0)
        b2cl= v.get("t5_b2cl_tx", 0.0)

        for k, val in [("t4a",t4a),("t4x",t4x),("t9a",t9a),("t9x",t9x),
                       ("b2b",b2b),("cdn",cdn),("cdu",cdu),("lib",lib),
                       ("b2cs",b2cs),("exp",exp),("sez",sez),("b2cl",b2cl)]:
            ann[k] += val

        row_vals = [period, t4a,t4x, t9a,t9x, b2b, cdn, cdu, lib,
                    b2cs, exp, sez, b2cl]
        for ci, val in enumerate(row_vals, 1):
            c = ws.cell(row=ri, column=ci, value=val)
            c.font = _fn(False, "000000", 9); c.fill = _f(bg)
            c.border = _bd()
            c.alignment = _al("center" if ci == 1 else "right")
            if isinstance(val, float): c.number_format = NUM_FMT
        ws.row_dimensions[ri].height = 16
        ri += 1

    if gstr1a_pdfs:
        _tot(ws, ri, ["ANNUAL TOTAL",
                      ann["t4a"],ann["t4x"],ann["t9a"],ann["t9x"],
                      ann["b2b"],ann["cdn"],ann["cdu"],ann["lib"],
                      ann["b2cs"],ann["exp"],ann["sez"],ann["b2cl"]],
             bg=PURPLE, fgc=WHITE)
    else:
        _note(ws, ri, "No GSTR1A_*.pdf files found in client folder.", NC)

    ws.freeze_panes = "A4"
    log(f"    Sheet 3 (GSTR1A_Monthly): {len(gstr1a_pdfs)} month(s)")


def _write_26as_detail(wb, tds_entries, client_name, pan, fy, log):
    ws = wb.create_sheet("26AS_TDS_Detail")
    ws.sheet_view.showGridLines = False

    COLS = [
        ("Sr.",  6),("Deductor / Collector Name",36),("TAN",13),
        ("Section(s)",14),("Gross Amount Paid ₹",19),
        ("TDS Deducted ₹",17),("TDS Deposited ₹",17),
        ("Txn Date",14),("Status",12),
        ("Txn Gross ₹",15),("Txn TDS Dep ₹",14),
    ]
    NC = len(COLS)
    _title(ws, f"Form 26AS — TDS/TCS Detail — {client_name} ({pan}) — FY {fy}", NC)
    _hdr(ws, COLS, row=2)

    ri = 3; total_gross = total_tds = 0.0

    for si, te in enumerate(tds_entries, 1):
        _sep(ws, ri,
             f"  {si}. {te.name}  |  TAN: {te.tan}  |  Section(s): {te.sections}",
             NC, bg=TEAL)
        ri += 1

        summary = [si, te.name, te.tan, te.sections,
                   te.gross_paid, te.tds_deducted, te.tds_deposited,
                   "DEDUCTOR TOTAL", "", "", ""]
        for ci, v in enumerate(summary, 1):
            c = ws.cell(row=ri, column=ci, value=v)
            c.font = _fn(True, "000000", 9); c.fill = _f(DGRAY)
            c.border = _bd()
            c.alignment = _al("right" if isinstance(v, float) else "left")
            if isinstance(v, float): c.number_format = NUM_FMT
        ws.row_dimensions[ri].height = 16; ri += 1

        total_gross += te.gross_paid; total_tds += te.tds_deposited

        for txn in te.transactions:
            bg = ALT2 if ri % 2 == 0 else ALT1
            row_out = [
                "", "", te.tan, txn.get("section",""), "", "", "",
                txn.get("date",""), txn.get("status",""),
                txn.get("gross", 0.0), txn.get("tds_deposited", 0.0),
            ]
            for ci, v in enumerate(row_out, 1):
                c = ws.cell(row=ri, column=ci, value=v)
                c.font = _fn(False, "555555", 8); c.fill = _f(bg)
                c.border = _bd()
                c.alignment = _al(
                    "right" if isinstance(v, float) else
                    ("center" if ci in (8, 9) else "left")
                )
                if isinstance(v, float): c.number_format = NUM_FMT
            ws.row_dimensions[ri].height = 14; ri += 1

    _tot(ws, ri, [
        "", "GRAND TOTAL", "", "",
        total_gross, total_tds, total_tds,
        "", "", "", "",
    ], bg=NAVY, fgc=WHITE)

    ws.freeze_panes = "A3"
    log(f"    Sheet 4 (26AS_TDS_Detail): {len(tds_entries)} deductors")


def _write_recon_notes(wb, company_map, gstr1a_pdfs, bill_rows,
                       client_name, fy, log):
    ws = wb.create_sheet("Reconciliation_Notes")
    ws.sheet_view.showGridLines = False

    NC = 7
    _title(ws, f"Reconciliation Notes & Action Items — {client_name} — FY {fy}", NC)
    COLS_N = [
        ("#",5),("Priority",10),("Category",20),
        ("GSTIN",22),("Buyer / Deductor",34),
        ("GSTR-1 ₹",15),("Detail / Action",36),
    ]
    _hdr(ws, COLS_N, row=2)

    ri = 3; item = 0

    def _row(vals, sbg=ALT2):
        nonlocal ri, item
        item += 1; vals[0] = item
        pbg = {
            "HIGH": RED_BG, "MEDIUM": AMBER, "LOW": GREEN,
        }.get(str(vals[1]), ALT1)
        for ci, v in enumerate(vals, 1):
            c = ws.cell(row=ri, column=ci, value=v)
            c.font = _fn(False, "000000", 9)
            c.fill = _f(pbg if ci == 2 else (sbg if ci > 3 else ALT1))
            c.border = _bd()
            c.alignment = _al("center" if ci <= 2 else "left", wrap=True)
        ws.row_dimensions[ri].height = 20; ri += 1

    # ── Mismatches ────────────────────────────────────────────────────────────
    _sep(ws, ri, "❌  AMOUNT MISMATCHES  (difference > 5% of GSTR-1 taxable)", NC, bg="C00000")
    ri += 1
    for g, rec in sorted(company_map.items(), key=lambda x: -abs(x[1]["diff"])):
        if "❌" not in rec["status"]: continue
        d = rec["diff"]
        _row([0,"HIGH","Amount Mismatch",
              g if not g.startswith("26AS_") else "—",
              rec["buyer_name"], f"₹{rec['r1_taxable']:,.2f}",
              f"Diff ₹{d:,.2f} | {'GSTR-1 > TDS base' if d > 0 else 'TDS base > GSTR-1'}"])

    # ── Missing in GSTR-1 ─────────────────────────────────────────────────────
    _sep(ws, ri, "⚠  IN 26AS BUT MISSING / ZERO IN GSTR-1", NC, bg="9C0006")
    ri += 1
    for g, rec in company_map.items():
        if "Missing" not in rec["status"]: continue
        _row([0,"HIGH","Missing Invoice",
              g if not g.startswith("26AS_") else "—",
              rec["buyer_name"], "₹0.00",
              f"26AS Gross ₹{rec['gross_26as']:,.2f} | TDS ₹{rec['tds_dep']:,.2f} | Sec {rec['sections']}"])

    # ── No TDS ────────────────────────────────────────────────────────────────
    _sep(ws, ri, "ℹ  GSTR-1 SALES WITH NO TDS IN 26AS", NC, bg=TEAL)
    ri += 1
    for g, rec in sorted(company_map.items(), key=lambda x: -x[1]["r1_taxable"]):
        if "No TDS" not in rec["status"]: continue
        _row([0,"MEDIUM","No TDS Deducted", g, rec["buyer_name"],
              f"₹{rec['r1_taxable']:,.2f}",
              "Verify if TDS applicable (194C works / 194J prof / 194Q goods)"])

    # ── GSTR-1A alert ─────────────────────────────────────────────────────────
    r1a_b2b = sum(v.get("b2b_tx", 0) for v in gstr1a_pdfs.values())
    if r1a_b2b > 0:
        _sep(ws, ri, "📋  GSTR-1A AMENDMENT ALERT", NC, bg=PURPLE)
        ri += 1
        r1a_cdn = sum(v.get("cdn_cr", 0) for v in gstr1a_pdfs.values())
        r1a_net = sum(v.get("total_liab_tx", 0) for v in gstr1a_pdfs.values())
        _row([0,"MEDIUM","GSTR-1A Check","—","FY Total",
              f"₹{r1a_b2b:,.2f}",
              f"B2B Amendments ₹{r1a_b2b:,.2f} | CDNR ₹{r1a_cdn:,.2f} | Net ₹{r1a_net:,.2f} — "
              "Verify buyer 26AS includes amended invoice amounts"])

    # ── Near matches ──────────────────────────────────────────────────────────
    _sep(ws, ri, "≈  NEAR MATCHES  (within 5%)", NC, bg=BLUE)
    ri += 1
    for g, rec in company_map.items():
        if "Near" not in rec["status"]: continue
        _row([0,"LOW","Near Match", g, rec["buyer_name"],
              f"₹{rec['r1_taxable']:,.2f}",
              f"Diff ₹{rec['diff']:,.2f} — may be advance payment or retention"])

    # ── Standard checklist ────────────────────────────────────────────────────
    _sep(ws, ri, "📋  STANDARD ACTION CHECKLIST", NC, bg=NAVY)
    ri += 1
    for priority, action in [
        ("HIGH",   "Collect Form 16A TDS certificates from all 26AS deductors and cross-verify amounts"),
        ("HIGH",   "For each mismatch: confirm whether advance payment / credit note explains the gap"),
        ("HIGH",   "For 'Missing in GSTR-1': raise invoice if not yet raised; amend GSTR-1 if already raised"),
        ("HIGH",   "Check whether GSTR-1A amendments are reflected in buyer's 26AS TDS base"),
        ("MEDIUM", "Confirm TDS deduction sections: 194C (works), 194J (professional), 194Q (goods purchase)"),
        ("MEDIUM", "Verify all credit notes in GSTR-1 are acknowledged by buyers (reduce TDS base accordingly)"),
        ("MEDIUM", "Compare GSTR-1 annual B2B turnover with AIS/TIS business income reported"),
        ("LOW",    "Retain this workbook and the source GSTR1_FY_*.xlsx as audit trail documents"),
        ("LOW",    "Re-run this comparison after filing any revised returns or GSTR-1A amendments"),
    ]:
        _row([0, priority, "Checklist", "—", "", "—", action])

    ws.freeze_panes = "A3"
    log(f"    Sheet 5 (Reconciliation_Notes): {item} action items")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def run_26as_gstr1_comparison(
    client_dir:  str | Path,
    client_name: str = "Company",
    gstin:       str = "",
    pan:         str = "",
    fy:          str = "2024-25",
    out_path:    str | Path | None = None,
    cache        = None,
    log          = None,
) -> Path | None:
    """
    Main function. Can be called from gst_suite or standalone.

    Parameters
    ----------
    client_dir   : folder with GSTR1_FY_*.xlsx, GSTR1A_*.pdf, *26AS*.pdf
    client_name  : company name
    gstin        : seller GSTIN
    pan          : company PAN
    fy           : financial year e.g. "2024-25"
    out_path     : output Excel (default: client_dir/26AS_GSTR1_Compare_<fy>.xlsx)
    cache        : GSTINNameCache instance (optional, for live buyer name lookup)
    log          : callable(msg) for progress output
    """
    _log  = log or print
    cdir  = Path(client_dir)

    if not cdir.exists():
        _log(f"❌ Folder not found: {cdir}"); return None

    _log(f"\n{'='*70}")
    _log(f"  26AS vs GSTR-1 Comparison — {client_name} — FY {fy}")
    _log(f"{'='*70}")

    # [1] CustomerMaster for buyer names
    _log("\n[1/5] Loading CustomerMaster buyer names…")
    name_map = load_customer_master(cdir, _log)

    # [2] Read GSTR-1 FY Excel (from gst_suite)
    _log("\n[2/5] Reading GSTR-1 FY Excel (from gst_suite output)…")
    bill_rows, gstin_totals = read_fy_excel(cdir, _log, name_map=name_map, cache=cache)

    if not bill_rows and not gstin_totals:
        _log("  ❌ No GSTR-1 data found. Please run gst_suite first to generate GSTR1_FY_*.xlsx")

    # [3] Read GSTR-1A PDFs
    _log("\n[3/5] Reading GSTR-1A PDFs (section-level summary)…")
    gstr1a_pdfs = read_gstr1a_pdfs(cdir, _log)

    # [4] Parse 26AS PDF
    _log("\n[4/5] Parsing Form 26AS…")
    pdf26 = (
        list(cdir.glob("*26as*.pdf")) + list(cdir.glob("*26AS*.pdf")) +
        list(cdir.glob("*26As*.pdf")) + list(cdir.glob("Form26AS*.pdf")) +
        list(cdir.glob("form26as*.pdf")) + list(cdir.glob("26AS_*.pdf"))
    )
    # FIX v10.10: also search subfolders (e.g. IT Download/) when client root is passed
    if not pdf26:
        pdf26 = (
            list(cdir.rglob("*26as*.pdf")) + list(cdir.rglob("*26AS*.pdf")) +
            list(cdir.rglob("26AS_*.pdf"))
        )
    tds_entries: list[TDS26ASEntry] = []
    if pdf26:
        tds_entries = parse_26as_pdf(str(pdf26[0]), _log)
    else:
        _log("  ⚠ No 26AS PDF found. Filename must contain '26as' or '26AS'.")

    # [5] Match + Write Excel
    _log("\n[5/5] Matching and writing output Excel…")
    company_map = build_company_map(gstin_totals, tds_entries, _log)

    safe_fy = fy.replace("/", "-").replace("\\", "-")
    out     = Path(out_path) if out_path else cdir / f"26AS_GSTR1_Compare_{safe_fy}.xlsx"

    wb = Workbook(); wb.remove(wb.active)
    _write_company_wise(wb, company_map, gstr1a_pdfs, client_name, gstin, fy, _log)
    _write_bill_wise(wb, bill_rows, company_map, client_name, gstin, fy, _log)
    _write_gstr1a_monthly(wb, gstr1a_pdfs, client_name, gstin, fy, _log)
    _write_26as_detail(wb, tds_entries, client_name, pan, fy, _log)
    _write_recon_notes(wb, company_map, gstr1a_pdfs, bill_rows, client_name, fy, _log)

    wb.save(str(out))
    _log(f"\n{'='*70}")
    _log(f"  ✅ Done!  Output → {out.name}")
    _log(f"{'='*70}\n")
    return out


# ══════════════════════════════════════════════════════════════════════════════
#  STANDALONE CLI
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(
        description="26AS vs GSTR-1 Comparison (reads from gst_suite FY Excel)"
    )
    ap.add_argument("--folder", "-f", default=".",        help="Client folder")
    ap.add_argument("--name",   "-n", default="Company",  help="Company name")
    ap.add_argument("--gstin",  "-g", default="",         help="Seller GSTIN")
    ap.add_argument("--pan",    "-p", default="",         help="Company PAN")
    ap.add_argument("--fy",     "-y", default="2024-25",  help="Financial Year")
    ap.add_argument("--out",    "-o", default=None,       help="Output Excel path")
    args = ap.parse_args()

    # Try to load GSTINNameCache if present in folder
    _cache = None
    try:
        sys.path.insert(0, args.folder)
        sys.path.insert(0, str(Path(__file__).parent))
        from gstin_name_cache import GSTINNameCache
        _cache = GSTINNameCache(log_fn=print)
        print("  ✓ GSTIN name cache loaded")
    except (ImportError, Exception):
        pass

    run_26as_gstr1_comparison(
        client_dir  = args.folder,
        client_name = args.name,
        gstin       = args.gstin,
        pan         = args.pan,
        fy          = args.fy,
        out_path    = args.out,
        cache       = _cache,
    )
