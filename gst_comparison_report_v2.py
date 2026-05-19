"""
GST COMPARISON REPORT GENERATOR
================================
Generates a 3-in-1 Excel comparison report:
  Sheet 1 — Tax_Liability_vs_ITC  : Tax Liability (3B) vs Annual Reconciliation ITC (2B ITC)
  Sheet 2 — ITC_Comparison_Detail : Full "ITC Other than IMPG" table from TaxLiability_Comparison file
  Sheet 3 — GSTR2B_RC_Monthwise   : GSTR-2B "Supply Attract Reverse Charge = YES" month-wise subtotals

Usage:
  python gst_comparison_report.py

The script will prompt for:
  1. Folder path containing:
       - TaxLiability_Comparison_FY*.xlsx  (portal download)
       - ANNUAL_RECONCILIATION_*.xlsx      (from GST suite)
       - GSTR2B_<Month>_<Year>.xlsx files  (one per month)
  2. Financial Year (e.g. 2025-26)
  3. Client name (for title)
"""

import os
import sys
import re
from pathlib import Path
import pandas as pd
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from datetime import datetime

# ── colour palette (matches GST Suite) ─────────────────────────────────────
HDR_BG   = "1F3864"; HDR_FG  = "FFFFFF"
SEC_BG   = "2E75B6"; SEC_FG  = "FFFFFF"
TOT_BG   = "D6DCE4"; TOT_FG  = "000000"
ALT1     = "FFFFFF"; ALT2    = "F2F2F2"
GREEN_BG = "C6EFCE"; GREEN_FG= "276221"
RED_BG   = "FFC7CE"; RED_FG  = "9C0006"
YELLOW_BG= "FFEB9C"; YELLOW_FG="9C6500"
BLUE_BG  = "DEEAF1"
NUM_FMT  = "#,##0.00"

MONTHS_ORDER = ["April","May","June","July","August","September",
                "October","November","December","January","February","March"]

# ── style helpers ───────────────────────────────────────────────────────────
def _f(h):  return PatternFill("solid", fgColor=h)
def _font(bold=False, color="000000", size=9):
    return Font(name="Arial", bold=bold, color=color, size=size)
def _bdr():
    s = Side(style="thin")
    return Border(left=s, right=s, top=s, bottom=s)
def _aln(h="left", wrap=False):
    return Alignment(horizontal=h, vertical="center", wrap_text=wrap)

def _cell(ws, r, c, v, bg=ALT1, bold=False, fg="000000", numfmt=None, align="left"):
    cl = ws.cell(row=r, column=c, value=v)
    cl.font  = _font(bold, fg, 9)
    cl.fill  = _f(bg)
    cl.alignment = _aln(align)
    cl.border = _bdr()
    if numfmt and (isinstance(v, (int, float)) or (isinstance(v, str) and v.startswith("="))):
        cl.number_format = numfmt
    return cl

def _title(ws, text, ncols, bg=HDR_BG):
    ws.merge_cells(f"A1:{get_column_letter(ncols)}1")
    c = ws["A1"]
    c.value = text
    c.font  = Font(name="Arial", bold=True, color=HDR_FG, size=12)
    c.fill  = _f(bg); c.alignment = _aln("center"); c.border = _bdr()
    ws.row_dimensions[1].height = 28

def _hdr(ws, labels_widths, row=2, bg=SEC_BG):
    for ci, (lbl, w) in enumerate(labels_widths, 1):
        c = ws.cell(row=row, column=ci, value=lbl)
        c.font  = _font(True, HDR_FG, 9)
        c.fill  = _f(bg); c.alignment = _aln("center"); c.border = _bdr()
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[row].height = 22

def _totrow(ws, r, vals, bg=TOT_BG, fg=TOT_FG):
    for ci, v in enumerate(vals, 1):
        cl = ws.cell(row=r, column=ci, value=v)
        cl.font  = _font(True, fg, 9)
        cl.fill  = _f(bg)
        is_num = isinstance(v, (int, float))
        is_fml = isinstance(v, str) and v.startswith("=")
        cl.alignment = _aln("right" if (is_num or is_fml) else "left")
        cl.border = _bdr()
        if is_num or is_fml:
            cl.number_format = NUM_FMT
    ws.row_dimensions[r].height = 18

def _secrow(ws, r, label, ncols, bg=SEC_BG):
    ws.merge_cells(f"A{r}:{get_column_letter(ncols)}{r}")
    c = ws.cell(row=r, column=1, value=label)
    c.font  = _font(True, SEC_FG, 9)
    c.fill  = _f(bg); c.alignment = _aln("left"); c.border = _bdr()
    ws.row_dimensions[r].height = 16

def _fsum(col, r_start, r_end):
    if r_end < r_start: return 0.0
    return f"=SUM({col}{r_start}:{col}{r_end})"

# ── number cleaner ──────────────────────────────────────────────────────────
def _cn(v):
    if v is None: return 0.0
    try:
        s = str(v).strip().replace(",","").replace("₹","").replace(" ","")
        s = re.sub(r"[^\d.\-]", "", s)
        return float(s) if s and s not in ("","-") else 0.0
    except Exception: return 0.0

# ── detect FY months ────────────────────────────────────────────────────────
def _fy_months(fy):
    """Return list of (month_name, year_str) for FY like '2025-26'."""
    try:
        y1 = int(fy.split("-")[0])
        y2 = y1 + 1
    except Exception:  # FIX v11
        y1 = 2025; y2 = 2026
    result = []
    for mn in MONTHS_ORDER:
        yr = y1 if mn in ["April","May","June","July","August","September",
                          "October","November","December"] else y2
        result.append((mn, str(yr)))
    return result

# ═══════════════════════════════════════════════════════════════════════════
#  READER 1 — TaxLiability_Comparison_FY*.xlsx
#  Extracts ITC (Other than IMPG) sheet: month-wise GSTR-3B ITC vs GSTR-2B ITC
# ═══════════════════════════════════════════════════════════════════════════
def read_tax_liability_file(folder):
    """
    Reads TaxLiability portal Excel.

    FILE PRIORITY (highest → lowest):
      1. TaxLiability_YYYY_YY.xlsx   — portal direct download  e.g. TaxLiability_2025_26.xlsx
      2. TaxLiability_Comparison_FY*.xlsx / TaxLiability_Comparison_FY*.xls  — suite naming
      3. TaxLiability_FY*.xlsx
      4. TaxLiability_*.xlsx  (any other variant)
      5. Tax_Liability*.xlsx  (space-underscore variant)
      6. taxliability*.xlsx   (lowercase)

    Within each priority tier, prefer the most recently modified file.
    Always exclude the GST_Comparison_Report output file itself.

    Returns (itc_monthly, tax_monthly, filepath).
    """
    folder = Path(folder)
    exclude = lambda f: "GST_Comparison_Report" in f.name or "Comparison_Report" in f.name

    # Search primary folder then parent (GSTIN-subfolder fix)
    # When run_all passes a GSTIN subfolder, TaxLiability sits in the parent.
    _search_dirs = [folder]
    if folder.parent != folder:
        _search_dirs.append(folder.parent)

    def _pick_from(globs, search_dir):
        seen = set(); out = []
        for pat in globs:
            for f in search_dir.glob(pat):
                if exclude(f): continue
                if f in seen: continue
                seen.add(f); out.append(f)
        return sorted(out, key=lambda p: p.stat().st_mtime, reverse=True)

    def _pick(globs):
        for _d in _search_dirs:
            result = _pick_from(globs, _d)
            if result:
                if _d != folder:
                    print(f"  ℹ  TaxLiability found in parent folder: {_d}")
                return result
        return []

    # Priority tiers
    tier1 = _pick(["TaxLiability_20[0-9][0-9]_[0-9][0-9].xlsx",
                   "TaxLiability_20[0-9][0-9]_20[0-9][0-9].xlsx"])
    tier2 = _pick(["TaxLiability_Comparison_FY*.xlsx",
                   "TaxLiability_Comparison_FY*.xls",
                   "TaxLiability_FY*.xlsx"])
    tier3 = _pick(["TaxLiability_*.xlsx",
                   "TaxLiability*.xlsx",
                   "Tax_Liability*.xlsx",
                   "taxliability*.xlsx"])

    tl = tier1 or tier2 or tier3
    if not tl:
        print("  ⚠  TaxLiability file NOT found in folder or parent.")
        return {}, {}, None
    fp = tl[0]
    tier_label = ("portal-direct" if fp in tier1 else
                  "suite-named"   if fp in tier2 else "variant")
    print(f"  ✓  Tax Liability file [{tier_label}]: {fp.name}")

    xl = pd.ExcelFile(fp, engine="openpyxl")
    print(f"     Sheets: {xl.sheet_names}")

    # ── Sheet detection ───────────────────────────────────────────────────────
    # Portal TaxLiability_YYYY_YY.xlsx sheet names (confirmed portal layout):
    #   "Tax Liability"  or "Tax liability"  or "Tax_Liability"
    #   "ITC (Other than IMPG)"  or "ITC Other than IMPG"
    #   "ITC IMPG"
    #   "Interest"  etc.
    # Suite TaxLiability_Comparison_FY*.xlsx may have different names.

    def _find_sheet(candidates_ordered):
        """Return first sheet whose name matches any candidate (case-insensitive substring)."""
        for candidate in candidates_ordered:
            for s in xl.sheet_names:
                if candidate.lower() in s.lower():
                    return s
        return None

    # ITC (Other than IMPG) sheet — many possible names
    itc_sheet = _find_sheet([
        "other than impg",      # portal: "ITC (Other than IMPG)"
        "itc other",            # portal short variant
        "other impg",
        "itc (other",
        "itc-other",
        "othr than impg",
        "itc comparison",       # suite naming
    ])
    # Fallback: any ITC sheet that is NOT IMPG and NOT RC
    if not itc_sheet:
        itc_sheet = next(
            (s for s in xl.sheet_names
             if "itc" in s.lower()
             and "impg" not in s.lower()
             and "rc"   not in s.lower()
             and "reverse" not in s.lower()),
            None
        )

    # Tax liability sheet
    tl_sheet = _find_sheet([
        "tax liability",        # portal + suite
        "tax_liability",
        "taxliability",
        "tax liab",
        "liability",
    ])
    # Fallback: first sheet if only one sheet
    if not tl_sheet and len(xl.sheet_names) == 1:
        tl_sheet = xl.sheet_names[0]

    print(f"     ITC sheet : {itc_sheet!r}")
    print(f"     TaxL sheet: {tl_sheet!r}")

    def _scan_sheet_for_months(sheet_name):
        """
        Reads a portal sheet and returns month_name -> dict of tax figures.
        Scans header rows (rows 0-8) to build a column-index map by detecting
        keywords: 'igst', 'cgst', 'sgst', '3b', '2b', 'claimed', 'available',
        'shortfall', 'excess', 'cess'.
        Falls back to positional mapping (confirmed portal column order) if
        header detection fails (merged cells, unlabelled columns, etc.).
        """
        df = pd.read_excel(fp, sheet_name=sheet_name, header=None, dtype=str)

        # Step 1: build column label map from header rows
        col_labels = {}   # col_index -> combined label string (lower)
        for hrow in range(min(8, len(df))):
            for ci in range(len(df.columns)):
                v = str(df.iloc[hrow, ci]).strip().lower()
                if v and v not in ("nan", "none", ""):
                    col_labels[ci] = col_labels.get(ci, "") + " " + v

        # Step 2: identify column roles
        def _find_cols(keyword_sets, exclude=None):
            result = []
            for ci, lbl in sorted(col_labels.items()):
                if exclude and any(e in lbl for e in exclude): continue
                if all(any(kw in lbl for kw in kset) for kset in keyword_sets):
                    result.append(ci)
            return result

        c3b_ig = _find_cols([["igst","integrated"], ["3b","claimed","gstr-3b","gstr 3b"]])
        c3b_cg = _find_cols([["cgst","central"],    ["3b","claimed","gstr-3b","gstr 3b"]])
        c3b_sg = _find_cols([["sgst","state","utgst"],["3b","claimed","gstr-3b","gstr 3b"]])
        c2b_ig = _find_cols([["igst","integrated"], ["2b","available","gstr-2b","gstr 2b"]])
        c2b_cg = _find_cols([["cgst","central"],    ["2b","available","gstr-2b","gstr 2b"]])
        c2b_sg = _find_cols([["sgst","state","utgst"],["2b","available","gstr-2b","gstr 2b"]])

        # Positional fallback — confirmed portal column order:
        # Col0=ReturnPeriod, 1=3B_IGST, 2=3B_CGST, 3=3B_SGST, 4=3B_CESS,
        # 5=2B_IGST, 6=2B_CGST, 7=2B_SGST, 8=2B_CESS,
        # 9=Shortfall_IGST, 10=Shortfall_CGST, 11=Shortfall_SGST, 12=Shortfall_CESS
        use_positional = not (c3b_ig or c2b_ig)

        SHORT_TO_FULL = {
            "apr":"April","may":"May","jun":"June","jul":"July",
            "aug":"August","sep":"September","oct":"October",
            "nov":"November","dec":"December","jan":"January",
            "feb":"February","mar":"March"
        }

        monthly_out = {}
        for ridx, row in df.iterrows():
            r0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
            if not r0 or r0.lower() in ("nan", "none", ""): continue
            r0_low = r0.lower()

            matched_mn = None
            for mn in MONTHS_ORDER:
                if mn.lower() in r0_low and "total" not in r0_low:
                    matched_mn = mn; break
            if not matched_mn:
                for short, full in SHORT_TO_FULL.items():
                    if r0_low.startswith(short) and "total" not in r0_low:
                        matched_mn = full; break
            if not matched_mn: continue

            def _g(ci):
                if ci >= len(row): return 0.0
                try:
                    s = str(row.iloc[ci]).strip().replace(",","").replace("₹","")
                    return float(s) if s and s not in ("nan","None","","-") else 0.0
                except Exception: return 0.0

            if use_positional:
                ig3b = _g(1); cg3b = _g(2); sg3b = _g(3)
                ig2b = _g(5); cg2b = _g(6); sg2b = _g(7)
                sh_ig= _g(9); sh_cg= _g(10);sh_sg= _g(11)
                all_nums = [_g(i) for i in range(1, min(20, len(row)))]
            else:
                ig3b = _g(c3b_ig[0]) if c3b_ig else 0.0
                cg3b = _g(c3b_cg[0]) if c3b_cg else 0.0
                sg3b = _g(c3b_sg[0]) if c3b_sg else 0.0
                ig2b = _g(c2b_ig[0]) if c2b_ig else 0.0
                cg2b = _g(c2b_cg[0]) if c2b_cg else 0.0
                sg2b = _g(c2b_sg[0]) if c2b_sg else 0.0
                sh_ig = ig3b - ig2b; sh_cg = cg3b - cg2b; sh_sg = sg3b - sg2b
                all_nums = [_g(i) for i in range(1, min(20, len(row)))]

            monthly_out[matched_mn] = {
                "3b_igst": ig3b, "3b_cgst": cg3b, "3b_sgst": sg3b,
                "3b_total": round(ig3b + cg3b + sg3b, 2),
                "2b_igst": ig2b, "2b_cgst": cg2b, "2b_sgst": sg2b,
                "2b_total": round(ig2b + cg2b + sg2b, 2),
                "shortfall_igst": sh_ig, "shortfall_cgst": sh_cg, "shortfall_sgst": sh_sg,
                "row_nums": all_nums,
                "raw_row": list(row),
            }

        print(f"     Sheet '{sheet_name}': found {len(monthly_out)} month rows "
              f"({'positional' if use_positional else 'header-detected'} mapping)")
        return monthly_out

    monthly     = _scan_sheet_for_months(itc_sheet) if itc_sheet else {}
    tax_monthly = _scan_sheet_for_months(tl_sheet)  if tl_sheet  else {}

    return monthly, tax_monthly, fp

# ═══════════════════════════════════════════════════════════════════════════
#  READER 2 — ANNUAL_RECONCILIATION_*.xlsx  (R1_vs_3B_Recon sheet)
# ═══════════════════════════════════════════════════════════════════════════
def read_annual_reconciliation(folder):
    """Returns dict: month_name -> {r1_taxable, r1_igst, r1_cgst, r1_sgst, r1_total,
                                     itc_igst, itc_cgst, itc_sgst, itc_total,
                                     net_payable, diff, status}"""
    files = (list(Path(folder).glob("ANNUAL_RECONCILIATION_*.xlsx")) +
             list(Path(folder).glob("ANNUAL_RECONCILIATION_*.xlsm")))
    if not files:
        print("  ⚠  ANNUAL_RECONCILIATION file NOT found in folder.")
        return {}, None

    fp = files[0]
    print(f"  ✓  Annual Reconciliation file: {fp.name}")
    xl = pd.ExcelFile(fp, engine="openpyxl")
    print(f"     Sheets: {xl.sheet_names}")

    recon = {}

    # Try R1_vs_3B_Recon or similar
    r1_sheet = next((s for s in xl.sheet_names
                     if "r1" in s.lower() and ("3b" in s.lower() or "recon" in s.lower())), None)
    if not r1_sheet:
        r1_sheet = next((s for s in xl.sheet_names if "recon" in s.lower()), None)

    if r1_sheet:
        df = pd.read_excel(fp, sheet_name=r1_sheet, header=None, dtype=str)
        for ridx, row in df.iterrows():
            r0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
            matched_mn = None
            for mn in MONTHS_ORDER:
                if mn.lower() in r0.lower() and "total" not in r0.lower():
                    matched_mn = mn
                    break
            if not matched_mn:
                continue
            nums = [_cn(v) for v in row.iloc[1:]]
            while len(nums) < 16: nums.append(0.0)
            # Based on image2: Month | R1 Taxable | R1 IGST | R1 CGST | R1 SGST | R1 Total Tax
            #                        | 2B ITC IGST | 2B ITC CGST | 2B ITC SGST | 2B Total ITC
            #                        | Net Tax Payable | 2A vs 2B Diff | Status
            recon[matched_mn] = {
                "r1_taxable":  nums[0],
                "r1_igst":     nums[1],
                "r1_cgst":     nums[2],
                "r1_sgst":     nums[3],
                "r1_total":    nums[4],
                "itc_igst":    nums[5] if len(nums) > 5 else 0.0,
                "itc_cgst":    nums[6] if len(nums) > 6 else 0.0,
                "itc_sgst":    nums[7] if len(nums) > 7 else 0.0,
                "itc_total":   nums[8] if len(nums) > 8 else 0.0,
                "net_payable": nums[9] if len(nums) > 9 else 0.0,
                "diff_2a_2b":  nums[10] if len(nums) > 10 else 0.0,
            }
        print(f"     Recon sheet '{r1_sheet}': found {len(recon)} month rows")

    return recon, fp

# ═══════════════════════════════════════════════════════════════════════════
#  READER 2B — GSTR2B_Consolidated_Analysis*.xlsx — Month-wise Summary Net ITC
# ═══════════════════════════════════════════════════════════════════════════
def read_gstr2b_consolidated(folder):
    """
    Reads GSTR2B_Consolidated_Analysis*.xlsx → 'Month-wise Summary' sheet.
    Returns dict: month_name -> {net_igst, net_cgst, net_sgst, net_cess,
                                  net_total, net_taxable, invoices}
    Month-wise Summary layout (header row 1, data rows 2+):
      Col 0=Month, Col 1=FY, ..., Col 19=Net Taxable Value,
      Col 20=Net IGST, Col 21=Net CGST, Col 22=Net SGST,
      Col 23=Net Cess, Col 24=Net Total Tax, Col 25=Invoices, Col 26=Amount

    FIX: also searches parent folder — handles the case where GSTR2B monthly
    files are in a GSTIN subfolder but consolidated file is in the parent
    (GST Automation) directory.
    """
    search_dirs = [Path(folder)]
    _parent = Path(folder).parent
    if _parent != Path(folder):
        search_dirs.append(_parent)
    files = []
    for _d in search_dirs:
        _f = (list(_d.glob("GSTR2B_Consolidated_Analysis*.xlsx")) +
              list(_d.glob("GSTR2B_Consolidated*.xlsx")) +
              list(_d.glob("*Consolidated_Analysis*.xlsx")))
        if _f:
            files = _f
            if _d != Path(folder):
                print(f"  ℹ  GSTR2B Consolidated Analysis found in parent folder: {_d}")
            break
    if not files:
        print("  ⚠  GSTR2B_Consolidated_Analysis file NOT found — All 2B ITC will use individual file totals.")
        return {}

    fp = files[0]
    print(f"  ✓  GSTR2B Consolidated Analysis file: {fp.name}")
    xl = pd.ExcelFile(fp, engine="openpyxl")

    sht = next((s for s in xl.sheet_names if "month" in s.lower() and "summ" in s.lower()), None)
    if not sht:
        sht = next((s for s in xl.sheet_names if "month" in s.lower()), xl.sheet_names[0])

    df = pd.read_excel(fp, sheet_name=sht, header=None, dtype=str)

    SHORT_TO_FULL = {
        "apr":"April","may":"May","jun":"June","jul":"July",
        "aug":"August","sep":"September","oct":"October",
        "nov":"November","dec":"December","jan":"January",
        "feb":"February","mar":"March"
    }

    result = {}
    for ridx, row in df.iterrows():
        r0 = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
        r0_low = r0.lower()
        if "total" in r0_low or "grand" in r0_low: continue

        matched_mn = None
        for mn in MONTHS_ORDER:
            if mn.lower() in r0_low:
                matched_mn = mn; break
        if not matched_mn:
            for short, full in SHORT_TO_FULL.items():
                if r0_low.startswith(short):
                    matched_mn = full; break
        if not matched_mn:
            continue

        def _g(ci):
            try:
                v = str(row.iloc[ci]).strip().replace(",","") if ci < len(row) else ""
                return float(v) if v and v not in ("nan","None","","--") else 0.0
            except Exception: return 0.0

        result[matched_mn] = {
            "net_taxable": round(_g(19), 2),
            "net_igst":    round(_g(20), 2),
            "net_cgst":    round(_g(21), 2),
            "net_sgst":    round(_g(22), 2),
            "net_cess":    round(_g(23), 2),
            "net_total":   round(_g(24), 2),
            "invoices":    int(_g(25)) if _g(25) else 0,
        }

    print(f"     Month-wise Summary '{sht}': loaded {len(result)} months from consolidated file")
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  READER 3 — GSTR2B_<Month>_<Year>.xlsx — Reverse Charge month-wise
# ═══════════════════════════════════════════════════════════════════════════
def read_gstr2b_rc_monthwise(folder, months_list):
    """Returns dict: month_name -> {rc_rows: [...], rc_igst, rc_cgst, rc_sgst,
                                     all_igst, all_cgst, all_sgst, rc_count, total_count}"""
    result = {}
    for mn, yr in months_list:
        key = mn
        direct_xl = Path(folder) / f"GSTR2B_{mn}_{yr}.xlsx"
        # Also try portal-named pattern
        if not direct_xl.exists():
            mnum = {"April":"04","May":"05","June":"06","July":"07","August":"08",
                    "September":"09","October":"10","November":"11","December":"12",
                    "January":"01","February":"02","March":"03"}.get(mn,"")
            matches = (list(Path(folder).glob(f"{mnum}{yr}*GSTR2B*.xlsx")) +
                       list(Path(folder).glob(f"*GSTR2B*{mnum}{yr}*.xlsx")) +
                       list(Path(folder).glob(f"*{mnum}{yr[2:]}*GSTR*2B*.xlsx")))
            if matches:
                direct_xl = matches[0]

        entry = {"rc_rows":[], "rc_igst":0., "rc_cgst":0., "rc_sgst":0.,
                 "all_igst":0., "all_cgst":0., "all_sgst":0.,
                 "rc_count":0, "total_count":0, "found": direct_xl.exists()}
        if not direct_xl.exists():
            print(f"  ⚠  GSTR2B_{mn}_{yr}.xlsx not found")
            result[key] = entry
            continue

        try:
            _xl = pd.ExcelFile(direct_xl, engine="openpyxl")
            all_sheets = _xl.sheet_names

            # ── helpers ────────────────────────────────────────────────────
            def _cs(row, i): return str(row.iloc[i]).strip() if i < len(row) and pd.notna(row.iloc[i]) else ""
            def _cnv(row, i):
                try:
                    v = str(row.iloc[i]).strip().replace(",","")
                    return float(v) if v and v not in ("nan","None","") else 0.0
                except Exception: return 0.0

            # ── 1. Read B2B sheet ──────────────────────────────────────────
            b2b_sht = next((s for s in all_sheets
                            if s.strip().upper() == "B2B"), None)
            if b2b_sht is None:
                b2b_sht = next((s for s in all_sheets
                                if "b2b" in s.lower()
                                and "cdnr" not in s.lower()
                                and "dnr" not in s.lower()
                                and "reject" not in s.lower()
                                and "revers" not in s.lower()), all_sheets[0])

            raw = _xl.parse(b2b_sht, header=None, dtype=str)

            # Auto-detect old vs new format
            fmt = "new"
            for hdr_idx in range(4, 7):
                if hdr_idx < len(raw):
                    hrow = raw.iloc[hdr_idx]
                    for j, v in enumerate(hrow):
                        if v and "Rate" in str(v) and j == 8:
                            fmt = "old"
                            break
                if fmt == "old": break

            data_rows = raw.iloc[6:].reset_index(drop=True)
            col0 = data_rows.iloc[:, 0].astype(str).str.strip()
            mask = col0.str.match(r"^[0-9]{2}[A-Z0-9]{13}$", na=False)
            data_rows = data_rows[mask]

            for _, row in data_rows.iterrows():
                sup = _cs(row, 0)
                if not sup or sup.lower() in ("nan","none","","-","gstin"): continue
                nm   = _cs(row, 1)
                inum = _cs(row, 2)
                idt  = _cs(row, 4)
                iv   = _cnv(row, 5)
                pos  = _cs(row, 6)
                rc   = _cs(row, 7)

                if fmt == "old":
                    tv = _cnv(row, 9);  ig = _cnv(row, 10)
                    cg = _cnv(row, 11); sg = _cnv(row, 12)
                else:
                    tv = _cnv(row, 8);  ig = _cnv(row, 9)
                    cg = _cnv(row, 10); sg = _cnv(row, 11)

                entry["all_igst"] += ig; entry["all_cgst"] += cg; entry["all_sgst"] += sg
                entry["total_count"] += 1

                if rc.strip().lower() in ("yes","y","true","1"):
                    entry["rc_igst"] += ig; entry["rc_cgst"] += cg; entry["rc_sgst"] += sg
                    entry["rc_count"] += 1
                    entry["rc_rows"].append((sup, nm, inum, idt, round(iv,2), pos,
                                             round(tv,2), round(ig,2), round(cg,2), round(sg,2)))

            # ── 2. Read B2B-CDNR sheet — subtract Credit Notes, add Debit Notes ──
            # B2B-CDNR columns (0-indexed):
            #   0=GSTIN, 1=Name, 2=NoteNo, 3=NoteType(Credit Note/Debit Note),
            #   4=SupplyType, 5=Date, 6=NoteValue, 7=PlaceOfSupply,
            #   8=RC(Yes/No), 9=TaxableValue, 10=IGST, 11=CGST, 12=SGST
            cdnr_sht = next((s for s in all_sheets
                             if s.strip().upper() == "B2B-CDNR"), None)
            if cdnr_sht is None:
                cdnr_sht = next((s for s in all_sheets
                                 if "cdnr" in s.lower()
                                 and "reject" not in s.lower()
                                 and "amend" not in s.lower()
                                 and "cdnra" not in s.lower()), None)

            cdnr_credit_igst = cdnr_credit_cgst = cdnr_credit_sgst = 0.0
            cdnr_debit_igst  = cdnr_debit_cgst  = cdnr_debit_sgst  = 0.0

            if cdnr_sht:
                try:
                    cdn_raw = _xl.parse(cdnr_sht, header=None, dtype=str)
                    # data starts at row 6 (0-indexed), header at row 4-5
                    cdn_data = cdn_raw.iloc[6:].reset_index(drop=True)
                    col0c = cdn_data.iloc[:, 0].astype(str).str.strip()
                    mask_c = col0c.str.match(r"^[0-9]{2}[A-Z0-9]{13}$", na=False)
                    cdn_data = cdn_data[mask_c]

                    for _, crow in cdn_data.iterrows():
                        sup_c = _cs(crow, 0)
                        if not sup_c or sup_c.lower() in ("nan","none","","-"): continue
                        note_type = _cs(crow, 3).strip().lower()  # "credit note" or "debit note"
                        rc_c      = _cs(crow, 8).strip().lower()  # "yes"/"no"
                        ig_c = _cnv(crow, 10)
                        cg_c = _cnv(crow, 11)
                        sg_c = _cnv(crow, 12)

                        is_credit = "credit" in note_type  # Credit Note → subtract
                        is_debit  = "debit"  in note_type  # Debit Note  → add

                        if is_credit:
                            cdnr_credit_igst += ig_c
                            cdnr_credit_cgst += cg_c
                            cdnr_credit_sgst += sg_c
                        elif is_debit:
                            cdnr_debit_igst += ig_c
                            cdnr_debit_cgst += cg_c
                            cdnr_debit_sgst += sg_c

                        # Net effect on All 2B ITC:
                        # Credit Note reduces ITC, Debit Note increases ITC
                        sign = -1 if is_credit else (1 if is_debit else 0)
                        entry["all_igst"] += sign * ig_c
                        entry["all_cgst"] += sign * cg_c
                        entry["all_sgst"] += sign * sg_c

                        # If RC=Yes, also adjust RC totals
                        if rc_c in ("yes","y","true","1"):
                            entry["rc_igst"] += sign * ig_c
                            entry["rc_cgst"] += sign * cg_c
                            entry["rc_sgst"] += sign * sg_c

                    net_cdnr_igst = cdnr_debit_igst - cdnr_credit_igst
                    print(f"     B2B-CDNR: Credit={cdnr_credit_igst:,.2f} IGST  "
                          f"Debit={cdnr_debit_igst:,.2f} IGST  "
                          f"Net adjustment={net_cdnr_igst:+,.2f}")
                except Exception as ce:
                    print(f"  ⚠  B2B-CDNR read error for {mn} {yr}: {ce}")

            # Store CDNR breakdown for reporting
            entry["cdnr_credit_igst"] = round(cdnr_credit_igst, 2)
            entry["cdnr_credit_cgst"] = round(cdnr_credit_cgst, 2)
            entry["cdnr_credit_sgst"] = round(cdnr_credit_sgst, 2)
            entry["cdnr_debit_igst"]  = round(cdnr_debit_igst,  2)
            entry["cdnr_debit_cgst"]  = round(cdnr_debit_cgst,  2)
            entry["cdnr_debit_sgst"]  = round(cdnr_debit_sgst,  2)

            # Round final totals
            entry["all_igst"] = round(entry["all_igst"], 2)
            entry["all_cgst"] = round(entry["all_cgst"], 2)
            entry["all_sgst"] = round(entry["all_sgst"], 2)
            entry["rc_igst"]  = round(entry["rc_igst"],  2)
            entry["rc_cgst"]  = round(entry["rc_cgst"],  2)
            entry["rc_sgst"]  = round(entry["rc_sgst"],  2)

            print(f"  ✓  GSTR2B {mn} {yr}: {entry['total_count']} records, "
                  f"RC={entry['rc_count']}, RC IGST={entry['rc_igst']:,.2f}  "
                  f"All 2B IGST (net of CDNR)={entry['all_igst']:,.2f}")
        except Exception as e:
            print(f"  ⚠  GSTR2B {mn} {yr} read error: {e}")
        result[key] = entry
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN REPORT BUILDER
# ═══════════════════════════════════════════════════════════════════════════
def build_report(folder, fy, client_name, out_path):
    months_list = _fy_months(fy)
    month_names = [m for m, _ in months_list]

    print(f"\n  Reading data for {len(months_list)} months...")
    itc_monthly, tax_monthly, tl_fp = read_tax_liability_file(folder)
    recon, rec_fp = read_annual_reconciliation(folder)
    rc_data = read_gstr2b_rc_monthwise(folder, months_list)
    consolidated = read_gstr2b_consolidated(folder)

    wb = Workbook()
    wb.remove(wb.active)

    # ──────────────────────────────────────────────────────────────────────
    # SHEET 1: Tax Liability vs Annual Reconciliation ITC — Month Wise
    # ──────────────────────────────────────────────────────────────────────
    ws1 = wb.create_sheet("Tax_Liability_vs_ITC")
    ws1.sheet_view.showGridLines = False
    ws1.freeze_panes = "A4"

    cols1 = [
        ("Month", 14),
        # From Annual Reconciliation R1_vs_3B
        ("R1 Output\nTaxable ₹", 16), ("R1 IGST ₹", 13), ("R1 CGST ₹", 13),
        ("R1 SGST ₹", 13), ("R1 Total Tax ₹", 16),
        # ITC from Annual Reconciliation (GSTR-2B)
        ("2B ITC\nIGST ₹", 13), ("2B ITC\nCGST ₹", 13), ("2B ITC\nSGST ₹", 13),
        ("2B ITC\nTotal ₹", 14),
        # Net payable
        ("Net Tax\nPayable ₹", 15),
        # ITC from TaxLiability portal file — 3B claimed
        ("Portal: 3B\nITC IGST ₹", 16), ("Portal: 3B\nITC CGST ₹", 16),
        ("Portal: 3B\nITC SGST ₹", 16), ("Portal: 3B\nITC Total ₹", 16),
        # ITC from TaxLiability portal file — 2B available
        ("Portal: 2B\nITC IGST ₹", 16), ("Portal: 2B\nITC CGST ₹", 16),
        ("Portal: 2B\nITC SGST ₹", 16), ("Portal: 2B\nITC Total ₹", 16),
        # Shortfall from portal
        ("Portal\nShortfall IGST", 16), ("Portal\nShortfall Total", 16),
        # Status
        ("Status", 14),
    ]
    _title(ws1, f"Tax Liability vs ITC Comparison — {client_name} — FY {fy}", len(cols1))

    # Source note row
    ws1.merge_cells(f"A2:{get_column_letter(len(cols1))}2")
    note1 = ws1["A2"]
    note1.value = ("Source A (cols B–F): ANNUAL_RECONCILIATION → R1_vs_3B_Recon sheet  |  "
                   "Source B (cols G–J): GSTR2B_Consolidated_Analysis → Month-wise Summary (Net IGST/CGST/SGST)  |  "
                   "Source C (cols L–T): TaxLiability_*.xlsx (portal download, ITC Other than IMPG sheet)  |  "
                   "Shortfall = 3B ITC claimed MINUS 2B ITC available (negative = under-claimed)")
    note1.font  = _font(False, "000000", 8)
    note1.fill  = _f(YELLOW_BG); note1.alignment = _aln("left"); note1.border = _bdr()
    ws1.row_dimensions[2].height = 14

    _hdr(ws1, cols1, row=3)
    ri1 = 4

    ann1 = {k: 0.0 for k in ["r1_taxable","r1_igst","r1_cgst","r1_sgst","r1_total",
                               "itc_igst","itc_cgst","itc_sgst","itc_total","net",
                               "tl3b_ig","tl3b_cg","tl3b_sg","tl3b_tot",
                               "tl2b_ig","tl2b_cg","tl2b_sg","tl2b_tot",
                               "sh_ig","sh_tot"]}

    for mn in month_names:
        d_r = recon.get(mn, {})
        d_i = itc_monthly.get(mn, {})

        r1_taxable = d_r.get("r1_taxable", 0.0)
        r1_ig      = d_r.get("r1_igst", 0.0)
        r1_cg      = d_r.get("r1_cgst", 0.0)
        r1_sg      = d_r.get("r1_sgst", 0.0)
        r1_tot     = d_r.get("r1_total", 0.0)
        net_pay    = d_r.get("net_payable", 0.0)

        # 2B ITC — use GSTR2B_Consolidated_Analysis Net values (preferred)
        # Fall back to Annual Reconciliation R1_vs_3B_Recon if consolidated not available
        d_con = consolidated.get(mn, {})
        if d_con:
            itc_ig  = d_con.get("net_igst",  0.0)
            itc_cg  = d_con.get("net_cgst",  0.0)
            itc_sg  = d_con.get("net_sgst",  0.0)
            itc_tot = d_con.get("net_total", round(itc_ig + itc_cg + itc_sg, 2))
        else:
            itc_ig  = d_r.get("itc_igst",  0.0)
            itc_cg  = d_r.get("itc_cgst",  0.0)
            itc_sg  = d_r.get("itc_sgst",  0.0)
            itc_tot = d_r.get("itc_total", 0.0)

        # From TaxLiability portal file
        tl3b_ig  = d_i.get("3b_igst",  0.0)
        tl3b_cg  = d_i.get("3b_cgst",  0.0)
        tl3b_sg  = d_i.get("3b_sgst",  0.0)
        tl3b_tot = d_i.get("3b_total", 0.0)
        tl2b_ig  = d_i.get("2b_igst",  0.0)
        tl2b_cg  = d_i.get("2b_cgst",  0.0)
        tl2b_sg  = d_i.get("2b_sgst",  0.0)
        tl2b_tot = d_i.get("2b_total", 0.0)
        sh_ig    = d_i.get("shortfall_igst",  round(tl3b_ig - tl2b_ig, 2))
        sh_cg    = d_i.get("shortfall_cgst",  round(tl3b_cg - tl2b_cg, 2))
        sh_sg    = d_i.get("shortfall_sgst",  round(tl3b_sg - tl2b_sg, 2))
        sh_tot   = round(sh_ig + sh_cg + sh_sg, 2)

        has_portal = bool(d_i)
        if not d_r and not d_i:
            status_val = "— No Data"; st_bg = ALT2; st_fg = "000000"
        elif has_portal and abs(sh_tot) <= 500:
            status_val = "✓ Match"; st_bg = GREEN_BG; st_fg = GREEN_FG
        elif has_portal and sh_tot < -500:
            status_val = "⚠ Under-Claimed"; st_bg = RED_BG; st_fg = RED_FG
        elif has_portal and sh_tot > 500:
            status_val = "⚠ Excess Claimed"; st_bg = YELLOW_BG; st_fg = YELLOW_FG
        else:
            status_val = "— Portal N/A"; st_bg = ALT2; st_fg = "000000"

        bg = ALT2 if ri1 % 2 == 0 else ALT1
        vals = [mn, r1_taxable, r1_ig, r1_cg, r1_sg, r1_tot,
                itc_ig, itc_cg, itc_sg, itc_tot, net_pay,
                tl3b_ig, tl3b_cg, tl3b_sg, tl3b_tot,
                tl2b_ig, tl2b_cg, tl2b_sg, tl2b_tot,
                sh_ig, sh_tot]
        for ci, v in enumerate(vals, 1):
            _cell(ws1, ri1, ci, v, bg,
                  numfmt=NUM_FMT if ci > 1 else None,
                  align="right" if ci > 1 else "left")
        # Status cell
        sc = ws1.cell(row=ri1, column=len(cols1), value=status_val)
        sc.font  = _font(True, st_fg, 9); sc.fill  = _f(st_bg)
        sc.alignment = _aln("center"); sc.border = _bdr()
        ws1.row_dimensions[ri1].height = 16

        for k, v in [("r1_taxable",r1_taxable),("r1_igst",r1_ig),("r1_cgst",r1_cg),
                     ("r1_sgst",r1_sg),("r1_total",r1_tot),("itc_igst",itc_ig),
                     ("itc_cgst",itc_cg),("itc_sgst",itc_sg),("itc_total",itc_tot),
                     ("net",net_pay),("tl3b_ig",tl3b_ig),("tl3b_cg",tl3b_cg),
                     ("tl3b_sg",tl3b_sg),("tl3b_tot",tl3b_tot),("tl2b_ig",tl2b_ig),
                     ("tl2b_cg",tl2b_cg),("tl2b_sg",tl2b_sg),("tl2b_tot",tl2b_tot),
                     ("sh_ig",sh_ig),("sh_tot",sh_tot)]:
            ann1[k] += v
        ri1 += 1

    # Annual total row
    nc1 = len(cols1)
    _totrow(ws1, ri1, [
        "ANNUAL TOTAL",
        f"=SUM(B4:B{ri1-1})", f"=SUM(C4:C{ri1-1})", f"=SUM(D4:D{ri1-1})",
        f"=SUM(E4:E{ri1-1})", f"=SUM(F4:F{ri1-1})",
        f"=SUM(G4:G{ri1-1})", f"=SUM(H4:H{ri1-1})", f"=SUM(I4:I{ri1-1})",
        f"=SUM(J4:J{ri1-1})", f"=SUM(K4:K{ri1-1})",
        f"=SUM(L4:L{ri1-1})", f"=SUM(M4:M{ri1-1})", f"=SUM(N4:N{ri1-1})",
        f"=SUM(O4:O{ri1-1})",
        f"=SUM(P4:P{ri1-1})", f"=SUM(Q4:Q{ri1-1})", f"=SUM(R4:R{ri1-1})",
        f"=SUM(S4:S{ri1-1})",
        f"=SUM(T4:T{ri1-1})", f"=SUM(U4:U{ri1-1})", ""
    ])
    ws1.sheet_properties.tabColor = "1F3864"

    # ──────────────────────────────────────────────────────────────────────
    # SHEET 2: Full ITC Comparison from TaxLiability file (raw portal data)
    # ──────────────────────────────────────────────────────────────────────
    ws2 = wb.create_sheet("ITC_Comparison_Portal")
    ws2.sheet_view.showGridLines = False

    _title(ws2, f"ITC Comparison — Portal TaxLiability File — {client_name} — FY {fy}", 4)
    ws2.merge_cells("A2:D2")
    note2 = ws2["A2"]
    note2.value = (f"Source: {tl_fp.name if tl_fp else 'NOT FOUND — TaxLiability_Comparison_FY*.xlsx missing'}  |  "
                    "Sheet: ITC (Other than IMPG)  |  "
                    "Shows GSTR-3B ITC claimed vs GSTR-2B ITC available (month-wise, from portal)")
    note2.font  = _font(False, "000000", 8)
    note2.fill  = _f(YELLOW_BG); note2.alignment = _aln("left", wrap=True); note2.border = _bdr()
    ws2.row_dimensions[2].height = 20

    if tl_fp and tl_fp.exists():
        xl2 = pd.ExcelFile(tl_fp, engine="openpyxl")
        itc_sht = next((s for s in xl2.sheet_names
                        if "other" in s.lower() and "impg" in s.lower()), None)
        if not itc_sht:
            itc_sht = next((s for s in xl2.sheet_names if "itc" in s.lower()), None)

        if itc_sht:
            df2 = pd.read_excel(tl_fp, sheet_name=itc_sht, header=None, dtype=str)
            # Write all sheets from the downloaded file into sheet 2
            # Determine max columns
            max_cols = max(len(row) for _, row in df2.iterrows()) if len(df2) > 0 else 4
            max_cols = min(max_cols, 30)

            # Re-title with proper columns
            ws2.delete_rows(1, 2)
            _title(ws2, f"ITC (Other than IMPG) — Portal Data — {client_name} — FY {fy}", max_cols)
            ws2.merge_cells(f"A2:{get_column_letter(max_cols)}2")
            note2b = ws2["A2"]
            note2b.value = f"Sheet: '{itc_sht}'  from  {tl_fp.name}"
            note2b.font  = _font(True, "000000", 9)
            note2b.fill  = _f(BLUE_BG); note2b.alignment = _aln("left"); note2b.border = _bdr()
            ws2.row_dimensions[2].height = 16

            ri2 = 3
            for ridx, row2 in df2.iterrows():
                vals2 = [str(v).strip() if pd.notna(v) and str(v).strip() != "nan" else "" for v in row2]
                if not any(vals2): continue

                # detect if header / total / data row
                is_mn = any(mn.lower() in (vals2[0].lower() if vals2 else "") for mn in MONTHS_ORDER)
                is_tot = "total" in (vals2[0].lower() if vals2 else "")
                is_hdr = any(kw in " ".join(vals2[:4]).lower()
                             for kw in ["month","igst","cgst","sgst","cess","period","return"])

                if is_tot:
                    bg2 = TOT_BG; bold2 = True
                elif is_hdr and not is_mn:
                    bg2 = SEC_BG; bold2 = True; fg2 = SEC_FG
                else:
                    bg2 = ALT2 if ri2 % 2 == 0 else ALT1; bold2 = False

                for ci2, rv in enumerate(vals2[:max_cols], 1):
                    # Try numeric
                    num_val = None
                    if rv:
                        try:
                            num_val = float(rv.replace(",",""))
                        except Exception: pass
                    v2 = num_val if num_val is not None else rv
                    cl2 = ws2.cell(row=ri2, column=ci2, value=v2)
                    if is_hdr and not is_mn:
                        cl2.font = _font(True, HDR_FG, 9); cl2.fill = _f(SEC_BG)
                    elif is_tot:
                        cl2.font = _font(True, TOT_FG, 9); cl2.fill = _f(TOT_BG)
                    else:
                        cl2.font = _font(bold2, "000000", 9); cl2.fill = _f(bg2)
                    cl2.alignment = _aln("right" if isinstance(v2, float) else "left")
                    cl2.border = _bdr()
                    if isinstance(v2, float):
                        cl2.number_format = NUM_FMT
                    ws2.column_dimensions[get_column_letter(ci2)].width = 16
                ws2.column_dimensions["A"].width = 20
                ws2.row_dimensions[ri2].height = 15
                ri2 += 1
        else:
            ws2.cell(row=3, column=1, value="⚠ ITC (Other than IMPG) sheet not found in file").font = _font(True, RED_FG, 10)
    else:
        ws2.cell(row=3, column=1,
                 value="⚠ TaxLiability_Comparison_FY*.xlsx not found. Download from: "
                       "GST Portal → Services → Returns → Tax liabilities and ITC comparison → "
                       f"Select FY {fy} → SEARCH → DOWNLOAD COMPARISON REPORTS (EXCEL)").font = _font(True, RED_FG, 9)
    ws2.sheet_properties.tabColor = "7030A0"

    # ──────────────────────────────────────────────────────────────────────
    # SHEET 3: GSTR-2B Reverse Charge — Month-wise Subtotals
    # ──────────────────────────────────────────────────────────────────────
    ws3 = wb.create_sheet("GSTR2B_RC_Monthwise")
    ws3.sheet_view.showGridLines = False
    ws3.freeze_panes = "A5"

    NCOLS3 = 10
    _title(ws3, f"GSTR-2B Reverse Charge (Supply Attract RC = YES) — Month-wise — {client_name} — FY {fy}", NCOLS3)
    ws3.merge_cells(f"A2:{get_column_letter(NCOLS3)}2")
    note3 = ws3["A2"]
    note3.value = ("Only invoices where 'Supply Attract Reverse Charge' = YES from GSTR-2B B2B sheet.  "
                   "Month subtotals + all invoice detail below each month.")
    note3.font  = _font(False, "000000", 8)
    note3.fill  = _f(YELLOW_BG); note3.alignment = _aln("left"); note3.border = _bdr()
    ws3.row_dimensions[2].height = 14

    # Summary header
    SUM_COLS = [("Month",14),("RC Invoices\n#",12),("Total\nInvoices #",12),
                ("RC Taxable\nValue ₹",16),("RC IGST ₹",13),("RC CGST ₹",13),
                ("RC SGST ₹",13),("RC Total Tax ₹",16),
                ("All 2B\nIGST ₹",13),("RC %\n(of all IGST)",14)]
    _hdr(ws3, SUM_COLS, row=3, bg=HDR_BG)
    ws3.freeze_panes = "A4"

    ri3 = 4
    sum_start = ri3
    ann3 = {"rc_inv":0,"tot_inv":0,"rc_tv":0.,"rc_ig":0.,"rc_cg":0.,"rc_sg":0.,
            "all_ig":0.}

    for mn in month_names:
        d = rc_data.get(mn, {})
        rc_count  = d.get("rc_count", 0)
        tot_count = d.get("total_count", 0)
        rc_tv     = sum(r[6] for r in d.get("rc_rows", []))
        rc_ig     = d.get("rc_igst", 0.)
        rc_cg     = d.get("rc_cgst", 0.)
        rc_sg     = d.get("rc_sgst", 0.)
        rc_tot    = round(rc_ig + rc_cg + rc_sg, 2)
        # All 2B IGST — use consolidated Net IGST (falls back to individual file total)
        _c3 = consolidated.get(mn, {})
        all_ig    = _c3.get("net_igst", d.get("all_igst", 0.))
        rc_pct    = round(rc_ig / all_ig * 100, 1) if all_ig else 0.0

        has_rc = rc_count > 0
        # Month summary row
        sm_bg = YELLOW_BG if has_rc else ALT2
        for ci, v in enumerate([mn, rc_count, tot_count, rc_tv, rc_ig, rc_cg,
                                 rc_sg, rc_tot, all_ig, rc_pct], 1):
            cl = ws3.cell(row=ri3, column=ci, value=v)
            cl.font  = _font(True, "000000", 9); cl.fill = _f(sm_bg)
            cl.alignment = _aln("right" if ci > 1 else "left")
            cl.border = _bdr()
            if ci > 3 and isinstance(v, float):
                cl.number_format = NUM_FMT if ci != 10 else "0.00%"
        ws3.row_dimensions[ri3].height = 17
        ri3 += 1

        # Invoice detail under month (if RC invoices exist)
        if has_rc:
            # Sub-header
            detail_cols = [("GSTIN of Supplier",22),("Trade/Legal Name",26),
                           ("Invoice No",16),("Invoice Date",13),
                           ("Invoice Value ₹",16),("Place of Supply",16),
                           ("Taxable Value ₹",16),("IGST ₹",12),
                           ("CGST ₹",12),("SGST ₹",12)]
            for ci, (lbl, _) in enumerate(detail_cols, 1):
                dc = ws3.cell(row=ri3, column=ci, value=lbl)
                dc.font  = _font(True, HDR_FG, 8); dc.fill = _f("4472C4")
                dc.alignment = _aln("center"); dc.border = _bdr()
            ws3.row_dimensions[ri3].height = 15; ri3 += 1

            for row_d in d.get("rc_rows", []):
                sup, nm, inum, idt, iv, pos, tv, ig, cg, sg = row_d
                bg_d = ALT2 if ri3 % 2 == 0 else ALT1
                for ci, v in enumerate([sup, nm, inum, idt, iv, pos, tv, ig, cg, sg], 1):
                    dc2 = ws3.cell(row=ri3, column=ci, value=v)
                    dc2.font  = _font(False, "000000", 8); dc2.fill = _f(bg_d)
                    dc2.alignment = _aln("right" if ci in (5,7,8,9,10) else "left")
                    dc2.border = _bdr()
                    if ci in (5,7,8,9,10) and isinstance(v, float):
                        dc2.number_format = NUM_FMT
                ws3.row_dimensions[ri3].height = 14; ri3 += 1

        # Blank spacer between months
        ws3.row_dimensions[ri3].height = 6; ri3 += 1

        # Accumulate
        ann3["rc_inv"]  += rc_count;   ann3["tot_inv"] += tot_count
        ann3["rc_tv"]   += rc_tv;      ann3["rc_ig"]   += rc_ig
        ann3["rc_cg"]   += rc_cg;      ann3["rc_sg"]   += rc_sg
        ann3["all_ig"]  += all_ig

    # Annual total
    ann3["rc_tot"] = round(ann3["rc_ig"] + ann3["rc_cg"] + ann3["rc_sg"], 2)
    ann3_pct = round(ann3["rc_ig"]/ann3["all_ig"]*100,1) if ann3["all_ig"] else 0.0
    _totrow(ws3, ri3, [
        "ANNUAL TOTAL",
        ann3["rc_inv"], ann3["tot_inv"],
        round(ann3["rc_tv"],2), round(ann3["rc_ig"],2), round(ann3["rc_cg"],2),
        round(ann3["rc_sg"],2), ann3["rc_tot"],
        round(ann3["all_ig"],2), ann3_pct
    ])
    ws3.sheet_properties.tabColor = "C00000"

    # ──────────────────────────────────────────────────────────────────────
    # SHEET 4: RC Summary (compact month-wise only, no detail rows)
    #          + Tax Liability ITC claimed columns from portal file
    # ──────────────────────────────────────────────────────────────────────
    ws4 = wb.create_sheet("RC_Summary_Only")
    ws4.sheet_view.showGridLines = False

    # Columns:
    # A=Month
    # B=RC Supplier GSTINs, C=RC Invoice Count, D=RC Taxable Value
    # E=RC IGST, F=RC CGST, G=RC SGST, H=RC Total Tax
    # ── separator ──
    # I=All 2B IGST, J=All 2B CGST, K=All 2B SGST, L=All 2B Total
    # ── Tax Liability ITC (from portal TaxLiability file) ──
    # M=3B ITC IGST (claimed), N=3B ITC CGST, O=3B ITC SGST, P=3B ITC Total
    # Q=2B ITC IGST (available), R=2B ITC CGST, S=2B ITC SGST, T=2B ITC Total
    # U=Shortfall IGST (3B−2B), V=Shortfall Total
    # W=RC IGST %, X=Status

    SUM_COLS4 = [
        ("Month", 14),
        # RC from GSTR-2B
        ("RC Supplier\nGSTINs", 16), ("RC Invoice\nCount", 14),
        ("RC Taxable\nValue ₹", 17), ("RC IGST ₹", 13),
        ("RC CGST ₹", 13), ("RC SGST ₹", 13), ("RC Total\nTax ₹", 14),
        # All 2B totals
        ("All 2B\nIGST ₹", 14), ("All 2B\nCGST ₹", 14),
        ("All 2B\nSGST ₹", 14), ("All 2B\nTotal ₹", 14),
        # Tax Liability portal — 3B ITC claimed
        ("TaxLib: 3B\nITC IGST ₹", 16), ("TaxLib: 3B\nITC CGST ₹", 16),
        ("TaxLib: 3B\nITC SGST ₹", 16), ("TaxLib: 3B\nITC Total ₹", 16),
        # Tax Liability portal — 2B ITC available
        ("TaxLib: 2B\nITC IGST ₹", 16), ("TaxLib: 2B\nITC CGST ₹", 16),
        ("TaxLib: 2B\nITC SGST ₹", 16), ("TaxLib: 2B\nITC Total ₹", 16),
        # Shortfall
        ("Shortfall\nIGST ₹", 15), ("Shortfall\nTotal ₹", 15),
        # Summary
        ("RC IGST %", 12), ("Status", 18),
        # NEW: 2B ITC − 3B Claimed difference
        ("Diff IGST ₹\n(2B−3B)", 16), ("Diff CGST ₹\n(2B−3B)", 16),
        ("Diff SGST ₹\n(2B−3B)", 16), ("Diff Total ₹\n(2B−3B)", 16),
    ]
    NC4 = len(SUM_COLS4)

    _title(ws4, f"GSTR-2B RC Month-wise Summary — {client_name} — FY {fy}", NC4)

    # Sub-header row 2: note
    ws4.merge_cells(f"A2:{get_column_letter(NC4)}2")
    ws4["A2"].value = ("RC = Supply Attract Reverse Charge = YES from GSTR-2B B2B sheet.  "
                       "All 2B ITC = Net values from GSTR2B_Consolidated_Analysis Month-wise Summary (net of Credit/Debit Notes).  "
                       "TaxLib columns from portal TaxLiability_*.xlsx (ITC Other than IMPG sheet).  "
                       "Shortfall = 3B ITC claimed − 2B ITC available (negative = under-claimed).")
    ws4["A2"].font  = _font(False, "000000", 8)
    ws4["A2"].fill  = _f(YELLOW_BG); ws4["A2"].alignment = _aln("left", wrap=True)
    ws4["A2"].border = _bdr(); ws4.row_dimensions[2].height = 20

    # Row 3: group header bands
    # Band 1: A = blank, B-H = GSTR-2B RC, I-L = All 2B, M-P = 3B claimed, Q-T = 2B available, U-V = Shortfall, W-X = misc
    def _band(ws, r, c_start, c_end, label, bg):
        if c_start == c_end:
            ws.cell(row=r, column=c_start, value=label).fill = _f(bg)
            ws.cell(row=r, column=c_start).font = _font(True, HDR_FG, 8)
            ws.cell(row=r, column=c_start).alignment = _aln("center")
            ws.cell(row=r, column=c_start).border = _bdr()
        else:
            ws.merge_cells(f"{get_column_letter(c_start)}{r}:{get_column_letter(c_end)}{r}")
            c = ws.cell(row=r, column=c_start, value=label)
            c.font = _font(True, HDR_FG, 8); c.fill = _f(bg)
            c.alignment = _aln("center"); c.border = _bdr()
        ws.row_dimensions[r].height = 16

    _band(ws4, 3, 1,  1,  "Month",                        "2E75B6")
    _band(ws4, 3, 2,  8,  "◀ GSTR-2B Reverse Charge (RC = YES) ▶", "C00000")
    _band(ws4, 3, 9,  12, "◀ All GSTR-2B ITC (Consolidated Net) ▶", "4472C4")
    _band(ws4, 3, 13, 16, "◀ Portal: 3B ITC Claimed ▶",    "375623")
    _band(ws4, 3, 17, 20, "◀ Portal: 2B ITC Available ▶",  "7030A0")
    _band(ws4, 3, 21, 22, "◀ Shortfall (3B−2B) ▶",         "843C0C")
    _band(ws4, 3, 23, 24, "Summary",                        "1F3864")
    _band(ws4, 3, 25, 28, "◀ 2B ITC − 3B Claimed ▶",       "7030A0")

    # Row 4: column headers
    _hdr(ws4, SUM_COLS4, row=4, bg="1F3864")
    ws4.freeze_panes = "A5"
    ri4 = 5

    # ── Pre-build portal fallback dicts from ITC_Comparison_Portal (already written) ──
    # Used when itc_monthly month-matching failed due to short date labels (Apr-25 etc.)
    _portal_3b = {}
    _portal_2b = {}
    _ws_itcp = wb["ITC_Comparison_Portal"] if "ITC_Comparison_Portal" in wb.sheetnames else None
    if _ws_itcp:
        _short_map = {"apr":"April","may":"May","jun":"June","jul":"July",
                      "aug":"August","sep":"September","oct":"October",
                      "nov":"November","dec":"December","jan":"January",
                      "feb":"February","mar":"March"}
        for _r in range(7, 25):
            _lbl = str(_ws_itcp.cell(row=_r, column=1).value or "").strip().lower()
            if not _lbl or _lbl in ("none",""): continue
            _mn = None
            for _sh, _fl in _short_map.items():
                if _lbl.startswith(_sh): _mn = _fl; break
            if not _mn:
                for _fl in MONTHS_ORDER:
                    if _fl.lower() in _lbl: _mn = _fl; break
            if not _mn: continue
            def _cv(r, c):
                v = _ws_itcp.cell(row=r, column=c).value
                try: return float(v) if v else 0.0
                except Exception: return 0.0
            _portal_3b[_mn] = {"ig":_cv(_r,2),"cg":_cv(_r,3),"sg":_cv(_r,4)}
            _portal_2b[_mn] = {"ig":_cv(_r,6),"cg":_cv(_r,7),"sg":_cv(_r,8)}
    if _portal_3b:
        print(f"     Portal fallback loaded: {len(_portal_3b)} months from ITC_Comparison_Portal")

    # Build colour bands for data cells (alternating by column group)
    BAND_BG = {
        "rc":   ("FFF2CC", "FFFADE"),   # yellow tones for RC columns
        "all":  ("DEEAF1", "EBF3F9"),   # blue tones for All 2B
        "3b":   ("E2EFDA", "EDF6E4"),   # green tones for 3B claimed
        "2b":   ("EAD1DC", "F4E8EE"),   # purple tones for 2B available
        "sh":   ("FCE4D6", "FEF2EC"),   # orange tones for shortfall
        "sum":  (ALT1,      ALT2),      # neutral for summary
    }

    def _bg4(col_group, row_idx):
        pair = BAND_BG[col_group]
        return pair[row_idx % 2]

    ann4 = {k: 0.0 for k in ["rc_cnt","rc_tv","rc_ig","rc_cg","rc_sg",
                               "all_ig","all_cg","all_sg","all_tot",
                               "tl3b_ig","tl3b_cg","tl3b_sg","tl3b_tot",
                               "tl2b_ig","tl2b_cg","tl2b_sg","tl2b_tot",
                               "sh_ig","sh_tot",
                               "diff_ig","diff_cg","diff_sg","diff_tot"]}

    for mn in month_names:
        d    = rc_data.get(mn, {})
        d_tl = itc_monthly.get(mn, {})
        row_idx = ri4 - 5   # 0-based for alternating colour

        rc_cnt  = d.get("rc_count", 0)
        rc_tv   = sum(r[6] for r in d.get("rc_rows", []))
        rc_ig   = d.get("rc_igst", 0.)
        rc_cg   = d.get("rc_cgst", 0.)
        rc_sg   = d.get("rc_sgst", 0.)
        rc_tot  = round(rc_ig + rc_cg + rc_sg, 2)
        # All 2B ITC — use GSTR2B_Consolidated_Analysis Net values
        _c4 = consolidated.get(mn, {})
        all_ig  = _c4.get("net_igst",  d.get("all_igst", 0.))
        all_cg  = _c4.get("net_cgst",  d.get("all_cgst", 0.))
        all_sg  = _c4.get("net_sgst",  d.get("all_sgst", 0.))
        all_tot = _c4.get("net_total", round(all_ig + all_cg + all_sg, 2))
        rc_pct  = round(rc_ig / all_ig * 100, 1) if all_ig else 0.0
        rc_gstins = len(set(r[0] for r in d.get("rc_rows", [])))

        # Tax Liability portal values — prefer itc_monthly, fallback to ITC_Comparison_Portal
        if d_tl.get("3b_igst") or d_tl.get("3b_cgst"):
            tl3b_ig  = d_tl.get("3b_igst",  0.0)
            tl3b_cg  = d_tl.get("3b_cgst",  0.0)
            tl3b_sg  = d_tl.get("3b_sgst",  0.0)
            tl3b_tot = d_tl.get("3b_total", 0.0)
            tl2b_ig  = d_tl.get("2b_igst",  0.0)
            tl2b_cg  = d_tl.get("2b_cgst",  0.0)
            tl2b_sg  = d_tl.get("2b_sgst",  0.0)
            tl2b_tot = d_tl.get("2b_total", 0.0)
        else:
            _fb3 = _portal_3b.get(mn, {}); _fb2 = _portal_2b.get(mn, {})
            tl3b_ig = _fb3.get("ig", 0.0); tl3b_cg = _fb3.get("cg", 0.0); tl3b_sg = _fb3.get("sg", 0.0)
            tl3b_tot = round(tl3b_ig + tl3b_cg + tl3b_sg, 2)
            tl2b_ig = _fb2.get("ig", 0.0); tl2b_cg = _fb2.get("cg", 0.0); tl2b_sg = _fb2.get("sg", 0.0)
            tl2b_tot = round(tl2b_ig + tl2b_cg + tl2b_sg, 2)
        sh_ig    = round(tl3b_ig - tl2b_ig, 2)
        sh_cg    = round(tl3b_cg - tl2b_cg, 2)
        sh_sg    = round(tl3b_sg - tl2b_sg, 2)
        sh_tot   = round(sh_ig + sh_cg + sh_sg, 2)

        # Status
        found = d.get("found", False)
        has_tl = bool(d_tl)
        if not found:
            status_v = "⚠ 2B File Missing"; st_bg = RED_BG; st_fg = RED_FG
        elif rc_cnt > 0 and has_tl and sh_tot < -500:
            status_v = f"⚠ RC+Under-Claimed"; st_bg = RED_BG; st_fg = RED_FG
        elif rc_cnt > 0:
            status_v = f"⚠ {rc_cnt} RC Inv"; st_bg = YELLOW_BG; st_fg = YELLOW_FG
        elif has_tl and sh_tot < -500:
            status_v = "⚠ Under-Claimed"; st_bg = YELLOW_BG; st_fg = YELLOW_FG
        elif has_tl and sh_tot > 500:
            status_v = "⚠ Excess Claimed"; st_bg = YELLOW_BG; st_fg = YELLOW_FG
        else:
            status_v = "✓ OK"; st_bg = GREEN_BG; st_fg = GREEN_FG

        # Write cells with column-group colours
        def _w4(col, val, grp):
            cl = ws4.cell(row=ri4, column=col, value=val)
            cl.font = _font(False, "000000", 9)
            cl.fill = _f(_bg4(grp, row_idx))
            cl.alignment = _aln("right" if col > 1 else "left")
            cl.border = _bdr()
            if isinstance(val, float) and col > 2:
                cl.number_format = NUM_FMT
            return cl

        _w4(1,  mn,        "sum")
        _w4(2,  rc_gstins, "rc");  _w4(3,  rc_cnt,  "rc")
        _w4(4,  round(rc_tv,2), "rc")
        _w4(5,  rc_ig,    "rc");   _w4(6,  rc_cg,  "rc")
        _w4(7,  rc_sg,    "rc");   _w4(8,  rc_tot, "rc")
        _w4(9,  all_ig,   "all");  _w4(10, all_cg, "all")
        _w4(11, all_sg,   "all");  _w4(12, all_tot,"all")
        _w4(13, tl3b_ig,  "3b");   _w4(14, tl3b_cg,"3b")
        _w4(15, tl3b_sg,  "3b");   _w4(16, tl3b_tot,"3b")
        _w4(17, tl2b_ig,  "2b");   _w4(18, tl2b_cg,"2b")
        _w4(19, tl2b_sg,  "2b");   _w4(20, tl2b_tot,"2b")
        _w4(21, sh_ig,    "sh");   _w4(22, sh_tot, "sh")
        _w4(23, rc_pct,   "sum")

        # Status cell
        sc4 = ws4.cell(row=ri4, column=NC4, value=status_v)
        sc4.font = _font(True, st_fg, 9); sc4.fill = _f(st_bg)
        sc4.alignment = _aln("center"); sc4.border = _bdr()

        # 2B ITC − 3B Claimed difference columns (25-28)
        # FIX: use Python-computed values (not Excel formula strings) so
        # load_workbook(data_only=True) in build_final_consolidated can read them.
        diff_ig  = round(all_ig  - tl3b_ig,  2)
        diff_cg  = round(all_cg  - tl3b_cg,  2)
        diff_sg  = round(all_sg  - tl3b_sg,  2)
        diff_tot = round(all_tot - tl3b_tot, 2)

        for col_d, val_d in [(25, diff_ig), (26, diff_cg), (27, diff_sg), (28, diff_tot)]:
            cl_d = ws4.cell(row=ri4, column=col_d, value=val_d)
            cl_d.font      = _font(True, "000000", 9)
            cl_d.fill      = _f("EAD1F5")
            cl_d.alignment = _aln("right")
            cl_d.border    = _bdr()
            cl_d.number_format = NUM_FMT

        ws4.row_dimensions[ri4].height = 16; ri4 += 1

        for k, v in [("rc_cnt",rc_cnt),("rc_tv",rc_tv),("rc_ig",rc_ig),
                     ("rc_cg",rc_cg),("rc_sg",rc_sg),("all_ig",all_ig),
                     ("all_cg",all_cg),("all_sg",all_sg),("all_tot",all_tot),
                     ("tl3b_ig",tl3b_ig),("tl3b_cg",tl3b_cg),("tl3b_sg",tl3b_sg),
                     ("tl3b_tot",tl3b_tot),("tl2b_ig",tl2b_ig),("tl2b_cg",tl2b_cg),
                     ("tl2b_sg",tl2b_sg),("tl2b_tot",tl2b_tot),
                     ("sh_ig",sh_ig),("sh_tot",sh_tot),
                     ("diff_ig",diff_ig),("diff_cg",diff_cg),
                     ("diff_sg",diff_sg),("diff_tot",diff_tot)]:
            ann4[k] += v

    ann4_pct = round(ann4["rc_ig"] / ann4["all_ig"] * 100, 1) if ann4["all_ig"] else 0.0
    _totrow(ws4, ri4, [
        "ANNUAL TOTAL",
        "", int(ann4["rc_cnt"]),
        f"=SUM(D5:D{ri4-1})",
        f"=SUM(E5:E{ri4-1})", f"=SUM(F5:F{ri4-1})",
        f"=SUM(G5:G{ri4-1})", f"=SUM(H5:H{ri4-1})",
        f"=SUM(I5:I{ri4-1})", f"=SUM(J5:J{ri4-1})",
        f"=SUM(K5:K{ri4-1})", f"=SUM(L5:L{ri4-1})",
        f"=SUM(M5:M{ri4-1})", f"=SUM(N5:N{ri4-1})",
        f"=SUM(O5:O{ri4-1})", f"=SUM(P5:P{ri4-1})",
        f"=SUM(Q5:Q{ri4-1})", f"=SUM(R5:R{ri4-1})",
        f"=SUM(S5:S{ri4-1})", f"=SUM(T5:T{ri4-1})",
        f"=SUM(U5:U{ri4-1})", f"=SUM(V5:V{ri4-1})",
        ann4_pct, "",
        # NEW diff totals
        f"=SUM(Y5:Y{ri4-1})", f"=SUM(Z5:Z{ri4-1})",
        f"=SUM(AA5:AA{ri4-1})", f"=SUM(AB5:AB{ri4-1})",
    ])
    
    # ── Populate Portal ITC Claimed values from ITC_Comparison_Portal sheet ───
    # Copy Portal ITC Claimed (IGST, CGST, SGST, Total) to RC_Summary_Only
    _snames = wb.sheetnames
    ws_itc = wb["ITC_Comparison_Portal"] if "ITC_Comparison_Portal" in _snames else (wb["ITC (Other than IMPG)"] if "ITC (Other than IMPG)" in _snames else None)
    
    if ws_itc:
        # ITC_Comparison_Portal: rows 7-18 contain Apr-25 to Mar-26 data
        # Columns B-D: IGST (col 2), CGST (col 3), SGST/UTGST (col 4)
        
        data_start_row = 5  # RC_Summary_Only data starts at row 5 (April)
        for ri in range(data_start_row, ri4):
            row_offset = ri - data_start_row
            itc_row = 7 + row_offset  # ITC data starts at row 7 (Apr-25)
            
            try:
                # Column M (13): Portal 3B ITC IGST
                igst_val = ws_itc.cell(row=itc_row, column=2).value
                if igst_val and isinstance(igst_val, (int, float)):
                    ws4.cell(row=ri, column=13, value=igst_val)
                    ws4.cell(row=ri, column=13).number_format = NUM_FMT
                
                # Column N (14): Portal 3B ITC CGST
                cgst_val = ws_itc.cell(row=itc_row, column=3).value
                if cgst_val and isinstance(cgst_val, (int, float)):
                    ws4.cell(row=ri, column=14, value=cgst_val)
                    ws4.cell(row=ri, column=14).number_format = NUM_FMT
                
                # Column O (15): Portal 3B ITC SGST
                sgst_val = ws_itc.cell(row=itc_row, column=4).value
                if sgst_val and isinstance(sgst_val, (int, float)):
                    ws4.cell(row=ri, column=15, value=sgst_val)
                    ws4.cell(row=ri, column=15).number_format = NUM_FMT
                
                # Column P (16): Portal 3B ITC Total (sum of IGST+CGST+SGST)
                try:
                    igst_f = float(igst_val) if igst_val else 0.0
                    cgst_f = float(cgst_val) if cgst_val else 0.0
                    sgst_f = float(sgst_val) if sgst_val else 0.0
                    total = round(igst_f + cgst_f + sgst_f, 2)
                    if total > 0:
                        ws4.cell(row=ri, column=16, value=total)
                        ws4.cell(row=ri, column=16).number_format = NUM_FMT
                except Exception:  # FIX v11
                    pass
            except Exception:  # FIX v11
                pass
        
        # Columns Q-T (Portal 2B ITC): pull IGST, CGST, SGST, Total from ITC_Comparison_Portal
        # ITC_Comparison_Portal cols: 6=2B IGST, 7=2B CGST, 8=2B SGST (1-indexed)
        if ws_itc:
            for ri in range(data_start_row, ri4):
                row_offset = ri - data_start_row
                itcp_row = 7 + row_offset  # ITC data starts at row 7 (Apr-25)

                try:
                    ig2b  = ws_itc.cell(row=itcp_row, column=6).value or 0.0
                    cg2b  = ws_itc.cell(row=itcp_row, column=7).value or 0.0
                    sg2b  = ws_itc.cell(row=itcp_row, column=8).value or 0.0
                    tot2b = round(float(ig2b) + float(cg2b) + float(sg2b), 2)

                    for col_idx, val in [(17, ig2b), (18, cg2b), (19, sg2b), (20, tot2b)]:
                        if isinstance(val, (int, float)):
                            ws4.cell(row=ri, column=col_idx, value=val).number_format = NUM_FMT

                    # Shortfall formulas: 3B − 2B (U=IGST, V=Total)
                    # FIX: use Python-computed shortfall values so data_only reads work
                    _sh_ig_v  = (ws_itc.cell(row=itcp_row, column=2).value or 0.0)
                    _sh_tot_v = (ws_itc.cell(row=itcp_row, column=2).value or 0.0) +                                 (ws_itc.cell(row=itcp_row, column=3).value or 0.0) +                                 (ws_itc.cell(row=itcp_row, column=4).value or 0.0)
                    try:
                        _q_val  = float(ws4.cell(row=ri, column=17).value or 0.0)
                        _t_val  = float(ws4.cell(row=ri, column=20).value or 0.0)
                        _m_val  = float(ws4.cell(row=ri, column=13).value or 0.0)
                        _p_val  = float(ws4.cell(row=ri, column=16).value or 0.0)
                        ws4.cell(row=ri, column=21, value=round(_m_val - _q_val, 2)).number_format = NUM_FMT
                        ws4.cell(row=ri, column=22, value=round(_p_val - _t_val, 2)).number_format = NUM_FMT
                    except Exception:
                        ws4.cell(row=ri, column=21, value=0.0).number_format = NUM_FMT
                        ws4.cell(row=ri, column=22, value=0.0).number_format = NUM_FMT
                except Exception:
                    pass
    
    ws4.sheet_properties.tabColor = "FF0000"

    # ──────────────────────────────────────────────────────────────────────
    # SHEET 5: ITC_Difference
    #   = All GSTR-2B ITC (net of CDNR)  MINUS  Portal 3B ITC Claimed
    #   Positive  → 2B has MORE ITC than claimed (excess available / under-claimed)
    #   Negative  → 3B claimed MORE than 2B shows (excess claimed / error)
    # Columns:
    #   A=Month
    #   B=All 2B IGST, C=All 2B CGST, D=All 2B SGST, E=All 2B Total
    #   F=Portal 3B Claimed IGST, G=CGST, H=SGST, I=Total
    #   J=Diff IGST (B−F), K=Diff CGST (C−G), L=Diff SGST (D−H), M=Diff Total (E−I)
    #   N=Remark
    # ──────────────────────────────────────────────────────────────────────
    ws5 = wb.create_sheet("ITC_Difference")
    ws5.sheet_view.showGridLines = False

    DIFF_COLS = [
        ("Month", 14),
        # All 2B ITC
        ("All 2B\nIGST ₹", 15), ("All 2B\nCGST ₹", 15),
        ("All 2B\nSGST ₹", 15), ("All 2B\nTotal ₹", 15),
        # Portal 3B Claimed
        ("3B Claimed\nIGST ₹", 15), ("3B Claimed\nCGST ₹", 15),
        ("3B Claimed\nSGST ₹", 15), ("3B Claimed\nTotal ₹", 15),
        # Difference = 2B − 3B
        ("Diff\nIGST ₹", 15), ("Diff\nCGST ₹", 15),
        ("Diff\nSGST ₹", 15), ("Diff\nTotal ₹", 15),
        # Remark
        ("Remark", 24),
    ]
    NC5 = len(DIFF_COLS)

    _title(ws5, f"All GSTR-2B ITC  vs  Portal 3B ITC Claimed — {client_name} — FY {fy}", NC5)

    ws5.merge_cells(f"A2:{get_column_letter(NC5)}2")
    ws5["A2"].value = ("All GSTR-2B ITC sourced from GSTR2B_Consolidated_Analysis Month-wise Summary (Net values, net of Credit Notes).  "
                       "Difference = Net 2B ITC  MINUS  Portal 3B ITC Claimed.  "
                       "Positive = 2B ITC exceeds 3B claimed (under-claimed / ITC available but not taken).  "
                       "Negative = 3B claimed exceeds 2B (excess claimed — needs attention).")
    ws5["A2"].font      = _font(False, "000000", 8)
    ws5["A2"].fill      = _f(YELLOW_BG)
    ws5["A2"].alignment = _aln("left", wrap=True)
    ws5["A2"].border    = _bdr()
    ws5.row_dimensions[2].height = 22

    # Band headers row 3
    _band(ws5, 3, 1,  1,  "Month",                             "2E75B6")
    _band(ws5, 3, 2,  5,  "◀ All GSTR-2B ITC (net of CDNR) ▶", "4472C4")
    _band(ws5, 3, 6,  9,  "◀ Portal: 3B ITC Claimed ▶",        "375623")
    _band(ws5, 3, 10, 13, "◀ Difference (2B − 3B Claimed) ▶",  "843C0C")
    _band(ws5, 3, 14, 14, "Remark",                             "1F3864")

    _hdr(ws5, DIFF_COLS, row=4, bg="1F3864")
    ws5.freeze_panes = "A5"
    ri5 = 5

    ann5 = {k: 0.0 for k in ["a_ig","a_cg","a_sg","a_tot",
                               "p_ig","p_cg","p_sg","p_tot",
                               "d_ig","d_cg","d_sg","d_tot"]}

    for mn in month_names:
        d    = rc_data.get(mn, {})
        d_tl = itc_monthly.get(mn, {})
        row_idx = ri5 - 5

        # All 2B ITC (net of CDNR) — use GSTR2B_Consolidated_Analysis Net values
        _c5 = consolidated.get(mn, {})
        a_ig  = _c5.get("net_igst",  d.get("all_igst", 0.0))
        a_cg  = _c5.get("net_cgst",  d.get("all_cgst", 0.0))
        a_sg  = _c5.get("net_sgst",  d.get("all_sgst", 0.0))
        a_tot = _c5.get("net_total", round(a_ig + a_cg + a_sg, 2))

        # Portal 3B Claimed — prefer itc_monthly, fall back to ITC_Comparison_Portal sheet
        if d_tl.get("3b_igst") or d_tl.get("3b_cgst"):
            p_ig  = d_tl.get("3b_igst",  0.0)
            p_cg  = d_tl.get("3b_cgst",  0.0)
            p_sg  = d_tl.get("3b_sgst",  0.0)
            p_tot = d_tl.get("3b_total", round(p_ig + p_cg + p_sg, 2))
        else:
            _fb = _portal_3b.get(mn, {})
            p_ig  = _fb.get("ig", 0.0)
            p_cg  = _fb.get("cg", 0.0)
            p_sg  = _fb.get("sg", 0.0)
            p_tot = round(p_ig + p_cg + p_sg, 2)

        # Difference
        d_ig  = round(a_ig  - p_ig,  2)
        d_cg  = round(a_cg  - p_cg,  2)
        d_sg  = round(a_sg  - p_sg,  2)
        d_tot = round(a_tot - p_tot, 2)

        # Remark
        if not d.get("found", False):
            remark = "⚠ GSTR-2B file missing"
            rem_bg = RED_BG; rem_fg = RED_FG
        elif p_ig == 0 and p_cg == 0 and p_sg == 0:
            remark = "⚠ Portal 3B data missing"
            rem_bg = YELLOW_BG; rem_fg = YELLOW_FG
        elif d_tot < -500:
            remark = f"❌ Excess Claimed ₹{abs(d_tot):,.0f}"
            rem_bg = RED_BG; rem_fg = RED_FG
        elif d_tot > 500:
            remark = f"⚠ Under-Claimed ₹{d_tot:,.0f}"
            rem_bg = YELLOW_BG; rem_fg = YELLOW_FG
        else:
            remark = "✓ Match"
            rem_bg = GREEN_BG; rem_fg = GREEN_FG

        def _w5(col, val, bg_hex, fg_hex="000000"):
            cl = ws5.cell(row=ri5, column=col, value=val)
            cl.font      = _font(False, fg_hex, 9)
            cl.fill      = _f(bg_hex)
            cl.alignment = _aln("right" if col > 1 else "left")
            cl.border    = _bdr()
            if isinstance(val, float) and col > 1:
                cl.number_format = NUM_FMT
            return cl

        # All 2B columns (blue)
        _bg_a = "DEEAF1" if row_idx % 2 == 0 else "EBF3F9"
        _w5(1, mn,    _bg_a, "000000"); _w5(1, mn, _bg_a)
        ws5.cell(row=ri5, column=1, value=mn).fill = _f(_bg_a)
        ws5.cell(row=ri5, column=1).font = _font(False, "000000", 9)
        ws5.cell(row=ri5, column=1).alignment = _aln("left")
        ws5.cell(row=ri5, column=1).border = _bdr()
        for ci, v in [(2, a_ig),(3, a_cg),(4, a_sg),(5, a_tot)]:
            _w5(ci, v, _bg_a)

        # Portal 3B columns (green)
        _bg_p = "E2EFDA" if row_idx % 2 == 0 else "EDF6E4"
        for ci, v in [(6, p_ig),(7, p_cg),(8, p_sg),(9, p_tot)]:
            _w5(ci, v, _bg_p)

        # Difference columns — colour by sign
        for ci, v in [(10, d_ig),(11, d_cg),(12, d_sg),(13, d_tot)]:
            if v < -1:
                diff_bg = RED_BG; diff_fg = RED_FG      # negative = excess claimed
            elif v > 1:
                diff_bg = YELLOW_BG; diff_fg = YELLOW_FG  # positive = under-claimed
            else:
                diff_bg = GREEN_BG; diff_fg = GREEN_FG   # near zero = match
            cl = ws5.cell(row=ri5, column=ci, value=v)
            cl.font = _font(True, diff_fg, 9); cl.fill = _f(diff_bg)
            cl.alignment = _aln("right"); cl.border = _bdr()
            cl.number_format = NUM_FMT

        # Remark
        rc5 = ws5.cell(row=ri5, column=14, value=remark)
        rc5.font = _font(True, rem_fg, 9); rc5.fill = _f(rem_bg)
        rc5.alignment = _aln("center"); rc5.border = _bdr()

        ws5.row_dimensions[ri5].height = 16
        ri5 += 1

        for k, v in [("a_ig",a_ig),("a_cg",a_cg),("a_sg",a_sg),("a_tot",a_tot),
                     ("p_ig",p_ig),("p_cg",p_cg),("p_sg",p_sg),("p_tot",p_tot),
                     ("d_ig",d_ig),("d_cg",d_cg),("d_sg",d_sg),("d_tot",d_tot)]:
            ann5[k] += v

    # Annual total row
    def _diff_tot_cell(ws, row, col, val):
        cl = ws.cell(row=row, column=col, value=round(val, 2))
        cl.font = _font(True, HDR_FG, 9); cl.fill = _f("1F3864")
        cl.alignment = _aln("right"); cl.border = _bdr()
        cl.number_format = NUM_FMT
        return cl

    tot_row5 = ri5
    ws5.cell(row=tot_row5, column=1, value="ANNUAL TOTAL").font  = _font(True, HDR_FG, 9)
    ws5.cell(row=tot_row5, column=1).fill      = _f("1F3864")
    ws5.cell(row=tot_row5, column=1).alignment = _aln("left")
    ws5.cell(row=tot_row5, column=1).border    = _bdr()
    for ci, k in [(2,"a_ig"),(3,"a_cg"),(4,"a_sg"),(5,"a_tot"),
                  (6,"p_ig"),(7,"p_cg"),(8,"p_sg"),(9,"p_tot"),
                  (10,"d_ig"),(11,"d_cg"),(12,"d_sg"),(13,"d_tot")]:
        _diff_tot_cell(ws5, tot_row5, ci, ann5[k])

    # Overall remark for annual total
    ann_d_tot = round(ann5["d_tot"], 2)
    if ann_d_tot < -500:
        ann_remark = f"❌ Annual Excess Claimed ₹{abs(ann_d_tot):,.0f}"
        ar_bg = RED_BG; ar_fg = RED_FG
    elif ann_d_tot > 500:
        ann_remark = f"⚠ Annual Under-Claimed ₹{ann_d_tot:,.0f}"
        ar_bg = YELLOW_BG; ar_fg = YELLOW_FG
    else:
        ann_remark = "✓ Annual Match"
        ar_bg = GREEN_BG; ar_fg = GREEN_FG
    arc = ws5.cell(row=tot_row5, column=14, value=ann_remark)
    arc.font = _font(True, ar_fg, 9); arc.fill = _f(ar_bg)
    arc.alignment = _aln("center"); arc.border = _bdr()
    ws5.row_dimensions[tot_row5].height = 18

    ws5.sheet_properties.tabColor = "FF6600"
    print(f"  ✓  ITC_Difference sheet created  "
          f"Annual 2B={ann5['a_tot']:,.2f}  "
          f"3B Claimed={ann5['p_tot']:,.2f}  "
          f"Diff={ann5['d_tot']:+,.2f}")

    # ──────────────────────────────────────────────────────────────────────
    # Save
    # ──────────────────────────────────────────────────────────────────────
    wb.save(str(out_path))
    print(f"\n  ✅  Report saved: {out_path}")
    return str(out_path)


# ═══════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════
def main():
    """
    Supports two modes:
      1. CLI mode (called by run_all.py):
           python gst_comparison_report_v2.py
             --folder <path>   GST Automation folder containing source files
             --fy    <2025-26> Financial year
             --client <name>   Client name for report title
             --out   <path>    Output file path (optional; defaults to folder/GST_Comparison_Report_*.xlsx)

      2. Interactive mode (run directly by user):
           python gst_comparison_report_v2.py
           (prompts for folder, FY, client name)
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="GST Comparison Report Generator",
        add_help=True,
    )
    parser.add_argument("--folder",  default=None, help="GST Automation folder path")
    parser.add_argument("--fy",      default=None, help="Financial Year e.g. 2026-27")
    parser.add_argument("--client",  default=None, help="Client name for report title")
    parser.add_argument("--out",     default=None, help="Output .xlsx file path (optional)")

    # Parse only known args so it doesn't fail if called with extra args
    args, _ = parser.parse_known_args()

    # ── Decide mode ──────────────────────────────────────────────────────────
    cli_mode = bool(args.folder)

    if cli_mode:
        # ── CLI mode ─────────────────────────────────────────────────────────
        folder_in = args.folder.strip().strip('"').strip("'")
        if not os.path.isdir(folder_in):
            print(f"  ✗ Folder not found: {folder_in}")
            sys.exit(1)

        fy          = (args.fy or "2026-27").strip()
        client_name = (args.client or "Client").strip()

        if args.out:
            out_path = Path(args.out.strip().strip('"').strip("'"))
            out_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            ts      = datetime.now().strftime("%Y%m%d_%H%M")
            fy_tag  = fy.replace("-", "_")
            out_name = f"GST_Comparison_Report_{client_name.replace(' ','_')}_{fy_tag}_{ts}.xlsx"
            out_path = Path(folder_in) / out_name

        print(f"\n  GST Comparison Report — CLI mode")
        print(f"  Folder : {folder_in}")
        print(f"  FY     : {fy}")
        print(f"  Client : {client_name}")
        print(f"  Output : {out_path}\n")

        result = build_report(folder_in, fy, client_name, out_path)
        if not result:
            sys.exit(1)

    else:
        # ── Interactive mode ─────────────────────────────────────────────────
        print("\n" + "="*65)
        print("  GST COMPARISON REPORT GENERATOR")
        print("  Tax Liability vs ITC | GSTR-2B RC Month-wise")
        print("="*65 + "\n")

        folder_in = input("  Enter folder path containing GST files: ").strip()
        if not folder_in or not os.path.isdir(folder_in):
            print(f"  ✗ Folder not found: {folder_in}"); return

        fy_in = input("  Financial Year (e.g. 2025-26) [ENTER = 2025-26]: ").strip()
        fy = fy_in if fy_in else "2025-26"

        client_in = input("  Client name (for report title): ").strip()
        client_name = client_in if client_in else "Client"

        ts      = datetime.now().strftime("%Y%m%d_%H%M")
        fy_tag  = fy.replace("-","_")
        out_name = f"GST_Comparison_Report_{client_name.replace(' ','_')}_{fy_tag}_{ts}.xlsx"
        out_path = Path(folder_in) / out_name

        build_report(folder_in, fy, client_name, out_path)
        input("\n  Press Enter to exit...")

if __name__ == "__main__":
    main()
