"""
build_final_consolidated.py  v4  (OptionB / run_all compatible)
===============================================================
Builds FINAL_CONSOLIDATED_REPORT_<ts>.xlsx  — 7 organised sheets.

FIXES in v4:
  Sheet 2 GSTR-1   : Bills sorted month→date→invoice_no, month subtotals, annual total (vertical)
  Sheet 3 GSTR-2B  : ITC bills sorted, month subtotals, annual total (vertical)
  Sheet 4 R1 vs 3B : Clean month-wise reconciliation + annual total
  Sheet 5 GST vs IT: AIS Sales / TIS Sales values correctly read from AIS_vs_GSTR_Monthly
  Sheet 6 Bridge   : AIS monthly turnover + purchase detail from IT_RECONCILIATION
  Sheet 7 26AS     : TDS detail + GSTR-3B status + IT filing checklist
  Invoice numbers  : sorted numerically within each month
  Totals           : recalculated cleanly, no double-counting

USAGE
-----
    python build_final_consolidated.py --base C:\\Users\\X\\Downloads --out report.xlsx
    python build_final_consolidated.py [CLIENT_FOLDER]   (legacy)
"""

import argparse
import re
import sys
from copy import copy
from datetime import datetime
from pathlib import Path

import openpyxl
from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

# ── Palette ──────────────────────────────────────────────────────────────────
DARK_BLUE = "1F3864"
MED_BLUE  = "2E75B6"
HDR_BG    = "1F3864"
SEC1_BG   = "1F4E79"
SEC2_BG   = "375623"
SEC3_BG   = "7B3F00"
ALT1      = "FFFFFF"
ALT2      = "EEF2FF"
GREEN_BG  = "C6EFCE"
RED_BG    = "FFC7CE"
YELLOW_BG = "FFEB9C"
MONTH_BG  = "BDD7EE"
SUB_BG    = "DDEBF7"
ANN_BG    = "1F3864"
NUM_FMT   = "#,##0.00"
FY_MONTHS = ["APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC","JAN","FEB","MAR"]


# ── Style helpers ─────────────────────────────────────────────────────────────
def _bd():
    s = Side(style="thin")
    return Border(left=s, right=s, top=s, bottom=s)

def _font(bold=False, color="000000", size=9, name="Arial"):
    return Font(name=name, bold=bold, color=color, size=size)

def _fill(color):
    return PatternFill("solid", fgColor=color)

def _aln(h="left", v="center"):
    return Alignment(horizontal=h, vertical=v, wrap_text=False)

def _w(ws, row, col, value, bg=ALT1, bold=False, fg="000000",
       numfmt=None, align="left", size=9):
    # Coerce numeric strings when a number format is expected
    if numfmt and isinstance(value, str):
        try: value = float(value.replace(",", ""))
        except (ValueError, AttributeError): pass
    c = ws.cell(row=row, column=col, value=value)
    c.font = _font(bold, fg, size)
    c.fill = _fill(bg)
    c.alignment = _aln(align)
    c.border = _bd()
    if numfmt and isinstance(value, (int, float)):
        c.number_format = numfmt
    return c

def _title(ws, row, c1, c2, text, bg=DARK_BLUE, fg="FFFFFF", size=12, h=24):
    ws.merge_cells(f"{get_column_letter(c1)}{row}:{get_column_letter(c2)}{row}")
    c = ws.cell(row=row, column=c1, value=text)
    c.font = _font(True, fg, size); c.fill = _fill(bg)
    c.alignment = _aln("center"); c.border = _bd()
    ws.row_dimensions[row].height = h

def _hdr(ws, row, hw, bg=HDR_BG):
    for ci, (h, w) in enumerate(hw, 1):
        c = ws.cell(row=row, column=ci, value=h)
        c.font = _font(True, "FFFFFF", 9); c.fill = _fill(bg)
        c.alignment = _aln("center"); c.border = _bd()
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[row].height = 16

def _month_sep(ws, row, ncols, label, count=None):
    text = f"── {label}" + (f"  ({count} records)" if count is not None else "")
    ws.merge_cells(f"A{row}:{get_column_letter(ncols)}{row}")
    c = ws.cell(row=row, column=1, value=text)
    c.font = _font(True, "1F3864", 9); c.fill = _fill(MONTH_BG)
    c.alignment = _aln("left"); c.border = _bd()
    ws.row_dimensions[row].height = 14
    return row + 1

def _subtot(ws, row, ncols, label, vals_by_col):
    for ci in range(1, ncols+1):
        v = vals_by_col.get(ci)
        is_n = isinstance(v, (int, float))
        c = ws.cell(row=row, column=ci, value=(v if v is not None else None))
        c.font = _font(True if (ci==1 or is_n) else False, "1F3864", 9)
        c.fill = _fill(SUB_BG); c.alignment = _aln("right" if is_n else "left")
        c.border = _bd()
        if is_n: c.number_format = NUM_FMT
    ws.cell(row=row, column=1).value = label
    ws.cell(row=row, column=1).alignment = _aln("left")
    ws.row_dimensions[row].height = 14
    return row + 1

def _anntot(ws, row, ncols, label, vals_by_col):
    for ci in range(1, ncols+1):
        v = vals_by_col.get(ci)
        is_n = isinstance(v, (int, float))
        c = ws.cell(row=row, column=ci, value=(v if v is not None else None))
        c.font = _font(True, "FFFFFF", 9); c.fill = _fill(ANN_BG)
        c.alignment = _aln("right" if is_n else "left"); c.border = _bd()
        if is_n: c.number_format = NUM_FMT
    ws.cell(row=row, column=1).value = label
    ws.cell(row=row, column=1).alignment = _aln("left")
    ws.row_dimensions[row].height = 16
    return row + 2

def _sec_banner(ws, row, ncols, text, bg):
    ws.merge_cells(f"A{row}:{get_column_letter(ncols)}{row}")
    c = ws.cell(row=row, column=1, value=text)
    c.font = _font(True, "FFFFFF", 10); c.fill = _fill(bg)
    c.alignment = _aln("left"); c.border = _bd()
    ws.row_dimensions[row].height = 18
    return row + 1


# ── File helpers ──────────────────────────────────────────────────────────────
def _rglob(folder, pattern):
    if not folder or not folder.exists(): return None
    m = sorted(folder.rglob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return m[0] if m else None

def _find_client_folders(base):
    out = []
    if not base or not base.exists(): return out
    try:
        for d in sorted(base.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True):
            if d.is_dir() and not d.name.startswith("."):
                if (d/"GST Automation").exists() or (d/"IT Download").exists():
                    out.append(d)
    except Exception: pass
    return out

def _sources(cf):
    g = cf/"GST Automation"; i = cf/"IT Download"
    ib = cf/"IT Bridge";     gc = cf/"GST IT Comparison"; r = cf/"26AS vs GSTR1"
    return (_rglob(g,"ANNUAL_RECONCILIATION*.xlsx"),
            _rglob(i,"IT_RECONCILIATION*.xlsx"),
            _rglob(r,"26AS_GSTR1_Compare*.xlsx"),
            _rglob(gc,"GSTR2B_EXTRACT*.xlsx"),
            _rglob(gc,"TIS_AIS_COMPARISON*.xlsx"),
            _rglob(ib,"MASTER_GST_IT_RECONCILIATION*.xlsx"),
            _rglob(g,"*RECONCILED*.xlsx") or _rglob(cf,"*RECONCILED*.xlsx"))

def _fy(client_list):
    for _, cf in client_list:
        for f in (cf/"GST Automation").glob("ANNUAL_RECONCILIATION*.xlsx"):
            m = re.search(r"(\d{4}_\d{2,4})", f.name)
            if m: return m.group(1).replace("_","-")
    return "2025-26"

def _sf(v):
    try: return float(v) if v not in (None,"","-") else 0.0
    except: return 0.0

def _inv_key(r):
    inv = str(r[3] or "") if len(r)>3 else ""
    dt  = str(r[4] or "") if len(r)>4 else ""
    num = int(re.sub(r"\D","",inv)) if re.sub(r"\D","",inv) else 0
    return (dt, num, inv)

def _load(wb_path, sheet, skip=2):
    if not wb_path or not Path(wb_path).exists(): return [], []
    try: wb = load_workbook(str(wb_path), data_only=True)
    except Exception as e:
        print(f"  [WARN] {Path(wb_path).name}: {e}"); return [], []
    if sheet not in wb.sheetnames:
        print(f"  [WARN] '{sheet}' not in {Path(wb_path).name} {wb.sheetnames[:5]}")
        return [], []
    ws   = wb[sheet]
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) <= skip: return [], []
    return [str(c or "") for c in rows[skip-1]], [r for r in rows[skip:] if any(c is not None for c in r)]

def _validate_columns(headers, data_rows, source_label):
    """
    For each header column, check whether any data row has a non-empty, non-zero value.
    Print a warning for columns that are always blank/zero — this ensures the user knows
    if a header exists in the source but no values flowed through to the output.
    """
    if not headers or not data_rows:
        return
    blank_cols = []
    for ci, hdr in enumerate(headers):
        if not hdr or hdr.strip() in ("", "None"): continue
        has_value = False
        for row in data_rows:
            if ci >= len(row): continue
            v = row[ci]
            if v is None or v == "" or v == 0 or v == 0.0: continue
            try:
                if float(str(v).replace(",","")) == 0: continue
            except: pass
            has_value = True; break
        if not has_value:
            blank_cols.append(hdr.strip())
    if blank_cols:
        print(f"  [COL-CHECK] {source_label}: headers with NO data values → {blank_cols}")
    else:
        print(f"  [COL-CHECK] {source_label}: all {len([h for h in headers if h.strip()])} header columns have data ✓")


# ── Sheet 1: Run Summary ──────────────────────────────────────────────────────
def _sh1(wb, clients, fy):
    ws = wb.create_sheet("1_Run_Summary"); ts = datetime.now().strftime("%d-%b-%Y %H:%M")
    _title(ws,1,1,9,f"FINAL CONSOLIDATED REPORT — FY {fy}  |  Generated: {ts}",h=28,size=13)
    _hdr(ws,2,[("Client",30),("Annual",14),("IT Recon",12),("26AS",12),
               ("Bridge",12),("GST-IT",12),("GSTR1-FY",14),("Status",12),("Notes",28)])
    ri=3
    for i,(cn,cf) in enumerate(clients):
        bg=ALT1 if i%2==0 else ALT2
        AN,IT,C2,GB,TA,BR,RC=_sources(cf)
        GF=_rglob(cf/"GST Automation","GSTR1_FY_*.xlsx")
        ok=all([AN,IT]); st="COMPLETE" if ok else ("PARTIAL" if any([AN,IT]) else "MISSING")
        sb=GREEN_BG if ok else (YELLOW_BG if st=="PARTIAL" else RED_BG)
        sf="276221" if ok else ("7D5A00" if st=="PARTIAL" else "9C0006")
        t=lambda f:"✓" if f else "✗"
        for ci,v in enumerate([cn,t(AN),t(IT),t(C2),t(BR),t(TA),t(GF)],1):
            _w(ws,ri,ci,v,bg=bg,bold=(ci==1),align="center" if ci>1 else "left")
        c=ws.cell(row=ri,column=8,value=st)
        c.font=_font(True,sf,9); c.fill=_fill(sb); c.alignment=_aln("center"); c.border=_bd()
        notes=[]
        if not AN: notes.append("No Annual Recon")
        if not IT: notes.append("No IT Recon")
        _w(ws,ri,9," | ".join(notes) if notes else "All files OK",bg=bg)
        ri+=1
    ws.freeze_panes="A3"
    print(f"  ✓ Sheet 1 — Summary ({len(clients)} clients)")


# ── Sheet 2: GSTR-1 — Bill-wise | Month-wise | Company-wise (3 stacked sections) ─
def _sh2(wb, cn, fy, annual):
    safe = re.sub(r"[^A-Za-z0-9 ]", "", cn)[:16]
    ws = wb.create_sheet(f"2_GSTR1_{safe}"[:31])
    ws.sheet_view.showGridLines = False
    NC = 12
    hw = [("Type",8),("GSTIN Receiver",22),("Receiver Name",28),("Invoice No",14),
          ("Invoice Date",13),("Invoice Value \u20b9",16),("Place of Supply",15),("Rate %",7),
          ("Taxable Value \u20b9",16),("IGST \u20b9",12),("CGST \u20b9",12),("SGST \u20b9",12)]

    _title(ws, 1, 1, NC, f"GSTR-1 Invoice Detail \u2014 {cn} \u2014 FY {fy}", bg=SEC1_BG, size=12, h=24)

    # ── SECTION 1: BILL-WISE DETAIL ──────────────────────────────────────────
    ri = 2
    ri = _sec_banner(ws, ri, NC, "\u25b6  SECTION 1 \u2014 BILL-WISE INVOICE DETAIL", SEC1_BG)
    _hdr(ws, ri, hw, bg=HDR_BG); ri += 1
    ws.freeze_panes = f"A{ri}"

    hdr_raw, raw = _load(annual, "GSTR1_Invoice_Detail", skip=2)
    # Fallback: try alternate sheet names used in some versions
    if not raw:
        hdr_raw, raw = _load(annual, "GSTR1_Bill_Wise", skip=2)
    if not raw:
        hdr_raw, raw = _load(annual, "Sales_Invoice_Detail", skip=2)
    if raw:
        _validate_columns(hdr_raw, raw, "GSTR1_Invoice_Detail")

    m_order = []; m_data = {}; cur = None
    for row in (raw or []):
        v0 = str(row[0] or "").strip()
        is_sep = (v0.startswith("--") or (
            row[1] is None and row[2] is None and
            any(m in v0.upper() for m in FY_MONTHS)))
        is_tot = v0.upper() in ("ANNUAL TOTAL","GRAND TOTAL")
        if is_sep:
            lbl = re.sub(r"[-\u2013\u2014]", " ", v0)
            lbl = re.sub(r"\d+\s*records?", "", lbl, flags=re.I).strip()
            lbl = re.sub(r"\s+", " ", lbl).strip()
            cur = lbl
            if cur not in m_data: m_order.append(cur); m_data[cur] = []
        elif is_tot:
            continue
        elif cur is not None:
            m_data[cur].append(row)

    ann = {k:0. for k in ["iv","tx","ig","cg","sg"]}; tot_r = 0
    for ml in m_order:
        rows = m_data.get(ml, [])
        if not rows: continue
        try: rows = sorted(rows, key=_inv_key)
        except: pass
        ri = _month_sep(ws, ri, NC, ml, len(rows))
        mv = mt = mi = mc = ms = 0.
        for row in rows:
            bg = ALT1 if ri % 2 == 0 else ALT2
            iv=_sf(row[5] if len(row)>5 else 0); tx=_sf(row[8] if len(row)>8 else 0)
            ig=_sf(row[9] if len(row)>9 else 0); cg=_sf(row[10] if len(row)>10 else 0)
            sg=_sf(row[11] if len(row)>11 else 0)
            vals=[row[0],row[1],row[2],row[3],row[4],iv,row[6],row[7],tx,ig,cg,sg]
            for ci,v in enumerate(vals,1):
                n = ci in (6,9,10,11,12)
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if n else None,
                   align="right" if n else ("center" if ci==7 else "left"))
            ws.row_dimensions[ri].height = 14
            mv+=iv; mt+=tx; mi+=ig; mc+=cg; ms+=sg
            ann["iv"]+=iv; ann["tx"]+=tx; ann["ig"]+=ig; ann["cg"]+=cg; ann["sg"]+=sg
            tot_r += 1; ri += 1
        ri = _subtot(ws, ri, NC, f"  Subtotal \u2014 {ml}",
                     {1:f"  Subtotal \u2014 {ml}",6:round(mv,2),9:round(mt,2),
                      10:round(mi,2),11:round(mc,2),12:round(ms,2)})

    if tot_r == 0:
        ws.merge_cells(f"A{ri}:{get_column_letter(NC)}{ri}")
        c = ws.cell(row=ri, column=1, value="  No bill-wise data found in source file (GSTR1_Invoice_Detail)")
        c.font = _font(False,"9C0006",9); c.fill = _fill(RED_BG); c.border = _bd()
        c.alignment = _aln("left"); ws.row_dimensions[ri].height = 14; ri += 1

    ri = _anntot(ws, ri, NC, f"ANNUAL TOTAL  ({tot_r} records)",
            {1:f"ANNUAL TOTAL  ({tot_r} records)",6:round(ann["iv"],2),
             9:round(ann["tx"],2),10:round(ann["ig"],2),
             11:round(ann["cg"],2),12:round(ann["sg"],2)})
    ri += 1  # blank gap

    # ── SECTION 2: COMPANY-WISE MONTHLY DETAIL (MONTH → COMPANY VERTICAL) ──────
    ri = _sec_banner(ws, ri, NC, "\u25b6  SECTION 2 \u2014 COMPANY-WISE MONTHLY DETAIL", SEC2_BG)
    sec2_hw = [("GSTIN / Party", 20), ("Company Name", 30),
               ("Taxable Value \u20b9", 16), ("IGST \u20b9", 13),
               ("CGST \u20b9", 13), ("SGST \u20b9", 13), ("Total Tax \u20b9", 13)]
    _hdr(ws, ri, sec2_hw, bg=SEC2_BG); ri += 1

    # ── Quarter / Month maps ──────────────────────────────────────────────────
    S2_QMAP = {"Apr-25":"Q1","May-25":"Q1","Jun-25":"Q1",
               "Jul-25":"Q2","Aug-25":"Q2","Sep-25":"Q2",
               "Oct-25":"Q3","Nov-25":"Q3","Dec-25":"Q3",
               "Jan-26":"Q4","Feb-26":"Q4","Mar-26":"Q4"}
    S2_QLBL = {"Q1":"Q1 TOTAL \u2014 APR to JUN","Q2":"Q2 TOTAL \u2014 JUL to SEP",
               "Q3":"Q3 TOTAL \u2014 OCT to DEC","Q4":"Q4 TOTAL \u2014 JAN to MAR"}
    S2_QBG  = {"Q1":"1F4E79","Q2":"375623","Q3":"7B3F00","Q4":"5B2C6F"}
    S2_MLBL = {"Apr-25":"April 2025","May-25":"May 2025","Jun-25":"June 2025",
               "Jul-25":"July 2025","Aug-25":"August 2025","Sep-25":"September 2025",
               "Oct-25":"October 2025","Nov-25":"November 2025","Dec-25":"December 2025",
               "Jan-26":"January 2026","Feb-26":"February 2026","Mar-26":"March 2026"}
    # 0-based tuple index of each month's Taxable column in Company_Month_Detail rows
    S2_MCOL = {"Apr-25":3,"May-25":8,"Jun-25":13,"Jul-25":18,"Aug-25":23,"Sep-25":28,
               "Oct-25":33,"Nov-25":38,"Dec-25":43,"Jan-26":48,"Feb-26":53,"Mar-26":58}
    S2_MSEQ = ["Apr-25","May-25","Jun-25","Jul-25","Aug-25","Sep-25",
               "Oct-25","Nov-25","Dec-25","Jan-26","Feb-26","Mar-26"]

    def _s2_qrow(row_i, qa, qbg, ql):
        """Emit a coloured quarter-total row for Section 2 (7 data columns)."""
        ttx = round(qa["ig"]+qa["cg"]+qa["sg"], 2)
        for ci, v in enumerate([ql, "", qa["tx"], qa["ig"], qa["cg"], qa["sg"], ttx], 1):
            n = ci in (3, 4, 5, 6, 7)
            c = ws.cell(row=row_i, column=ci, value=v)
            c.font = _font(True, "FFFFFF", 9); c.fill = _fill(qbg)
            c.alignment = _aln("right" if n else "left"); c.border = _bd()
            if n and isinstance(v, float): c.number_format = NUM_FMT
        ws.row_dimensions[row_i].height = 15
        return row_i + 1

    # ── Load Company_Month_Detail (horizontal: rows=companies, col-groups=months) ─
    s2_companies = []   # [(gstin, name, full_row_tuple), ...]
    if annual and Path(str(annual)).exists():
        try:
            _cmd_wb = load_workbook(str(annual), data_only=True)
            if "Company_Month_Detail" in _cmd_wb.sheetnames:
                _cmd_ws = _cmd_wb["Company_Month_Detail"]
                _cmd_rows = list(_cmd_ws.iter_rows(values_only=True))
                # rows[0]=title, rows[1]=month-hdrs, rows[2]=col-hdrs, rows[3+]=companies
                for _r in _cmd_rows[3:]:
                    if not _r or not _r[0]: continue
                    _g = str(_r[0] or "").strip()
                    _n = str(_r[1] or "").strip()
                    if _g.upper() in ("GRAND TOTAL", "GSTIN", "TOTAL", "ANNUAL TOTAL"):
                        continue
                    s2_companies.append((_g, _n, _r))
            else:
                print(f"  [WARN] 'Company_Month_Detail' sheet not found in annual file")
        except Exception as _e:
            print(f"  [WARN] Company_Month_Detail load error: {_e}")

    # ── Fallback: build company-month table from Section-1 bill data ──────────
    if not s2_companies:
        print("  [INFO] Section 2: falling back to bill data for company-month breakdown")
        _co_month = {}   # {(gstin,name): {month_label: {tx,ig,cg,sg}}}
        for ml in m_order:
            for row in m_data.get(ml, []):
                _g = str(row[1] or "").strip() if len(row) > 1 else ""
                _n = str(row[2] or "").strip() if len(row) > 2 else ""
                _key = (_g or "UNKNOWN", _n)
                _co_month.setdefault(_key, {})
                _co_month[_key].setdefault(ml, {"tx":0.,"ig":0.,"cg":0.,"sg":0.})
                _d = _co_month[_key][ml]
                _d["tx"] += _sf(row[8]  if len(row) > 8  else 0)
                _d["ig"] += _sf(row[9]  if len(row) > 9  else 0)
                _d["cg"] += _sf(row[10] if len(row) > 10 else 0)
                _d["sg"] += _sf(row[11] if len(row) > 11 else 0)
        # Convert to same tuple format used by Company_Month_Detail path
        # We'll handle this inline below via s2_co_month dict
        s2_co_month_fb = _co_month   # fallback dict
    else:
        s2_co_month_fb = None

    s2_ann  = {k:0. for k in ["tx","ig","cg","sg"]}
    s2_qacc = {q:{k:0. for k in ["tx","ig","cg","sg"]} for q in ["Q1","Q2","Q3","Q4"]}
    s2_prevq = None
    s2_has_data = False

    for mk in S2_MSEQ:
        ci0  = S2_MCOL[mk]   # 0-based index of Taxable col in row tuple
        curq = S2_QMAP[mk]

        # ── Gather company rows for this month ────────────────────────────────
        m_cos = []   # [(gstin, name, tx, ig, cg, sg, ttx), ...]

        if s2_companies:
            for (gstin, name, row) in s2_companies:
                tx  = _sf(row[ci0]   if len(row) > ci0   else 0)
                ig  = _sf(row[ci0+1] if len(row) > ci0+1 else 0)
                cg  = _sf(row[ci0+2] if len(row) > ci0+2 else 0)
                sg  = _sf(row[ci0+3] if len(row) > ci0+3 else 0)
                ttx = _sf(row[ci0+4] if len(row) > ci0+4 else 0)
                if tx == 0 and ig == 0 and cg == 0 and sg == 0 and ttx == 0:
                    continue
                m_cos.append((gstin, name, tx, ig, cg, sg, ttx))
        elif s2_co_month_fb:
            # Fallback: pull from bill data dict
            # mk here is like "Apr-25" but m_order labels may differ — match by first 3 chars
            _mk3 = mk[:3].upper()
            for (_g, _n), _mdict in s2_co_month_fb.items():
                for _ml, _d in _mdict.items():
                    if str(_ml).strip().upper()[:3] != _mk3:
                        continue
                    tx = _d["tx"]; ig = _d["ig"]; cg = _d["cg"]; sg = _d["sg"]
                    ttx = round(ig+cg+sg, 2)
                    if tx == 0 and ig == 0 and cg == 0 and sg == 0:
                        continue
                    m_cos.append((_g, _n, tx, ig, cg, sg, ttx))

        if not m_cos:
            continue

        s2_has_data = True

        # Quarter change: emit previous quarter total first
        if s2_prevq and curq != s2_prevq:
            ri = _s2_qrow(ri, s2_qacc[s2_prevq], S2_QBG[s2_prevq], S2_QLBL[s2_prevq])
        s2_prevq = curq

        # Month banner
        ri = _month_sep(ws, ri, NC, S2_MLBL[mk])

        # Company rows for this month
        m_tx = m_ig = m_cg = m_sg = 0.
        for (gstin, name, tx, ig, cg, sg, ttx) in m_cos:
            bg = ALT1 if ri % 2 == 0 else ALT2
            for ci, v in enumerate([gstin, name, tx, ig, cg, sg, ttx], 1):
                n = ci >= 3
                _w(ws, ri, ci, v, bg=bg,
                   numfmt=NUM_FMT if n else None,
                   align="right" if n else "left")
            ws.row_dimensions[ri].height = 14
            ri += 1
            # Accumulate
            m_tx += tx;  m_ig += ig;  m_cg += cg;  m_sg += sg
            s2_qacc[curq]["tx"] += tx; s2_qacc[curq]["ig"] += ig
            s2_qacc[curq]["cg"] += cg; s2_qacc[curq]["sg"] += sg
            s2_ann["tx"] += tx; s2_ann["ig"] += ig
            s2_ann["cg"] += cg; s2_ann["sg"] += sg

        # Month subtotal row
        m_ttx = round(m_ig+m_cg+m_sg, 2)
        ri = _subtot(ws, ri, NC, f"{S2_MLBL[mk]} TOTAL",
                     {1: f"{S2_MLBL[mk]} TOTAL",
                      3: round(m_tx,2), 4: round(m_ig,2),
                      5: round(m_cg,2), 6: round(m_sg,2), 7: m_ttx})

    # Last quarter total
    if s2_prevq:
        ri = _s2_qrow(ri, s2_qacc[s2_prevq], S2_QBG[s2_prevq], S2_QLBL[s2_prevq])

    if not s2_has_data:
        c = ws.cell(row=ri, column=1,
                    value="  No company-month data found (Company_Month_Detail sheet missing)")
        c.font = _font(False,"9C0006",9); c.fill = _fill(RED_BG); c.border = _bd()
        c.alignment = _aln("left"); ws.row_dimensions[ri].height = 14; ri += 1

    ri = _anntot(ws, ri, NC, "ANNUAL TOTAL",
            {1:"ANNUAL TOTAL",
             3: round(s2_ann["tx"],2), 4: round(s2_ann["ig"],2),
             5: round(s2_ann["cg"],2), 6: round(s2_ann["sg"],2),
             7: round(s2_ann["ig"]+s2_ann["cg"]+s2_ann["sg"],2)})
    ri += 1  # blank gap

    # ── SECTION 3: COMPANY-WISE SUMMARY ──────────────────────────────────────
    ri = _sec_banner(ws, ri, NC, "\u25b6  SECTION 3 \u2014 COMPANY-WISE (RECEIVER) SUMMARY", SEC3_BG)
    cw_hw = [("GSTIN Receiver",22),("Receiver Name",30),("No. of Invoices",16),
             ("Invoice Value \u20b9",18),("Taxable Value \u20b9",18),
             ("IGST \u20b9",13),("CGST \u20b9",13),("SGST \u20b9",13)]
    _hdr(ws, ri, cw_hw, bg=SEC3_BG); ri += 1

    # Try exact sheet names produced by gst_suite_v32 first, then legacy names
    _, cwraw = _load(annual, "Company_Wise_Summary", skip=3)
    cw_col_layout = "new"   # GSTIN|Name|InvCount|TaxableValue|IGST|CGST|SGST|TotalTax|InvoiceValue|...
    if not cwraw:
        _, cwraw = _load(annual, "GSTR1_Companywise", skip=2);    cw_col_layout = "old"
    if not cwraw:
        _, cwraw = _load(annual, "Companywise_Summary", skip=2);  cw_col_layout = "old"
    if not cwraw:
        _, cwraw = _load(annual, "Sales_Companywise", skip=2);    cw_col_layout = "old"

    cw_ann = {k:0. for k in ["cnt","iv","tx","ig","cg","sg"]}
    if cwraw:
        for row in cwraw:
            if not row or not row[0]: continue
            v0 = str(row[0]).strip()
            if v0.upper() in ("GSTIN","ANNUAL TOTAL","GRAND TOTAL","TOTAL"): continue
            bg = ALT1 if ri % 2 == 0 else ALT2
            if cw_col_layout == "new":
                # Company_Wise_Summary: GSTIN|Name|InvCount|TaxableValue|IGST|CGST|SGST|TotalTax|InvoiceValue|...
                cnt = _sf(row[2] if len(row)>2 else 0)
                tx  = _sf(row[3] if len(row)>3 else 0)   # Taxable Value
                ig  = _sf(row[4] if len(row)>4 else 0)
                cg  = _sf(row[5] if len(row)>5 else 0)
                sg  = _sf(row[6] if len(row)>6 else 0)
                iv  = _sf(row[8] if len(row)>8 else 0)   # Invoice Value
            else:
                # Legacy: GSTIN|Name|Count|InvoiceValue|Taxable|IGST|CGST|SGST
                cnt = _sf(row[2] if len(row)>2 else 0)
                iv  = _sf(row[3] if len(row)>3 else 0)
                tx  = _sf(row[4] if len(row)>4 else 0)
                ig  = _sf(row[5] if len(row)>5 else 0)
                cg  = _sf(row[6] if len(row)>6 else 0)
                sg  = _sf(row[7] if len(row)>7 else 0)
            for ci,v in enumerate([row[0], row[1] if len(row)>1 else "", cnt, iv, tx, ig, cg, sg], 1):
                n = ci in (3,4,5,6,7,8)
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if n else None,
                   align="right" if n else "left")
            ws.row_dimensions[ri].height = 14
            cw_ann["cnt"]+=cnt; cw_ann["iv"]+=iv; cw_ann["tx"]+=tx
            cw_ann["ig"]+=ig; cw_ann["cg"]+=cg; cw_ann["sg"]+=sg; ri+=1
    else:
        # Build company-wise from bill data
        cw_from_bills = {}
        for ml in m_order:
            for row in m_data.get(ml,[]):
                gstin = str(row[1] or "").strip()
                name  = str(row[2] or "").strip()
                key   = gstin or name or "UNKNOWN"
                if key not in cw_from_bills:
                    cw_from_bills[key] = {"gstin":gstin,"name":name,"cnt":0,"iv":0.,"tx":0.,"ig":0.,"cg":0.,"sg":0.}
                d = cw_from_bills[key]
                d["cnt"]+=1
                d["iv"] +=_sf(row[5] if len(row)>5 else 0)
                d["tx"] +=_sf(row[8] if len(row)>8 else 0)
                d["ig"] +=_sf(row[9] if len(row)>9 else 0)
                d["cg"] +=_sf(row[10] if len(row)>10 else 0)
                d["sg"] +=_sf(row[11] if len(row)>11 else 0)
        for key in sorted(cw_from_bills.keys()):
            d = cw_from_bills[key]
            bg = ALT1 if ri % 2 == 0 else ALT2
            for ci,v in enumerate([d["gstin"],d["name"],d["cnt"],d["iv"],d["tx"],
                                    d["ig"],d["cg"],d["sg"]],1):
                n = ci in (3,4,5,6,7,8)
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if n else None,
                   align="right" if n else "left")
            ws.row_dimensions[ri].height = 14
            cw_ann["cnt"]+=d["cnt"]; cw_ann["iv"]+=d["iv"]; cw_ann["tx"]+=d["tx"]
            cw_ann["ig"]+=d["ig"]; cw_ann["cg"]+=d["cg"]; cw_ann["sg"]+=d["sg"]; ri+=1

    _anntot(ws, ri, NC, "ANNUAL TOTAL",
            {1:"ANNUAL TOTAL",3:round(cw_ann["cnt"],0),4:round(cw_ann["iv"],2),
             5:round(cw_ann["tx"],2),6:round(cw_ann["ig"],2),
             7:round(cw_ann["cg"],2),8:round(cw_ann["sg"],2)})

    ws.sheet_properties.tabColor = SEC1_BG
    print(f"  \u2713 Sheet 2 \u2014 GSTR-1 3-section ({tot_r} bills, Taxable \u20b9{ann['tx']:,.0f})")


# ── Sheet 3: GSTR-2B — Bill-wise | Month-wise | Supplier-wise (3 stacked sections) ─
def _sh3(wb, cn, fy, annual):
    """
    Sheet 3: GSTR-2B  —  3 stacked sections sourced from the GSTR2B_Consolidated_Analysis
    workbook (searched in the same folder tree as the annual file):

      Section 1  ─  All Data          : every document row (B2B + CDN), month-grouped,
                                        sorted by Doc Date then Doc Number within each month,
                                        with per-month subtotals and an annual total.
                                        38 source columns mapped to 15 output columns.

      Section 2  ─  GSTR2B_Supplierwise: month-wise supplier rows (monthly section of the
                                        Supplierwise sheet, not the FY summary section),
                                        with per-month subtotals and quarter totals, plus an
                                        annual total. 8 columns.

      Section 3  ─  GSTIN Annual Summary: one row per supplier GSTIN, 15 columns including
                                        gross and net taxable/GST values. Annual total row.
    """
    safe = re.sub(r"[^A-Za-z0-9 ]", "", cn)[:15]
    ws   = wb.create_sheet(f"3_GSTR2B_{safe}"[:31])
    ws.sheet_view.showGridLines = False

    # ── Locate GSTR2B_Consolidated_Analysis workbook ───────────────────────────
    gstr2b_wb = None
    search_paths = []
    if annual:
        af = Path(annual)
        search_paths += [af.parent, af.parent.parent,
                         af.parent.parent / "GST Automation",
                         af.parent.parent / "GST IT Comparison"]
    for sp in search_paths:
        if not sp or not sp.exists():
            continue
        hits = sorted(sp.glob("GSTR2B_Consolidated_Analysis*.xlsx"),
                      key=lambda p: p.stat().st_mtime, reverse=True)
        if not hits:
            hits = sorted(sp.glob("GSTR2B_Consolidated*.xlsx"),
                          key=lambda p: p.stat().st_mtime, reverse=True)
        if hits:
            try:
                gstr2b_wb = load_workbook(str(hits[0]), data_only=True)
                print(f"    GSTR2B source: {hits[0].name}")
            except Exception as e:
                print(f"    [WARN] GSTR2B_Consolidated load error: {e}")
            break

    if gstr2b_wb is None:
        print(f"    [WARN] GSTR2B_Consolidated_Analysis*.xlsx not found — falling back to ANNUAL_RECONCILIATION sheets")

    def _load_gstr2b(sheet_name, skip=2):
        """Load from gstr2b_wb first, then annual as fallback."""
        if gstr2b_wb and sheet_name in gstr2b_wb.sheetnames:
            rows = list(gstr2b_wb[sheet_name].iter_rows(values_only=True))
            if len(rows) > skip:
                hdrs = [str(c or "") for c in rows[skip - 1]]
                data = [r for r in rows[skip:] if any(c is not None for c in r)]
                return hdrs, data
        return _load(annual, sheet_name, skip=skip)

    # ── FY month sequence (full names, for All Data grouping) ─────────────────
    MONTH_SEQ = ["April","May","June","July","August","September",
                 "October","November","December","January","February","March"]
    QMAP3 = {"April":"Q1","May":"Q1","June":"Q1",
              "July":"Q2","August":"Q2","September":"Q2",
              "October":"Q3","November":"Q3","December":"Q3",
              "January":"Q4","February":"Q4","March":"Q4"}
    QLBL3 = {"Q1":"Q1 TOTAL \u2014 APR to JUN","Q2":"Q2 TOTAL \u2014 JUL to SEP",
              "Q3":"Q3 TOTAL \u2014 OCT to DEC","Q4":"Q4 TOTAL \u2014 JAN to MAR"}
    QBG3  = {"Q1":"1F4E79","Q2":"375623","Q3":"7B3F00","Q4":"5B2C6F"}

    # ── Total column count (Section 1 has 15 output cols — the widest) ─────────
    NC = 15

    # ── Sheet title ───────────────────────────────────────────────────────────
    _title(ws, 1, 1, NC,
           f"GSTR-2B ITC Detail \u2014 {cn} \u2014 FY {fy}",
           bg=SEC2_BG, size=12, h=24)

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 1 — All Data (38 source cols → 15 output cols)
    # Source cols (0-based):
    #  0=Month  1=FY  2=Doc Type  3=Supplier GSTIN  4=Supplier Name
    #  5=Doc Number  6=Doc Sub-Type  7=Doc Date  8=Doc Value  9=POS
    # 10=RCM  11=Rate%  12=Taxable Value  13=IGST  14=CGST  15=SGST
    # 16=Cess  17=Total Tax  18-22=Debit  23-27=Credit  28-32=Net  33=Net Total Tax
    # 34=Filing Period  35=Filing Date  36=ITC Availability  37=Reason
    # ══════════════════════════════════════════════════════════════════════════
    S1_HW = [
        ("Month",           11),
        ("Doc Type",        20),
        ("Supplier GSTIN",  22),
        ("Supplier Name",   30),
        ("Doc Number",      18),
        ("Doc Sub-Type",    16),
        ("Doc Date",        12),
        ("Doc Value \u20b9", 16),
        ("POS",             14),
        ("Rate %",           8),
        ("Taxable Value \u20b9", 18),
        ("IGST \u20b9",     13),
        ("CGST \u20b9",     13),
        ("SGST \u20b9",     13),
        ("Total Tax \u20b9", 15),
    ]

    ri = 2
    ri = _sec_banner(ws, ri, NC,
                     "\u25b6  SECTION 1 \u2014 ALL DATA (B2B + CDN) \u2014 Month-wise Bill Detail",
                     SEC2_BG)
    _hdr(ws, ri, S1_HW, bg=HDR_BG); ri += 1
    ws.freeze_panes = f"A{ri}"

    _, all_raw = _load_gstr2b("All Data", skip=2)

    # Group by month in FY order
    m_data1 = {m: [] for m in MONTH_SEQ}
    for row in (all_raw or []):
        if not row or row[0] is None:
            continue
        mo = str(row[0] or "").strip()
        if mo in m_data1:
            m_data1[mo].append(row)

    # Sort each month: Doc Date asc, then Doc Number asc (numeric prefix preferred)
    def _doc_sort_key(r):
        dt  = str(r[7] or "") if len(r) > 7 else ""
        num = str(r[5] or "") if len(r) > 5 else ""
        n   = int(re.sub(r"\D", "", num)) if re.sub(r"\D", "", num) else 0
        return (dt, n, num)

    ann1   = {k: 0. for k in ["iv", "tx", "ig", "cg", "sg", "tt"]}
    tot_r1 = 0

    for mo in MONTH_SEQ:
        rows = m_data1.get(mo, [])
        if not rows:
            continue
        try:
            rows = sorted(rows, key=_doc_sort_key)
        except Exception:
            pass

        ri = _month_sep(ws, ri, NC, mo, len(rows))
        m_iv = m_tx = m_ig = m_cg = m_sg = m_tt = 0.

        for row in rows:
            bg  = ALT1 if ri % 2 == 0 else ALT2
            iv  = _sf(row[8]  if len(row) >  8 else 0)
            tx  = _sf(row[12] if len(row) > 12 else 0)
            ig  = _sf(row[13] if len(row) > 13 else 0)
            cg  = _sf(row[14] if len(row) > 14 else 0)
            sg  = _sf(row[15] if len(row) > 15 else 0)
            tt  = _sf(row[17] if len(row) > 17 else 0)
            rate = row[11] if len(row) > 11 else ""

            vals = [
                mo,
                row[2]  if len(row) >  2 else "",   # Doc Type
                row[3]  if len(row) >  3 else "",   # Supplier GSTIN
                row[4]  if len(row) >  4 else "",   # Supplier Name
                row[5]  if len(row) >  5 else "",   # Doc Number
                row[6]  if len(row) >  6 else "",   # Doc Sub-Type
                row[7]  if len(row) >  7 else "",   # Doc Date
                iv,                                  # Doc Value
                row[9]  if len(row) >  9 else "",   # POS
                rate,                                # Rate %
                tx,                                  # Taxable Value
                ig,                                  # IGST
                cg,                                  # CGST
                sg,                                  # SGST
                tt,                                  # Total Tax
            ]
            NUM_COLS_S1 = {8, 11, 12, 13, 14, 15}   # 1-based col indices that are numeric
            for ci, v in enumerate(vals, 1):
                n = ci in NUM_COLS_S1
                _w(ws, ri, ci, v, bg=bg,
                   numfmt=NUM_FMT if n else None,
                   align="right" if n else ("center" if ci == 10 else "left"))
            ws.row_dimensions[ri].height = 14

            m_iv += iv;  m_tx += tx;  m_ig += ig;  m_cg += cg;  m_sg += sg;  m_tt += tt
            ann1["iv"] += iv;  ann1["tx"] += tx
            ann1["ig"] += ig;  ann1["cg"] += cg;  ann1["sg"] += sg;  ann1["tt"] += tt
            tot_r1 += 1;  ri += 1

        ri = _subtot(ws, ri, NC, f"  Subtotal \u2014 {mo}",
                     {1:  f"  Subtotal \u2014 {mo}",
                      8:  round(m_iv, 2),
                      11: round(m_tx, 2),
                      12: round(m_ig, 2),
                      13: round(m_cg, 2),
                      14: round(m_sg, 2),
                      15: round(m_tt, 2)})

    if tot_r1 == 0:
        ws.merge_cells(f"A{ri}:{get_column_letter(NC)}{ri}")
        c = ws.cell(row=ri, column=1,
                    value="  No data found in 'All Data' sheet of GSTR2B_Consolidated_Analysis.xlsx")
        c.font = _font(False, "9C0006", 9); c.fill = _fill(RED_BG)
        c.border = _bd(); c.alignment = _aln("left")
        ws.row_dimensions[ri].height = 14;  ri += 1

    ri = _anntot(ws, ri, NC, f"ANNUAL TOTAL  ({tot_r1} documents)",
                 {1:  f"ANNUAL TOTAL  ({tot_r1} documents)",
                  8:  round(ann1["iv"], 2),
                  11: round(ann1["tx"], 2),
                  12: round(ann1["ig"], 2),
                  13: round(ann1["cg"], 2),
                  14: round(ann1["sg"], 2),
                  15: round(ann1["tt"], 2)})
    ri += 1   # blank gap

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 2 — GSTR2B_Supplierwise  (monthly section only, not FY summary)
    # Source structure:
    #   Row 0 : title
    #   Row 1 : headers  (Supplier GSTIN | Name | No. of Bills | Inv Value |
    #                      Taxable Value | ITC IGST | ITC CGST | ITC SGST)
    #   Then for each month:
    #     '── <Month>'  banner row
    #     supplier data rows
    #     'GRAND TOTAL (N companies)' row  ← skip
    #     '<Month> TOTAL'  subtotal row    ← read values, emit styled subtotal
    #   After all months:
    #     'ANNUAL TOTAL' row               ← read and emit annual total
    #     blank row
    #     '══ FY TOTAL …'  section banner  ← stop; don't read FY summary section
    # ══════════════════════════════════════════════════════════════════════════
    S2_HW = [
        ("Supplier GSTIN",   22),
        ("Supplier Name",    34),
        ("No. of Bills",     14),
        ("Invoice Value \u20b9", 18),
        ("Taxable Value \u20b9", 18),
        ("ITC IGST \u20b9",  14),
        ("ITC CGST \u20b9",  14),
        ("ITC SGST \u20b9",  14),
    ]
    S2_NC = len(S2_HW)   # 8

    ri = _sec_banner(ws, ri, NC,
                     "\u25b6  SECTION 2 \u2014 SUPPLIER-WISE MONTHLY ITC  (GSTR2B_Supplierwise)",
                     SEC1_BG)
    _hdr(ws, ri, S2_HW, bg=SEC1_BG); ri += 1

    _, sw_raw = _load_gstr2b("GSTR2B_Supplierwise", skip=2)

    def _s2_qrow(q, qacc):
        """Emit a quarter-total row (8 cols) for Section 2."""
        nonlocal ri
        qbg  = QBG3[q]
        ttc  = round(qacc["ig"] + qacc["cg"] + qacc["sg"], 2)
        for ci, v in enumerate([QLBL3[q], "", qacc["cnt"],
                                 qacc["iv"], qacc["tx"],
                                 qacc["ig"], qacc["cg"], qacc["sg"]], 1):
            n = ci in (3, 4, 5, 6, 7, 8)
            c = ws.cell(row=ri, column=ci, value=v)
            c.font      = _font(True, "FFFFFF", 9)
            c.fill      = _fill(qbg)
            c.alignment = _aln("right" if n else "left")
            c.border    = _bd()
            if n and isinstance(v, float): c.number_format = NUM_FMT
        ws.row_dimensions[ri].height = 15;  ri += 1

    ann2   = {k: 0. for k in ["cnt", "iv", "tx", "ig", "cg", "sg"]}
    q_acc2 = {q: {k: 0. for k in ["cnt", "iv", "tx", "ig", "cg", "sg"]}
              for q in ["Q1", "Q2", "Q3", "Q4"]}
    prev_q2     = None
    cur_month2  = None
    in_fy_sect  = False   # once we hit the FY summary banner, stop

    if sw_raw:
        for row in sw_raw:
            if not row or not row[0]:
                continue
            v0 = str(row[0]).strip()

            # Stop at the FY annual summary section
            if "FY TOTAL" in v0.upper() or ("\u2550" in v0 and "FY" in v0.upper()):
                in_fy_sect = True
            if in_fy_sect:
                continue

            # Month banner row  ('── April', '── May', …)
            if row[1] is None and row[2] is None:
                # Could be a GRAND TOTAL, ANNUAL TOTAL, or month banner
                v0u = v0.upper()
                if "ANNUAL TOTAL" in v0u or "GRAND TOTAL" in v0u:
                    # Skip row-level grand/annual; we'll recalculate
                    continue
                # Detect month name in banner
                matched_month = None
                for mo in MONTH_SEQ:
                    if mo.upper() in v0u:
                        matched_month = mo;  break
                if matched_month:
                    cur_month2 = matched_month
                    curq = QMAP3.get(matched_month)
                    if curq and prev_q2 and curq != prev_q2:
                        _s2_qrow(prev_q2, q_acc2[prev_q2])
                    prev_q2 = curq
                    ri = _month_sep(ws, ri, NC, matched_month)
                continue

            # Month TOTAL row  ('April TOTAL', …)
            if "TOTAL" in v0.upper() and row[2] is not None:
                # This is the month subtotal from source — we emit our own below
                # but we can cross-check; skip source total row to avoid double use
                continue

            # Supplier data row
            if cur_month2 is None:
                continue   # haven't seen a month banner yet

            gstin = str(row[0] or "").strip()
            name  = str(row[1] or "") if len(row) > 1 else ""
            cnt   = _sf(row[2] if len(row) > 2 else 0)
            iv    = _sf(row[3] if len(row) > 3 else 0)
            tx    = _sf(row[4] if len(row) > 4 else 0)
            ig    = _sf(row[5] if len(row) > 5 else 0)
            cg    = _sf(row[6] if len(row) > 6 else 0)
            sg    = _sf(row[7] if len(row) > 7 else 0)

            bg = ALT1 if ri % 2 == 0 else ALT2
            for ci, v in enumerate([gstin, name, cnt, iv, tx, ig, cg, sg], 1):
                n = ci in (3, 4, 5, 6, 7, 8)
                _w(ws, ri, ci, v, bg=bg,
                   numfmt=NUM_FMT if n else None,
                   align="right" if n else "left")
            ws.row_dimensions[ri].height = 14;  ri += 1

            # Accumulate
            curq = QMAP3.get(cur_month2)
            if curq:
                q_acc2[curq]["cnt"] += cnt;  q_acc2[curq]["iv"] += iv
                q_acc2[curq]["tx"]  += tx;   q_acc2[curq]["ig"] += ig
                q_acc2[curq]["cg"]  += cg;   q_acc2[curq]["sg"] += sg
            ann2["cnt"] += cnt;  ann2["iv"] += iv;  ann2["tx"] += tx
            ann2["ig"]  += ig;   ann2["cg"] += cg;  ann2["sg"] += sg

        # emit last quarter total
        if prev_q2:
            _s2_qrow(prev_q2, q_acc2[prev_q2])
    else:
        ws.merge_cells(f"A{ri}:{get_column_letter(NC)}{ri}")
        c = ws.cell(row=ri, column=1,
                    value="  'GSTR2B_Supplierwise' sheet not found")
        c.font = _font(False, "9C0006", 9);  c.fill = _fill(RED_BG)
        c.border = _bd();  c.alignment = _aln("left")
        ws.row_dimensions[ri].height = 14;  ri += 1

    ri = _anntot(ws, ri, NC, "ANNUAL TOTAL",
                 {1: "ANNUAL TOTAL",
                  3: round(ann2["cnt"], 0),
                  4: round(ann2["iv"],  2),
                  5: round(ann2["tx"],  2),
                  6: round(ann2["ig"],  2),
                  7: round(ann2["cg"],  2),
                  8: round(ann2["sg"],  2)})
    ri += 1   # blank gap

    # ══════════════════════════════════════════════════════════════════════════
    # SECTION 3 — GSTIN Annual Summary  (15 source cols, all rows except grand total)
    # Source cols (0-based):
    #  0=Supplier GSTIN  1=Supplier Name  2=Total Docs
    #  3=Taxable Value   4=IGST           5=CGST           6=SGST          7=Cess
    #  8=Total Tax       9=Net Taxable   10=Net IGST      11=Net CGST     12=Net SGST
    # 13=Net Cess       14=Net Total Tax
    # ══════════════════════════════════════════════════════════════════════════
    S3_HW = [
        ("Supplier GSTIN",          22),
        ("Supplier Name",           34),
        ("Total Docs",              11),
        ("Taxable Value \u20b9",    18),
        ("IGST \u20b9",             13),
        ("CGST \u20b9",             13),
        ("SGST \u20b9",             13),
        ("Cess \u20b9",             10),
        ("Total Tax \u20b9",        15),
        ("Net Taxable \u20b9",      18),
        ("Net IGST \u20b9",         13),
        ("Net CGST \u20b9",         13),
        ("Net SGST \u20b9",         13),
        ("Net Cess \u20b9",         10),
        ("Net Total Tax \u20b9",    16),
    ]
    S3_NC = len(S3_HW)   # 15

    ri = _sec_banner(ws, ri, NC,
                     "\u25b6  SECTION 3 \u2014 GSTIN ANNUAL SUMMARY",
                     SEC3_BG)
    _hdr(ws, ri, S3_HW, bg=SEC3_BG); ri += 1

    _, gs_raw = _load_gstr2b("GSTIN Annual Summary", skip=2)

    ann3 = {k: 0. for k in ["docs", "tx", "ig", "cg", "sg", "cs", "tt",
                              "ntx", "nig", "ncg", "nsg", "ncs", "ntt"]}
    tot_r3 = 0

    SKIP_S3 = {"SUPPLIER GSTIN", "GRAND TOTAL", "ANNUAL TOTAL", "TOTAL", ""}

    if gs_raw:
        for row in gs_raw:
            if not row or not row[0]:
                continue
            v0 = str(row[0] or "").strip()
            if v0.upper() in SKIP_S3 or "GRAND" in v0.upper():
                continue
            bg   = ALT1 if ri % 2 == 0 else ALT2
            docs = _sf(row[2]  if len(row) >  2 else 0)
            tx   = _sf(row[3]  if len(row) >  3 else 0)
            ig   = _sf(row[4]  if len(row) >  4 else 0)
            cg   = _sf(row[5]  if len(row) >  5 else 0)
            sg   = _sf(row[6]  if len(row) >  6 else 0)
            cs   = _sf(row[7]  if len(row) >  7 else 0)
            tt   = _sf(row[8]  if len(row) >  8 else 0)
            ntx  = _sf(row[9]  if len(row) >  9 else 0)
            nig  = _sf(row[10] if len(row) > 10 else 0)
            ncg  = _sf(row[11] if len(row) > 11 else 0)
            nsg  = _sf(row[12] if len(row) > 12 else 0)
            ncs  = _sf(row[13] if len(row) > 13 else 0)
            ntt  = _sf(row[14] if len(row) > 14 else 0)

            vals = [v0,
                    str(row[1] or "") if len(row) > 1 else "",
                    docs, tx, ig, cg, sg, cs, tt,
                    ntx, nig, ncg, nsg, ncs, ntt]
            NUM_COLS_S3 = set(range(3, 16))   # cols 3-15 are numeric (1-based)
            for ci, v in enumerate(vals, 1):
                n = ci in NUM_COLS_S3
                _w(ws, ri, ci, v, bg=bg,
                   numfmt=NUM_FMT if n else None,
                   align="right" if n else "left")
            ws.row_dimensions[ri].height = 14

            ann3["docs"] += docs;  ann3["tx"]  += tx;   ann3["ig"]  += ig
            ann3["cg"]   += cg;    ann3["sg"]  += sg;   ann3["cs"]  += cs
            ann3["tt"]   += tt;    ann3["ntx"] += ntx;  ann3["nig"] += nig
            ann3["ncg"]  += ncg;   ann3["nsg"] += nsg;  ann3["ncs"] += ncs
            ann3["ntt"]  += ntt
            tot_r3 += 1;  ri += 1
    else:
        ws.merge_cells(f"A{ri}:{get_column_letter(NC)}{ri}")
        c = ws.cell(row=ri, column=1,
                    value="  'GSTIN Annual Summary' sheet not found")
        c.font = _font(False, "9C0006", 9);  c.fill = _fill(RED_BG)
        c.border = _bd();  c.alignment = _aln("left")
        ws.row_dimensions[ri].height = 14;  ri += 1

    _anntot(ws, ri, NC, f"ANNUAL TOTAL  ({tot_r3} suppliers)",
            {1:  f"ANNUAL TOTAL  ({tot_r3} suppliers)",
             3:  round(ann3["docs"], 0),
             4:  round(ann3["tx"],   2),
             5:  round(ann3["ig"],   2),
             6:  round(ann3["cg"],   2),
             7:  round(ann3["sg"],   2),
             8:  round(ann3["cs"],   2),
             9:  round(ann3["tt"],   2),
             10: round(ann3["ntx"],  2),
             11: round(ann3["nig"],  2),
             12: round(ann3["ncg"],  2),
             13: round(ann3["nsg"],  2),
             14: round(ann3["ncs"],  2),
             15: round(ann3["ntt"],  2)})

    # ── Column widths: use S3 widths (widest) for all sections ────────────────
    for ci, (_, w) in enumerate(S3_HW, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    ws.sheet_properties.tabColor = SEC2_BG
    print(f"  \u2713 Sheet 3 \u2014 GSTR-2B  S1={tot_r1} docs  S2 monthly suppliers  S3={tot_r3} GSTINs  "
          f"ITC \u20b9{ann2['ig']+ann2['cg']+ann2['sg']:,.0f}")


# ── Sheet 4: R1 vs 3B — simple vertical stack of all sheets ──────────────────
def _sh4(wb, cn, fy, annual):
    """Stack Annual + Q1-Q4 sheets vertically.
    Each period (month / quarter-total) gets a banner + header + data rows.
    Rows where ALL value columns are zero are skipped (except Total/Difference rows).
    """
    safe = re.sub(r"[^A-Za-z0-9 ]", "", cn)[:15]
    ws = wb.create_sheet(f"4_R1vs3B_{safe}"[:31])
    ws.sheet_view.showGridLines = False

    # 8-column layout: Section | Line Item | Taxable | IGST | CGST | SGST | Cess | Total
    NC = 8
    COL_W = [("Section", 28), ("Line Item", 52), ("Taxable ₹", 16),
             ("IGST ₹", 14), ("CGST ₹", 14), ("SGST ₹", 14),
             ("Cess ₹", 11), ("Total ₹", 15)]

    _title(ws, 1, 1, NC,
           f"GSTR-1 vs GSTR-3B Reconciliation — {cn} — FY {fy}",
           bg=SEC2_BG, size=12, h=24)

    # ── Locate GSTR1R3B_RECONCILED file ───────────────────────────────────────
    recon_wb = None
    if annual:
        folder = Path(annual).parent
        # Prefer GSTR1R3B over GSTR3BR2A
        matches = sorted(
            [p for p in folder.rglob("*GSTR1R3B*RECONCILED*.xlsx")],
            key=lambda p: p.stat().st_mtime, reverse=True
        )
        if not matches:
            matches = sorted(folder.rglob("*RECONCILED*.xlsx"),
                             key=lambda p: p.stat().st_mtime, reverse=True)
        if matches:
            try:
                recon_wb = load_workbook(str(matches[0]), data_only=True)
                print(f"    R1vs3B source: {matches[0].name}")
            except Exception as e:
                print(f"    [WARN] Could not open reconciled file: {e}")

    if recon_wb is None:
        ri = 2
        ri = _sec_banner(ws, ri, NC, "▶  SOURCE FILE NOT FOUND — run Step 2+3 first", RED_BG)
        ws.sheet_properties.tabColor = RED_BG
        print(f"  ✗ Sheet 4 — GSTR1R3B_RECONCILED file not found")
        return

    # ── Section/row styling constants ─────────────────────────────────────────
    SECT_BG = {
        "GSTR-1 Supply Details":   "1F4E79",
        "GSTR-1A: Direct Supplies":"2E75B6",
        "GSTR-1A: 9A Amendments":  "2E75B6",
        "GSTR-1A: Credit Notes":   "2E75B6",
        "GSTR-1A Amendment":       "2E75B6",
        "GSTR-3B Supply Details":  "375623",
    }
    TOTAL_ITEMS = {"Total from GSTR-1 + GSTR-1A (B)",
                   "Total from GSTR-3B (A)",
                   "Difference (A - B)"}

    def _vals_from_row(row, start, count=6):
        """Extract `count` numeric values starting at 0-based index `start`."""
        return [_sf(row[start + k] if len(row) > start + k else None)
                for k in range(count)]

    def _all_zero(vals):
        return all(v == 0.0 for v in vals)

    def _write_header(row_i, bg):
        """Write the 8-column sub-header row."""
        for ci, (h, _) in enumerate(COL_W, 1):
            c = ws.cell(row=row_i, column=ci, value=h)
            c.font = _font(True, "FFFFFF", 9)
            c.fill = _fill(bg)
            c.alignment = _aln("center")
            c.border = _bd()
        ws.row_dimensions[row_i].height = 14

    def _write_data_row(row_i, sect, item, vals, is_tot, is_diff):
        bg_sect = SECT_BG.get(sect)
        if is_diff:
            all_ok = abs(vals[0]) < 1 and _all_zero(vals[1:])
            row_bg = GREEN_BG if all_ok else RED_BG
            row_fg = "276221" if all_ok else "9C0006"
            bold   = True
        elif is_tot:
            row_bg = ANN_BG; row_fg = "FFFFFF"; bold = True
        else:
            row_bg = ALT1 if row_i % 2 == 0 else ALT2
            row_fg = "000000"; bold = False

        for ci, v in enumerate([sect, item] + vals, 1):
            is_num = ci > 2
            if is_diff or is_tot:
                cell_bg = row_bg; cell_fg = row_fg; b = bold
            else:
                cell_bg = bg_sect if (ci == 1 and bg_sect) else row_bg
                cell_fg = "FFFFFF" if (ci == 1 and bg_sect) else row_fg
                b = bool(ci == 1 and bg_sect)
            c = ws.cell(row=row_i, column=ci, value=v)
            c.font = _font(b, cell_fg, 9)
            c.fill = _fill(cell_bg)
            c.alignment = _aln("right" if is_num else "left")
            c.border = _bd()
            if is_num and isinstance(v, float):
                c.number_format = NUM_FMT
        ws.row_dimensions[row_i].height = 14

    def _render_sheet(src_ws, periods):
        """
        Render one source sheet.
        `periods` is a list of (label, banner_bg, col_start_0based, col_count).
        For Annual: one period, 6 cols starting at index 3.
        For Quarterly: four periods (3 months + Quarter total), 6 cols each.
        """
        nonlocal ri
        src_rows = list(src_ws.iter_rows(values_only=True))

        # Auto-detect where actual data rows start: find first row where col B
        # (index 1) looks like a real Line Item (non-empty, not a pure header word).
        HEADER_WORDS = {"LINE ITEM","SECTION","PERIOD","DESCRIPTION","MONTH","QUARTER","ANNUAL"}
        data_start = 5   # safe default (original behaviour)
        for idx, r in enumerate(src_rows):
            if r and r[1] and str(r[1]).strip().upper() not in HEADER_WORDS and idx >= 2:
                data_start = idx
                break
        data_rows = src_rows[data_start:]

        for p_label, p_banner_bg, p_start, _p_count in periods:
            # Validate: check that p_start is within the row width of at least one data row
            max_col = max((len(r) for r in data_rows if r), default=0)
            if p_start >= max_col:
                print(f"    [WARN] '{p_label}': col start {p_start} >= row width {max_col} — check source layout")
            # Collect visible rows for this period
            visible = []
            for row in data_rows:
                if not row or (row[0] is None and row[1] is None):
                    continue
                sect = str(row[0] or "").strip()
                item = str(row[1] or "").strip()
                if not item:
                    continue
                vals = _vals_from_row(row, p_start, 6)
                is_tot  = item in TOTAL_ITEMS
                is_diff = "Difference" in item
                # Skip zero rows unless it's a total/difference row
                if not is_tot and not is_diff and _all_zero(vals):
                    continue
                visible.append((sect, item, vals, is_tot, is_diff))

            if not visible:
                continue   # nothing to show for this period — skip banner entirely

            # Banner
            ri = _sec_banner(ws, ri, NC, f"▶  {p_label}", p_banner_bg)
            # Sub-header
            _write_header(ri, p_banner_bg); ri += 1
            # Data
            for sect, item, vals, is_tot, is_diff in visible:
                _write_data_row(ri, sect, item, vals, is_tot, is_diff)
                ri += 1
            ri += 1   # blank gap after each period

    # ── Render order: Annual first, then Q1 → Q4 ──────────────────────────────
    ri = 2

    SHEET_ORDER = [
        ("Annual - APR-MAR",
         [("ANNUAL SUMMARY — APR-MAR", ANN_BG, 3, 6)]),
        ("Q1 - APR-JUN",
         [("Q1 APR-JUN  —  April",        "1F4E79", 3,  6),
          ("Q1 APR-JUN  —  May",           "1F4E79", 9,  6),
          ("Q1 APR-JUN  —  June",          "1F4E79", 15, 6),
          ("Q1 APR-JUN  —  Quarter Total", "243F60", 21, 6)]),
        ("Q2 - JUL-SEP",
         [("Q2 JUL-SEP  —  July",          "375623", 3,  6),
          ("Q2 JUL-SEP  —  August",        "375623", 9,  6),
          ("Q2 JUL-SEP  —  September",     "375623", 15, 6),
          ("Q2 JUL-SEP  —  Quarter Total", "1E4620", 21, 6)]),
        ("Q3 - OCT-DEC",
         [("Q3 OCT-DEC  —  October",       "7B3F00", 3,  6),
          ("Q3 OCT-DEC  —  November",      "7B3F00", 9,  6),
          ("Q3 OCT-DEC  —  December",      "7B3F00", 15, 6),
          ("Q3 OCT-DEC  —  Quarter Total", "4C2700", 21, 6)]),
        ("Q4 - JAN-MAR",
         [("Q4 JAN-MAR  —  January",       "5B2C6F", 3,  6),
          ("Q4 JAN-MAR  —  February",      "5B2C6F", 9,  6),
          ("Q4 JAN-MAR  —  March",         "5B2C6F", 15, 6),
          ("Q4 JAN-MAR  —  Quarter Total", "3B1A4A", 21, 6)]),
    ]

    # Debug: show what sheets are actually in the reconciled workbook
    print(f"    R1vs3B sheets found: {recon_wb.sheetnames}")

    def _fuzzy_match(target, available):
        """Match sheet name loosely: strip spaces/dashes/case, check if key tokens match."""
        def _norm(s): return re.sub(r"[\s\-_]+", "", s).upper()
        tn = _norm(target)
        for s in available:
            if _norm(s) == tn:
                return s
        # Token-based fallback: e.g. "Q1" and "APR" both present in sheet name
        tokens = [t for t in re.split(r"[\s\-_]+", target.upper()) if len(t) >= 2]
        for s in available:
            sn = _norm(s)
            if all(t in sn for t in tokens):
                return s
        return None

    for sheet_name, periods in SHEET_ORDER:
        matched = _fuzzy_match(sheet_name, recon_wb.sheetnames)
        if matched is None:
            print(f"    [WARN] Sheet '{sheet_name}' not found in reconciled file — quarter skipped")
            continue
        if matched != sheet_name:
            print(f"    [INFO] Matched '{sheet_name}' → '{matched}'")
        _render_sheet(recon_wb[matched], periods)

    # ── Column widths & freeze ─────────────────────────────────────────────────
    for ci, (_, w) in enumerate(COL_W, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.freeze_panes = "C3"
    ws.sheet_properties.tabColor = SEC3_BG
    print(f"  ✓ Sheet 4 — R1 vs 3B (vertical stack; Annual + Q1-Q4; zero rows hidden)")


# ── Sheet 5: GST vs IT — AIS/TIS correctly placed ────────────────────────────
def _sh5(wb, cn, fy, annual, it_rc, master_xl=None):
    safe=re.sub(r"[^A-Za-z0-9 ]","",cn)[:14]
    ws=wb.create_sheet(f"5_GSTvsIT_{safe}"[:31]); ws.sheet_view.showGridLines=False
    NC=5
    hw=[("Month",14),("GSTR-1 Taxable ₹",18),("AIS Sales ₹",16),
        ("Sales Diff ₹",14),("Sales Flag",11)]
    _title(ws,1,1,NC,
           f"GST ↔ IT Month-wise — {cn} — FY {fy}  "
           f"[GSTR-1+2B vs AIS/TIS via PAN]",
           bg=SEC1_BG,size=11,h=26)
    _hdr(ws,2,hw); ws.freeze_panes="A3"

    # GSTR data from Monthwise_Reconciliation
    _,mw=_load(annual,"Monthwise_Reconciliation",skip=3)
    gm={}
    for row in mw:
        v0=str(row[0] or "").strip()
        if not v0 or v0.upper() in ("ANNUAL TOTAL","MONTH","GRAND TOTAL"): continue
        gm[v0]={"r1tx":_sf(row[1] if len(row)>1 else 0),
                "r1tt":_sf(row[5] if len(row)>5 else 0),
                "b3tt":_sf(row[9] if len(row)>9 else 0),
                "b2tt":_sf(row[14] if len(row)>14 else 0)}

    # ── PRIMARY: read AIS/TIS from Master Excel company sheet ──
    # Company sheet layout (_build_company_sheet in master_bridge.py):
    # Row 1 = title, Row 2 = headers, Row 3+ = data
    # Col A=Month(APR-2025) B=R1_Taxable C=R3B_Tax D=ITC
    # Col E=AIS_Sales F=TIS_Sales G=Sales_Diff H=Sales_Flag
    # Col I=AIS_Purchases J=ITC_AIS_Diff K=Purch_Flag L=Action
    import re as _re
    am_map={}; tm_map={}
    if master_xl and Path(master_xl).exists():
        try:
            from openpyxl import load_workbook as _lw2
            _mwb = _lw2(str(master_xl), data_only=True)
            # Find client sheet (not DASHBOARD)
            for _sn in _mwb.sheetnames:
                if _sn.upper()=="DASHBOARD": continue
                _mws = _mwb[_sn]
                for _rr in _mws.iter_rows(min_row=3, values_only=True):
                    if not _rr or len(_rr)<9: continue
                    _mkey = str(_rr[0] or "").strip()   # col A = Month (APR-2025)
                    # Only rows where col A matches APR-YYYY pattern
                    if not _re.match(r"^[A-Z]{3}-[0-9]{4}$", _mkey): continue
                    _ais_s = _sf(_rr[4])   # col E = AIS Sales
                    _tis_s = _sf(_rr[5])   # col F = TIS Sales
                    _ais_p = _sf(_rr[8])   # col I = AIS Purchases
                    am_map[_mkey]={"ais_s":_ais_s,"ais_p":_ais_p}
                    tm_map[_mkey]={"tis_s":_tis_s}
            if am_map:
                print(f"  [Sheet5] Master Excel: {len(am_map)} months loaded from {Path(str(master_xl)).name}")
            else:
                print(f"  [Sheet5] Master Excel found but no APR-YYYY rows — falling back")
        except Exception as _e:
            print(f"  [WARN] master_xl read failed: {_e}")

    # ── FALLBACK: AIS_vs_GSTR_Monthly from IT_RECONCILIATION ──
    if not am_map:
        _,am=_load(it_rc,"AIS_vs_GSTR_Monthly",skip=2)
        for row in am:
            v0=str(row[0] or "").strip()
            if not v0 or v0.upper() in ("MONTH","ANNUAL TOTAL","GRAND TOTAL"): continue
            am_map[v0]={"ais_s":_sf(row[1] if len(row)>1 else 0),
                        "ais_p":_sf(row[4] if len(row)>4 else 0)}
    if not tm_map:
        _,tm=_load(it_rc,"TIS_vs_GSTR_Monthly",skip=2)
        for row in tm:
            v0=str(row[0] or "").strip()
            if not v0 or v0.upper() in ("MONTH","ANNUAL TOTAL"): continue
            tm_map[v0]={"tis_s":_sf(row[1] if len(row)>1 else 0)}

    # Build month-label→APR-2025 converter (gm keys are "April 2025", master uses "APR-2025")
    _M2A={"January":"JAN","February":"FEB","March":"MAR","April":"APR","May":"MAY","June":"JUN",
           "July":"JUL","August":"AUG","September":"SEP","October":"OCT","November":"NOV","December":"DEC"}
    def _to_mkey(label):
        """Convert 'April 2025' → 'APR-2025'"""
        parts=str(label).strip().split()
        if len(parts)==2 and parts[0] in _M2A:
            return f"{_M2A[parts[0]]}-{parts[1]}"
        return label  # already in APR-2025 format or unknown

    VAR=5000
    ri=3; ann={k:0. for k in ["r1tx","ais_s"]}
    for ml,g in gm.items():
        mkey_std=_to_mkey(ml)  # convert "April 2025" → "APR-2025"
        # Try direct match, then converted key, then fuzzy
        a=am_map.get(ml,{}) or am_map.get(mkey_std,{})
        if not a:
            for k in am_map:
                if k.upper()[:6]==ml.upper()[:6] or k.upper()[:6]==mkey_std.upper()[:6]:
                    a=am_map[k]; break
        t=tm_map.get(ml,{}) or tm_map.get(mkey_std,{})
        if not t:
            for k in tm_map:
                if k.upper()[:6]==ml.upper()[:6] or k.upper()[:6]==mkey_std.upper()[:6]:
                    t=tm_map[k]; break
        r1tx=g["r1tx"]; b3tt=g["b3tt"]; b2tt=g["b2tt"]
        ais_s=a.get("ais_s",0.); tis_s=t.get("tis_s",0.); ais_p=a.get("ais_p",0.)
        sd=round(r1tx-ais_s,2); pd=round(b2tt-ais_p,2)
        if abs(sd)<1:      sf,sbg="✓ OK",GREEN_BG
        elif abs(sd)<=VAR: sf,sbg="⚠ Minor",YELLOW_BG
        else:              sf,sbg="✗ CHECK",RED_BG
        if ais_p==0:       pf,pbg="⚠ Minor",YELLOW_BG
        elif abs(pd)<1:    pf,pbg="✓ OK",GREEN_BG
        elif abs(pd)<=VAR: pf,pbg="⚠ Minor",YELLOW_BG
        else:              pf,pbg="✗ CHECK",RED_BG
        bg=ALT1 if ri%2==0 else ALT2
        # Only 5 columns: Month, R1 Taxable, AIS Sales, Sales Diff, Sales Flag
        _w(ws,ri,1,ml,bg=bg,align="left")
        _w(ws,ri,2,r1tx,bg=bg,numfmt=NUM_FMT,align="right")
        _w(ws,ri,3,ais_s,bg=bg,numfmt=NUM_FMT,align="right")
        _w(ws,ri,4,sd,bg=sbg if abs(sd)>1 else bg,numfmt=NUM_FMT,align="right")
        _w(ws,ri,5,sf,bg=sbg,bold=True,align="center")
        ws.row_dimensions[ri].height=15
        ann["r1tx"]+=r1tx; ann["ais_s"]+=ais_s
        ri+=1
    asd=round(ann["r1tx"]-ann["ais_s"],2)
    msg=f"R1 ₹{ann['r1tx']:,.0f} vs AIS ₹{ann['ais_s']:,.0f} | Diff ₹{asd:,.0f}"
    _anntot(ws,ri,NC,"ANNUAL TOTAL",
            {1:"ANNUAL TOTAL",2:round(ann["r1tx"],2),3:round(ann["ais_s"],2),
             4:asd,5:"✗ CHECK" if abs(asd)>VAR else "⚠ Minor"})
    ws.sheet_properties.tabColor=MED_BLUE
    print(f"  ✓ Sheet 5 — GST vs IT  R1=₹{ann['r1tx']:,.0f} AIS=₹{ann['ais_s']:,.0f} Diff=₹{asd:,.0f}")


# ── Sheet 6: TIS_AIS_COMPARISON — TIS_vs_2B + AIS_vs_2B stacked ──────────────
def _sh6(wb, cn, fy, it_rc):
    safe = re.sub(r"[^A-Za-z0-9 ]", "", cn)[:16]
    ws = wb.create_sheet(f"6_TIS_AIS_{safe}"[:31])
    ws.sheet_view.showGridLines = False

    # Find TIS_AIS_COMPARISON file — search GST IT Comparison folder and client folder
    tis_wb = None
    search_paths = []
    if it_rc:
        cf = Path(it_rc).parent.parent  # go up from IT Download to client folder
        search_paths += [cf/"GST IT Comparison", cf/"GST Automation", cf]
    for sp in search_paths:
        m = sorted(sp.glob("TIS_AIS_COMPARISON*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True) if sp.exists() else []
        if m:
            try:
                tis_wb = load_workbook(str(m[0]), data_only=True)
                print(f"    TIS_AIS source: {m[0].name}")
            except Exception as e:
                print(f"    [WARN] Could not open TIS_AIS file: {e}")
            break

    NC = 12
    _title(ws, 1, 1, NC,
           f"TIS / AIS vs GSTR-2B Comparison \u2014 {cn} \u2014 FY {fy}",
           bg=SEC2_BG, size=12, h=24)
    ri = 2

    # ── SECTION 1: TIS vs 2B ────────────────────────────────────────────────
    ri = _sec_banner(ws, ri, NC, "\u25b6  SECTION 1 \u2014 TIS vs GSTR-2B (SUPPLIER-WISE ANNUAL)", SEC2_BG)

    TIS_HW = [("Supplier GSTIN",22),("Party Name",32),("2B Taxable Value \u20b9",18),
              ("2B IGST \u20b9",13),("2B CGST \u20b9",13),("2B SGST \u20b9",13),
              ("2B Total GST \u20b9",15),("TIS Accepted \u20b9",15),
              ("Difference \u20b9",14),("% Variance",10),("Status",14),("Action Needed",28)]
    _hdr(ws, ri, TIS_HW, bg=SEC2_BG); ri += 1
    ws.freeze_panes = f"A{ri}"

    STATUS_BG = {"✓ Match": GREEN_BG, "\u26a0 Minor Var": YELLOW_BG, "\u2717": RED_BG,
                 "Enter TIS": "DCE6F1", "N/A": ALT2}

    if tis_wb and "TIS_vs_2B" in tis_wb.sheetnames:
        src = tis_wb["TIS_vs_2B"]
        src_rows = list(src.iter_rows(values_only=True))
        # skip title rows (find actual header row)
        data_start = 0
        for i, row in enumerate(src_rows):
            if row and str(row[0] or "").strip() == "Supplier GSTIN":
                data_start = i + 1; break

        ann = {k: 0. for k in ["tx","ig","cg","sg","tt","tis","diff"]}
        tot_r = 0
        for row in src_rows[data_start:]:
            if not row or not row[0]: continue
            v0 = str(row[0] or "").strip()
            if v0.upper() in ("GRAND TOTAL","SUPPLIER GSTIN",""): continue
            bg = ALT1 if ri % 2 == 0 else ALT2
            status = str(row[10] or "").strip() if len(row) > 10 else ""
            # colour by status
            if "\u2713" in status:   sbg = GREEN_BG
            elif "\u26a0" in status: sbg = YELLOW_BG
            elif "\u2717" in status: sbg = RED_BG
            else:                    sbg = "DCE6F1"

            tx = _sf(row[2] if len(row)>2 else 0)
            ig = _sf(row[3] if len(row)>3 else 0)
            cg = _sf(row[4] if len(row)>4 else 0)
            sg = _sf(row[5] if len(row)>5 else 0)
            tt = _sf(row[6] if len(row)>6 else 0)
            tis= _sf(row[7] if len(row)>7 else 0)
            df = _sf(row[8] if len(row)>8 else 0)
            pct= row[9] if len(row)>9 else ""
            act= str(row[11] or "") if len(row)>11 else ""

            vals = [row[0], row[1] if len(row)>1 else "", tx, ig, cg, sg, tt, tis, df, pct, status, act]
            for ci, v in enumerate(vals, 1):
                n = ci in (3,4,5,6,7,8,9)
                cell_bg = sbg if ci in (11, 12) else bg
                c = ws.cell(row=ri, column=ci, value=v)
                c.font = _font(False, "000000", 9)
                c.fill = _fill(cell_bg)
                c.alignment = _aln("right" if n else ("center" if ci == 10 else "left"))
                c.border = _bd()
                if n and isinstance(v, float): c.number_format = NUM_FMT
            ws.row_dimensions[ri].height = 14
            ann["tx"]+=tx; ann["ig"]+=ig; ann["cg"]+=cg; ann["sg"]+=sg
            ann["tt"]+=tt; ann["tis"]+=tis; ann["diff"]+=df
            tot_r += 1; ri += 1

        ri = _anntot(ws, ri, NC, f"GRAND TOTAL  ({tot_r} suppliers)",
                {1:f"GRAND TOTAL  ({tot_r} suppliers)",
                 3:round(ann["tx"],2), 4:round(ann["ig"],2), 5:round(ann["cg"],2),
                 6:round(ann["sg"],2), 7:round(ann["tt"],2), 8:round(ann["tis"],2),
                 9:round(ann["diff"],2)})
    else:
        ws.merge_cells(f"A{ri}:{get_column_letter(NC)}{ri}")
        c = ws.cell(row=ri, column=1, value="  TIS_vs_2B sheet not found — place TIS_AIS_COMPARISON*.xlsx in GST IT Comparison folder")
        c.font = _font(False,"9C0006",9); c.fill = _fill(RED_BG); c.border = _bd()
        c.alignment = _aln("left"); ws.row_dimensions[ri].height = 14; ri += 1

    ri += 1  # blank gap

    # ── SECTION 2: AIS vs 2B ────────────────────────────────────────────────
    ri = _sec_banner(ws, ri, NC, "\u25b6  SECTION 2 \u2014 AIS vs GSTR-2B (SUPPLIER MONTH-WISE DETAIL)", SEC1_BG)

    AIS_HW = [("Supplier GSTIN",22),("Party Name",32),("Return Period",14),
              ("2B Purchase \u20b9",16),("2B Total GST \u20b9",15),("AIS Reported \u20b9",15),
              ("Difference \u20b9",14),("AIS Status",12),("Match Status",14),("Remarks",24)]
    _hdr(ws, ri, AIS_HW[:10], bg=SEC1_BG); ri += 1

    if tis_wb and "AIS_vs_2B" in tis_wb.sheetnames:
        src2 = tis_wb["AIS_vs_2B"]
        src2_rows = list(src2.iter_rows(values_only=True))
        data_start2 = 0
        for i, row in enumerate(src2_rows):
            if row and str(row[0] or "").strip() == "Supplier GSTIN":
                data_start2 = i + 1; break

        cur_gstin = None
        ann2 = {k: 0. for k in ["b2b","gst","ais","diff"]}
        tot_r2 = 0
        for row in src2_rows[data_start2:]:
            if not row: continue
            v0 = str(row[0] or "").strip()
            # supplier header rows look like "  NAME  |  GSTIN"
            if "|" in v0 and len(v0) > 10 and row[1] is None:
                # section separator — print as month-style divider
                parts = v0.split("|")
                label = parts[0].strip() if parts else v0.strip()
                ri = _month_sep(ws, ri, 10, label)
                cur_gstin = parts[-1].strip() if len(parts) > 1 else ""
                continue
            if not v0 or v0.upper() in ("SUPPLIER GSTIN","GRAND TOTAL"): continue

            bg = ALT1 if ri % 2 == 0 else ALT2
            b2b = _sf(row[3] if len(row)>3 else 0)
            gst = _sf(row[4] if len(row)>4 else 0)
            ais = _sf(row[5] if len(row)>5 else 0)
            df2 = _sf(row[6] if len(row)>6 else 0)
            match_st = str(row[8] or "") if len(row)>8 else ""
            if "\u2713" in match_st:   mbg = GREEN_BG
            elif "\u26a0" in match_st: mbg = YELLOW_BG
            elif "\u2717" in match_st: mbg = RED_BG
            elif "Only in" in match_st or "Not in" in match_st: mbg = YELLOW_BG
            else: mbg = bg

            vals = [row[0], row[1] if len(row)>1 else "",
                    row[2] if len(row)>2 else "",
                    b2b, gst, ais, df2,
                    row[7] if len(row)>7 else "",
                    match_st,
                    row[9] if len(row)>9 else ""]
            for ci, v in enumerate(vals, 1):
                n = ci in (4,5,6,7)
                cell_bg = mbg if ci in (9,10) else bg
                c = ws.cell(row=ri, column=ci, value=v)
                c.font = _font(False, "000000", 9)
                c.fill = _fill(cell_bg)
                c.alignment = _aln("right" if n else "left")
                c.border = _bd()
                if n and isinstance(v, float): c.number_format = NUM_FMT
            ws.row_dimensions[ri].height = 14
            ann2["b2b"]+=b2b; ann2["gst"]+=gst; ann2["ais"]+=ais; ann2["diff"]+=df2
            tot_r2 += 1; ri += 1

        _anntot(ws, ri, NC, f"GRAND TOTAL  ({tot_r2} line items)",
                {1:f"GRAND TOTAL  ({tot_r2} line items)",
                 4:round(ann2["b2b"],2), 5:round(ann2["gst"],2),
                 6:round(ann2["ais"],2), 7:round(ann2["diff"],2)})
    else:
        ws.merge_cells(f"A{ri}:{get_column_letter(NC)}{ri}")
        c = ws.cell(row=ri, column=1, value="  AIS_vs_2B sheet not found in TIS_AIS_COMPARISON file")
        c.font = _font(False,"9C0006",9); c.fill = _fill(RED_BG); c.border = _bd()
        c.alignment = _aln("left"); ws.row_dimensions[ri].height = 14

    # Column widths from Section 1 (wider of the two)
    for ci, (_, w) in enumerate(TIS_HW, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

    ws.sheet_properties.tabColor = SEC2_BG
    print(f"  \u2713 Sheet 6 \u2014 TIS/AIS vs 2B")


# ── Sheet 7: 26AS vs GSTR-1 Company-Wise Summary ─────────────────────────────
def _sh7(wb, cn, fy, it_rc, annual, compare_26as=None):
    """Embed the full Company_Wise_Summary sheet from 26AS_GSTR1_Compare file."""
    safe = re.sub(r"[^A-Za-z0-9 ]", "", cn)[:18]
    ws = wb.create_sheet(f"7_26AS_{safe}"[:31])
    ws.sheet_view.showGridLines = False

    # ── Load Company_Wise_Summary from 26AS_GSTR1_Compare file ──────────────
    src_wb = None
    if compare_26as and Path(compare_26as).exists():
        try:
            src_wb = load_workbook(str(compare_26as), data_only=True)
        except Exception as e:
            print(f"    [WARN] Could not open 26AS compare file: {e}")

    SHEET_NAMES = ["Company_Wise_Summary", "Company Wise Summary",
                   "CompanyWise", "Summary", "Sheet1"]

    src_ws = None
    if src_wb:
        for sn in SHEET_NAMES:
            if sn in src_wb.sheetnames:
                src_ws = src_wb[sn]; break
        if src_ws is None and src_wb.sheetnames:
            src_ws = src_wb[src_wb.sheetnames[0]]   # fallback: first sheet

    if src_ws:
        # ── Copy every row/cell preserving values, widths, row heights ──────
        # Copy column widths
        for col_letter, col_dim in src_ws.column_dimensions.items():
            ws.column_dimensions[col_letter].width = col_dim.width or 14
        # Copy merged cells
        for merge in src_ws.merged_cells.ranges:
            ws.merge_cells(str(merge))
        # Copy rows
        for row in src_ws.iter_rows():
            for cell in row:
                nc = ws.cell(row=cell.row, column=cell.column, value=cell.value)
                if cell.has_style:
                    try:
                        nc.font      = copy(cell.font)
                        nc.fill      = copy(cell.fill)
                        nc.border    = copy(cell.border)
                        nc.alignment = copy(cell.alignment)
                        if cell.number_format:
                            nc.number_format = cell.number_format
                    except Exception:
                        pass
            if cell.row in src_ws.row_dimensions:
                ws.row_dimensions[cell.row].height = src_ws.row_dimensions[cell.row].height
        ws.freeze_panes = src_ws.freeze_panes
        ws.sheet_properties.tabColor = "1F5C99"
        print(f"  ✓ Sheet 7 — 26AS vs GSTR-1 Company-Wise Summary (from {compare_26as.name if compare_26as else 'N/A'})")
    else:
        # Fallback: show a message if source not found
        _title(ws, 1, 1, 10,
               f"26AS vs GSTR-1 Company-Wise Summary — {cn} — FY {fy}",
               bg=SEC1_BG, size=12, h=24)
        ws.merge_cells("A3:J3")
        c = ws.cell(row=3, column=1,
                    value="  Source file '26AS_GSTR1_Compare*.xlsx' not found — "
                          "run Step 6d first.")
        c.font = _font(False, "9C0006", 10)
        c.fill = _fill(RED_BG)
        c.alignment = _aln("left")
        ws.row_dimensions[3].height = 18
        ws.sheet_properties.tabColor = SEC3_BG
        print(f"  ✗ Sheet 7 — 26AS compare file not found")


# ── Main ──────────────────────────────────────────────────────────────────────
def build(client_folder_or_base=None, output_file=None):
    ts=datetime.now().strftime("%Y%m%d_%H%M")
    base=Path(client_folder_or_base) if client_folder_or_base else None
    if base is None:
        home=Path.home()
        for c in [home/"Downloads",home/"Desktop"/"OUTPUT"]:
            if _find_client_folders(c): base=c; break
        if base is None: base=Path.home()/"Downloads"

    raw=_find_client_folders(base)
    if not raw:
        if (base/"GST Automation").exists() or (base/"IT Download").exists():
            raw=[base]
        else:
            print(f"  ✗ No client folders under {base}"); sys.exit(1)

    clients=[(cf.name.replace("_"," "),cf) for cf in raw]
    fy=_fy(clients)
    print(f"\nBase : {base}\nFY   : {fy}\nClients: {len(clients)}")
    out=Path(output_file) if output_file else base/f"FINAL_CONSOLIDATED_REPORT_{ts}.xlsx"

    wb=openpyxl.Workbook(); wb.remove(wb.active)
    _sh1(wb,clients,fy)
    for cn,cf in clients:
        AN,IT,C26,GB,TA,MX,RC=_sources(cf)
        if not AN and not IT:
            print(f"\n  [SKIP] {cn}: no source files"); continue
        # Also search base folder for master Excel if not in IT Bridge subfolder
        if not MX:
            MX=_rglob(base,"MASTER_GST_IT_RECONCILIATION*.xlsx")
        if not MX:
            MX=_rglob(cf/"IT Bridge","MASTER_GST_IT_RECONCILIATION*.xlsx")
        print(f"\n{'─'*60}\nClient: {cn}")
        print(f"  Annual: {AN.name if AN else 'NOT FOUND'}")
        print(f"  IT RC : {IT.name if IT else 'NOT FOUND'}")
        print(f"  Master: {MX.name if MX else 'NOT FOUND - AIS/TIS will be 0'}")
        _sh2(wb,cn,fy,AN)
        _sh3(wb,cn,fy,AN)
        _sh4(wb,cn,fy,RC)
        _sh5(wb,cn,fy,AN,IT,MX)
        _sh6(wb,cn,fy,IT)
        _sh7(wb,cn,fy,IT,AN,compare_26as=C26)

    wb.save(str(out))
    print(f"\n{'='*60}")
    print(f"  ✓ Saved : {out}")
    print(f"  Sheets  : {list(wb.sheetnames)}")
    return out


# Legacy stubs
def _find_client_folder(hint=None):
    if hint and Path(hint).exists(): return Path(hint)
    for base in [Path.home()/"Downloads",Path.home()/"Desktop"/"OUTPUT"]:
        f=_find_client_folders(base)
        if f: return f[0]
    return Path(".")


if __name__=="__main__":
    p=argparse.ArgumentParser(description="Final Consolidated Report v4")
    p.add_argument("--base",default=None)
    p.add_argument("--out",default=None)
    p.add_argument("folder",nargs="?")
    a=p.parse_args()
    build(a.base or a.folder, a.out)
