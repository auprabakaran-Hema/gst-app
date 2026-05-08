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
            _rglob(ib,"MASTER_GST_IT_RECONCILIATION*.xlsx"))

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


# ── Sheet 1: Run Summary ──────────────────────────────────────────────────────
def _sh1(wb, clients, fy):
    ws = wb.create_sheet("1_Run_Summary"); ts = datetime.now().strftime("%d-%b-%Y %H:%M")
    _title(ws,1,1,9,f"FINAL CONSOLIDATED REPORT — FY {fy}  |  Generated: {ts}",h=28,size=13)
    _hdr(ws,2,[("Client",30),("Annual",14),("IT Recon",12),("26AS",12),
               ("Bridge",12),("GST-IT",12),("GSTR1-FY",14),("Status",12),("Notes",28)])
    ri=3
    for i,(cn,cf) in enumerate(clients):
        bg=ALT1 if i%2==0 else ALT2
        AN,IT,C2,GB,TA,BR=_sources(cf)
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


# ── Sheet 2: GSTR-1 Invoice Detail (vertical: bills→month subtot→annual tot) ─
def _sh2(wb, cn, fy, annual):
    safe=re.sub(r"[^A-Za-z0-9 ]","",cn)[:16]
    ws=wb.create_sheet(f"2_GSTR1_{safe}"[:31]); ws.sheet_view.showGridLines=False
    NC=12
    hw=[("Type",8),("GSTIN Receiver",22),("Receiver Name",28),("Invoice No",14),
        ("Invoice Date",13),("Invoice Value ₹",16),("Place of Supply",15),("Rate %",7),
        ("Taxable Value ₹",16),("IGST ₹",12),("CGST ₹",12),("SGST ₹",12)]
    _title(ws,1,1,NC,f"GSTR-1 Invoice Detail — {cn} — FY {fy}",bg=SEC1_BG,size=12,h=24)
    _hdr(ws,2,hw); ws.freeze_panes="A3"

    _,raw=_load(annual,"GSTR1_Invoice_Detail",skip=2)
    if not raw:
        print(f"  [WARN] GSTR1_Invoice_Detail missing"); return

    # Group rows into months
    m_order=[]; m_data={}; cur=None
    for row in raw:
        v0=str(row[0] or "").strip()
        is_sep=(v0.startswith("--") or (
            row[1] is None and row[2] is None and
            any(m in v0.upper() for m in FY_MONTHS)))
        is_tot=v0.upper() in ("ANNUAL TOTAL","GRAND TOTAL")
        if is_sep:
            lbl=re.sub(r"[-–—]"," ",v0)
            lbl=re.sub(r"\d+\s*records?","",lbl,flags=re.I).strip()
            lbl=re.sub(r"\s+"," ",lbl).strip()
            cur=lbl
            if cur not in m_data: m_order.append(cur); m_data[cur]=[]
        elif is_tot:
            continue
        elif cur is not None:
            m_data[cur].append(row)

    ri=3; ann={k:0. for k in ["iv","tx","ig","cg","sg"]}; tot_r=0
    for ml in m_order:
        rows=m_data.get(ml,[])
        if not rows: continue
        try: rows=sorted(rows,key=_inv_key)
        except: pass
        ri=_month_sep(ws,ri,NC,ml,len(rows))
        mv=mt=mi=mc=ms=0.
        for row in rows:
            bg=ALT1 if ri%2==0 else ALT2
            iv=_sf(row[5] if len(row)>5 else 0)
            tx=_sf(row[8] if len(row)>8 else 0)
            ig=_sf(row[9] if len(row)>9 else 0)
            cg=_sf(row[10] if len(row)>10 else 0)
            sg=_sf(row[11] if len(row)>11 else 0)
            vals=[row[0],row[1],row[2],row[3],row[4],iv,row[6],row[7],tx,ig,cg,sg]
            for ci,v in enumerate(vals,1):
                n=ci in (6,9,10,11,12)
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if n else None,
                   align="right" if n else ("center" if ci==7 else "left"))
            ws.row_dimensions[ri].height=14
            mv+=iv;mt+=tx;mi+=ig;mc+=cg;ms+=sg
            ann["iv"]+=iv;ann["tx"]+=tx;ann["ig"]+=ig;ann["cg"]+=cg;ann["sg"]+=sg
            tot_r+=1; ri+=1
        ri=_subtot(ws,ri,NC,f"  Subtotal — {ml}",
                   {1:f"  Subtotal — {ml}",6:round(mv,2),9:round(mt,2),
                    10:round(mi,2),11:round(mc,2),12:round(ms,2)})
    _anntot(ws,ri,NC,f"ANNUAL TOTAL  ({tot_r} records)",
            {1:f"ANNUAL TOTAL  ({tot_r} records)",6:round(ann["iv"],2),
             9:round(ann["tx"],2),10:round(ann["ig"],2),
             11:round(ann["cg"],2),12:round(ann["sg"],2)})
    ws.sheet_properties.tabColor=SEC1_BG
    print(f"  ✓ Sheet 2 — GSTR-1 ({tot_r} rows, Taxable ₹{ann['tx']:,.0f})")


# ── Sheet 3: GSTR-2B ITC Detail (vertical: bills→month subtot→annual tot) ────
def _sh3(wb, cn, fy, annual):
    safe=re.sub(r"[^A-Za-z0-9 ]","",cn)[:15]
    ws=wb.create_sheet(f"3_GSTR2B_{safe}"[:31]); ws.sheet_view.showGridLines=False
    NC=11
    hw=[("Month",10),("Supplier GSTIN",22),("Supplier Name",28),("Invoice No",14),
        ("Invoice Date",13),("Invoice Value ₹",16),("POS",10),("Rate %",7),
        ("Taxable Value ₹",16),("ITC IGST ₹",13),("ITC CGST+SGST ₹",16)]
    _title(ws,1,1,NC,f"GSTR-2B ITC Detail (All Months) — {cn} — FY {fy}",bg=SEC2_BG,size=12,h=24)
    _hdr(ws,2,hw); ws.freeze_panes="A3"

    _,raw=_load(annual,"GSTR2B_ITC_Detail",skip=2)
    if not raw: _,raw=_load(annual,"Purchase_2B_Detail",skip=2)
    if not raw: print(f"  [WARN] GSTR2B_ITC_Detail missing"); return

    m_order=[]; m_data={}; cur=None
    for row in raw:
        v0=str(row[0] or "").strip()
        is_sep=(v0.startswith("--") or (
            row[1] is None and row[2] is None and
            any(m in v0.upper() for m in FY_MONTHS)))
        is_tot=v0.upper() in ("ANNUAL TOTAL","GRAND TOTAL")
        if is_sep:
            lbl=re.sub(r"[-–—]"," ",v0)
            lbl=re.sub(r"\d+\s*records?","",lbl,flags=re.I).strip()
            lbl=re.sub(r"\s+"," ",lbl).strip()
            cur=lbl
            if cur not in m_data: m_order.append(cur); m_data[cur]=[]
        elif is_tot: continue
        elif cur is not None: m_data[cur].append(row)

    ri=3; ann={k:0. for k in ["iv","tx","ig","cs"]}; tot_r=0
    for ml in m_order:
        rows=m_data.get(ml,[])
        if not rows: continue
        try: rows=sorted(rows,key=_inv_key)
        except: pass
        ri=_month_sep(ws,ri,NC,ml,len(rows))
        mv=mt=mi=mc=0.
        for row in rows:
            bg=ALT1 if ri%2==0 else ALT2
            iv=_sf(row[5] if len(row)>5 else 0)
            tx=_sf(row[8] if len(row)>8 else 0)
            ig=_sf(row[9] if len(row)>9 else 0)
            cs=_sf(row[10] if len(row)>10 else 0)
            vals=[ml,row[1] if len(row)>1 else "",row[2] if len(row)>2 else "",
                  row[3] if len(row)>3 else "",row[4] if len(row)>4 else "",
                  iv,row[6] if len(row)>6 else "",row[7] if len(row)>7 else "",tx,ig,cs]
            for ci,v in enumerate(vals,1):
                n=ci in (6,9,10,11)
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if n else None,
                   align="right" if n else "left")
            ws.row_dimensions[ri].height=14
            mv+=iv;mt+=tx;mi+=ig;mc+=cs
            ann["iv"]+=iv;ann["tx"]+=tx;ann["ig"]+=ig;ann["cs"]+=cs
            tot_r+=1; ri+=1
        ri=_subtot(ws,ri,NC,f"  Subtotal — {ml}",
                   {1:f"  Subtotal — {ml}",6:round(mv,2),9:round(mt,2),
                    10:round(mi,2),11:round(mc,2)})
    _anntot(ws,ri,NC,f"ANNUAL TOTAL  ({tot_r} ITC records)",
            {1:f"ANNUAL TOTAL  ({tot_r} ITC records)",6:round(ann["iv"],2),
             9:round(ann["tx"],2),10:round(ann["ig"],2),11:round(ann["cs"],2)})
    ws.sheet_properties.tabColor=SEC2_BG
    print(f"  ✓ Sheet 3 — GSTR-2B ({tot_r} rows, ITC ₹{ann['ig']+ann['cs']:,.0f})")


# ── Sheet 4: R1 vs 3B Month-wise ─────────────────────────────────────────────
def _sh4(wb, cn, fy, annual):
    safe=re.sub(r"[^A-Za-z0-9 ]","",cn)[:15]
    ws=wb.create_sheet(f"4_R1vs3B_{safe}"[:31]); ws.sheet_view.showGridLines=False
    NC=11
    hw=[("Month",13),("R1 Taxable ₹",16),("R1 Tax Total ₹",16),
        ("3B Out Tax ₹",16),("R1 vs 3B ₹",14),
        ("2B ITC Total ₹",16),("2A ITC Total ₹",16),("2A vs 2B ₹",14),
        ("Net Tax Payable ₹",18),("CDN Net TV ₹",15),("Status",14)]
    _title(ws,1,1,NC,f"GSTR-1 vs GSTR-3B Month-wise Reconciliation — {cn} — FY {fy}",
           bg=SEC2_BG,size=12,h=24)
    _title(ws,2,1,NC,
           "R1=GSTR-1  3B=GSTR-3B  2B=Confirmed ITC  2A=Auto-ITC  "
           "Net=R1 Tax−2B ITC  Green=OK  Yellow=Minor  Red=Review",
           bg="FFF2CC",fg="7D5A00",size=8,h=14)
    _hdr(ws,3,hw); ws.freeze_panes="A4"

    _,raw=_load(annual,"Monthwise_Reconciliation",skip=3)
    if not raw: _,raw=_load(annual,"R1_vs_3B_Recon",skip=2)

    ri=4; ann={k:0. for k in ["tx","r1t","b3t","b2t","a2t","net","cdn"]}
    for row in raw:
        if not row[0]: continue
        v0=str(row[0]).strip()
        if v0.upper() in ("ANNUAL TOTAL","GRAND TOTAL","MONTH"): continue
        n=len(row)
        r1tx=_sf(row[1] if n>1 else 0); r1tt=_sf(row[5] if n>5 else 0)
        b3tt=_sf(row[9] if n>9 else 0); r1v3b=_sf(row[10] if n>10 else 0)
        b2tt=_sf(row[14] if n>14 else 0); a2t=_sf(row[15] if n>15 else 0)
        a2v2b=_sf(row[16] if n>16 else 0)
        net=_sf(row[20] if n>20 else 0)
        cdn=_sf(row[17] if n>17 else 0)-_sf(row[18] if n>18 else 0)
        status=str(row[21]).strip() if n>21 and row[21] else ""
        if abs(r1v3b)<=100 and abs(a2v2b)<=500: sbg,sfg=GREEN_BG,"276221"
        elif abs(r1v3b)>1000 or abs(a2v2b)>5000: sbg,sfg=RED_BG,"9C0006"
        else: sbg,sfg=YELLOW_BG,"7D5A00"
        bg=ALT1 if ri%2==0 else ALT2
        vals=[v0,r1tx,r1tt,b3tt,r1v3b,b2tt,a2t,a2v2b,net,cdn,status]
        for ci,v in enumerate(vals,1):
            n2=isinstance(v,float)
            if ci==5: _w(ws,ri,ci,v,bg=sbg,bold=True,fg=sfg,numfmt=NUM_FMT,align="right")
            elif ci==11: _w(ws,ri,ci,v,bg=sbg,bold=True,fg=sfg,align="center")
            else: _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if n2 else None,
                     align="right" if n2 else "left")
        ws.row_dimensions[ri].height=15
        ann["tx"]+=r1tx;ann["r1t"]+=r1tt;ann["b3t"]+=b3tt
        ann["b2t"]+=b2tt;ann["a2t"]+=a2t;ann["net"]+=net;ann["cdn"]+=cdn
        ri+=1
    _anntot(ws,ri,NC,"ANNUAL TOTAL",
            {1:"ANNUAL TOTAL",2:round(ann["tx"],2),3:round(ann["r1t"],2),
             4:round(ann["b3t"],2),5:round(ann["r1t"]-ann["b3t"],2),
             6:round(ann["b2t"],2),7:round(ann["a2t"],2),
             8:round(ann["a2t"]-ann["b2t"],2),9:round(ann["net"],2),
             10:round(ann["cdn"],2)})
    ws.sheet_properties.tabColor=SEC3_BG
    print(f"  ✓ Sheet 4 — R1 vs 3B ({ri-4} months)")


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


# ── Sheet 6: Bridge ───────────────────────────────────────────────────────────
def _sh6(wb, cn, fy, it_rc):
    safe=re.sub(r"[^A-Za-z0-9 ]","",cn)[:16]
    ws=wb.create_sheet(f"6_Bridge_{safe}"[:31]); ws.sheet_view.showGridLines=False
    _title(ws,1,1,10,f"GST ↔ IT Bridge — {cn} — FY {fy}",bg=SEC1_BG,size=12,h=24)
    ri=2

    # AIS vs GSTR Monthly
    ri=_sec_banner(ws,ri+1,10,"▶  AIS vs GSTR-1 MONTHLY TURNOVER COMPARISON",SEC1_BG)
    _,am=_load(it_rc,"AIS_vs_GSTR_Monthly",skip=2)
    if am:
        _hdr(ws,ri,[("Month",12),("AIS Sales ₹",16),("GSTR-1 Sales ₹",16),
                    ("Sales Diff ₹",15),("AIS Purchases ₹",16),("GSTR-2B ITC ₹",16),
                    ("Purch Diff ₹",14)],bg=MED_BLUE); ri+=1
        for row in am:
            v0=str(row[0] or "").strip()
            if not v0 or v0.upper() in ("MONTH","ANNUAL TOTAL","GRAND TOTAL"): continue
            bg=ALT1 if ri%2==0 else ALT2
            for ci,v in enumerate(row[:7],1):
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if isinstance(v,(int,float)) else None,
                   align="right" if isinstance(v,(int,float)) else "left")
            ws.row_dimensions[ri].height=14; ri+=1
    ri+=2

    # Purchase Detail
    ri=_sec_banner(ws,ri,10,"▶  PURCHASE DETAIL — SUPPLIER-WISE FROM AIS",SEC2_BG)
    _,pr=_load(it_rc,"Purchase_Detail",skip=2)
    if pr:
        _hdr(ws,ri,[("Supplier GSTIN",22),("Supplier Name",30),("Return Period",14),
                    ("Purchase Amount ₹",18),("Status",12),("Source",16)],bg=SEC2_BG)
        ri+=1; ann_p=0.
        for row in pr:
            if not row or not row[0]: continue
            v0=str(row[0]).strip()
            if v0.upper() in ("SUPPLIER GSTIN","ANNUAL TOTAL"): continue
            bg=ALT1 if ri%2==0 else ALT2
            for ci,v in enumerate(row[:6],1):
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if isinstance(v,(int,float)) else None,
                   align="right" if isinstance(v,(int,float)) else "left")
            ws.row_dimensions[ri].height=14
            ann_p+=_sf(row[3] if len(row)>3 else 0); ri+=1
        _anntot(ws,ri,6,"TOTAL PURCHASES",{1:"TOTAL PURCHASES",4:round(ann_p,2)}); ri+=1
    ws.sheet_properties.tabColor=SEC2_BG
    print(f"  ✓ Sheet 6 — Bridge")


# ── Sheet 7: 26AS + GSTR-3B Status + IT Checklist ────────────────────────────
def _sh7(wb, cn, fy, it_rc, annual):
    safe=re.sub(r"[^A-Za-z0-9 ]","",cn)[:18]
    ws=wb.create_sheet(f"7_26AS_{safe}"[:31]); ws.sheet_view.showGridLines=False
    _title(ws,1,1,10,f"26AS TDS + GSTR-3B Status + IT Checklist — {cn} — FY {fy}",
           bg=SEC1_BG,size=12,h=24)
    ri=2

    # 26AS TDS Detail
    ri=_sec_banner(ws,ri+1,10,"▶  FORM 26AS — TDS / TCS DEDUCTOR-WISE DETAIL",SEC1_BG)
    _,td=_load(it_rc,"TDS_26AS_Detail",skip=2)
    if td:
        _hdr(ws,ri,[("Deductor Name",30),("TAN",16),("Section",12),
                    ("Amount Paid ₹",16),("TDS ₹",14),("Deposit Date",14),
                    ("Status",12)],bg=MED_BLUE); ri+=1
        ann_t=0.
        for row in td:
            if not row or not row[0]: continue
            bg=ALT1 if ri%2==0 else ALT2
            for ci,v in enumerate(row[:7],1):
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if isinstance(v,(int,float)) else None,
                   align="right" if isinstance(v,(int,float)) else "left")
            ws.row_dimensions[ri].height=14
            ann_t+=_sf(row[4] if len(row)>4 else 0); ri+=1
        _anntot(ws,ri,7,"TOTAL TDS",{1:"TOTAL TDS",5:round(ann_t,2)}); ri+=2

    # GSTR-3B Status
    ri=_sec_banner(ws,ri,10,"▶  GSTR-3B FILING STATUS — MONTH-WISE",SEC2_BG)
    _,gs=_load(annual,"GSTR3B_Status",skip=2)
    if gs:
        _hdr(ws,ri,[("Month",12),("Filing Status",16),("Date of Filing",16),
                    ("Total Tax ₹",15),("Late Fee ₹",12),
                    ("Interest ₹",12),("Remarks",20)],bg=SEC2_BG); ri+=1
        for row in gs:
            if not row or not row[0]: continue
            bg=ALT1 if ri%2==0 else ALT2
            for ci,v in enumerate(row[:7],1):
                _w(ws,ri,ci,v,bg=bg,numfmt=NUM_FMT if isinstance(v,(int,float)) else None,
                   align="right" if isinstance(v,(int,float)) else "left")
            ws.row_dimensions[ri].height=14; ri+=1
    ri+=2

    # IT Filing Checklist
    ri=_sec_banner(ws,ri,10,"▶  IT FILING CHECKLIST — ACTIONS REQUIRED",SEC3_BG)
    _,cl=_load(it_rc,"IT_Filing_Checklist",skip=2)
    if cl:
        _hdr(ws,ri,[("No",5),("Action Item",42),("Data Source",20),
                    ("Status",12),("Reference",25),("Priority",10)],bg=SEC3_BG); ri+=1
        for row in cl:
            if not row or not row[0]: continue
            bg=ALT1 if ri%2==0 else ALT2
            pri=str(row[5] if len(row)>5 else "").strip()
            pb=RED_BG if pri=="HIGH" else (YELLOW_BG if pri=="MEDIUM" else bg)
            for ci,v in enumerate(row[:6],1):
                _w(ws,ri,ci,v,bg=pb if ci==6 else bg,
                   align="center" if ci in (1,4,6) else "left")
            ws.row_dimensions[ri].height=14; ri+=1
    ws.sheet_properties.tabColor=SEC3_BG
    print(f"  ✓ Sheet 7 — 26AS + GSTR-3B Status + IT Checklist")


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
        AN,IT,C26,GB,TA,MX=_sources(cf)
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
        _sh4(wb,cn,fy,AN)
        _sh5(wb,cn,fy,AN,IT,MX)
        _sh6(wb,cn,fy,IT)
        _sh7(wb,cn,fy,IT,AN)

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
