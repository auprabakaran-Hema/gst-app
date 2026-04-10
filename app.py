"""
GST Reconciliation Web App — v4 FINAL
=======================================
• No license / no trial — full access for everyone
• No Selenium / no browser — upload files and get Excel
• 3 tabs: Reconciliation | GSTR-1 Detail | Download Status
• Works on Render.com (reads $PORT from environment)
• gst_suite_final.py and gstr1_extract.py stay on server only

DEPLOY FILES (all in same folder / GitHub repo):
  app.py
  gst_suite_final.py    ← full 6660-line version
  gstr1_extract.py      ← full 589-line version
  requirements.txt
  Procfile
"""

import os, sys, json, zipfile, re, time, shutil, uuid, threading
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, send_file, render_template_string, abort

import tempfile, platform

# ── Directories ───────────────────────────────────────────────────
def _get_app_dir(subfolder):
    if platform.system() == "Windows":
        base = Path(os.path.expanduser("~")) / "Downloads" / "GST_WebApp"
    else:
        base = Path(tempfile.gettempdir()) / "gst_webapp"
    d = base / subfolder
    d.mkdir(parents=True, exist_ok=True)
    return d

UPLOAD_DIR  = _get_app_dir("uploads")
OUTPUT_DIR  = _get_app_dir("outputs")
ALLOWED_EXT = {".zip", ".xlsx", ".xls", ".pdf", ".json"}
MAX_FILE_MB = 100
JOB_TTL_S   = 7200   # files deleted after 2 hours

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024

# ── Job tracking ──────────────────────────────────────────────────
jobs = {}
jobs_lock = threading.Lock()

# ── Rate limiting ─────────────────────────────────────────────────
_rate = {}; _rate_lock = threading.Lock()

def _check_rate(ip, limit=30, window=60):
    now = time.time()
    with _rate_lock:
        hits = [t for t in _rate.get(ip, []) if now - t < window]
        if len(hits) >= limit: return False
        hits.append(now); _rate[ip] = hits
    return True

def rate_limit(limit=30, window=60):
    from functools import wraps
    def dec(f):
        @wraps(f)
        def wrapped(*a, **kw):
            ip = request.remote_addr or "unknown"
            if not _check_rate(ip, limit, window):
                return jsonify(error="Too many requests. Wait 1 minute."), 429
            return f(*a, **kw)
        return wrapped
    return dec

# ── Helpers ───────────────────────────────────────────────────────
def _cleanup_old_jobs():
    try:
        now = time.time()
        for d in [UPLOAD_DIR, OUTPUT_DIR]:
            for sub in d.iterdir():
                if sub.is_dir() and (now - sub.stat().st_mtime) > JOB_TTL_S:
                    shutil.rmtree(str(sub), ignore_errors=True)
    except: pass

def _cleanup_uploads(job_id):
    try:
        up = UPLOAD_DIR / job_id
        if up.exists(): shutil.rmtree(str(up), ignore_errors=True)
    except: pass

def _find_engine(name):
    """Locate script by name — checks app folder first, then cwd."""
    for loc in [
        Path(__file__).parent / name,
        Path(os.getcwd()) / name,
        Path(os.path.expanduser("~")) / "Desktop" / name,
        Path(os.path.expanduser("~")) / "Downloads" / name,
    ]:
        if loc.exists(): return loc
    return None

MONTHS_MAP = {
    "april":"April","may":"May","june":"June","july":"July","august":"August",
    "september":"September","october":"October","november":"November",
    "december":"December","january":"January","february":"February","march":"March",
    "04":"April","05":"May","06":"June","07":"July","08":"August",
    "09":"September","10":"October","11":"November","12":"December",
    "01":"January","02":"February","03":"March",
}

def _fy_months(fy):
    s = int(fy.split("-")[0]); e = s + 1
    return {
        "April":str(s),"May":str(s),"June":str(s),"July":str(s),
        "August":str(s),"September":str(s),"October":str(s),"November":str(s),
        "December":str(s),"January":str(e),"February":str(e),"March":str(e),
    }

def _detect_month(fpath, FY_MONTHS):
    name = Path(fpath).stem.lower()
    for part in re.split(r'[_\-\s]', name):
        if part in MONTHS_MAP:
            mon = MONTHS_MAP[part]
            return mon, FY_MONTHS.get(mon, list(FY_MONTHS.values())[0])
    try:
        with zipfile.ZipFile(fpath) as z:
            for jn in z.namelist():
                if jn.endswith(".json"):
                    with z.open(jn) as jf:
                        d = json.load(jf)
                        fp = re.sub(r'[^0-9]', '', d.get("fp", ""))
                        if len(fp) == 6:
                            mon = MONTHS_MAP.get(fp[:2])
                            if mon: return mon, fp[2:]
    except: pass
    return None, None

# ── Block script access ───────────────────────────────────────────
@app.before_request
def block_scripts():
    p = request.path.lower()
    if p.endswith((".py", ".pyc")) or "gst_suite" in p or "gstr1_extract" in p:
        abort(403)

# ═══════════════════════════════════════════════════════════════════
# HTML — 3 tabs: Reconciliation | GSTR-1 Detail | Download Status
# ═══════════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GST Reconciliation Portal</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{--bg:#0a0e1a;--surf:#111827;--surf2:#1a2235;--bdr:#1e3050;--accent:#00e5ff;
  --accent2:#7c3aed;--grn:#00e676;--org:#ff6d00;--red:#ff1744;--txt:#e8edf5;
  --muted:#6b7fa3;--mono:'IBM Plex Mono',monospace;--sans:'Syne',sans-serif}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;
  background-image:linear-gradient(rgba(0,229,255,.04) 1px,transparent 1px),
  linear-gradient(90deg,rgba(0,229,255,.04) 1px,transparent 1px);
  background-size:40px 40px;pointer-events:none;z-index:0}
.wrap{max-width:1000px;margin:0 auto;padding:2rem 1.5rem;position:relative;z-index:1}
header{text-align:center;padding:1.75rem 0 1rem}
.logo{display:inline-flex;align-items:center;gap:.7rem;margin-bottom:.75rem}
.logo-icon{width:44px;height:44px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:10px;display:flex;align-items:center;justify-content:center;font-size:1.3rem}
.logo-text{font-size:1rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
h1{font-size:clamp(1.4rem,3vw,2rem);font-weight:800;letter-spacing:-.02em;line-height:1.1}
h1 span{background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.sub{color:var(--muted);font-size:.82rem;margin-top:.35rem;font-family:var(--mono)}
.badge{display:inline-flex;align-items:center;gap:.35rem;padding:.28rem .8rem;
  border-radius:100px;font-size:.72rem;font-weight:700;font-family:var(--mono);margin-top:.5rem;
  background:rgba(0,230,118,.15);color:var(--grn);border:1px solid rgba(0,230,118,.4)}
/* Tabs */
.tabs{display:flex;gap:.25rem;border-bottom:2px solid var(--bdr);margin-bottom:1.1rem;overflow-x:auto}
.tb{padding:.55rem 1.1rem;background:none;border:none;color:var(--muted);
  font-family:var(--sans);font-size:.78rem;font-weight:700;cursor:pointer;
  border-bottom:2px solid transparent;margin-bottom:-2px;transition:all .2s;
  text-transform:uppercase;letter-spacing:.06em;white-space:nowrap}
.tb:hover{color:var(--txt)}.tb.active{color:var(--accent);border-bottom-color:var(--accent)}
.tp{display:none}.tp.active{display:block}
/* Cards */
.card{background:var(--surf);border:1px solid var(--bdr);border-radius:13px;
  padding:1.35rem;margin-bottom:1rem;transition:border-color .2s}
.card:hover{border-color:rgba(0,229,255,.15)}
.ct{font-size:.8rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--accent);margin-bottom:.9rem;display:flex;align-items:center;gap:.45rem}
.ct::before{content:'';width:3px;height:1em;background:var(--accent);border-radius:2px}
/* Form */
.fg2{display:grid;grid-template-columns:1fr 1fr;gap:.75rem}
@media(max-width:600px){.fg2{grid-template-columns:1fr}}
.fg{display:flex;flex-direction:column;gap:.3rem}
label{font-size:.68rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
input[type=text]{background:var(--surf2);border:1px solid var(--bdr);border-radius:6px;
  padding:.5rem .75rem;color:var(--txt);font-family:var(--mono);font-size:.82rem;
  transition:border-color .2s;width:100%}
input:focus{outline:none;border-color:var(--accent)}
input::placeholder{color:var(--muted)}
/* Drop zones */
.dg{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:.65rem;margin-top:.45rem}
.dz{background:var(--surf2);border:2px dashed var(--bdr);border-radius:10px;
  padding:1rem .65rem;text-align:center;cursor:pointer;transition:all .2s;
  position:relative;min-height:100px;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:.3rem}
.dz:hover,.dz.drag-over{border-color:var(--accent);background:rgba(0,229,255,.04)}
.dz.has-files{border-color:var(--grn);border-style:solid;background:rgba(0,230,118,.04)}
.dz-ic{font-size:1.6rem;line-height:1}
.dz-lb{font-size:.64rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.dz-ht{font-size:.6rem;color:var(--muted);font-family:var(--mono)}
.dz-cn{font-size:.64rem;color:var(--grn);font-weight:600;font-family:var(--mono)}
.dz input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer}
/* Buttons */
.btn{width:100%;padding:.8rem;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border:none;border-radius:10px;color:#000;font-family:var(--sans);font-size:.88rem;
  font-weight:800;letter-spacing:.05em;text-transform:uppercase;cursor:pointer;
  transition:transform .15s,box-shadow .15s;margin-top:.3rem}
.btn:hover{transform:translateY(-2px);box-shadow:0 8px 24px rgba(0,229,255,.25)}
.btn:disabled{opacity:.4;cursor:not-allowed;transform:none}
/* Progress */
.pw{display:none}
.pb-w{background:var(--surf2);border-radius:100px;height:5px;overflow:hidden;margin:.65rem 0}
.pb{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));
  border-radius:100px;transition:width .4s;width:0%}
.lb{background:#000;border:1px solid var(--bdr);border-radius:6px;
  padding:.75rem;font-family:var(--mono);font-size:.7rem;height:155px;overflow-y:auto;
  color:#aaffcc;line-height:1.7}
.lb .err{color:#ff6b6b}.lb .info{color:var(--accent)}.lb .ok{color:var(--grn)}.lb .warn{color:var(--org)}
/* Downloads */
.dw{display:none}
.dl-g{display:grid;grid-template-columns:repeat(auto-fill,minmax(185px,1fr));gap:.65rem;margin-top:.65rem}
.dlc{background:var(--surf2);border:1px solid var(--bdr);border-radius:10px;
  padding:.9rem;display:flex;flex-direction:column;gap:.5rem}
.dl-n{font-size:.72rem;font-weight:600;color:var(--txt)}
.dl-s{font-size:.64rem;color:var(--muted);font-family:var(--mono)}
.btn-dl{padding:.38rem .8rem;background:var(--surf);border:1px solid var(--accent);
  border-radius:5px;color:var(--accent);font-family:var(--mono);font-size:.72rem;
  cursor:pointer;text-decoration:none;display:inline-block;transition:background .15s}
.btn-dl:hover{background:rgba(0,229,255,.1)}
/* Status badge */
.sbg{display:inline-flex;align-items:center;gap:.28rem;padding:.2rem .55rem;
  border-radius:100px;font-size:.64rem;font-weight:700;font-family:var(--mono)}
.s-p{background:rgba(255,109,0,.15);color:var(--org);border:1px solid rgba(255,109,0,.4)}
.s-d{background:rgba(0,230,118,.15);color:var(--grn);border:1px solid rgba(0,230,118,.4)}
.s-e{background:rgba(255,23,68,.15);color:var(--red);border:1px solid rgba(255,23,68,.4)}
.pulse{animation:pulse 1.2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
/* Pills */
.pills{display:flex;flex-wrap:wrap;gap:.35rem;margin-bottom:.75rem}
.pill{padding:.22rem .6rem;background:var(--surf2);border:1px solid var(--bdr);
  border-radius:100px;font-size:.64rem;color:var(--muted);font-family:var(--mono)}
/* Download status table */
.dst{width:100%;border-collapse:collapse;font-size:.69rem;font-family:var(--mono);margin-top:.55rem}
.dst th{background:var(--surf2);color:var(--muted);font-size:.6rem;font-weight:700;
  text-transform:uppercase;letter-spacing:.05em;padding:.4rem .5rem;
  border:1px solid var(--bdr);text-align:center}
.dst th:first-child{text-align:left}
.dst td{padding:.36rem .5rem;border:1px solid var(--bdr);text-align:center}
.dst tr:nth-child(even) td{background:rgba(255,255,255,.016)}
.dst td:first-child{text-align:left;color:var(--txt);font-weight:600}
.c-ok{color:var(--grn);font-weight:700}.c-fl{color:var(--red);font-weight:700}
.c-pd{color:var(--org)}.c-sk{color:var(--muted)}
/* Info box */
.info-box{background:rgba(0,229,255,.06);border:1px solid rgba(0,229,255,.2);
  border-radius:9px;padding:.9rem 1rem;margin-bottom:.9rem;font-size:.78rem;
  color:var(--muted);line-height:1.65}
.info-box strong{color:var(--txt)}
</style>
</head>
<body>
<div class="wrap">

<header>
  <div class="logo">
    <div class="logo-icon">₹</div>
    <div class="logo-text">GST Recon</div>
  </div>
  <h1>Annual GST <span>Reconciliation Portal</span></h1>
  <p class="sub">Upload returns → Get reconciliation Excel + GSTR-1 full detail</p>
  <div class="badge">⭐ FULL ACCESS — All Features Enabled</div>
</header>

<!-- TABS -->
<div class="tabs">
  <button class="tb active" onclick="switchTab('recon',event)">📊 Reconciliation</button>
  <button class="tb" onclick="switchTab('gstr1',event)">📋 GSTR-1 Detail</button>
  <button class="tb" onclick="switchTab('dlstatus',event)">🔄 Download Status</button>
</div>

<!-- ══ TAB 1: RECONCILIATION ══ -->
<div class="tp active" id="tab-recon">

<div class="info-box">
  <strong>How it works:</strong> Upload your downloaded GST return files below.
  The portal generates a full <strong>Annual Reconciliation Excel</strong> (7 sheets: Summary, GSTR-1 Sales,
  GSTR-2B ITC, GSTR-2A Purchases, GSTR-3B Status, R1 vs 3B, Tax Liability) plus a separate
  <strong>GSTR-1 Full Detail Excel</strong> (13 sheets) — automatically.
</div>

<form id="recon-form">
<div class="card">
  <div class="ct">Client Details</div>
  <div class="fg2">
    <div class="fg"><label>GSTIN *</label>
      <input type="text" id="r-gstin" placeholder="33ABCDE1234F1ZX" maxlength="15" required></div>
    <div class="fg"><label>Company Name *</label>
      <input type="text" id="r-name" placeholder="ABC Traders" required></div>
    <div class="fg"><label>Financial Year</label>
      <input type="text" id="r-fy" value="2025-26"></div>
    <div class="fg"><label>State (optional)</label>
      <input type="text" id="r-state" placeholder="Tamil Nadu"></div>
  </div>
</div>

<div class="card">
  <div class="ct">Upload Return Files</div>
  <div class="dg">
    <div class="dz" id="zone-r1">
      <div class="dz-ic">📋</div><div class="dz-lb">GSTR-1</div>
      <div class="dz-ht">ZIP files (up to 12)</div>
      <div class="dz-cn" id="cnt-r1">No files</div>
      <input type="file" multiple accept=".zip,.json" data-zone="r1" onchange="updateZone('r1',this)">
    </div>
    <div class="dz" id="zone-r2b">
      <div class="dz-ic">🏦</div><div class="dz-lb">GSTR-2B</div>
      <div class="dz-ht">Excel (.xlsx)</div>
      <div class="dz-cn" id="cnt-r2b">No files</div>
      <input type="file" multiple accept=".xlsx,.xls,.zip" data-zone="r2b" onchange="updateZone('r2b',this)">
    </div>
    <div class="dz" id="zone-r2a">
      <div class="dz-ic">📊</div><div class="dz-lb">GSTR-2A</div>
      <div class="dz-ht">ZIP or Excel</div>
      <div class="dz-cn" id="cnt-r2a">No files</div>
      <input type="file" multiple accept=".zip,.xlsx" data-zone="r2a" onchange="updateZone('r2a',this)">
    </div>
    <div class="dz" id="zone-r3b">
      <div class="dz-ic">📄</div><div class="dz-lb">GSTR-3B</div>
      <div class="dz-ht">PDF files</div>
      <div class="dz-cn" id="cnt-r3b">No files</div>
      <input type="file" multiple accept=".pdf" data-zone="r3b" onchange="updateZone('r3b',this)">
    </div>
    <div class="dz" id="zone-cust">
      <div class="dz-ic">👥</div><div class="dz-lb">Customer Names</div>
      <div class="dz-ht">GSTIN→Name Excel</div>
      <div class="dz-cn" id="cnt-cust">No file</div>
      <input type="file" accept=".xlsx,.xls" data-zone="cust" onchange="updateZone('cust',this)">
    </div>
    <div class="dz" id="zone-taxlib">
      <div class="dz-ic">📑</div><div class="dz-lb">Tax Liability</div>
      <div class="dz-ht">Portal Excel export</div>
      <div class="dz-cn" id="cnt-taxlib">No file</div>
      <input type="file" accept=".xlsx,.xls" data-zone="taxlib" onchange="updateZone('taxlib',this)">
    </div>
  </div>
</div>

<div class="card">
  <button type="submit" class="btn" id="r-submit">Generate Reconciliation + GSTR-1 Detail →</button>
</div>
</form>

<div class="card pw" id="r-pw">
  <div class="ct">Processing <span class="sbg s-p pulse" id="r-badge">Running</span></div>
  <div class="pb-w"><div class="pb" id="r-pb"></div></div>
  <div class="lb" id="r-lb"></div>
</div>
<div class="card dw" id="r-dw">
  <div class="ct">Downloads Ready</div>
  <div class="dl-g" id="r-dlg"></div>
  <p style="color:var(--muted);font-size:.66rem;margin-top:.7rem;font-family:var(--mono)">
    ⏳ Files available for 2 hours. Download before closing the page.
  </p>
</div>
</div><!-- /tab-recon -->

<!-- ══ TAB 2: GSTR-1 DETAIL ══ -->
<div class="tp" id="tab-gstr1">
<div class="card">
  <div class="ct">GSTR-1 Comprehensive Extraction — 13 Sheets</div>
  <div class="pills">
    <span class="pill">B2B Invoices</span><span class="pill">B2B Item Detail</span>
    <span class="pill">HSN Summary</span><span class="pill">B2CS / B2CL</span>
    <span class="pill">Credit Notes</span><span class="pill">Debit Notes</span>
    <span class="pill">Exports</span><span class="pill">Nil Rated</span>
    <span class="pill">GSTR-1A Amendments</span><span class="pill">Document Summary</span>
    <span class="pill">Master Summary</span>
  </div>
  <p style="color:var(--muted);font-size:.78rem;line-height:1.6">
    Upload GSTR-1 ZIP files for all months. Customer names are auto-looked up from any GSTR-2B/2A
    file you include. Add a <strong style="color:var(--txt)">customer_names.xlsx</strong>
    (GSTIN + Name columns) for local lookup.
  </p>
</div>
<form id="g1-form">
<div class="card">
  <div class="ct">Client Details</div>
  <div class="fg2">
    <div class="fg"><label>GSTIN *</label>
      <input type="text" id="g1-gstin" placeholder="33ABCDE1234F1ZX" maxlength="15" required></div>
    <div class="fg"><label>Company Name *</label>
      <input type="text" id="g1-name" placeholder="ABC Traders" required></div>
    <div class="fg"><label>Financial Year</label>
      <input type="text" id="g1-fy" value="2025-26"></div>
  </div>
</div>
<div class="card">
  <div class="ct">Upload Files</div>
  <div class="dg">
    <div class="dz" id="zone-g1r1">
      <div class="dz-ic">📋</div><div class="dz-lb">GSTR-1 ZIPs</div>
      <div class="dz-ht">All 12 months</div>
      <div class="dz-cn" id="cnt-g1r1">No files</div>
      <input type="file" multiple accept=".zip" data-zone="g1r1" onchange="updateZone('g1r1',this)">
    </div>
    <div class="dz" id="zone-g1r2b">
      <div class="dz-ic">🏦</div><div class="dz-lb">GSTR-2B / 2A</div>
      <div class="dz-ht">For customer names</div>
      <div class="dz-cn" id="cnt-g1r2b">No files</div>
      <input type="file" multiple accept=".xlsx,.zip" data-zone="g1r2b" onchange="updateZone('g1r2b',this)">
    </div>
    <div class="dz" id="zone-g1cust">
      <div class="dz-ic">👥</div><div class="dz-lb">Customer Names</div>
      <div class="dz-ht">GSTIN→Name Excel</div>
      <div class="dz-cn" id="cnt-g1cust">No file</div>
      <input type="file" accept=".xlsx" data-zone="g1cust" onchange="updateZone('g1cust',this)">
    </div>
  </div>
</div>
<div class="card">
  <button type="submit" class="btn" id="g1-submit">Generate GSTR-1 Full Detail Excel →</button>
</div>
</form>
<div class="card pw" id="g1-pw">
  <div class="ct">Extracting <span class="sbg s-p pulse" id="g1-badge">Running</span></div>
  <div class="pb-w"><div class="pb" id="g1-pb"></div></div>
  <div class="lb" id="g1-lb"></div>
</div>
<div class="card dw" id="g1-dw">
  <div class="ct">GSTR-1 Detail Ready</div>
  <div class="dl-g" id="g1-dlg"></div>
</div>
</div><!-- /tab-gstr1 -->

<!-- ══ TAB 3: DOWNLOAD STATUS ══ -->
<div class="tp" id="tab-dlstatus">
<div class="card">
  <div class="ct">Download Status — 12 Months × 5 Returns</div>
  <p style="color:var(--muted);font-size:.78rem;line-height:1.6;margin-bottom:.85rem">
    After running <strong style="color:var(--txt)">RUN_ME.bat</strong> locally, upload the generated
    <strong style="color:var(--txt)">MASTER_REPORT_*.xlsx</strong> file here to see which returns
    downloaded OK and which failed. Or paste a live Job ID from the Reconciliation tab.
  </p>
  <div class="fg2" style="margin-bottom:.8rem">
    <div class="fg">
      <label>Live Job ID (from Reconciliation tab)</label>
      <input type="text" id="ds-jid" placeholder="e.g. a3f2c9b1">
    </div>
    <div class="fg">
      <label>Upload MASTER_REPORT Excel</label>
      <div class="dz" id="zone-master"
           style="min-height:60px;flex-direction:row;padding:.6rem;gap:.6rem;min-width:0">
        <div class="dz-ic" style="font-size:1.2rem">📊</div>
        <div style="text-align:left">
          <div class="dz-lb">MASTER_REPORT*.xlsx</div>
          <div class="dz-cn" id="cnt-master">No file</div>
        </div>
        <input type="file" accept=".xlsx" data-zone="master" onchange="updateZone('master',this)">
      </div>
    </div>
  </div>
  <button class="btn" style="margin-top:0" onclick="loadDlStatus()">Load Status →</button>
</div>

<div class="card" id="ds-result" style="display:none">
  <div class="ct">Status — <span id="ds-title">—</span></div>
  <div style="overflow-x:auto">
    <table class="dst">
      <thead><tr>
        <th style="text-align:left">Month</th>
        <th>GSTR-1</th><th>GSTR-1A</th><th>GSTR-2B</th><th>GSTR-2A</th><th>GSTR-3B</th>
        <th>Summary</th>
      </tr></thead>
      <tbody id="ds-tb"></tbody>
    </table>
  </div>
  <div id="ds-sum" style="margin-top:.6rem;font-size:.72rem;font-family:var(--mono);color:var(--muted)"></div>
</div>

<div class="card pw" id="ds-pw">
  <div class="ct">Job Progress <span class="sbg s-p pulse" id="ds-badge">Running</span></div>
  <div class="pb-w"><div class="pb" id="ds-pb"></div></div>
  <div class="lb" id="ds-lb"></div>
</div>
</div><!-- /tab-dlstatus -->

</div><!-- /wrap -->
<script>
// Tab switching
function switchTab(name, e){
  if(e) e.preventDefault();
  document.querySelectorAll('.tb').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.tp').forEach(p=>p.classList.remove('active'));
  if(e&&e.currentTarget) e.currentTarget.classList.add('active');
  else document.querySelectorAll('.tb').forEach(b=>{
    if(b.getAttribute('onclick')&&b.getAttribute('onclick').includes("'"+name+"'"))
      b.classList.add('active');
  });
  document.getElementById('tab-'+name).classList.add('active');
}

// File zones
const zoneFiles={};
function updateZone(zone, inp){
  const files=Array.from(inp.files);
  zoneFiles[zone]=files;
  const cn=document.getElementById('cnt-'+zone);
  const el=document.getElementById('zone-'+zone);
  if(cn) cn.textContent=files.length?files.length+' file'+(files.length>1?'s':'')+' selected':'No files';
  if(el) el.classList.toggle('has-files', files.length>0);
}
document.querySelectorAll('.dz').forEach(z=>{
  z.addEventListener('dragover',e=>{e.preventDefault();z.classList.add('drag-over');});
  z.addEventListener('dragleave',()=>z.classList.remove('drag-over'));
  z.addEventListener('drop',e=>{
    e.preventDefault();z.classList.remove('drag-over');
    const inp=z.querySelector('input[type=file]');if(!inp) return;
    const dt=new DataTransfer();
    [...e.dataTransfer.files].forEach(f=>dt.items.add(f));
    inp.files=dt.files; updateZone(inp.dataset.zone, inp);
  });
});

// Reconciliation form
document.getElementById('recon-form').addEventListener('submit', async e=>{
  e.preventDefault();
  const gstin=document.getElementById('r-gstin').value.trim().toUpperCase();
  const cname=document.getElementById('r-name').value.trim();
  const fy=document.getElementById('r-fy').value.trim()||'2025-26';
  if(!gstin||gstin.length!==15){alert('Enter a valid 15-character GSTIN');return;}
  if(!cname){alert('Enter company name');return;}
  const hasFiles=['r1','r2b','r2a','r3b','cust','taxlib'].some(z=>(zoneFiles[z]||[]).length>0);
  if(!hasFiles){alert('Upload at least one return file');return;}
  const fd=new FormData();
  fd.append('gstin',gstin);fd.append('client_name',cname);
  fd.append('fy',fy);fd.append('mode','recon');
  ['r1','r2b','r2a','r3b','cust','taxlib'].forEach(z=>
    (zoneFiles[z]||[]).forEach(f=>fd.append('files_'+z,f)));
  await startJob(fd,'r','Generate Reconciliation + GSTR-1 Detail →');
});

// GSTR-1 detail form
document.getElementById('g1-form').addEventListener('submit', async e=>{
  e.preventDefault();
  const gstin=document.getElementById('g1-gstin').value.trim().toUpperCase();
  const cname=document.getElementById('g1-name').value.trim();
  const fy=document.getElementById('g1-fy').value.trim()||'2025-26';
  if(!gstin||gstin.length!==15){alert('Enter a valid 15-character GSTIN');return;}
  if(!cname){alert('Enter company name');return;}
  if(!(zoneFiles['g1r1']||[]).length){alert('Upload at least one GSTR-1 ZIP');return;}
  const fd=new FormData();
  fd.append('gstin',gstin);fd.append('client_name',cname);
  fd.append('fy',fy);fd.append('mode','gstr1only');
  (zoneFiles['g1r1']||[]).forEach(f=>fd.append('files_r1',f));
  (zoneFiles['g1r2b']||[]).forEach(f=>fd.append('files_r2b',f));
  (zoneFiles['g1cust']||[]).forEach(f=>fd.append('files_cust',f));
  await startJob(fd,'g1','Generate GSTR-1 Full Detail Excel →');
});

async function startJob(fd, pfx, btnLbl){
  document.getElementById(pfx+'-pw').style.display='block';
  const dw=document.getElementById(pfx+'-dw');if(dw) dw.style.display='none';
  document.getElementById(pfx+'-lb').innerHTML='';
  document.getElementById(pfx+'-pb').style.width='0%';
  const btn=document.getElementById(pfx+'-submit');
  btn.disabled=true; btn.textContent='Uploading...';
  try{
    const res=await fetch('/api/upload',{method:'POST',body:fd});
    const d=await res.json();
    if(!d.job_id) throw new Error(d.error||'Upload failed');
    addLog(pfx,'info','Uploaded. Processing...');
    btn.textContent='Processing...';
    document.getElementById('ds-jid').value=d.job_id;
    pollJob(d.job_id, pfx, btnLbl);
  }catch(err){
    addLog(pfx,'err','Error: '+err.message);
    setBadge(pfx,'e','Failed');
    btn.disabled=false; btn.textContent=btnLbl;
  }
}

async function pollJob(jid, pfx, btnLbl){
  try{
    const res=await fetch('/api/job/'+jid);
    const d=await res.json();
    if(d.logs) d.logs.forEach(l=>addLog(pfx,l.type,l.msg));
    if(d.progress!==undefined)
      document.getElementById(pfx+'-pb').style.width=d.progress+'%';
    if(d.dl_status&&Object.keys(d.dl_status).length) renderDlStatus(d.dl_status,jid);
    if(d.status==='done'){
      setBadge(pfx,'d','Complete');
      document.getElementById(pfx+'-pb').style.width='100%';
      const btn=document.getElementById(pfx+'-submit');
      btn.disabled=false; btn.textContent=btnLbl;
      showDownloads(pfx,jid,d.files);
      return;
    }
    if(d.status==='error'){
      addLog(pfx,'err','Error: '+(d.error||'Unknown'));
      setBadge(pfx,'e','Failed');
      const btn=document.getElementById(pfx+'-submit');
      btn.disabled=false; btn.textContent=btnLbl;
      return;
    }
    setTimeout(()=>pollJob(jid,pfx,btnLbl), 1500);
  }catch(err){setTimeout(()=>pollJob(jid,pfx,btnLbl), 3000);}
}

function addLog(pfx,type,msg){
  const b=document.getElementById(pfx+'-lb');if(!b) return;
  const l=document.createElement('div'); l.className=type;
  l.textContent='['+new Date().toLocaleTimeString()+'] '+msg;
  b.appendChild(l); b.scrollTop=b.scrollHeight;
}
function setBadge(pfx,type,label){
  const b=document.getElementById(pfx+'-badge');if(!b) return;
  b.className='sbg s-'+type; b.textContent=label;
  if(type!=='p') b.classList.remove('pulse');
}
function showDownloads(pfx,jid,files){
  const sec=document.getElementById(pfx+'-dw');
  const grid=document.getElementById(pfx+'-dlg');
  if(!sec||!grid) return;
  sec.style.display='block'; grid.innerHTML='';
  const ICONS={'ANNUAL':'📊','GSTR3BR1':'📋','GSTR3BR2A':'📈','GSTR1_FULL':'📑',
               'RECONCIL':'📊','SUMMARY':'📊','R1_VS':'📋','TAX_LI':'📑'};
  files.forEach(f=>{
    const icon=Object.entries(ICONS).find(([k])=>f.name.toUpperCase().includes(k))?.[1]||'📁';
    const c=document.createElement('div'); c.className='dlc';
    c.innerHTML=`<div style="font-size:1.5rem">${icon}</div>
      <div class="dl-n">${f.name}</div><div class="dl-s">${f.size}</div>
      <a href="/api/download/${jid}/${encodeURIComponent(f.name)}" class="btn-dl" download>Download ↓</a>`;
    grid.appendChild(c);
  });
}

// Download Status
const MONS=['April','May','June','July','August','September','October','November','December','January','February','March'];
const RETS=['GSTR1','GSTR1A','GSTR2B','GSTR2A','GSTR3B'];

function renderDlStatus(st, jid){
  document.getElementById('ds-result').style.display='block';
  document.getElementById('ds-title').textContent=jid||'—';
  const tb=document.getElementById('ds-tb'); tb.innerHTML='';
  let ok=0,fl=0,pd=0;
  MONS.forEach(m=>{
    const tr=document.createElement('tr');
    let rok=0,rfl=0;
    let td=`<td>${m}</td>`;
    RETS.forEach(r=>{
      const v=(st[m+'_'+r]||'SKIP').toUpperCase();
      let cls,txt;
      if(v==='OK'||v==='DONE'){cls='c-ok';txt='✓ OK';rok++;ok++;}
      else if(['TILE_FAIL','NOT_FOUND','TILE_NOT_FOUND','GEN_FAIL','ERR'].some(x=>v.includes(x)))
        {cls='c-fl';txt='✗';rfl++;fl++;}
      else if(v==='TRIGGERED'||v==='PENDING')
        {cls='c-pd';txt='⋯';pd++;}
      else{cls='c-sk';txt='—';}
      td+=`<td class="${cls}">${txt}</td>`;
    });
    const rs=rfl>0?`<span style="color:var(--red)">${rfl} failed</span>`:
              rok===5?`<span style="color:var(--grn)">All OK</span>`:
              rok>0?`<span style="color:var(--org)">${rok}/5 OK</span>`:
              `<span style="color:var(--muted)">—</span>`;
    td+=`<td>${rs}</td>`; tr.innerHTML=td; tb.appendChild(tr);
  });
  document.getElementById('ds-sum').innerHTML=
    `<strong style="color:var(--grn)">${ok} ✓ OK</strong> &nbsp; `+
    `<strong style="color:var(--red)">${fl} ✗ Failed</strong> &nbsp; `+
    `<strong style="color:var(--org)">${pd} ⋯ Pending</strong> &nbsp; `+
    `/ ${MONS.length*RETS.length} expected`;
}

async function loadDlStatus(){
  const jid=document.getElementById('ds-jid').value.trim();
  if(jid){
    document.getElementById('ds-pw').style.display='block';
    try{
      const res=await fetch('/api/job/'+jid);
      const d=await res.json();
      if(d.error){alert('Job not found: '+jid);return;}
      if(d.logs) d.logs.forEach(l=>addLog('ds',l.type,l.msg));
      if(d.progress!==undefined) document.getElementById('ds-pb').style.width=d.progress+'%';
      const st=d.dl_status&&Object.keys(d.dl_status).length
        ?d.dl_status:buildStFromFiles(d.files||[]);
      renderDlStatus(st,jid);
      setBadge('ds',d.status==='done'?'d':d.status==='error'?'e':'p',
        d.status==='done'?'Complete':d.status==='error'?'Failed':'Running');
    }catch(e){alert('Error: '+e.message);}
    return;
  }
  const mf=(zoneFiles['master']||[]);
  if(!mf.length){alert('Enter a Job ID or upload a Master Report Excel');return;}
  const fd=new FormData(); mf.forEach(f=>fd.append('master_file',f));
  try{
    const res=await fetch('/api/parse_master',{method:'POST',body:fd});
    const d=await res.json();
    if(d.dl_status) renderDlStatus(d.dl_status,'Master Report');
    else alert('Parse error: '+(d.error||'Unknown'));
  }catch(e){alert('Upload error: '+e.message);}
}

function buildStFromFiles(files){
  const st={};
  MONS.forEach(m=>RETS.forEach(r=>{
    st[m+'_'+r]=files.some(f=>f.name.includes(r)&&f.name.includes(m))?'OK':'SKIP';
  }));
  return st;
}
</script>
</body>
</html>"""

# ── Routes ────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/upload", methods=["POST"])
@rate_limit(limit=20, window=60)
def api_upload():
    _cleanup_old_jobs()
    gstin       = request.form.get("gstin","").strip().upper()
    client_name = request.form.get("client_name","").strip()
    fy          = request.form.get("fy","2025-26").strip() or "2025-26"
    mode        = request.form.get("mode","recon")

    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN (must be 15 characters)"), 400
    if not client_name:
        return jsonify(error="Company name is required"), 400

    job_id  = str(uuid.uuid4())[:8]
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save files
    saved = {k: [] for k in ("r1","r2b","r2a","r3b","cust","taxlib")}
    for zone in saved:
        for fobj in request.files.getlist(f"files_{zone}"):
            if not fobj.filename: continue
            from werkzeug.utils import secure_filename
            safe = secure_filename(fobj.filename) or f"upload_{zone}_{uuid.uuid4().hex[:6]}"
            if Path(safe).suffix.lower() not in ALLOWED_EXT: continue
            dest = job_dir / safe
            fobj.save(str(dest))
            saved[zone].append(str(dest))

    with jobs_lock:
        jobs[job_id] = {
            "status":"queued","progress":0,"logs":[],"files":[],
            "error":None,"gstin":gstin,"client_name":client_name,
            "fy":fy,"job_dir":str(job_dir),"out_dir":str(out_dir),
            "saved":saved,"mode":mode,"dl_status":{},
        }

    target = run_gstr1_only if mode == "gstr1only" else run_reconciliation
    threading.Thread(target=target, args=(job_id,), daemon=True).start()
    return jsonify(job_id=job_id)

@app.route("/api/job/<job_id>")
@rate_limit(limit=120, window=60)
def api_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify(error="Job not found"), 404
    new_logs = job["logs"][:]
    job["logs"] = []
    return jsonify(status=job["status"], progress=job["progress"],
                   logs=new_logs, files=job["files"],
                   error=job["error"], dl_status=job.get("dl_status",{}))

@app.route("/api/download/<job_id>/<filename>")
@rate_limit(limit=30, window=60)
def api_download(job_id, filename):
    if not re.match(r'^[\w\-. ()]+\.(xlsx|pdf)$', filename):
        abort(400)
    fpath = OUTPUT_DIR / job_id / filename
    if not fpath.exists():
        abort(404)
    return send_file(str(fpath), as_attachment=True, download_name=filename)

@app.route("/api/parse_master", methods=["POST"])
@rate_limit(limit=10, window=60)
def api_parse_master():
    fobj = request.files.get("master_file")
    if not fobj:
        return jsonify(error="No file uploaded"), 400
    tmp = Path(tempfile.gettempdir()) / f"master_{uuid.uuid4().hex[:8]}.xlsx"
    try:
        fobj.save(str(tmp))
        import openpyxl
        wb = openpyxl.load_workbook(str(tmp), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            return jsonify(error="Empty file"), 400
        headers = [str(c or "").strip().upper() for c in rows[0]]
        col = {h: i for i, h in enumerate(headers)}
        def _c(*names):
            for n in names:
                if n in col: return col[n]
            return -1
        MC  = _c("MONTH","PERIOD","MONTH YEAR")
        R1  = _c("GSTR-1","GSTR1","R1")
        R1A = _c("GSTR-1A","GSTR1A","R1A")
        R2B = _c("GSTR-2B","GSTR2B","R2B")
        R2A = _c("GSTR-2A","GSTR2A","R2A")
        R3B = _c("GSTR-3B","GSTR3B","R3B")
        dl_status = {}
        for row in rows[1:]:
            try:
                if MC < 0: continue
                raw = str(row[MC] or "").strip()
                if not raw or raw.lower() in ("none","nan",""): continue
                mon = raw.split()[0]
                def _st(i):
                    if i < 0 or i >= len(row): return "SKIP"
                    return str(row[i] or "SKIP").strip().upper()
                dl_status[f"{mon}_GSTR1"]  = _st(R1)
                dl_status[f"{mon}_GSTR1A"] = _st(R1A)
                dl_status[f"{mon}_GSTR2B"] = _st(R2B)
                dl_status[f"{mon}_GSTR2A"] = _st(R2A)
                dl_status[f"{mon}_GSTR3B"] = _st(R3B)
            except: continue
        wb.close()
        return jsonify(dl_status=dl_status)
    except Exception as e:
        return jsonify(error=str(e)), 500
    finally:
        try: tmp.unlink(missing_ok=True)
        except: pass

# ── Worker: Reconciliation + GSTR-1 detail ────────────────────────
def run_reconciliation(job_id):
    def log(msg, t="info"):
        with jobs_lock: jobs[job_id]["logs"].append({"type":t,"msg":msg})
    def prog(p):
        with jobs_lock: jobs[job_id]["progress"] = p
    def set_dl(k, v):
        with jobs_lock: jobs[job_id]["dl_status"][k] = v

    try:
        job         = jobs[job_id]
        gstin       = job["gstin"]
        client_name = job["client_name"]
        fy          = job["fy"]
        job_dir     = Path(job["job_dir"])
        out_dir     = Path(job["out_dir"])
        saved       = job["saved"]
        FY_MONTHS   = _fy_months(fy)

        log(f"Starting: {client_name} ({gstin}) FY {fy}")
        log("⭐ Full access — all features enabled")
        prog(5)

        # ── Rename uploaded files to standard names ────────────────
        log("Preparing files...")
        for fpath in saved["r1"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1: {mon} {yr}"); set_dl(f"{mon}_GSTR1", "OK")
            else:
                log(f"  ⚠ Month not detected: {Path(fpath).name}", "warn")

        for fpath in saved["r2b"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR2B_{mon}_{yr}.xlsx"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2B: {mon} {yr}"); set_dl(f"{mon}_GSTR2B", "OK")

        for fpath in saved["r2a"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                ext = Path(fpath).suffix.lower()
                dest = job_dir / f"GSTR2A_{mon}_{yr}{ext}"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2A: {mon} {yr}"); set_dl(f"{mon}_GSTR2A", "OK")

        for fpath in saved["r3b"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR3B_{mon}_{yr}.pdf"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-3B: {mon} {yr}"); set_dl(f"{mon}_GSTR3B", "OK")

        for fpath in saved["cust"]:
            dest = job_dir / "customer_names.xlsx"
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))
            log("  Customer names loaded"); break

        # Tax Liability Excel
        for fpath in saved["taxlib"]:
            dest = job_dir / f"TAX_LIABILITY_{Path(fpath).name}"
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))
            log(f"  Tax Liability: {Path(dest).name}"); break

        prog(25)

        # ── Load and run reconciliation engine ────────────────────
        suite_path = _find_engine("gst_suite_final.py")
        if not suite_path:
            raise FileNotFoundError(
                "gst_suite_final.py not found. "
                "Place the full script alongside app.py on the server.")

        log("Loading reconciliation engine...")
        import importlib.util as _ilu, logging as _lg
        spec = _ilu.spec_from_file_location("gst_suite", str(suite_path))
        gst  = _ilu.module_from_spec(spec)
        spec.loader.exec_module(gst)

        s = int(fy.split("-")[0]); e = s + 1
        gst.FY_LABEL = fy
        gst.MONTHS = [
            ("April","04",str(s)),("May","05",str(s)),("June","06",str(s)),
            ("July","07",str(s)),("August","08",str(s)),("September","09",str(s)),
            ("October","10",str(s)),("November","11",str(s)),("December","12",str(s)),
            ("January","01",str(e)),("February","02",str(e)),("March","03",str(e)),
        ]

        _log = _lg.getLogger(f"gst_{job_id}")
        _log.setLevel(_lg.DEBUG)
        class WL(_lg.Handler):
            def emit(self, r):
                log(self.format(r), "err" if r.levelno >= _lg.WARNING else "info")
        _log.addHandler(WL())

        prog(30)
        log("Running annual reconciliation (this may take 1-2 minutes)...")
        gst.write_annual_reconciliation(str(job_dir), client_name, gstin, _log)
        prog(65)
        log("  ✓ Annual reconciliation complete", "ok")

        # ── GSTR-1 Full Detail ────────────────────────────────────
        extract_path = _find_engine("gstr1_extract.py")
        gstr1_zips   = list(job_dir.glob("GSTR1_*.zip"))

        if extract_path and gstr1_zips:
            log(f"Running GSTR-1 detail extraction ({len(gstr1_zips)} months)...")
            try:
                spec2 = _ilu.spec_from_file_location("gstr1_extract", str(extract_path))
                gstr1 = _ilu.module_from_spec(spec2)
                spec2.loader.exec_module(gstr1)
                out_xl = job_dir / f"GSTR1_FULL_DETAIL_{client_name.replace(' ','_')}.xlsx"
                gstr1.extract_gstr1_to_excel(str(job_dir), str(out_xl))
                log(f"  ✓ GSTR-1 detail: {out_xl.name}", "ok")
            except Exception as ex:
                log(f"  ⚠ GSTR-1 extraction error: {ex}", "warn")
        elif not extract_path:
            log("⚠ gstr1_extract.py not found on server — GSTR-1 detail skipped", "warn")
        else:
            log("ℹ No GSTR-1 ZIPs uploaded — GSTR-1 detail not generated")

        prog(85)
        log("Collecting output files...")

        output_files = []
        for fp in sorted(job_dir.glob("*.xlsx")):
            dest_fp = out_dir / fp.name
            shutil.copy2(str(fp), str(dest_fp))
            sz = dest_fp.stat().st_size // 1024
            output_files.append({"name": fp.name, "size": f"{sz} KB"})
            log(f"  ✓ {fp.name} ({sz} KB)", "ok")

        if not output_files:
            raise RuntimeError(
                "No Excel files were generated. "
                "Check that uploaded ZIP files contain valid GST JSON data.")

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready to download.", "ok")
        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"]  = output_files
        _cleanup_uploads(job_id)

    except Exception as exc:
        import traceback
        log(f"Error: {exc}", "err")
        for line in traceback.format_exc().split('\n'):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)
        _cleanup_uploads(job_id)


# ── Worker: GSTR-1 Detail only ───────────────────────────────────
def run_gstr1_only(job_id):
    def log(msg, t="info"):
        with jobs_lock: jobs[job_id]["logs"].append({"type":t,"msg":msg})
    def prog(p):
        with jobs_lock: jobs[job_id]["progress"] = p

    try:
        job         = jobs[job_id]
        client_name = job["client_name"]
        fy          = job["fy"]
        job_dir     = Path(job["job_dir"])
        out_dir     = Path(job["out_dir"])
        saved       = job["saved"]
        FY_MONTHS   = _fy_months(fy)

        log(f"GSTR-1 Detail: {client_name} FY {fy}")
        prog(5)

        for fpath in saved["r1"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  ✓ GSTR-1: {mon} {yr}")
            else:
                log(f"  ⚠ Month not detected: {Path(fpath).name}", "warn")

        for fpath in saved["r2b"] + saved["r2a"]:
            dest = job_dir / Path(fpath).name
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))

        for fpath in saved["cust"]:
            dest = job_dir / "customer_names.xlsx"
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))
            break

        prog(20)
        gstr1_zips = list(job_dir.glob("GSTR1_*.zip"))
        if not gstr1_zips:
            raise RuntimeError(
                "No GSTR1_*.zip files found. "
                "Ensure filenames contain the month name (e.g. GSTR1_April_2025.zip).")

        extract_path = _find_engine("gstr1_extract.py")
        if not extract_path:
            raise FileNotFoundError("gstr1_extract.py not found. Place it alongside app.py.")

        log(f"Extracting {len(gstr1_zips)} month(s)...")
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location("gstr1_extract", str(extract_path))
        gstr1_mod = _ilu.module_from_spec(spec)
        spec.loader.exec_module(gstr1_mod)

        prog(30)
        out_xl = job_dir / f"GSTR1_FULL_DETAIL_{client_name.replace(' ','_')}.xlsx"
        gstr1_mod.extract_gstr1_to_excel(str(job_dir), str(out_xl))
        prog(90)

        output_files = []
        for fp in sorted(job_dir.glob("*.xlsx")):
            dest_fp = out_dir / fp.name
            shutil.copy2(str(fp), str(dest_fp))
            sz = dest_fp.stat().st_size // 1024
            output_files.append({"name": fp.name, "size": f"{sz} KB"})
            log(f"  ✓ {fp.name} ({sz} KB)", "ok")

        if not output_files:
            raise RuntimeError("No output files generated.")

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready.", "ok")
        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"]  = output_files
        _cleanup_uploads(job_id)

    except Exception as exc:
        import traceback
        log(f"Error: {exc}", "err")
        for line in traceback.format_exc().split('\n'):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)
        _cleanup_uploads(job_id)


# ── Startup ───────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n  ============================================================")
    print(f"   GST Reconciliation Portal v4 — FULL ACCESS")
    print(f"  ============================================================")
    print(f"   Upload dir    : {UPLOAD_DIR}")
    print(f"   Output dir    : {OUTPUT_DIR}")
    suite = _find_engine("gst_suite_final.py")
    ext   = _find_engine("gstr1_extract.py")
    print(f"   Suite engine  : {suite or '⚠ NOT FOUND — place gst_suite_final.py here'}")
    print(f"   GSTR-1 engine : {ext   or '⚠ NOT FOUND — place gstr1_extract.py here'}")
    print(f"\n   Open: http://localhost:{port}")
    print(f"  ============================================================\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
