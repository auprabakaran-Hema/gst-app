"""
GST Reconciliation Web App — v4 COMPLETE with Auto-Download
===========================================================
FIXES:
  1. Removed PyArmor dependency - using clean Python code
  2. Added GST Portal auto-download functionality
  3. GSTR-1 Reconciliation with 13-sheet extraction
  4. Auto-download status dashboard
  5. Rate limiting + file cleanup
"""
import os, sys, json, zipfile, re, time, shutil, uuid, threading, hashlib
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, send_file, render_template_string, abort

import tempfile, platform

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
MAX_FILE_MB = 50
JOB_TTL_S   = 7200

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024

jobs      = {}
jobs_lock = threading.Lock()

_rate = {}
_rate_lock = threading.Lock()

def _check_rate(ip, limit=10, window=60):
    now = time.time()
    with _rate_lock:
        hits = [t for t in _rate.get(ip, []) if now - t < window]
        if len(hits) >= limit:
            return False
        hits.append(now)
        _rate[ip] = hits
    return True

def rate_limit(limit=10, window=60):
    from functools import wraps
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            ip = request.remote_addr or "unknown"
            if not _check_rate(ip, limit, window):
                return jsonify(error="Too many requests"), 429
            return f(*args, **kwargs)
        return wrapped
    return decorator

def _cleanup_old_jobs():
    try:
        now = time.time()
        for d in [UPLOAD_DIR, OUTPUT_DIR]:
            for sub in d.iterdir():
                if sub.is_dir() and (now - sub.stat().st_mtime) > JOB_TTL_S:
                    shutil.rmtree(str(sub), ignore_errors=True)
    except:
        pass

def _cleanup_job_files(job_id):
    try:
        up = UPLOAD_DIR / job_id
        if up.exists():
            shutil.rmtree(str(up), ignore_errors=True)
    except:
        pass

@app.before_request
def block_scripts():
    p = request.path.lower()
    if p.endswith(".py") or p.endswith(".pyc") or "gst_suite" in p or "gstr1_extract" in p:
        abort(403)

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GST Reconciliation Portal</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0a0e1a;--surface:#111827;--surface2:#1a2235;--border:#1e3050;
  --accent:#00e5ff;--accent2:#7c3aed;--green:#00e676;--orange:#ff6d00;
  --red:#ff1744;--gold:#ffd700;--text:#e8edf5;--muted:#6b7fa3;
  --mono:'IBM Plex Mono',monospace;--sans:'Syne',sans-serif;
}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;
  background-image:linear-gradient(rgba(0,229,255,.04) 1px,transparent 1px),
    linear-gradient(90deg,rgba(0,229,255,.04) 1px,transparent 1px);
  background-size:40px 40px;pointer-events:none;z-index:0}
.container{max-width:1000px;margin:0 auto;padding:2rem 1.5rem;position:relative;z-index:1}
header{text-align:center;padding:2rem 0 1.25rem}
.logo{display:inline-flex;align-items:center;gap:.75rem;margin-bottom:.85rem}
.logo-icon{width:46px;height:46px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:1.4rem}
.logo-text{font-size:1.05rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
h1{font-size:clamp(1.5rem,3.2vw,2.2rem);font-weight:800;line-height:1.1;letter-spacing:-.02em}
h1 span{background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.subtitle{color:var(--muted);font-size:.85rem;margin-top:.4rem;font-family:var(--mono)}
.vbadge{display:inline-flex;align-items:center;gap:.4rem;padding:.3rem .85rem;
  border-radius:100px;font-size:.75rem;font-weight:700;font-family:var(--mono);margin-top:.6rem;
  background:rgba(255,215,0,.15);color:var(--gold);border:1px solid rgba(255,215,0,.4)}
.tabs{display:flex;gap:.35rem;margin-bottom:1.25rem;border-bottom:2px solid var(--border);padding-bottom:0;
  overflow-x:auto}
.tab-btn{padding:.6rem 1.2rem;background:none;border:none;color:var(--muted);
  font-family:var(--sans);font-size:.82rem;font-weight:700;cursor:pointer;
  border-bottom:2px solid transparent;margin-bottom:-2px;transition:all .2s;
  text-transform:uppercase;letter-spacing:.06em;white-space:nowrap}
.tab-btn:hover{color:var(--text)}
.tab-btn.active{color:var(--accent);border-bottom-color:var(--accent)}
.tab-pane{display:none}.tab-pane.active{display:block}
.card{background:var(--surface);border:1px solid var(--border);border-radius:14px;
  padding:1.5rem;margin-bottom:1.1rem;transition:border-color .2s}
.card:hover{border-color:rgba(0,229,255,.18)}
.card-title{font-size:.85rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--accent);margin-bottom:1rem;display:flex;align-items:center;gap:.5rem}
.card-title::before{content:'';width:3px;height:1em;background:var(--accent);border-radius:2px}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:.85rem}
@media(max-width:600px){.form-grid{grid-template-columns:1fr}}
.fg{display:flex;flex-direction:column;gap:.35rem}
label{font-size:.72rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
input[type=text],input[type=password]{
  background:var(--surface2);border:1px solid var(--border);border-radius:7px;
  padding:.55rem .8rem;color:var(--text);font-family:var(--mono);font-size:.85rem;
  transition:border-color .2s;width:100%}
input:focus{outline:none;border-color:var(--accent)}
input::placeholder{color:var(--muted)}
.drop-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:.75rem;margin-top:.5rem}
.dz{background:var(--surface2);border:2px dashed var(--border);border-radius:11px;
  padding:1.1rem .75rem;text-align:center;cursor:pointer;transition:all .2s;
  position:relative;min-height:105px;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:.35rem}
.dz:hover,.dz.drag-over{border-color:var(--accent);background:rgba(0,229,255,.04)}
.dz.has-files{border-color:var(--green);border-style:solid;background:rgba(0,230,118,.04)}
.dz-icon{font-size:1.7rem;line-height:1}
.dz-label{font-size:.68rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.dz-hint{font-size:.63rem;color:var(--muted);font-family:var(--mono)}
.dz-cnt{font-size:.68rem;color:var(--green);font-weight:600;font-family:var(--mono)}
.dz input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer}
.lic-box{background:linear-gradient(135deg,rgba(0,229,255,.07),rgba(124,58,237,.07));
  border:1px solid var(--accent);border-radius:11px;padding:1rem;margin-bottom:1.1rem}
.lic-title{font-size:.82rem;font-weight:700;color:var(--gold);margin-bottom:.55rem}
.lic-row{display:flex;gap:.4rem}
.lic-row input{flex:1}
.btn-lic{padding:.55rem .85rem;background:var(--gold);border:none;border-radius:7px;
  color:#000;font-weight:700;font-size:.75rem;cursor:pointer;white-space:nowrap}
.lic-msg{font-size:.72rem;margin-top:.35rem;font-family:var(--mono)}
.lic-msg.ok{color:var(--green)}.lic-msg.err{color:var(--red)}
.btn-sub{width:100%;padding:.85rem;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border:none;border-radius:11px;color:#000;font-family:var(--sans);font-size:.9rem;
  font-weight:800;letter-spacing:.05em;text-transform:uppercase;cursor:pointer;
  transition:transform .15s,box-shadow .15s;margin-top:.35rem}
.btn-sub:hover{transform:translateY(-2px);box-shadow:0 8px 26px rgba(0,229,255,.28)}
.btn-sub:disabled{opacity:.42;cursor:not-allowed;transform:none}
.btn-sec{width:100%;padding:.6rem;background:var(--surface2);border:1px solid var(--accent);
  border-radius:7px;color:var(--accent);font-family:var(--sans);font-size:.8rem;
  font-weight:700;cursor:pointer;transition:all .15s;margin-top:.5rem}
.btn-sec:hover{background:rgba(0,229,255,.1)}
.prog-wrap{display:none}
.pbar-wrap{background:var(--surface2);border-radius:100px;height:6px;overflow:hidden;margin:.7rem 0}
.pbar{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));
  border-radius:100px;transition:width .4s;width:0%}
.logbox{background:#000;border:1px solid var(--border);border-radius:7px;
  padding:.85rem;font-family:var(--mono);font-size:.72rem;height:160px;overflow-y:auto;
  color:#aaffcc;line-height:1.7}
.logbox .err{color:#ff6b6b}.logbox .info{color:var(--accent)}
.logbox .ok{color:var(--green)}.logbox .warn{color:var(--orange)}
.dl-wrap{display:none}
.dl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(190px,1fr));gap:.75rem;margin-top:.75rem}
.dlcard{background:var(--surface2);border:1px solid var(--border);border-radius:11px;
  padding:1rem;display:flex;flex-direction:column;gap:.55rem}
.dl-name{font-size:.75rem;font-weight:600;color:var(--text)}
.dl-size{font-size:.67rem;color:var(--muted);font-family:var(--mono)}
.btn-dl{padding:.4rem .85rem;background:var(--surface);border:1px solid var(--accent);
  border-radius:6px;color:var(--accent);font-family:var(--mono);font-size:.75rem;
  cursor:pointer;text-decoration:none;display:inline-block;transition:background .15s}
.btn-dl:hover{background:rgba(0,229,255,.1)}
.sbadge{display:inline-flex;align-items:center;gap:.3rem;padding:.22rem .6rem;
  border-radius:100px;font-size:.68rem;font-weight:700;font-family:var(--mono)}
.s-proc{background:rgba(255,109,0,.15);color:var(--orange);border:1px solid rgba(255,109,0,.4)}
.s-done{background:rgba(0,230,118,.15);color:var(--green);border:1px solid rgba(0,230,118,.4)}
.s-err{background:rgba(255,23,68,.15);color:var(--red);border:1px solid rgba(255,23,68,.4)}
.s-warn{background:rgba(255,215,0,.15);color:var(--gold);border:1px solid rgba(255,215,0,.4)}
.pulse{animation:pulse 1.2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* Download status table */
.dst{width:100%;border-collapse:collapse;font-size:.73rem;font-family:var(--mono);margin-top:.65rem}
.dst th{background:var(--surface2);color:var(--muted);font-size:.64rem;font-weight:700;
  text-transform:uppercase;letter-spacing:.05em;padding:.45rem .55rem;
  border:1px solid var(--border);text-align:center}
.dst th:first-child{text-align:left}
.dst td{padding:.4rem .55rem;border:1px solid var(--border);text-align:center}
.dst tr:nth-child(even) td{background:rgba(255,255,255,.018)}
.dst td:first-child{text-align:left;color:var(--text);font-weight:600}
.c-ok{color:var(--green);font-weight:700}
.c-fail{color:var(--red);font-weight:700}
.c-pend{color:var(--orange)}
.c-skip{color:var(--muted)}

.info-pills{display:flex;flex-wrap:wrap;gap:.4rem;margin-bottom:.85rem}
.pill{padding:.25rem .65rem;background:var(--surface2);border:1px solid var(--border);
  border-radius:100px;font-size:.68rem;color:var(--muted);font-family:var(--mono)}
</style>
</head>
<body>
<div class="container">

<header>
  <div class="logo">
    <div class="logo-icon">₹</div>
    <div class="logo-text">GST Recon</div>
  </div>
  <h1>Annual GST <span>Reconciliation Portal</span></h1>
  <p class="subtitle">Upload returns → Instant reconciliation + GSTR-1 full detail</p>
  <div class="vbadge" style="background:rgba(0,230,118,.15);color:var(--green);border:1px solid rgba(0,230,118,.4)">⭐ FULL ACCESS - Auto Download Enabled</div>
</header>

<!-- TABS -->
<div class="tabs">
  <button class="tab-btn active" onclick="switchTab('recon',event)">📊 Reconciliation</button>
  <button class="tab-btn" onclick="switchTab('gstr1',event)">📋 GSTR-1 Detail</button>
  <button class="tab-btn" onclick="switchTab('download',event)">⬇️ Auto Download</button>
  <button class="tab-btn" onclick="switchTab('dlstatus',event)">🔄 Status</button>
</div>

<!-- ══ TAB 1: RECONCILIATION ══ -->
<div class="tab-pane active" id="tab-recon">
<form id="recon-form">
<div class="card">
  <div class="card-title">Client Details</div>
  <div class="form-grid">
    <div class="fg"><label>GSTIN *</label><input type="text" id="r-gstin" placeholder="33ABCDE1234F1ZX" maxlength="15" required></div>
    <div class="fg"><label>Company Name *</label><input type="text" id="r-name" placeholder="ABC Traders" required></div>
    <div class="fg"><label>Financial Year *</label><input type="text" id="r-fy" value="2025-26" required></div>
    <div class="fg"><label>State</label><input type="text" id="r-state" placeholder="Tamil Nadu"></div>
  </div>
</div>
<div class="card">
  <div class="card-title">Upload Returns</div>
  <div class="drop-grid">
    <div class="dz" id="zone-r1"><div class="dz-icon">📋</div><div class="dz-label">GSTR-1</div>
      <div class="dz-hint">ZIP files (12 months)</div><div class="dz-cnt" id="cnt-r1">No files</div>
      <input type="file" multiple accept=".zip,.json" data-zone="r1" onchange="updateZone('r1',this)"></div>
    <div class="dz" id="zone-r2b"><div class="dz-icon">🏦</div><div class="dz-label">GSTR-2B</div>
      <div class="dz-hint">Excel (.xlsx)</div><div class="dz-cnt" id="cnt-r2b">No files</div>
      <input type="file" multiple accept=".xlsx,.xls,.zip" data-zone="r2b" onchange="updateZone('r2b',this)"></div>
    <div class="dz" id="zone-r2a"><div class="dz-icon">📊</div><div class="dz-label">GSTR-2A</div>
      <div class="dz-hint">ZIP or Excel</div><div class="dz-cnt" id="cnt-r2a">No files</div>
      <input type="file" multiple accept=".zip,.xlsx" data-zone="r2a" onchange="updateZone('r2a',this)"></div>
    <div class="dz" id="zone-r3b"><div class="dz-icon">📄</div><div class="dz-label">GSTR-3B</div>
      <div class="dz-hint">PDF files</div><div class="dz-cnt" id="cnt-r3b">No files</div>
      <input type="file" multiple accept=".pdf" data-zone="r3b" onchange="updateZone('r3b',this)"></div>
    <div class="dz" id="zone-cust"><div class="dz-icon">👥</div><div class="dz-label">Customer Names</div>
      <div class="dz-hint">GSTIN→Name Excel</div><div class="dz-cnt" id="cnt-cust">No file</div>
      <input type="file" accept=".xlsx,.xls" data-zone="cust" onchange="updateZone('cust',this)"></div>
  </div>
</div>
<div class="card"><button type="submit" class="btn-sub" id="r-submit">Generate Reconciliation + GSTR-1 Detail →</button></div>
</form>
<div class="card prog-wrap" id="r-prog">
  <div class="card-title">Processing <span class="sbadge s-proc pulse" id="r-badge">Running</span></div>
  <div class="pbar-wrap"><div class="pbar" id="r-bar"></div></div>
  <div class="logbox" id="r-log"></div>
</div>
<div class="card dl-wrap" id="r-dl">
  <div class="card-title">Downloads Ready</div>
  <div class="dl-grid" id="r-dl-grid"></div>
  <p style="color:var(--muted);font-size:.68rem;margin-top:.75rem;font-family:var(--mono)">⏳ Files available for 2 hours.</p>
</div>
</div>

<!-- ══ TAB 2: GSTR-1 DETAIL ══ -->
<div class="tab-pane" id="tab-gstr1">
<div class="card">
  <div class="card-title">GSTR-1 Comprehensive Extraction — 13 Sheets</div>
  <div class="info-pills">
    <span class="pill">B2B Invoices</span><span class="pill">B2B Item Detail</span>
    <span class="pill">HSN Summary</span><span class="pill">B2CS / B2CL</span>
    <span class="pill">Credit Notes</span><span class="pill">Debit Notes</span>
    <span class="pill">Exports</span><span class="pill">Nil Rated</span>
    <span class="pill">Amendments</span><span class="pill">Doc Summary</span>
    <span class="pill">Master Summary</span>
  </div>
  <p style="color:var(--muted);font-size:.8rem;line-height:1.6">
    Upload all GSTR-1 ZIP files. Customer names are auto-looked up from GSTR-2B/2A.
    Optionally add a <strong style="color:var(--text)">customer_names.xlsx</strong> for local lookup.
  </p>
</div>
<form id="g1-form">
<div class="card">
  <div class="card-title">Client Details</div>
  <div class="form-grid">
    <div class="fg"><label>GSTIN *</label><input type="text" id="g1-gstin" placeholder="33ABCDE1234F1ZX" maxlength="15" required></div>
    <div class="fg"><label>Company Name *</label><input type="text" id="g1-name" placeholder="ABC Traders" required></div>
    <div class="fg"><label>Financial Year *</label><input type="text" id="g1-fy" value="2025-26" required></div>
  </div>
</div>
<div class="card">
  <div class="card-title">Upload Files</div>
  <div class="drop-grid">
    <div class="dz" id="zone-g1r1"><div class="dz-icon">📋</div><div class="dz-label">GSTR-1 ZIPs</div>
      <div class="dz-hint">All 12 months</div><div class="dz-cnt" id="cnt-g1r1">No files</div>
      <input type="file" multiple accept=".zip" data-zone="g1r1" onchange="updateZone('g1r1',this)"></div>
    <div class="dz" id="zone-g1r2b"><div class="dz-icon">🏦</div><div class="dz-label">GSTR-2B / 2A</div>
      <div class="dz-hint">For name lookup</div><div class="dz-cnt" id="cnt-g1r2b">No files</div>
      <input type="file" multiple accept=".xlsx,.zip" data-zone="g1r2b" onchange="updateZone('g1r2b',this)"></div>
    <div class="dz" id="zone-g1cust"><div class="dz-icon">👥</div><div class="dz-label">Customer Names</div>
      <div class="dz-hint">GSTIN→Name Excel</div><div class="dz-cnt" id="cnt-g1cust">No file</div>
      <input type="file" accept=".xlsx" data-zone="g1cust" onchange="updateZone('g1cust',this)"></div>
  </div>
</div>
<div class="card"><button type="submit" class="btn-sub" id="g1-submit">Generate GSTR-1 Full Detail Excel →</button></div>
</form>
<div class="card prog-wrap" id="g1-prog">
  <div class="card-title">Extracting GSTR-1 <span class="sbadge s-proc pulse" id="g1-badge">Running</span></div>
  <div class="pbar-wrap"><div class="pbar" id="g1-bar"></div></div>
  <div class="logbox" id="g1-log"></div>
</div>
<div class="card dl-wrap" id="g1-dl">
  <div class="card-title">GSTR-1 Detail Ready</div>
  <div class="dl-grid" id="g1-dl-grid"></div>
</div>
</div>

<!-- ══ TAB 3: AUTO DOWNLOAD ══ -->
<div class="tab-pane" id="tab-download">
<div class="card">
  <div class="card-title">Auto Download from GST Portal</div>
  <p style="color:var(--muted);font-size:.8rem;line-height:1.6;margin-bottom:1rem">
    Login to GST Portal and automatically download all returns for the financial year.
    Requires valid GST Portal credentials. Chrome browser must be installed.
  </p>
  <div class="info-pills">
    <span class="pill">GSTR-1</span><span class="pill">GSTR-2B</span>
    <span class="pill">GSTR-2A</span><span class="pill">GSTR-3B</span>
  </div>
</div>
<form id="dl-form">
<div class="card">
  <div class="card-title">GST Portal Credentials</div>
  <div class="form-grid">
    <div class="fg"><label>GSTIN *</label><input type="text" id="dl-gstin" placeholder="33ABCDE1234F1ZX" maxlength="15" required></div>
    <div class="fg"><label>Financial Year *</label><input type="text" id="dl-fy" value="2025-26" required></div>
    <div class="fg"><label>GST Portal Username *</label><input type="text" id="dl-user" placeholder="your@email.com" required></div>
    <div class="fg"><label>GST Portal Password *</label><input type="password" id="dl-pass" placeholder="••••••••" required></div>
  </div>
  <p style="color:var(--orange);font-size:.7rem;margin-top:.75rem;font-family:var(--mono)">
    ⚠️ Cloud server deployment: Auto-download requires a LOCAL installation because:<br>
    &nbsp;&nbsp;1. GST Portal has CAPTCHA that must be entered manually in a browser window<br>
    &nbsp;&nbsp;2. Cloud servers (Render) have no display to show the browser<br><br>
    For auto-download, use the <strong style="color:var(--text)">RUN_ME.bat</strong> 
    on your local PC with <strong style="color:var(--text)">gst_suite_final.py</strong>.<br>
    This online portal supports: Upload files → Generate reconciliation Excel ✓
  </p>
</div>
<div class="card">
  <button type="submit" class="btn-sub" id="dl-submit">Start Auto Download →</button>
  <p id="dl-cloud-warn" style="display:none;color:var(--orange);font-size:.72rem;
     margin-top:.5rem;font-family:var(--mono)">
    ⚠️ Auto-download is not available on cloud servers. 
    Use RUN_ME.bat on your local PC instead.
  </p>
</div>
</form>
<div class="card prog-wrap" id="dl-prog">
  <div class="card-title">Downloading from GST Portal <span class="sbadge s-proc pulse" id="dl-badge">Running</span></div>
  <div class="pbar-wrap"><div class="pbar" id="dl-bar"></div></div>
  <div class="logbox" id="dl-log"></div>
</div>
<div class="card dl-wrap" id="dl-dl">
  <div class="card-title">Downloaded Files</div>
  <div class="dl-grid" id="dl-dl-grid"></div>
</div>
</div>

<!-- ══ TAB 4: DOWNLOAD STATUS ══ -->
<div class="tab-pane" id="tab-dlstatus">
<div class="card">
  <div class="card-title">Download Status — All Returns × 12 Months</div>
  <p style="color:var(--muted);font-size:.78rem;line-height:1.6;margin-bottom:.85rem">
    View which returns were downloaded OK and which failed. Paste a Job ID to check status.
  </p>
  <div class="form-grid" style="margin-bottom:.85rem">
    <div class="fg">
      <label>Job ID</label>
      <input type="text" id="ds-jobid" placeholder="e.g. a3f2c9b1">
    </div>
    <div class="fg">
      <label>Upload MASTER_REPORT Excel</label>
      <div class="dz" id="zone-master" style="min-height:65px;flex-direction:row;padding:.65rem;gap:.65rem">
        <div class="dz-icon" style="font-size:1.3rem">📊</div>
        <div><div class="dz-label" style="text-align:left">MASTER_REPORT*.xlsx</div>
          <div class="dz-cnt" id="cnt-master">No file</div></div>
        <input type="file" accept=".xlsx" data-zone="master" onchange="updateZone('master',this)">
      </div>
    </div>
  </div>
  <button class="btn-sub" style="margin-top:0" onclick="loadDownloadStatus()">Load Status →</button>
</div>
<div class="card" id="ds-result" style="display:none">
  <div class="card-title">Download Status — <span id="ds-title">—</span></div>
  <div style="overflow-x:auto">
    <table class="dst">
      <thead><tr>
        <th style="text-align:left">Month</th>
        <th>GSTR-1</th><th>GSTR-1A</th><th>GSTR-2B</th><th>GSTR-2A</th><th>GSTR-3B</th>
        <th>Row Status</th>
      </tr></thead>
      <tbody id="ds-tbody"></tbody>
    </table>
  </div>
  <div id="ds-summary" style="margin-top:.65rem;font-size:.75rem;font-family:var(--mono);color:var(--muted)"></div>
</div>
</div>

</div><!-- /container -->
<script>
// Tabs
function switchTab(name, e){
  if(e) e.preventDefault();
  document.querySelectorAll('.tab-btn').forEach(b=>b.classList.remove('active'));
  document.querySelectorAll('.tab-pane').forEach(p=>p.classList.remove('active'));
  if(e && e.currentTarget) e.currentTarget.classList.add('active');
  else document.querySelectorAll('.tab-btn').forEach(b=>{
    if(b.getAttribute('onclick')&&b.getAttribute('onclick').includes("'"+name+"'")) b.classList.add('active');
  });
  document.getElementById('tab-'+name).classList.add('active');
}

// File zones
const zoneFiles={};
function updateZone(zone,input){
  const files=Array.from(input.files);
  zoneFiles[zone]=files;
  const cnt=document.getElementById('cnt-'+zone);
  const el=document.getElementById('zone-'+zone);
  if(cnt) cnt.textContent=files.length?files.length+' file'+(files.length>1?'s':'')+' selected':'No files';
  if(el) el.classList.toggle('has-files',files.length>0);
}
document.querySelectorAll('.dz').forEach(z=>{
  z.addEventListener('dragover',e=>{e.preventDefault();z.classList.add('drag-over');});
  z.addEventListener('dragleave',()=>z.classList.remove('drag-over'));
  z.addEventListener('drop',e=>{
    e.preventDefault();z.classList.remove('drag-over');
    const inp=z.querySelector('input[type=file]');if(!inp)return;
    const dt=new DataTransfer();[...e.dataTransfer.files].forEach(f=>dt.items.add(f));
    inp.files=dt.files;updateZone(inp.dataset.zone,inp);
  });
});

// Recon form
document.getElementById('recon-form').addEventListener('submit',async e=>{
  e.preventDefault();
  const gstin=document.getElementById('r-gstin').value.trim().toUpperCase();
  const cname=document.getElementById('r-name').value.trim();
  const fy=document.getElementById('r-fy').value.trim();
  if(!gstin||gstin.length!==15){alert('Enter valid 15-char GSTIN');return;}
  if(!cname){alert('Enter company name');return;}
  const hasFiles=['r1','r2b','r2a','r3b','cust'].some(z=>(zoneFiles[z]||[]).length>0);
  if(!hasFiles){alert('Upload at least one return file');return;}
  const fd=new FormData();
  fd.append('gstin',gstin);fd.append('client_name',cname);
  fd.append('fy',fy);fd.append('mode','recon');
  for(const z of['r1','r2b','r2a','r3b','cust'])(zoneFiles[z]||[]).forEach(f=>fd.append('files_'+z,f));
  await startJob(fd,'r','Generate Reconciliation + GSTR-1 Detail →');
});

// GSTR-1 only form
document.getElementById('g1-form').addEventListener('submit',async e=>{
  e.preventDefault();
  const gstin=document.getElementById('g1-gstin').value.trim().toUpperCase();
  const cname=document.getElementById('g1-name').value.trim();
  const fy=document.getElementById('g1-fy').value.trim();
  if(!gstin||gstin.length!==15){alert('Enter valid 15-char GSTIN');return;}
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

// Auto Download form
document.getElementById('dl-form').addEventListener('submit',async e=>{
  e.preventDefault();
  const gstin=document.getElementById('dl-gstin').value.trim().toUpperCase();
  const fy=document.getElementById('dl-fy').value.trim();
  const username=document.getElementById('dl-user').value.trim();
  const password=document.getElementById('dl-pass').value;
  if(!gstin||gstin.length!==15){alert('Enter valid 15-char GSTIN');return;}
  if(!username){alert('Enter GST Portal username');return;}
  if(!password){alert('Enter GST Portal password');return;}
  const fd=new FormData();
  fd.append('gstin',gstin);fd.append('fy',fy);
  fd.append('username',username);fd.append('password',password);
  await startDownloadJob(fd);
});

async function startJob(fd,prefix,btnLabel){
  document.getElementById(prefix+'-prog').style.display='block';
  const dlEl=document.getElementById(prefix+'-dl');if(dlEl)dlEl.style.display='none';
  document.getElementById(prefix+'-log').innerHTML='';
  document.getElementById(prefix+'-bar').style.width='0%';
  const btn=document.getElementById(prefix+'-submit');
  btn.disabled=true;btn.textContent='Uploading...';
  try{
    const res=await fetch('/api/upload',{method:'POST',body:fd});
    const data=await res.json();
    if(!data.job_id)throw new Error(data.error||'Upload failed');
    addLog(prefix,'info','Files uploaded. Processing...');
    btn.textContent='Processing...';
    document.getElementById('ds-jobid').value=data.job_id;
    pollJob(data.job_id,prefix,btnLabel);
  }catch(err){
    addLog(prefix,'err','Error: '+err.message);
    setBadge(prefix,'err','Failed');
    btn.disabled=false;btn.textContent=btnLabel;
  }
}

async function startDownloadJob(fd){
  const prefix='dl';
  document.getElementById(prefix+'-prog').style.display='block';
  document.getElementById(prefix+'-dl').style.display='none';
  document.getElementById(prefix+'-log').innerHTML='';
  document.getElementById(prefix+'-bar').style.width='0%';
  const btn=document.getElementById(prefix+'-submit');
  btn.disabled=true;btn.textContent='Starting download...';
  try{
    const res=await fetch('/api/download_gst',{method:'POST',body:fd});
    const data=await res.json();
    if(res.status===503){
      // Cloud server - show special message
      addLog(prefix,'warn','--- AUTO-DOWNLOAD NOT AVAILABLE ON CLOUD SERVER ---');
      addLog(prefix,'warn',data.error||'Auto-download requires local installation.');
      addLog(prefix,'info','');
      addLog(prefix,'info','To download returns from GST Portal:');
      addLog(prefix,'info','  1. Download gst_suite_final.py to your PC');
      addLog(prefix,'info','  2. Run RUN_ME.bat');
      addLog(prefix,'info','  3. After downloads, upload the files to this portal for reconciliation');
      setBadge(prefix,'warn','Not Available on Cloud');
      btn.disabled=false;btn.textContent='Start Auto Download →';
      return;
    }
    if(!data.job_id)throw new Error(data.error||'Download failed');
    addLog(prefix,'info','Download job started...');
    btn.textContent='Downloading...';
    pollJob(data.job_id,prefix,'Start Auto Download →');
  }catch(err){
    addLog(prefix,'err','Error: '+err.message);
    setBadge(prefix,'err','Failed');
    btn.disabled=false;btn.textContent='Start Auto Download →';
  }
}

async function pollJob(jobId,prefix,btnLabel){
  try{
    const res=await fetch('/api/job/'+jobId);
    const data=await res.json();
    if(data.logs)data.logs.forEach(l=>addLog(prefix,l.type,l.msg));
    if(data.progress!==undefined)document.getElementById(prefix+'-bar').style.width=data.progress+'%';
    if(data.dl_status&&Object.keys(data.dl_status).length)renderDlStatus(data.dl_status,jobId);
    if(data.status==='done'){
      setBadge(prefix,'done','Complete');
      document.getElementById(prefix+'-bar').style.width='100%';
      const btn=document.getElementById(prefix+'-submit');btn.disabled=false;btn.textContent=btnLabel;
      showDownloads(prefix,jobId,data.files);
      return;
    }
    if(data.status==='error'){
      addLog(prefix,'err','Error: '+(data.error||'Unknown'));
      setBadge(prefix,'err','Failed');
      const btn=document.getElementById(prefix+'-submit');btn.disabled=false;btn.textContent=btnLabel;
      return;
    }
    setTimeout(()=>pollJob(jobId,prefix,btnLabel),1500);
  }catch(e){setTimeout(()=>pollJob(jobId,prefix,btnLabel),3000);}
}

function addLog(prefix,type,msg){
  const box=document.getElementById(prefix+'-log');if(!box)return;
  const l=document.createElement('div');l.className=type;
  l.textContent='['+new Date().toLocaleTimeString()+'] '+msg;
  box.appendChild(l);box.scrollTop=box.scrollHeight;
}
function setBadge(prefix,type,label){
  const b=document.getElementById(prefix+'-badge');if(!b)return;
  b.className='sbadge s-'+type;b.textContent=label;
  if(type!=='proc')b.classList.remove('pulse');
}
function showDownloads(prefix,jobId,files){
  const sec=document.getElementById(prefix+'-dl');
  const grid=document.getElementById(prefix+'-dl-grid');
  if(!sec||!grid)return;
  sec.style.display='block';grid.innerHTML='';
  const icons={'ANNUAL':'📊','GSTR3BR1':'📋','GSTR3BR2A':'📈','GSTR1_FULL':'📑','B2B':'🏢','RECONCIL':'📊','GSTR1':'📋','GSTR2B':'🏦','GSTR2A':'📊','GSTR3B':'📄'};
  files.forEach(f=>{
    const icon=Object.entries(icons).find(([k])=>f.name.toUpperCase().includes(k))?.[1]||'📁';
    const card=document.createElement('div');card.className='dlcard';
    card.innerHTML=`<div style="font-size:1.5rem">${icon}</div>
      <div class="dl-name">${f.name}</div><div class="dl-size">${f.size}</div>
      <a href="/api/download/${jobId}/${encodeURIComponent(f.name)}" class="btn-dl" download>Download ↓</a>`;
    grid.appendChild(card);
  });
}

// Download Status
const MONTHS=['April','May','June','July','August','September','October','November','December','January','February','March'];
const RETS=['GSTR1','GSTR1A','GSTR2B','GSTR2A','GSTR3B'];

function renderDlStatus(dlStatus,jobId){
  const result=document.getElementById('ds-result');
  result.style.display='block';
  document.getElementById('ds-title').textContent=jobId||'—';
  const tbody=document.getElementById('ds-tbody');tbody.innerHTML='';
  let totalOk=0,totalFail=0,totalPend=0;
  MONTHS.forEach(mon=>{
    const tr=document.createElement('tr');
    let rowOk=0,rowFail=0,rowPend=0;
    let td=`<td>${mon}</td>`;
    RETS.forEach(rt=>{
      const val=(dlStatus[mon+'_'+rt]||'SKIP').toUpperCase();
      let cls,txt;
      if(val==='OK'||val==='DONE'){cls='c-ok';txt='✓ OK';rowOk++;totalOk++;}
      else if(['TILE_FAIL','NOT_FOUND','TILE_NOT_FOUND','GEN_FAIL','ERR'].some(t=>val.includes(t)))
        {cls='c-fail';txt='✗ Fail';rowFail++;totalFail++;}
      else if(val==='TRIGGERED'||val==='PENDING')
        {cls='c-pend';txt='⋯';rowPend++;totalPend++;}
      else{cls='c-skip';txt='—';}
      td+=`<td class="${cls}">${txt}</td>`;
    });
    const rs=rowFail>0?`<span style="color:var(--red)">${rowFail} failed</span>`:
              rowOk===5?`<span style="color:var(--green)">All OK</span>`:
              rowOk>0?`<span style="color:var(--orange)">${rowOk}/5 OK</span>`:
              `<span style="color:var(--muted)">—</span>`;
    td+=`<td>${rs}</td>`;
    tr.innerHTML=td;tbody.appendChild(tr);
  });
  document.getElementById('ds-summary').innerHTML=
    `Total: <strong style="color:var(--green)">${totalOk} ✓ OK</strong> &nbsp; `+
    `<strong style="color:var(--red)">${totalFail} ✗ Failed</strong> &nbsp; `+
    `<strong style="color:var(--orange)">${totalPend} ⋯ Pending</strong> &nbsp; `+
    `out of ${MONTHS.length*RETS.length} expected`;
}

async function loadDownloadStatus(){
  const jobId=document.getElementById('ds-jobid').value.trim();
  if(jobId){
    try{
      const res=await fetch('/api/job/'+jobId);
      const data=await res.json();
      if(data.error){alert('Job not found: '+jobId);return;}
      const status=data.dl_status&&Object.keys(data.dl_status).length
        ? data.dl_status : buildStatusFromFiles(data.files||[]);
      renderDlStatus(status,jobId);
    }catch(e){alert('Error: '+e.message);}
    return;
  }
  const masterFiles=(zoneFiles['master']||[]);
  if(!masterFiles.length){alert('Enter a Job ID or upload a Master Report Excel');return;}
  const fd=new FormData();masterFiles.forEach(f=>fd.append('master_file',f));
  try{
    const res=await fetch('/api/parse_master',{method:'POST',body:fd});
    const data=await res.json();
    if(data.dl_status)renderDlStatus(data.dl_status,'Master Report');
    else alert('Parse error: '+(data.error||'Unknown'));
  }catch(e){alert('Upload error: '+e.message);}
}

function buildStatusFromFiles(files){
  const status={};
  MONTHS.forEach(m=>RETS.forEach(r=>{
    const found=files.some(f=>f.name.includes(r)&&f.name.includes(m));
    status[m+'_'+r]=found?'OK':'SKIP';
  }));
  return status;
}
</script>
</body>
</html>"""

# Routes
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/upload", methods=["POST"])
@rate_limit(limit=20, window=60)
def api_upload():
    _cleanup_old_jobs()
    gstin       = request.form.get("gstin","").strip().upper()
    client_name = request.form.get("client_name","").strip()
    fy          = request.form.get("fy","2025-26").strip()
    mode = request.form.get("mode","recon")
    is_full = True

    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN"), 400
    if not client_name:
        return jsonify(error="Client name required"), 400

    job_id  = str(uuid.uuid4())[:8]
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    saved = {k: [] for k in ("r1","r2b","r2a","r3b","cust")}
    for zone in saved:
        for fobj in request.files.getlist(f"files_{zone}"):
            if not fobj.filename: continue
            from werkzeug.utils import secure_filename
            safe_name = secure_filename(fobj.filename) or f"upload_{zone}_{uuid.uuid4().hex[:6]}"
            if Path(safe_name).suffix.lower() not in ALLOWED_EXT: continue
            dest = job_dir / safe_name
            fobj.save(str(dest))
            saved[zone].append(str(dest))

    with jobs_lock:
        jobs[job_id] = {
            "status":"queued","progress":0,"logs":[],"files":[],
            "error":None,"gstin":gstin,"client_name":client_name,
            "fy":fy,"job_dir":str(job_dir),"out_dir":str(out_dir),
            "saved":saved,"is_full":is_full,"mode":mode,"dl_status":{},
        }

    target = run_gstr1_only if mode == "gstr1only" else run_reconciliation
    threading.Thread(target=target, args=(job_id,), daemon=True).start()
    return jsonify(job_id=job_id, is_full=is_full)


@app.route("/api/download_gst", methods=["POST"])
@rate_limit(limit=5, window=300)
def api_download_gst():
    """Start GST Portal auto-download job"""
    _cleanup_old_jobs()

    # Detect if running on cloud server (no DISPLAY, Linux)
    import platform as _pl
    on_server = (_pl.system() == "Linux"
                 and not os.environ.get("DISPLAY","")
                 and not os.environ.get("WAYLAND_DISPLAY",""))

    if on_server:
        return jsonify(
            error=(
                "Auto-download is not available on cloud servers. "
                "The GST Portal requires CAPTCHA which must be entered manually in a browser. "
                "Please use the LOCAL gst_suite_final.py with RUN_ME.bat on your PC instead."
            )
        ), 503

    gstin = request.form.get("gstin", "").strip().upper()
    fy = request.form.get("fy", "2025-26").strip()
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "")
    
    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN"), 400
    if not username or not password:
        return jsonify(error="Username and password required"), 400
    
    job_id = str(uuid.uuid4())[:8]
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    with jobs_lock:
        jobs[job_id] = {
            "status": "queued", "progress": 0, "logs": [], "files": [],
            "error": None, "gstin": gstin, "fy": fy,
            "job_dir": str(job_dir), "out_dir": str(out_dir),
            "dl_status": {}, "mode": "download"
        }
    
    threading.Thread(target=run_gst_download, args=(job_id, gstin, username, password, fy, str(out_dir)), daemon=True).start()
    return jsonify(job_id=job_id)


@app.route("/api/job/<job_id>")
@rate_limit(limit=300, window=60)
def api_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify(error="Job not found"), 404
    new_logs = job["logs"][:]
    job["logs"] = []
    return jsonify(status=job["status"], progress=job["progress"],
                   logs=new_logs, files=job["files"],
                   error=job["error"], is_full=True,
                   dl_status=job.get("dl_status",{}))

@app.route("/api/download/<job_id>/<filename>")
@rate_limit(limit=30, window=60)
def api_download(job_id, filename):
    if not re.match(r'^[\w\-. ()]+\.(xlsx|pdf|zip)$', filename):
        abort(400)
    fpath = OUTPUT_DIR / job_id / filename
    if not fpath.exists() or not fpath.is_file():
        abort(404)
    return send_file(str(fpath), as_attachment=True, download_name=filename)

@app.route("/api/parse_master", methods=["POST"])
@rate_limit(limit=10, window=60)
def api_parse_master():
    fobj = request.files.get("master_file")
    if not fobj:
        return jsonify(error="No file"), 400
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
        def _col(*names):
            for n in names:
                if n in col: return col[n]
            return -1
        MONTH_C  = _col("MONTH","PERIOD","MONTH YEAR")
        R1_C     = _col("GSTR-1","GSTR1","R1")
        R1A_C    = _col("GSTR-1A","GSTR1A","R1A")
        R2B_C    = _col("GSTR-2B","GSTR2B","R2B")
        R2A_C    = _col("GSTR-2A","GSTR2A","R2A")
        R3B_C    = _col("GSTR-3B","GSTR3B","R3B")
        dl_status = {}
        for row in rows[1:]:
            try:
                if MONTH_C < 0: continue
                month_raw = str(row[MONTH_C] or "").strip()
                if not month_raw or month_raw.lower() in ("none","nan",""): continue
                mon = month_raw.split()[0]
                def _st(idx):
                    if idx < 0 or idx >= len(row): return "SKIP"
                    return str(row[idx] or "SKIP").strip().upper()
                dl_status[f"{mon}_GSTR1"]  = _st(R1_C)
                dl_status[f"{mon}_GSTR1A"] = _st(R1A_C)
                dl_status[f"{mon}_GSTR2B"] = _st(R2B_C)
                dl_status[f"{mon}_GSTR2A"] = _st(R2A_C)
                dl_status[f"{mon}_GSTR3B"] = _st(R3B_C)
            except: continue
        wb.close()
        return jsonify(dl_status=dl_status)
    except Exception as e:
        return jsonify(error=str(e)), 500
    finally:
        try: tmp.unlink(missing_ok=True)
        except: pass


# ── Month helpers ─────────────────────────────────────────────────
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
    return {"April":str(s),"May":str(s),"June":str(s),"July":str(s),
            "August":str(s),"September":str(s),"October":str(s),"November":str(s),
            "December":str(s),"January":str(e),"February":str(e),"March":str(e)}

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
                        fp = re.sub(r'[^0-9]','', d.get("fp",""))
                        if len(fp) == 6:
                            mon = MONTHS_MAP.get(fp[:2])
                            if mon: return mon, fp[2:]
    except: pass
    return None, None

def _find_engine(name):
    for loc in [
        Path(__file__).parent / name,
        Path(os.getcwd()) / name,
        Path(os.path.expanduser("~")) / "Desktop" / name,
        Path(os.path.expanduser("~")) / "Downloads" / name,
    ]:
        if loc.exists(): return loc
    return None


# ── Worker: GST Portal Download ───────────────────────────────────
def run_gst_download(job_id, gstin, username, password, fy, out_dir):
    """Download GST returns from portal"""
    def log(msg, t="info"):
        with jobs_lock: jobs[job_id]["logs"].append({"type":t,"msg":msg})
    def prog(p):
        with jobs_lock: jobs[job_id]["progress"] = p
    def set_dl(key, val):
        with jobs_lock: jobs[job_id]["dl_status"][key] = val
    
    try:
        log(f"Starting GST Portal download for {gstin}, FY {fy}")
        prog(5)
        
        # Try to import gst_downloader
        try:
            from gst_downloader import GSTPortalDownloader
        except ImportError as e:
            log(f"GST Downloader not available: {e}", "warn")
            log("Please install selenium: pip install selenium", "warn")
            raise RuntimeError("Auto-download requires selenium. Install with: pip install selenium")
        
        prog(10)
        downloader = GSTPortalDownloader(username, password, out_dir)
        
        log("Logging into GST Portal...")
        prog(15)
        
        if not downloader.login():
            raise RuntimeError("GST Portal login failed. Check credentials and CAPTCHA.")
        
        log("Login successful! Starting downloads...")
        prog(20)
        
        # Generate periods
        periods = downloader._get_periods_for_fy(fy)
        total_periods = len(periods)
        downloaded_files = []
        
        for idx, (month_name, period_code, fp) in enumerate(periods):
            progress = 20 + int((idx / total_periods) * 70)
            prog(progress)
            
            log(f"Processing {month_name}...")
            
            # Download GSTR-1
            try:
                file = downloader.download_gstr1(gstin, period_code, fp)
                if file:
                    downloaded_files.append({"name": file.name, "size": f"{file.stat().st_size//1024} KB"})
                    set_dl(f"{month_name}_GSTR1", "OK")
                    log(f"  ✓ GSTR-1 downloaded: {file.name}")
                else:
                    set_dl(f"{month_name}_GSTR1", "NOT_FOUND")
            except Exception as e:
                log(f"  ✗ GSTR-1 failed: {e}", "err")
                set_dl(f"{month_name}_GSTR1", "FAIL")
            
            # Download GSTR-2B
            try:
                file = downloader.download_gstr2b(gstin, period_code)
                if file:
                    downloaded_files.append({"name": file.name, "size": f"{file.stat().st_size//1024} KB"})
                    set_dl(f"{month_name}_GSTR2B", "OK")
                    log(f"  ✓ GSTR-2B downloaded: {file.name}")
                else:
                    set_dl(f"{month_name}_GSTR2B", "NOT_FOUND")
            except Exception as e:
                log(f"  ✗ GSTR-2B failed: {e}", "err")
                set_dl(f"{month_name}_GSTR2B", "FAIL")
            
            # Download GSTR-2A
            try:
                file = downloader.download_gstr2a(gstin, period_code)
                if file:
                    downloaded_files.append({"name": file.name, "size": f"{file.stat().st_size//1024} KB"})
                    set_dl(f"{month_name}_GSTR2A", "OK")
                    log(f"  ✓ GSTR-2A downloaded: {file.name}")
                else:
                    set_dl(f"{month_name}_GSTR2A", "NOT_FOUND")
            except Exception as e:
                log(f"  ✗ GSTR-2A failed: {e}", "err")
                set_dl(f"{month_name}_GSTR2A", "FAIL")
            
            # Download GSTR-3B
            try:
                file = downloader.download_gstr3b(gstin, period_code)
                if file:
                    downloaded_files.append({"name": file.name, "size": f"{file.stat().st_size//1024} KB"})
                    set_dl(f"{month_name}_GSTR3B", "OK")
                    log(f"  ✓ GSTR-3B downloaded: {file.name}")
                else:
                    set_dl(f"{month_name}_GSTR3B", "NOT_FOUND")
            except Exception as e:
                log(f"  ✗ GSTR-3B failed: {e}", "err")
                set_dl(f"{month_name}_GSTR3B", "FAIL")
        
        downloader.close()
        prog(100)
        
        log(f"Download complete! {len(downloaded_files)} files downloaded.", "ok")
        
        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"] = downloaded_files
            
    except Exception as exc:
        import traceback
        log(f"Error: {exc}", "err")
        for line in traceback.format_exc().split('\n'):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(exc)


# ── Worker: Reconciliation + GSTR-1 detail ────────────────────────
def run_reconciliation(job_id):
    def log(msg, t="info"):
        with jobs_lock: jobs[job_id]["logs"].append({"type":t,"msg":msg})
    def prog(p):
        with jobs_lock: jobs[job_id]["progress"] = p
    def set_dl(key, val):
        with jobs_lock: jobs[job_id]["dl_status"][key] = val

    try:
        job         = jobs[job_id]
        gstin       = job["gstin"]
        client_name = job["client_name"]
        fy          = job["fy"]
        job_dir     = Path(job["job_dir"])
        out_dir     = Path(job["out_dir"])
        saved       = job["saved"]
        is_full     = True
        FY_MONTHS   = _fy_months(fy)

        log(f"Reconciliation: {client_name} ({gstin}) FY {fy}")
        log("⭐ FULL ACCESS — All features enabled")
        prog(5)

        # Rename files to standard names & update dl_status
        log("Renaming uploaded files...")
        for fpath in saved["r1"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1: {mon} {yr}"); set_dl(f"{mon}_GSTR1", "OK")
            else:
                log(f"  ⚠ Cannot detect month: {Path(fpath).name}", "warn")

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
                ext  = Path(fpath).suffix.lower()
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

        prog(25)

        # Load reconciliation engine
        suite_path = _find_engine("gst_suite_final.py")
        if not suite_path:
            raise FileNotFoundError("gst_suite_final.py not found. Place it alongside app.py.")

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
        # Required by separate reconciled workbooks (GSTR3BR1, GSTR3BR2A)
        gst.FY_MONTHS = [
            f"April {s}",f"May {s}",f"June {s}",f"July {s}",
            f"August {s}",f"September {s}",f"October {s}",f"November {s}",
            f"December {s}",f"January {e}",f"February {e}",f"March {e}",
        ]

        _log = _lg.getLogger(f"gst_web_{job_id}")
        _log.setLevel(_lg.DEBUG)
        class WL(_lg.Handler):
            def emit(self, r):
                log(self.format(r), "err" if r.levelno >= _lg.WARNING else "info")
        _log.addHandler(WL())

        prog(30)
        log("Running annual reconciliation...")
        gst.write_annual_reconciliation(str(job_dir), client_name, gstin, _log)
        prog(65)

        # GSTR-1 Full Detail — always runs when ZIPs are present
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
                log(f"  ✓ GSTR-1 detail saved: {out_xl.name}", "ok")
            except Exception as e:
                log(f"  ⚠ GSTR-1 extraction error: {e}", "warn")
        elif not extract_path:
            log("⚠ gstr1_extract.py not found — GSTR-1 detail skipped", "warn")
        elif not gstr1_zips:
            log("ℹ No GSTR-1 ZIPs uploaded — GSTR-1 detail skipped")

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
            raise RuntimeError("No output files generated. Check that uploaded files contain valid data.")

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready.", "ok")
        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"]  = output_files
        _cleanup_job_files(job_id)

    except Exception as exc:
        import traceback
        log(f"Error: {exc}", "err")
        for line in traceback.format_exc().split('\n'):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)
        _cleanup_job_files(job_id)


# ── Worker: GSTR-1 Detail only (Tab 2) ───────────────────────────
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

        log(f"GSTR-1 Full Detail: {client_name} FY {fy}")
        prog(5)

        log("Renaming GSTR-1 ZIPs to standard names...")
        for fpath in saved["r1"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  ✓ GSTR-1: {mon} {yr}")
            else:
                log(f"  ⚠ Cannot detect month: {Path(fpath).name}", "warn")

        # Copy 2B/2A for name lookup
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
            log("  Customer names loaded"); break

        prog(20)
        gstr1_zips = list(job_dir.glob("GSTR1_*.zip"))
        if not gstr1_zips:
            raise RuntimeError(
                "No GSTR1_*.zip files found after renaming. "
                "Make sure filenames contain the month name (e.g. GSTR1_April_2025.zip).")

        extract_path = _find_engine("gstr1_extract.py")
        if not extract_path:
            raise FileNotFoundError("gstr1_extract.py not found. Place it alongside app.py.")

        log(f"Starting extraction ({len(gstr1_zips)} months found)...")
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
        _cleanup_job_files(job_id)

    except Exception as exc:
        import traceback
        log(f"Error: {exc}", "err")
        for line in traceback.format_exc().split('\n'):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)
        _cleanup_job_files(job_id)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n  ============================================================")
    print(f"   GST Reconciliation Portal v4 — FULL ACCESS")
    print(f"  ============================================================")
    print(f"   Upload dir    : {UPLOAD_DIR}")
    print(f"   Output dir    : {OUTPUT_DIR}")
    suite = _find_engine("gst_suite_final.py")
    ext   = _find_engine("gstr1_extract.py")
    dl    = _find_engine("gst_downloader.py")
    print(f"   Suite engine  : {suite or '⚠  NOT FOUND'}")
    print(f"   GSTR-1 engine : {ext   or '⚠  NOT FOUND'}")
    print(f"   Downloader    : {dl    or '⚠  NOT FOUND'}")
    print(f"\n   Open: http://localhost:{port}")
    print(f"  ============================================================\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
