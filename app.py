"""
GST Reconciliation Web App — v6 with Auto Download
===================================================
• Fully free — no license, no restrictions
• Scripts (gst_suite_final.py, gstr1_extract.py) never exposed to users
• 4 tabs: Reconciliation | GSTR-1 Detail | Download Status | Auto Download
• NEW: Download directly from GST portal using PC browser bridge
• Render.com ready — binds to $PORT
"""

import os, sys, json, zipfile, re, time, shutil, uuid, threading, asyncio
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, send_file, render_template_string, abort
import tempfile, platform

# ── HTTP long-poll bridge (no WebSocket needed) ───────────────────
WEBSOCKET_AVAILABLE = True   # always True — uses plain HTTP polling

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
FEEDBACK_FILE = Path(__file__).parent / "feedback.json"
ALLOWED_EXT = {".zip", ".xlsx", ".xls", ".pdf", ".json"}
MAX_FILE_MB = 100
JOB_TTL_S   = 7200

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024

# (No WebSocket init needed — using HTTP long-poll)

jobs      = {}
jobs_lock = threading.Lock()

# ── HTTP Bridge State ─────────────────────────────────────────────
# cmd_queue  : server → PC  (commands waiting to be picked up)
# resp_queue : PC → server  (responses from PC browser)
import queue as _queue
_cmd_queue  = _queue.Queue()
_resp_queue = _queue.Queue()
_bridge_last_seen = 0       # epoch seconds of last PC poll
_bridge_lock = threading.Lock()

def _bridge_connected():
    """True if PC polled within last 8 seconds"""
    return (time.time() - _bridge_last_seen) < 8

# ── Rate limiting ─────────────────────────────────────────────────
_rate = {}
_rate_lock = threading.Lock()

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

# ── Feedback store ────────────────────────────────────────────────
def _load_feedback():
    try:
        if FEEDBACK_FILE.exists():
            return json.loads(FEEDBACK_FILE.read_text(encoding="utf-8"))
    except: pass
    return []

def _save_feedback(fb_list):
    try:
        FEEDBACK_FILE.write_text(json.dumps(fb_list, ensure_ascii=False, indent=2), encoding="utf-8")
    except: pass

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
    for loc in [
        Path(__file__).parent / name,
        Path(os.getcwd()) / name,
        Path(os.path.expanduser("~")) / "Desktop" / name,
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
                        fp = re.sub(r'[^0-9]','', d.get("fp",""))
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
# HTML
# ═══════════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>GST Reconciliation Portal — Free</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#0a0e1a;--surf:#111827;--surf2:#1a2235;--bdr:#1e3050;
  --accent:#00e5ff;--accent2:#7c3aed;--grn:#00e676;--org:#ff6d00;
  --red:#ff1744;--txt:#e8edf5;--muted:#6b7fa3;
  --mono:'IBM Plex Mono',monospace;--sans:'Syne',sans-serif;
}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;
  background-image:linear-gradient(rgba(0,229,255,.04) 1px,transparent 1px),
  linear-gradient(90deg,rgba(0,229,255,.04) 1px,transparent 1px);
  background-size:40px 40px;pointer-events:none;z-index:0}
.wrap{max-width:1000px;margin:0 auto;padding:2rem 1.5rem;position:relative;z-index:1}

/* Header */
header{text-align:center;padding:2rem 0 1.25rem}
.logo{display:inline-flex;align-items:center;gap:.7rem;margin-bottom:.8rem}
.logo-icon{width:46px;height:46px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.4rem}
.logo-text{font-size:1rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
h1{font-size:clamp(1.5rem,3.2vw,2.2rem);font-weight:800;letter-spacing:-.02em;line-height:1.1}
h1 span{background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.sub{color:var(--muted);font-size:.82rem;margin-top:.35rem;font-family:var(--mono)}
.badges{display:flex;gap:.5rem;justify-content:center;flex-wrap:wrap;margin-top:.6rem}
.badge{display:inline-flex;align-items:center;gap:.3rem;padding:.28rem .8rem;border-radius:100px;
  font-size:.7rem;font-weight:700;font-family:var(--mono)}
.badge-grn{background:rgba(0,230,118,.15);color:var(--grn);border:1px solid rgba(0,230,118,.4)}
.badge-blue{background:rgba(0,229,255,.1);color:var(--accent);border:1px solid rgba(0,229,255,.3)}
.badge-purple{background:rgba(124,58,237,.15);color:#a78bfa;border:1px solid rgba(124,58,237,.3)}
.badge-orange{background:rgba(255,109,0,.15);color:var(--org);border:1px solid rgba(255,109,0,.4)}

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
  padding:1.4rem;margin-bottom:1rem;transition:border-color .2s}
.card:hover{border-color:rgba(0,229,255,.15)}
.ct{font-size:.8rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--accent);margin-bottom:.9rem;display:flex;align-items:center;gap:.45rem}
.ct::before{content:'';width:3px;height:1em;background:var(--accent);border-radius:2px}

/* Form */
.fg2{display:grid;grid-template-columns:1fr 1fr;gap:.75rem}
@media(max-width:600px){.fg2{grid-template-columns:1fr}}
.fg{display:flex;flex-direction:column;gap:.3rem}
label{font-size:.68rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
input[type=text],input[type=password],textarea,select{
  background:var(--surf2);border:1px solid var(--bdr);border-radius:7px;
  padding:.52rem .78rem;color:var(--txt);font-family:var(--mono);font-size:.82rem;
  transition:border-color .2s;width:100%}
textarea{resize:vertical;min-height:90px;line-height:1.55}
input:focus,textarea:focus,select:focus{outline:none;border-color:var(--accent)}
input::placeholder,textarea::placeholder{color:var(--muted)}
select option{background:var(--surf)}

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
.btn-sec{width:100%;padding:.65rem;background:var(--surf2);border:1px solid var(--accent);
  border-radius:9px;color:var(--accent);font-family:var(--sans);font-size:.82rem;
  font-weight:700;cursor:pointer;transition:all .15s;margin-top:.4rem}
.btn-sec:hover{background:rgba(0,229,255,.08)}
.btn-orange{width:100%;padding:.8rem;background:linear-gradient(135deg,var(--org),#ff9100);
  border:none;border-radius:10px;color:#000;font-family:var(--sans);font-size:.88rem;
  font-weight:800;letter-spacing:.05em;text-transform:uppercase;cursor:pointer;
  transition:transform .15s,box-shadow .15s;margin-top:.3rem}
.btn-orange:hover{transform:translateY(-2px);box-shadow:0 8px 24px rgba(255,109,0,.25)}
.btn-orange:disabled{opacity:.4;cursor:not-allowed;transform:none}

/* Progress */
.pw{display:none}
.pb-w{background:var(--surf2);border-radius:100px;height:5px;overflow:hidden;margin:.65rem 0}
.pb{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));
  border-radius:100px;transition:width .4s;width:0%}
.lb{background:#000;border:1px solid var(--bdr);border-radius:7px;
  padding:.75rem;font-family:var(--mono);font-size:.7rem;height:160px;overflow-y:auto;
  color:#aaffcc;line-height:1.7}
.lb .err{color:#ff6b6b}.lb .info{color:var(--accent)}
.lb .ok{color:var(--grn)}.lb .warn{color:var(--org)}

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
.sbg{display:inline-flex;align-items:center;gap:.25rem;padding:.2rem .55rem;
  border-radius:100px;font-size:.64rem;font-weight:700;font-family:var(--mono)}
.s-p{background:rgba(255,109,0,.15);color:var(--org);border:1px solid rgba(255,109,0,.4)}
.s-d{background:rgba(0,230,118,.15);color:var(--grn);border:1px solid rgba(0,230,118,.4)}
.s-e{background:rgba(255,23,68,.15);color:var(--red);border:1px solid rgba(255,23,68,.4)}
.s-w{background:rgba(0,229,255,.15);color:var(--accent);border:1px solid rgba(0,229,255,.4)}
.pulse{animation:pulse 1.2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* Status table */
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

/* Pills */
.pills{display:flex;flex-wrap:wrap;gap:.35rem;margin-bottom:.75rem}
.pill{padding:.22rem .6rem;background:var(--surf2);border:1px solid var(--bdr);
  border-radius:100px;font-size:.64rem;color:var(--muted);font-family:var(--mono)}

/* Info box */
.info-box{background:rgba(0,229,255,.05);border:1px solid rgba(0,229,255,.18);
  border-radius:9px;padding:.85rem 1rem;margin-bottom:.9rem;
  font-size:.78rem;color:var(--muted);line-height:1.65}
.info-box strong{color:var(--txt)}
.info-box.warn{background:rgba(255,109,0,.05);border-color:rgba(255,109,0,.18)}
.info-box.success{background:rgba(0,230,118,.05);border-color:rgba(0,230,118,.18)}

/* Connection status */
.conn-status{display:flex;align-items:center;gap:.5rem;padding:.5rem .75rem;
  background:var(--surf2);border:1px solid var(--bdr);border-radius:8px;
  font-size:.72rem;font-family:var(--mono);margin-bottom:1rem}
.conn-dot{width:8px;height:8px;border-radius:50%;background:var(--red)}
.conn-dot.online{background:var(--grn);box-shadow:0 0 8px var(--grn)}
.conn-dot.connecting{background:var(--org);animation:pulse 1s infinite}

/* Feedback section */
.fb-card{background:var(--surf);border:1px solid var(--bdr);border-radius:13px;
  padding:1.4rem;margin-top:2rem;margin-bottom:2rem}
.fb-list{margin-top:1rem;display:flex;flex-direction:column;gap:.65rem;max-height:380px;overflow-y:auto}
.fb-item{background:var(--surf2);border:1px solid var(--bdr);border-radius:9px;padding:.85rem}
.fb-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:.4rem;flex-wrap:wrap;gap:.3rem}
.fb-name{font-size:.8rem;font-weight:700;color:var(--txt)}
.fb-type{font-size:.65rem;font-family:var(--mono);padding:.2rem .55rem;border-radius:100px}
.fb-bug{background:rgba(255,23,68,.12);color:var(--red);border:1px solid rgba(255,23,68,.3)}
.fb-sugg{background:rgba(0,229,255,.1);color:var(--accent);border:1px solid rgba(0,229,255,.25)}
.fb-praise{background:rgba(0,230,118,.1);color:var(--grn);border:1px solid rgba(0,230,118,.25)}
.fb-other{background:rgba(255,109,0,.1);color:var(--org);border:1px solid rgba(255,109,0,.25)}
.fb-time{font-size:.62rem;color:var(--muted);font-family:var(--mono)}
.fb-msg{font-size:.8rem;color:var(--muted);line-height:1.6;margin-top:.25rem}
.stars{color:#ffd700;font-size:.85rem;letter-spacing:.05rem}
.no-fb{text-align:center;color:var(--muted);font-size:.8rem;padding:1.5rem;font-family:var(--mono)}

/* Footer */
footer{text-align:center;padding:1.5rem 0 2rem;color:var(--muted);font-size:.72rem;font-family:var(--mono)}
footer a{color:var(--accent);text-decoration:none}
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
  <p class="sub">Upload your returns → Get reconciliation Excel in seconds</p>
  <div class="badges">
    <span class="badge badge-grn">⭐ 100% Free</span>
    <span class="badge badge-blue">📊 Full Reconciliation</span>
    <span class="badge badge-purple">🔒 Your files stay private</span>
    <span class="badge badge-orange">🌐 Auto Download NEW</span>
  </div>
</header>

<!-- TABS -->
<div class="tabs">
  <button class="tb active" onclick="switchTab('recon',event)">📊 Reconciliation</button>
  <button class="tb" onclick="switchTab('gstr1',event)">📋 GSTR-1 Detail</button>
  <button class="tb" onclick="switchTab('dlstatus',event)">🔄 Download Status</button>
  <button class="tb" onclick="switchTab('autodl',event)">🌐 Auto Download</button>
</div>

<!-- ══ TAB 1: RECONCILIATION ══ -->
<div class="tp active" id="tab-recon">

<div class="info-box">
  <strong>How it works:</strong> Download your GST return files using <strong>RUN_ME.bat</strong> on your PC,
  then upload them here. The portal generates a full
  <strong>Annual Reconciliation Excel</strong> (7 sheets: Summary, GSTR-1 Sales, GSTR-2B ITC,
  GSTR-2A Purchases, GSTR-3B Status, R1 vs 3B Recon, Tax Liability) plus a separate
  <strong>GSTR-1 Full Detail Excel</strong> (13 sheets) — instantly.
  <br><br>
  <strong>Files are auto-deleted after 2 hours. Nothing is stored permanently.</strong>
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
      <div class="dz-ht">ZIP files (12 months)</div>
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
      <div class="dz-ht">GSTIN → Name Excel</div>
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
  <p style="color:var(--muted);font-size:.66rem;margin-top:.65rem;font-family:var(--mono)">
    ⏳ Files deleted automatically after 2 hours. Download before closing.
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
    Upload all 12 GSTR-1 ZIP files. Customer names are auto-looked up from GSTR-2B/2A files.
    Add <strong style="color:var(--txt)">customer_names.xlsx</strong> (GSTIN + Name columns) for manual lookup.
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
      <div class="dz-ht">For name lookup</div>
      <div class="dz-cn" id="cnt-g1r2b">No files</div>
      <input type="file" multiple accept=".xlsx,.zip" data-zone="g1r2b" onchange="updateZone('g1r2b',this)">
    </div>
    <div class="dz" id="zone-g1cust">
      <div class="dz-ic">👥</div><div class="dz-lb">Customer Names</div>
      <div class="dz-ht">GSTIN → Name Excel</div>
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
  <p style="color:var(--muted);font-size:.78rem;line-height:1.6;margin-bottom:.8rem">
    After running <strong style="color:var(--txt)">RUN_ME.bat</strong> on your PC,
    upload the <strong style="color:var(--txt)">MASTER_REPORT_*.xlsx</strong> file here
    to see which returns downloaded successfully. Or paste a live Job ID from Tab 1.
  </p>
  <div class="fg2" style="margin-bottom:.75rem">
    <div class="fg">
      <label>Live Job ID (from Reconciliation tab)</label>
      <input type="text" id="ds-jid" placeholder="e.g. a3f2c9b1">
    </div>
    <div class="fg">
      <label>Upload MASTER_REPORT Excel</label>
      <div class="dz" id="zone-master"
           style="min-height:60px;flex-direction:row;padding:.6rem .75rem;gap:.65rem">
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
  <div id="ds-sum" style="margin-top:.55rem;font-size:.7rem;font-family:var(--mono);color:var(--muted)"></div>
</div>
<div class="card pw" id="ds-pw">
  <div class="ct">Job Progress <span class="sbg s-p pulse" id="ds-badge">Running</span></div>
  <div class="pb-w"><div class="pb" id="ds-pb"></div></div>
  <div class="lb" id="ds-lb"></div>
</div>
</div><!-- /tab-dlstatus -->

<!-- ══ TAB 4: AUTO DOWNLOAD ══ -->
<div class="tp" id="tab-autodl">

<div class="card">
  <div class="ct">🌐 Auto Download from GST Portal</div>
  <div class="pills">
    <span class="pill">GSTR-1</span><span class="pill">GSTR-2B</span>
    <span class="pill">GSTR-2A</span><span class="pill">GSTR-3B</span>
  </div>
  <p style="color:var(--muted);font-size:.78rem;line-height:1.65;margin-bottom:.5rem">
    Enter your GST credentials below. The server opens the GST portal, fills your login,
    then shows you the CAPTCHA image here — you type it and the rest downloads automatically.
    <strong style="color:var(--txt)">No software to install on your PC.</strong>
  </p>
</div>

<!-- Step 1: Credentials form -->
<div id="ad-step1">
<form id="ad-form">
<div class="card">
  <div class="ct">GST Portal Credentials</div>
  <div class="fg2">
    <div class="fg"><label>GSTIN *</label>
      <input type="text" id="ad-gstin" placeholder="33ABCDE1234F1ZX" maxlength="15" required></div>
    <div class="fg"><label>Company Name *</label>
      <input type="text" id="ad-name" placeholder="ABC Traders" required></div>
    <div class="fg"><label>Username *</label>
      <input type="text" id="ad-username" placeholder="Your GST portal username" required></div>
    <div class="fg"><label>Password *</label>
      <input type="password" id="ad-password" placeholder="Your GST portal password" required></div>
    <div class="fg"><label>Financial Year</label>
      <select id="ad-fy">
        <option value="2025-26">2025-26</option>
        <option value="2024-25">2024-25</option>
        <option value="2023-24">2023-24</option>
        <option value="2022-23">2022-23</option>
      </select></div>
    <div class="fg"><label>Returns to Download</label>
      <select id="ad-returns">
        <option value="all">All Returns (GSTR-1, 2B, 2A, 3B)</option>
        <option value="gstr1">GSTR-1 Only</option>
        <option value="gstr2b">GSTR-2B Only</option>
        <option value="gstr3b">GSTR-3B Only</option>
      </select></div>
  </div>
</div>
<div class="card">
  <button type="submit" class="btn-orange" id="ad-submit">🚀 Start Auto Download</button>
</div>
</form>
</div><!-- /ad-step1 -->

<!-- Step 2: CAPTCHA entry (hidden until server is ready) -->
<div id="ad-step2" style="display:none">
<div class="card">
  <div class="ct">🔐 Type the CAPTCHA to Continue</div>
  <p style="color:var(--muted);font-size:.8rem;margin-bottom:.9rem">
    The GST portal login page is open on the server. Type the CAPTCHA characters shown below, then click Submit.
  </p>
  <div style="margin-bottom:1rem;text-align:center">
    <img id="ad-captcha-img" src="" alt="CAPTCHA"
         style="border:2px solid var(--accent);border-radius:8px;max-width:220px;background:#fff;padding:4px">
    <br>
    <button type="button" onclick="refreshCaptcha()"
            style="margin-top:.5rem;background:none;border:1px solid var(--bdr);border-radius:5px;
                   color:var(--muted);font-size:.7rem;padding:.3rem .7rem;cursor:pointer">
      🔄 Refresh CAPTCHA
    </button>
  </div>
  <div class="fg" style="max-width:260px;margin:0 auto">
    <label>CAPTCHA Text *</label>
    <input type="text" id="ad-captcha-input" placeholder="Type characters above"
           autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false"
           style="font-size:1.1rem;letter-spacing:.15em;text-align:center">
  </div>
  <div style="max-width:260px;margin:.75rem auto 0">
    <button type="button" class="btn" id="ad-captcha-submit" onclick="submitCaptcha()">
      Submit CAPTCHA &amp; Login →
    </button>
  </div>
</div>
</div><!-- /ad-step2 -->

<!-- Progress & logs -->
<div class="card pw" id="ad-pw">
  <div class="ct">Progress <span class="sbg s-p pulse" id="ad-badge">Running</span></div>
  <div class="pb-w"><div class="pb" id="ad-pb"></div></div>
  <div class="lb" id="ad-lb"></div>
</div>

<!-- Download results -->
<div class="card dw" id="ad-dw">
  <div class="ct">Downloaded Files</div>
  <div class="dl-g" id="ad-dlg"></div>
  <p style="color:var(--muted);font-size:.7rem;margin-top:.65rem;font-family:var(--mono)">
    ⬇️ Click Download to save each file. Then upload them to the Reconciliation tab.
  </p>
</div>

</div><!-- /tab-autodl -->

<!-- ══ FEEDBACK SECTION ══ -->
<div class="fb-card" id="feedback-section">
  <div class="ct">💬 Feedback &amp; Comments</div>
  <p style="color:var(--muted);font-size:.78rem;line-height:1.6;margin-bottom:1rem">
    This portal is in <strong style="color:var(--txt)">public beta</strong> — completely free.
    Found a bug? Have a suggestion? We'd love to hear from you.
    Your feedback helps improve this tool for all GST practitioners in India.
  </p>

  <form id="fb-form">
  <div class="fg2" style="margin-bottom:.65rem">
    <div class="fg">
      <label>Your Name</label>
      <input type="text" id="fb-name" placeholder="CA Rajesh Kumar (optional)">
    </div>
    <div class="fg">
      <label>Feedback Type</label>
      <select id="fb-type">
        <option value="bug">🐛 Bug Report</option>
        <option value="suggestion" selected>💡 Suggestion</option>
        <option value="praise">👍 Works Great!</option>
        <option value="other">💬 Other</option>
      </select>
    </div>
  </div>
  <div class="fg" style="margin-bottom:.65rem">
    <label>Rating</label>
    <div style="display:flex;gap:.5rem;margin-top:.25rem" id="star-row">
      <span class="star-btn" data-val="1" onclick="setRating(1)" style="font-size:1.4rem;cursor:pointer;opacity:.4">★</span>
      <span class="star-btn" data-val="2" onclick="setRating(2)" style="font-size:1.4rem;cursor:pointer;opacity:.4">★</span>
      <span class="star-btn" data-val="3" onclick="setRating(3)" style="font-size:1.4rem;cursor:pointer;opacity:.4">★</span>
      <span class="star-btn" data-val="4" onclick="setRating(4)" style="font-size:1.4rem;cursor:pointer;opacity:.4">★</span>
      <span class="star-btn" data-val="5" onclick="setRating(5)" style="font-size:1.4rem;cursor:pointer;opacity:.4">★</span>
      <span id="rating-lbl" style="font-size:.72rem;color:var(--muted);font-family:var(--mono);align-self:center;margin-left:.35rem"></span>
    </div>
  </div>
  <div class="fg" style="margin-bottom:.75rem">
    <label>Your Comment *</label>
    <textarea id="fb-msg" placeholder="e.g. GSTR-2B extraction is not matching for April month..." required></textarea>
  </div>
  <button type="submit" class="btn-sec" id="fb-submit">Submit Feedback →</button>
  <div id="fb-status" style="font-size:.72rem;margin-top:.45rem;font-family:var(--mono)"></div>
  </form>

  <!-- Existing feedback -->
  <div style="margin-top:1.4rem">
    <div style="font-size:.78rem;font-weight:700;color:var(--accent);text-transform:uppercase;
                letter-spacing:.06em;margin-bottom:.65rem">
      Recent Comments (<span id="fb-count">0</span>)
    </div>
    <div class="fb-list" id="fb-list">
      <div class="no-fb">No comments yet. Be the first!</div>
    </div>
  </div>
</div>

<footer>
  GST Reconciliation Portal — Public Beta &nbsp;|&nbsp;
  Built for Indian CA firms &nbsp;|&nbsp;
  100% Free · No data stored permanently<br>
  <span style="color:rgba(107,127,163,.5)">Scripts run on server only — your code is never shared</span>
</footer>

</div><!-- /wrap -->

<script>
// ── Tab switching ─────────────────────────────────────────────────
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
  
  // Check connection status when switching to autodl tab
  if(name==='autodl') checkBrowserConnection();
}

// ── File zones ────────────────────────────────────────────────────
const zoneFiles={};
function updateZone(zone, inp){
  const files=Array.from(inp.files);
  zoneFiles[zone]=files;
  const cn=document.getElementById('cnt-'+zone);
  const el=document.getElementById('zone-'+zone);
  if(cn) cn.textContent=files.length?files.length+' file'+(files.length>1?'s':'')+' selected':'No files';
  if(el) el.classList.toggle('has-files',files.length>0);
}
document.querySelectorAll('.dz').forEach(z=>{
  z.addEventListener('dragover',e=>{e.preventDefault();z.classList.add('drag-over');});
  z.addEventListener('dragleave',()=>z.classList.remove('drag-over'));
  z.addEventListener('drop',e=>{
    e.preventDefault();z.classList.remove('drag-over');
    const inp=z.querySelector('input[type=file]');if(!inp) return;
    const dt=new DataTransfer();
    [...e.dataTransfer.files].forEach(f=>dt.items.add(f));
    inp.files=dt.files; updateZone(inp.dataset.zone,inp);
  });
});

// ── Recon form ────────────────────────────────────────────────────
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

// ── GSTR-1 form ───────────────────────────────────────────────────
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

// ── Generic job runner ────────────────────────────────────────────
async function startJob(fd, pfx, btnLbl){
  document.getElementById(pfx+'-pw').style.display='block';
  const dw=document.getElementById(pfx+'-dw');if(dw)dw.style.display='none';
  document.getElementById(pfx+'-lb').innerHTML='';
  document.getElementById(pfx+'-pb').style.width='0%';
  const btn=document.getElementById(pfx+'-submit');
  btn.disabled=true;btn.textContent='Uploading...';
  try{
    const res=await fetch('/api/upload',{method:'POST',body:fd});
    const d=await res.json();
    if(!d.job_id) throw new Error(d.error||'Upload failed');
    addLog(pfx,'info','Files uploaded. Processing started...');
    btn.textContent='Processing...';
    document.getElementById('ds-jid').value=d.job_id;
    pollJob(d.job_id,pfx,btnLbl);
  }catch(err){
    addLog(pfx,'err','Error: '+err.message);
    setBadge(pfx,'e','Failed');
    btn.disabled=false;btn.textContent=btnLbl;
  }
}

async function pollJob(jid,pfx,btnLbl){
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
      btn.disabled=false;btn.textContent=btnLbl;
      showDownloads(pfx,jid,d.files);
      return;
    }
    if(d.status==='error'){
      addLog(pfx,'err','Error: '+(d.error||'Unknown error'));
      setBadge(pfx,'e','Failed');
      const btn=document.getElementById(pfx+'-submit');
      btn.disabled=false;btn.textContent=btnLbl;
      return;
    }
    setTimeout(()=>pollJob(jid,pfx,btnLbl),1500);
  }catch(err){setTimeout(()=>pollJob(jid,pfx,btnLbl),3000);}
}

function addLog(pfx,type,msg){
  const b=document.getElementById(pfx+'-lb');if(!b) return;
  const l=document.createElement('div');l.className=type;
  l.textContent='['+new Date().toLocaleTimeString()+'] '+msg;
  b.appendChild(l);b.scrollTop=b.scrollHeight;
}
function setBadge(pfx,type,label){
  const b=document.getElementById(pfx+'-badge');if(!b) return;
  b.className='sbg s-'+type;b.textContent=label;
  if(type!=='p') b.classList.remove('pulse');
}
function showDownloads(pfx,jid,files){
  const sec=document.getElementById(pfx+'-dw');
  const grid=document.getElementById(pfx+'-dlg');
  if(!sec||!grid) return;
  sec.style.display='block';grid.innerHTML='';
  const ICONS={'ANNUAL':'📊','GSTR3BR1':'📋','GSTR3BR2A':'📈','GSTR1_FULL':'📑',
               'RECONCIL':'📊','SUMMARY':'📊','R1_VS':'📋','TAX_LI':'📑'};
  files.forEach(f=>{
    const icon=Object.entries(ICONS).find(([k])=>f.name.toUpperCase().includes(k))?.[1]||'📁';
    const c=document.createElement('div');c.className='dlc';
    c.innerHTML=`<div style="font-size:1.4rem">${icon}</div>
      <div class="dl-n">${f.name}</div><div class="dl-s">${f.size}</div>
      <a href="/api/download/${jid}/${encodeURIComponent(f.name)}" class="btn-dl" download>Download ↓</a>`;
    grid.appendChild(c);
  });
}

// ── Auto Download ────────────────────────────────────────────────
let _adJobId=null;
function checkBrowserConnection(){} // server runs the browser

document.getElementById('ad-form').addEventListener('submit',async e=>{
  e.preventDefault();
  const gstin=document.getElementById('ad-gstin').value.trim().toUpperCase();
  const cname=document.getElementById('ad-name').value.trim();
  const username=document.getElementById('ad-username').value.trim();
  const password=document.getElementById('ad-password').value;
  const fy=document.getElementById('ad-fy').value;
  const returns=document.getElementById('ad-returns').value;
  if(!gstin||gstin.length!==15){alert('Enter valid 15-char GSTIN');return;}
  if(!cname){alert('Enter company name');return;}
  if(!username||!password){alert('Enter username and password');return;}
  document.getElementById('ad-pw').style.display='block';
  document.getElementById('ad-dw').style.display='none';
  document.getElementById('ad-captcha-box').style.display='none';
  document.getElementById('ad-lb').innerHTML='';
  document.getElementById('ad-pb').style.width='0%';
  const btn=document.getElementById('ad-submit');
  btn.disabled=true;btn.textContent='Starting…';
  addLog('ad','info','Connecting to GST portal on server...');
  try{
    const res=await fetch('/api/auto-download',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({gstin,client_name:cname,username,password,fy,returns})});
    const d=await res.json();
    if(d.error){addLog('ad','err',d.error);setBadge('ad','e','Failed');
      btn.disabled=false;btn.textContent='🚀 Start Auto Download';return;}
    _adJobId=d.job_id;btn.textContent='Running…';_adPoll(_adJobId);
  }catch(err){addLog('ad','err','Network error: '+err.message);setBadge('ad','e','Failed');
    btn.disabled=false;btn.textContent='🚀 Start Auto Download';}
});

let _adCapShown=false;
async function _adPoll(jid){
  try{
    const r=await fetch('/api/job/'+jid);const d=await r.json();
    if(d.logs)d.logs.forEach(l=>addLog('ad',l.type,l.msg));
    if(d.progress!=null)document.getElementById('ad-pb').style.width=d.progress+'%';
    if(d.captcha_needed&&d.captcha_img&&!_adCapShown){
      _adCapShown=true;
      document.getElementById('ad-captcha-img').src='data:image/png;base64,'+d.captcha_img;
      document.getElementById('ad-captcha-box').style.display='block';
      document.getElementById('ad-captcha-val').value='';
      document.getElementById('ad-captcha-val').focus();
    }
    if(!d.captcha_needed)_adCapShown=false;
    if(d.status==='done'){
      setBadge('ad','d','Complete');
      document.getElementById('ad-pb').style.width='100%';
      document.getElementById('ad-submit').disabled=false;
      document.getElementById('ad-submit').textContent='🚀 Start Auto Download';
      document.getElementById('ad-captcha-box').style.display='none';
      _adShowFiles(jid,d.files);return;
    }
    if(d.status==='error'){
      addLog('ad','err',d.error||'Unknown error');setBadge('ad','e','Failed');
      document.getElementById('ad-submit').disabled=false;
      document.getElementById('ad-submit').textContent='🚀 Start Auto Download';
      document.getElementById('ad-captcha-box').style.display='none';return;
    }
    setTimeout(()=>_adPoll(jid),1500);
  }catch(e){setTimeout(()=>_adPoll(jid),3000);}
}
async function adRefreshCaptcha(){
  if(!_adJobId)return;
  try{
    const r=await fetch('/api/captcha-refresh/'+_adJobId,{method:'POST'});
    const d=await r.json();
    if(d.img){document.getElementById('ad-captcha-img').src='data:image/png;base64,'+d.img;
      document.getElementById('ad-captcha-val').value='';
      document.getElementById('ad-captcha-val').focus();}
  }catch(e){}
}
async function adSubmitCaptcha(){
  const txt=document.getElementById('ad-captcha-val').value.trim();
  if(!txt){alert('Type the CAPTCHA first');return;}
  const btn=document.getElementById('ad-captcha-btn');
  btn.disabled=true;btn.textContent='Submitting…';
  try{
    const r=await fetch('/api/captcha-submit/'+_adJobId,{method:'POST',
      headers:{'Content-Type':'application/json'},body:JSON.stringify({captcha:txt})});
    const d=await r.json();
    if(d.ok){addLog('ad','info','CAPTCHA submitted — logging in...');
      document.getElementById('ad-captcha-box').style.display='none';_adCapShown=false;}
    else{addLog('ad','warn','Wrong CAPTCHA — refreshing...');await adRefreshCaptcha();}
  }catch(e){addLog('ad','warn','Submit failed — try again');}
  btn.disabled=false;btn.textContent='Submit & Login →';
}
function _adShowFiles(jid,files){
  const sec=document.getElementById('ad-dw'),grid=document.getElementById('ad-dlg');
  sec.style.display='block';grid.innerHTML='';
  if(!files||!files.length){grid.innerHTML='<p style="color:var(--muted);font-size:.8rem">No files. Check logs.</p>';return;}
  files.forEach(f=>{
    const c=document.createElement('div');c.className='dlc';
    c.innerHTML=`<div style="font-size:1.5rem">📥</div>
      <div class="dl-n">${f.name}</div><div class="dl-s">${f.size||''}</div>
      <a href="/api/dl-file/${jid}/${encodeURIComponent(f.name)}" class="btn-dl" download>⬇ Download</a>`;
    grid.appendChild(c);
  });
}

const MONS=['April','May','June','July','August','September',
            'October','November','December','January','February','March'];
const RETS=['GSTR1','GSTR1A','GSTR2B','GSTR2A','GSTR3B'];

function renderDlStatus(st,jid){
  document.getElementById('ds-result').style.display='block';
  document.getElementById('ds-title').textContent=jid||'—';
  const tb=document.getElementById('ds-tb');tb.innerHTML='';
  let ok=0,fl=0,pd=0;
  MONS.forEach(m=>{
    const tr=document.createElement('tr');
    let rok=0,rfl=0;
    let td=`<td>${m}</td>`;
    RETS.forEach(r=>{
      const v=(st[m+'_'+r]||'SKIP').toUpperCase();
      let cls,txt;
      if(v==='OK'||v==='DONE'){cls='c-ok';txt='✓ OK';rok++;ok++;}
      else if(['TILE_FAIL','NOT_FOUND','TILE_NOT_FOUND','GEN_FAIL','ERR','FAIL'].some(x=>v.includes(x)))
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
    td+=`<td>${rs}</td>`;tr.innerHTML=td;tb.appendChild(tr);
  });
  document.getElementById('ds-sum').innerHTML=
    `<strong style="color:var(--grn)">${ok} ✓ OK</strong> &nbsp; `+
    `<strong style="color:var(--red)">${fl} ✗ Failed</strong> &nbsp; `+
    `<strong style="color:var(--org)">${pd} ⋯ Pending</strong> &nbsp; `+
    `out of ${MONS.length*RETS.length} expected`;
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
  const fd=new FormData();mf.forEach(f=>fd.append('master_file',f));
  try{
    const res=await fetch('/api/parse_master',{method:'POST',body:fd});
    const d=await res.json();
    if(d.dl_status) renderDlStatus(d.dl_status,'Master Report');
    else alert('Parse error: '+(d.error||'Unknown'));
  }catch(e){alert('Error: '+e.message);}
}

function buildStFromFiles(files){
  const st={};
  MONS.forEach(m=>RETS.forEach(r=>{
    st[m+'_'+r]=files.some(f=>f.name.includes(r)&&f.name.includes(m))?'OK':'SKIP';
  }));
  return st;
}

// ── Feedback / Comments ───────────────────────────────────────────
let currentRating=0;
function setRating(val){
  currentRating=val;
  const LABELS={1:'Poor',2:'Fair',3:'Good',4:'Very Good',5:'Excellent'};
  document.querySelectorAll('.star-btn').forEach(s=>{
    s.style.opacity=parseInt(s.dataset.val)<=val?'1':'.35';
    s.style.color=parseInt(s.dataset.val)<=val?'#ffd700':'var(--muted)';
  });
  document.getElementById('rating-lbl').textContent=LABELS[val]||'';
}

document.getElementById('fb-form').addEventListener('submit', async e=>{
  e.preventDefault();
  const name=document.getElementById('fb-name').value.trim();
  const type=document.getElementById('fb-type').value;
  const msg=document.getElementById('fb-msg').value.trim();
  const stat=document.getElementById('fb-status');
  if(!msg){stat.textContent='Please write a comment.';stat.style.color='var(--red)';return;}
  const btn=document.getElementById('fb-submit');
  btn.disabled=true;btn.textContent='Submitting...';
  stat.textContent='';
  try{
    const res=await fetch('/api/feedback',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({name,type,msg,rating:currentRating})
    });
    const d=await res.json();
    if(d.success){
      stat.textContent='✓ Thank you! Your feedback was recorded.';
      stat.style.color='var(--grn)';
      document.getElementById('fb-msg').value='';
      document.getElementById('fb-name').value='';
      currentRating=0;
      document.querySelectorAll('.star-btn').forEach(s=>{s.style.opacity='.35';s.style.color='var(--muted)';});
      document.getElementById('rating-lbl').textContent='';
      loadFeedback();
    } else {
      stat.textContent='Error: '+(d.error||'Unknown');
      stat.style.color='var(--red)';
    }
  }catch(err){
    stat.textContent='Network error. Please try again.';
    stat.style.color='var(--red)';
  }
  btn.disabled=false;btn.textContent='Submit Feedback →';
});

async function loadFeedback(){
  try{
    const res=await fetch('/api/feedback');
    const d=await res.json();
    const list=document.getElementById('fb-list');
    const count=document.getElementById('fb-count');
    count.textContent=d.count||0;
    if(!d.items||!d.items.length){
      list.innerHTML='<div class="no-fb">No comments yet. Be the first!</div>';
      return;
    }
    list.innerHTML='';
    d.items.forEach(fb=>{
      const TCLS={bug:'fb-bug',suggestion:'fb-sugg',praise:'fb-praise',other:'fb-other'};
      const TLBL={bug:'🐛 Bug',suggestion:'💡 Suggestion',praise:'👍 Praise',other:'💬 Other'};
      const stars=fb.rating?'★'.repeat(fb.rating)+'☆'.repeat(5-fb.rating):'';
      const div=document.createElement('div');div.className='fb-item';
      div.innerHTML=`
        <div class="fb-header">
          <div style="display:flex;align-items:center;gap:.5rem;flex-wrap:wrap">
            <span class="fb-name">${escHtml(fb.name||'Anonymous')}</span>
            <span class="fb-type ${TCLS[fb.type]||'fb-other'}">${TLBL[fb.type]||'Other'}</span>
            ${stars?`<span class="stars" title="${fb.rating}/5 stars">${stars}</span>`:''}
          </div>
          <span class="fb-time">${escHtml(fb.time||'')}</span>
        </div>
        <div class="fb-msg">${escHtml(fb.msg)}</div>`;
      list.appendChild(div);
    });
  }catch(e){console.error('Feedback load error:',e);}
}

function escHtml(s){
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;')
    .replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// Load feedback on page open
loadFeedback();

// ── Self-Ping to Keep Render App Alive ────────────────────────────
// Pings every 4 minutes while page is open (prevents Render free tier sleep)
(function keepAlive(){
  const PING_INTERVAL = 4 * 60 * 1000; // 4 minutes
  
  async function ping(){
    try{
      const res = await fetch('/health');
      if(res.ok){
        console.log('[KeepAlive] ✓ Ping successful at', new Date().toLocaleTimeString());
      } else {
        console.log('[KeepAlive] ⚠ Ping failed:', res.status);
      }
    }catch(e){
      console.log('[KeepAlive] ⚠ Ping error:', e.message);
    }
  }
  
  // Ping immediately on load
  ping();
  
  // Then ping every 4 minutes
  setInterval(ping, PING_INTERVAL);
  
  console.log('[KeepAlive] Self-ping started - app will stay awake while this tab is open');
})();
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
    return jsonify(
        status=job["status"], progress=job["progress"],
        logs=new_logs, files=job["files"], error=job["error"],
        dl_status=job.get("dl_status",{}),
        captcha_needed=job.get("captcha_needed", False),
        captcha_img=job.get("captcha_img", None),
    )

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

# ── Feedback API ──────────────────────────────────────────────────
@app.route("/api/feedback", methods=["GET","POST"])
@rate_limit(limit=20, window=60)
def api_feedback():
    if request.method == "GET":
        fb = _load_feedback()
        return jsonify(count=len(fb), items=list(reversed(fb[-50:])))

    data = request.get_json(silent=True) or {}
    msg  = str(data.get("msg","")).strip()[:1000]
    if not msg:
        return jsonify(error="Comment cannot be empty"), 400

    name   = str(data.get("name","Anonymous")).strip()[:60] or "Anonymous"
    ftype  = data.get("type","other")
    if ftype not in ("bug","suggestion","praise","other"): ftype = "other"
    rating = int(data.get("rating",0))
    if rating not in range(0,6): rating = 0

    entry = {
        "name":   name,
        "type":   ftype,
        "msg":    msg,
        "rating": rating,
        "time":   datetime.now().strftime("%d-%b-%Y %H:%M"),
    }
    fb = _load_feedback()
    fb.append(entry)
    if len(fb) > 500: fb = fb[-500:]
    _save_feedback(fb)
    return jsonify(success=True)

# ═══════════════════════════════════════════════════════════════════
# SERVER-SIDE AUTO DOWNLOAD  — No bridge, no OTP, no paid service
# User types CAPTCHA once in the web UI. Server does everything else.
# ═══════════════════════════════════════════════════════════════════
import queue as _queue, base64 as _b64

# Per-job state: captcha_q gets the text user types; screenshot holds b64 PNG
_sessions: dict = {}
_sess_lock = threading.Lock()

# ── Health ────────────────────────────────────────────────────────
@app.route("/health")
def health():
    return jsonify(status="ok")

# ── Browser status — always ready (server runs browser) ───────────
@app.route("/api/browser-status")
def browser_status():
    return jsonify(connected=True, mode="server")

# ── CAPTCHA image endpoint ────────────────────────────────────────
@app.route("/api/captcha-img/<job_id>")
def captcha_img(job_id):
    with _sess_lock:
        s = _sessions.get(job_id, {})
    img = s.get("screenshot")
    if not img:
        return jsonify(error="not ready"), 404
    return jsonify(img=img)

# ── CAPTCHA refresh ───────────────────────────────────────────────
@app.route("/api/captcha-refresh/<job_id>", methods=["POST"])
def captcha_refresh(job_id):
    with _sess_lock:
        s = _sessions.get(job_id, {})
    if not s:
        return jsonify(error="no session"), 404
    s.get("refresh_event", threading.Event()).set()
    # wait up to 6s for new screenshot
    for _ in range(60):
        time.sleep(0.1)
        img = s.get("screenshot")
        if img:
            return jsonify(img=img)
    return jsonify(error="timeout"), 504

# ── CAPTCHA submit ────────────────────────────────────────────────
@app.route("/api/captcha-submit/<job_id>", methods=["POST"])
def captcha_submit(job_id):
    text = (request.get_json(silent=True) or {}).get("captcha","").strip()
    if not text:
        return jsonify(ok=False, error="empty")
    with _sess_lock:
        s = _sessions.get(job_id)
    if not s:
        return jsonify(ok=False, error="no session")
    s["captcha_q"].put(text)
    return jsonify(ok=True)

# ── Download file ─────────────────────────────────────────────────
@app.route("/api/dl-file/<job_id>/<filename>")
def dl_file(job_id, filename):
    filename = Path(filename).name
    fp = OUTPUT_DIR / job_id / filename
    if not fp.exists():
        abort(404)
    return send_file(str(fp), as_attachment=True, download_name=filename)

# ── Start Auto Download ───────────────────────────────────────────
@app.route("/api/auto-download", methods=["POST"])
@rate_limit(limit=5, window=60)
def api_auto_download():
    d = request.get_json(silent=True) or {}
    gstin       = d.get("gstin","").strip().upper()
    client_name = d.get("client_name","").strip()
    username    = d.get("username","").strip()
    password    = d.get("password","")
    fy          = d.get("fy","2025-26")
    returns     = d.get("returns","all")

    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN"), 400
    if not client_name:
        return jsonify(error="Company name required"), 400
    if not username or not password:
        return jsonify(error="Username and password required"), 400

    job_id = str(uuid.uuid4())[:8]
    out_dir = OUTPUT_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)

    with jobs_lock:
        jobs[job_id] = {
            "status": "running", "progress": 0,
            "logs": [{"type":"info","msg":"Starting..."}],
            "files": [], "error": None,
            "captcha_needed": False, "captcha_img": None,
            "out_dir": str(out_dir),
        }

    sess = {
        "captcha_q":     _queue.Queue(),
        "refresh_event": threading.Event(),
        "screenshot":    None,
    }
    with _sess_lock:
        _sessions[job_id] = sess

    def run_bg():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(
                _auto_download(job_id, gstin, client_name,
                               username, password, fy, returns, sess))
        except Exception as _bg_exc:
            import traceback as _tb
            _msg = str(_bg_exc)
            with jobs_lock:
                if job_id in jobs:
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"]  = _msg
                    jobs[job_id]["logs"].append({"type":"err","msg":f"Fatal: {_msg}"})
                    for _l in _tb.format_exc().split("\n"):
                        if _l.strip():
                            jobs[job_id]["logs"].append({"type":"err","msg":f"  {_l}"})
        finally:
            loop.close()
            with _sess_lock:
                _sessions.pop(job_id, None)

    threading.Thread(target=run_bg, daemon=True).start()
    return jsonify(job_id=job_id)


async def _auto_download(job_id, gstin, client_name,
                          username, password, fy, returns, sess):
    """Server-side headless Playwright automation."""
    # ── Step 0: Check Playwright is installed ─────────────────────
    try:
        from playwright.async_api import async_playwright
    except ImportError:
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"]  = "Playwright not installed on server"
                jobs[job_id]["logs"].append({"type":"err",
                    "msg":"❌ Playwright not installed. Add 'playwright>=1.40' to requirements.txt"})
        return

    def log(msg, t="info"):
        print(f"[{job_id}] {msg}")   # also print to Render logs
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["logs"].append({"type":t,"msg":msg})

    def prog(p):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["progress"] = p

    log("✅ Background thread started")
    log("🔍 Checking Playwright / Chromium...")

    def set_captcha(img_b64):
        sess["screenshot"] = img_b64
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["captcha_needed"] = True
                jobs[job_id]["captcha_img"]    = img_b64

    def clear_captcha():
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["captcha_needed"] = False
                jobs[job_id]["captcha_img"]    = None

    fy_start = int(fy.split("-")[0])
    MONTHS = [
        ("April","04",str(fy_start)),    ("May","05",str(fy_start)),
        ("June","06",str(fy_start)),     ("July","07",str(fy_start)),
        ("August","08",str(fy_start)),   ("September","09",str(fy_start)),
        ("October","10",str(fy_start)),  ("November","11",str(fy_start)),
        ("December","12",str(fy_start)), ("January","01",str(fy_start+1)),
        ("February","02",str(fy_start+1)),("March","03",str(fy_start+1)),
    ]
    out_dir = Path(jobs[job_id]["out_dir"])
    downloaded = []

    async with async_playwright() as pw:
        log("Launching Chromium browser on server...")
        prog(3)
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox","--disable-setuid-sandbox",
                  "--disable-dev-shm-usage","--disable-gpu","--single-process"]
        )
        log("✅ Browser launched")
        prog(5)
        ctx = await browser.new_context(
            viewport={"width":1366,"height":768},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            accept_downloads=True,
        )
        ctx.set_default_timeout(30000)
        page = await ctx.new_page()

        try:
            # ── LOGIN ─────────────────────────────────────────────
            log("Opening GST portal login page...")
            prog(5)
            await page.goto("https://services.gst.gov.in/services/login",
                            wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector("#username", timeout=20000)

            log("Filling username and password...")
            await page.fill("#username", username)
            await page.fill("#user_pass", password)
            prog(10)

            # ── CAPTCHA LOOP (retry if wrong) ─────────────────────
            login_ok = False
            for attempt in range(4):
                # Screenshot just the CAPTCHA widget
                try:
                    cap_el = await page.wait_for_selector(
                        "img[src*='captcha' i], img[alt*='captcha' i], "
                        "#imgCaptcha, .captcha img",
                        timeout=8000)
                    img_bytes = await cap_el.screenshot()
                except Exception:
                    img_bytes = await page.screenshot(clip={"x":0,"y":0,"width":640,"height":500})

                img_b64 = _b64.b64encode(img_bytes).decode()
                set_captcha(img_b64)

                if attempt == 0:
                    log("🔐 CAPTCHA shown — type it in the box and click Submit", "warn")
                else:
                    log(f"❌ Wrong CAPTCHA — try again (attempt {attempt+1})", "warn")

                # Wait for user to type CAPTCHA (up to 5 min)
                try:
                    captcha_text = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: sess["captcha_q"].get(timeout=300))
                except Exception:
                    raise RuntimeError("CAPTCHA not entered within 5 minutes")

                clear_captcha()
                log(f"Submitting CAPTCHA...")
                prog(15 + attempt * 3)

                # Fill CAPTCHA field and click login
                try:
                    await page.fill(
                        "input[name='captcha' i], #captcha, input[placeholder*='captcha' i], "
                        "input[id*='captcha' i], input[maxlength='6'], input[maxlength='8']",
                        captcha_text)
                except Exception:
                    # Try filling by index — CAPTCHA is usually the 3rd input
                    inputs = await page.query_selector_all("input[type=text], input:not([type=password])")
                    for inp in inputs:
                        visible = await inp.is_visible()
                        if visible:
                            val = await inp.input_value()
                            if not val:  # empty field = probably captcha
                                await inp.fill(captcha_text)
                                break

                await page.click("#btnSubmit, button[type=submit], #loginBtn, "
                                 "button:has-text('LOGIN'), input[type=submit]")
                prog(20 + attempt * 3)

                # Wait for result — either dashboard or error
                try:
                    await page.wait_for_function(
                        """() => {
                            const url = window.location.href;
                            const err = document.querySelector('.error-msg, .alert-danger, #errMsg, .err-msg');
                            return url.includes('dashboard') || url.includes('mainmenu') ||
                                   url.includes('taxpayer') || (err && err.innerText.trim().length > 0);
                        }""",
                        timeout=20000)
                except Exception:
                    pass

                cur_url = page.url
                if any(x in cur_url for x in
                       ["dashboard","mainmenu","taxpayer","auth/","returns/"]):
                    if "login" not in cur_url:
                        login_ok = True
                        log(f"✅ Login successful!")
                        prog(30)
                        break

                # Check for error message on page
                err_el = await page.query_selector(
                    ".error-msg, .alert-danger, #errMsg, .err-msg, [class*='error']")
                if err_el:
                    err_txt = await err_el.inner_text()
                    log(f"Portal says: {err_txt.strip()[:80]}", "warn")

                # Still on login? Reload and retry
                if attempt < 3:
                    await page.reload(wait_until="domcontentloaded")
                    await page.wait_for_selector("#username", timeout=15000)
                    await page.fill("#username", username)
                    await page.fill("#user_pass", password)

            if not login_ok:
                raise RuntimeError("Login failed after 4 attempts. Check username/password.")

            # ── NAVIGATE TO RETURNS DASHBOARD ─────────────────────
            log("Navigating to returns dashboard...")
            prog(33)
            await page.goto("https://return.gst.gov.in/returns/auth/dashboard",
                            wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(3)

            # ── DOWNLOAD EACH RETURN ──────────────────────────────
            total  = (12 if returns in ["all","gstr1"] else 0) + \
                     (12 if returns in ["all","gstr2b"] else 0) + \
                     (12 if returns in ["all","gstr3b"] else 0)
            done   = 0

            async def dl_month(return_type, mon_name, mon_num, mon_yr):
                """Navigate to a return for one month and trigger download."""
                nonlocal done
                url_map = {
                    "gstr1":  "https://return.gst.gov.in/returns/auth/gstr1",
                    "gstr2b": "https://return.gst.gov.in/returns/auth/gstr2b",
                    "gstr3b": "https://return.gst.gov.in/returns/auth/gstr3b",
                }
                await page.goto(url_map[return_type],
                                wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2)

                # Select FY
                for fy_sel in ["#finYear","select[ng-model*='year' i]",
                               "select[id*='year' i]","select:nth-of-type(1)"]:
                    try:
                        await page.select_option(fy_sel, fy); break
                    except Exception: pass
                await asyncio.sleep(0.5)

                # Select period
                period_val = f"{mon_num}{mon_yr}"
                for p_sel in ["#taxPeriod","select[ng-model*='period' i]",
                              "select[id*='period' i]","select:nth-of-type(2)"]:
                    try:
                        await page.select_option(p_sel, period_val); break
                    except Exception: pass
                await asyncio.sleep(0.5)

                # Click Search/Proceed
                for btn_sel in ["#searchBtn","#proceedBtn",
                                "button:has-text('Search')",
                                "button:has-text('Proceed')",
                                "button[type=submit]"]:
                    try:
                        await page.click(btn_sel, timeout=5000); break
                    except Exception: pass
                await asyncio.sleep(3)

                # Trigger download
                ext = ".zip" if return_type == "gstr1" else \
                      ".xlsx" if return_type == "gstr2b" else ".pdf"
                fname = f"{return_type.upper()}_{mon_name}_{mon_yr}{ext}"
                fpath = out_dir / fname

                async with page.expect_download(timeout=30000) as dl_info:
                    for dl_sel in ["#downloadBtn","a:has-text('Download')",
                                   "button:has-text('Download JSON')",
                                   "button:has-text('Download Excel')",
                                   "button:has-text('Download')",".download-btn"]:
                        try:
                            await page.click(dl_sel, timeout=5000); break
                        except Exception: pass
                try:
                    dl = await dl_info.value
                    await dl.save_as(str(fpath))
                    sz = fpath.stat().st_size // 1024
                    log(f"  ✓ {fname} ({sz} KB)", "ok")
                    downloaded.append({"name": fname, "size": f"{sz} KB"})
                except Exception as ex:
                    log(f"  ⚠ {fname}: {ex}", "warn")

                done += 1
                prog(33 + int(done / max(total,1) * 62))

            if returns in ["all","gstr1"]:
                log("── Downloading GSTR-1 ──────────────────────────")
                for mn, mm, my in MONTHS:
                    log(f"  GSTR-1 {mn} {my}...")
                    try: await dl_month("gstr1", mn, mm, my)
                    except Exception as ex: log(f"  ⚠ {mn}: {ex}","warn"); done+=1

            if returns in ["all","gstr2b"]:
                log("── Downloading GSTR-2B ─────────────────────────")
                for mn, mm, my in MONTHS:
                    log(f"  GSTR-2B {mn} {my}...")
                    try: await dl_month("gstr2b", mn, mm, my)
                    except Exception as ex: log(f"  ⚠ {mn}: {ex}","warn"); done+=1

            if returns in ["all","gstr3b"]:
                log("── Downloading GSTR-3B ─────────────────────────")
                for mn, mm, my in MONTHS:
                    log(f"  GSTR-3B {mn} {my}...")
                    try: await dl_month("gstr3b", mn, mm, my)
                    except Exception as ex: log(f"  ⚠ {mn}: {ex}","warn"); done+=1

            prog(100)
            log(f"✅ Done! {len(downloaded)} file(s) ready to download.", "ok")
            with jobs_lock:
                jobs[job_id]["status"] = "done"
                jobs[job_id]["files"]  = downloaded

        except Exception as exc:
            import traceback
            log(f"Error: {exc}", "err")
            for line in traceback.format_exc().split("\n"):
                if line.strip(): log(f"  {line}", "err")
            with jobs_lock:
                jobs[job_id]["status"] = "error"
                jobs[job_id]["error"]  = str(exc)
        finally:
            await browser.close()



# ── Startup ───────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n  ============================================================")
    print(f"   GST Reconciliation Portal v6 — with Auto Download")
    print(f"  ============================================================")
    print(f"   Upload dir    : {UPLOAD_DIR}")
    print(f"   Output dir    : {OUTPUT_DIR}")
    print(f"   Feedback file : {FEEDBACK_FILE}")
    suite = _find_engine("gst_suite_final.py")
    ext   = _find_engine("gstr1_extract.py")
    print(f"   Suite engine  : {suite or '⚠ NOT FOUND'}")
    print(f"   GSTR-1 engine : {ext   or '⚠ NOT FOUND'}")
    print(f"   WebSocket     : {'✅ Enabled' if WEBSOCKET_AVAILABLE else '⚠ Not available'}")
    print(f"\n   Open: http://localhost:{port}")
    print(f"  ============================================================\n")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
