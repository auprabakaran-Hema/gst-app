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
    <span class="pill">GSTR-1A</span>
  </div>
  <p style="color:var(--muted);font-size:.78rem;line-height:1.6;margin-bottom:.8rem">
    Download all GST returns <strong style="color:var(--txt)">directly from the portal</strong> using your PC browser.
    Your credentials stay secure on the server — only the browser runs on your PC for CAPTCHA solving.
  </p>
  
  <!-- Connection Status -->
  <div class="conn-status" id="conn-status">
    <div class="conn-dot" id="conn-dot"></div>
    <span id="conn-text">Checking browser connection...</span>
  </div>
</div>

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
        <option value="all">All Returns (GSTR-1, 2B, 2A, 3B, 1A)</option>
        <option value="gstr1">GSTR-1 Only</option>
        <option value="gstr2b">GSTR-2B Only</option>
        <option value="gstr3b">GSTR-3B Only</option>
      </select></div>
  </div>
</div>

<div class="card">
  <div class="ct">How It Works</div>
  <div class="info-box">
    <strong>Step 1:</strong> Click "Start Auto Download" below<br>
    <strong>Step 2:</strong> Run <code>browser_bridge.py</code> on your PC (one-time setup)<br>
    <strong>Step 3:</strong> Chrome will open on your PC with GST portal<br>
    <strong>Step 4:</strong> Type the CAPTCHA when it appears<br>
    <strong>Step 5:</strong> Files download automatically to your PC!
  </div>
  <button type="submit" class="btn-orange" id="ad-submit">🚀 Start Auto Download</button>
</div>
</form>

<div class="card pw" id="ad-pw">
  <div class="ct">Auto Download Progress <span class="sbg s-p pulse" id="ad-badge">Running</span></div>
  <div class="pb-w"><div class="pb" id="ad-pb"></div></div>
  <div class="lb" id="ad-lb"></div>
</div>

<div class="card dw" id="ad-dw">
  <div class="ct">Downloaded Files</div>
  <div class="dl-g" id="ad-dlg"></div>
  <p style="color:var(--muted);font-size:.66rem;margin-top:.65rem;font-family:var(--mono)">
    ✅ Files downloaded to your PC's Downloads folder. Upload them to the Reconciliation tab to process.
  </p>
</div>

<div class="card" id="ad-setup" style="display:none">
  <div class="ct">⚠️ Browser Bridge Not Connected</div>
  <div class="info-box warn">
    <strong>To use Auto Download, you need to run the browser bridge on your PC:</strong><br><br>
    <strong>1.</strong> Download <code>browser_bridge.py</code> to your PC<br>
    <strong>2.</strong> Install Python dependencies: <code>pip install playwright websockets</code><br>
    <strong>3.</strong> Install browser: <code>playwright install chromium</code><br>
    <strong>4.</strong> Run: <code>python browser_bridge.py</code><br><br>
    Then refresh this page and try again.
  </div>
  <a href="/api/download_bridge" class="btn-dl" style="display:inline-block;padding:.6rem 1.2rem">⬇️ Download browser_bridge.py</a>
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

// ── Browser Connection Check ─────────────────────────────────────
async function checkBrowserConnection(){
  const dot=document.getElementById('conn-dot');
  const txt=document.getElementById('conn-text');
  const setup=document.getElementById('ad-setup');
  const form=document.getElementById('ad-form');
  
  dot.className='conn-dot connecting';
  txt.textContent='Checking browser connection...';
  
  try{
    const res=await fetch('/api/browser-status');
    const d=await res.json();
    
    if(d.connected){
      dot.className='conn-dot online';
      txt.textContent='✅ Browser bridge connected! Ready to download.';
      if(setup) setup.style.display='none';
      if(form) form.style.display='block';
    } else {
      dot.className='conn-dot';
      txt.textContent='❌ Browser bridge not connected. Run browser_bridge.py on your PC.';
      if(setup) setup.style.display='block';
      if(form) form.style.display='none';
    }
  }catch(e){
    dot.className='conn-dot';
    txt.textContent='❌ Cannot check connection. Server may be busy.';
  }
}

// ── Auto Download Form ────────────────────────────────────────────
document.getElementById('ad-form').addEventListener('submit', async e=>{
  e.preventDefault();
  
  const gstin=document.getElementById('ad-gstin').value.trim().toUpperCase();
  const cname=document.getElementById('ad-name').value.trim();
  const username=document.getElementById('ad-username').value.trim();
  const password=document.getElementById('ad-password').value;
  const fy=document.getElementById('ad-fy').value;
  const returns=document.getElementById('ad-returns').value;
  
  if(!gstin||gstin.length!==15){alert('Enter a valid 15-character GSTIN');return;}
  if(!cname){alert('Enter company name');return;}
  if(!username){alert('Enter GST portal username');return;}
  if(!password){alert('Enter GST portal password');return;}
  
  document.getElementById('ad-pw').style.display='block';
  document.getElementById('ad-dw').style.display='none';
  document.getElementById('ad-lb').innerHTML='';
  document.getElementById('ad-pb').style.width='0%';
  
  const btn=document.getElementById('ad-submit');
  btn.disabled=true;btn.textContent='Connecting to your PC browser...';
  
  addLog('ad','info','Starting auto download...');
  addLog('ad','info','Waiting for browser connection...');
  
  try{
    const res=await fetch('/api/auto-download',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({gstin,client_name:cname,username,password,fy,returns})
    });
    const d=await res.json();
    
    if(d.error){
      addLog('ad','err','Error: '+d.error);
      setBadge('ad','e','Failed');
      btn.disabled=false;btn.textContent='🚀 Start Auto Download';
      return;
    }
    
    addLog('ad','info','Browser connected! Opening GST portal...');
    btn.textContent='Downloading...';
    
    // Poll for progress
    pollAutoDownload(d.job_id);
    
  }catch(err){
    addLog('ad','err','Network error: '+err.message);
    setBadge('ad','e','Failed');
    btn.disabled=false;btn.textContent='🚀 Start Auto Download';
  }
});

async function pollAutoDownload(jid){
  try{
    const res=await fetch('/api/job/'+jid);
    const d=await res.json();
    
    if(d.logs) d.logs.forEach(l=>addLog('ad',l.type,l.msg));
    if(d.progress!==undefined) document.getElementById('ad-pb').style.width=d.progress+'%';
    
    if(d.status==='done'){
      setBadge('ad','d','Complete');
      document.getElementById('ad-pb').style.width='100%';
      document.getElementById('ad-submit').disabled=false;
      document.getElementById('ad-submit').textContent='🚀 Start Auto Download';
      showAutoDownloadResults(d.files);
      return;
    }
    if(d.status==='error'){
      addLog('ad','err','Error: '+(d.error||'Unknown error'));
      setBadge('ad','e','Failed');
      document.getElementById('ad-submit').disabled=false;
      document.getElementById('ad-submit').textContent='🚀 Start Auto Download';
      return;
    }
    
    setTimeout(()=>pollAutoDownload(jid),2000);
  }catch(err){
    setTimeout(()=>pollAutoDownload(jid),3000);
  }
}

function showAutoDownloadResults(files){
  const sec=document.getElementById('ad-dw');
  const grid=document.getElementById('ad-dlg');
  if(!sec||!grid) return;
  
  sec.style.display='block';
  grid.innerHTML='';
  
  if(!files||!files.length){
    grid.innerHTML='<div style="color:var(--muted);font-size:.8rem">No files downloaded. Check logs above.</div>';
    return;
  }
  
  files.forEach(f=>{
    const c=document.createElement('div');c.className='dlc';
    c.innerHTML=`<div style="font-size:1.4rem">📥</div>
      <div class="dl-n">${f.name}</div>
      <div class="dl-s">${f.size||'Downloaded to PC'}</div>
      <span style="color:var(--grn);font-size:.7rem">✅ Saved to Downloads</span>`;
    grid.appendChild(c);
  });
}

// ── Download Status ───────────────────────────────────────────────
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
    return jsonify(status=job["status"], progress=job["progress"],
                   logs=new_logs, files=job["files"],
                   error=job["error"], dl_status=job.get("dl_status",{}))

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
# HTTP LONG-POLL BRIDGE  (replaces WebSocket — works on all hosts)
# ═══════════════════════════════════════════════════════════════════

def send_browser_command(command_dict, timeout=45):
    """Put a command on the queue and wait up to `timeout` s for the response."""
    if not _bridge_connected():
        return {"status": "error", "error": "No browser connected"}
    # Drain any stale response
    while not _resp_queue.empty():
        try: _resp_queue.get_nowait()
        except: pass
    _cmd_queue.put(command_dict)
    try:
        resp = _resp_queue.get(timeout=timeout)
        return resp
    except _queue.Empty:
        return {"status": "error", "error": "Timeout waiting for browser response"}


class RemoteBrowser:
    """Browser automation via HTTP long-poll bridge"""

    def _cmd(self, d):
        return send_browser_command(d)

    async def goto(self, url):
        return self._cmd({"action": "goto", "url": url})

    async def fill(self, selector, value):
        return self._cmd({"action": "fill", "selector": selector, "value": value})

    async def click(self, selector):
        return self._cmd({"action": "click", "selector": selector})

    async def screenshot(self):
        return self._cmd({"action": "screenshot"})

    async def wait_for_selector(self, selector, timeout=30000):
        return self._cmd({"action": "wait_for_selector", "selector": selector, "timeout": timeout})

    async def get_text(self, selector):
        return self._cmd({"action": "get_text", "selector": selector})

    async def select_option(self, selector, value):
        return self._cmd({"action": "select_option", "selector": selector, "value": value})

    async def get_url(self):
        return self._cmd({"action": "get_url"})

    async def wait_for_navigation(self):
        return self._cmd({"action": "wait_for_navigation"})


# ── Bridge HTTP endpoints (called by browser_bridge.py on PC) ─────

@app.route("/api/bridge/poll", methods=["GET"])
def bridge_poll():
    """PC calls this every ~3 s to signal it is alive and pick up a command."""
    global _bridge_last_seen
    with _bridge_lock:
        _bridge_last_seen = time.time()
    try:
        cmd = _cmd_queue.get(timeout=5)   # wait up to 5 s for a command
        return jsonify(cmd)
    except _queue.Empty:
        return jsonify({"action": "idle"})   # nothing to do


@app.route("/api/bridge/respond", methods=["POST"])
def bridge_respond():
    """PC posts the result of the last command here."""
    data = request.get_json(silent=True) or {}
    _resp_queue.put(data)
    return jsonify(ok=True)

# ── Browser Status API ────────────────────────────────────────────
@app.route("/api/browser-status")
def browser_status():
    return jsonify(connected=_bridge_connected())

# ── Auto Download API ─────────────────────────────────────────────
@app.route("/api/auto-download", methods=["POST"])
@rate_limit(limit=5, window=60)
def api_auto_download():
    """Start auto download from GST portal"""
    if not WEBSOCKET_AVAILABLE:
        return jsonify(error="Auto Download feature not available"), 503
    
    data = request.get_json(silent=True) or {}
    gstin = data.get("gstin", "").strip().upper()
    client_name = data.get("client_name", "").strip()
    username = data.get("username", "").strip()
    password = data.get("password", "")
    fy = data.get("fy", "2025-26")
    returns = data.get("returns", "all")
    
    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN"), 400
    if not client_name:
        return jsonify(error="Company name required"), 400
    if not username or not password:
        return jsonify(error="Username and password required"), 400
    
    if not _bridge_connected():
        return jsonify(error="No browser connected. Run browser_bridge.py on your PC first."), 400
    
    job_id = str(uuid.uuid4())[:8]
    
    with jobs_lock:
        jobs[job_id] = {
            "status": "running",
            "progress": 0,
            "logs": [{"type": "info", "msg": "Starting auto download..."}],
            "files": [],
            "error": None,
            "gstin": gstin,
            "client_name": client_name,
            "fy": fy,
            "mode": "autodownload",
            "dl_status": {}
        }
    
    # Start auto download in background
    def run_async():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(run_auto_download(job_id, gstin, client_name, username, password, fy, returns))
        print(f"Auto download result: {result}")
    
    threading.Thread(target=run_async, daemon=True).start()
    
    return jsonify(job_id=job_id)

async def run_auto_download(job_id, gstin, client_name, username, password, fy, returns):
    """
    Auto-download GST returns via the PC browser bridge.
    Follows the real GST portal flow at services.gst.gov.in.
    """
    def log(msg, t="info"):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["logs"].append({"type": t, "msg": msg})
    def prog(p):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["progress"] = p

    # FY → start year e.g. "2024-25" → "2024"
    fy_start = fy.split("-")[0].strip()

    # Month tuples: (display-name, number, year)
    s = int(fy_start); e = s + 1
    MONTH_LIST = [
        ("April",     "04", str(s)), ("May",      "05", str(s)),
        ("June",      "06", str(s)), ("July",     "07", str(s)),
        ("August",    "08", str(s)), ("September","09", str(s)),
        ("October",   "10", str(s)), ("November", "11", str(s)),
        ("December",  "12", str(s)), ("January",  "01", str(e)),
        ("February",  "02", str(e)), ("March",    "03", str(e)),
    ]

    try:
        browser = RemoteBrowser()
        downloaded_files = []
        prog(5)

        # ── Step 1: Go directly to login page ────────────────────
        log("Opening GST portal login page...")
        r = browser._cmd({"action": "goto",
                           "url": "https://services.gst.gov.in/services/login"})
        if r.get("status") == "error":
            raise RuntimeError(f"Cannot open GST portal: {r.get('error')}")
        prog(8)

        # ── Step 2: Wait for username field ──────────────────────
        log("Waiting for login form...")
        r = browser._cmd({"action": "wait_for_selector",
                           "selector": "#username", "timeout": 30000})
        if r.get("status") == "error":
            raise RuntimeError("Login form not found on page")
        prog(10)

        # ── Step 3: Fill username & password ─────────────────────
        log("Entering username and password...")
        browser._cmd({"action": "fill", "selector": "#username",
                       "value": username})
        browser._cmd({"action": "fill", "selector": "#user_pass",
                       "value": password})
        prog(15)

        # ── Step 4: Ask user to solve CAPTCHA then click LOGIN ────
        log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "info")
        log("🛑  ACTION REQUIRED ON YOUR PC:", "warn")
        log("    1. Look at the browser_bridge window on your PC", "warn")
        log("    2. Type the CAPTCHA characters in the browser", "warn")
        log("    3. Click the LOGIN button", "warn")
        log("    4. Press ENTER in the bridge window to continue", "warn")
        log("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "info")

        # This blocks until the user presses ENTER in browser_bridge
        r = browser._cmd({"action": "wait_captcha"})
        if r.get("status") == "error":
            raise RuntimeError(f"CAPTCHA wait failed: {r.get('error')}")
        prog(20)

        # ── Step 5: Wait for successful login (URL change) ────────
        log("Waiting for login to complete...")
        login_success = False
        for attempt in range(60):   # max 60 s
            await asyncio.sleep(1)
            r = browser._cmd({"action": "get_url"})
            url = r.get("url", "")
            # GST portal post-login URLs contain these fragments
            if any(x in url for x in [
                "taxpayerDashboard", "taxpayer/dashboard",
                "mainmenu", "dashboard", "returns/dashboard",
                "services/dashboard", "auth/"
            ]) and "login" not in url:
                login_success = True
                log(f"✅ Login successful! ({url[:60]}...)")
                break
            if attempt % 10 == 0 and attempt > 0:
                log(f"Still waiting for dashboard... ({attempt}s)")

        if not login_success:
            # Last chance: check page title / content
            r2 = browser._cmd({"action": "get_url"})
            url2 = r2.get("url", "")
            if "login" not in url2 and "gst.gov.in" in url2:
                login_success = True
                log(f"✅ Logged in (URL: {url2[:60]})")
            else:
                raise RuntimeError(
                    "Login failed or timed out. Wrong CAPTCHA / credentials?")

        prog(25)

        # ── Step 6: Download returns ──────────────────────────────
        total_steps = 0
        if returns in ["all", "gstr1"]:   total_steps += 12
        if returns in ["all", "gstr2b"]:  total_steps += 12
        if returns in ["all", "gstr2a"]:  total_steps += 12
        if returns in ["all", "gstr3b"]:  total_steps += 12
        step = 0
        base_prog = 25

        def step_prog():
            nonlocal step; step += 1
            p = base_prog + int((step / max(total_steps, 1)) * 65)
            prog(min(p, 90))

        # ── GSTR-1 ───────────────────────────────────────────────
        if returns in ["all", "gstr1"]:
            log("─── Downloading GSTR-1 ───────────────────────────")
            r = browser._cmd({"action": "goto",
                "url": "https://return.gst.gov.in/returns/auth/dashboard"})
            await asyncio.sleep(3)

            for mon_name, mon_num, mon_yr in MONTH_LIST:
                log(f"  GSTR-1 {mon_name} {mon_yr}...")
                try:
                    # Navigate to GSTR-1 filing
                    browser._cmd({"action": "goto",
                        "url": f"https://return.gst.gov.in/returns/auth/gstr1"})
                    await asyncio.sleep(2)

                    # Select FY and month
                    browser._cmd({"action": "select_option",
                        "selector": "select[name='fy'], #finYear, select.fy-select",
                        "value": fy})
                    await asyncio.sleep(0.5)
                    browser._cmd({"action": "select_option",
                        "selector": "select[name='fp'], #taxPeriod, select.period-select",
                        "value": f"{mon_num}{mon_yr}"})
                    await asyncio.sleep(0.5)

                    # Click Search / Proceed
                    browser._cmd({"action": "click",
                        "selector": "button[type=submit], #proceed-btn, #searchBtn, "
                                    "button:has-text('Search'), button:has-text('Proceed')"})
                    await asyncio.sleep(3)

                    # Click Download JSON
                    dl = browser._cmd({"action": "click",
                        "selector": "#downloadBtn, a:has-text('Download'), "
                                    "button:has-text('Download JSON'), .download-btn"})
                    if dl.get("status") == "done":
                        log(f"  ✓ GSTR-1 {mon_name} {mon_yr} downloaded", "ok")
                        downloaded_files.append(
                            {"name": f"GSTR1_{mon_name}_{mon_yr}.zip",
                             "size": "downloaded to PC"})
                    else:
                        log(f"  ⚠ GSTR-1 {mon_name} {mon_yr}: {dl.get('error','no download btn')}", "warn")

                except Exception as ex:
                    log(f"  ⚠ GSTR-1 {mon_name} {mon_yr} skipped: {ex}", "warn")
                step_prog()

        # ── GSTR-2B ──────────────────────────────────────────────
        if returns in ["all", "gstr2b"]:
            log("─── Downloading GSTR-2B ──────────────────────────")
            for mon_name, mon_num, mon_yr in MONTH_LIST:
                log(f"  GSTR-2B {mon_name} {mon_yr}...")
                try:
                    browser._cmd({"action": "goto",
                        "url": "https://return.gst.gov.in/returns/auth/gstr2b"})
                    await asyncio.sleep(2)
                    browser._cmd({"action": "select_option",
                        "selector": "select[name='fy'], #finYear", "value": fy})
                    await asyncio.sleep(0.5)
                    browser._cmd({"action": "select_option",
                        "selector": "select[name='fp'], #taxPeriod",
                        "value": f"{mon_num}{mon_yr}"})
                    await asyncio.sleep(0.5)
                    browser._cmd({"action": "click",
                        "selector": "button[type=submit], #proceed-btn, #searchBtn"})
                    await asyncio.sleep(3)
                    dl = browser._cmd({"action": "click",
                        "selector": "#downloadBtn, a:has-text('Download'), "
                                    "button:has-text('Download'), .download-btn"})
                    if dl.get("status") == "done":
                        log(f"  ✓ GSTR-2B {mon_name} {mon_yr} downloaded", "ok")
                        downloaded_files.append(
                            {"name": f"GSTR2B_{mon_name}_{mon_yr}.xlsx",
                             "size": "downloaded to PC"})
                    else:
                        log(f"  ⚠ GSTR-2B {mon_name} {mon_yr}: {dl.get('error','no btn')}", "warn")
                except Exception as ex:
                    log(f"  ⚠ GSTR-2B {mon_name} {mon_yr} skipped: {ex}", "warn")
                step_prog()

        # ── GSTR-3B ──────────────────────────────────────────────
        if returns in ["all", "gstr3b"]:
            log("─── Downloading GSTR-3B ──────────────────────────")
            for mon_name, mon_num, mon_yr in MONTH_LIST:
                log(f"  GSTR-3B {mon_name} {mon_yr}...")
                try:
                    browser._cmd({"action": "goto",
                        "url": "https://return.gst.gov.in/returns/auth/gstr3b"})
                    await asyncio.sleep(2)
                    browser._cmd({"action": "select_option",
                        "selector": "select[name='fy'], #finYear", "value": fy})
                    await asyncio.sleep(0.5)
                    browser._cmd({"action": "select_option",
                        "selector": "select[name='fp'], #taxPeriod",
                        "value": f"{mon_num}{mon_yr}"})
                    await asyncio.sleep(0.5)
                    browser._cmd({"action": "click",
                        "selector": "button[type=submit], #proceed-btn, #searchBtn"})
                    await asyncio.sleep(3)
                    dl = browser._cmd({"action": "click",
                        "selector": "#downloadBtn, a:has-text('PDF'), "
                                    "button:has-text('Download'), .download-btn"})
                    if dl.get("status") == "done":
                        log(f"  ✓ GSTR-3B {mon_name} {mon_yr} downloaded", "ok")
                        downloaded_files.append(
                            {"name": f"GSTR3B_{mon_name}_{mon_yr}.pdf",
                             "size": "downloaded to PC"})
                    else:
                        log(f"  ⚠ GSTR-3B {mon_name} {mon_yr}: {dl.get('error','no btn')}", "warn")
                except Exception as ex:
                    log(f"  ⚠ GSTR-3B {mon_name} {mon_yr} skipped: {ex}", "warn")
                step_prog()

        prog(95)
        n = len(downloaded_files)
        log(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "info")
        log(f"✅ All done! {n} file(s) downloaded to your PC.", "ok")
        log(f"   Check your PC Downloads folder.", "ok")
        log(f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", "info")
        prog(100)

        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"]  = downloaded_files

    except Exception as exc:
        import traceback
        log(f"Error: {exc}", "err")
        for line in traceback.format_exc().split("\n"):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)

# ── Download browser_bridge.py ────────────────────────────────────
@app.route("/api/download_bridge")
def download_bridge():
    """Download browser_bridge.py for user's PC"""
    bridge_code = '''"""
browser_bridge.py - Run this on YOUR PC
Connects your local browser to the Render server for GST automation
"""

import asyncio
import websockets
import json
import base64
import os
from playwright.sync_api import sync_playwright

# UPDATE THIS with your Render app WebSocket URL
# Example: "wss://my-gst-app.onrender.com"
RENDER_SERVER = "wss://YOUR-RENDER-APP.onrender.com"

async def browser_handler():
    print("=" * 60)
    print("🖥️  GST Browser Bridge")
    print("=" * 60)
    print("📱 Connecting to Render server...")
    
    ws_url = RENDER_SERVER.replace("https://", "wss://").replace("http://", "ws://")
    if not ws_url.startswith("ws"):
        ws_url = "wss://" + ws_url
    if not ws_url.endswith("/ws"):
        ws_url += "/ws"
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=False,
            args=['--start-maximized']
        )
        context = browser.new_context(viewport={"width": 1366, "height": 768})
        page = context.new_page()
        
        print("✅ Browser ready!")
        print("⏳ Waiting for commands from Render...")
        print("=" * 60)
        
        try:
            async with websockets.connect(ws_url) as ws:
                print("🔗 Connected to Render!")
                print("🌐 GST Portal will open automatically...")
                print("📝 Type CAPTCHA when it appears")
                print("=" * 60)
                
                while True:
                    try:
                        message = await ws.recv()
                        data = json.loads(message)
                        action = data.get("action")
                        
                        if action == "goto":
                            url = data.get("url")
                            print(f"🌐 Navigating to: {url}")
                            page.goto(url, wait_until="networkidle")
                            await ws.send(json.dumps({"status": "done", "url": page.url}))
                            
                        elif action == "fill":
                            selector = data.get("selector")
                            value = data.get("value")
                            display_value = "*" * len(value) if "pass" in selector.lower() else value
                            print(f"⌨️  Filling {selector}: {display_value}")
                            page.fill(selector, value)
                            await ws.send(json.dumps({"status": "done"}))
                            
                        elif action == "click":
                            selector = data.get("selector")
                            print(f"🖱️  Clicking: {selector}")
                            page.click(selector)
                            await ws.send(json.dumps({"status": "done"}))
                            
                        elif action == "screenshot":
                            print("📸 Taking screenshot...")
                            screenshot = page.screenshot(full_page=True)
                            encoded = base64.b64encode(screenshot).decode()
                            await ws.send(json.dumps({
                                "status": "screenshot", 
                                "image": encoded
                            }))
                            
                        elif action == "wait_for_selector":
                            selector = data.get("selector")
                            timeout = data.get("timeout", 30000)
                            print(f"⏳ Waiting for: {selector}")
                            page.wait_for_selector(selector, timeout=timeout)
                            await ws.send(json.dumps({"status": "found"}))
                            
                        elif action == "get_text":
                            selector = data.get("selector")
                            text = page.inner_text(selector)
                            await ws.send(json.dumps({"status": "text", "text": text}))
                            
                        elif action == "select_option":
                            selector = data.get("selector")
                            value = data.get("value")
                            page.select_option(selector, value)
                            await ws.send(json.dumps({"status": "done"}))
                            
                        elif action == "get_url":
                            await ws.send(json.dumps({
                                "status": "url", 
                                "url": page.url
                            }))
                            
                        elif action == "wait_for_navigation":
                            print("⏳ Waiting for navigation...")
                            page.wait_for_load_state("networkidle")
                            await ws.send(json.dumps({"status": "done", "url": page.url}))
                            
                    except Exception as e:
                        error_msg = str(e)
                        print(f"❌ Error: {error_msg}")
                        await ws.send(json.dumps({"status": "error", "error": error_msg}))
                        
        except websockets.exceptions.ConnectionClosed:
            print("❌ Connection to Render lost")
        except Exception as e:
            print(f"❌ Error: {e}")
        finally:
            browser.close()
            print("🔒 Browser closed")

if __name__ == "__main__":
    if "YOUR-RENDER-APP" in RENDER_SERVER:
        print("⚠️  WARNING: Update RENDER_SERVER with your actual Render URL!")
        print("Example: wss://my-gst-app.onrender.com")
        exit(1)
    
    asyncio.run(browser_handler())
'''
    
    from flask import Response
    return Response(
        bridge_code,
        mimetype="text/plain",
        headers={"Content-Disposition": "attachment; filename=browser_bridge.py"}
    )

# ═══════════════════════════════════════════════════════════════════
# WORKERS (Your existing code)
# ═══════════════════════════════════════════════════════════════════

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
        log("⭐ Full access — processing all uploaded files")
        prog(5)

        for fpath in saved["r1"]:
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1: {mon} {yr}"); set_dl(f"{mon}_GSTR1", "OK")
            else:
                log(f"  ⚠ Month not detected in: {Path(fpath).name}", "warn")

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

        for fpath in saved["taxlib"]:
            dest = job_dir / f"TAX_LIABILITY_{Path(fpath).name}"
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))
            log(f"  Tax Liability: {Path(dest).name}"); break

        prog(25)

        suite_path = _find_engine("gst_suite_final.py")
        if not suite_path:
            raise FileNotFoundError("gst_suite_final.py not found on server.")

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
        log("Running annual reconciliation (1–2 minutes)...")
        gst.write_annual_reconciliation(str(job_dir), client_name, gstin, _log)
        prog(65)
        log("  ✓ Annual reconciliation complete", "ok")

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
            log("⚠ gstr1_extract.py not on server — GSTR-1 detail skipped", "warn")
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
            raise RuntimeError("No Excel output generated.")

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
            raise RuntimeError("No GSTR1_*.zip files found.")

        extract_path = _find_engine("gstr1_extract.py")
        if not extract_path:
            raise FileNotFoundError("gstr1_extract.py not found on server.")

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
