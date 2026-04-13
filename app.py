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
    """Server-side mode — always connected"""
    return True

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
                from flask import Response
                return Response('{"error":"Too many requests. Wait 1 minute."}',
                    status=429, mimetype="application/json")
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
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="color-scheme" content="dark">
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
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
h1{font-size:clamp(1.5rem,3.2vw,2.2rem);font-weight:800;letter-spacing:-.02em;line-height:1.1}
h1 span{background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
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
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}

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
  <button class="tb" onclick="switchTab('bulk',event)">📋 Bulk Download</button>
  <button class="tb" onclick="switchTab('itrecon',event)">🏦 Income Tax</button>
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
    <div class="dz" id="zone-r1a">
      <div class="dz-ic">📋</div><div class="dz-lb">GSTR-1A</div>
      <div class="dz-ht">ZIP files (amendments)</div>
      <div class="dz-cn" id="cnt-r1a">No files</div>
      <input type="file" multiple accept=".zip,.json" data-zone="r1a" onchange="updateZone('r1a',this)">
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
<div class="card" style="display:flex;gap:.65rem;align-items:stretch;flex-wrap:wrap">
  <button type="submit" class="btn" id="r-submit" style="flex:1;margin-top:0">Generate Reconciliation + GSTR-1 Detail →</button>
  <button type="button" onclick="resetRecon()" id="r-reset"
          style="flex:0 0 auto;padding:.8rem 1.4rem;background:var(--surf2);
                 border:1px solid var(--red);border-radius:10px;color:var(--red);
                 font-family:var(--sans);font-size:.82rem;font-weight:700;
                 cursor:pointer;transition:all .15s;white-space:nowrap;margin-top:0"
          title="Clear all files and reset the form">
    🔄 Reset
  </button>
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
    <span class="pill">GSTR-1</span><span class="pill">GSTR-1A</span>
    <span class="pill">GSTR-2B</span><span class="pill">GSTR-2A</span><span class="pill">GSTR-3B</span>
  </div>
  <div class="info-box" style="margin-top:.7rem">
    <strong>How it works:</strong> Login to GST portal in your browser → copy your session token → paste it here. The server uses that token to download all your returns automatically. <strong style="color:var(--txt)">No CAPTCHA issues. No software needed.</strong>
  </div>
</div>

<!-- Step 1: How it works -->
<div class="card" id="ad-step1">
  <div class="ct">How It Works</div>
  <div style="font-size:.78rem;color:var(--muted);line-height:1.9">
    1. Fill in your GSTIN, Company Name, Username &amp; Password below<br>
    2. Click <strong style="color:var(--org)">🚀 Start Auto Download</strong><br>
    3. A CAPTCHA screenshot appears here — type it and click <strong style="color:var(--accent)">Submit</strong><br>
    4. Server logs into GST portal and downloads all returns automatically<br>
    5. Files appear below — also auto-loaded into the <strong style="color:var(--txt)">Reconciliation tab</strong>
  </div>
  <div class="info-box" style="margin-top:.8rem;font-size:.72rem">
    Your password is used only for this session and is never stored.
  </div>
</div>

<!-- Step 2: Enter details -->
<form id="ad-form">
<div class="card">
  <div class="ct">Enter Your GST Portal Details</div>
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
        <option value="all">All Returns (GSTR-1, 1A, 2B, 2A, 3B)</option>
        <option value="gstr1">GSTR-1 Only</option>
        <option value="gstr1a">GSTR-1A Only</option>
        <option value="gstr2b">GSTR-2B Only</option>
        <option value="gstr2a">GSTR-2A Only</option>
        <option value="gstr3b">GSTR-3B Only</option>
      </select></div>
  </div>
</div>
<div class="card">
  <button type="submit" class="btn-orange" id="ad-submit">🚀 Start Auto Download</button>
</div>
</form>

<!-- Progress & logs -->
<div class="card pw" id="ad-pw">
  <div class="ct">Progress <span class="sbg s-p pulse" id="ad-badge">Running</span>
    <a id="ad-screenshot-link" href="#" target="_blank"
       style="display:none;margin-left:.8rem;font-size:.7rem;padding:.2rem .6rem;
              background:rgba(0,229,255,.12);border:1px solid var(--accent);border-radius:5px;
              color:var(--accent);text-decoration:none;font-family:var(--mono)">
      🖥 View Live Screenshot
    </a>
  </div>
  <div class="pb-w"><div class="pb" id="ad-pb"></div></div>
  <div class="lb" id="ad-lb"></div>

  <!-- Live downloaded files tracker — shown inside progress card as files arrive -->
  <div id="ad-live-files" style="display:none;margin-top:.85rem">
    <div style="font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.07em;
                color:var(--grn);margin-bottom:.5rem;display:flex;align-items:center;gap:.45rem">
      <span style="width:7px;height:7px;border-radius:50%;background:var(--grn);
                   display:inline-block;box-shadow:0 0 6px var(--grn);animation:pulse 1.2s infinite"></span>
      Downloaded So Far
      <span id="ad-live-count" style="color:var(--muted);font-weight:400;font-family:var(--mono)"></span>
    </div>
    <div id="ad-live-grid"
         style="display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:.5rem"></div>
  </div>
</div>

<!-- CAPTCHA card (shown when server browser needs CAPTCHA input) -->
<div class="card" id="ad-captcha-card" style="display:none">
  <div class="ct">🔐 CAPTCHA Required — Type Below</div>
  <div class="info-box" style="margin-bottom:.8rem;font-size:.75rem">
    The server has opened GST portal and needs you to solve the CAPTCHA.<br>
    <strong style="color:var(--txt)">Type exactly what you see in the image, then click Submit.</strong>
  </div>
  <div style="text-align:center;margin-bottom:.8rem">
    <img id="ad-captcha-img" src="" alt="CAPTCHA"
         style="max-width:100%;border-radius:8px;border:2px solid var(--accent);
                background:#fff;padding:4px;cursor:pointer"
         title="Click to refresh screenshot"
         onclick="refreshAdCaptcha()">
    <div style="font-size:.68rem;color:var(--muted);margin-top:.35rem;font-family:var(--mono)">
      Click image to refresh screenshot
    </div>
  </div>
  <div style="display:flex;gap:.5rem;align-items:center">
    <input type="text" id="ad-captcha-input"
           placeholder="Type CAPTCHA letters here"
           style="flex:1;font-size:.85rem;letter-spacing:.15em;text-transform:uppercase"
           onkeydown="if(event.key==='Enter')submitAdCaptcha()">
    <button onclick="submitAdCaptcha()"
            style="padding:.55rem 1.2rem;background:linear-gradient(135deg,var(--accent),var(--accent2));
                   border:none;border-radius:8px;color:#000;font-weight:800;font-size:.82rem;
                   cursor:pointer;white-space:nowrap">
      Submit →
    </button>
  </div>
  <div id="ad-captcha-err" style="color:var(--red);font-size:.72rem;margin-top:.4rem;font-family:var(--mono)"></div>
</div>

<!-- Re-login card (shown if token expires mid-download) -->
<div class="card" id="ad-relogin-card" style="display:none">
  <div class="ct">🔄 Token Expired — Re-login Required</div>
  <div class="info-box warn" style="margin-bottom:.75rem;font-size:.74rem">
    Your GST portal session expired during download.<br>
    <strong>Option A (Easy):</strong> <a href="https://services.gst.gov.in/services/login" target="_blank"
       style="color:var(--accent);font-weight:700">Login to GST Portal again →</a>
    then click the <strong style="color:#a78bfa">🔖 GST Token Capture</strong> bookmark — token will be sent automatically<br>
    <strong>Option B (Manual):</strong> Press F12 → Application → Cookies → copy fresh <strong>AuthToken</strong> → paste below
  </div>
  <div class="fg">
    <label>Fresh AuthToken *</label>
    <input type="text" id="ad-relogin-token" placeholder="Paste new AuthToken here" style="font-size:.72rem">
  </div>
  <button class="btn" onclick="submitAdRelogin()" id="ad-relogin-btn" style="margin-top:.5rem">
    Submit New Token →
  </button>
</div>

<!-- Download results -->
<div class="card dw" id="ad-dw">
  <div class="ct">✅ Downloaded Files
    <span id="ad-file-count" style="font-size:.7rem;color:var(--muted);font-family:var(--mono);margin-left:.4rem"></span>
  </div>
  <div class="dl-g" id="ad-dlg"></div>
  <div style="margin-top:1rem;padding:.85rem;background:rgba(0,229,255,.04);border:1px solid rgba(0,229,255,.15);border-radius:9px">
    <div style="font-size:.75rem;font-weight:700;color:var(--accent);margin-bottom:.5rem;text-transform:uppercase;letter-spacing:.05em">
      📤 Send to Reconciliation Tab
    </div>
    <p style="font-size:.72rem;color:var(--muted);line-height:1.6;margin-bottom:.65rem">
      Click below to automatically load all downloaded return files (GSTR-1, 2B, 2A, 3B) directly into the Reconciliation tab — no manual upload needed.
    </p>
    <div style="display:flex;gap:.6rem;flex-wrap:wrap;align-items:center">
      <button onclick="adTransferToRecon()" id="ad-transfer-btn"
              style="padding:.65rem 1.4rem;background:linear-gradient(135deg,var(--grn),#00c853);
                     border:none;border-radius:9px;color:#000;font-weight:800;font-size:.85rem;
                     cursor:pointer;transition:transform .15s;letter-spacing:.03em"
              onmouseover="this.style.transform='translateY(-2px)'"
              onmouseout="this.style.transform=''">
        📤 Load into Reconciliation Tab →
      </button>
      <span id="ad-transfer-status" style="font-size:.7rem;color:var(--muted);font-family:var(--mono)"></span>
    </div>
  </div>
  <p style="color:var(--muted);font-size:.68rem;margin-top:.65rem;font-family:var(--mono)">
    ⏳ Files deleted after 2 hours. Download ZIP before closing.
  </p>
</div>

<!-- Failure Screenshots Panel (auto download tab) -->
<div class="card" id="ad-fail-shots" style="display:none">
  <div class="ct" style="color:var(--red)">
    📸 Failure Screenshots
    <span id="ad-fail-count" style="font-size:.7rem;color:var(--muted);font-family:var(--mono);margin-left:.4rem"></span>
    <button onclick="refreshFailShots()"
            style="margin-left:auto;padding:.22rem .65rem;background:var(--surf2);
                   border:1px solid var(--bdr);border-radius:6px;color:var(--muted);
                   font-family:var(--mono);font-size:.65rem;cursor:pointer">
      🔄 Refresh
    </button>
  </div>
  <p style="color:var(--muted);font-size:.74rem;line-height:1.65;margin-bottom:.85rem">
    These screenshots were captured <strong style="color:var(--txt)">automatically</strong>
    at each point where a download failed or a button was not found —
    so you can see exactly what the GST portal showed at that moment.
    Share these with support if downloads are not working.
  </p>
  <div id="ad-fail-grid" style="display:flex;flex-direction:column;gap:1.1rem"></div>
</div>

</div><!-- /tab-autodl -->

<!-- ══ TAB 5: BULK DOWNLOAD ══ -->
<div class="tp" id="tab-bulk">

<div class="card">
  <div class="ct">📋 Bulk Download — Multiple Companies</div>
  <div class="pills">
    <span class="pill">Multiple GSTINs</span><span class="pill">One by one</span>
    <span class="pill">CAPTCHA per company</span><span class="pill">Auto ZIP output</span>
  </div>
  <div class="info-box" style="margin-top:.75rem">
    <strong>How it works:</strong><br>
    1. Download the Excel template below → fill in your company list<br>
    2. Upload it here → Click Start Bulk Download<br>
    3. For each company, login to GST portal → copy AuthToken → paste here → Download proceeds<br>
    4. All files are saved and available to download as a ZIP<br><br>
    <strong style="color:var(--grn)">✅ No limit on number of companies. Each uses its own login session.</strong>
  </div>
  <a href="/api/bulk-template" class="btn-dl"
     style="display:inline-block;padding:.55rem 1.1rem;margin-top:.5rem">
    ⬇ Download Excel Template
  </a>
</div>

<div class="card">
  <div class="ct">Upload Company List</div>
  <div class="fg2">
    <div class="fg">
      <label>Company List Excel *</label>
      <div class="dz" id="zone-bulk" style="min-height:70px;flex-direction:row;padding:.6rem .75rem;gap:.65rem">
        <div class="dz-ic" style="font-size:1.2rem">📊</div>
        <div style="text-align:left">
          <div class="dz-lb">companies.xlsx</div>
          <div class="dz-cn" id="cnt-bulk">No file</div>
        </div>
        <input type="file" accept=".xlsx,.xls" data-zone="bulk" onchange="updateZone('bulk',this)">
      </div>
    </div>
    <div class="fg">
      <label>Financial Year</label>
      <select id="bulk-fy">
        <option value="2025-26">2025-26</option>
        <option value="2024-25">2024-25</option>
        <option value="2023-24">2023-24</option>
        <option value="2022-23">2022-23</option>
      </select>
    </div>
    <div class="fg">
      <label>Returns to Download</label>
      <select id="bulk-returns">
        <option value="all">All Returns (GSTR-1, 1A, 2B, 2A, 3B)</option>
        <option value="gstr1">GSTR-1 Only</option>
        <option value="gstr2b">GSTR-2B Only</option>
        <option value="gstr2a">GSTR-2A Only</option>
        <option value="gstr3b">GSTR-3B Only</option>
      </select>
    </div>
  </div>
  <button class="btn-orange" onclick="startBulk()" id="bulk-submit" style="margin-top:.5rem">
    🚀 Start Bulk Download
  </button>
</div>

<!-- Per-company CAPTCHA card (shown one at a time) -->
<div class="card" id="bulk-captcha-card" style="display:none">
  <div class="ct">🔐 Login Required — <span id="bulk-co-name" style="color:var(--accent)"></span></div>
  <div class="info-box" style="margin-bottom:.8rem;font-size:.75rem">
    <strong>Step 1:</strong>
    <a href="https://services.gst.gov.in/services/login" target="_blank"
       style="color:var(--accent);font-weight:700">Open GST Portal Login →</a>
    login with username &amp; password + CAPTCHA<br>
    <strong>Step 2:</strong> Press F12 → Application → Cookies → copy <strong>AuthToken</strong><br>
    <strong>Step 3:</strong> Paste it below and click Submit
  </div>
  <div class="fg2">
    <div class="fg">
      <label>GSTIN</label>
      <input type="text" id="bulk-cap-gstin" readonly style="opacity:.6">
    </div>
    <div class="fg">
      <label>Username</label>
      <input type="text" id="bulk-cap-user" readonly style="opacity:.6">
    </div>
  </div>
  <div class="fg" style="margin-top:.6rem">
    <label>AuthToken from GST Portal Cookies *</label>
    <input type="text" id="bulk-token-input" placeholder="Paste AuthToken here"
           style="font-size:.72rem">
  </div>
  <button class="btn" onclick="submitBulkToken()" id="bulk-token-btn" style="margin-top:.5rem">
    Submit Token &amp; Download →
  </button>
  <div id="bulk-token-err" style="color:var(--red);font-size:.72rem;margin-top:.4rem;font-family:var(--mono)"></div>
</div>

<!-- Progress -->
<div class="card" id="bulk-pw" style="display:none">
  <div class="ct">Bulk Progress <span class="sbg s-p pulse" id="bulk-badge">Running</span>
    <span id="bulk-counter" style="font-size:.7rem;color:var(--muted);font-family:var(--mono);margin-left:.5rem"></span>
  </div>
  <div class="pb-w"><div class="pb" id="bulk-pb"></div></div>
  <div class="lb" id="bulk-lb"></div>
</div>

<!-- Results -->
<div class="card" id="bulk-dw" style="display:none">
  <div class="ct">✅ Bulk Download Complete</div>
  <div class="dl-g" id="bulk-dlg"></div>
  <p style="color:var(--muted);font-size:.66rem;margin-top:.65rem;font-family:var(--mono)">
    ⏳ Files deleted after 2 hours. Download the ZIP and upload each company's files to Reconciliation tab.
  </p>
</div>

</div><!-- /tab-bulk -->

<!-- ══ TAB 6: INCOME TAX RECONCILIATION ══ -->
<div class="tp" id="tab-itrecon">

<div class="info-box">
  <strong>Income Tax Reconciliation — What this does:</strong>
  Upload your <strong>26AS PDF</strong> (from IT Portal → e-File → Income Tax Returns → View Form 26AS)
  and optionally your <strong>AIS PDF</strong> (IT Portal → Services → Annual Information Statement).
  <br><br>
  The portal generates an Excel with 9 sheets:
  <strong>IT_Summary</strong> (key figures with TIS vs GSTR-1 comparison) |
  <strong>TDS_26AS_Detail</strong> (all deductors with every transaction line) |
  <strong>TIS_vs_GSTR_Annual</strong> (TIS vs GSTR-1, GSTR-1A & GSTR-3B — includes CDNR/Debit/Amendments) |
  <strong>TIS_vs_GSTR_Monthly</strong> (12 months × 2 GSTINs, APR→MAR, with GSTR-1A & 3B) |
  <strong>Purchase_Detail</strong> (all supplier transactions grouped by supplier with totals) |
  <strong>AIS_vs_Turnover</strong> (full reconciliation with blank rows for manual differences) |
  <strong>Advance_Tax_Challan</strong> (Part B3 tax paid details) |
  <strong>AIS_vs_GSTR_Monthly</strong> (12 months × 2 GSTINs — GSTR-1, GSTR-1A, GSTR-3B Sales & Purchases) |
  <strong>IT_Filing_Checklist</strong> (40-item ITR verification checklist with auto-detected data).
  <br><br>
  <strong>Files are auto-deleted after 2 hours. Nothing stored permanently.</strong>
</div>

<form id="it-form">
<div class="card">
  <div class="ct">Company Details</div>
  <div class="fg2">
    <div class="fg"><label>Company Name *</label>
      <input type="text" id="it-name" placeholder="ABC Traders Pvt Ltd" required></div>
    <div class="fg"><label>PAN *</label>
      <input type="text" id="it-pan" placeholder="ABCDE1234F" maxlength="10"
             style="text-transform:uppercase" required></div>
    <div class="fg"><label>GSTIN (linked to PAN)</label>
      <input type="text" id="it-gstin" placeholder="33ABCDE1234F1ZX" maxlength="15"
             style="text-transform:uppercase"></div>
    <div class="fg"><label>Financial Year</label>
      <select id="it-fy">
        <option value="2024-25">2024-25</option>
        <option value="2025-26" selected>2025-26</option>
        <option value="2023-24">2023-24</option>
        <option value="2022-23">2022-23</option>
      </select>
    </div>
  </div>
</div>

<div class="card">
  <div class="ct">Upload Files</div>
  <div class="dg">
    <div class="dz" id="zone-it26as">
      <div class="dz-ic">📄</div>
      <div class="dz-lb">Form 26AS</div>
      <div class="dz-ht">PDF from IT Portal</div>
      <div class="dz-cn" id="cnt-it26as">No file</div>
      <input type="file" accept=".pdf" data-zone="it26as" onchange="updateZone('it26as',this)">
    </div>
    <div class="dz" id="zone-itais">
      <div class="dz-ic">📊</div>
      <div class="dz-lb">AIS PDF</div>
      <div class="dz-ht">Annual Info Statement</div>
      <div class="dz-cn" id="cnt-itais">No file (optional)</div>
      <input type="file" accept=".pdf" data-zone="itais" onchange="updateZone('itais',this)">
    </div>
    <div class="dz" id="zone-itgst">
      <div class="dz-ic">📋</div>
      <div class="dz-lb">GST Recon Excel</div>
      <div class="dz-ht">Output from Tab 1 (optional)</div>
      <div class="dz-cn" id="cnt-itgst">No file (optional)</div>
      <input type="file" accept=".xlsx,.xls" data-zone="itgst" onchange="updateZone('itgst',this)">
    </div>
  </div>
  <div class="info-box" style="margin-top:.75rem;font-size:.74rem">
    <strong>How to download 26AS:</strong>
    IT Portal (incometax.gov.in) → Login → e-File → Income Tax Returns → View Form 26AS →
    Select Assessment Year → Export to PDF<br>
    <strong>How to download AIS:</strong>
    IT Portal → Services → Annual Information Statement (AIS) → Download PDF
  </div>
</div>

<div class="card" style="display:flex;gap:.65rem;align-items:stretch;flex-wrap:wrap">
  <button type="submit" class="btn" id="it-submit" style="flex:1;margin-top:0">
    Generate IT Reconciliation Excel →
  </button>
  <button type="button" onclick="resetIT()" id="it-reset"
          style="flex:0 0 auto;padding:.8rem 1.4rem;background:var(--surf2);
                 border:1px solid var(--red);border-radius:10px;color:var(--red);
                 font-family:var(--sans);font-size:.82rem;font-weight:700;
                 cursor:pointer;transition:all .15s;white-space:nowrap;margin-top:0"
          title="Clear all files and reset">
    🔄 Reset
  </button>
</div>
</form>

<!-- Progress -->
<div class="card pw" id="it-pw">
  <div class="ct">Processing <span class="sbg s-p pulse" id="it-badge">Running</span></div>
  <div class="pb-w"><div class="pb" id="it-pb"></div></div>
  <div class="lb" id="it-lb"></div>
</div>

<!-- Downloads -->
<div class="card dw" id="it-dw">
  <div class="ct">✅ IT Reconciliation Ready</div>
  <div class="dl-g" id="it-dlg"></div>
  <p style="color:var(--muted);font-size:.66rem;margin-top:.65rem;font-family:var(--mono)">
    ⏳ Files deleted automatically after 2 hours. Download before closing.
  </p>
</div>

</div><!-- /tab-itrecon -->


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
    try{const dt=new DataTransfer();[...e.dataTransfer.files].forEach(f=>dt.items.add(f));inp.files=dt.files;}catch(_){}
    updateZone(inp.dataset.zone,inp);
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
  const hasFiles=['r1','r1a','r2b','r2a','r3b','cust','taxlib'].some(z=>(zoneFiles[z]||[]).length>0);
  if(!hasFiles){alert('Upload at least one return file');return;}
  const fd=new FormData();
  fd.append('gstin',gstin);fd.append('client_name',cname);
  fd.append('fy',fy);fd.append('mode','recon');
  ['r1','r1a','r2b','r2a','r3b','cust','taxlib'].forEach(z=>
    (zoneFiles[z]||[]).forEach(f=>fd.append('files_'+z,f)));
  await startJob(fd,'r','Generate Reconciliation + GSTR-1 Detail →');
});

// ── Recon reset ───────────────────────────────────────────────────
function resetRecon(){
  // Clear text inputs
  document.getElementById('r-gstin').value='';
  document.getElementById('r-name').value='';
  document.getElementById('r-fy').value='2025-26';
  document.getElementById('r-state').value='';

  // Clear all file zones for reconciliation tab
  ['r1','r1a','r2b','r2a','r3b','cust','taxlib'].forEach(zone=>{
    zoneFiles[zone]=[];
    const cnt=document.getElementById('cnt-'+zone);
    const el=document.getElementById('zone-'+zone);
    if(cnt) cnt.textContent='No file'+(zone==='r1'||zone==='r1a'||zone==='r2b'||zone==='r2a'||zone==='r3b'?'s':'');
    if(el){
      el.classList.remove('has-files');
      const inp=el.querySelector('input[type=file]');
      if(inp) inp.value='';
    }
  });

  // Hide progress & download panels
  const pw=document.getElementById('r-pw');
  const dw=document.getElementById('r-dw');
  if(pw) pw.style.display='none';
  if(dw) dw.style.display='none';

  // Re-enable submit button
  const btn=document.getElementById('r-submit');
  if(btn){btn.disabled=false;btn.textContent='Generate Reconciliation + GSTR-1 Detail →';}

  // Clear log & progress bar
  const lb=document.getElementById('r-lb');
  const pb=document.getElementById('r-pb');
  if(lb) lb.innerHTML='';
  if(pb) pb.style.width='0%';

  // Reset badge
  setBadge('r','p','Running');
}

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
    let d;try{d=await res.json();}catch(_){throw new Error('Server error (HTTP '+res.status+'). Try again.');}
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
    let d;try{d=await res.json();}catch(_){setTimeout(()=>pollJob(jid,pfx,btnLbl),3000);return;}
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
function checkBrowserConnection(){}

document.getElementById('ad-form').addEventListener('submit',async e=>{
  e.preventDefault();
  const gstin=document.getElementById('ad-gstin').value.trim().toUpperCase();
  const cname=document.getElementById('ad-name').value.trim();
  const username=document.getElementById('ad-username').value.trim();
  const password=document.getElementById('ad-password').value;
  const token='';
  const fy=document.getElementById('ad-fy').value;
  const returns=document.getElementById('ad-returns').value;
  if(!gstin||gstin.length!==15){alert('Enter valid 15-char GSTIN');return;}
  if(!cname){alert('Enter company name');return;}
  if(!username){alert('Enter username');return;}
  if(!password){alert('Enter your GST portal password');return;}
  document.getElementById('ad-pw').style.display='block';
  document.getElementById('ad-dw').style.display='none';
  document.getElementById('ad-lb').innerHTML='';
  document.getElementById('ad-pb').style.width='0%';
  setBadge('ad','p','Running');
  // Reset failure screenshots panel for new job
  _failShotsRendered = 0;
  document.getElementById('ad-fail-shots').style.display='none';
  document.getElementById('ad-fail-grid').innerHTML='';
  document.getElementById('ad-fail-count').textContent='';
  // Reset live file tracker
  const livePanel = document.getElementById('ad-live-files');
  if(livePanel){ livePanel.style.display='none'; }
  const liveGrid = document.getElementById('ad-live-grid');
  if(liveGrid){ liveGrid.innerHTML=''; }
  const liveCount = document.getElementById('ad-live-count');
  if(liveCount){ liveCount.textContent=''; }
  const btn=document.getElementById('ad-submit');
  btn.disabled=true;btn.textContent='Starting…';
  addLog('ad','info','Starting browser on server — GST portal login in progress...');
  try{
    const res=await fetch('/api/auto-download',{method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({gstin,client_name:cname,username,password,token,fy,returns})});
    let d;try{d=await res.json();}catch(_){
      addLog('ad','err','Server error — try again');setBadge('ad','e','Failed');
      btn.disabled=false;btn.textContent='🚀 Start Auto Download';return;}
    if(d.error){addLog('ad','err',d.error);setBadge('ad','e','Failed');
      btn.disabled=false;btn.textContent='🚀 Start Auto Download';return;}
    _adJobId=d.job_id;
    // Show live screenshot link
    const ssLink=document.getElementById('ad-screenshot-link');
    if(ssLink){ssLink.href='/api/debug-screenshot/'+d.job_id;ssLink.style.display='inline';}
    btn.textContent='Running…';
    _adPoll(_adJobId);
  }catch(err){addLog('ad','err','Network error: '+err.message);setBadge('ad','e','Failed');
    btn.disabled=false;btn.textContent='🚀 Start Auto Download';}
});

async function _adPoll(jid){
  try{
    const r=await fetch('/api/job/'+jid);
    let d;try{d=await r.json();}catch(_){setTimeout(()=>_adPoll(jid),3000);return;}
    if(d.logs)d.logs.forEach(l=>addLog('ad',l.type,l.msg));
    if(d.progress!=null)document.getElementById('ad-pb').style.width=d.progress+'%';

    const captchaCard = document.getElementById('ad-captcha-card');
    const reloginCard = document.getElementById('ad-relogin-card');

    // Show CAPTCHA card when server has a screenshot waiting
    if(captchaCard){
      const wasHidden = captchaCard.style.display === 'none' || captchaCard.style.display === '';
      if(d.captcha_needed && d.captcha_img){
        // Update image
        document.getElementById('ad-captcha-img').src = 'data:image/png;base64,' + d.captcha_img;
        captchaCard.style.display = 'block';
        if(wasHidden){
          captchaCard.scrollIntoView({behavior:'smooth', block:'nearest'});
          document.getElementById('ad-captcha-input').focus();
        }
      } else {
        captchaCard.style.display = 'none';
        document.getElementById('ad-captcha-input').value = '';
      }
    }

    if(reloginCard){
      const wasHidden2 = reloginCard.style.display === 'none' || reloginCard.style.display === '';
      if(d.captcha_needed && !d.captcha_img){
        reloginCard.style.display = 'block';
        if(wasHidden2) reloginCard.scrollIntoView({behavior:'smooth', block:'nearest'});
      } else {
        reloginCard.style.display = 'none';
      }
    }

    // Show files as they arrive during download (live update)
    if(d.files && d.files.length){
      // Always keep _adLastJobId/_adLastFiles up to date so transfer works mid-run
      _adLastJobId = jid;
      _adLastFiles = d.files;

      // ── Live tracker inside the progress card ───────────────────
      const livePanel = document.getElementById('ad-live-files');
      const liveGrid  = document.getElementById('ad-live-grid');
      const liveCount = document.getElementById('ad-live-count');
      if(livePanel && liveGrid){
        livePanel.style.display = 'block';
        // Only render newly added files (append, don't rebuild)
        const rendered = liveGrid.children.length;
        const incoming = d.files.filter(f => !f.name.startsWith('GST_Downloads')); // skip master zip
        if(incoming.length > rendered){
          const newFiles = incoming.slice(rendered);
          newFiles.forEach(f => {
            const icon = f.name.endsWith('.pdf') ? '📄' : f.name.includes('GSTR3B') ? '📄'
                       : f.name.endsWith('.zip') || f.name.endsWith('.json') ? '🗜' : '📊';
            const retType = f.name.split('_')[0] || '';
            const month   = f.name.split('_')[1] || '';
            const label   = retType + (month ? ' · ' + month : '');
            const chip = document.createElement('div');
            chip.style.cssText = `background:rgba(0,230,118,.07);border:1px solid rgba(0,230,118,.25);
              border-radius:8px;padding:.45rem .65rem;display:flex;align-items:center;gap:.45rem;
              animation:fadeIn .4s ease`;
            chip.innerHTML = `<span style="font-size:1.1rem;line-height:1">${icon}</span>
              <div style="min-width:0">
                <div style="font-size:.68rem;font-weight:700;color:var(--grn);font-family:var(--mono);
                            white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="${f.name}">${label}</div>
                <div style="font-size:.6rem;color:var(--muted);font-family:var(--mono)">${f.size||''}</div>
              </div>
              <a href="/api/dl-file/${jid}/${encodeURIComponent(f.name)}" download
                 style="margin-left:auto;font-size:.65rem;color:var(--accent);text-decoration:none;
                        white-space:nowrap;font-family:var(--mono)" title="Download ${f.name}">⬇</a>`;
            liveGrid.appendChild(chip);
          });
          if(liveCount) liveCount.textContent = `(${incoming.length} file${incoming.length>1?'s':''})`;
        }
      }

      // ── Full downloads card below (shown when done) ─────────────
      const grid = document.getElementById('ad-dlg');
      const sec  = document.getElementById('ad-dw');
      if(sec) sec.style.display = 'block';
      if(grid && grid.children.length !== d.files.length){
        grid.innerHTML = '';
        d.files.forEach(f => {
          const icon = f.name.endsWith('.pdf') ? '📄' : f.name.endsWith('.zip') ? '🗜' : '📊';
          const c = document.createElement('div'); c.className = 'dlc';
          c.innerHTML = `<div style="font-size:1.4rem">${icon}</div>
            <div class="dl-n">${f.name}</div>
            <div class="dl-s">${f.size||''}</div>
            <a href="/api/dl-file/${jid}/${encodeURIComponent(f.name)}" class="btn-dl" download>⬇ Download</a>`;
          grid.appendChild(c);
        });
        // Show transfer button as soon as first files arrive
        const transferBtn = document.querySelector('[onclick="adTransferToRecon()"]');
        if(transferBtn) transferBtn.style.display='inline-block';
      }
    }

    // Live-update failure screenshots during polling
    if(d.failure_screenshots && d.failure_screenshots.length > _failShotsRendered){
      _renderFailShots(d.failure_screenshots);
    }

    if(d.status==='done'){
      setBadge('ad','d','Complete');
      document.getElementById('ad-pb').style.width='100%';
      document.getElementById('ad-submit').disabled=false;
      document.getElementById('ad-submit').textContent='🚀 Start Auto Download';
      if(captchaCard) captchaCard.style.display='none';
      if(reloginCard) reloginCard.style.display='none';
      // Stop pulsing dot on live tracker and update label
      const livePanel = document.getElementById('ad-live-files');
      if(livePanel){
        const dot = livePanel.querySelector('span[style*="border-radius:50%"]');
        if(dot){ dot.style.animation='none'; dot.style.background='var(--grn)'; }
        const lbl = livePanel.querySelector('div[style*="color:var(--grn)"]');
        if(lbl){ const t=lbl.firstChild; if(t&&t.nodeType===3) t.textContent=''; }
        const hdr = livePanel.querySelector('div[style*="color:var(--grn)"]');
        if(hdr) hdr.innerHTML = hdr.innerHTML.replace('Downloaded So Far','✅ Download Complete');
      }
      // Auto-fill Download Status job ID
      const dsJid = document.getElementById('ds-jid');
      if(dsJid) dsJid.value = jid;
      _adShowFiles(jid, d.files);
      // Final refresh of failure screenshots
      refreshFailShots();
      return;
    }
    if(d.status==='error'){
      addLog('ad','err',d.error||'Unknown error');setBadge('ad','e','Failed');
      document.getElementById('ad-submit').disabled=false;
      document.getElementById('ad-submit').textContent='🚀 Start Auto Download';
      if(captchaCard) captchaCard.style.display='none';
      if(reloginCard) reloginCard.style.display='none';
      return;
    }
    setTimeout(()=>_adPoll(jid),1500);
  }catch(e){setTimeout(()=>_adPoll(jid),3000);}
}

async function submitAdCaptcha(){
  const text=document.getElementById('ad-captcha-input').value.trim();
  if(!text){document.getElementById('ad-captcha-err').textContent='Please type the CAPTCHA first';return;}
  document.getElementById('ad-captcha-err').textContent='';
  try{
    const res=await fetch(`/api/captcha-submit/${_adJobId}`,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({captcha:text})});
    const d=await res.json();
    if(d.ok){
      document.getElementById('ad-captcha-input').value='';
      addLog('ad','ok','CAPTCHA submitted — logging in...');
    } else {
      document.getElementById('ad-captcha-err').textContent='Error: '+(d.error||'Failed');
    }
  }catch(err){document.getElementById('ad-captcha-err').textContent='Network error: '+err.message;}
}

async function refreshAdCaptcha(){
  if(!_adJobId)return;
  try{
    const res=await fetch(`/api/captcha-refresh/${_adJobId}`,{method:'POST'});
    const d=await res.json();
    if(d.img)document.getElementById('ad-captcha-img').src='data:image/png;base64,'+d.img;
  }catch(e){}
}

async function submitAdRelogin(){
  const token=document.getElementById('ad-relogin-token').value.trim();
  if(!token){alert('Paste the new AuthToken');return;}
  const btn=document.getElementById('ad-relogin-btn');
  btn.disabled=true;btn.textContent='Submitting…';
  try{
    const res=await fetch(`/api/captcha-submit/${_adJobId}`,{
      method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({captcha:token})});
    const d=await res.json();
    if(d.ok){
      document.getElementById('ad-relogin-card').style.display='none';
      document.getElementById('ad-relogin-token').value='';
      addLog('ad','ok','Re-login submitted — resuming download…');
    } else {
      alert('Error: '+(d.error||'Failed'));
    }
  }catch(err){alert('Network error: '+err.message);}
  btn.disabled=false;btn.textContent='Submit New Token →';
}

function showBmInstr(browser){
  document.querySelectorAll('.bm-instr').forEach(el=>el.style.display='none');
  const el=document.getElementById('bm-instr-'+browser);
  if(el)el.style.display='block';
}
function bookmarkletClick(e){e.preventDefault();}


// Store job files globally for transfer
let _adLastJobId = '';
let _adLastFiles = [];

function _adShowFiles(jid, files){
  _adLastJobId = jid;
  _adLastFiles = files || [];
  const sec = document.getElementById('ad-dw'), grid = document.getElementById('ad-dlg');
  sec.style.display = 'block'; grid.innerHTML = '';
  // Update file count badge
  const countEl = document.getElementById('ad-file-count');
  const individual = (files||[]).filter(f=>!f.name.startsWith('GST_Downloads'));
  if(countEl) countEl.textContent = individual.length ? `(${individual.length} return file${individual.length>1?'s':''} + ZIP)` : '';
  if(!files || !files.length){
    grid.innerHTML = '<p style="color:var(--muted);font-size:.8rem">No files downloaded. Check logs above.</p>';
    return;
  }
  files.forEach(f => {
    const icon = f.name.endsWith('.pdf') ? '📄' : f.name.endsWith('.zip') ? '🗜' : '📊';
    const c = document.createElement('div'); c.className = 'dlc';
    c.innerHTML = `<div style="font-size:1.4rem">${icon}</div>
      <div class="dl-n">${f.name}</div>
      <div class="dl-s">${f.size || ''}</div>
      <a href="/api/dl-file/${jid}/${encodeURIComponent(f.name)}" class="btn-dl" download>⬇ Download</a>`;
    grid.appendChild(c);
  });

  // Also update Download Status tab automatically
  const dlStatus = {};
  files.forEach(f => {
    const m = f.name.match(/^(GSTR[^_]+)_([A-Za-z]+)_(\d{4})/);
    if(m){
      const rt = m[1].replace('-',''), mon = m[2];
      dlStatus[`${mon}_${rt}`] = 'OK';
    }
  });
  if(Object.keys(dlStatus).length) renderDlStatus(dlStatus, jid);
}

async function adTransferToRecon(){
  if(!_adLastJobId || !_adLastFiles.length){
    alert('No downloaded files to transfer. Run Auto Download first.'); return;
  }

  // Zone mapping: return type prefix → drop zone id
  const zoneMap = {
    'GSTR1':  'r1',
    'GSTR1A': 'r1a',
    'GSTR2B': 'r2b',
    'GSTR2A': 'r2a',
    'GSTR3B': 'r3b',
  };

  // Filter only individual return files (skip master ZIP)
  const toTransfer = _adLastFiles.filter(f => {
    if(!f.name) return false;
    const n = f.name.toUpperCase();
    // Skip the master ZIP bundle
    if(n.startsWith('GST_DOWNLOADS') && n.endsWith('.ZIP')) return false;
    // Must match a known return type prefix
    return Object.keys(zoneMap).some(rt => n.startsWith(rt + '_'));
  });

  if(!toTransfer.length){
    alert('No individual return files found to transfer. Only the ZIP bundle was downloaded — please download files individually first.');
    return;
  }

  // Switch to reconciliation tab first
  switchTab('recon', null);

  // Show progress in recon log
  document.getElementById('r-pw').style.display='block';
  addLog('r','info',`📥 Transferring ${toTransfer.length} file(s) from Auto Download...`);

  const fetched = {r1:[], r1a:[], r2b:[], r2a:[], r3b:[]};
  let fetchOk = 0, fetchFail = 0;

  for(const f of toTransfer){
    // Determine zone from filename prefix (case-insensitive)
    const upper = f.name.toUpperCase();
    let zone = null;
    for(const [rt, zn] of Object.entries(zoneMap)){
      if(upper.startsWith(rt + '_')){ zone = zn; break; }
    }
    if(!zone){ addLog('r','warn',`⚠ Skipped (unknown type): ${f.name}`); continue; }

    try{
      addLog('r','info',`  Fetching ${f.name}...`);
      const resp = await fetch(`/api/dl-file/${_adLastJobId}/${encodeURIComponent(f.name)}`);
      if(!resp.ok){
        addLog('r','err',`  ✗ HTTP ${resp.status} for ${f.name}`);
        fetchFail++;
        continue;
      }
      const blob = await resp.blob();
      if(blob.size < 100){
        addLog('r','warn',`  ⚠ ${f.name} looks empty (${blob.size} bytes) — skipping`);
        fetchFail++;
        continue;
      }
      const mimeMap = {'.pdf':'application/pdf','.zip':'application/zip','.xlsx':'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet','.json':'application/json'};
      const ext = f.name.slice(f.name.lastIndexOf('.')).toLowerCase();
      const mime = mimeMap[ext] || blob.type || 'application/octet-stream';
      fetched[zone].push(new File([blob], f.name, {type: mime}));
      fetchOk++;
    }catch(err){
      addLog('r','err',`  ✗ Fetch error for ${f.name}: ${err.message}`);
      fetchFail++;
    }
  }

  // Load into zone files and update UI
  let total = 0;
  for(const [zone, files] of Object.entries(fetched)){
    if(!files.length) continue;
    zoneFiles[zone] = (zoneFiles[zone] || []).concat(files);
    const cnt = document.getElementById('cnt-'+zone);
    const el  = document.getElementById('zone-'+zone);
    if(cnt){
      const n = zoneFiles[zone].length;
      cnt.textContent = n + ' file' + (n>1?'s':'') + ' selected';
    }
    if(el) el.classList.add('has-files');
    total += files.length;
  }

  document.getElementById('r-pw').style.display='none';

  // Auto-fill company name and GSTIN from auto-download tab
  const adName  = document.getElementById('ad-name');
  const adGstin = document.getElementById('ad-gstin');
  const adFy    = document.getElementById('ad-fy');
  if(adName  && adName.value)  { const el=document.getElementById('r-name');  if(el && !el.value) el.value=adName.value;  }
  if(adGstin && adGstin.value) { const el=document.getElementById('r-gstin'); if(el && !el.value) el.value=adGstin.value.trim().toUpperCase(); }
  if(adFy    && adFy.value)    { const el=document.getElementById('r-fy');    if(el) el.value=adFy.value; }

  if(total > 0){
    addLog('r','ok',`✅ ${total} file(s) loaded! ${fetchFail?fetchFail+' skipped — ':''} GSTIN & Company auto-filled. Click Generate when ready.`);
    // Update transfer button status
    const ts = document.getElementById('ad-transfer-status');
    if(ts){ ts.textContent = `✅ ${total} file(s) loaded into Reconciliation tab`; ts.style.color='var(--grn)'; }
    // Scroll to top of reconciliation tab so user sees the filled fields
    window.scrollTo({top:0, behavior:'smooth'});
  } else {
    addLog('r','err','❌ No files transferred. Server may still be copying files — wait 10 seconds and try again, or download the ZIP and upload manually.');
    const ts = document.getElementById('ad-transfer-status');
    if(ts){ ts.textContent='❌ Transfer failed — try again in 10s'; ts.style.color='var(--red)'; }
  }
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

// ── Bulk Download ─────────────────────────────────────────────────
let _bulkJobId = null;
let _bulkPollTimer = null;

async function startBulk(){
  const files = zoneFiles['bulk'] || [];
  if(!files.length){ alert('Upload a company list Excel first'); return; }
  const fy      = document.getElementById('bulk-fy').value;
  const returns = document.getElementById('bulk-returns').value;
  const fd = new FormData();
  files.forEach(f => fd.append('companies_file', f));
  fd.append('fy', fy);
  fd.append('returns', returns);

  document.getElementById('bulk-pw').style.display = 'block';
  document.getElementById('bulk-dw').style.display = 'none';
  document.getElementById('bulk-captcha-card').style.display = 'none';
  document.getElementById('bulk-lb').innerHTML = '';
  document.getElementById('bulk-pb').style.width = '0%';
  setBadge('bulk','p','Running');
  const btn = document.getElementById('bulk-submit');
  btn.disabled = true; btn.textContent = 'Starting…';
  addLog('bulk','info','Uploading company list…');
  try{
    const res = await fetch('/api/bulk-start', {method:'POST', body:fd});
    const d   = await res.json();
    if(d.error){ addLog('bulk','err',d.error); setBadge('bulk','e','Failed'); btn.disabled=false; btn.textContent='🚀 Start Bulk Download'; return; }
    _bulkJobId = d.job_id;
    addLog('bulk','ok',`Loaded ${d.total} companies. Starting downloads…`);
    _bulkPoll(_bulkJobId);
  }catch(err){
    addLog('bulk','err','Network error: '+err.message);
    setBadge('bulk','e','Failed');
    btn.disabled=false; btn.textContent='🚀 Start Bulk Download';
  }
}

async function _bulkPoll(jid){
  try{
    const r = await fetch('/api/job/'+jid);
    let d; try{ d = await r.json(); }catch(_){ _bulkPollTimer=setTimeout(()=>_bulkPoll(jid),3000); return; }
    if(d.logs) d.logs.forEach(l=>addLog('bulk',l.type,l.msg));
    if(d.progress!=null) document.getElementById('bulk-pb').style.width=d.progress+'%';
    if(d.counter) document.getElementById('bulk-counter').textContent=d.counter;

    // Company needs token
    if(d.captcha_needed && d.captcha_company){
      _showBulkTokenCard(d.captcha_company);
    } else {
      document.getElementById('bulk-captcha-card').style.display='none';
    }

    if(d.status==='done'){
      setBadge('bulk','d','Complete');
      document.getElementById('bulk-pb').style.width='100%';
      document.getElementById('bulk-captcha-card').style.display='none';
      document.getElementById('bulk-submit').disabled=false;
      document.getElementById('bulk-submit').textContent='🚀 Start Bulk Download';
      _bulkShowFiles(jid, d.files);
      return;
    }
    if(d.status==='error'){
      setBadge('bulk','e','Failed');
      document.getElementById('bulk-captcha-card').style.display='none';
      document.getElementById('bulk-submit').disabled=false;
      document.getElementById('bulk-submit').textContent='🚀 Start Bulk Download';
      return;
    }
    _bulkPollTimer = setTimeout(()=>_bulkPoll(jid), 1500);
  }catch(e){ _bulkPollTimer=setTimeout(()=>_bulkPoll(jid),3000); }
}

function _showBulkTokenCard(company){
  document.getElementById('bulk-captcha-card').style.display='block';
  document.getElementById('bulk-co-name').textContent = company.name || company.gstin;
  document.getElementById('bulk-cap-gstin').value = company.gstin || '';
  document.getElementById('bulk-cap-user').value  = company.username || '';
  document.getElementById('bulk-token-input').value = '';
  document.getElementById('bulk-token-err').textContent = '';
  document.getElementById('bulk-token-btn').disabled = false;
  document.getElementById('bulk-token-btn').textContent = 'Submit Token & Download →';
  // Scroll into view
  document.getElementById('bulk-captcha-card').scrollIntoView({behavior:'smooth',block:'center'});
}

async function submitBulkToken(){
  const token = document.getElementById('bulk-token-input').value.trim();
  if(!token){ document.getElementById('bulk-token-err').textContent='Please paste the AuthToken'; return; }
  const btn = document.getElementById('bulk-token-btn');
  btn.disabled=true; btn.textContent='Submitting…';
  document.getElementById('bulk-token-err').textContent='';
  try{
    const res = await fetch(`/api/bulk-token/${_bulkJobId}`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({token})
    });
    const d = await res.json();
    if(!d.ok){
      document.getElementById('bulk-token-err').textContent = d.error||'Failed';
      btn.disabled=false; btn.textContent='Submit Token & Download →';
      return;
    }
    document.getElementById('bulk-captcha-card').style.display='none';
    addLog('bulk','ok','Token submitted — downloading…');
  }catch(err){
    document.getElementById('bulk-token-err').textContent='Network error: '+err.message;
    btn.disabled=false; btn.textContent='Submit Token & Download →';
  }
}

function _bulkShowFiles(jid, files){
  const sec=document.getElementById('bulk-dw'), grid=document.getElementById('bulk-dlg');
  sec.style.display='block'; grid.innerHTML='';
  if(!files||!files.length){ grid.innerHTML='<p style="color:var(--muted);font-size:.8rem">No files downloaded.</p>'; return; }
  files.forEach(f=>{
    const c=document.createElement('div'); c.className='dlc';
    c.innerHTML=`<div style="font-size:1.4rem">📥</div>
      <div class="dl-n">${f.name}</div><div class="dl-s">${f.size||''}</div>
      <a href="/api/dl-file/${jid}/${encodeURIComponent(f.name)}" class="btn-dl" download>⬇ Download</a>`;
    grid.appendChild(c);
  });
}

// ── Failure Screenshots ──────────────────────────────────────────
let _failShotsRendered = 0;

async function refreshFailShots(){
  if(!_adJobId) return;
  try{
    const res = await fetch(`/api/failure-screenshots/${_adJobId}`);
    const d   = await res.json();
    if(d.error || !d.screenshots) return;
    _renderFailShots(d.screenshots);
  }catch(e){ console.warn('Failure shots fetch error:', e); }
}

function _renderFailShots(shots){
  const panel = document.getElementById('ad-fail-shots');
  const grid  = document.getElementById('ad-fail-grid');
  const count = document.getElementById('ad-fail-count');
  if(!panel || !grid) return;

  if(!shots || shots.length === 0){
    panel.style.display = 'none';
    _failShotsRendered = 0;
    return;
  }

  panel.style.display = 'block';
  count.textContent = `(${shots.length} screenshot${shots.length>1?'s':''})`;

  // Only add newly arrived screenshots (don't re-render existing ones)
  const newShots = shots.slice(_failShotsRendered);
  newShots.forEach((shot, i) => {
    const idx = _failShotsRendered + i + 1;
    const card = document.createElement('div');
    card.style.cssText = 'background:var(--surf2);border:1px solid rgba(255,23,68,.25);border-radius:10px;padding:.85rem;';

    // Header row
    const hdr = document.createElement('div');
    hdr.style.cssText = 'display:flex;align-items:center;gap:.6rem;margin-bottom:.6rem;flex-wrap:wrap;';
    hdr.innerHTML = `
      <span style="background:rgba(255,23,68,.15);color:var(--red);border:1px solid rgba(255,23,68,.3);
                   border-radius:100px;font-size:.62rem;font-weight:700;padding:.18rem .55rem;
                   font-family:var(--mono)">#${idx}</span>
      <span style="font-size:.78rem;font-weight:700;color:var(--txt);flex:1">${escHtml(shot.label)}</span>
      <span style="font-size:.65rem;color:var(--muted);font-family:var(--mono)">⏰ ${escHtml(shot.ts)}</span>`;
    card.appendChild(hdr);

    // Screenshot image
    const img = document.createElement('img');
    img.src = 'data:image/png;base64,' + shot.img_b64;
    img.alt = shot.label;
    img.style.cssText = 'width:100%;border-radius:7px;border:1px solid var(--bdr);cursor:zoom-in;display:block;';
    img.title = 'Click to open full size';
    img.onclick = () => {
      const win = window.open();
      win.document.write(`<html><head><title>${escHtml(shot.label)}</title>
        <style>body{margin:0;background:#111;display:flex;flex-direction:column;align-items:center;padding:1rem}
        img{max-width:100%;border-radius:8px}
        p{color:#aaa;font-family:monospace;font-size:.8rem;margin:.5rem 0}</style></head>
        <body><p>📸 ${escHtml(shot.label)} — ${escHtml(shot.ts)}</p>
        <img src="data:image/png;base64,${shot.img_b64}"></body></html>`);
    };
    card.appendChild(img);

    // Download link
    const dlRow = document.createElement('div');
    dlRow.style.cssText = 'margin-top:.5rem;display:flex;gap:.5rem;align-items:center;flex-wrap:wrap;';
    const dlBtn = document.createElement('a');
    dlBtn.href = 'data:image/png;base64,' + shot.img_b64;
    dlBtn.download = `failure_${idx}_${shot.label.replace(/[^a-zA-Z0-9]/g,'_').slice(0,40)}.png`;
    dlBtn.className = 'btn-dl';
    dlBtn.style.cssText = 'font-size:.68rem;padding:.3rem .75rem;';
    dlBtn.textContent = '⬇ Download PNG';
    dlRow.appendChild(dlBtn);
    const hint = document.createElement('span');
    hint.style.cssText = 'font-size:.65rem;color:var(--muted);font-family:var(--mono);';
    hint.textContent = 'Click image to open full size • Download to save for reference';
    dlRow.appendChild(hint);
    card.appendChild(dlRow);

    grid.appendChild(card);
  });

  _failShotsRendered = shots.length;
}

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

// ── Income Tax Reconciliation ─────────────────────────────────────
let _itJobId = null;
let _itPollTimer = null;

document.getElementById('it-form').addEventListener('submit', async function(e){
  e.preventDefault();
  const name  = document.getElementById('it-name').value.trim();
  const pan   = document.getElementById('it-pan').value.trim().toUpperCase();
  const gstin = document.getElementById('it-gstin').value.trim().toUpperCase();
  const fy    = document.getElementById('it-fy').value;

  if(!name){ alert('Please enter company name'); return; }
  if(!pan || pan.length !== 10){ alert('PAN must be 10 characters (e.g. ABCDE1234F)'); return; }

  const zones = ['it26as','itais','itgst'];
  const hasFile = zones.some(z => {
    const inp = document.querySelector(`input[data-zone="${z}"]`);
    return inp && inp.files && inp.files.length > 0;
  });
  if(!hasFile){ alert('Please upload at least the 26AS PDF'); return; }

  const btn = document.getElementById('it-submit');
  btn.disabled = true; btn.textContent = 'Uploading…';

  const fd = new FormData();
  fd.append('company_name', name);
  fd.append('pan',  pan);
  fd.append('gstin', gstin);
  fd.append('fy',   fy);

  zones.forEach(z => {
    const inp = document.querySelector(`input[data-zone="${z}"]`);
    if(inp && inp.files){
      Array.from(inp.files).forEach(f => fd.append(`files_${z}`, f));
    }
  });

  try{
    const res = await fetch('/api/it-upload', {method:'POST', body:fd});
    const d   = await res.json();
    if(d.error){ alert('Error: '+d.error); btn.disabled=false; btn.textContent='Generate IT Reconciliation Excel →'; return; }
    _itJobId = d.job_id;
    document.getElementById('it-pw').style.display = 'block';
    document.getElementById('it-dw').style.display = 'none';
    setBadge('it','p','Running');
    btn.textContent = 'Processing…';
    _itPoll(_itJobId);
  }catch(err){
    alert('Network error: '+err.message);
    btn.disabled=false; btn.textContent='Generate IT Reconciliation Excel →';
  }
});

async function _itPoll(jid){
  try{
    const res = await fetch(`/api/it-job/${jid}`);
    const d   = await res.json();
    if(d.error){ return; }

    // Update progress bar
    document.getElementById('it-pb').style.width = (d.progress||0)+'%';

    // Stream logs
    if(d.logs && d.logs.length){
      const lb = document.getElementById('it-lb');
      d.logs.forEach(l => {
        const sp = document.createElement('span');
        sp.className = l.type || 'info';
        sp.textContent = l.msg + '\n';
        lb.appendChild(sp);
      });
      lb.scrollTop = lb.scrollHeight;
    }

    if(d.status === 'done'){
      setBadge('it','d','Complete');
      document.getElementById('it-pb').style.width = '100%';
      document.getElementById('it-submit').disabled = false;
      document.getElementById('it-submit').textContent = 'Generate IT Reconciliation Excel →';
      _itShowFiles(jid, d.files);
      return;
    }
    if(d.status === 'error'){
      setBadge('it','e','Failed');
      document.getElementById('it-submit').disabled = false;
      document.getElementById('it-submit').textContent = 'Generate IT Reconciliation Excel →';
      return;
    }
    _itPollTimer = setTimeout(() => _itPoll(jid), 1500);
  }catch(e){ _itPollTimer = setTimeout(() => _itPoll(jid), 3000); }
}

function _itShowFiles(jid, files){
  const sec  = document.getElementById('it-dw');
  const grid = document.getElementById('it-dlg');
  sec.style.display = 'block';
  grid.innerHTML = '';
  if(!files || !files.length){
    grid.innerHTML = '<p style="color:var(--muted);font-size:.8rem">No files generated.</p>';
    return;
  }
  files.forEach(f => {
    const c = document.createElement('div'); c.className = 'dlc';
    c.innerHTML = `<div style="font-size:1.4rem">🏦</div>
      <div class="dl-n">${f.name}</div>
      <div class="dl-s">${f.size||''}</div>
      <a href="/api/it-dl/${jid}/${encodeURIComponent(f.name)}" class="btn-dl" download>⬇ Download</a>`;
    grid.appendChild(c);
  });
}

function resetIT(){
  if(_itPollTimer) clearTimeout(_itPollTimer);
  _itJobId = null;
  document.getElementById('it-form').reset();
  ['it26as','itais','itgst'].forEach(z => {
    const inp = document.querySelector(`input[data-zone="${z}"]`);
    if(inp) inp.value = '';
    updateZone(z, {files:[]});
  });
  document.getElementById('it-pw').style.display = 'none';
  document.getElementById('it-dw').style.display = 'none';
  document.getElementById('it-lb').innerHTML = '';
  document.getElementById('it-pb').style.width = '0%';
  const btn = document.getElementById('it-submit');
  btn.disabled = false; btn.textContent = 'Generate IT Reconciliation Excel →';
}

</script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════
# RECONCILIATION WORKERS
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
        prog(5)

        # GSTR-1
        for fpath in saved.get("r1", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1: {mon} {yr}"); set_dl(f"{mon}_GSTR1", "OK")
            else:
                log(f"  ⚠ Month not detected: {Path(fpath).name}", "warn")

        # GSTR-1A
        for fpath in saved.get("r1a", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1A_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1A: {mon} {yr}"); set_dl(f"{mon}_GSTR1A", "OK")
            else:
                log(f"  ⚠ GSTR-1A month not detected: {Path(fpath).name}", "warn")

        # GSTR-2B
        for fpath in saved.get("r2b", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR2B_{mon}_{yr}.xlsx"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2B: {mon} {yr}"); set_dl(f"{mon}_GSTR2B", "OK")

        # GSTR-2A
        for fpath in saved.get("r2a", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                ext = Path(fpath).suffix.lower()
                dest = job_dir / f"GSTR2A_{mon}_{yr}{ext}"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2A: {mon} {yr}"); set_dl(f"{mon}_GSTR2A", "OK")

        # GSTR-3B
        for fpath in saved.get("r3b", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR3B_{mon}_{yr}.pdf"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-3B: {mon} {yr}"); set_dl(f"{mon}_GSTR3B", "OK")

        # Customer names
        for fpath in saved.get("cust", []):
            dest = job_dir / "customer_names.xlsx"
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))
            log("  Customer names loaded"); break

        # Tax liability
        for fpath in saved.get("taxlib", []):
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
        log("Running annual reconciliation (1-2 minutes)...")
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
        for line in traceback.format_exc().split("\n"):
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

        for fpath in saved.get("r1", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1: {mon} {yr}")
            else:
                log(f"  ⚠ Month not detected: {Path(fpath).name}", "warn")

        for fpath in saved.get("r2b", []) + saved.get("r2a", []):
            dest = job_dir / Path(fpath).name
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))

        for fpath in saved.get("cust", []):
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
        for line in traceback.format_exc().split("\n"):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)
        _cleanup_uploads(job_id)

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

    saved = {k: [] for k in ("r1","r1a","r2b","r2a","r3b","cust","taxlib")}
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
        # Return failure screenshots (without img data in poll — just label+ts for count)
        # Full images are fetched via /api/failure-screenshots/<job_id>
        fail_shots = job.get("failure_screenshots", [])
        fail_shots_light = [{"label": s["label"], "ts": s["ts"], "img_b64": s["img_b64"]} for s in fail_shots]
        return jsonify(
            status=job["status"], progress=job["progress"],
            logs=new_logs, files=job["files"], error=job["error"],
            dl_status=job.get("dl_status",{}),
            captcha_needed=job.get("captcha_needed", False),
            captcha_img=job.get("captcha_img", None),
            captcha_company=job.get("captcha_company", None),
            counter=job.get("counter",""),
            failure_screenshots=fail_shots_light,
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

@app.route("/api/debug-screenshot/<job_id>")
def debug_screenshot(job_id):
    """Returns the latest screenshot as an HTML page for easy viewing."""
    with jobs_lock:
        job = jobs.get(job_id)
    img = (job or {}).get("captcha_img") or ""
    with _sess_lock:
        s = _sessions.get(job_id, {})
    img = img or s.get("screenshot") or ""
    if not img:
        return "<h2>No screenshot available yet</h2><p>Run the download first, then refresh.</p>"
    return f"""<!DOCTYPE html><html><head>
    <title>Debug Screenshot - Job {job_id}</title>
    <meta http-equiv="refresh" content="3">
    <style>body{{background:#111;color:#eee;font-family:monospace;padding:20px}}
    img{{max-width:100%;border:2px solid #0f0;border-radius:8px}}</style>
    </head><body>
    <h3>🖥 Server Browser Screenshot — Job {job_id}</h3>
    <p style="color:#0f0">Auto-refreshes every 3 seconds</p>
    <img src="data:image/png;base64,{img}">
    </body></html>"""



# ── Failure screenshots endpoint ─────────────────────────────────
@app.route("/api/failure-screenshots/<job_id>")
def failure_screenshots(job_id):
    """Returns list of failure screenshots captured during auto download."""
    with jobs_lock:
        job = jobs.get(job_id)
    if not job:
        return jsonify(error="job not found"), 404
    shots = job.get("failure_screenshots", [])
    # Return label, ts, and img_b64 for each
    return jsonify(count=len(shots), screenshots=[
        {"label": s["label"], "ts": s["ts"], "img_b64": s["img_b64"]}
        for s in shots
    ])

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

# ── Receive token from bookmarklet ───────────────────────────────
@app.route("/api/receive-token/<job_id>", methods=["POST","OPTIONS"])
def receive_token(job_id):
    # CORS — bookmarklet runs on gst.gov.in, needs cross-origin POST
    if request.method == "OPTIONS":
        from flask import Response
        r = Response("", 204)
        r.headers["Access-Control-Allow-Origin"]  = "*"
        r.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        r.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return r

    data  = request.get_json(silent=True) or {}
    token = str(data.get("token","")).strip()
    if not token:
        from flask import Response
        r = Response('{"ok":false,"error":"empty token"}', 400, mimetype="application/json")
        r.headers["Access-Control-Allow-Origin"] = "*"
        return r

    # Put token into the job's captcha_q so the background thread picks it up
    with _sess_lock:
        s = _sessions.get(job_id)
    if s:
        # Clear stale tokens first, then add new one
        while not s["captcha_q"].empty():
            try: s["captcha_q"].get_nowait()
            except: pass
        s["captcha_q"].put(token)
        # Update job state so UI knows token arrived
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["captcha_needed"] = False
                jobs[job_id]["captcha_img"]    = None
        from flask import Response
        r = Response('{"ok":true,"msg":"Token received — download resuming!"}',
                     200, mimetype="application/json")
        r.headers["Access-Control-Allow-Origin"] = "*"
        return r

    # Job not found — but still return ok (bookmarklet shouldn't see errors)
    from flask import Response
    r = Response('{"ok":false,"error":"job not found"}', 404, mimetype="application/json")
    r.headers["Access-Control-Allow-Origin"] = "*"
    return r


@app.route("/api/dl-file/<job_id>/<filename>")
def dl_file(job_id, filename):
    filename = Path(filename).name
    fp = OUTPUT_DIR / job_id / filename
    if not fp.exists():
        abort(404)
    return send_file(str(fp), as_attachment=True, download_name=filename)

# ── Start Auto Download ───────────────────────────────────────────
@app.route("/api/auto-download", methods=["POST"])
@rate_limit(limit=10, window=60)
def api_auto_download():
    d = request.get_json(silent=True) or {}
    gstin       = d.get("gstin","").strip().upper()
    client_name = d.get("client_name","").strip()
    username    = d.get("username","").strip()
    password    = d.get("password","")
    token       = d.get("token","").strip()
    fy          = d.get("fy","2025-26")
    returns     = d.get("returns","all")

    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN"), 400
    if not client_name:
        return jsonify(error="Company name required"), 400
    if not username:
        return jsonify(error="Username required"), 400
    # Accept either token (direct paste) or no token (bookmarklet will send later)
    # password field kept for backwards compatibility

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
            "failure_screenshots": [],   # list of {label, img_b64, ts}
        }

    sess = {
        "captcha_q":     _queue.Queue(),
        "refresh_event": threading.Event(),
        "screenshot":    None,
    }
    with _sess_lock:
        _sessions[job_id] = sess

    def run_bg():
        try:
            _auto_download(job_id, gstin, client_name,
                           username, password, fy, returns, sess, token=token)
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
            with _sess_lock:
                _sessions.pop(job_id, None)

    threading.Thread(target=run_bg, daemon=True).start()
    return jsonify(job_id=job_id)


def _auto_download(job_id, gstin, client_name,
                    username, password, fy, returns, sess, token=""):
    """
    Selenium-based download — exactly follows gst_suite_final.py flow:
      1. Open www.gst.gov.in → LOGIN button → fill username + password
      2. Show CAPTCHA screenshot → user types CAPTCHA in web UI → click LOGIN
      3. Services → Returns → Returns Dashboard
      4. For each month: select FY/Quarter/Period → SEARCH → click tile DOWNLOAD
         GSTR-3B  : PDF downloads directly
         GSTR-1/1A: GENERATE JSON → wait → download link
         GSTR-2B/2A: GENERATE EXCEL → wait → download link
      5. ZIP all files → done
    """
    import base64, tempfile, shutil as _shutil

    def log(msg, t="info"):
        print(f"[{job_id}] {msg}")
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["logs"].append({"type": t, "msg": msg})

    def prog(p):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["progress"] = p

    def show_captcha(img_b64):
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

    def save_failure_screenshot(label):
        """Capture current browser state and store as a failure screenshot for reference."""
        try:
            img_b64 = _screenshot_b64()
            if not img_b64:
                return
            entry = {
                "label": label,
                "img_b64": img_b64,
                "ts": datetime.now().strftime("%H:%M:%S"),
            }
            with jobs_lock:
                if job_id in jobs:
                    jobs[job_id].setdefault("failure_screenshots", []).append(entry)
            log(f"  📸 Failure screenshot saved: {label}", "warn")
        except Exception as _se:
            log(f"  ⚠ Could not save failure screenshot: {_se}", "warn")

    def wait_for_captcha_input():
        """Block until user submits CAPTCHA text from web UI. Returns the text."""
        while not sess["captcha_q"].empty():
            try: sess["captcha_q"].get_nowait()
            except: pass
        log("⏳ CAPTCHA screenshot shown — please type the CAPTCHA in the box above and click Submit")
        try:
            return sess["captcha_q"].get(timeout=600)
        except _queue.Empty:
            raise RuntimeError("CAPTCHA timeout — no input received in 10 minutes")

    # ── FY and months setup ──────────────────────────────────────────
    fy_start = int(fy.split("-")[0])
    MONTHS_LIST = [
        ("April","04",str(fy_start)),    ("May","05",str(fy_start)),
        ("June","06",str(fy_start)),     ("July","07",str(fy_start)),
        ("August","08",str(fy_start)),   ("September","09",str(fy_start)),
        ("October","10",str(fy_start)),  ("November","11",str(fy_start)),
        ("December","12",str(fy_start)), ("January","01",str(fy_start+1)),
        ("February","02",str(fy_start+1)),("March","03",str(fy_start+1)),
    ]
    QUARTER_MAP_LOCAL = {
        "April":"Quarter 1 (Apr - Jun)","May":"Quarter 1 (Apr - Jun)","June":"Quarter 1 (Apr - Jun)",
        "July":"Quarter 2 (Jul - Sep)","August":"Quarter 2 (Jul - Sep)","September":"Quarter 2 (Jul - Sep)",
        "October":"Quarter 3 (Oct - Dec)","November":"Quarter 3 (Oct - Dec)","December":"Quarter 3 (Oct - Dec)",
        "January":"Quarter 4 (Jan - Mar)","February":"Quarter 4 (Jan - Mar)","March":"Quarter 4 (Jan - Mar)",
    }

    out_dir = Path(jobs[job_id]["out_dir"])
    dl_dir  = out_dir / "browser_downloads"
    dl_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    driver = None

    # ── Selenium imports ────────────────────────────────────────────
    try:
        from selenium import webdriver
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait, Select
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.webdriver.chrome.options import Options as ChromeOptions
        from selenium.webdriver.chrome.service import Service as ChromeService
        import selenium.common.exceptions as SeEx
    except ImportError:
        raise RuntimeError("Selenium not installed on server. Run: pip install selenium")

    # ── Helper functions (mirrors gst_suite_final.py) ───────────────
    def _try_click(xpaths, timeout=8):
        for xp in xpaths:
            try:
                el = WebDriverWait(driver, timeout).until(
                    EC.element_to_be_clickable((By.XPATH, xp)))
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                time.sleep(0.3)
                try: el.click()
                except: driver.execute_script("arguments[0].click();", el)
                return True
            except: continue
        return False

    def _human_type(by, val, text):
        try:
            el = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((by, val)))
            driver.execute_script("arguments[0].scrollIntoView(true);", el)
            time.sleep(0.3); el.click(); time.sleep(0.2)
            el.clear(); time.sleep(0.2)
            for ch in str(text): el.send_keys(ch); time.sleep(0.03)
            time.sleep(0.3)
            return True
        except Exception as e:
            log(f"  Type failed {val}: {e}", "warn")
            return False

    def _is_session_lost():
        try:
            url = driver.current_url.lower()
            if "accessdenied" in url: return True
            if "login" in url and "fowelcome" not in url and "gst.gov.in" in url: return True
            body = driver.find_element(By.TAG_NAME, "body").text.lower()
            for phrase in ["session expired","you are not logged in","please login again","access denied"]:
                if phrase in body: return True
        except: pass
        return False

    def _screenshot_b64():
        try: return base64.b64encode(driver.get_screenshot_as_png()).decode()
        except: return None

    def _do_login():
        """Navigate to GST portal, fill creds, show CAPTCHA screenshot, wait for user input."""
        log("🌐 Opening www.gst.gov.in ...")
        driver.get("https://www.gst.gov.in")
        time.sleep(4)

        log("  Clicking LOGIN button...")
        _try_click([
            "//a[normalize-space()='LOGIN']",
            "//a[normalize-space()='Login']",
            "//button[normalize-space()='LOGIN']",
            "//a[contains(@href,'login')]",
        ])
        time.sleep(8)
        log(f"  Login page: {driver.current_url}")

        log(f"  Filling username: {username}")
        filled = False
        from selenium.webdriver.common.by import By as _By
        for by, val in [
            (_By.ID,"username"),(_By.NAME,"username"),
            (_By.ID,"user_name"),(_By.NAME,"user_name"),
            (_By.CSS_SELECTOR,"input[placeholder*='sername']"),
            (_By.CSS_SELECTOR,"input[type='text']:not([readonly])"),
        ]:
            if _human_type(by, val, username):
                filled = True; break
        if not filled:
            raise RuntimeError("Cannot find username field on GST portal login page")

        time.sleep(2)
        log("  Filling password...")
        filled = False
        for by, val in [
            (_By.ID,"user_pass"),(_By.NAME,"user_pass"),
            (_By.ID,"password"),(_By.NAME,"password"),
            (_By.CSS_SELECTOR,"input[type='password']"),
        ]:
            if _human_type(by, val, password):
                filled = True; break
        if not filled:
            raise RuntimeError("Cannot find password field on GST portal login page")

        time.sleep(2)

        # Show CAPTCHA screenshot to user
        log("📸 Taking CAPTCHA screenshot — please type the CAPTCHA in the box below...")
        img = _screenshot_b64()
        show_captcha(img)

        # Wait for user to type CAPTCHA in web UI
        captcha_text = wait_for_captcha_input()
        clear_captcha()
        log(f"  CAPTCHA received: {'*' * len(captcha_text)}")

        # Fill CAPTCHA in browser
        log("  Filling CAPTCHA in browser...")
        captcha_filled = False
        for by, val in [
            (_By.ID,"captcha"),(_By.NAME,"captcha"),
            (_By.ID,"imgCaptcha"),(_By.NAME,"imgCaptcha"),
            (_By.CSS_SELECTOR,"input[placeholder*='aptcha']"),
            (_By.CSS_SELECTOR,"input[placeholder*='APTCHA']"),
            (_By.XPATH,"//input[@id='captcha' or @name='captcha' or contains(@placeholder,'aptcha')]"),
        ]:
            try:
                if _human_type(by, val, captcha_text):
                    captcha_filled = True; break
            except: continue

        if not captcha_filled:
            log("  ⚠ Could not auto-fill CAPTCHA field — please type it manually in the browser", "warn")
            # Give user 30 seconds to fill it manually
            time.sleep(30)

        time.sleep(2)

        # Click LOGIN button
        log("  Clicking LOGIN button...")
        _try_click([
            "//button[@id='btnlogin']",
            "//button[normalize-space()='LOGIN']",
            "//button[normalize-space()='Login']",
            "//button[@type='submit']",
            "//input[@type='submit']",
        ])
        time.sleep(10)

        # OTP check
        try:
            body = driver.find_element(By.TAG_NAME, "body").text.lower()
            if "otp" in body and ("enter" in body or "verify" in body):
                log("📱 OTP required — please enter OTP in the browser...")
                img = _screenshot_b64()
                show_captcha(img)
                otp = wait_for_captcha_input()
                clear_captcha()
                for by, val in [
                    (By.ID,"otp"),(By.NAME,"otp"),
                    (By.CSS_SELECTOR,"input[placeholder*='OTP']"),
                    (By.CSS_SELECTOR,"input[placeholder*='otp']"),
                ]:
                    try:
                        if _human_type(by, val, otp): break
                    except: continue
                _try_click(["//button[contains(text(),'VERIFY')]","//button[contains(text(),'Submit')]","//button[@type='submit']"])
                time.sleep(8)
        except: pass

        cur = driver.current_url.lower()
        log(f"  Post-login URL: {driver.current_url}")
        if "accessdenied" in cur or ("login" in cur and "fowelcome" not in cur):
            raise RuntimeError("Login failed — wrong username/password/CAPTCHA. Please try again.")
        log("  ✅ Login successful!", "ok")
        return True

    def _go_to_dashboard():
        """Navigate to Returns Dashboard by clicking Services→Returns→Returns Dashboard.
        Never uses direct URLs — always follows menu clicks to avoid Access Denied."""
        cur = driver.current_url
        if "return.gst.gov.in" in cur and "dashboard" in cur:
            return True

        if _is_session_lost():
            log("  ⚠ Session lost — re-logging in...", "warn")
            _do_login()

        log("  Navigating: Services → Returns → Returns Dashboard")

        for attempt in range(3):
            log(f"  Nav attempt {attempt+1} from: {driver.current_url}")

            # Step 1: Click Services (also hover to open dropdown)
            try:
                from selenium.webdriver.common.action_chains import ActionChains
                svc_el = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[normalize-space(text())='Services']")))
                ActionChains(driver).move_to_element(svc_el).click(svc_el).perform()
                log("  Services clicked ✓")
            except:
                _try_click([
                    "//a[normalize-space(text())='Services']",
                    "//nav//a[normalize-space()='Services']",
                ])
            time.sleep(2)

            # Step 2: Click Returns in dropdown (hover first to keep dropdown open)
            try:
                ret_el = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[normalize-space(text())='Returns']")))
                ActionChains(driver).move_to_element(ret_el).click(ret_el).perform()
                log("  Returns clicked ✓")
            except:
                _try_click([
                    "//a[normalize-space(text())='Returns']",
                    "//*[contains(@class,'dropdown-menu')]//a[normalize-space()='Returns']",
                    "//*[contains(@class,'open')]//a[normalize-space()='Returns']",
                ])
            time.sleep(2)

            # Step 3: Click Returns Dashboard — XPath first, then full page scan
            clicked = _try_click([
                "//a[contains(normalize-space(text()),'Returns Dashboard')]",
            ])
            if not clicked:
                # Scan ALL links on page (same as local script "scan" approach)
                for el in driver.find_elements(By.TAG_NAME, "a"):
                    try:
                        if "Returns Dashboard" in (el.text or "") and el.is_displayed():
                            driver.execute_script("arguments[0].click();", el)
                            log("  Returns Dashboard clicked via scan ✓")
                            clicked = True
                            break
                    except: continue
            time.sleep(10)

            final = driver.current_url
            log(f"  URL after nav attempt {attempt+1}: {final}")

            if "accessdenied" in final.lower():
                log("  Access Denied — re-logging in...", "warn")
                _do_login()
                continue

            if "return.gst.gov.in" in final and "dashboard" in final:
                log("  ✅ Returns Dashboard loaded", "ok")
                return True

            # Still on wrong page — log what's visible to help diagnose
            try:
                links = [(a.text.strip(), a.get_attribute("href") or "")
                         for a in driver.find_elements(By.TAG_NAME, "a")
                         if a.is_displayed() and a.text.strip()]
                log(f"  Links on page: {links[:10]}", "info")
            except: pass

        raise RuntimeError(f"Could not reach Returns Dashboard. Last URL: {driver.current_url}")

    def _select_and_search(month_name):
        """Select FY, Quarter, Period then click SEARCH (mirrors select_and_search)"""
        log(f"  Setting: FY={fy}  Quarter={QUARTER_MAP_LOCAL.get(month_name,'')}  Period={month_name}")
        time.sleep(3)

        all_sels = driver.find_elements(By.TAG_NAME, "select")
        # FY
        for sel_el in all_sels:
            try:
                s = Select(sel_el)
                opts = [o.text.strip() for o in s.options]
                if any("-" in o and len(o) <= 9 for o in opts):
                    for opt in s.options:
                        if fy in opt.text:
                            s.select_by_visible_text(opt.text)
                            log(f"  FY: {opt.text} ✓")
                            break
                    break
            except: continue
        time.sleep(1)

        all_sels = driver.find_elements(By.TAG_NAME, "select")
        # Quarter
        qtr = QUARTER_MAP_LOCAL.get(month_name, "")
        for sel_el in all_sels:
            try:
                s = Select(sel_el)
                opts = [o.text.strip() for o in s.options]
                if any("quarter" in o.lower() for o in opts):
                    for opt in s.options:
                        if qtr[:9].lower() in opt.text.lower():
                            s.select_by_visible_text(opt.text)
                            log(f"  Quarter: {opt.text} ✓")
                            break
                    break
            except: continue
        time.sleep(1)

        all_sels = driver.find_elements(By.TAG_NAME, "select")
        # Period/Month
        month_names_lower = ["january","february","march","april","may","june",
                             "july","august","september","october","november","december"]
        for sel_el in all_sels:
            try:
                s = Select(sel_el)
                opts = [o.text.strip() for o in s.options]
                if any(m in " ".join(opts).lower() for m in month_names_lower):
                    for opt in s.options:
                        if month_name.lower() in opt.text.lower():
                            s.select_by_visible_text(opt.text)
                            log(f"  Period: {opt.text} ✓")
                            break
                    break
            except: continue
        time.sleep(1)

        # SEARCH
        clicked = _try_click([
            "//button[normalize-space()='SEARCH']",
            "//button[normalize-space()='Search']",
            "//button[contains(text(),'SEARCH')]",
            "//input[@value='SEARCH']",
        ])
        if not clicked:
            driver.execute_script("""
                var btns=document.querySelectorAll('button,input[type=submit]');
                for(var i=0;i<btns.length;i++){
                    if((btns[i].innerText||btns[i].value||'').toUpperCase().includes('SEARCH')){
                        btns[i].click(); break;
                    }
                }
            """)
        time.sleep(8)
        # ── Debug: screenshot + page dump after SEARCH ────────────────
        try:
            img_b64 = _screenshot_b64()
            if img_b64:
                show_captcha(img_b64)   # reuse captcha slot to show screenshot
                time.sleep(0.5)
                clear_captcha()
        except: pass
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
            # Log all visible text to find tile names
            lines = [l.strip() for l in body_text.split("\n") if l.strip()]
            log(f"  Page text after SEARCH ({len(lines)} lines): {' | '.join(lines[:30])}", "info")
            # Log all buttons
            btns_txt = [b.text.strip() for b in driver.find_elements(By.TAG_NAME, "button") if b.is_displayed() and b.text.strip()]
            log(f"  Visible buttons: {btns_txt[:15]}", "info")
            # Log all links
            links_txt = [(a.text.strip(), (a.get_attribute("href") or "")[:60])
                         for a in driver.find_elements(By.TAG_NAME, "a")
                         if a.is_displayed() and a.text.strip()]
            log(f"  Visible links: {links_txt[:15]}", "info")
        except Exception as _de:
            log(f"  Debug dump error: {_de}", "warn")
        log(f"  Tiles loaded after SEARCH ✓")

    def _click_tile_download(tile_name):
        """Find tile and click its DOWNLOAD button"""
        log(f"  Finding {tile_name} tile DOWNLOAD button...")
        time.sleep(3)

        # Log full page text to diagnose tile names on this portal version
        try:
            all_text = driver.find_element(By.TAG_NAME, "body").text
            log(f"  Page has text: {bool('GSTR' in all_text or 'gstr' in all_text.lower())}")
            # Find all elements containing GSTR to see what's on the page
            gstr_els = driver.find_elements(By.XPATH, "//*[contains(text(),'GSTR') or contains(text(),'gstr')]")
            gstr_texts = list(set(e.text.strip()[:40] for e in gstr_els if e.is_displayed() and e.text.strip()))
            log(f"  GSTR elements on page: {gstr_texts[:20]}", "info")
        except: pass

        name_variants = {
            "GSTR1":  ["GSTR1","GSTR-1","GSTR 1","gstr1","Gstr1"],
            "GSTR1A": ["GSTR1A","GSTR-1A","GSTR 1A","gstr1a"],
            "GSTR2B": ["GSTR2B","GSTR-2B","GSTR 2B","gstr2b"],
            "GSTR2A": ["GSTR2A","GSTR-2A","GSTR 2A","gstr2a"],
            "GSTR3B": ["GSTR3B","GSTR-3B","GSTR 3B","gstr3b"],
        }
        variants = name_variants.get(tile_name.upper().replace("-",""), [tile_name])

        # Strategy 1: find subtitle text → walk up to container → find DOWNLOAD button inside
        for variant in variants:
            try:
                subtitle_els = driver.find_elements(By.XPATH,
                    f"//*[normalize-space(text())='{variant}' or "
                    f"contains(normalize-space(text()),'{variant}')]")
                for subtitle_el in subtitle_els:
                    if not subtitle_el.is_displayed(): continue
                    parent = subtitle_el
                    for level in range(8):
                        try:
                            parent = driver.execute_script("return arguments[0].parentElement;", parent)
                            if parent is None: break
                            btns = parent.find_elements(By.XPATH,
                                ".//*[contains(translate(normalize-space(.),'download','DOWNLOAD'),'DOWNLOAD') "
                                "and (self::button or self::a)]")
                            for btn in btns:
                                if btn.is_displayed():
                                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                                    time.sleep(0.4)
                                    driver.execute_script("arguments[0].click();", btn)
                                    log(f"  ✅ {tile_name} DOWNLOAD clicked (strategy 1, level {level})", "ok")
                                    return True
                        except: break
            except: continue

        # Strategy 2: scan all DOWNLOAD buttons, find the one near the tile title
        try:
            all_dl_btns = driver.find_elements(By.XPATH,
                "//*[contains(translate(normalize-space(text()),'download','DOWNLOAD'),'DOWNLOAD') "
                "and (self::button or self::a) and not(contains(text(),'GENERATE'))]")
            log(f"  All DOWNLOAD buttons on page: {[b.text.strip() for b in all_dl_btns if b.is_displayed()][:10]}", "info")
            for btn in all_dl_btns:
                if not btn.is_displayed(): continue
                # Check if any ancestor contains the tile name
                try:
                    parent = btn
                    for _ in range(10):
                        parent = driver.execute_script("return arguments[0].parentElement;", parent)
                        if parent is None: break
                        ptext = (driver.execute_script("return arguments[0].innerText;", parent) or "").upper()
                        tile_key = tile_name.upper().replace("-","")
                        if tile_key in ptext.replace("-","").replace(" ",""):
                            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
                            time.sleep(0.4)
                            driver.execute_script("arguments[0].click();", btn)
                            log(f"  ✅ {tile_name} DOWNLOAD clicked (strategy 2)", "ok")
                            return True
                except: continue
        except Exception as e:
            log(f"  Strategy 2 error: {e}", "warn")

        log(f"  ⚠ {tile_name} DOWNLOAD tile not found — check page dump above", "warn")
        return False

    def _get_latest_file(extensions):
        files = []
        for ext in extensions:
            files.extend(dl_dir.glob(f"*{ext}"))
        if not files: return None
        return max(files, key=lambda f: f.stat().st_mtime)

    def _rename_latest(save_name, extensions):
        try:
            f = _get_latest_file(extensions)
            if f:
                dest = dl_dir / save_name
                if not dest.exists():
                    f.rename(dest)
                log(f"  ✅ Saved: {save_name}", "ok")
                return True
        except Exception as e:
            log(f"  Rename failed: {e}", "warn")
        return False

    def _generate_and_download(save_name, gen_xpaths, dl_extensions, max_wait=120):
        """Click GENERATE → poll for download link → click it (mirrors generate_then_download_immediate)"""
        time.sleep(3)
        log(f"  Generate page: {driver.current_url}")

        gen_clicked = _try_click(gen_xpaths, timeout=10)
        if gen_clicked:
            log(f"  GENERATE clicked — polling for download link...")
        else:
            log(f"  ⚠ GENERATE button not found — checking for existing link...", "warn")
        time.sleep(3)

        DOWNLOAD_XP = [
            "//a[contains(text(),'Click here to download')]",
            "//a[contains(text(),'click here to download')]",
            "//a[contains(text(),'File 1')]",
            "//a[contains(text(),'File 2')]",
            "//a[contains(@href,'.xlsx')]",
            "//a[contains(@href,'.zip')]",
            "//a[contains(@href,'filedownload')]",
            "//a[contains(@href,'download') and string-length(@href) > 50]",
            "//button[contains(text(),'Download') or contains(text(),'DOWNLOAD')]",
        ]

        elapsed = 0
        while elapsed < max_wait:
            for xp in DOWNLOAD_XP:
                try:
                    els = driver.find_elements(By.XPATH, xp)
                    for el in els:
                        if el.is_displayed():
                            href = el.get_attribute("href") or ""
                            if len(href) > 20:
                                txt = el.text.strip() or href[:50]
                                log(f"  Download link found: '{txt}'")
                                log(f"  Downloading: {save_name}")
                                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                                time.sleep(0.5)
                                driver.execute_script("arguments[0].click();", el)
                                time.sleep(10)
                                if _rename_latest(save_name, dl_extensions):
                                    return True
                except: continue

            elapsed += 5
            time.sleep(5)

            if elapsed % 30 == 0:
                log(f"  Still waiting... ({elapsed}s) — refreshing page")
                try:
                    driver.refresh()
                    time.sleep(4)
                    if _is_session_lost():
                        log("  Session lost during wait — re-logging in...", "warn")
                        _do_login()
                        _go_to_dashboard()
                        _select_and_search(current_month[0])
                        _click_tile_download(current_tile[0])
                        time.sleep(8)
                except: pass

        log(f"  ⚠ No download link found for {save_name} after {max_wait}s", "warn")
        return False

    # Mutable refs for session recovery inside _generate_and_download
    current_month = [""]
    current_tile  = [""]

    GENERATE_JSON_XP = [
        "//button[contains(text(),'GENERATE JSON FILE TO DOWNLOAD')]",
        "//button[contains(text(),'GENERATE JSON')]",
        "//button[contains(text(),'Generate JSON')]",
        "//a[contains(text(),'GENERATE JSON')]",
    ]
    GENERATE_EXCEL_XP = [
        "//button[contains(text(),'GENERATE EXCEL FILE TO DOWNLOAD')]",
        "//button[contains(text(),'GENERATE EXCEL')]",
        "//button[contains(text(),'Generate Excel')]",
        "//a[contains(text(),'GENERATE EXCEL')]",
        # fallback to JSON if no Excel button
        "//button[contains(text(),'GENERATE JSON FILE TO DOWNLOAD')]",
        "//button[contains(text(),'Generate JSON')]",
    ]

    returns_set = set()
    if returns in ("all","gstr1"):  returns_set.add("GSTR1")
    if returns in ("all","gstr1a"): returns_set.add("GSTR1A")
    if returns in ("all","gstr2b"): returns_set.add("GSTR2B")
    if returns in ("all","gstr2a"): returns_set.add("GSTR2A")
    if returns in ("all","gstr3b"): returns_set.add("GSTR3B")
    if returns == "gstr1":  returns_set = {"GSTR1"}
    if returns == "gstr1a": returns_set = {"GSTR1A"}
    if returns == "gstr2b": returns_set = {"GSTR2B"}
    if returns == "gstr2a": returns_set = {"GSTR2A"}
    if returns == "gstr3b": returns_set = {"GSTR3B"}

    # ── Setup browser ────────────────────────────────────────────────
    try:
        opts = ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1280,900")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        )
        opts.add_experimental_option("prefs", {
            "download.default_directory": str(dl_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        })
        opts.add_experimental_option("excludeSwitches", ["enable-automation","enable-logging"])
        opts.add_experimental_option("useAutomationExtension", False)

        try:
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)
        except Exception:
            driver = webdriver.Chrome(options=opts)

        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
        })
        log("✅ Headless Chrome started", "ok")
        prog(5)

        # ── Step 1: Login ────────────────────────────────────────────
        if not password and not token:
            # Token mode — inject cookie instead of full login
            log("  Using session token to authenticate...")
            driver.get("https://services.gst.gov.in/services/login")
            time.sleep(2)
            driver.add_cookie({"name": "AuthToken", "value": token, "domain": ".gst.gov.in"})
            driver.add_cookie({"name": "token",     "value": token, "domain": ".gst.gov.in"})
            driver.get("https://services.gst.gov.in/services/auth/api/profile")
            time.sleep(3)
        else:
            _do_login()

        prog(15)

        # ── Step 2: Phase 1 — GSTR-3B (immediate PDF) + trigger generators ─
        triggered = {}
        total_months = len(MONTHS_LIST)

        log(f"\n📋 Phase 1 — Triggering file generation for {total_months} months...")
        log(f"   Returns: {', '.join(sorted(returns_set))}")

        for idx, (month_name, month_num, year) in enumerate(MONTHS_LIST):
            key = f"{month_name}_{year}"
            current_month[0] = month_name
            prog(15 + int(idx / total_months * 40))

            # GSTR-3B: PDF direct download — single click, check if already exists
            if "GSTR3B" in returns_set:
                try:
                    save_name = f"GSTR3B_{month_name}_{year}.pdf"
                    # ── SINGLE-TRIGGER GUARD: skip if already downloaded ──
                    if (dl_dir / save_name).exists() or (out_dir / save_name).exists():
                        log(f"── {month_name} {year}: GSTR-3B already downloaded — skipping ✓", "ok")
                        triggered[f"{key}_GSTR3B"] = "OK"
                        existing = out_dir / save_name
                        if not existing.exists():
                            import shutil as _shutil2
                            _shutil2.copy2(str(dl_dir / save_name), str(existing))
                        if not any(f["name"] == save_name for f in downloaded):
                            sz = existing.stat().st_size // 1024
                            downloaded.append({"name": save_name, "size": f"{sz} KB"})
                            with jobs_lock:
                                if job_id in jobs: jobs[job_id]["files"] = list(downloaded)
                    else:
                        log(f"\n── {month_name} {year}: GSTR-3B ──")
                        _go_to_dashboard()
                        _select_and_search(month_name)
                        current_tile[0] = "GSTR3B"
                        if _click_tile_download("GSTR3B"):
                            time.sleep(11)
                            if _rename_latest(save_name, [".pdf"]):
                                triggered[f"{key}_GSTR3B"] = "OK"
                                src_f = dl_dir / save_name
                                sz = src_f.stat().st_size // 1024
                                try: _shutil.copy2(str(src_f), str(out_dir / save_name))
                                except: pass
                                downloaded.append({"name": save_name, "size": f"{sz} KB"})
                                with jobs_lock:
                                    if job_id in jobs: jobs[job_id]["files"] = list(downloaded)
                            else:
                                triggered[f"{key}_GSTR3B"] = "NOT_FOUND"
                                save_failure_screenshot(f"GSTR3B {month_name} {year} — File Not Found after click")
                        else:
                            triggered[f"{key}_GSTR3B"] = "TILE_FAIL"
                            save_failure_screenshot(f"GSTR3B {month_name} {year} — Tile Not Found on Dashboard")
                except Exception as e:
                    log(f"  GSTR3B error [{month_name}]: {e}", "warn")
                    triggered[f"{key}_GSTR3B"] = f"ERR:{e}"
                    save_failure_screenshot(f"GSTR3B {month_name} {year} — Exception: {str(e)[:60]}")

            # GSTR-1: trigger GENERATE JSON
            if "GSTR1" in returns_set:
                try:
                    log(f"\n── {month_name} {year}: GSTR-1 (trigger generate) ──")
                    _go_to_dashboard()
                    _select_and_search(month_name)
                    current_tile[0] = "GSTR1"
                    if _click_tile_download("GSTR1"):
                        time.sleep(8)
                        if _try_click(GENERATE_JSON_XP, timeout=8):
                            log(f"  GSTR-1 GENERATE JSON clicked ✓")
                            triggered[f"{key}_GSTR1"] = "TRIGGERED"
                            time.sleep(2)
                        else:
                            triggered[f"{key}_GSTR1"] = "GEN_FAIL"
                            save_failure_screenshot(f"GSTR1 {month_name} {year} — Generate Button Not Found")
                    else:
                        triggered[f"{key}_GSTR1"] = "TILE_FAIL"
                        save_failure_screenshot(f"GSTR1 {month_name} {year} — Tile Not Found on Dashboard")
                except Exception as e:
                    log(f"  GSTR1 trigger error [{month_name}]: {e}", "warn")
                    triggered[f"{key}_GSTR1"] = f"ERR:{e}"
                    save_failure_screenshot(f"GSTR1 {month_name} {year} — Exception: {str(e)[:60]}")

            # GSTR-1A: trigger GENERATE JSON
            if "GSTR1A" in returns_set:
                try:
                    log(f"\n── {month_name} {year}: GSTR-1A (trigger generate) ──")
                    _go_to_dashboard()
                    _select_and_search(month_name)
                    current_tile[0] = "GSTR1A"
                    if _click_tile_download("GSTR1A"):
                        time.sleep(8)
                        if _try_click(GENERATE_JSON_XP, timeout=8):
                            log(f"  GSTR-1A GENERATE JSON clicked ✓")
                            triggered[f"{key}_GSTR1A"] = "TRIGGERED"
                            time.sleep(2)
                        else:
                            triggered[f"{key}_GSTR1A"] = "GEN_FAIL"
                            save_failure_screenshot(f"GSTR1A {month_name} {year} — Generate Button Not Found")
                    else:
                        triggered[f"{key}_GSTR1A"] = "TILE_NOT_FOUND"
                        save_failure_screenshot(f"GSTR1A {month_name} {year} — Tile Not Found on Dashboard")
                except Exception as e:
                    log(f"  GSTR1A trigger error [{month_name}]: {e}", "warn")
                    triggered[f"{key}_GSTR1A"] = f"ERR:{e}"
                    save_failure_screenshot(f"GSTR1A {month_name} {year} — Exception: {str(e)[:60]}")

            # GSTR-2B: GENERATE EXCEL + download immediately (generates in seconds)
            if "GSTR2B" in returns_set:
                try:
                    log(f"\n── {month_name} {year}: GSTR-2B (generate + download) ──")
                    _go_to_dashboard()
                    _select_and_search(month_name)
                    save_name = f"GSTR2B_{month_name}_{year}.xlsx"
                    current_tile[0] = "GSTR2B"
                    if _click_tile_download("GSTR2B"):
                        time.sleep(8)
                        if _generate_and_download(save_name, GENERATE_EXCEL_XP, [".xlsx",".zip"], max_wait=90):
                            triggered[f"{key}_GSTR2B"] = "OK"
                            src_f = dl_dir / save_name
                            sz = src_f.stat().st_size // 1024
                            # Copy immediately to out_dir so /api/dl-file works during live polling
                            try: _shutil.copy2(str(src_f), str(out_dir / save_name))
                            except: pass
                            downloaded.append({"name": save_name, "size": f"{sz} KB"})
                            with jobs_lock:
                                if job_id in jobs: jobs[job_id]["files"] = list(downloaded)
                        else:
                            triggered[f"{key}_GSTR2B"] = "NOT_FOUND"
                            save_failure_screenshot(f"GSTR2B {month_name} {year} — Download Link Not Found")
                    else:
                        triggered[f"{key}_GSTR2B"] = "TILE_FAIL"
                        save_failure_screenshot(f"GSTR2B {month_name} {year} — Tile Not Found on Dashboard")
                except Exception as e:
                    log(f"  GSTR2B error [{month_name}]: {e}", "warn")
                    triggered[f"{key}_GSTR2B"] = f"ERR:{e}"
                    save_failure_screenshot(f"GSTR2B {month_name} {year} — Exception: {str(e)[:60]}")

            # GSTR-2A: trigger GENERATE EXCEL
            if "GSTR2A" in returns_set:
                try:
                    log(f"\n── {month_name} {year}: GSTR-2A (trigger generate) ──")
                    _go_to_dashboard()
                    _select_and_search(month_name)
                    current_tile[0] = "GSTR2A"
                    if _click_tile_download("GSTR2A"):
                        time.sleep(8)
                        if _try_click(GENERATE_EXCEL_XP, timeout=8):
                            log(f"  GSTR-2A GENERATE EXCEL clicked ✓")
                            triggered[f"{key}_GSTR2A"] = "TRIGGERED"
                            time.sleep(2)
                        else:
                            triggered[f"{key}_GSTR2A"] = "GEN_FAIL"
                            save_failure_screenshot(f"GSTR2A {month_name} {year} — Generate Button Not Found")
                    else:
                        triggered[f"{key}_GSTR2A"] = "TILE_FAIL"
                        save_failure_screenshot(f"GSTR2A {month_name} {year} — Tile Not Found on Dashboard")
                except Exception as e:
                    log(f"  GSTR2A trigger error [{month_name}]: {e}", "warn")
                    triggered[f"{key}_GSTR2A"] = f"ERR:{e}"
                    save_failure_screenshot(f"GSTR2A {month_name} {year} — Exception: {str(e)[:60]}")

        prog(55)

        # ── Step 3: Phase 2 — Download generated files (GSTR-1, 1A, 2A) ─
        need_phase2 = any(
            triggered.get(f"{month_name}_{year}_{rt}") == "TRIGGERED"
            for month_name, month_num, year in MONTHS_LIST
            for rt in ("GSTR1","GSTR1A","GSTR2A")
        )

        if need_phase2:
            log(f"\n📥 Phase 2 — Downloading generated files (portal generates in ~30s-2min)...")
            log("  Waiting 60 seconds for portal to finish generating files...")
            time.sleep(60)

            ret_config = {
                "GSTR1":  (GENERATE_JSON_XP,  [".zip",".json"]),
                "GSTR1A": (GENERATE_JSON_XP,  [".zip",".json"]),
                "GSTR2A": (GENERATE_EXCEL_XP, [".zip",".xlsx"]),
            }
            p2_total = sum(
                1 for mn, mm, yr in MONTHS_LIST
                for rt in ret_config
                if triggered.get(f"{mn}_{yr}_{rt}") == "TRIGGERED"
            )
            p2_done = 0

            for idx2, (month_name, month_num, year) in enumerate(MONTHS_LIST):
                key = f"{month_name}_{year}"
                current_month[0] = month_name

                for ret_type, (gen_xp, dl_exts) in ret_config.items():
                    if ret_type not in returns_set: continue
                    tkey = f"{key}_{ret_type}"
                    if triggered.get(tkey) != "TRIGGERED": continue

                    save_name = f"{ret_type}_{month_name}_{year}" + (".zip" if ret_type != "GSTR2A" else ".xlsx")
                    log(f"\n── {month_name} {year}: {ret_type} (download) ──")
                    current_tile[0] = ret_type

                    try:
                        _go_to_dashboard()
                        _select_and_search(month_name)
                        if _click_tile_download(ret_type):
                            time.sleep(8)
                            if _generate_and_download(save_name, gen_xp, dl_exts, max_wait=120):
                                triggered[tkey] = "OK"
                                src_f = dl_dir / save_name
                                sz = src_f.stat().st_size // 1024
                                # Copy immediately to out_dir so /api/dl-file works during live polling
                                try: _shutil.copy2(str(src_f), str(out_dir / save_name))
                                except: pass
                                downloaded.append({"name": save_name, "size": f"{sz} KB"})
                                with jobs_lock:
                                    if job_id in jobs: jobs[job_id]["files"] = list(downloaded)
                            else:
                                triggered[tkey] = "NOT_FOUND"
                                save_failure_screenshot(f"{ret_type} {month_name} {year} — Download Link Not Found (Phase 2)")
                        else:
                            triggered[tkey] = "TILE_FAIL"
                            save_failure_screenshot(f"{ret_type} {month_name} {year} — Tile Not Found (Phase 2)")
                    except Exception as e:
                        log(f"  {ret_type} download error [{month_name}]: {e}", "warn")
                        triggered[tkey] = f"ERR:{e}"
                        save_failure_screenshot(f"{ret_type} {month_name} {year} — Exception Phase 2: {str(e)[:60]}")

                    p2_done += 1
                    prog(55 + int(p2_done / max(p2_total,1) * 40))

        prog(97)

        # ── Step 4: Copy all downloaded files to job output + ZIP ───────
        log("\n📦 Packaging files...")
        import zipfile as _zf
        zip_name = f"GST_Downloads_{client_name.replace(' ','_')}_{fy}.zip"
        zip_path = out_dir / zip_name
        with _zf.ZipFile(str(zip_path), "w", _zf.ZIP_DEFLATED) as zf:
            for item in downloaded:
                src = dl_dir / item["name"]
                if src.exists():
                    dest_in_job = out_dir / item["name"]
                    _shutil.copy2(str(src), str(dest_in_job))
                    zf.write(str(src), item["name"])
        if zip_path.exists():
            sz = zip_path.stat().st_size // 1024
            downloaded.insert(0, {"name": zip_name, "size": f"{sz} KB"})
            log(f"✅ ZIP created: {zip_name} ({sz} KB)", "ok")

        prog(100)
        n = len([d for d in downloaded if not d["name"].endswith(".zip")]) if len(downloaded) > 1 else 0
        log(f"✅ Complete! {n} file(s) downloaded. Click ZIP to save all.", "ok")

        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"]  = downloaded

    except Exception as exc:
        import traceback
        log(f"❌ Error: {exc}", "err")
        for ln in traceback.format_exc().split("\n"):
            if ln.strip(): log(f"  {ln}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)
    finally:
        if driver:
            try: driver.quit()
            except: pass


# ═══════════════════════════════════════════════════════════════════
# BULK DOWNLOAD — multiple companies from Excel list
# ═══════════════════════════════════════════════════════════════════

@app.route("/api/bulk-template")
def bulk_template():
    """Return a sample Excel template for bulk download."""
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Companies"
    headers = ["COMPANY NAME", "GSTIN", "USERNAME", "NOTES"]
    widths  = [30, 20, 20, 30]
    for i,(h,w) in enumerate(zip(headers,widths),1):
        c = ws.cell(row=1, column=i, value=h)
        c.font = Font(bold=True, color="FFFFFF")
        c.fill = PatternFill("solid", fgColor="1a2235")
        c.alignment = Alignment(horizontal="center")
        ws.column_dimensions[chr(64+i)].width = w
    examples = [
        ["ABC Traders", "33ABCDE1234F1ZX", "abctraders_gst", "Example row"],
        ["XYZ Pvt Ltd", "29XYZAB5678G2ZY", "xyz_gst_login",  ""],
    ]
    for row in examples:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf); buf.seek(0)
    from flask import Response
    return Response(buf.read(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=bulk_companies_template.xlsx"})


@app.route("/api/bulk-start", methods=["POST"])
@rate_limit(limit=5, window=60)
def api_bulk_start():
    fobj = request.files.get("companies_file")
    if not fobj:
        return jsonify(error="No file uploaded"), 400
    fy      = request.form.get("fy","2025-26")
    returns = request.form.get("returns","all")

    # Parse the Excel
    import io, openpyxl
    try:
        wb = openpyxl.load_workbook(io.BytesIO(fobj.read()), read_only=True, data_only=True)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        wb.close()
    except Exception as e:
        return jsonify(error=f"Cannot read Excel: {e}"), 400

    if not rows or len(rows) < 2:
        return jsonify(error="Excel is empty or has only headers"), 400

    headers = [str(c or "").strip().upper() for c in rows[0]]
    def _col(*names):
        for n in names:
            if n in headers: return headers.index(n)
        return -1

    ci_name = _col("COMPANY NAME","NAME","COMPANY")
    ci_gst  = _col("GSTIN","GSTIN NO","GST")
    ci_user = _col("USERNAME","USER","LOGIN","USER NAME")

    if ci_gst < 0:
        return jsonify(error="Column 'GSTIN' not found in Excel"), 400

    companies = []
    for row in rows[1:]:
        gstin = str(row[ci_gst] or "").strip().upper() if ci_gst >= 0 else ""
        if not gstin or len(gstin) != 15:
            continue
        companies.append({
            "name":     str(row[ci_name] or "").strip() if ci_name >= 0 else gstin,
            "gstin":    gstin,
            "username": str(row[ci_user] or "").strip() if ci_user >= 0 else "",
        })

    if not companies:
        return jsonify(error="No valid GSTINs found in Excel"), 400

    job_id  = str(uuid.uuid4())[:8]
    out_dir = OUTPUT_DIR / job_id
    out_dir.mkdir(parents=True, exist_ok=True)

    with jobs_lock:
        jobs[job_id] = {
            "status":"running","progress":0,
            "logs":[{"type":"info","msg":f"Loaded {len(companies)} companies. Starting…"}],
            "files":[],"error":None,
            "captcha_needed":False,"captcha_img":None,"captcha_company":None,
            "out_dir":str(out_dir),"counter":"",
        }

    sess = {"token_q": _queue.Queue(), "screenshot": None, "refresh_event": threading.Event()}
    with _sess_lock:
        _sessions[job_id] = sess

    def _run():
        try:
            _bulk_worker(job_id, companies, fy, returns, sess, out_dir)
        except Exception as exc:
            import traceback as _tb
            with jobs_lock:
                if job_id in jobs:
                    jobs[job_id]["status"] = "error"
                    jobs[job_id]["error"]  = str(exc)
                    for ln in _tb.format_exc().split("\n"):
                        if ln.strip():
                            jobs[job_id]["logs"].append({"type":"err","msg":ln})
        finally:
            with _sess_lock:
                _sessions.pop(job_id, None)

    threading.Thread(target=_run, daemon=True).start()
    return jsonify(job_id=job_id, total=len(companies))


@app.route("/api/bulk-token/<job_id>", methods=["POST"])
def api_bulk_token(job_id):
    """User submits token for a company during bulk download."""
    token = (request.get_json(silent=True) or {}).get("token","").strip()
    if not token:
        return jsonify(ok=False, error="Empty token")
    with _sess_lock:
        sess = _sessions.get(job_id)
    if not sess:
        return jsonify(ok=False, error="No active session")
    sess["token_q"].put(token)
    # Clear captcha_needed immediately so UI hides the card
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["captcha_needed"]  = False
            jobs[job_id]["captcha_company"] = None
    return jsonify(ok=True)


def _bulk_worker(job_id, companies, fy, returns, sess, out_dir):
    """Process each company one by one, requesting a token for each."""
    import requests as _req

    def log(msg, t="info"):
        print(f"[BULK {job_id}] {msg}")
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["logs"].append({"type":t,"msg":msg})

    def prog(p):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["progress"] = p

    def set_counter(i, total):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["counter"] = f"Company {i}/{total}"

    total = len(companies)
    all_files = []
    out_path  = Path(out_dir)

    for idx, company in enumerate(companies, 1):
        set_counter(idx, total)
        name     = company["name"]
        gstin    = company["gstin"]
        username = company["username"]
        log(f"━━━ [{idx}/{total}] {name} ({gstin}) ━━━", "info")
        prog(int((idx-1)/total*100))

        # ── Ask user for token ──────────────────────────────────────
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["captcha_needed"]  = True
                jobs[job_id]["captcha_company"] = {"name":name,"gstin":gstin,"username":username}

        log(f"  Waiting for AuthToken from user for {name}…")

        # Wait up to 15 minutes for token
        try:
            token = sess["token_q"].get(timeout=900)
        except _queue.Empty:
            log(f"  ⏱ Timeout waiting for token — skipping {name}", "warn")
            continue

        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["captcha_needed"]  = False
                jobs[job_id]["captcha_company"] = None

        log(f"  Token received — starting download for {name}…", "ok")

        # ── Download returns for this company ───────────────────────
        company_dir = out_path / gstin
        company_dir.mkdir(exist_ok=True)

        S = _req.Session()
        S.headers.update({
            "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept":          "application/json, text/plain, */*",
            "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
            "Authorization":   f"Bearer {token}",
        })
        S.cookies.set("AuthToken", token, domain=".gst.gov.in")
        S.cookies.set("token",     token, domain=".gst.gov.in")

        # Verify token
        try:
            vr = S.get(f"https://services.gst.gov.in/services/api/search/taxpayerDetails?gstin={gstin}",
                       timeout=15)
            if vr.status_code in (401, 403):
                log(f"  ⚠ Token rejected for {name} — skipping", "warn")
                continue
        except Exception as ve:
            log(f"  Token check warning: {ve} — continuing", "warn")

        fy_start = int(fy.split("-")[0])
        MONTHS = [
            ("April","04",str(fy_start)),    ("May","05",str(fy_start)),
            ("June","06",str(fy_start)),     ("July","07",str(fy_start)),
            ("August","08",str(fy_start)),   ("September","09",str(fy_start)),
            ("October","10",str(fy_start)),  ("November","11",str(fy_start)),
            ("December","12",str(fy_start)), ("January","01",str(fy_start+1)),
            ("February","02",str(fy_start+1)),("March","03",str(fy_start+1)),
        ]
        BASE_RET = "https://return.gst.gov.in/returns/auth"
        co_files = []

        def dl_one(ret_type, mon_name, mon_num, mon_yr):
            period = f"{mon_num}{mon_yr}"
            ext    = {"gstr1":".zip","gstr1a":".zip","gstr2b":".xlsx",
                      "gstr2a":".xlsx","gstr3b":".pdf"}.get(ret_type,".zip")
            fname  = f"{gstin}_{ret_type.upper()}_{mon_name}_{mon_yr}{ext}"
            fpath  = company_dir / fname
            urls   = [
                f"{BASE_RET}/{ret_type}/download?gstin={gstin}&ret_period={period}&action_type=download",
                f"{BASE_RET}/{ret_type}?action=download&gstin={gstin}&ret_period={period}",
                f"https://return.gst.gov.in/returns/api/{ret_type}/{gstin}/{period}/download",
            ]
            for url in urls:
                try:
                    r = S.get(url, timeout=60)
                    if r.status_code == 200 and len(r.content) > 500:
                        # Check if token expired (portal returns JSON error instead of file)
                        if r.headers.get("content-type","").startswith("application/json"):
                            ec = r.json().get("errorCode","")
                            if ec in ("AUTH4033","AUTH4035","SWEB_9000"):
                                log(f"  ⚠ Token expired mid-download for {name}", "warn")
                                return "TOKEN_EXPIRED"
                        fpath.write_bytes(r.content)
                        sz = fpath.stat().st_size // 1024
                        co_files.append({"name":fname,"size":f"{sz} KB"})
                        log(f"  ✓ {ret_type.upper()} {mon_name} {mon_yr} ({sz} KB)", "ok")
                        return "OK"
                except Exception:
                    pass
            log(f"  – {ret_type.upper()} {mon_name} {mon_yr}: not available", "warn")
            return "SKIP"

        ret_types = []
        if returns in ("all","gstr1"):  ret_types.append("gstr1")
        if returns in ("all","gstr1a"): ret_types.append("gstr1a")
        if returns in ("all","gstr2b"): ret_types.append("gstr2b")
        if returns in ("all","gstr2a"): ret_types.append("gstr2a")
        if returns in ("all","gstr3b"): ret_types.append("gstr3b")

        token_expired = False
        for rt in ret_types:
            if token_expired: break
            log(f"  ── {rt.upper()} ──")
            for mn, mm, my in MONTHS:
                result = dl_one(rt, mn, mm, my)
                if result == "TOKEN_EXPIRED":
                    token_expired = True
                    # Ask user for a fresh token
                    log(f"  🔄 Token expired — requesting new token for {name}…", "warn")
                    with jobs_lock:
                        if job_id in jobs:
                            jobs[job_id]["captcha_needed"]  = True
                            jobs[job_id]["captcha_company"] = {
                                "name": f"{name} (RE-LOGIN)", "gstin": gstin, "username": username}
                    # Clear token queue and wait
                    while not sess["token_q"].empty():
                        try: sess["token_q"].get_nowait()
                        except: pass
                    try:
                        new_token = sess["token_q"].get(timeout=600)
                        S.headers["Authorization"] = f"Bearer {new_token}"
                        S.cookies.set("AuthToken", new_token, domain=".gst.gov.in")
                        S.cookies.set("token",     new_token, domain=".gst.gov.in")
                        with jobs_lock:
                            if job_id in jobs:
                                jobs[job_id]["captcha_needed"]  = False
                                jobs[job_id]["captcha_company"] = None
                        token_expired = False
                        log(f"  ✅ New token received — resuming {name}…", "ok")
                        # Retry this month
                        dl_one(rt, mn, mm, my)
                    except _queue.Empty:
                        log(f"  ⏱ Re-login timeout — stopping {name}", "warn")
                        break

        all_files.extend(co_files)
        log(f"  ✅ {name}: {len(co_files)} file(s) downloaded", "ok")

        # Update running file list
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["files"] = list(all_files)

    # ── Create ZIP of everything ────────────────────────────────────
    prog(98)
    if all_files:
        zip_name = f"BULK_DOWNLOAD_{fy}.zip"
        zip_path = out_path / zip_name
        import zipfile as _zf
        with _zf.ZipFile(str(zip_path), "w", _zf.ZIP_DEFLATED) as zf:
            for f in all_files:
                # find the file
                for sub in out_path.rglob(f["name"]):
                    zf.write(str(sub), f["name"])
                    break
        sz = zip_path.stat().st_size // 1024
        all_files.insert(0, {"name": zip_name, "size": f"{sz} KB"})
        log(f"✅ ZIP created: {zip_name} ({sz} KB)", "ok")

    prog(100)
    log(f"✅ Bulk complete — {len(companies)} companies, {len(all_files)-1} total files.", "ok")
    with jobs_lock:
        if job_id in jobs:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"]  = all_files
            jobs[job_id]["captcha_needed"]  = False
            jobs[job_id]["captcha_company"] = None


# ── Startup ───────────────────────────────────────────────────────

# ═══════════════════════════════════════════════════════════════════
# INCOME TAX RECONCILIATION — Upload + Worker + Job + Download
# Same pattern as GST reconciliation routes above
# ═══════════════════════════════════════════════════════════════════

def run_it_reconciliation(job_id):
    def log(msg, t="info"):
        with jobs_lock:
            jobs[job_id]["logs"].append({"type": t, "msg": msg})
    def prog(p):
        with jobs_lock:
            jobs[job_id]["progress"] = p

    try:
        job          = jobs[job_id]
        company_name = job["company_name"]
        pan          = job["pan"]
        gstin        = job["gstin"]
        fy           = job["fy"]
        job_dir      = Path(job["job_dir"])
        out_dir      = Path(job["out_dir"])
        saved        = job["saved"]

        log(f"Starting IT Reconciliation: {company_name} ({pan}) FY {fy}")
        prog(5)

        # -- Move uploaded files into job_dir --------------------------
        for zone, dest_prefix in [("it26as","26AS"), ("itais","AIS"), ("itgst","GST_RECON")]:
            for fpath in saved.get(zone, []):
                ext  = Path(fpath).suffix.lower()
                dest = job_dir / f"{dest_prefix}{ext}"
                if not dest.exists():
                    try:    Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  {dest_prefix}: {dest.name}")

        prog(20)

        # -- Load it_recon_engine.py (same as gst_suite_final.py loading) --
        engine_path = _find_engine("it_recon_engine.py")
        if not engine_path:
            raise FileNotFoundError(
                "it_recon_engine.py not found. "
                "Place it in the same folder as app.py.")

        log("Loading IT reconciliation engine...")
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location("it_recon", str(engine_path))
        it   = _ilu.module_from_spec(spec)
        spec.loader.exec_module(it)
        prog(30)

        log("Parsing 26AS PDF and AIS PDF...")
        out_xl = it.write_it_reconciliation(
            str(job_dir), company_name, pan, gstin, fy, log=None
        )
        prog(85)

        # -- Collect outputs -------------------------------------------
        output_files = []
        for fp in sorted(job_dir.glob("IT_RECONCILIATION_*.xlsx")):
            dest_fp = out_dir / fp.name
            shutil.copy2(str(fp), str(dest_fp))
            sz = dest_fp.stat().st_size // 1024
            output_files.append({"name": fp.name, "size": f"{sz} KB"})
            log(f"  ✓ {fp.name} ({sz} KB)", "ok")

        if not output_files:
            raise RuntimeError("No IT Reconciliation Excel generated. "
                               "Check that 26AS PDF was uploaded correctly.")

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready to download.", "ok")
        with jobs_lock:
            jobs[job_id]["status"] = "done"
            jobs[job_id]["files"]  = output_files
        _cleanup_uploads(job_id)

    except Exception as exc:
        import traceback
        log(f"Error: {exc}", "err")
        for line in traceback.format_exc().split("\n"):
            if line.strip(): log(f"  {line}", "err")
        with jobs_lock:
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"]  = str(exc)
        _cleanup_uploads(job_id)


@app.route("/api/it-upload", methods=["POST"])
@rate_limit(limit=20, window=60)
def api_it_upload():
    _cleanup_old_jobs()
    company_name = request.form.get("company_name","").strip()
    pan          = request.form.get("pan","").strip().upper()
    gstin        = request.form.get("gstin","").strip().upper()
    fy           = request.form.get("fy","2024-25").strip() or "2024-25"

    if not company_name:
        return jsonify(error="Company name is required"), 400
    if not pan or len(pan) != 10:
        return jsonify(error="PAN must be 10 characters (e.g. ABCDE1234F)"), 400

    job_id  = str(uuid.uuid4())[:8]
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    saved = {k: [] for k in ("it26as", "itais", "itgst")}
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
            "status":       "queued",
            "progress":     0,
            "logs":         [],
            "files":        [],
            "error":        None,
            "company_name": company_name,
            "pan":          pan,
            "gstin":        gstin,
            "fy":           fy,
            "job_dir":      str(job_dir),
            "out_dir":      str(out_dir),
            "saved":        saved,
        }

    threading.Thread(target=run_it_reconciliation, args=(job_id,), daemon=True).start()
    return jsonify(job_id=job_id)


@app.route("/api/it-job/<job_id>")
@rate_limit(limit=120, window=60)
def api_it_job(job_id):
    with jobs_lock:
        job = jobs.get(job_id)
        if not job:
            return jsonify(error="Job not found"), 404
        new_logs = job["logs"][:]
        job["logs"] = []
        return jsonify(
            status   = job["status"],
            progress = job["progress"],
            logs     = new_logs,
            files    = job["files"],
            error    = job["error"],
        )


@app.route("/api/it-dl/<job_id>/<filename>")
@rate_limit(limit=30, window=60)
def api_it_dl(job_id, filename):
    filename = Path(filename).name
    if not re.match(r'^[\w\-. ()]+\.(xlsx|pdf|zip)$', filename):
        abort(400)
    fp = OUTPUT_DIR / job_id / filename
    if not fp.exists() or not fp.is_file():
        abort(404)
    return send_file(str(fp), as_attachment=True, download_name=filename)


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
