"""
RPR GST Reconciliation — ONLINE DEMO (Render.com)
==================================================
• Hosted online — customers test from browser, no install needed
• Reconciliation only — 1 run per IP (attract → push to buy)
• After result: strong "Buy Full Suite" CTA with WhatsApp button
• Deploy to Render.com — reads $PORT automatically
• DIFFERENT from demo_app.py (which is the local EXE — unlimited runs)
"""

import os, sys, json, zipfile, re, time, shutil, uuid, threading
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, send_file, render_template_string, abort
import tempfile

# ── Contact / Branding ─────────────────────────────────────────────
DEMO_CONTACT_PHONE    = "7845998125"
DEMO_CONTACT_EMAIL    = "auprabakaran@gmail.com"
DEMO_CONTACT_WHATSAPP = "917845998125"
DEMO_PRICE_BASIC      = "₹2,500/year"
DEMO_PRICE_PRO        = "₹6,500/year"

# ── Online Demo Limit ──────────────────────────────────────────────
MAX_RUNS_PER_IP = 1   # 1 free reconciliation per IP — then push to buy

# ── Directories ────────────────────────────────────────────────────
def _get_app_dir(subfolder):
    base = Path(tempfile.gettempdir()) / "rpr_online_demo"
    d = base / subfolder
    d.mkdir(parents=True, exist_ok=True)
    return d

UPLOAD_DIR  = _get_app_dir("uploads")
OUTPUT_DIR  = _get_app_dir("outputs")
ALLOWED_EXT = {".zip", ".xlsx", ".xls", ".pdf", ".json"}
MAX_FILE_MB = 30
JOB_TTL_S   = 1800

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024

jobs      = {}
jobs_lock = threading.Lock()

# ── IP run tracking ────────────────────────────────────────────────
_ip_runs      = {}  # ip -> run count
_ip_runs_lock = threading.Lock()

def _check_ip_limit(ip):
    with _ip_runs_lock:
        count = _ip_runs.get(ip, 0)
        if count >= MAX_RUNS_PER_IP:
            return False
        _ip_runs[ip] = count + 1
        return True

# ── Rate limiting ──────────────────────────────────────────────────
_rate = {}
_rate_lock = threading.Lock()

def _check_rate(ip, limit=15, window=60):
    now = time.time()
    with _rate_lock:
        hits = [t for t in _rate.get(ip, []) if now - t < window]
        if len(hits) >= limit: return False
        hits.append(now); _rate[ip] = hits
    return True

def rate_limit(limit=15, window=60):
    from functools import wraps
    def dec(f):
        @wraps(f)
        def wrapped(*a, **kw):
            ip = request.headers.get("X-Forwarded-For","").split(",")[0].strip() or request.remote_addr or "unknown"
            if not _check_rate(ip, limit, window):
                return jsonify(error="Too many requests. Please wait 1 minute."), 429
            return f(*a, **kw)
        return wrapped
    return dec

# ── Helpers ────────────────────────────────────────────────────────
def _get_ip():
    return request.headers.get("X-Forwarded-For","").split(",")[0].strip() or request.remote_addr or "unknown"

def _cleanup_old_jobs():
    try:
        now = time.time()
        for d in [UPLOAD_DIR, OUTPUT_DIR]:
            for sub in d.iterdir():
                if sub.is_dir() and (now - sub.stat().st_mtime) > JOB_TTL_S:
                    shutil.rmtree(str(sub), ignore_errors=True)
    except Exception: pass

def _cleanup_uploads(job_id):
    try:
        up = UPLOAD_DIR / job_id
        if up.exists(): shutil.rmtree(str(up), ignore_errors=True)
    except Exception: pass

def _find_engine(name):
    _ALIASES = {
        "gst_suite_final.py": ["gst_suite_v32.py", "gst_suite_v31.py", "gst_suite_final.py"],
        "it_recon_engine.py": ["it_recon_engine.py"],
    }
    candidates = _ALIASES.get(name, [name])
    search_dirs = [
        Path(__file__).parent,
        Path(os.getcwd()),
        Path(sys._MEIPASS) if hasattr(sys, "_MEIPASS") else Path("."),
    ]
    for cname in candidates:
        for d in search_dirs:
            if not d.exists(): continue
            loc = d / cname
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
    except Exception: pass
    return None, None

@app.before_request
def block_scripts():
    p = request.path.lower()
    if p.endswith((".py", ".pyc")):
        abort(403)

# ══════════════════════════════════════════════════════════════════
# HTML — Online Demo UI
# ══════════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RPR GST Reconciliation — Try Free Online</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#06080e;--surf:#0d1422;--surf2:#141d2e;--bdr:#192436;
  --accent:#00d4ff;--accent2:#7c3aed;--grn:#00e676;--org:#ff9800;
  --red:#ff3d57;--txt:#d8e8f5;--muted:#526070;--gold:#ffc107;
  --mono:'IBM Plex Mono',monospace;--sans:'Syne',sans-serif;
}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;
  background:radial-gradient(ellipse 70% 45% at 50% -5%,rgba(0,212,255,.06),transparent),
             radial-gradient(ellipse 50% 35% at 90% 80%,rgba(124,58,237,.05),transparent);
  pointer-events:none;z-index:0}
.wrap{max-width:800px;margin:0 auto;padding:1.5rem 1.2rem;position:relative;z-index:1}

/* Try banner */
.try-bar{background:linear-gradient(135deg,rgba(0,230,118,.08),rgba(0,212,255,.06));
  border:1.5px solid rgba(0,230,118,.3);border-radius:13px;
  padding:.85rem 1.1rem;margin-bottom:1.1rem;text-align:center}
.try-bar h2{font-size:.88rem;font-weight:800;color:var(--grn);letter-spacing:.04em;
  text-transform:uppercase;margin-bottom:.3rem}
.try-bar p{font-size:.73rem;color:var(--muted);line-height:1.6}
.try-bar strong{color:var(--txt)}

/* Header */
header{text-align:center;padding:1.5rem 0 .9rem}
.logo{display:inline-flex;align-items:center;gap:.65rem;margin-bottom:.7rem}
.logo-icon{width:44px;height:44px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:11px;display:flex;align-items:center;justify-content:center;
  font-size:1.35rem;box-shadow:0 3px 16px rgba(0,212,255,.28)}
.logo-text{font-size:.9rem;font-weight:800;letter-spacing:.12em;text-transform:uppercase;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
h1{font-size:clamp(1.35rem,3.2vw,2rem);font-weight:800;letter-spacing:-.02em;margin-bottom:.3rem}
h1 em{font-style:normal;background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
.sub{color:var(--muted);font-size:.76rem;font-family:var(--mono)}
.badges{display:flex;gap:.45rem;justify-content:center;flex-wrap:wrap;margin-top:.6rem}
.badge{display:inline-flex;align-items:center;gap:.28rem;padding:.2rem .6rem;
  border-radius:100px;font-size:.65rem;font-weight:700;font-family:var(--mono)}
.bg-grn{background:rgba(0,230,118,.1);color:var(--grn);border:1px solid rgba(0,230,118,.3)}
.bg-gold{background:rgba(255,193,7,.08);color:var(--gold);border:1px solid rgba(255,193,7,.3)}
.bg-blue{background:rgba(0,212,255,.07);color:var(--accent);border:1px solid rgba(0,212,255,.25)}

/* Buy bar */
.buy-bar{background:linear-gradient(135deg,rgba(124,58,237,.14),rgba(0,212,255,.07));
  border:1.5px solid rgba(124,58,237,.4);border-radius:13px;
  padding:.95rem 1.2rem;margin-bottom:1.1rem;
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.75rem}
.buy-left h3{font-size:.85rem;font-weight:800;color:#a78bfa;margin-bottom:.28rem}
.buy-left p{font-size:.7rem;color:var(--muted);line-height:1.6}
.buy-left p strong{color:var(--txt)}
.buy-btns{display:flex;flex-direction:column;gap:.38rem;min-width:165px}
.btn-wa{padding:.52rem .95rem;background:linear-gradient(135deg,#25d366,#128c7e);
  border:none;border-radius:8px;color:#fff;font-family:var(--sans);font-size:.74rem;
  font-weight:700;cursor:pointer;text-align:center;text-decoration:none;display:block;
  transition:transform .15s,box-shadow .15s;letter-spacing:.04em}
.btn-wa:hover{transform:translateY(-2px);box-shadow:0 5px 16px rgba(37,211,102,.4)}
.btn-mail{padding:.52rem .95rem;background:rgba(124,58,237,.2);
  border:1px solid rgba(124,58,237,.45);border-radius:8px;color:#a78bfa;
  font-family:var(--sans);font-size:.74rem;font-weight:700;
  text-align:center;text-decoration:none;display:block;transition:all .15s}
.btn-mail:hover{background:rgba(124,58,237,.35)}

/* Card */
.card{background:var(--surf);border:1px solid var(--bdr);border-radius:13px;
  padding:1.25rem;margin-bottom:1rem}
.ct{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.09em;
  color:var(--accent);margin-bottom:.8rem;display:flex;align-items:center;gap:.38rem}
.ct::before{content:'';width:3px;height:.9em;background:var(--accent);border-radius:2px}

/* Form */
.fg2{display:grid;grid-template-columns:1fr 1fr;gap:.65rem}
@media(max-width:500px){.fg2{grid-template-columns:1fr}}
.fg{display:flex;flex-direction:column;gap:.26rem}
label{font-size:.63rem;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--muted)}
input[type=text],select{
  background:var(--surf2);border:1px solid var(--bdr);border-radius:7px;
  padding:.48rem .72rem;color:var(--txt);font-family:var(--mono);font-size:.78rem;
  transition:border-color .2s;width:100%}
input:focus,select:focus{outline:none;border-color:var(--accent)}
select option{background:var(--surf)}

/* Dropzones */
.dg{display:grid;grid-template-columns:repeat(auto-fill,minmax(142px,1fr));gap:.55rem;margin-top:.5rem}
.dz{background:var(--surf2);border:2px dashed var(--bdr);border-radius:9px;
  padding:.85rem .55rem;text-align:center;cursor:pointer;transition:all .2s;
  position:relative;min-height:88px;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:.28rem}
.dz:hover,.dz.drag-over{border-color:var(--accent);background:rgba(0,212,255,.04)}
.dz.has-files{border-color:var(--grn);border-style:solid;background:rgba(0,230,118,.04)}
.dz-ic{font-size:1.4rem;line-height:1}
.dz-lb{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.dz-ht{font-size:.57rem;color:var(--muted);font-family:var(--mono);opacity:.7}
.dz-cn{font-size:.61rem;color:var(--grn);font-weight:600;font-family:var(--mono)}
.dz input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}

/* Run button */
.btn-run{width:100%;padding:.8rem;margin-top:.9rem;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  border:none;border-radius:10px;color:#fff;font-family:var(--sans);font-size:.86rem;
  font-weight:800;letter-spacing:.06em;text-transform:uppercase;cursor:pointer;
  transition:transform .15s,box-shadow .15s}
.btn-run:hover{transform:translateY(-2px);box-shadow:0 8px 28px rgba(0,212,255,.22)}
.btn-run:disabled{opacity:.35;cursor:not-allowed;transform:none}

/* Progress */
.pw{display:none}
.status-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:.3rem}
.pb-wrap{background:var(--surf2);border-radius:100px;height:5px;overflow:hidden;margin:.5rem 0}
.pb{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));
  border-radius:100px;transition:width .5s ease;width:0%}
.sbg{display:inline-flex;align-items:center;gap:.22rem;padding:.17rem .48rem;
  border-radius:100px;font-size:.6rem;font-weight:700;font-family:var(--mono)}
.s-r{background:rgba(255,152,0,.1);color:var(--org);border:1px solid rgba(255,152,0,.3)}
.s-d{background:rgba(0,230,118,.1);color:var(--grn);border:1px solid rgba(0,230,118,.3)}
.s-e{background:rgba(255,61,87,.1);color:var(--red);border:1px solid rgba(255,61,87,.3)}
.pulse{animation:pulse 1.2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}
.logbox{background:#050810;border:1px solid var(--bdr);border-radius:7px;
  padding:.65rem;font-family:var(--mono);font-size:.67rem;height:150px;
  overflow-y:auto;color:#88ddbb;line-height:1.75}
.logbox .err{color:#ff6b8a}.logbox .warn{color:#ffb347}
.logbox .ok{color:var(--grn)}.logbox .info{color:var(--accent)}

/* Downloads */
.dw{display:none}
.dl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:.55rem;margin:.6rem 0}
.dlc{background:var(--surf2);border:1px solid var(--bdr);border-radius:8px;
  padding:.75rem;display:flex;flex-direction:column;gap:.38rem}
.dl-name{font-size:.68rem;font-weight:600;color:var(--txt)}
.dl-size{font-size:.6rem;color:var(--muted);font-family:var(--mono)}
.btn-dl{padding:.3rem .65rem;background:rgba(0,212,255,.1);
  border:1px solid rgba(0,212,255,.35);border-radius:5px;color:var(--accent);
  font-family:var(--mono);font-size:.68rem;cursor:pointer;
  text-decoration:none;display:inline-block;transition:background .15s}
.btn-dl:hover{background:rgba(0,212,255,.2)}

/* Online limit reached page */
.limit-box{background:linear-gradient(135deg,rgba(124,58,237,.12),rgba(0,212,255,.08));
  border:2px solid rgba(124,58,237,.4);border-radius:14px;
  padding:2rem 1.5rem;text-align:center;margin-top:1rem}
.limit-box .icon{font-size:2.5rem;margin-bottom:.75rem}
.limit-box h2{font-size:1.1rem;font-weight:800;color:#a78bfa;margin-bottom:.5rem}
.limit-box p{font-size:.78rem;color:var(--muted);line-height:1.7;margin-bottom:1rem}
.limit-box p strong{color:var(--txt)}
.limit-btns{display:flex;gap:.65rem;justify-content:center;flex-wrap:wrap}
.limit-btns a{padding:.6rem 1.3rem;border-radius:9px;font-family:var(--sans);
  font-size:.8rem;font-weight:700;text-decoration:none;transition:all .15s}
.lwa{background:linear-gradient(135deg,#25d366,#128c7e);color:#fff}
.lwa:hover{box-shadow:0 5px 18px rgba(37,211,102,.4);transform:translateY(-2px)}
.lemail{background:rgba(124,58,237,.2);border:1px solid rgba(124,58,237,.4);color:#a78bfa}
.lemail:hover{background:rgba(124,58,237,.35)}

/* Success CTA */
.success-cta{background:linear-gradient(135deg,rgba(0,230,118,.07),rgba(0,212,255,.05));
  border:1.5px solid rgba(0,230,118,.28);border-radius:11px;
  padding:1rem 1.1rem;text-align:center;margin-top:.85rem}
.success-cta .headline{font-size:.92rem;font-weight:800;color:var(--grn);margin-bottom:.38rem}
.success-cta .sub{font-size:.72rem;color:var(--muted);line-height:1.6;margin-bottom:.8rem}
.success-cta .sub strong{color:var(--txt)}
.cta-btns{display:flex;gap:.55rem;justify-content:center;flex-wrap:wrap}
.cta-btns a{padding:.52rem 1.1rem;border-radius:8px;font-family:var(--sans);
  font-size:.76rem;font-weight:700;text-decoration:none;transition:all .15s}
.cta-wa{background:linear-gradient(135deg,#25d366,#128c7e);color:#fff}
.cta-wa:hover{box-shadow:0 5px 16px rgba(37,211,102,.4);transform:translateY(-2px)}
.cta-email{background:rgba(124,58,237,.2);border:1px solid rgba(124,58,237,.4);color:#a78bfa}
.cta-email:hover{background:rgba(124,58,237,.35)}

/* Footer */
footer{text-align:center;padding:1.6rem 0 2rem;color:var(--muted);font-size:.67rem;font-family:var(--mono)}
footer a{color:var(--muted);text-decoration:none}
footer a:hover{color:var(--accent)}

/* ── Demo Tabs ─────────────── */
.demo-tabs{display:flex;border-radius:10px;overflow:hidden;border:1px solid var(--bdr);margin:0 auto 1.1rem;max-width:440px}
.demo-tab{flex:1;padding:.6rem 1rem;font-size:.78rem;font-weight:700;cursor:pointer;border:none;transition:all .2s;text-align:center;font-family:inherit}
.demo-tab.act-gst{background:linear-gradient(135deg,#00c9b1,#3b8beb);color:#fff}
.demo-tab.act-it{background:linear-gradient(135deg,#3b8beb,#7c3aed);color:#fff}
.demo-tab:not(.act-gst):not(.act-it){background:var(--surf);color:var(--muted)}
#panelIT{display:none}
.it-pw{display:none;background:var(--surf);border:1px solid var(--bdr);border-radius:12px;padding:1.2rem;margin-bottom:1rem}
.it-dw{display:none;background:var(--surf);border:1px solid var(--bdr);border-radius:12px;padding:1.2rem;margin-bottom:1rem}
</style>
</head>
<body>
<div class="wrap">

<!-- Try Free banner -->
<div class="try-bar">
  <h2>🌐 Try Free Online — No Install Needed</h2>
  <p>
    Upload your GST files and get an <strong>Excel reconciliation report</strong> right in your browser.<br>
    <strong>1 free reconciliation</strong> · No login · No data stored on server
  </p>
</div>

<!-- Header -->
<header>
  <div class="logo">
    <div class="logo-icon">₹</div>
    <div class="logo-text">RPR Suite</div>
  </div>
  <h1>GST <em>Reconciliation</em></h1>
  <div class="sub">GSTR-1 · 2A · 2B · 3B → Annual Excel Report</div>
  <div class="badges">
    <span class="badge bg-gold">⭐ 1 FREE ONLINE RUN</span>
    <span class="badge bg-blue">🔒 No Data Stored</span>
    <span class="badge bg-grn">📄 Excel Output</span>
  </div>
</header>

<!-- Buy bar (always visible) -->
<div class="buy-bar">
  <div class="buy-left">
    <h3>🚀 Get the Full Suite (Local EXE)</h3>
    <p>
      Unlimited clients · GSTR-1 Detail · IT Suite (AIS/TIS)<br>
      Auto-Download from Portal · Tally comparison · Bulk batch<br>
      <strong>Basic: PRICE_BASIC &nbsp;|&nbsp; Pro: PRICE_PRO</strong>
    </p>
  </div>
  <div class="buy-btns">
    <a class="btn-wa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp Us Now</a>
    <a class="btn-mail" href="mailto:CONTACT_EMAIL" target="_blank">✉ Email Us</a>
  </div>
</div>

<!-- Form Card -->
<!-- Demo Tabs -->
<div class="demo-tabs">
  <button class="demo-tab act-gst" id="tabGST" onclick="switchDemoTab('gst')">📊 GST Reconciliation</button>
  <button class="demo-tab" id="tabIT" onclick="switchDemoTab('it')">🏦 Income Tax Recon</button>
</div>

<!-- GST Panel -->
<div id="panelGST">
<div class="card" id="formCard">
  <div class="ct">Client Details</div>
  <div class="fg2" style="margin-bottom:.72rem">
    <div class="fg">
      <label>GSTIN (15 chars)</label>
      <input type="text" id="gstin" maxlength="15" placeholder="29XXXXX..." style="text-transform:uppercase">
    </div>
    <div class="fg">
      <label>Company Name</label>
      <input type="text" id="cname" placeholder="Your Company Pvt Ltd">
    </div>
  </div>
  <div class="fg" style="margin-bottom:.85rem">
    <label>Financial Year</label>
    <select id="fy">
      <option value="2025-26">2025-26</option>
      <option value="2024-25">2024-25</option>
      <option value="2023-24">2023-24</option>
      <option value="2022-23">2022-23</option>
    </select>
  </div>

  <div class="ct">Upload GST Files</div>
  <div style="font-size:.68rem;color:var(--muted);margin-bottom:.55rem;font-family:var(--mono)">
    Tip: name files with month — e.g. <code style="color:var(--accent)">GSTR1_April.zip</code>
  </div>

  <div class="dg">
    <div class="dz" id="dz_r1">
      <div class="dz-ic">📦</div>
      <div class="dz-lb">GSTR-1</div>
      <div class="dz-ht">.zip files</div>
      <div class="dz-cn" id="cnt_r1"></div>
      <input type="file" id="f_r1" multiple accept=".zip,.json">
    </div>
    <div class="dz" id="dz_r2b">
      <div class="dz-ic">📊</div>
      <div class="dz-lb">GSTR-2B</div>
      <div class="dz-ht">.xlsx files</div>
      <div class="dz-cn" id="cnt_r2b"></div>
      <input type="file" id="f_r2b" multiple accept=".xlsx,.xls">
    </div>
    <div class="dz" id="dz_r2a">
      <div class="dz-ic">📋</div>
      <div class="dz-lb">GSTR-2A</div>
      <div class="dz-ht">.xlsx / .zip</div>
      <div class="dz-cn" id="cnt_r2a"></div>
      <input type="file" id="f_r2a" multiple accept=".xlsx,.xls,.zip">
    </div>
    <div class="dz" id="dz_r3b">
      <div class="dz-ic">📄</div>
      <div class="dz-lb">GSTR-3B</div>
      <div class="dz-ht">.pdf files</div>
      <div class="dz-cn" id="cnt_r3b"></div>
      <input type="file" id="f_r3b" multiple accept=".pdf">
    </div>
    <div class="dz" id="dz_cust">
      <div class="dz-ic">👥</div>
      <div class="dz-lb">Customer Names</div>
      <div class="dz-ht">.xlsx (optional)</div>
      <div class="dz-cn" id="cnt_cust"></div>
      <input type="file" id="f_cust" accept=".xlsx,.xls">
    </div>
  </div>

  <button class="btn-run" id="runBtn" onclick="startRun()">▶ Run Free Reconciliation</button>
</div>

<!-- Progress Card -->
<div class="card pw" id="progCard">
  <div class="status-row">
    <div class="ct" style="margin:0">Processing</div>
    <span class="sbg s-r pulse" id="statusBadge">Running</span>
  </div>
  <div class="pb-wrap"><div class="pb" id="pb"></div></div>
  <div class="logbox" id="logBox"></div>
</div>

<!-- Downloads Card -->
<div class="card dw" id="dlCard">
  <div class="ct">✅ Your Results</div>
  <div class="dl-grid" id="dlGrid"></div>

  <div class="success-cta">
    <div class="headline">🎉 Done! Like the output?</div>
    <div class="sub">
      The <strong>Full Suite (local EXE)</strong> works offline on your PC —
      runs much faster, unlimited clients, GSTR-1 Detail, IT reconciliation (AIS/TIS),
      Auto-Download from GST portal, Tally comparison, and bulk batch processing.
      <br><br>
      <strong>Basic: PRICE_BASIC (20 clients) &nbsp;·&nbsp; Pro: PRICE_PRO (unlimited)</strong>
    </div>
    <div class="cta-btns">
      <a class="cta-wa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp to Buy Full Suite</a>
      <a class="cta-email" href="mailto:CONTACT_EMAIL?subject=RPR Full Suite Purchase" target="_blank">✉ Email Us</a>
    </div>
  </div>
</div>

</div><!-- /panelGST -->

<!-- ══ IT Reconciliation Panel ══ -->
<div id="panelIT">
<div class="card" id="itFormCard">
  <div class="ct">Client Details</div>
  <div class="fg2" style="margin-bottom:.72rem">
    <div class="fg">
      <label>Company Name *</label>
      <input type="text" id="itCname" placeholder="ABC Traders Pvt Ltd">
    </div>
    <div class="fg">
      <label>PAN (10 chars) *</label>
      <input type="text" id="itPan" maxlength="10" placeholder="ABCDE1234F" style="text-transform:uppercase">
    </div>
    <div class="fg">
      <label>GSTIN (optional, for cross-check)</label>
      <input type="text" id="itGstin" maxlength="15" placeholder="33ABCDE1234F1ZX" style="text-transform:uppercase">
    </div>
    <div class="fg">
      <label>Financial Year</label>
      <select id="itFy">
        <option value="2026-27" selected>2026-27</option>
        <option value="2024-25">2024-25</option>
        <option value="2023-24">2023-24</option>
        <option value="2022-23">2022-23</option>
      </select>
    </div>
    <div class="fg">
      <label>ITR Form</label>
      <select id="itItrForm">
        <option value="ITR-3">ITR-3 (Business / Profession)</option>
        <option value="ITR-6">ITR-6 (Companies)</option>
        <option value="ITR-5">ITR-5 (LLP / Firm)</option>
        <option value="ITR-4">ITR-4 (Presumptive)</option>
      </select>
    </div>
    <div class="fg">
      <label>Entity Type</label>
      <select id="itEntityType">
        <option value="company">Company / Firm</option>
        <option value="proprietorship">Proprietorship</option>
        <option value="huf">HUF</option>
        <option value="individual">Individual</option>
      </select>
    </div>
  </div>

  <div class="ct">Upload IT Portal PDFs</div>
  <div style="font-size:.72rem;color:var(--muted);margin-bottom:.6rem;font-family:var(--mono)">
    📎 26AS required · AIS + TIS improve output quality<br>
    Download from incometax.gov.in → AIS/TIS section · 26AS from TRACES
  </div>
  <div class="dg">
    <div class="dz" id="dz_it26as" onclick="document.getElementById('f_it26as').click()">
      <div class="dz-ic">📄</div>
      <div class="dz-lb">Form 26AS</div>
      <div class="dz-ht">PDF from IT Portal (required)</div>
      <div class="dz-cn" id="cnt_it26as"></div>
      <input type="file" id="f_it26as" accept=".pdf" style="display:none">
    </div>
    <div class="dz" id="dz_itais" onclick="document.getElementById('f_itais').click()">
      <div class="dz-ic">📊</div>
      <div class="dz-lb">AIS PDF</div>
      <div class="dz-ht">Annual Info Statement (optional)</div>
      <div class="dz-cn" id="cnt_itais"></div>
      <input type="file" id="f_itais" accept=".pdf" style="display:none">
    </div>
    <div class="dz" id="dz_ittis" onclick="document.getElementById('f_ittis').click()">
      <div class="dz-ic">📋</div>
      <div class="dz-lb">TIS PDF</div>
      <div class="dz-ht">Taxpayer Info Summary (optional)</div>
      <div class="dz-cn" id="cnt_ittis"></div>
      <input type="file" id="f_ittis" accept=".pdf" style="display:none">
    </div>
  </div>

  <button class="run-btn" id="itRunBtn" onclick="startITRun()" style="background:linear-gradient(135deg,#3b8beb,#7c3aed);margin-top:1rem">
    ⚡ Generate IT Reconciliation Excel
  </button>
  <div style="font-size:.65rem;color:var(--muted);text-align:center;margin-top:.4rem;font-family:var(--mono)">
    1 free IT reconciliation per session · 26AS PDF required
  </div>
</div><!-- /itFormCard -->

<!-- IT Progress -->
<div class="card it-pw" id="itProgCard">
  <div class="prog-hdr">
    <span>Processing IT Reconciliation…</span>
    <span class="sbg s-r pulse" id="itStatusBadge">Running</span>
  </div>
  <div class="prog-wrap"><div class="prog-bar" id="itPb" style="width:0%"></div></div>
  <div class="log-box" id="itLogBox"></div>
</div>

<!-- IT Downloads -->
<div class="card it-dw" id="itDlCard">
  <div class="ct">✅ IT Reconciliation Ready</div>
  <div class="dl-grid" id="itDlGrid"></div>
  <div class="success-cta" style="margin-top:1rem;padding:1.1rem;background:rgba(124,58,237,.07);border:1px solid rgba(124,58,237,.25);border-radius:12px">
    <div class="headline">🚀 Get Unlimited IT + GST Reconciliation</div>
    <div class="sub">
      Full Suite: Auto-download from IT Portal · Unlimited clients · Bulk processing · GSTR-1 Detail · Tally comparison<br>
      <strong>Basic: PRICE_BASIC &nbsp;·&nbsp; Pro: PRICE_PRO</strong>
    </div>
    <div class="cta-btns">
      <a class="cta-wa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp to Buy Full Suite</a>
      <a class="cta-email" href="mailto:CONTACT_EMAIL?subject=RPR Full Suite Purchase" target="_blank">✉ Email Us</a>
    </div>
  </div>
</div>
</div><!-- /panelIT -->

<footer>
  RPR GST + IT Online Demo &nbsp;·&nbsp; Files auto-deleted after 30 min &nbsp;·&nbsp;
  <a href="mailto:CONTACT_EMAIL">CONTACT_EMAIL</a> &nbsp;·&nbsp;
  <a href="WHATSAPP_LINK" target="_blank">WhatsApp: CONTACT_PHONE</a>
</footer>
</div>

<script>
const FILES = {};
const ZONES = ['r1','r2b','r2a','r3b','cust'];

ZONES.forEach(z => {
  const inp = document.getElementById('f_'+z);
  const dz  = document.getElementById('dz_'+z);
  const cnt = document.getElementById('cnt_'+z);
  inp.addEventListener('change', () => {
    FILES[z] = inp.files;
    const n = inp.files.length;
    cnt.textContent = n ? n+' file'+(n>1?'s':'') : '';
    dz.classList.toggle('has-files', n > 0);
  });
  dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag-over'); });
  dz.addEventListener('dragleave', () => dz.classList.remove('drag-over'));
  dz.addEventListener('drop', e => {
    e.preventDefault(); dz.classList.remove('drag-over');
    const dt = e.dataTransfer;
    if (dt.files.length) {
      FILES[z] = dt.files;
      const n = dt.files.length;
      cnt.textContent = n ? n+' file'+(n>1?'s':'') : '';
      dz.classList.add('has-files');
    }
  });
});

let jobId = null, pollTimer = null;

async function startRun() {
  const gstin = document.getElementById('gstin').value.trim().toUpperCase();
  const cname = document.getElementById('cname').value.trim();
  const fy    = document.getElementById('fy').value;

  if (!gstin || gstin.length !== 15) { alert('Please enter a valid 15-character GSTIN'); return; }
  if (!cname) { alert('Please enter the company name'); return; }

  const fd = new FormData();
  fd.append('gstin', gstin);
  fd.append('client_name', cname);
  fd.append('fy', fy);

  let hasFiles = false;
  ZONES.forEach(z => {
    if (FILES[z]) {
      Array.from(FILES[z]).forEach(f => { fd.append('files_'+z, f); hasFiles = true; });
    }
  });
  if (!hasFiles) { alert('Please upload at least one GST file'); return; }

  document.getElementById('runBtn').disabled = true;
  document.getElementById('progCard').style.display = 'block';
  document.getElementById('dlCard').style.display   = 'none';
  document.getElementById('logBox').innerHTML = '';
  document.getElementById('pb').style.width   = '0%';
  document.getElementById('statusBadge').textContent = 'Running';
  document.getElementById('statusBadge').className   = 'sbg s-r pulse';
  document.getElementById('progCard').scrollIntoView({behavior:'smooth'});

  try {
    const res  = await fetch('/api/upload', { method:'POST', body:fd });
    const data = await res.json();

    if (res.status === 429 && data.limit_reached) {
      // Show limit reached UI
      showLimitReached();
      return;
    }
    if (!res.ok || data.error) {
      alert(data.error || 'Upload failed. Please try again.');
      document.getElementById('runBtn').disabled = false;
      document.getElementById('progCard').style.display = 'none';
      return;
    }
    jobId = data.job_id;
    pollTimer = setInterval(poll, 1500);
  } catch(e) {
    alert('Connection error. Please try again.');
    document.getElementById('runBtn').disabled = false;
    document.getElementById('progCard').style.display = 'none';
  }
}

async function poll() {
  try {
    const res = await fetch('/api/job/'+jobId);
    const d   = await res.json();
    const lb  = document.getElementById('logBox');

    (d.logs||[]).forEach(l => {
      const div = document.createElement('div');
      div.className = l.type || 'info';
      div.textContent = l.msg;
      lb.appendChild(div);
    });
    lb.scrollTop = lb.scrollHeight;
    document.getElementById('pb').style.width = (d.progress||0)+'%';

    if (d.status === 'done') {
      clearInterval(pollTimer);
      document.getElementById('statusBadge').textContent = 'Done ✓';
      document.getElementById('statusBadge').className   = 'sbg s-d';
      showDownloads(d.files||[]);
    } else if (d.status === 'error') {
      clearInterval(pollTimer);
      document.getElementById('statusBadge').textContent = 'Error';
      document.getElementById('statusBadge').className   = 'sbg s-e';
    }
  } catch(e) {}
}

function showDownloads(files) {
  const grid = document.getElementById('dlGrid');
  grid.innerHTML = '';
  files.forEach(f => {
    grid.innerHTML += `<div class="dlc">
      <div class="dl-name">📊 ${f.name}</div>
      <div class="dl-size">${f.size}</div>
      <a class="btn-dl" href="/api/download/${jobId}/${encodeURIComponent(f.name)}" download="${f.name}">⬇ Download</a>
    </div>`;
  });
  document.getElementById('dlCard').style.display = 'block';
  document.getElementById('dlCard').scrollIntoView({behavior:'smooth'});
}

function showLimitReached() {
  document.getElementById('progCard').style.display = 'none';
  document.getElementById('formCard').innerHTML = `
    <div class="limit-box">
      <div class="icon">🚀</div>
      <h2>Online demo limit reached</h2>
      <p>
        You have already used your <strong>1 free online reconciliation</strong>.<br><br>
        To run unlimited reconciliations — and get GSTR-1 Detail, IT Suite,
        Auto-Download, Tally comparison and more —
        contact us for the <strong>Full Suite</strong> (local EXE, works offline, instant).
        <br><br>
        <strong>Basic: PRICE_BASIC &nbsp;·&nbsp; Pro: PRICE_PRO</strong>
      </p>
      <div class="limit-btns">
        <a class="lwa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp to Buy Full Suite</a>
        <a class="lemail" href="mailto:CONTACT_EMAIL?subject=RPR Full Suite Purchase" target="_blank">✉ Email Us</a>
      </div>
    </div>`;
}


// ── IT Reconciliation Tab ─────────────────────────────────────────
const IT_FILES = {};
['it26as','itais','ittis'].forEach(z => {
  const inp = document.getElementById('f_'+z);
  const dz  = document.getElementById('dz_'+z);
  const cnt = document.getElementById('cnt_'+z);
  if(!inp) return;
  inp.addEventListener('change', () => {
    IT_FILES[z] = inp.files;
    const n = inp.files.length;
    cnt.textContent = n ? n+' file'+(n>1?'s':'') : '';
    dz.classList.toggle('has-files', n > 0);
  });
  dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag-over'); });
  dz.addEventListener('dragleave', () => dz.classList.remove('drag-over'));
  dz.addEventListener('drop', e => {
    e.preventDefault(); dz.classList.remove('drag-over');
    const dt = e.dataTransfer;
    if(dt.files.length){
      IT_FILES[z] = dt.files;
      const n = dt.files.length;
      cnt.textContent = n ? n+' file'+(n>1?'s':'') : '';
      dz.classList.add('has-files');
    }
  });
});

let itJobId = null, itPollTimer = null;

function switchDemoTab(tab) {
  const panelGST = document.getElementById('panelGST');
  const panelIT  = document.getElementById('panelIT');
  const tabGST   = document.getElementById('tabGST');
  const tabIT    = document.getElementById('tabIT');
  if(tab === 'gst'){
    panelGST.style.display = 'block'; panelIT.style.display = 'none';
    tabGST.className = 'demo-tab act-gst'; tabIT.className = 'demo-tab';
  } else {
    panelGST.style.display = 'none';  panelIT.style.display = 'block';
    tabGST.className = 'demo-tab';    tabIT.className = 'demo-tab act-it';
  }
}

async function startITRun() {
  const cname      = document.getElementById('itCname').value.trim();
  const pan        = document.getElementById('itPan').value.trim().toUpperCase();
  const gstin      = document.getElementById('itGstin').value.trim().toUpperCase();
  const fy         = document.getElementById('itFy').value;
  const itrForm    = document.getElementById('itItrForm').value;
  const entityType = document.getElementById('itEntityType').value;

  if(!cname){ alert('Please enter company name'); return; }
  if(!pan || pan.length !== 10){ alert('PAN must be exactly 10 characters (e.g. ABCDE1234F)'); return; }
  if(!(IT_FILES['it26as'] && IT_FILES['it26as'].length > 0)){
    alert('Please upload the 26AS PDF (required)'); return;
  }

  const fd = new FormData();
  fd.append('company_name', cname);
  fd.append('pan', pan);
  fd.append('gstin', gstin);
  fd.append('fy', fy);
  fd.append('itr_form', itrForm);
  fd.append('entity_type', entityType);
  ['it26as','itais','ittis'].forEach(z => {
    if(IT_FILES[z]) Array.from(IT_FILES[z]).forEach(f => fd.append('files_'+z, f));
  });

  document.getElementById('itRunBtn').disabled = true;
  document.getElementById('itProgCard').style.display = 'block';
  document.getElementById('itDlCard').style.display   = 'none';
  document.getElementById('itLogBox').innerHTML = '';
  document.getElementById('itPb').style.width   = '0%';
  document.getElementById('itStatusBadge').textContent = 'Running';
  document.getElementById('itStatusBadge').className   = 'sbg s-r pulse';
  document.getElementById('itProgCard').scrollIntoView({behavior:'smooth'});

  try {
    const res  = await fetch('/api/it-upload', {method:'POST', body:fd});
    const data = await res.json();
    if(res.status === 429 && data.limit_reached){
      document.getElementById('itProgCard').style.display = 'none';
      document.getElementById('itFormCard').innerHTML = `
        <div class="limit-box">
          <div class="icon">🚀</div>
          <h2>IT demo limit reached</h2>
          <p>You've used your <strong>1 free IT reconciliation</strong>.<br><br>
          Contact us for the <strong>Full Suite</strong> — unlimited clients, auto-download from IT Portal, bulk processing.</p>
          <div class="limit-btns">
            <a class="lwa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp to Buy Full Suite</a>
            <a class="lemail" href="mailto:CONTACT_EMAIL?subject=RPR Full Suite Purchase" target="_blank">✉ Email Us</a>
          </div>
        </div>`;
      return;
    }
    if(!res.ok || data.error){ alert(data.error || 'Upload failed'); document.getElementById('itRunBtn').disabled=false; return; }
    itJobId = data.job_id;
    itPollTimer = setInterval(pollIT, 1500);
  } catch(e){
    alert('Connection error. Please try again.');
    document.getElementById('itRunBtn').disabled = false;
    document.getElementById('itProgCard').style.display = 'none';
  }
}

async function pollIT() {
  try {
    const res = await fetch('/api/it-job/'+itJobId);
    const d   = await res.json();
    const lb  = document.getElementById('itLogBox');
    (d.logs||[]).forEach(l => {
      const div = document.createElement('div');
      div.className = l.type || 'info';
      div.textContent = l.msg;
      lb.appendChild(div);
    });
    lb.scrollTop = lb.scrollHeight;
    document.getElementById('itPb').style.width = (d.progress||0)+'%';
    if(d.status === 'done'){
      clearInterval(itPollTimer);
      document.getElementById('itStatusBadge').textContent = 'Done ✓';
      document.getElementById('itStatusBadge').className   = 'sbg s-d';
      showITDownloads(d.files||[]);
    } else if(d.status === 'error'){
      clearInterval(itPollTimer);
      document.getElementById('itStatusBadge').textContent = 'Error';
      document.getElementById('itStatusBadge').className   = 'sbg s-e';
    }
  } catch(e){}
}

function showITDownloads(files) {
  const grid = document.getElementById('itDlGrid');
  grid.innerHTML = '';
  files.forEach(f => {
    grid.innerHTML += `<div class="dlc">
      <div class="dl-name">🏦 ${f.name}</div>
      <div class="dl-size">${f.size}</div>
      <a class="btn-dl" href="/api/it-dl/${itJobId}/${encodeURIComponent(f.name)}" download="${f.name}">⬇ Download</a>
    </div>`;
  });
  document.getElementById('itDlCard').style.display = 'block';
  document.getElementById('itDlCard').scrollIntoView({behavior:'smooth'});
}
</script>
</body>
</html>
"""

# ── Inject contact details ─────────────────────────────────────────
WA_LINK = f"https://wa.me/{DEMO_CONTACT_WHATSAPP}?text=Hi%2C+I+tried+the+RPR+GST+online+demo+and+want+the+Full+Suite"
HTML = HTML.replace("PRICE_BASIC",    DEMO_PRICE_BASIC)
HTML = HTML.replace("PRICE_PRO",      DEMO_PRICE_PRO)
HTML = HTML.replace("CONTACT_EMAIL",  DEMO_CONTACT_EMAIL)
HTML = HTML.replace("CONTACT_PHONE",  DEMO_CONTACT_PHONE)
HTML = HTML.replace("WHATSAPP_LINK",  WA_LINK)

# ── Routes ─────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/upload", methods=["POST"])
@rate_limit(limit=5, window=60)
def api_upload():
    ip = _get_ip()
    if not _check_ip_limit(ip):
        return jsonify(
            error=f"Online demo limit reached (1 free run). Contact us for the Full Suite: {DEMO_CONTACT_PHONE}",
            limit_reached=True
        ), 429

    _cleanup_old_jobs()
    gstin       = request.form.get("gstin","").strip().upper()
    client_name = request.form.get("client_name","").strip()
    fy          = request.form.get("fy","2026-27").strip() or "2026-27"

    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN — must be exactly 15 characters"), 400
    if not client_name:
        return jsonify(error="Company name is required"), 400

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
            safe = secure_filename(fobj.filename) or f"upload_{zone}_{uuid.uuid4().hex[:6]}"
            if Path(safe).suffix.lower() not in ALLOWED_EXT: continue
            dest = job_dir / safe
            fobj.save(str(dest))
            saved[zone].append(str(dest))

    with jobs_lock:
        jobs[job_id] = {
            "status":"queued","progress":0,"logs":[],"files":[],
            "error":None,"gstin":gstin,"client_name":client_name,
            "fy":fy,"job_dir":str(job_dir),"out_dir":str(out_dir),"saved":saved,
        }

    threading.Thread(target=run_reconciliation, args=(job_id,), daemon=True).start()
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
        )

@app.route("/api/download/<job_id>/<filename>")
@rate_limit(limit=20, window=60)
def api_download(job_id, filename):
    if not re.match(r'^[\w\-. ()]+\.(xlsx|pdf|zip)$', filename):
        abort(400)
    fpath = OUTPUT_DIR / job_id / filename
    if not fpath.exists() or not fpath.is_file():
        abort(404)
    return send_file(str(fpath), as_attachment=True, download_name=filename)

# ── Reconciliation Engine ──────────────────────────────────────────
def run_reconciliation(job_id):
    def log(msg, t="info"):
        with jobs_lock: jobs[job_id]["logs"].append({"type":t,"msg":msg})
    def prog(p):
        with jobs_lock: jobs[job_id]["progress"] = p
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

        for fpath in saved.get("r1", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except Exception: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1: {mon} {yr}")
            else:
                log(f"  ⚠ Month not detected: {Path(fpath).name}", "warn")

        for fpath in saved.get("r2b", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR2B_{mon}_{yr}.xlsx"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except Exception: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2B: {mon} {yr}")

        for fpath in saved.get("r2a", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                ext  = Path(fpath).suffix.lower()
                dest = job_dir / f"GSTR2A_{mon}_{yr}{ext}"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except Exception: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2A: {mon} {yr}")

        for fpath in saved.get("r3b", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR3B_{mon}_{yr}.pdf"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except Exception: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-3B: {mon} {yr}")

        for fpath in saved.get("cust", []):
            dest = job_dir / "customer_names.xlsx"
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except Exception: shutil.copy2(fpath, str(dest))
            log("  Customer names loaded"); break

        prog(25)

        suite_path = _find_engine("gst_suite_final.py")
        if not suite_path:
            raise FileNotFoundError(
                "GST engine not found. "
                "Deploy gst_suite_v32.py alongside online_demo.py on Render."
            )

        log("Loading reconciliation engine...")
        import importlib.util as _ilu, logging as _lg
        spec = _ilu.spec_from_file_location("gst_suite", str(suite_path))
        gst  = _ilu.module_from_spec(spec)
        spec.loader.exec_module(gst)

        s = int(fy.split("-")[0]); e = s + 1
        gst.FY_LABEL = fy
        gst.MONTHS   = [
            ("April","04",str(s)),("May","05",str(s)),("June","06",str(s)),
            ("July","07",str(s)),("August","08",str(s)),("September","09",str(s)),
            ("October","10",str(s)),("November","11",str(s)),("December","12",str(s)),
            ("January","01",str(e)),("February","02",str(e)),("March","03",str(e)),
        ]

        _log = _lg.getLogger(f"gst_{job_id}")
        _log.setLevel(_lg.DEBUG)
        class _WL(_lg.Handler):
            def emit(self, r):
                log(self.format(r), "err" if r.levelno >= _lg.WARNING else "info")
        _log.addHandler(_WL())

        prog(30)
        log("Running annual reconciliation...")
        try:
            gst.write_annual_reconciliation(str(job_dir), client_name, gstin, _log, with_formulas=True)
        except TypeError:
            gst.write_annual_reconciliation(str(job_dir), client_name, gstin, _log)

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
            raise RuntimeError("No Excel output generated. Check that the GST engine is present.")

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready to download.", "ok")
        log(f"─── Want the Full Suite? WhatsApp {DEMO_CONTACT_PHONE} ───", "info")

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


# ═══════════════════════════════════════════════════════════════════
# INCOME TAX RECONCILIATION — Online Demo Routes
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
        itr_form     = job.get("itr_form", "ITR-3")
        entity_type  = job.get("entity_type", "company")
        job_dir      = Path(job["job_dir"])
        out_dir      = Path(job["out_dir"])
        saved        = job["saved"]

        log(f"Starting IT Reconciliation: {company_name} ({pan}) FY {fy} | {itr_form} | {entity_type}")
        prog(5)

        import shutil as _shutil
        pdf_found = {}
        for zone, dest_prefix in [("it26as","26AS"), ("itais","AIS"), ("ittis","TIS")]:
            for fpath in saved.get(zone, []):
                ext  = Path(fpath).suffix.lower()
                dest = job_dir / f"{dest_prefix}{ext}"
                if not dest.exists():
                    try:    Path(fpath).rename(dest)
                    except Exception: _shutil.copy2(fpath, str(dest))
                log(f"  ✓ {dest_prefix}: {dest.name}")
                pdf_found[zone] = dest.name

        if "it26as" not in pdf_found:
            log("  ⚠ 26AS PDF not uploaded — results will be limited", "warn")
        prog(20)

        engine_path = _find_engine("it_recon_engine.py")
        if not engine_path:
            raise FileNotFoundError(
                "it_recon_engine.py not found. "
                "Deploy it_recon_engine.py alongside online_demo.py on Render.")

        log("Loading IT reconciliation engine...")
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location("it_recon", str(engine_path))
        it   = _ilu.module_from_spec(spec)
        spec.loader.exec_module(it)
        prog(30)

        log(f"Parsing PDFs and generating IT Reconciliation for {itr_form}...")
        call_kwargs = {"log": log, "itr_form": itr_form, "entity_type": entity_type}
        try:
            out_xl = it.write_it_reconciliation(str(job_dir), company_name, pan, gstin, fy, **call_kwargs)
        except TypeError:
            try:
                out_xl = it.write_it_reconciliation(str(job_dir), company_name, pan, gstin, fy, log=log)
            except TypeError:
                out_xl = it.write_it_reconciliation(str(job_dir), company_name, pan, gstin, fy)
        prog(85)

        output_files = []
        seen = set()
        for search_dir in [job_dir, out_dir]:
            for fp in sorted(search_dir.glob("IT_RECONCILIATION_*.xlsx")):
                if fp.name in seen: continue
                seen.add(fp.name)
                dest_fp = out_dir / fp.name
                if fp.parent != out_dir:
                    _shutil.copy2(str(fp), str(dest_fp))
                sz = dest_fp.stat().st_size // 1024
                output_files.append({"name": fp.name, "size": f"{sz} KB"})
                log(f"  ✓ {fp.name} ({sz} KB)", "ok")

        if not output_files:
            raise RuntimeError("No IT Reconciliation Excel generated. Check that 26AS PDF was uploaded correctly.")

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready to download.", "ok")
        log(f"─── Want unlimited clients + auto-download? WhatsApp {DEMO_CONTACT_PHONE} ───", "info")

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
@rate_limit(limit=5, window=60)
def api_it_upload():
    ip = _get_ip()
    if not _check_ip_limit_it(ip):
        return jsonify(
            error=f"IT demo limit reached (1 free run). Contact us for the Full Suite: {DEMO_CONTACT_PHONE}",
            limit_reached=True
        ), 429

    _cleanup_old_jobs()
    company_name = request.form.get("company_name", "").strip()
    pan          = request.form.get("pan", "").strip().upper()
    gstin        = request.form.get("gstin", "").strip().upper()
    fy           = request.form.get("fy", "2025-26").strip() or "2025-26"
    itr_form     = request.form.get("itr_form", "ITR-3").strip()
    entity_type  = request.form.get("entity_type", "company").strip()

    if not company_name:
        return jsonify(error="Company name is required"), 400
    if not pan or len(pan) != 10:
        return jsonify(error="PAN must be 10 characters (e.g. ABCDE1234F)"), 400

    job_id  = str(uuid.uuid4())[:8]
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    saved = {k: [] for k in ("it26as", "itais", "ittis")}
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
            "itr_form":     itr_form,
            "entity_type":  entity_type,
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
            error    = job.get("error"),
        )


@app.route("/api/it-dl/<job_id>/<filename>")
@rate_limit(limit=20, window=60)
def api_it_dl(job_id, filename):
    filename = Path(filename).name
    if not re.match(r'^[\w\-. ()]+\.(xlsx|pdf|zip)$', filename):
        abort(400)
    fp = OUTPUT_DIR / job_id / filename
    if not fp.exists() or not fp.is_file():
        abort(404)
    return send_file(str(fp), as_attachment=True, download_name=filename)


# ── Startup (Render.com) ───────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print()
    print("  ============================================================")
    print("   RPR GST Reconciliation — ONLINE DEMO (Render.com)")
    print(f"   Port: {port}")
    suite = _find_engine("gst_suite_final.py")
    print(f"   GST Engine: {'✅ ' + suite.name if suite else '❌ NOT FOUND — deploy gst_suite_v32.py'}")
    print("  ============================================================")
    print()
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
