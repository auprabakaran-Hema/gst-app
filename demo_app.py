"""
RPR GST Reconciliation — FREE DEMO (Local EXE)
===============================================
• Runs locally on your PC — fast, no network, no limits
• Reconciliation only (Full Suite has GSTR-1 Detail, IT, Auto-Download, Bulk)
• Unlimited runs — customers can test as many clients as they want
• Strong "Buy Full Suite" CTA after every result
• Build as EXE using BUILD_DEMO_EXE.bat
"""

import os, sys, json, zipfile, re, time, shutil, uuid, threading
from pathlib import Path
from datetime import datetime
from flask import Flask, request, jsonify, send_file, render_template_string, abort
import tempfile, platform, webbrowser

# ── Contact / Branding ─────────────────────────────────────────────
DEMO_CONTACT_PHONE    = "7845998125"
DEMO_CONTACT_EMAIL    = "auprabakaran@gmail.com"
DEMO_CONTACT_WHATSAPP = "917845998125"
DEMO_PRICE_BASIC      = "₹2,500/year"
DEMO_PRICE_PRO        = "₹6,500/year"

# ── Directories ────────────────────────────────────────────────────
def _get_app_dir(subfolder):
    if platform.system() == "Windows":
        base = Path(os.path.expanduser("~")) / "Downloads" / "GST_Demo"
    else:
        base = Path(tempfile.gettempdir()) / "gst_demo"
    d = base / subfolder
    d.mkdir(parents=True, exist_ok=True)
    return d

UPLOAD_DIR  = _get_app_dir("uploads")
OUTPUT_DIR  = _get_app_dir("outputs")
ALLOWED_EXT = {".zip", ".xlsx", ".xls", ".pdf", ".json"}
MAX_FILE_MB = 50
JOB_TTL_S   = 3600

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024

jobs      = {}
jobs_lock = threading.Lock()

# ── Rate limiting ──────────────────────────────────────────────────
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

# ── Helpers ────────────────────────────────────────────────────────
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
    _ALIASES = {
        "gst_suite_final.py": ["gst_suite_v32.py", "gst_suite_v31.py", "gst_suite_final.py"],
        "gstr1_extract.py":   ["gstr1_fy_v5.py", "gstr1_extract.py"],
    }
    candidates = _ALIASES.get(name, [name])
    search_dirs = [
        Path(__file__).parent,
        Path(os.getcwd()),
        Path(sys._MEIPASS) if hasattr(sys, "_MEIPASS") else Path("."),
        Path(os.path.expanduser("~")) / "Desktop",
        Path(os.path.expanduser("~")) / "Downloads",
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
    except: pass
    return None, None

@app.before_request
def block_scripts():
    p = request.path.lower()
    if p.endswith((".py", ".pyc")):
        abort(403)

# ══════════════════════════════════════════════════════════════════
# HTML — Local Demo UI (clean, fast, strong buy CTA)
# ══════════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RPR GST Reconciliation — Free Demo</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#07090f;--surf:#0f1420;--surf2:#161d2e;--bdr:#1c2a40;
  --accent:#00d4ff;--accent2:#7c3aed;--grn:#00e676;--org:#ff9800;
  --red:#ff3d57;--txt:#dce8f5;--muted:#5a6e8a;--gold:#ffc107;
  --mono:'IBM Plex Mono',monospace;--sans:'Syne',sans-serif;
}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;
  background:radial-gradient(ellipse 80% 50% at 50% -10%,rgba(0,212,255,.07),transparent);
  pointer-events:none;z-index:0}
.wrap{max-width:820px;margin:0 auto;padding:1.5rem 1.2rem;position:relative;z-index:1}

/* Header */
header{text-align:center;padding:1.8rem 0 1rem}
.logo{display:inline-flex;align-items:center;gap:.7rem;margin-bottom:.8rem}
.logo-icon{width:48px;height:48px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:12px;display:flex;align-items:center;justify-content:center;
  font-size:1.5rem;box-shadow:0 4px 20px rgba(0,212,255,.3)}
.logo-text{font-size:1rem;font-weight:800;letter-spacing:.12em;text-transform:uppercase;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
h1{font-size:clamp(1.5rem,3.5vw,2.2rem);font-weight:800;letter-spacing:-.02em;margin-bottom:.35rem}
h1 em{font-style:normal;background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
.sub{color:var(--muted);font-size:.78rem;font-family:var(--mono)}
.badges{display:flex;gap:.5rem;justify-content:center;flex-wrap:wrap;margin-top:.7rem}
.badge{display:inline-flex;align-items:center;gap:.3rem;padding:.22rem .65rem;
  border-radius:100px;font-size:.67rem;font-weight:700;font-family:var(--mono)}
.bg-grn{background:rgba(0,230,118,.12);color:var(--grn);border:1px solid rgba(0,230,118,.35)}
.bg-gold{background:rgba(255,193,7,.1);color:var(--gold);border:1px solid rgba(255,193,7,.35)}
.bg-blue{background:rgba(0,212,255,.08);color:var(--accent);border:1px solid rgba(0,212,255,.28)}

/* Demo notice bar */
.notice{background:linear-gradient(90deg,rgba(0,212,255,.06),rgba(124,58,237,.06));
  border:1px solid rgba(0,212,255,.2);border-radius:10px;
  padding:.75rem 1rem;margin-bottom:1rem;display:flex;align-items:flex-start;gap:.75rem;
  font-size:.75rem;color:var(--muted);line-height:1.6}
.notice-icon{font-size:1.1rem;line-height:1;flex-shrink:0;margin-top:.05rem}
.notice strong{color:var(--txt)}

/* Buy bar — always visible */
.buy-bar{background:linear-gradient(135deg,rgba(124,58,237,.15),rgba(0,212,255,.08));
  border:1.5px solid rgba(124,58,237,.45);border-radius:13px;
  padding:1rem 1.2rem;margin-bottom:1.2rem;
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:.8rem}
.buy-left h3{font-size:.88rem;font-weight:800;color:#a78bfa;margin-bottom:.3rem}
.buy-left p{font-size:.72rem;color:var(--muted);line-height:1.6}
.buy-left p strong{color:var(--txt)}
.buy-btns{display:flex;flex-direction:column;gap:.4rem;min-width:170px}
.btn-wa{padding:.55rem 1rem;background:linear-gradient(135deg,#25d366,#128c7e);
  border:none;border-radius:8px;color:#fff;font-family:var(--sans);font-size:.76rem;
  font-weight:700;cursor:pointer;text-align:center;text-decoration:none;display:block;
  transition:transform .15s,box-shadow .15s;letter-spacing:.04em}
.btn-wa:hover{transform:translateY(-2px);box-shadow:0 5px 16px rgba(37,211,102,.4)}
.btn-mail{padding:.55rem 1rem;background:rgba(124,58,237,.25);
  border:1px solid rgba(124,58,237,.5);border-radius:8px;color:#a78bfa;
  font-family:var(--sans);font-size:.76rem;font-weight:700;cursor:pointer;
  text-align:center;text-decoration:none;display:block;transition:all .15s}
.btn-mail:hover{background:rgba(124,58,237,.4)}

/* Card */
.card{background:var(--surf);border:1px solid var(--bdr);border-radius:13px;
  padding:1.3rem;margin-bottom:1rem}
.ct{font-size:.72rem;font-weight:700;text-transform:uppercase;letter-spacing:.09em;
  color:var(--accent);margin-bottom:.85rem;display:flex;align-items:center;gap:.4rem}
.ct::before{content:'';width:3px;height:.95em;background:var(--accent);border-radius:2px}

/* Form */
.fg2{display:grid;grid-template-columns:1fr 1fr;gap:.7rem}
@media(max-width:520px){.fg2{grid-template-columns:1fr}}
.fg{display:flex;flex-direction:column;gap:.28rem}
label{font-size:.64rem;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--muted)}
input[type=text],select{
  background:var(--surf2);border:1px solid var(--bdr);border-radius:7px;
  padding:.5rem .75rem;color:var(--txt);font-family:var(--mono);font-size:.8rem;
  transition:border-color .2s;width:100%}
input:focus,select:focus{outline:none;border-color:var(--accent);box-shadow:0 0 0 2px rgba(0,212,255,.08)}
select option{background:var(--surf)}

/* Dropzones */
.dg{display:grid;grid-template-columns:repeat(auto-fill,minmax(148px,1fr));gap:.6rem;margin-top:.5rem}
.dz{background:var(--surf2);border:2px dashed var(--bdr);border-radius:10px;
  padding:.9rem .6rem;text-align:center;cursor:pointer;transition:all .2s;
  position:relative;min-height:92px;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:.3rem}
.dz:hover,.dz.drag-over{border-color:var(--accent);background:rgba(0,212,255,.04)}
.dz.has-files{border-color:var(--grn);border-style:solid;background:rgba(0,230,118,.04)}
.dz-ic{font-size:1.5rem;line-height:1}
.dz-lb{font-size:.61rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.dz-ht{font-size:.58rem;color:var(--muted);font-family:var(--mono);opacity:.7}
.dz-cn{font-size:.63rem;color:var(--grn);font-weight:600;font-family:var(--mono)}
.dz input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}

/* Run button */
.btn-run{width:100%;padding:.82rem;margin-top:.9rem;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  border:none;border-radius:10px;color:#fff;font-family:var(--sans);font-size:.88rem;
  font-weight:800;letter-spacing:.06em;text-transform:uppercase;cursor:pointer;
  transition:transform .15s,box-shadow .15s}
.btn-run:hover{transform:translateY(-2px);box-shadow:0 8px 28px rgba(0,212,255,.25)}
.btn-run:disabled{opacity:.35;cursor:not-allowed;transform:none}

/* Progress */
.pw{display:none}
.pb-wrap{background:var(--surf2);border-radius:100px;height:5px;overflow:hidden;margin:.5rem 0}
.pb{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));
  border-radius:100px;transition:width .5s ease;width:0%}
.status-row{display:flex;align-items:center;justify-content:space-between;margin-bottom:.3rem}
.sbg{display:inline-flex;align-items:center;gap:.25rem;padding:.18rem .5rem;
  border-radius:100px;font-size:.62rem;font-weight:700;font-family:var(--mono)}
.s-r{background:rgba(255,152,0,.12);color:var(--org);border:1px solid rgba(255,152,0,.35)}
.s-d{background:rgba(0,230,118,.12);color:var(--grn);border:1px solid rgba(0,230,118,.35)}
.s-e{background:rgba(255,61,87,.12);color:var(--red);border:1px solid rgba(255,61,87,.35)}
.pulse{animation:pulse 1.2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.35}}
.logbox{background:#060a10;border:1px solid var(--bdr);border-radius:8px;
  padding:.7rem;font-family:var(--mono);font-size:.68rem;height:160px;
  overflow-y:auto;color:#88ddbb;line-height:1.75}
.logbox .err{color:#ff6b8a}.logbox .warn{color:#ffb347}
.logbox .ok{color:var(--grn)}.logbox .info{color:var(--accent)}

/* Downloads */
.dw{display:none}
.dl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(175px,1fr));gap:.6rem;margin:.6rem 0}
.dlc{background:var(--surf2);border:1px solid var(--bdr);border-radius:9px;
  padding:.8rem;display:flex;flex-direction:column;gap:.4rem}
.dl-name{font-size:.7rem;font-weight:600;color:var(--txt)}
.dl-size{font-size:.62rem;color:var(--muted);font-family:var(--mono)}
.btn-dl{padding:.32rem .7rem;background:rgba(0,212,255,.1);
  border:1px solid rgba(0,212,255,.4);border-radius:5px;color:var(--accent);
  font-family:var(--mono);font-size:.7rem;cursor:pointer;
  text-decoration:none;display:inline-block;transition:background .15s}
.btn-dl:hover{background:rgba(0,212,255,.2)}

/* Success CTA */
.success-cta{background:linear-gradient(135deg,rgba(0,230,118,.07),rgba(0,212,255,.05));
  border:1.5px solid rgba(0,230,118,.3);border-radius:11px;
  padding:1.1rem 1.2rem;text-align:center;margin-top:.9rem}
.success-cta .headline{font-size:.95rem;font-weight:800;color:var(--grn);margin-bottom:.4rem}
.success-cta .sub{font-size:.74rem;color:var(--muted);line-height:1.6;margin-bottom:.85rem}
.success-cta .sub strong{color:var(--txt)}
.cta-btns{display:flex;gap:.6rem;justify-content:center;flex-wrap:wrap}
.cta-btns a{padding:.55rem 1.2rem;border-radius:8px;font-family:var(--sans);
  font-size:.78rem;font-weight:700;text-decoration:none;transition:all .15s;letter-spacing:.04em}
.cta-wa{background:linear-gradient(135deg,#25d366,#128c7e);color:#fff}
.cta-wa:hover{box-shadow:0 5px 18px rgba(37,211,102,.4);transform:translateY(-2px)}
.cta-email{background:rgba(124,58,237,.25);border:1px solid rgba(124,58,237,.45);color:#a78bfa}
.cta-email:hover{background:rgba(124,58,237,.4)}

.btn-reset{width:100%;margin-top:.7rem;padding:.6rem;
  background:transparent;border:1px solid var(--bdr);border-radius:8px;
  color:var(--muted);font-family:var(--sans);font-size:.75rem;
  cursor:pointer;transition:border-color .15s}
.btn-reset:hover{border-color:var(--accent)}

/* Footer */
footer{text-align:center;padding:1.8rem 0 2rem;color:var(--muted);font-size:.68rem;font-family:var(--mono)}
footer a{color:var(--muted);text-decoration:none}
footer a:hover{color:var(--accent)}
</style>
</head>
<body>
<div class="wrap">

<!-- Header -->
<header>
  <div class="logo">
    <div class="logo-icon">₹</div>
    <div class="logo-text">RPR Suite</div>
  </div>
  <h1>GST <em>Reconciliation</em> — Free Demo</h1>
  <div class="sub">Upload files → get Excel output — 100% on your PC, no internet needed</div>
  <div class="badges">
    <span class="badge bg-gold">⭐ FREE DEMO</span>
    <span class="badge bg-grn">✓ Unlimited Runs</span>
    <span class="badge bg-blue">💻 Works Offline</span>
    <span class="badge bg-grn">📄 Excel Output</span>
  </div>
</header>

<!-- Demo notice -->
<div class="notice">
  <span class="notice-icon">ℹ️</span>
  <div>
    This demo includes <strong>GST Reconciliation only</strong> — upload GSTR-1, 2B, 2A, 3B and get an annual Excel report.
    The <strong>Full Suite</strong> adds GSTR-1 Detail extraction, IT reconciliation (AIS/TIS), Auto-Download from portal,
    Bulk multi-client processing, Tally comparison, and more.
    All files stay on your PC — <strong>nothing is sent anywhere</strong>.
  </div>
</div>

<!-- Buy Bar (always visible) -->
<div class="buy-bar">
  <div class="buy-left">
    <h3>🚀 Upgrade to Full Suite</h3>
    <p>
      GSTR-1 Detail · IT Suite (AIS/TIS) · Auto-Download from Portal<br>
      Bulk multi-client · Tally comparison · No run limits<br>
      <strong>Basic: PRICE_BASIC &nbsp;|&nbsp; Pro: PRICE_PRO</strong>
    </p>
  </div>
  <div class="buy-btns">
    <a class="btn-wa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp Us Now</a>
    <a class="btn-mail" href="mailto:CONTACT_EMAIL" target="_blank">✉ Email Us</a>
  </div>
</div>

<!-- Form Card -->
<div class="card" id="formCard">
  <div class="ct">Client Details</div>
  <div class="fg2" style="margin-bottom:.75rem">
    <div class="fg">
      <label>GSTIN (15 chars)</label>
      <input type="text" id="gstin" maxlength="15" placeholder="29XXXXX..." style="text-transform:uppercase">
    </div>
    <div class="fg">
      <label>Company Name</label>
      <input type="text" id="cname" placeholder="Your Company Pvt Ltd">
    </div>
  </div>
  <div class="fg" style="margin-bottom:.9rem">
    <label>Financial Year</label>
    <select id="fy">
      <option value="2025-26">2025-26</option>
      <option value="2024-25">2024-25</option>
      <option value="2023-24">2023-24</option>
      <option value="2022-23">2022-23</option>
    </select>
  </div>

  <div class="ct">Upload GST Files</div>
  <div style="font-size:.7rem;color:var(--muted);margin-bottom:.6rem;font-family:var(--mono)">
    Tip: rename files with month name — e.g. <code style="color:var(--accent)">GSTR1_April.zip</code> — for auto-detection
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
    <div class="dz" id="dz_r1a">
      <div class="dz-ic">📝</div>
      <div class="dz-lb">GSTR-1A</div>
      <div class="dz-ht">.zip (optional)</div>
      <div class="dz-cn" id="cnt_r1a"></div>
      <input type="file" id="f_r1a" multiple accept=".zip">
    </div>
    <div class="dz" id="dz_cust">
      <div class="dz-ic">👥</div>
      <div class="dz-lb">Customer Names</div>
      <div class="dz-ht">.xlsx (optional)</div>
      <div class="dz-cn" id="cnt_cust"></div>
      <input type="file" id="f_cust" accept=".xlsx,.xls">
    </div>
  </div>

  <button class="btn-run" id="runBtn" onclick="startRun()">▶ Run Reconciliation</button>
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
  <div class="ct">✅ Results Ready</div>
  <div class="dl-grid" id="dlGrid"></div>

  <!-- Strong Buy CTA after result -->
  <div class="success-cta">
    <div class="headline">🎉 Reconciliation Complete!</div>
    <div class="sub">
      Liked the output? The <strong>Full Suite</strong> includes GSTR-1 Detail extraction,
      IT reconciliation (AIS/TIS comparison), Auto-Download directly from the GST portal,
      and bulk processing for unlimited clients — all on your PC, fully offline.
      <br><br>
      <strong>Basic: PRICE_BASIC &nbsp;·&nbsp; Pro: PRICE_PRO</strong>
    </div>
    <div class="cta-btns">
      <a class="cta-wa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp to Buy Now</a>
      <a class="cta-email" href="mailto:CONTACT_EMAIL?subject=Full Suite Purchase&body=Hi, I tried the RPR GST Demo and want to buy the Full Suite." target="_blank">✉ Email Us</a>
    </div>
  </div>

  <button class="btn-reset" onclick="resetForm()">↩ Run Another Client</button>
</div>

<footer>
  RPR GST Demo — All processing is local on your PC &nbsp;·&nbsp;
  <a href="mailto:CONTACT_EMAIL">CONTACT_EMAIL</a> &nbsp;·&nbsp;
  <a href="WHATSAPP_LINK" target="_blank">WhatsApp: CONTACT_PHONE</a>
</footer>
</div>

<script>
const FILES = {};
const ZONES = ['r1','r2b','r2a','r3b','r1a','cust'];

// File / drag-drop handlers
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
    if (!res.ok || data.error) {
      alert(data.error || 'Upload failed. Please try again.');
      document.getElementById('runBtn').disabled = false;
      return;
    }
    jobId = data.job_id;
    pollTimer = setInterval(poll, 1200);
  } catch(e) {
    alert('Could not connect to local server. Try restarting the application.');
    document.getElementById('runBtn').disabled = false;
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
      document.getElementById('runBtn').disabled = false;
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

function resetForm() {
  // Allow running another client — no restart needed
  document.getElementById('runBtn').disabled = false;
  document.getElementById('progCard').style.display = 'none';
  document.getElementById('dlCard').style.display   = 'none';
  document.getElementById('gstin').value = '';
  document.getElementById('cname').value = '';
  ZONES.forEach(z => {
    FILES[z] = null;
    document.getElementById('cnt_'+z).textContent = '';
    document.getElementById('dz_'+z).classList.remove('has-files');
  });
  window.scrollTo({top:0,behavior:'smooth'});
}
</script>
</body>
</html>
"""

# ── Inject contact details ─────────────────────────────────────────
HTML = HTML.replace("PRICE_BASIC",    DEMO_PRICE_BASIC)
HTML = HTML.replace("PRICE_PRO",      DEMO_PRICE_PRO)
HTML = HTML.replace("CONTACT_EMAIL",  DEMO_CONTACT_EMAIL)
HTML = HTML.replace("CONTACT_PHONE",  DEMO_CONTACT_PHONE)
WA_LINK = f"https://wa.me/{DEMO_CONTACT_WHATSAPP}?text=Hi%2C+I+tried+the+RPR+GST+Demo+and+want+the+Full+Suite"
HTML = HTML.replace("WHATSAPP_LINK",  WA_LINK)

# ── Routes ─────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/upload", methods=["POST"])
@rate_limit(limit=10, window=60)
def api_upload():
    _cleanup_old_jobs()
    gstin       = request.form.get("gstin","").strip().upper()
    client_name = request.form.get("client_name","").strip()
    fy          = request.form.get("fy","2025-26").strip() or "2025-26"

    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN — must be exactly 15 characters"), 400
    if not client_name:
        return jsonify(error="Company name is required"), 400

    job_id  = str(uuid.uuid4())[:8]
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    saved = {k: [] for k in ("r1","r1a","r2b","r2a","r3b","cust")}
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
@rate_limit(limit=30, window=60)
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

        # Organise files by type and rename with month info
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

        for fpath in saved.get("r1a", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR1A_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1A: {mon} {yr}")

        for fpath in saved.get("r2b", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR2B_{mon}_{yr}.xlsx"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2B: {mon} {yr}")

        for fpath in saved.get("r2a", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                ext  = Path(fpath).suffix.lower()
                dest = job_dir / f"GSTR2A_{mon}_{yr}{ext}"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-2A: {mon} {yr}")

        for fpath in saved.get("r3b", []):
            mon, yr = _detect_month(fpath, FY_MONTHS)
            if mon:
                dest = job_dir / f"GSTR3B_{mon}_{yr}.pdf"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-3B: {mon} {yr}")

        for fpath in saved.get("cust", []):
            dest = job_dir / "customer_names.xlsx"
            if not dest.exists():
                try: Path(fpath).rename(dest)
                except: shutil.copy2(fpath, str(dest))
            log("  Customer names loaded"); break

        prog(25)

        suite_path = _find_engine("gst_suite_final.py")
        if not suite_path:
            raise FileNotFoundError(
                "GST engine not found. "
                "Place gst_suite_v32.py in the same folder as this application."
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
        log("Running annual reconciliation — this takes 1-2 minutes...")
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
            raise RuntimeError(
                "No Excel output generated. "
                "Ensure the GST engine (gst_suite_v32.py) is present and the uploaded files are valid."
            )

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready.", "ok")
        log(f"─── Want the Full Suite? Call/WhatsApp {DEMO_CONTACT_PHONE} ───", "info")

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

# ── Startup ────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    url  = f"http://localhost:{port}"

    print()
    print("  ============================================================")
    print("   RPR GST Reconciliation — FREE DEMO")
    print("   Version: Local EXE (Unlimited Runs)")
    print("  ============================================================")
    print(f"   Upload dir  : {UPLOAD_DIR}")
    print(f"   Output dir  : {OUTPUT_DIR}")
    print()
    suite = _find_engine("gst_suite_final.py")
    if suite:
        print(f"   GST Engine  : ✅  {suite.name}")
    else:
        print("   GST Engine  : ⚠   NOT FOUND")
        print("                     Place gst_suite_v32.py in same folder as this EXE")
    print()
    print(f"   Opening     : {url}")
    print("  ============================================================")
    print()

    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    app.run(host="127.0.0.1", port=port, debug=False, threaded=True)
