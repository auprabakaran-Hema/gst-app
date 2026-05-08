"""
RPR IT Reconciliation — ONLINE DEMO (Render.com)
=================================================
• Hosted online — customers upload 26AS + AIS + TIS PDFs and get IT Recon Excel
• 1 free reconciliation per IP (attract → push to buy full suite)
• After result: strong "Buy Full Suite" CTA with WhatsApp button
• Deploy to Render.com — reads $PORT automatically
• Engine: it_recon_engine.py  (must be deployed alongside this file)

DEPLOY STEPS (Render.com):
  1. Create new Web Service on render.com
  2. Connect your GitHub repo (or upload manually)
  3. Include these files:
       online_it_demo.py
       it_recon_engine.py
       requirements_it_demo.txt
       render_it.yaml
  4. Build command:  pip install -r requirements_it_demo.txt
  5. Start command:  python online_it_demo.py
  6. URL will be:    https://rpr-it-demo.onrender.com  (or your custom domain)
"""

import os, sys, shutil, uuid, threading, time
from pathlib import Path
import tempfile
from flask import Flask, request, jsonify, send_file, render_template_string, abort

# ── Contact / Branding ─────────────────────────────────────────────
CONTACT_PHONE    = "7845998125"
CONTACT_EMAIL    = "auprabakaran@gmail.com"
CONTACT_WHATSAPP = "917845998125"
PRICE_BASIC      = "₹2,500/year"
PRICE_PRO        = "₹6,500/year"
WHATSAPP_LINK    = f"https://wa.me/{CONTACT_WHATSAPP}?text=Hi%2C+I+tried+the+RPR+IT+Recon+Demo+and+want+to+buy+the+Full+Suite."

# ── Demo Limit ─────────────────────────────────────────────────────
MAX_RUNS_PER_IP = 1      # 1 free IT reconciliation per IP

# ── Directories ────────────────────────────────────────────────────
def _get_dir(subfolder):
    base = Path(tempfile.gettempdir()) / "rpr_it_online"
    d = base / subfolder
    d.mkdir(parents=True, exist_ok=True)
    return d

UPLOAD_DIR  = _get_dir("uploads")
OUTPUT_DIR  = _get_dir("outputs")
MAX_FILE_MB = 30
JOB_TTL_S   = 1800   # 30 min auto-cleanup

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024

jobs      = {}
jobs_lock = threading.Lock()

# ── IP run tracking ────────────────────────────────────────────────
_ip_runs      = {}
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

def _check_rate(ip, limit=10, window=60):
    now = time.time()
    with _rate_lock:
        hits = [t for t in _rate.get(ip, []) if now - t < window]
        if len(hits) >= limit: return False
        hits.append(now); _rate[ip] = hits
    return True

def _get_ip():
    return request.headers.get("X-Forwarded-For","").split(",")[0].strip() or request.remote_addr or "unknown"

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
    """Locate it_recon_engine.py in possible locations."""
    search_dirs = [
        Path(__file__).parent,
        Path(os.getcwd()),
    ]
    if hasattr(sys, "_MEIPASS"):
        search_dirs.append(Path(sys._MEIPASS))
    for d in search_dirs:
        if not d.exists(): continue
        loc = d / name
        if loc.exists(): return loc
    return None

@app.before_request
def block_scripts():
    p = request.path.lower()
    if p.endswith((".py", ".pyc")):
        abort(403)

# ══════════════════════════════════════════════════════════════════
# HTML — IT Reconciliation Online Demo UI
# ══════════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RPR IT Reconciliation — Free Online Demo</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#06080e;--surf:#0d1422;--surf2:#141d2e;--bdr:#192436;
  --accent:#3b8beb;--accent2:#7c3aed;--grn:#00e676;--org:#ff9800;
  --red:#ff3d57;--txt:#d8e8f5;--muted:#526070;--gold:#ffc107;
  --mono:'IBM Plex Mono',monospace;--sans:'Syne',sans-serif;
}
body{background:var(--bg);color:var(--txt);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;
  background:radial-gradient(ellipse 70% 45% at 50% -5%,rgba(59,139,235,.06),transparent),
             radial-gradient(ellipse 50% 35% at 90% 80%,rgba(124,58,237,.05),transparent);
  pointer-events:none;z-index:0}
.wrap{max-width:820px;margin:0 auto;padding:1.5rem 1.2rem;position:relative;z-index:1}

.try-bar{background:linear-gradient(135deg,rgba(59,139,235,.08),rgba(124,58,237,.06));
  border:1.5px solid rgba(59,139,235,.3);border-radius:13px;
  padding:.85rem 1.1rem;margin-bottom:1.1rem;text-align:center}
.try-bar h2{font-size:.88rem;font-weight:800;color:var(--accent);letter-spacing:.04em;
  text-transform:uppercase;margin-bottom:.3rem}
.try-bar p{font-size:.73rem;color:var(--muted);line-height:1.6}
.try-bar strong{color:var(--txt)}

header{text-align:center;padding:1.5rem 0 .9rem}
.logo{display:inline-flex;align-items:center;gap:.65rem;margin-bottom:.7rem}
.logo-icon{width:44px;height:44px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:11px;display:flex;align-items:center;justify-content:center;
  font-size:1.35rem;box-shadow:0 3px 16px rgba(59,139,235,.28)}
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
.bg-blue{background:rgba(59,139,235,.07);color:var(--accent);border:1px solid rgba(59,139,235,.25)}
.bg-purple{background:rgba(124,58,237,.08);color:#a78bfa;border:1px solid rgba(124,58,237,.3)}

.buy-bar{background:linear-gradient(135deg,rgba(124,58,237,.14),rgba(59,139,235,.07));
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

.card{background:var(--surf);border:1px solid var(--bdr);border-radius:13px;
  padding:1.25rem;margin-bottom:1rem}
.ct{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.09em;
  color:var(--accent);margin-bottom:.8rem;display:flex;align-items:center;gap:.38rem}
.ct::before{content:'';width:3px;height:.9em;background:var(--accent);border-radius:2px}

.fg2{display:grid;grid-template-columns:1fr 1fr;gap:.65rem}
@media(max-width:500px){.fg2{grid-template-columns:1fr}}
.fg3{display:grid;grid-template-columns:1fr 1fr 1fr;gap:.65rem}
@media(max-width:600px){.fg3{grid-template-columns:1fr 1fr}}
.fg{display:flex;flex-direction:column;gap:.26rem}
label{font-size:.63rem;font-weight:600;letter-spacing:.07em;text-transform:uppercase;color:var(--muted)}
input[type=text],select{
  background:var(--surf2);border:1px solid var(--bdr);border-radius:7px;
  padding:.48rem .72rem;color:var(--txt);font-family:var(--mono);font-size:.78rem;
  transition:border-color .2s;width:100%}
input:focus,select:focus{outline:none;border-color:var(--accent)}
select option{background:var(--surf)}

/* Drop zones */
.dg{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:.55rem;margin-top:.5rem}
.dz{background:var(--surf2);border:2px dashed var(--bdr);border-radius:9px;
  padding:.85rem .55rem;text-align:center;cursor:pointer;transition:all .2s;
  position:relative;min-height:96px;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:.28rem}
.dz:hover,.dz.drag-over{border-color:var(--accent);background:rgba(59,139,235,.04)}
.dz.has-files{border-color:var(--grn);border-style:solid;background:rgba(0,230,118,.04)}
.dz-ic{font-size:1.4rem;line-height:1}
.dz-lb{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.dz-ht{font-size:.57rem;color:var(--muted);font-family:var(--mono);opacity:.7;line-height:1.4;margin-top:.15rem}
.dz-cn{font-size:.61rem;color:var(--grn);font-weight:600;font-family:var(--mono)}
.dz input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer;width:100%;height:100%}

.info-box{background:rgba(59,139,235,.06);border:1px solid rgba(59,139,235,.2);border-radius:8px;
  padding:.75rem 1rem;font-size:.68rem;color:var(--muted);line-height:1.7;margin-top:.7rem}
.info-box strong{color:var(--txt)}
.info-box code{color:var(--accent);font-family:var(--mono)}

.sheets-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:.4rem;margin-top:.55rem}
.sheet-item{background:var(--surf2);border:1px solid var(--bdr);border-radius:6px;
  padding:.5rem .7rem;font-size:.65rem;color:var(--muted);font-family:var(--mono)}
.sheet-item strong{color:var(--txt);display:block;margin-bottom:.15rem}

.btn-run{width:100%;padding:.8rem;margin-top:.9rem;
  background:linear-gradient(135deg,var(--accent),var(--accent2));
  border:none;border-radius:10px;color:#fff;font-family:var(--sans);font-size:.86rem;
  font-weight:800;letter-spacing:.06em;text-transform:uppercase;cursor:pointer;
  transition:transform .15s,box-shadow .15s}
.btn-run:hover{transform:translateY(-2px);box-shadow:0 8px 28px rgba(59,139,235,.3)}
.btn-run:disabled{opacity:.35;cursor:not-allowed;transform:none}

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
  padding:.65rem;font-family:var(--mono);font-size:.67rem;height:160px;
  overflow-y:auto;color:#88ddbb;line-height:1.75}
.logbox .err{color:#ff6b8a}.logbox .warn{color:#ffb347}
.logbox .ok{color:var(--grn)}.logbox .info{color:var(--accent)}

.dw{display:none}
.dl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(170px,1fr));gap:.55rem;margin:.6rem 0}
.dlc{background:var(--surf2);border:1px solid var(--bdr);border-radius:8px;
  padding:.75rem;display:flex;flex-direction:column;gap:.38rem}
.dl-name{font-size:.68rem;font-weight:600;color:var(--txt)}
.dl-size{font-size:.6rem;color:var(--muted);font-family:var(--mono)}
.btn-dl{padding:.3rem .65rem;background:rgba(59,139,235,.1);
  border:1px solid rgba(59,139,235,.35);border-radius:5px;color:var(--accent);
  font-family:var(--mono);font-size:.68rem;cursor:pointer;
  text-decoration:none;display:inline-block;transition:background .15s}
.btn-dl:hover{background:rgba(59,139,235,.22)}

.limit-box{background:linear-gradient(135deg,rgba(124,58,237,.12),rgba(59,139,235,.08));
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

.success-cta{background:linear-gradient(135deg,rgba(0,230,118,.07),rgba(59,139,235,.05));
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

footer{text-align:center;padding:1.6rem 0 2rem;color:var(--muted);font-size:.67rem;font-family:var(--mono)}
footer a{color:var(--muted);text-decoration:none}
footer a:hover{color:var(--accent)}
</style>
</head>
<body>
<div class="wrap">

<div class="try-bar">
  <h2>🏦 IT Reconciliation — Free Online Demo</h2>
  <p>
    Upload your <strong>Form 26AS + AIS + TIS PDFs</strong> and get a ready-to-use <strong>IT Recon Excel</strong> in 2 minutes.<br>
    <strong>1 free reconciliation</strong> · No login required · Files auto-deleted after 30 min
  </p>
</div>

<header>
  <div class="logo">
    <div class="logo-icon">🏛️</div>
    <div class="logo-text">RPR Suite</div>
  </div>
  <h1>Income Tax <em>Reconciliation</em></h1>
  <div class="sub">26AS · AIS · TIS → Annual Excel Report (8 sheets)</div>
  <div class="badges">
    <span class="badge bg-gold">⭐ 1 FREE ONLINE RUN</span>
    <span class="badge bg-blue">🔒 No Data Stored</span>
    <span class="badge bg-grn">📊 Excel Output</span>
    <span class="badge bg-purple">8 Report Sheets</span>
  </div>
</header>

<div class="buy-bar">
  <div class="buy-left">
    <h3>🚀 Get the Full Suite (Local EXE)</h3>
    <p>
      Unlimited clients · Auto-download 26AS+AIS+TIS from portal<br>
      GST Suite · Tally comparison · Machine-locked license<br>
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
  <div class="fg3" style="margin-bottom:.72rem">
    <div class="fg">
      <label>Company / Client Name</label>
      <input type="text" id="cname" placeholder="ABC Trading Co">
    </div>
    <div class="fg">
      <label>PAN (10 chars)</label>
      <input type="text" id="pan" maxlength="10" placeholder="AAAAA0000A" style="text-transform:uppercase">
    </div>
    <div class="fg">
      <label>Assessment Year</label>
      <select id="fy">
        <option value="2025-26">AY 2025-26 (FY 2024-25)</option>
        <option value="2024-25">AY 2024-25 (FY 2023-24)</option>
        <option value="2023-24">AY 2023-24 (FY 2022-23)</option>
      </select>
    </div>
  </div>
  <div class="fg" style="margin-bottom:.72rem">
    <label>GSTIN (optional — for GST vs IT turnover matching)</label>
    <input type="text" id="gstin" maxlength="15" placeholder="29XXXXX... (leave blank if not applicable)" style="text-transform:uppercase">
  </div>

  <div class="ct">Upload IT Portal PDFs</div>
  <div class="info-box">
    <strong>How to download from incometax.gov.in:</strong><br>
    Login → e-File → Income Tax Returns → <code>View Form 26AS</code> (download PDF)<br>
    AIS/TIS: Login → Services → <code>Annual Information Statement</code> → Download PDF<br>
    💡 <strong>Best results: upload all 3 PDFs.</strong> TIS is most critical for ITR reconciliation.
  </div>

  <div class="dg" style="margin-top:.8rem">
    <div class="dz" id="dz_26as">
      <div class="dz-ic">📄</div>
      <div class="dz-lb">Form 26AS</div>
      <div class="dz-ht">TRACES PDF<br>(XXXPA*.pdf)</div>
      <div class="dz-cn" id="cnt_26as"></div>
      <input type="file" id="f_26as" accept=".pdf">
    </div>
    <div class="dz" id="dz_ais">
      <div class="dz-ic">📋</div>
      <div class="dz-lb">AIS PDF</div>
      <div class="dz-ht">Annual Information<br>Statement PDF</div>
      <div class="dz-cn" id="cnt_ais"></div>
      <input type="file" id="f_ais" accept=".pdf">
    </div>
    <div class="dz" id="dz_tis">
      <div class="dz-ic">📊</div>
      <div class="dz-lb">TIS PDF</div>
      <div class="dz-ht">Taxpayer Information<br>Summary PDF</div>
      <div class="dz-cn" id="cnt_tis"></div>
      <input type="file" id="f_tis" accept=".pdf">
    </div>
    <div class="dz" id="dz_gst">
      <div class="dz-ic">📈</div>
      <div class="dz-lb">GST Recon Excel</div>
      <div class="dz-ht">From GST Demo<br>(optional)</div>
      <div class="dz-cn" id="cnt_gst"></div>
      <input type="file" id="f_gst" accept=".xlsx,.xls">
    </div>
  </div>

  <!-- What you'll get -->
  <div style="margin-top:1rem">
    <div style="font-size:.63rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);margin-bottom:.5rem">📋 Excel output — 8 sheets:</div>
    <div class="sheets-grid">
      <div class="sheet-item"><strong>IT_Summary</strong>Key figures, TDS total, turnover</div>
      <div class="sheet-item"><strong>TDS_26AS_Detail</strong>All deductors, Part A/A1/A2/B/C</div>
      <div class="sheet-item"><strong>TIS_vs_GSTR_Annual</strong>Annual turnover comparison</div>
      <div class="sheet-item"><strong>TIS_vs_GSTR_Monthly</strong>12-month breakdown</div>
      <div class="sheet-item"><strong>Purchase_Detail</strong>Supplier-wise from AIS</div>
      <div class="sheet-item"><strong>AIS_vs_Turnover</strong>Full recon with adjustment rows</div>
      <div class="sheet-item"><strong>AIS_Detail</strong>All AIS income/purchase lines</div>
      <div class="sheet-item"><strong>AIS_vs_GSTR_Monthly</strong>GSTR-1 vs AIS monthly</div>
    </div>
  </div>

  <button class="btn-run" id="runBtn" onclick="startRun()">▶ Run Free IT Reconciliation</button>
</div>

<!-- Progress Card -->
<div class="card pw" id="progCard">
  <div class="status-row">
    <div class="ct" style="margin:0">Processing IT Reconciliation</div>
    <span class="sbg s-r pulse" id="statusBadge">Running</span>
  </div>
  <div class="pb-wrap"><div class="pb" id="pb"></div></div>
  <div class="logbox" id="logBox"></div>
</div>

<!-- Downloads Card -->
<div class="card dw" id="dlCard">
  <div class="ct">✅ Your IT Reconciliation Report</div>
  <div class="dl-grid" id="dlGrid"></div>
  <div class="success-cta">
    <div class="headline">🎉 Done! See the power of IT Recon?</div>
    <div class="sub">
      The <strong>Full Suite (local EXE)</strong> auto-downloads 26AS, AIS, TIS for ALL your clients
      in one click — one OTP, 3 tabs, 4-5 minutes per client. Unlimited clients, machine-locked license,
      full GST Suite bundled.<br><br>
      <strong>Basic: PRICE_BASIC (20 clients) &nbsp;·&nbsp; Pro: PRICE_PRO (unlimited)</strong>
    </div>
    <div class="cta-btns">
      <a class="cta-wa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp to Buy Full Suite</a>
      <a class="cta-email" href="mailto:CONTACT_EMAIL?subject=RPR Full Suite Purchase — IT Recon Demo" target="_blank">✉ Email Us</a>
    </div>
  </div>
</div>

<footer>
  RPR IT Reconciliation Online Demo &nbsp;·&nbsp; Files auto-deleted after 30 min &nbsp;·&nbsp;
  <a href="mailto:CONTACT_EMAIL">CONTACT_EMAIL</a> &nbsp;·&nbsp;
  <a href="WHATSAPP_LINK" target="_blank">WhatsApp: CONTACT_PHONE</a>
</footer>
</div>

<script>
const FILES = {};
const ZONES = ['26as','ais','tis','gst'];

ZONES.forEach(z => {
  const inp = document.getElementById('f_'+z);
  const dz  = document.getElementById('dz_'+z);
  const cnt = document.getElementById('cnt_'+z);
  inp.addEventListener('change', () => {
    FILES[z] = inp.files;
    const n = inp.files.length;
    cnt.textContent = n ? inp.files[0].name.slice(0,18)+'…' : '';
    dz.classList.toggle('has-files', n > 0);
  });
  dz.addEventListener('dragover', e => { e.preventDefault(); dz.classList.add('drag-over'); });
  dz.addEventListener('dragleave', () => dz.classList.remove('drag-over'));
  dz.addEventListener('drop', e => {
    e.preventDefault(); dz.classList.remove('drag-over');
    const dt = e.dataTransfer;
    if (dt.files.length) {
      FILES[z] = dt.files;
      cnt.textContent = dt.files[0].name.slice(0,18)+'…';
      dz.classList.add('has-files');
    }
  });
});

let jobId = null, pollTimer = null;

async function startRun() {
  const cname = document.getElementById('cname').value.trim();
  const pan   = document.getElementById('pan').value.trim().toUpperCase();
  const gstin = document.getElementById('gstin').value.trim().toUpperCase();
  const fy    = document.getElementById('fy').value;

  if (!cname) { alert('Please enter the company / client name'); return; }
  if (!pan || pan.length !== 10) { alert('Please enter a valid 10-character PAN'); return; }

  const hasAny = ZONES.some(z => FILES[z] && FILES[z].length > 0);
  if (!hasAny) { alert('Please upload at least one PDF (Form 26AS, AIS, or TIS)'); return; }

  const fd = new FormData();
  fd.append('cname', cname);
  fd.append('pan',   pan);
  fd.append('gstin', gstin);
  fd.append('fy',    fy);
  ZONES.forEach(z => {
    if (FILES[z]) Array.from(FILES[z]).forEach(f => fd.append('file_'+z, f));
  });

  document.getElementById('runBtn').disabled = true;
  document.getElementById('progCard').style.display = 'block';
  document.getElementById('dlCard').style.display   = 'none';
  document.getElementById('logBox').innerHTML = '';
  document.getElementById('pb').style.width   = '0%';
  document.getElementById('statusBadge').textContent = 'Running';
  document.getElementById('statusBadge').className   = 'sbg s-r pulse';
  document.getElementById('progCard').scrollIntoView({behavior:'smooth'});

  try {
    const res  = await fetch('/api/it_upload', { method:'POST', body:fd });
    const data = await res.json();

    if (res.status === 429 && data.limit_reached) {
      document.getElementById('formCard').style.display  = 'none';
      document.getElementById('progCard').style.display  = 'none';
      document.getElementById('dlCard').style.display    = 'none';
      document.body.innerHTML += `
        <div class="wrap"><div class="limit-box">
          <div class="icon">🔒</div>
          <h2>You've used your 1 Free Demo Run</h2>
          <p>
            Each IP address gets <strong>1 free IT reconciliation</strong>.<br>
            To run unlimited reconciliations for all your clients,<br>
            get the <strong>full local EXE suite</strong>.
          </p>
          <div class="limit-btns">
            <a class="lwa" href="WHATSAPP_LINK" target="_blank">📱 WhatsApp to Buy Full Suite</a>
            <a class="lemail" href="mailto:CONTACT_EMAIL?subject=RPR Full Suite — IT Demo Limit Reached" target="_blank">✉ Email Us</a>
          </div>
        </div></div>`;
      return;
    }

    if (!res.ok || data.error) { throw new Error(data.error || 'Upload failed'); }
    jobId = data.job_id;
    pollTimer = setInterval(pollJob, 1500);
  } catch(err) {
    addLog('Error: '+err.message, 'err');
    document.getElementById('statusBadge').textContent = 'Error';
    document.getElementById('statusBadge').className   = 'sbg s-e';
    document.getElementById('runBtn').disabled = false;
  }
}

function addLog(msg, cls='') {
  const lb = document.getElementById('logBox');
  const line = document.createElement('div');
  if(cls) line.className = cls;
  line.textContent = msg;
  lb.appendChild(line);
  lb.scrollTop = lb.scrollHeight;
}

async function pollJob() {
  try {
    const res  = await fetch('/api/it_job/'+jobId);
    const data = await res.json();

    if (data.progress !== undefined) {
      document.getElementById('pb').style.width = data.progress+'%';
    }
    if (data.logs && data.logs.length) {
      data.logs.forEach(l => addLog(l.msg, l.type||''));
    }
    if (data.status === 'done') {
      clearInterval(pollTimer);
      document.getElementById('statusBadge').textContent = 'Done';
      document.getElementById('statusBadge').className   = 'sbg s-d';
      document.getElementById('pb').style.width          = '100%';
      showDownloads(data.files||[]);
    } else if (data.status === 'error') {
      clearInterval(pollTimer);
      document.getElementById('statusBadge').textContent = 'Error';
      document.getElementById('statusBadge').className   = 'sbg s-e';
      addLog('Job failed: '+(data.error||'unknown'), 'err');
      document.getElementById('runBtn').disabled = false;
    }
  } catch(err) { addLog('Poll error: '+err.message, 'err'); }
}

function showDownloads(files) {
  const grid = document.getElementById('dlGrid');
  grid.innerHTML = '';
  files.forEach(f => {
    grid.innerHTML += `<div class="dlc">
      <div class="dl-name">📊 ${f.name}</div>
      <div class="dl-size">${f.size}</div>
      <a class="btn-dl" href="/api/it_download/${jobId}/${encodeURIComponent(f.name)}" download="${f.name}">⬇ Download</a>
    </div>`;
  });
  document.getElementById('dlCard').style.display = 'block';
  document.getElementById('dlCard').scrollIntoView({behavior:'smooth'});
}
</script>
</body>
</html>
"""

# ── Flask Routes ────────────────────────────────────────────────────

@app.route("/")
def index():
    html = HTML
    html = html.replace("WHATSAPP_LINK",  WHATSAPP_LINK)
    html = html.replace("CONTACT_EMAIL",  CONTACT_EMAIL)
    html = html.replace("CONTACT_PHONE",  CONTACT_PHONE)
    html = html.replace("PRICE_BASIC",    PRICE_BASIC)
    html = html.replace("PRICE_PRO",      PRICE_PRO)
    return render_template_string(html)


@app.route("/api/it_upload", methods=["POST"])
def it_upload():
    _cleanup_old_jobs()
    ip = _get_ip()

    if not _check_rate(ip):
        return jsonify(error="Too many requests. Wait 1 minute."), 429

    if not _check_ip_limit(ip):
        return jsonify(error="Demo limit reached", limit_reached=True), 429

    cname = request.form.get("cname","").strip()
    pan   = request.form.get("pan","").strip().upper()
    gstin = request.form.get("gstin","").strip().upper()
    fy    = request.form.get("fy","2025-26")

    if not cname or not pan or len(pan) != 10:
        return jsonify(error="Client name and valid PAN are required"), 400

    job_id  = str(uuid.uuid4())
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    saved = {}
    for zone, label in [("26as","26AS"), ("ais","AIS"), ("tis","TIS"), ("gst","GST")]:
        files = request.files.getlist(f"file_{zone}")
        for f in files:
            if f and f.filename:
                ext = Path(f.filename).suffix.lower()
                if ext not in {".pdf", ".xlsx", ".xls"}: continue
                dest = job_dir / f"{label}_{f.filename}"
                f.save(str(dest))
                saved[zone] = str(dest)

    if not saved:
        return jsonify(error="No valid PDF files uploaded"), 400

    with jobs_lock:
        jobs[job_id] = {"status": "running", "logs": [], "progress": 0, "files": []}

    threading.Thread(target=_run_it_recon,
                     args=(job_id, job_dir, out_dir, cname, pan, gstin, fy, saved),
                     daemon=True).start()

    return jsonify(job_id=job_id)


@app.route("/api/it_job/<job_id>")
def it_job(job_id):
    with jobs_lock:
        j = jobs.get(job_id)
    if not j:
        return jsonify(error="Job not found"), 404
    logs_copy = j.get("logs", [])[:]
    j["logs"] = []   # flush to client
    return jsonify(
        status   = j["status"],
        progress = j.get("progress", 0),
        logs     = logs_copy,
        files    = j.get("files", []),
        error    = j.get("error", "")
    )


@app.route("/api/it_download/<job_id>/<filename>")
def it_download(job_id, filename):
    out_dir = OUTPUT_DIR / job_id
    fp = out_dir / filename
    if not fp.exists() or not str(fp).startswith(str(OUTPUT_DIR)):
        abort(404)
    return send_file(str(fp), as_attachment=True, download_name=filename)


# ── Background Worker ───────────────────────────────────────────────

def _run_it_recon(job_id, job_dir, out_dir, cname, pan, gstin, fy, saved):
    def log(msg, t="info"):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["logs"].append({"msg": msg, "type": t})

    def prog(pct):
        with jobs_lock:
            if job_id in jobs:
                jobs[job_id]["progress"] = pct

    try:
        log(f"Starting IT Reconciliation for {cname} (PAN: {pan})")
        prog(5)

        # Locate it_recon_engine.py
        engine_path = _find_engine("it_recon_engine.py")
        if not engine_path:
            raise FileNotFoundError(
                "it_recon_engine.py not found. "
                "Deploy it_recon_engine.py alongside online_it_demo.py."
            )

        log("Loading IT reconciliation engine...", "info")
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location("it_recon_engine", str(engine_path))
        eng  = _ilu.module_from_spec(spec)
        spec.loader.exec_module(eng)
        prog(15)

        # Log what files were found
        for zone, label in [("26as","Form 26AS"), ("ais","AIS PDF"), ("tis","TIS PDF"), ("gst","GST Recon Excel")]:
            if zone in saved:
                log(f"  ✓ {label}: {Path(saved[zone]).name}", "ok")
            else:
                log(f"  ⚠ {label}: not uploaded (optional)", "warn")

        prog(20)
        log("Parsing PDFs and running reconciliation...", "info")
        log("(This may take 1-3 minutes depending on PDF size)", "info")
        prog(30)

        # Run the IT reconciliation engine
        import logging as _lg
        _log = _lg.getLogger(f"it_{job_id}")
        _log.setLevel(_lg.DEBUG)
        class _WL(_lg.Handler):
            def emit(self, r):
                t = "err" if r.levelno >= _lg.WARNING else "info"
                log(self.format(r), t)
        _log.addHandler(_WL())

        # Call the engine — it reads PDFs from job_dir automatically
        eng.write_it_reconciliation(
            job_dir    = str(job_dir),
            company_name = cname,
            pan        = pan,
            gstin      = gstin if gstin else "",
            fy         = fy,
            log        = log,
        )

        prog(85)
        log("Collecting output files...", "info")

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
                "Make sure at least one PDF was uploaded and parsed successfully."
            )

        prog(100)
        log(f"Done! {len(output_files)} file(s) ready for download.", "ok")
        log(f"─── Want full auto-download? WhatsApp {CONTACT_PHONE} ───", "info")

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


# ── Startup ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5001))
    print()
    print("  ============================================================")
    print("   RPR IT Reconciliation — ONLINE DEMO")
    print(f"   Port: {port}")
    engine = _find_engine("it_recon_engine.py")
    print(f"   IT Engine: {'✅ ' + engine.name if engine else '❌ NOT FOUND — deploy it_recon_engine.py'}")
    print("  ============================================================")
    print()
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
