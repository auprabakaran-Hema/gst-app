"""
GST Reconciliation Web App — PROTECTED EDITION (No Trial, Full Features)
========================================================================
• Scripts protected: gst_suite_final.py (encoded), gstr1_extract.py (PyArmor)
• Full version: No restrictions, unlimited uses
• GSTR-3B PDF extraction enabled
• Automatic download features included
"""
import os, sys, json, zipfile, re, time, shutil, uuid, threading, hashlib
from pathlib import Path
from datetime import datetime
from flask import (Flask, request, jsonify, send_file,
                   render_template_string, abort)

# ── Config ────────────────────────────────────────────────────────
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
JOB_TTL_S   = 7200   # 2 hours — jobs & files auto-deleted after this

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = MAX_FILE_MB * 1024 * 1024

# In-memory job tracking
jobs      = {}
jobs_lock = threading.Lock()

# ── Rate limiting (per IP) ────────────────────────────────────────
_rate: dict[str, list] = {}   # ip → [timestamps]
_rate_lock = threading.Lock()

def _check_rate(ip: str, limit: int = 10, window: int = 60) -> bool:
    """Return True if request is allowed, False if rate-limited."""
    now = time.time()
    with _rate_lock:
        hits = [t for t in _rate.get(ip, []) if now - t < window]
        if len(hits) >= limit:
            return False
        hits.append(now)
        _rate[ip] = hits
    return True

def rate_limit(limit=10, window=60):
    """Decorator — abort 429 if rate exceeded."""
    from functools import wraps
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            ip = request.remote_addr or "unknown"
            if not _check_rate(ip, limit, window):
                return jsonify(error="Too many requests. Please wait."), 429
            return f(*args, **kwargs)
        return wrapped
    return decorator

# ── License validation (FULL VERSION - No Restrictions) ───────────
def validate_license(key: str) -> dict:
    """FULL VERSION MODE - No restrictions, unlimited uses."""
    return {
        "valid":      True,
        "plan":       "full",
        "customer":   "Full Version User",
        "expires_at": None,
    }

# ── Cleanup ───────────────────────────────────────────────────────
def _cleanup_old_jobs():
    """Delete job folders older than JOB_TTL_S seconds."""
    try:
        now = time.time()
        for d in [UPLOAD_DIR, OUTPUT_DIR]:
            for sub in d.iterdir():
                if sub.is_dir() and (now - sub.stat().st_mtime) > JOB_TTL_S:
                    shutil.rmtree(str(sub), ignore_errors=True)
    except:
        pass

def _cleanup_job_files(job_id: str):
    """Delete upload files for a completed job immediately."""
    try:
        up = UPLOAD_DIR / job_id
        if up.exists():
            shutil.rmtree(str(up), ignore_errors=True)
    except:
        pass

# ── Block access to .py files ─────────────────────────────────────
@app.before_request
def block_script_access():
    path = request.path.lower()
    if path.endswith(".py") or path.endswith(".pyc") or "gst_suite" in path or "gstr1_extract" in path:
        abort(403)

# ── HTML (embedded) ───────────────────────────────────────────────
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
  --accent:#00e5ff;--accent2:#7c3aed;--green:#00e676;--orange:#ff6d00;--red:#ff1744;
  --gold:#ffd700;--text:#e8edf5;--muted:#6b7fa3;--mono:'IBM Plex Mono',monospace;
  --sans:'Syne',sans-serif;
}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::before{content:'';position:fixed;inset:0;
  background-image:linear-gradient(rgba(0,229,255,.04) 1px,transparent 1px),
                    linear-gradient(90deg,rgba(0,229,255,.04) 1px,transparent 1px);
  background-size:40px 40px;pointer-events:none;z-index:0}
.container{max-width:960px;margin:0 auto;padding:2rem 1.5rem;position:relative;z-index:1}
header{text-align:center;padding:3rem 0 2rem}
.logo{display:inline-flex;align-items:center;gap:.75rem;margin-bottom:1.5rem}
.logo-icon{width:48px;height:48px;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border-radius:12px;display:flex;align-items:center;justify-content:center;font-size:1.5rem}
.logo-text{font-size:1.1rem;font-weight:700;letter-spacing:.1em;text-transform:uppercase;
  background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;
  -webkit-text-fill-color:transparent}
h1{font-size:clamp(1.8rem,4vw,2.8rem);font-weight:800;line-height:1.1;letter-spacing:-.02em}
h1 span{background:linear-gradient(135deg,var(--accent),var(--accent2));
  -webkit-background-clip:text;-webkit-text-fill-color:transparent}
.subtitle{color:var(--muted);font-size:.95rem;margin-top:.75rem;font-family:var(--mono)}
.version-badge{display:inline-flex;align-items:center;gap:.4rem;padding:.4rem 1rem;
  border-radius:100px;font-size:.8rem;font-weight:700;font-family:var(--mono);margin-top:1rem;
  background:rgba(0,230,118,.15);color:var(--green);border:1px solid rgba(0,230,118,.4)}
.card{background:var(--surface);border:1px solid var(--border);border-radius:16px;
  padding:1.75rem;margin-bottom:1.5rem;transition:border-color .2s}
.card:hover{border-color:rgba(0,229,255,.3)}
.card-title{font-size:1rem;font-weight:700;text-transform:uppercase;letter-spacing:.08em;
  color:var(--accent);margin-bottom:1.25rem;display:flex;align-items:center;gap:.6rem}
.card-title::before{content:'';width:3px;height:1em;background:var(--accent);border-radius:2px}
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:1rem}
@media(max-width:600px){.form-grid{grid-template-columns:1fr}}
.form-group{display:flex;flex-direction:column;gap:.5rem}
label{font-size:.8rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
input[type=text],input[type=password]{background:var(--surface2);border:1px solid var(--border);
  border-radius:8px;padding:.65rem .9rem;color:var(--text);font-family:var(--mono);font-size:.9rem;
  transition:border-color .2s;width:100%}
input:focus{outline:none;border-color:var(--accent)}
input::placeholder{color:var(--muted)}
.drop-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:1rem;margin-top:.5rem}
.drop-zone{background:var(--surface2);border:2px dashed var(--border);border-radius:12px;
  padding:1.5rem 1rem;text-align:center;cursor:pointer;transition:all .2s;
  position:relative;min-height:120px;display:flex;flex-direction:column;
  align-items:center;justify-content:center;gap:.5rem}
.drop-zone:hover,.drop-zone.drag-over{border-color:var(--accent);background:rgba(0,229,255,.05)}
.drop-zone.has-files{border-color:var(--green);border-style:solid;background:rgba(0,230,118,.05)}
.drop-icon{font-size:2rem;line-height:1}
.drop-label{font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.drop-hint{font-size:.7rem;color:var(--muted);font-family:var(--mono)}
.drop-count{font-size:.75rem;color:var(--green);font-weight:600;font-family:var(--mono)}
.drop-zone input[type=file]{position:absolute;inset:0;opacity:0;cursor:pointer}
.name-lookup{background:var(--surface2);border:1px solid rgba(124,58,237,.4);border-radius:12px;
  padding:1.25rem;margin-top:1rem}
.name-lookup-title{font-size:.8rem;font-weight:700;text-transform:uppercase;
  letter-spacing:.06em;color:var(--accent2);margin-bottom:.75rem}
.name-info{font-size:.78rem;color:var(--muted);font-family:var(--mono);line-height:1.6}
.name-info strong{color:var(--text)}
.btn-submit{width:100%;padding:1rem;background:linear-gradient(135deg,var(--accent),var(--accent2));
  border:none;border-radius:12px;color:#000;font-family:var(--sans);font-size:1rem;
  font-weight:800;letter-spacing:.05em;text-transform:uppercase;cursor:pointer;
  transition:transform .15s,box-shadow .15s;margin-top:.5rem}
.btn-submit:hover{transform:translateY(-2px);box-shadow:0 8px 32px rgba(0,229,255,.3)}
.btn-submit:disabled{opacity:.5;cursor:not-allowed;transform:none}
#progress-section{display:none}
.progress-bar-wrap{background:var(--surface2);border-radius:100px;height:8px;overflow:hidden;margin:1rem 0}
.progress-bar-fill{height:100%;background:linear-gradient(90deg,var(--accent),var(--accent2));
  border-radius:100px;transition:width .3s;width:0%}
.log-box{background:#000;border:1px solid var(--border);border-radius:8px;
  padding:1rem;font-family:var(--mono);font-size:.78rem;height:180px;overflow-y:auto;
  color:#aaffcc;line-height:1.7}
.log-box .err{color:#ff6b6b}.log-box .info{color:var(--accent)}
.log-box .ok{color:var(--green)}.log-box .warn{color:var(--orange)}
#downloads-section{display:none}
.dl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:1rem;margin-top:1rem}
.dl-card{background:var(--surface2);border:1px solid var(--border);border-radius:12px;
  padding:1.25rem;display:flex;flex-direction:column;gap:.75rem;align-items:flex-start}
.dl-name{font-size:.8rem;font-weight:600;color:var(--text)}
.dl-size{font-size:.72rem;color:var(--muted);font-family:var(--mono)}
.btn-dl{padding:.5rem 1rem;background:var(--surface);border:1px solid var(--accent);
  border-radius:8px;color:var(--accent);font-family:var(--mono);font-size:.8rem;
  cursor:pointer;text-decoration:none;display:inline-block;transition:background .15s}
.btn-dl:hover{background:rgba(0,229,255,.1)}
.btn-auto-dl{padding:.5rem 1rem;background:var(--green);border:none;
  border-radius:8px;color:#000;font-family:var(--mono);font-size:.8rem;
  cursor:pointer;margin-top:.5rem;font-weight:600}
.btn-auto-dl:hover{background:#00c853}
.status-badge{display:inline-flex;align-items:center;gap:.4rem;padding:.3rem .75rem;
  border-radius:100px;font-size:.75rem;font-weight:700;font-family:var(--mono)}
.status-processing{background:rgba(255,109,0,.15);color:var(--orange);border:1px solid rgba(255,109,0,.4)}
.status-done{background:rgba(0,230,118,.15);color:var(--green);border:1px solid rgba(0,230,118,.4)}
.status-error{background:rgba(255,23,68,.15);color:var(--red);border:1px solid rgba(255,23,68,.4)}
.pulse{animation:pulse 1.2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.step{display:flex;gap:1rem;align-items:flex-start;margin-bottom:.85rem}
.step-num{width:28px;height:28px;border-radius:50%;background:linear-gradient(135deg,var(--accent),var(--accent2));
  color:#000;font-weight:800;font-size:.8rem;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.step-text{font-size:.88rem;color:var(--muted);line-height:1.5}
.step-text strong{color:var(--text)}
.auto-download-section{background:var(--surface2);border:1px solid var(--green);border-radius:12px;
  padding:1rem;margin-top:1rem}
.auto-download-title{font-size:.8rem;font-weight:700;color:var(--green);margin-bottom:.5rem}
</style>
</head>
<body>
<div class="container">

<header>
  <div class="logo">
    <div class="logo-icon">₹</div>
    <div class="logo-text">GST Recon</div>
  </div>
  <h1>Annual GST<br><span>Reconciliation Portal</span></h1>
  <p class="subtitle">Upload returns → Get reconciliation Excel in seconds</p>
  <div class="version-badge">⭐ FULL VERSION — Unlimited Access</div>
</header>

<!-- Instructions -->
<div class="card">
  <div class="card-title">How It Works</div>
  <div class="step"><div class="step-num">1</div><div class="step-text"><strong>Enter client details</strong> — GSTIN, company name, financial year</div></div>
  <div class="step"><div class="step-num">2</div><div class="step-text"><strong>Upload your files</strong> — GSTR-1 ZIPs, GSTR-2B Excel, GSTR-3B PDFs, GSTR-2A ZIPs</div></div>
  <div class="step"><div class="step-num">3</div><div class="step-text"><strong>Optional:</strong> Upload <strong>customer_names.xlsx</strong> for auto-fill receiver names</div></div>
  <div class="step"><div class="step-num">4</div><div class="step-text"><strong>Click Generate</strong> — Downloads Annual Reconciliation + GSTR3B-R1 + GSTR3B-R2A Summaries</div></div>
</div>

<form id="main-form">
<div class="card">
  <div class="card-title">Client Details</div>
  <div class="form-grid">
    <div class="form-group"><label>GSTIN *</label>
      <input type="text" id="gstin" placeholder="33ABCDE1234F1ZX" maxlength="15" required></div>
    <div class="form-group"><label>Company Name *</label>
      <input type="text" id="client_name" placeholder="ABC Traders" required></div>
    <div class="form-group"><label>Financial Year *</label>
      <input type="text" id="fy" value="2025-26" required></div>
    <div class="form-group"><label>State</label>
      <input type="text" id="state" placeholder="Tamil Nadu (optional)"></div>
  </div>
</div>

<div class="card">
  <div class="card-title">Upload Returns</div>
  <div class="drop-grid">
    <div class="drop-zone" id="zone-r1">
      <div class="drop-icon">📋</div><div class="drop-label">GSTR-1</div>
      <div class="drop-hint">ZIP files (12 months)</div>
      <div class="drop-count" id="count-r1">No files</div>
      <input type="file" multiple accept=".zip,.json" data-zone="r1" onchange="updateZone('r1',this)">
    </div>
    <div class="drop-zone" id="zone-r2b">
      <div class="drop-icon">🏦</div><div class="drop-label">GSTR-2B</div>
      <div class="drop-hint">Excel files (.xlsx)</div>
      <div class="drop-count" id="count-r2b">No files</div>
      <input type="file" multiple accept=".xlsx,.xls,.zip" data-zone="r2b" onchange="updateZone('r2b',this)">
    </div>
    <div class="drop-zone" id="zone-r2a">
      <div class="drop-icon">📊</div><div class="drop-label">GSTR-2A</div>
      <div class="drop-hint">ZIP or Excel</div>
      <div class="drop-count" id="count-r2a">No files</div>
      <input type="file" multiple accept=".zip,.xlsx" data-zone="r2a" onchange="updateZone('r2a',this)">
    </div>
    <div class="drop-zone" id="zone-r3b">
      <div class="drop-icon">📄</div><div class="drop-label">GSTR-3B PDF</div>
      <div class="drop-hint">PDF files (auto-extract)</div>
      <div class="drop-count" id="count-r3b">No files</div>
      <input type="file" multiple accept=".pdf" data-zone="r3b" onchange="updateZone('r3b',this)">
    </div>
  </div>
  <div class="name-lookup">
    <div class="name-lookup-title">Customer Name Lookup (Optional)</div>
    <div class="drop-zone" id="zone-cust" style="min-height:80px;flex-direction:row;justify-content:flex-start;gap:1rem;padding:1rem">
      <div class="drop-icon" style="font-size:1.5rem">👥</div>
      <div>
        <div class="drop-label" style="text-align:left">customer_names.xlsx</div>
        <div class="name-info" style="margin-top:.25rem">
          Any Excel with <strong>GSTIN</strong> + <strong>Company Name</strong> columns.<br>
          Auto-fills receiver names in B2B, CDNR sheets.
        </div>
        <div class="drop-count" id="count-cust" style="margin-top:.4rem">No file</div>
      </div>
      <input type="file" accept=".xlsx,.xls" data-zone="cust" onchange="updateZone('cust',this)"
             style="position:absolute;inset:0;opacity:0;cursor:pointer">
    </div>
  </div>
</div>

<div class="card">
  <button type="submit" class="btn-submit" id="submit-btn">Generate Reconciliation →</button>
</div>
</form>

<!-- Progress -->
<div class="card" id="progress-section">
  <div class="card-title">Processing
    <span class="status-badge status-processing pulse" id="status-badge">Running</span>
  </div>
  <div class="progress-bar-wrap"><div class="progress-bar-fill" id="progress-bar"></div></div>
  <div class="log-box" id="log-box"></div>
</div>

<!-- Downloads -->
<div class="card" id="downloads-section">
  <div class="card-title">Downloads Ready</div>
  <div class="dl-grid" id="dl-grid"></div>
  <div class="auto-download-section" id="auto-download-section" style="display:none">
    <div class="auto-download-title">📥 Auto-Download</div>
    <p style="color:var(--muted);font-size:.75rem;margin-bottom:.5rem">All files will download automatically</p>
    <button class="btn-auto-dl" onclick="downloadAll()">Download All Files</button>
  </div>
  <p style="color:var(--muted);font-size:.75rem;margin-top:1rem;font-family:var(--mono)">
    ⏳ Files are available for 2 hours after generation.
  </p>
</div>

</div><!-- /container -->

<script>
// ── File zones ───────────────────────────────────────────────────
const zoneFiles = {r1:[],r2b:[],r2a:[],r3b:[],cust:[]};
let currentJobId = '';
let currentFiles = [];

function updateZone(zone, input) {
  const files = Array.from(input.files);
  zoneFiles[zone] = files;
  const countEl = document.getElementById('count-'+zone);
  const zoneEl  = document.getElementById('zone-'+zone);
  countEl.textContent = files.length ? files.length + ' file' + (files.length>1?'s':'') + ' selected' : 'No files';
  zoneEl.classList.toggle('has-files', files.length > 0);
}

document.querySelectorAll('.drop-zone').forEach(zone => {
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('drag-over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
  zone.addEventListener('drop', e => {
    e.preventDefault(); zone.classList.remove('drag-over');
    const input = zone.querySelector('input[type=file]');
    if (!input) return;
    const dt = new DataTransfer();
    [...e.dataTransfer.files].forEach(f => dt.items.add(f));
    input.files = dt.files;
    updateZone(input.dataset.zone, input);
  });
});

// ── Submit ───────────────────────────────────────────────────────
document.getElementById('main-form').addEventListener('submit', async e => {
  e.preventDefault();
  const gstin  = document.getElementById('gstin').value.trim().toUpperCase();
  const cname  = document.getElementById('client_name').value.trim();
  const fy     = document.getElementById('fy').value.trim();
  if (!gstin || gstin.length !== 15) { alert('Please enter a valid 15-character GSTIN'); return; }
  if (!cname) { alert('Please enter company name'); return; }
  if (Object.values(zoneFiles).every(a => a.length === 0)) { alert('Please upload at least one file'); return; }

  const fd = new FormData();
  fd.append('gstin', gstin);
  fd.append('client_name', cname);
  fd.append('fy', fy);
  for (const [zone, files] of Object.entries(zoneFiles))
    files.forEach(f => fd.append('files_'+zone, f));

  document.getElementById('submit-btn').disabled = true;
  document.getElementById('submit-btn').textContent = 'Uploading...';
  document.getElementById('progress-section').style.display = 'block';
  document.getElementById('downloads-section').style.display = 'none';
  document.getElementById('dl-grid').innerHTML = '';
  document.getElementById('log-box').innerHTML = '';
  document.getElementById('progress-bar').style.width = '0%';
  document.getElementById('auto-download-section').style.display = 'none';

  try {
    const res  = await fetch('/api/upload', {method:'POST', body:fd});
    const data = await res.json();
    if (!data.job_id) throw new Error(data.error || 'Upload failed');
    currentJobId = data.job_id;
    addLog('info', 'Files uploaded. Starting reconciliation...');
    document.getElementById('submit-btn').textContent = 'Processing...';
    pollJob(data.job_id);
  } catch(err) {
    addLog('err', 'Upload error: ' + err.message);
    setStatus('error', 'Upload Failed');
    document.getElementById('submit-btn').disabled = false;
    document.getElementById('submit-btn').textContent = 'Generate Reconciliation →';
  }
});

// ── Polling ──────────────────────────────────────────────────────
async function pollJob(jobId) {
  try {
    const res  = await fetch('/api/job/'+jobId);
    const data = await res.json();
    if (data.logs) data.logs.forEach(l => addLog(l.type, l.msg));
    if (data.progress !== undefined)
      document.getElementById('progress-bar').style.width = data.progress + '%';
    if (data.status === 'done') {
      setStatus('done','Complete');
      document.getElementById('progress-bar').style.width='100%';
      document.getElementById('submit-btn').disabled = false;
      document.getElementById('submit-btn').textContent = 'Generate Reconciliation →';
      showDownloads(jobId, data.files);
      // Auto-download after 2 seconds
      setTimeout(() => autoDownloadAll(data.files), 2000);
      return;
    }
    if (data.status === 'error') {
      addLog('err','Error: '+(data.error||'Unknown error'));
      setStatus('error','Failed');
      document.getElementById('submit-btn').disabled = false;
      document.getElementById('submit-btn').textContent = 'Generate Reconciliation →';
      return;
    }
    setTimeout(() => pollJob(jobId), 1500);
  } catch(e) {
    setTimeout(() => pollJob(jobId), 3000);
  }
}

function addLog(type, msg) {
  const box = document.getElementById('log-box');
  const line = document.createElement('div');
  line.className = type;
  line.textContent = '[' + new Date().toLocaleTimeString() + '] ' + msg;
  box.appendChild(line);
  box.scrollTop = box.scrollHeight;
}
function setStatus(type, label) {
  const b = document.getElementById('status-badge');
  b.className = 'status-badge status-'+type;
  b.textContent = label;
  if (type !== 'processing') b.classList.remove('pulse');
}
function showDownloads(jobId, files) {
  currentFiles = files;
  document.getElementById('downloads-section').style.display = 'block';
  document.getElementById('auto-download-section').style.display = 'block';
  const grid = document.getElementById('dl-grid');
  const icons = {'ANNUAL':'📊','GSTR3BR1':'📋','GSTR3BR2A':'📈','GSTR1_FULL':'📑','B2B':'🏢','SUMMARY':'📋'};
  files.forEach(f => {
    const icon = Object.entries(icons).find(([k])=>f.name.includes(k))?.[1] || '📁';
    const card = document.createElement('div');
    card.className = 'dl-card';
    card.innerHTML = `<div style="font-size:1.8rem">${icon}</div>
      <div class="dl-name">${f.name}</div>
      <div class="dl-size">${f.size}</div>
      <a href="/api/download/${jobId}/${encodeURIComponent(f.name)}" class="btn-dl" download id="dl-${f.name}">Download ↓</a>`;
    grid.appendChild(card);
  });
}

// ── Auto Download ────────────────────────────────────────────────
function autoDownloadAll(files) {
  files.forEach((f, i) => {
    setTimeout(() => {
      const link = document.getElementById('dl-' + f.name);
      if (link) link.click();
    }, i * 500);
  });
}

function downloadAll() {
  if (currentFiles.length > 0) {
    autoDownloadAll(currentFiles);
  }
}
</script>
</body>
</html>"""

# ── Routes ────────────────────────────────────────────────────────
@app.route("/", methods=["GET"])
def index():
    return render_template_string(HTML)

@app.route("/api/upload", methods=["POST"])
@rate_limit(limit=20, window=60)
def api_upload():
    _cleanup_old_jobs()

    gstin       = request.form.get("gstin", "").strip().upper()
    client_name = request.form.get("client_name", "").strip()
    fy          = request.form.get("fy", "2025-26").strip()

    if not gstin or len(gstin) != 15:
        return jsonify(error="Invalid GSTIN"), 400
    if not client_name:
        return jsonify(error="Client name required"), 400

    job_id  = str(uuid.uuid4())[:8]
    job_dir = UPLOAD_DIR / job_id
    out_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Save uploaded files
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
            "status":      "queued",
            "progress":    0,
            "logs":        [],
            "files":       [],
            "error":       None,
            "gstin":       gstin,
            "client_name": client_name,
            "fy":          fy,
            "job_dir":     str(job_dir),
            "out_dir":     str(out_dir),
            "saved":       saved,
            "is_full":     True,
        }

    threading.Thread(target=run_reconciliation, args=(job_id,), daemon=True).start()
    return jsonify(job_id=job_id, is_full=True)

@app.route("/api/job/<job_id>", methods=["GET"])
@rate_limit(limit=120, window=60)
def api_job(job_id):
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
        is_full  = job.get("is_full", True),
    )

@app.route("/api/download/<job_id>/<filename>", methods=["GET"])
@rate_limit(limit=30, window=60)
def api_download(job_id, filename):
    # Strict filename check — no path traversal possible
    if not re.match(r'^[\w\-. ()]+\.(xlsx|pdf|zip)$', filename):
        abort(400)
    fpath = OUTPUT_DIR / job_id / filename
    if not fpath.exists() or not fpath.is_file():
        abort(404)
    return send_file(str(fpath), as_attachment=True, download_name=filename)

# ── GSTR-3B PDF Extraction ────────────────────────────────────────
def extract_3b_pdf(pdf_path):
    """
    Robust GSTR-3B PDF extraction — line-by-line scan.
    Handles portal PDF quirks: 'L0', 'I0', split rows, stray chars.
    Returns ALL fields from Tables 3.1, 3.1(c), 4, 5.1, 6.1.
    """
    result = {
        # Meta
        "gstin":"","legal_name":"","trade_name":"","period":"","year":"","arn":"","arn_date":"",
        # 3.1(a) Outward taxable
        "taxable":0.,"o_igst":0.,"o_cgst":0.,"o_sgst":0.,"o_cess":0.,
        # 3.1(b) Zero rated
        "zero_taxable":0.,"zero_igst":0.,
        # 3.1(c) Nil / Exempt
        "nil_exempt":0.,
        # 3.1(d) RCM inward
        "rcm_taxable":0.,"rcm_igst":0.,"rcm_cgst":0.,"rcm_sgst":0.,
        # 3.1(e) Non-GST
        "non_gst":0.,
        # 4A ITC available
        "itc_import_goods":0.,"itc_import_svc":0.,"itc_rcm":0.,"itc_isd":0.,
        "itc_igst":0.,"itc_cgst":0.,"itc_sgst":0.,"itc_cess":0.,
        # 4B ITC reversed
        "rev_igst":0.,"rev_cgst":0.,"rev_sgst":0.,
        # 4C Net ITC
        "net_itc_igst":0.,"net_itc_cgst":0.,"net_itc_sgst":0.,
        # 5.1
        "interest_igst":0.,"interest_cgst":0.,"interest_sgst":0.,
        "late_fee_cgst":0.,"late_fee_sgst":0.,
        # 6.1 Tax paid
        "tax_paid_igst":0.,"tax_paid_cgst":0.,"tax_paid_sgst":0.,
    }
    if not pdf_path.exists():
        return result
    import re
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as pdf2:
            text = "\n".join(p.extract_text() or "" for p in pdf2.pages)
    except ImportError:
        try:
            import PyPDF2
            with open(str(pdf_path),"rb") as f2:
                r2 = PyPDF2.PdfReader(f2)
                text = "\n".join(p.extract_text() or "" for p in r2.pages)
        except: pass
    except Exception: pass
    if not text:
        return result

    def _n(s):
        """Convert string to float, handling L0/I0 portal artifacts."""
        try:
            return float(re.sub(r"[^\d.\-]","",str(s).replace("L","").replace("I","").replace(",","")))
        except: return 0.0

    def nums_on_line(line):
        """Extract all decimal numbers from a single PDF line."""
        clean = re.sub(r"[LI](\d)", r"\1", line)
        return [_n(n) for n in re.findall(r"-?\d[\d,]*\.\d*", clean)]

    lines = text.split("\n")
    in_section_4  = False
    in_section_6  = False
    past_51_header = False

    for i, line in enumerate(lines):
        lo   = line.lower().strip()
        nums = nums_on_line(line)

        # -- Meta fields --------------------------------------------
        if "gstin of the supplier" in lo:
            parts = re.split(r"supplier\s*", line, flags=re.I)
            if len(parts) > 1: result["gstin"] = parts[-1].strip()
        elif "2(a)." in line and "legal name" in lo:
            result["legal_name"] = re.sub(r"2\(a\)\..*?person\s*","",line,flags=re.I).strip()
        elif "2(b)." in line and "trade name" in lo:
            result["trade_name"] = re.sub(r"2\(b\)\.\s*Trade name,?\s*if any\s*","",line,flags=re.I).strip()
        elif re.match(r"Year\s+\d{4}", line):
            result["year"] = line.replace("Year","").strip()
        elif re.match(r"Period\s+[A-Za-z]", line) and "supply" not in lo:
            result["period"] = line.replace("Period","").strip()
        elif "2(c)." in line and "arn" in lo and "nil" not in lo and "exempt" not in lo:
            result["arn"] = re.sub(r"2\(c\)\.\s*ARN\s*","",line,flags=re.I).strip()
        elif "2(d)." in line and "date of arn" in lo:
            result["arn_date"] = re.sub(r"2\(d\)\.\s*Date of ARN\s*","",line,flags=re.I).strip()

        # -- Section flags ------------------------------------------
        elif re.search(r"4\.?\s*eligible itc|table.*4|^4\.\s", lo):
            in_section_4 = True; in_section_6 = False
        elif re.search(r"6\.?1.*payment of tax|payment of tax", lo):
            in_section_6 = True; in_section_4 = False
        elif re.search(r"5\.?1.*interest.*late fee|interest.*late fee.*previous", lo):
            past_51_header = True

        # -- 3.1(a) Outward taxable ---------------------------------
        elif re.search(r"\(a\).*outward taxable", lo) and "(b)" not in lo:
            combined = line
            if len(nums) < 4 and i+1 < len(lines):
                combined = line + " " + lines[i+1]
                nums = nums_on_line(combined)
            if len(nums) >= 4:
                result["taxable"] = nums[0]
                result["o_igst"]  = nums[1]
                result["o_cgst"]  = nums[2]
                result["o_sgst"]  = nums[3]
                if len(nums) >= 5: result["o_cess"] = nums[4]

        # -- 3.1(b) Zero rated --------------------------------------
        elif re.search(r"\(b\).*zero rated", lo):
            if len(nums) >= 1: result["zero_taxable"] = nums[0]
            if len(nums) >= 2: result["zero_igst"]    = nums[1]

        # -- 3.1(c) Nil / Exempt ------------------------------------
        elif re.search(r"^\(c[\s\)]", lo) and re.search(r"nil|exempt", lo):
            if nums: result["nil_exempt"] = nums[0]

        # -- 3.1(d) Inward supplies liable to RCM -------------------
        elif re.search(r"\(d\).*inward.*reverse charge|\(d\).*reverse charge.*inward", lo):
            if len(nums) >= 4:
                result["rcm_taxable"] = nums[0]
                result["rcm_igst"]    = nums[1]
                result["rcm_cgst"]    = nums[2]
                result["rcm_sgst"]    = nums[3]

        # -- 3.1(e) Non-GST -----------------------------------------
        elif re.search(r"\(e\).*non.?gst", lo):
            if nums: result["non_gst"] = nums[0]

        # -- Table 4: ITC -------------------------------------------
        elif re.search(r"\(1\).*import of goods", lo):
            if len(nums) >= 1: result["itc_import_goods"] = nums[0]
        elif re.search(r"\(2\).*import of services", lo):
            if len(nums) >= 1: result["itc_import_svc"] = nums[0]
        elif re.search(r"\(3\).*reverse charge.*other than.*1.*2|\(3\).*inward.*reverse charge.*other", lo):
            if len(nums) >= 1: result["itc_rcm"] = nums[0]
        elif re.search(r"\(4\).*isd|\(4\).*inward.*isd", lo):
            if len(nums) >= 1: result["itc_isd"] = nums[0]
        elif re.search(r"\(5\).*all other itc", lo):
            if len(nums) >= 3:
                result["itc_igst"] = nums[0]
                result["itc_cgst"] = nums[1]
                result["itc_sgst"] = nums[2]
                if len(nums) >= 4: result["itc_cess"] = nums[3]

        elif re.search(r"\(1\).*rules 38.*42|\(1\).*as per rules", lo):
            if len(nums) >= 3:
                result["rev_igst"] = nums[0]
                result["rev_cgst"] = nums[1]
                result["rev_sgst"] = nums[2]

        elif re.search(r"net itc available|^c\..*net itc", lo):
            if len(nums) >= 3:
                result["net_itc_igst"] = nums[0]
                result["net_itc_cgst"] = nums[1]
                result["net_itc_sgst"] = nums[2]

        # -- 5.1 Interest Paid --------------------------------------
        elif re.search(r"^interest paid|interest paid\s", lo):
            if len(nums) >= 3:
                result["interest_igst"] = nums[0]
                result["interest_cgst"] = nums[1]
                result["interest_sgst"] = nums[2]
            elif len(nums) == 2:
                result["interest_cgst"] = nums[0]
                result["interest_sgst"] = nums[1]

        # -- 5.1 Late fee -------------------------------------------
        elif re.search(r"late fee", lo) and "5.1" not in lo and "interest and late" not in lo:
            late_nums = [n for n in nums if n > 0]
            if len(late_nums) >= 2:
                result["late_fee_cgst"] = late_nums[0]
                result["late_fee_sgst"] = late_nums[1]
            elif len(late_nums) == 1:
                result["late_fee_cgst"] = late_nums[0]

        # -- 6.1 Tax paid rows --------------------------------------
        elif in_section_6:
            if re.match(r"central\s*(tax)?", lo) and len(nums) >= 3:
                tp = nums[0]
                candidates = [n for n in nums[2:] if 0 < n < tp*2]
                if candidates:
                    result["tax_paid_cgst"] = candidates[-2] if len(candidates)>=2 else candidates[0]
            elif re.match(r"state|ut\s*(tax)?", lo) and len(nums) >= 3:
                tp = nums[0]
                candidates = [n for n in nums[2:] if 0 < n < tp*2]
                if candidates:
                    result["tax_paid_sgst"] = candidates[-2] if len(candidates)>=2 else candidates[0]
            elif re.match(r"integrated\s*(tax)?", lo) and len(nums) >= 3:
                tp = nums[0]
                candidates = [n for n in nums[2:] if 0 < n < tp*2]
                if candidates:
                    result["tax_paid_igst"] = candidates[-2] if len(candidates)>=2 else candidates[0]

    # Fallback: derive ITC from net if 4A(5) was zero
    if result["itc_cgst"] == 0 and result["net_itc_cgst"] > 0:
        result["itc_cgst"] = result["net_itc_cgst"]
        result["itc_sgst"] = result["net_itc_sgst"]
        result["itc_igst"] = result["net_itc_igst"]

    return result

# ── Background worker ─────────────────────────────────────────────
def run_reconciliation(job_id):
    def log(msg, t="info"):
        with jobs_lock:
            jobs[job_id]["logs"].append({"type": t, "msg": msg})
    def set_progress(p):
        with jobs_lock:
            jobs[job_id]["progress"] = p

    try:
        import zipfile as _zf
        from openpyxl import Workbook, load_workbook
        from openpyxl.styles import PatternFill, Font, Border, Side, Alignment
        from openpyxl.utils import get_column_letter

        job         = jobs[job_id]
        gstin       = job["gstin"]
        client_name = job["client_name"]
        fy          = job["fy"]
        job_dir     = Path(job["job_dir"])
        out_dir     = Path(job["out_dir"])
        saved       = job["saved"]

        log(f"Starting reconciliation for {client_name} ({gstin}) FY {fy}")
        log("⭐ FULL VERSION — All features enabled")
        set_progress(5)

        # Month maps
        MONTHS_MAP = {
            "april":"April","may":"May","june":"June","july":"July","august":"August",
            "september":"September","october":"October","november":"November",
            "december":"December","january":"January","february":"February","march":"March",
            "04":"April","05":"May","06":"June","07":"July","08":"August",
            "09":"September","10":"October","11":"November","12":"December",
            "01":"January","02":"February","03":"March",
        }
        start_yr = int(fy.split("-")[0]); end_yr = start_yr + 1
        FY_MONTHS = {
            "April":str(start_yr),"May":str(start_yr),"June":str(start_yr),
            "July":str(start_yr),"August":str(start_yr),"September":str(start_yr),
            "October":str(start_yr),"November":str(start_yr),"December":str(start_yr),
            "January":str(end_yr),"February":str(end_yr),"March":str(end_yr),
        }

        def detect_month_from_zip(zpath):
            name = Path(zpath).stem.lower()
            for part in re.split(r'[_\-\s]', name):
                if part in MONTHS_MAP:
                    mon = MONTHS_MAP[part]
                    return mon, FY_MONTHS.get(mon, str(start_yr))
            try:
                with _zf.ZipFile(zpath) as z:
                    for jn in z.namelist():
                        if jn.endswith(".json"):
                            with z.open(jn) as jf:
                                d = __import__("json").load(jf)
                                fp = re.sub(r'[^0-9]','',d.get("fp",""))
                                if len(fp) == 6:
                                    mon = MONTHS_MAP.get(fp[:2])
                                    if mon: return mon, fp[2:]
            except: pass
            return None, None

        # Rename uploaded files to standard names
        log("Processing GSTR-1 files...")
        for fpath in saved["r1"]:
            mon, yr = detect_month_from_zip(fpath)
            if mon:
                dest = job_dir / f"GSTR1_{mon}_{yr}.zip"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except OSError: shutil.copy2(fpath, str(dest))
                log(f"  GSTR-1: {mon} {yr}")

        set_progress(15)
        log("Processing GSTR-2B files...")
        for fpath in saved["r2b"]:
            name = Path(fpath).stem.lower()
            for part in re.split(r'[_\-\s]', name):
                if part in MONTHS_MAP:
                    mon = MONTHS_MAP[part]; yr = FY_MONTHS.get(mon, str(start_yr))
                    dest = job_dir / f"GSTR2B_{mon}_{yr}.xlsx"
                    if not dest.exists():
                        try: Path(fpath).rename(dest)
                        except OSError: shutil.copy2(fpath, str(dest))
                    log(f"  GSTR-2B: {mon} {yr}"); break

        log("Processing GSTR-2A files...")
        for fpath in saved["r2a"]:
            name = Path(fpath).stem.lower(); ext = Path(fpath).suffix.lower()
            for part in re.split(r'[_\-\s]', name):
                if part in MONTHS_MAP:
                    mon = MONTHS_MAP[part]; yr = FY_MONTHS.get(mon, str(start_yr))
                    dest = job_dir / f"GSTR2A_{mon}_{yr}{ext}"
                    if not dest.exists():
                        try: Path(fpath).rename(dest)
                        except OSError: shutil.copy2(fpath, str(dest))
                    log(f"  GSTR-2A: {mon} {yr}"); break

        # Process GSTR-3B PDFs with extraction
        gstr3b_data = {}
        log("Processing GSTR-3B PDFs...")
        for fpath in saved["r3b"]:
            name = Path(fpath).stem.lower()
            mon = None; yr = None
            for part in re.split(r'[_\-\s]', name):
                if part in MONTHS_MAP:
                    mon = MONTHS_MAP[part]; yr = FY_MONTHS.get(mon, str(start_yr))
                    break
            if mon:
                dest = job_dir / f"GSTR3B_{mon}_{yr}.pdf"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except OSError: shutil.copy2(fpath, str(dest))
                # Extract data from PDF
                log(f"  Extracting GSTR-3B: {mon} {yr}...")
                pdf_data = extract_3b_pdf(dest)
                gstr3b_data[f"{mon}_{yr}"] = pdf_data
                log(f"    ✓ Taxable: {pdf_data['taxable']:,.2f}, ITC: {pdf_data['net_itc_cgst']+pdf_data['net_itc_sgst']+pdf_data['net_itc_igst']:,.2f}")

        # Process customer names file
        if saved["cust"]:
            for fpath in saved["cust"]:
                dest = job_dir / "customer_names.xlsx"
                if not dest.exists():
                    try: Path(fpath).rename(dest)
                    except OSError: shutil.copy2(fpath, str(dest))
                log("  Customer names Excel loaded"); break

        set_progress(25)
        log("File preparation complete. Running reconciliation engine...")

        # ── Load the engine (works with both plain and PyArmor-protected scripts) ──
        import importlib.util, logging as _logging
        suite_path = Path(__file__).parent / "gst_suite_final.py"
        if not suite_path.exists():
            raise FileNotFoundError(
                "gst_suite_final.py not found. Place it in the same folder as app.py.")

        spec = importlib.util.spec_from_file_location("gst_suite", str(suite_path))
        gst  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gst)

        # Patch FY/MONTHS if not 2025-26
        gst.FY_LABEL = fy
        gst.MONTHS = [
            ("April","04",str(start_yr)),("May","05",str(start_yr)),
            ("June","06",str(start_yr)),("July","07",str(start_yr)),
            ("August","08",str(start_yr)),("September","09",str(start_yr)),
            ("October","10",str(start_yr)),("November","11",str(start_yr)),
            ("December","12",str(start_yr)),("January","01",str(end_yr)),
            ("February","02",str(end_yr)),("March","03",str(end_yr)),
        ]

        _log = _logging.getLogger(f"gst_web_{job_id}")
        _log.setLevel(_logging.DEBUG)
        class WebLogHandler(_logging.Handler):
            def emit(self, record):
                t = "err" if record.levelno >= _logging.WARNING else "info"
                log(self.format(record), t)
        _log.addHandler(WebLogHandler())

        set_progress(30)
        log("Running write_annual_reconciliation...")
        gst.write_annual_reconciliation(str(job_dir), client_name, gstin, _log)
        set_progress(70)

        # GSTR-1 detailed extraction
        extract_path = Path(__file__).parent / "gstr1_extract.py"
        if extract_path.exists():
            log("Running GSTR-1 detailed extraction...")
            try:
                spec2 = importlib.util.spec_from_file_location("gstr1_extract", str(extract_path))
                gstr1 = importlib.util.module_from_spec(spec2)
                spec2.loader.exec_module(gstr1)
                out_xl = job_dir / f"GSTR1_FULL_DETAIL_{client_name.replace(' ','_')}.xlsx"
                gstr1.extract_gstr1_to_excel(str(job_dir), str(out_xl))
                log(f"  GSTR-1 detail: {out_xl.name}")
            except Exception as e:
                log(f"  GSTR-1 extraction warning: {e}", "warn")

        # Create GSTR-3B Summary Excel if PDFs were extracted
        if gstr3b_data:
            log("Creating GSTR-3B Summary sheet...")
            try:
                wb3b = Workbook()
                wb3b.remove(wb3b.active)
                ws = wb3b.create_sheet("GSTR3B_Summary")
                
                # Headers
                headers = ["Month", "Year", "Taxable Value", "IGST", "CGST", "SGST", 
                          "Net ITC IGST", "Net ITC CGST", "Net ITC SGST", 
                          "Tax Paid IGST", "Tax Paid CGST", "Tax Paid SGST"]
                for col, h in enumerate(headers, 1):
                    c = ws.cell(row=1, column=col, value=h)
                    c.font = Font(bold=True)
                
                # Data rows
                row = 2
                for key, data in sorted(gstr3b_data.items()):
                    mon, yr = key.split("_")
                    ws.cell(row=row, column=1, value=mon)
                    ws.cell(row=row, column=2, value=yr)
                    ws.cell(row=row, column=3, value=data.get("taxable", 0))
                    ws.cell(row=row, column=4, value=data.get("o_igst", 0))
                    ws.cell(row=row, column=5, value=data.get("o_cgst", 0))
                    ws.cell(row=row, column=6, value=data.get("o_sgst", 0))
                    ws.cell(row=row, column=7, value=data.get("net_itc_igst", 0))
                    ws.cell(row=row, column=8, value=data.get("net_itc_cgst", 0))
                    ws.cell(row=row, column=9, value=data.get("net_itc_sgst", 0))
                    ws.cell(row=row, column=10, value=data.get("tax_paid_igst", 0))
                    ws.cell(row=row, column=11, value=data.get("tax_paid_cgst", 0))
                    ws.cell(row=row, column=12, value=data.get("tax_paid_sgst", 0))
                    row += 1
                
                out_3b = job_dir / f"GSTR3B_SUMMARY_{client_name.replace(' ','_')}.xlsx"
                wb3b.save(str(out_3b))
                log(f"  GSTR-3B Summary: {out_3b.name}")
            except Exception as e:
                log(f"  GSTR-3B summary warning: {e}", "warn")

        set_progress(80)
        log("Collecting output files...")

        output_files = []
        for fp in Path(job_dir).glob("*.xlsx"):
            dest_fp = out_dir / fp.name
            shutil.copy2(str(fp), str(dest_fp))
            size_kb = dest_fp.stat().st_size // 1024
            output_files.append({"name": fp.name, "size": f"{size_kb} KB"})
            log(f"  Output: {fp.name} ({size_kb} KB)", "ok")

        if not output_files:
            raise RuntimeError("No output files generated. Check uploaded files.")

        set_progress(100)
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


# ── Startup ───────────────────────────────────────────────────────
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print(f"\n  ============================================================")
    print(f"   GST Reconciliation Web App — PROTECTED FULL VERSION")
    print(f"  ============================================================")
    print(f"   Upload dir  : {UPLOAD_DIR}")
    print(f"   Output dir  : {OUTPUT_DIR}")
    print(f"   Suite file  : {Path(__file__).parent / 'gst_suite_final.py'}")
    print(f"   Extract file: {Path(__file__).parent / 'gstr1_extract.py'}")
    print(f"   File TTL    : {JOB_TTL_S // 3600}h (auto-deleted after)")
    print(f"   Features    : GSTR-3B PDF extraction, Auto-download")
    print(f"\n   Open your browser:  http://localhost:{port}")
    print(f"   Press Ctrl+C to stop")
    print(f"  ============================================================\n")

    import socket as _sock
    for _p in [port, 5001, 5002, 8080]:
        try:
            s = _sock.socket(); s.bind(("", _p)); s.close()
            port = _p; break
        except OSError: continue

    print(f"  Server starting on port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)
