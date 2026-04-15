# GST + IT Reconciliation Portal — Render.com Deployment Guide

## Files in this project

| File | Purpose |
|---|---|
| `app.py` | Main Flask web app (v6) |
| `gst_suite_final.py` | GST reconciliation engine |
| `gstr1_extract.py` | GSTR-1 detail extractor |
| `it_suite.py` | Income Tax portal automation |
| `it_recon_engine.py` | IT reconciliation engine |
| `requirements.txt` | Python dependencies |
| `Procfile` | Gunicorn start command |
| `render.yaml` | Render.com auto-deploy config |
| `runtime.txt` | Python 3.11 pin |

---

## Deploy to Render.com (Free/Paid)

### Option A — render.yaml (Recommended, one-click)

1. Push all files to a **GitHub repository**
2. Go to [render.com](https://render.com) → **New** → **Web Service**
3. Connect your GitHub repo
4. Render will auto-detect `render.yaml` and configure everything
5. Click **Create Web Service** — done!

> ⚠️ **Important**: The `buildCommand` in `render.yaml` installs `chromium-browser` and `libqpdf` as system packages. This is required for Selenium and PDF unlock to work. Render's **standard plan** (512 MB RAM) is the minimum; use **pro** for bulk GST/IT jobs with many clients.

### Option B — Manual setup on Render

1. Push to GitHub
2. **New Web Service** → connect repo
3. Set:
   - **Build Command:**
     ```
     apt-get update && apt-get install -y chromium-browser chromium-chromedriver libqpdf-dev libqpdf29 --no-install-recommends && pip install -r requirements.txt
     ```
   - **Start Command:**
     ```
     gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 300 --worker-class gthread
     ```
4. **Environment Variables** → Add:
   | Key | Value |
   |---|---|
   | `RENDER` | `true` |
   | `PYTHONUNBUFFERED` | `true` |
   | `CHROME_BIN` | `/usr/bin/chromium-browser` |
   | `CHROMEDRIVER_PATH` | `/usr/bin/chromedriver` |

---

## Run Locally (Windows / Mac / Linux)

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Put all 5 .py files in the same folder
# 3. Run
python app.py

# App opens at http://localhost:5000
```

> On Windows, Chrome/Edge will open normally (not headless). On Render, headless mode is auto-detected via the `RENDER` environment variable.

---

## What was fixed for online deployment

| Issue | Fix |
|---|---|
| `it_suite.py` — Chrome opened without `--headless` on server | Added `_IS_SERVER` detection; adds `--headless=new`, `--disable-gpu`, `--window-size` when `RENDER` env var is set |
| Chrome binary not found on Render | Both `app.py` and `it_suite.py` now detect system `chromium-browser` path via `shutil.which()` |
| `pikepdf` needs `libqpdf` C library | Added to `buildCommand` in `render.yaml` |
| No `gunicorn` → app crashes on Render | Added `gunicorn` to `requirements.txt` + `Procfile` |
| Long Selenium jobs timing out | `--timeout 300` set on gunicorn |

---

## Environment Variables Reference

| Variable | Required | Description |
|---|---|---|
| `PORT` | Auto-set by Render | Port to bind to (default 5000 locally) |
| `RENDER` | Set to `true` | Enables headless Chrome + system chromium |
| `HEADLESS` | Optional override | Set to `true` to force headless locally |
| `PYTHONUNBUFFERED` | Recommended | Real-time logs in Render dashboard |

---

## Troubleshooting

**Chrome fails to start on Render:**
- Check that `buildCommand` ran `apt-get install -y chromium-browser chromium-chromedriver`
- Verify `RENDER=true` env var is set

**pikepdf import error:**
- Ensure `libqpdf-dev` and `libqpdf29` were installed in the build step

**Job timeout:**
- Increase gunicorn `--timeout` (default 300s = 5 min)
- Upgrade to a larger Render plan for bulk jobs
