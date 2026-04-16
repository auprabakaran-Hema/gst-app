# GST + IT Reconciliation Portal — Render.com Deployment Guide

## Files in this project

| File | Purpose |
|---|---|
| `app.py` | Main Flask web app |
| `gst_suite_final.py` | GST reconciliation engine |
| `gstr1_extract.py` | GSTR-1 detail extractor |
| `it_suite.py` | Income Tax portal automation |
| `it_recon_engine.py` | IT reconciliation engine |
| `requirements.txt` | Python dependencies |
| `Procfile` | Gunicorn start command |
| `render.yaml` | Render.com auto-deploy config |
| `runtime.txt` | Python 3.11 pin |

---

## Deploy to Render.com

### Option A — render.yaml (Recommended)

1. Push all files to a **GitHub repository**
2. Go to [render.com](https://render.com) → **New** → **Web Service**
3. Connect your GitHub repo
4. Render will auto-detect `render.yaml` and configure everything
5. Click **Create Web Service** — done!

### Option B — Manual setup on Render

1. Push to GitHub
2. **New Web Service** → connect repo
3. Set **Environment** → **Python 3**
4. Set **Build Command:**
   ```
   apt-get update && apt-get install -y chromium-browser chromium-chromedriver libqpdf-dev libqpdf29 --no-install-recommends && pip install --upgrade pip setuptools wheel && pip install -r requirements.txt
   ```
5. Set **Start Command:**
   ```
   gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --threads 4 --timeout 300 --worker-class gthread
   ```
6. Under **Environment Variables** → Add:

   | Key | Value |
   |---|---|
   | `RENDER` | `true` |
   | `PYTHONUNBUFFERED` | `true` |
   | `CHROME_BIN` | `/usr/bin/chromium-browser` |
   | `CHROMEDRIVER_PATH` | `/usr/bin/chromedriver` |

7. Under **Settings → Python Version** → set `3.11.9`

---

## ⚠️ Critical: Python Version Must Be 3.11

Render's default runtime may be Python 3.12+ or 3.14, where `pikepdf` has **no pre-built wheel** and the source build fails. Always ensure:
- `runtime.txt` contains exactly `python-3.11.9`
- In the Render dashboard under your service → **Settings** → confirm Python version is 3.11

---

## Run Locally

```bash
pip install -r requirements.txt
python app.py
# App opens at http://localhost:5000
```

---

## Troubleshooting

**`pikepdf` build fails (qpdf/Constants.h not found):**
- You are on Python 3.12+ or 3.14 — switch to Python 3.11
- Ensure `libqpdf-dev` is installed *before* pip runs (it is in the buildCommand)

**Chrome fails to start on Render:**
- Confirm `buildCommand` installed `chromium-browser` and `chromium-chromedriver`
- Confirm `RENDER=true` env var is set

**Job timeout:**
- Increase gunicorn `--timeout` value
- Upgrade to a larger Render plan for bulk jobs
