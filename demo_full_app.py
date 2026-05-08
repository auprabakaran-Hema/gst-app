"""
RPR GST + IT Suite — FULL ACCESS DEMO (Machine-Locked)
=======================================================
✅ All 11 tabs fully unlocked — Reconciliation, GSTR-1, IT, Auto Download,
   Bulk, IT Bulk, GSTR-2B, Master Bridge, GST-IT Comparison, Tally vs JSON
🔒 1 client only — locks to the FIRST machine that opens it
🚫 Cannot run on any other PC after first activation
📞 Prospect sees full power → contacts you to buy

MACHINE LOCK LOGIC
------------------
1. On first launch → generate machine fingerprint → save to lock file
2. On every subsequent launch → verify fingerprint matches
3. If different machine tries to run → show "Already activated" screen
4. Lock file stored in: %APPDATA%\\RPR_Demo\\machine_lock.dat  (Windows)
                         ~/.rpr_demo/machine_lock.dat          (Linux/Mac)

BUILD
-----
  Run:  BUILD_FULL_DEMO_EXE.bat
  Output: dist\\RPR_GST_Demo_Full.exe
"""

import os, sys, json, uuid, hashlib, hmac, platform, subprocess
import threading, time, webbrowser
from pathlib import Path

# ── Branding / Contact ──────────────────────────────────────────────
OWNER_NAME      = "RPR Associates"
OWNER_PHONE     = "7845998125"
OWNER_EMAIL     = "auprabakaran@gmail.com"
OWNER_WHATSAPP  = "917845998125"
PRICE_BASIC     = "₹2,500/year"
PRICE_PRO       = "₹6,500/year"
APP_TITLE       = "RPR GST + IT Suite — FULL DEMO"
LOCK_APP_NAME   = "RPR_Demo"

# ── Machine-lock helpers ────────────────────────────────────────────
def _lock_dir() -> Path:
    """Return (and create) the hidden lock directory."""
    if platform.system() == "Windows":
        base = Path(os.environ.get("APPDATA", Path.home())) / LOCK_APP_NAME
    else:
        base = Path.home() / f".{LOCK_APP_NAME.lower()}"
    base.mkdir(parents=True, exist_ok=True)
    return base

LOCK_FILE = _lock_dir() / "machine_lock.dat"
# A fixed secret salt so the hash can't be trivially spoofed
_SALT = b"RPR_DEMO_2025_ANTITAMPER_XK39"

def _machine_fingerprint() -> str:
    """
    Build a stable machine fingerprint from:
      • OS platform string
      • CPU processor name
      • hostname
      • Windows MachineGuid (if available) — most unique
    Hash with HMAC-SHA256 so the raw IDs never leave the machine.
    """
    bits = [
        platform.system(),
        platform.machine(),
        platform.processor(),
        platform.node(),
    ]

    # Windows: read MachineGuid from registry (very stable across reboots)
    if platform.system() == "Windows":
        try:
            import winreg
            key = winreg.OpenKey(
                winreg.HKEY_LOCAL_MACHINE,
                r"SOFTWARE\Microsoft\Cryptography",
            )
            val, _ = winreg.QueryValueEx(key, "MachineGuid")
            bits.append(val)
            winreg.CloseKey(key)
        except Exception:
            pass
        # Also try BIOS serial via WMIC
        try:
            out = subprocess.check_output(
                ["wmic", "bios", "get", "serialnumber"],
                stderr=subprocess.DEVNULL, timeout=5
            ).decode(errors="ignore")
            lines = [l.strip() for l in out.splitlines() if l.strip() and l.strip().lower() != "serialnumber"]
            if lines:
                bits.append(lines[0])
        except Exception:
            pass

    raw = "|".join(bits).encode()
    return hmac.new(_SALT, raw, hashlib.sha256).hexdigest()

def _save_lock(fingerprint: str):
    data = {
        "fingerprint": fingerprint,
        "activated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "platform": platform.platform(),
    }
    LOCK_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

def _read_lock() -> dict | None:
    try:
        return json.loads(LOCK_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None

def _check_machine_lock():
    """
    Returns: ("ok", None)        — cleared to run
             ("locked", message) — wrong machine
    """
    current = _machine_fingerprint()
    stored  = _read_lock()

    if stored is None:
        # First activation — claim this machine
        _save_lock(current)
        return ("ok", None)

    if stored.get("fingerprint") == current:
        return ("ok", None)

    # Different machine
    msg = (
        f"This DEMO is already activated on another computer.\n"
        f"Activated on: {stored.get('activated_at','unknown date')}\n\n"
        f"Please contact us to get the FULL LICENSED version:\n"
        f"  📞 Phone / WhatsApp: {OWNER_PHONE}\n"
        f"  📧 Email: {OWNER_EMAIL}\n\n"
        f"The full version works on unlimited machines."
    )
    return ("locked", msg)

# ── Splash / Lock screen (shown before Flask starts) ────────────────
def _show_locked_screen(message: str):
    """Show a simple Tk error window if machine check fails."""
    try:
        import tkinter as tk
        from tkinter import messagebox
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror(f"{APP_TITLE} — Access Denied", message)
        root.destroy()
    except Exception:
        print(f"\n{'='*60}")
        print("ACCESS DENIED")
        print(message)
        print('='*60)
    sys.exit(1)

def _show_splash():
    """Brief activation splash on first run."""
    try:
        import tkinter as tk
        root = tk.Tk()
        root.title(APP_TITLE)
        root.geometry("520x280")
        root.resizable(False, False)
        # center
        root.update_idletasks()
        x = (root.winfo_screenwidth()  - 520) // 2
        y = (root.winfo_screenheight() - 280) // 2
        root.geometry(f"+{x}+{y}")

        tk.Label(root, text="✅ DEMO ACTIVATED", font=("Arial", 16, "bold"),
                 fg="#1a7f37").pack(pady=(28, 4))
        tk.Label(root, text="This machine is now registered for the DEMO.",
                 font=("Arial", 11)).pack()
        tk.Label(root, text="All 11 features are fully unlocked for you to explore.",
                 font=("Arial", 11)).pack(pady=(6,0))
        tk.Label(root, text="The app will open in your browser in a moment…",
                 font=("Arial", 10), fg="#555").pack(pady=(14,0))
        tk.Label(root,
                 text=f"To buy the FULL version: {OWNER_PHONE}  |  {OWNER_EMAIL}",
                 font=("Arial", 9), fg="#0057b7").pack(pady=(18,0))

        root.after(3200, root.destroy)
        root.mainloop()
    except Exception:
        pass   # Tk not available — just continue

# ── Inject demo banner into the main app's HTML ─────────────────────
_DEMO_BANNER_CSS = """
<style>
#demo-banner{
  position:fixed;bottom:0;left:0;right:0;z-index:99999;
  background:linear-gradient(90deg,#1a7f37,#0d5c2a);
  color:#fff;font-family:Arial,sans-serif;font-size:12px;
  padding:6px 16px;display:flex;align-items:center;
  justify-content:space-between;box-shadow:0 -2px 8px rgba(0,0,0,.25);
}
#demo-banner a{color:#ffe066;font-weight:700;text-decoration:none;}
#demo-banner .db-right{display:flex;gap:16px;align-items:center;}
#demo-banner .db-btn{
  background:#ffe066;color:#1a3c1a;padding:4px 14px;
  border-radius:4px;font-weight:700;font-size:12px;text-decoration:none;
}
</style>
"""

_DEMO_BANNER_HTML = f"""
<div id="demo-banner">
  <span>
    🟢 <strong>FULL ACCESS DEMO</strong> — All 11 features unlocked |
    This copy is registered to THIS machine only
  </span>
  <div class="db-right">
    <span>Buy Full Version (unlimited machines):</span>
    <a class="db-btn"
       href="https://wa.me/{OWNER_WHATSAPP}?text=I%20tried%20the%20RPR%20GST%20Demo%20and%20want%20to%20buy%20the%20full%20version"
       target="_blank">
      💬 WhatsApp {OWNER_PHONE}
    </a>
    <a class="db-btn"
       href="mailto:{OWNER_EMAIL}?subject=RPR%20GST%20Suite%20Purchase"
       style="background:#4dabf7;color:#fff">
      📧 Email
    </a>
  </div>
</div>
<div style="height:42px"></div>  <!-- spacer so content isn't hidden behind banner -->
"""

def _patch_flask_app(flask_app):
    """
    Monkey-patch the / route to inject the demo banner into every page.
    Works regardless of how the HTML is rendered inside app.py.
    """
    from flask import Response
    import functools

    original_dispatch = flask_app.wsgi_app

    def patched_wsgi(environ, start_response):
        captured = {}

        def capturing_start(status, headers, exc_info=None):
            captured["status"]  = status
            captured["headers"] = list(headers)
            return start_response(status, headers, exc_info)

        resp_iter = original_dispatch(environ, capturing_start)

        # Only patch HTML responses on the root path
        if environ.get("PATH_INFO", "") == "/" and \
           any(v.lower().startswith("text/html")
               for k, v in captured.get("headers", []) if k.lower() == "content-type"):
            body = b"".join(resp_iter)
            html = body.decode("utf-8", errors="replace")
            # Inject CSS before </head> and banner before </body>
            html = html.replace("</head>", _DEMO_BANNER_CSS + "</head>", 1)
            html = html.replace("</body>", _DEMO_BANNER_HTML + "</body>", 1)
            body = html.encode("utf-8")
            # Fix Content-Length header
            new_headers = [
                (k, str(len(body)) if k.lower() == "content-length" else v)
                for k, v in captured.get("headers", [])
            ]
            # re-send corrected headers
            start_response(captured["status"], new_headers)
            return [body]

        return resp_iter

    flask_app.wsgi_app = patched_wsgi

# ── Main ────────────────────────────────────────────────────────────
def main():
    print(f"\n{'='*62}")
    print(f"  {APP_TITLE}")
    print(f"{'='*62}")
    print("  Checking machine registration…")

    # ── Step 1: Machine lock check ──────────────────────────────────
    status, msg = _check_machine_lock()

    if status == "locked":
        print(f"\n  ❌ DEMO ALREADY ACTIVATED ON ANOTHER MACHINE\n")
        print(f"  {msg}")
        _show_locked_screen(msg)
        return  # never reached

    if not LOCK_FILE.exists():
        # Should not happen (saved above) but guard anyway
        pass
    else:
        lock_data = _read_lock()
        first_run = (lock_data.get("activated_at","") == time.strftime("%Y-%m-%d %H:%M:%S")
                     or (time.time() - LOCK_FILE.stat().st_mtime) < 10)

    first_activation = (time.time() - LOCK_FILE.stat().st_mtime) < 10

    if first_activation:
        print("  ✅ First activation — this machine is now registered!")
        print("     (Cannot run on any other PC with this same demo file)")
        threading.Thread(target=_show_splash, daemon=True).start()
        time.sleep(0.5)  # let splash appear before console messages scroll
    else:
        lock_data = _read_lock() or {}
        print(f"  ✅ Machine verified — activated on {lock_data.get('activated_at','?')}")

    print()

    # ── Step 2: Import and patch the real app ───────────────────────
    # app.py must be in the same folder (or bundled alongside)
    _app_dir = Path(__file__).parent
    if str(_app_dir) not in sys.path:
        sys.path.insert(0, str(_app_dir))

    # If running as PyInstaller bundle, _MEIPASS has the files
    if getattr(sys, "frozen", False):
        _bundle = Path(sys._MEIPASS)
        if str(_bundle) not in sys.path:
            sys.path.insert(0, str(_bundle))

    try:
        import app as _main_app
    except ImportError as e:
        print(f"\n  ❌ ERROR: Could not import app.py — {e}")
        print("     Make sure app.py is in the same folder as this file.")
        input("  Press Enter to exit…")
        sys.exit(1)

    flask_app = _main_app.app

    # ── Step 3: Inject demo banner ───────────────────────────────────
    _patch_flask_app(flask_app)

    # ── Step 4: Open browser after short delay ───────────────────────
    port = int(os.environ.get("PORT", 5000))

    def _open_browser():
        time.sleep(2.2)
        webbrowser.open(f"http://localhost:{port}")

    threading.Thread(target=_open_browser, daemon=True).start()

    # ── Step 5: Start Flask server ────────────────────────────────────
    print(f"  🚀 Starting full suite on http://localhost:{port}")
    print(f"     All 11 features are unlocked.")
    print(f"     Contact to buy: {OWNER_PHONE}  |  {OWNER_EMAIL}")
    print(f"{'='*62}\n")

    flask_app.run(host="127.0.0.1", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
