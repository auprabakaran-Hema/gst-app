r"""
RPR GST + IT Suite — FULL DEMO LAUNCHER
========================================
One-time use, all features unlocked, machine-locked entry point.

This wrapper:
  1. Checks/enforces one-time machine lock (via rpr_license.py)
  2. If cleared → launches full Flask app (demo_full_app.py or app.py)
  3. If already burned → shows "Buy Now" screen and exits

Build:  BUILD_FULL_DEMO_EXE.bat
Result: dist\RPR_GST_Demo_Full.exe

Usage:
  python demo_full_app.py
  (or just double-click the EXE)
"""

import os
import sys
import time
import threading
import webbrowser
from pathlib import Path

# ── Branding / Contact (SYNC WITH rpr_license.py) ────────────────
CONTACT_PHONE     = "7845998125"
CONTACT_EMAIL     = "auprabakaran@gmail.com"
CONTACT_WHATSAPP  = "917845998125"
PRICE_BASIC       = "₹2,500/year"
PRICE_PRO         = "₹6,500/year"
APP_TITLE         = "RPR GST + IT Suite — FULL DEMO"

# ══════════════════════════════════════════════════════════════════════
#  STEP 1: IMPORT LOCK ENFORCER
# ══════════════════════════════════════════════════════════════════════

# If running as PyInstaller bundle, _MEIPASS has the bundled modules
if getattr(sys, "frozen", False):
    sys.path.insert(0, sys._MEIPASS)

try:
    from rpr_license import enforce_one_time_trial
except ImportError as e:
    print(f"\nERROR: Could not import rpr_license.py")
    print(f"  {e}")
    print("\nMake sure rpr_license.py is in the same folder or bundled with the EXE.")
    input("Press Enter to exit...")
    sys.exit(1)


# ══════════════════════════════════════════════════════════════════════
#  STEP 2: SPLASH SCREEN (first activation only)
# ══════════════════════════════════════════════════════════════════════

def _show_activation_splash():
    """
    Brief splash on first successful activation.
    Shows that demo is now locked to this machine.
    """
    try:
        import tkinter as tk
        root = tk.Tk()
        root.title(APP_TITLE)
        root.geometry("540x320")
        root.resizable(False, False)

        # Center on screen
        root.update_idletasks()
        x = (root.winfo_screenwidth() - 540) // 2
        y = (root.winfo_screenheight() - 320) // 2
        root.geometry(f"+{x}+{y}")

        # Title
        tk.Label(root, text="✅  DEMO ACTIVATED", font=("Segoe UI", 18, "bold"),
                 fg="#1a7f37").pack(pady=(28, 6))

        # Message
        tk.Label(root,
                 text="This FREE DEMO is now registered to THIS MACHINE.\n"
                      "All 11 features are fully unlocked for you to explore.",
                 font=("Segoe UI", 11), fg="#333", justify="center").pack(pady=10)

        tk.Label(root, text="The app will open in your browser in a moment…",
                 font=("Segoe UI", 10), fg="#666").pack(pady=(6, 0))

        # Separator
        tk.Frame(root, bg="#ddd", height=1).pack(fill="x", padx=40, pady=16)

        # Contact info
        contact_text = (
            f"To purchase the FULL SUITE (unlimited machines):\n\n"
            f"📞  Phone / WhatsApp: {CONTACT_PHONE}\n"
            f"📧  Email: {CONTACT_EMAIL}\n\n"
            f"Basic: {PRICE_BASIC}  |  Pro: {PRICE_PRO}"
        )
        tk.Label(root, text=contact_text, font=("Segoe UI", 9), fg="#0057b7",
                 justify="center").pack(pady=8)

        root.after(3500, root.destroy)
        root.mainloop()

    except Exception as e:
        # Tk not available, silently continue
        pass


# ══════════════════════════════════════════════════════════════════════
#  STEP 3: IMPORT AND CONFIGURE FLASK APP
# ══════════════════════════════════════════════════════════════════════

def _import_flask_app():
    """
    Import the Flask app from demo_full_app.py or app.py.
    Returns the Flask app object.
    """
    # Try to import app from the bundled module
    if getattr(sys, "frozen", False):
        bundle_dir = Path(sys._MEIPASS)
        if str(bundle_dir) not in sys.path:
            sys.path.insert(0, str(bundle_dir))

    # Look for Flask app in demo_full_app.py or app.py
    app_module = None
    app = None

    for module_name in ["demo_full_app", "app"]:
        try:
            module = __import__(module_name)
            if hasattr(module, "app"):
                app_module = module
                app = module.app
                print(f"  ✅ Imported Flask app from {module_name}.py")
                break
        except ImportError:
            continue

    if app is None:
        print(f"\n  ❌ ERROR: Could not import Flask app")
        print(f"     Tried: demo_full_app.py, app.py")
        print(f"     Make sure one of these files exists and contains 'app = Flask(...)'")
        input("\n  Press Enter to exit...")
        sys.exit(1)

    return app


# ══════════════════════════════════════════════════════════════════════
#  STEP 4: DEMO BANNER INJECTION
# ══════════════════════════════════════════════════════════════════════

_DEMO_BANNER_CSS = """
<style>
#demo-banner {
  position: fixed;
  bottom: 0;
  left: 0;
  right: 0;
  z-index: 99999;
  background: linear-gradient(90deg, #1a7f37 0%, #0d5c2a 100%);
  color: #fff;
  font-family: 'Segoe UI', Arial, sans-serif;
  font-size: 13px;
  padding: 8px 16px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  box-shadow: 0 -3px 12px rgba(0,0,0,0.3);
  gap: 16px;
}
#demo-banner .db-left {
  flex: 1;
}
#demo-banner .db-right {
  display: flex;
  gap: 12px;
  align-items: center;
  white-space: nowrap;
}
#demo-banner a, #demo-banner button {
  color: #fff;
  text-decoration: none;
  padding: 6px 14px;
  border-radius: 4px;
  font-weight: 600;
  font-size: 12px;
  border: none;
  cursor: pointer;
  transition: all 0.2s;
}
#demo-banner .db-whatsapp {
  background: #25d366;
  color: #fff;
}
#demo-banner .db-whatsapp:hover {
  background: #20ba5a;
}
#demo-banner .db-email {
  background: #4dabf7;
  color: #fff;
}
#demo-banner .db-email:hover {
  background: #3d9ae6;
}
@media (max-width: 768px) {
  #demo-banner {
    flex-direction: column;
    text-align: center;
    gap: 8px;
    padding: 12px;
  }
  #demo-banner .db-right {
    justify-content: center;
    flex-wrap: wrap;
  }
}
</style>
"""

_DEMO_BANNER_HTML = f"""
<div id="demo-banner">
  <div class="db-left">
    🟢 <strong>FULL ACCESS DEMO</strong> — All features unlocked | 
    Machine-locked (one-time use)
  </div>
  <div class="db-right">
    <span>Purchase full version:</span>
    <a class="db-whatsapp" href="https://wa.me/{CONTACT_WHATSAPP}?text=Hi%2C%20I%20tried%20the%20RPR%20demo%20and%20want%20to%20buy%20the%20full%20version" target="_blank">
      💬 WhatsApp
    </a>
    <a class="db-email" href="mailto:{CONTACT_EMAIL}?subject=RPR%20GST%20Suite%20Purchase%20Inquiry" target="_blank">
      📧 Email
    </a>
  </div>
</div>
<div style="height: 60px;"></div>
"""


def _patch_flask_app(flask_app):
    """
    Monkey-patch Flask app to inject demo banner into every HTML page.
    """
    original_wsgi = flask_app.wsgi_app

    def patched_wsgi(environ, start_response):
        captured = {}

        def capturing_start(status, headers, exc_info=None):
            captured["status"] = status
            captured["headers"] = list(headers)
            return start_response(status, headers, exc_info)

        # Call original WSGI app
        resp_iter = original_wsgi(environ, capturing_start)

        # Check if this is an HTML response
        is_html = any(
            v.lower().startswith("text/html")
            for k, v in captured.get("headers", [])
            if k.lower() == "content-type"
        )

        if is_html:
            try:
                body = b"".join(resp_iter)
                html = body.decode("utf-8", errors="replace")

                # Inject CSS before </head>
                html = html.replace("</head>", _DEMO_BANNER_CSS + "</head>", 1)

                # Inject banner before </body>
                html = html.replace("</body>", _DEMO_BANNER_HTML + "</body>", 1)

                body = html.encode("utf-8")

                # Update Content-Length
                new_headers = [
                    (k, str(len(body)) if k.lower() == "content-length" else v)
                    for k, v in captured.get("headers", [])
                ]

                start_response(captured["status"], new_headers)
                return [body]
            except Exception:
                # If injection fails, return original response
                start_response(captured["status"], captured["headers"])
                return [b"".join(resp_iter)]

        return resp_iter

    flask_app.wsgi_app = patched_wsgi


# ══════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "=" * 70)
    print(f"  {APP_TITLE}")
    print("=" * 70)

    # ── STEP 1: Check machine lock ──────────────────────────────────
    print("\n  Checking machine registration…\n")

    lock_is_new = False
    try:
        # This call enforces the one-time lock
        # If already burned on this machine → shows blocked UI and exits
        enforce_one_time_trial()
        # If we reach here, lock check passed (either first run or same machine)
        lock_is_new = True
    except SystemExit:
        # rpr_license.py called sys.exit() (lock already burned)
        sys.exit(0)
    except Exception as e:
        print(f"  ⚠️  License check failed: {e}")
        print(f"     Continuing anyway (demo mode)...\n")

    print("  ✅ Machine lock verified — proceeding to full demo\n")

    # ── STEP 2: Import Flask app ────────────────────────────────────
    print("  Loading Flask application…\n")
    flask_app = _import_flask_app()

    # ── STEP 3: Inject demo banner ──────────────────────────────────
    print("  Injecting demo banner into UI…\n")
    _patch_flask_app(flask_app)

    # ── STEP 4: Show activation splash (first run only) ─────────────
    if lock_is_new:
        print("  Showing activation splash…")
        _show_activation_splash()

    # ── STEP 5: Auto-open browser ───────────────────────────────────
    port = int(os.environ.get("PORT", 5000))

    def _open_browser():
        time.sleep(2.0)
        try:
            webbrowser.open(f"http://localhost:{port}", new=2, autoraise=True)
        except Exception:
            pass

    threading.Thread(target=_open_browser, daemon=True).start()

    # ── STEP 6: Start Flask server ──────────────────────────────────
    print("\n" + "=" * 70)
    print(f"  🚀 Starting Full Demo on http://localhost:{port}")
    print(f"     All 11 features are fully unlocked!")
    print(f"     Contact: {CONTACT_PHONE} | {CONTACT_EMAIL}")
    print("=" * 70 + "\n")

    try:
        flask_app.run(
            host="127.0.0.1",
            port=port,
            debug=False,
            threaded=True,
            use_reloader=False,  # Important for PyInstaller bundled apps
        )
    except Exception as e:
        print(f"\n  ❌ Error starting Flask server: {e}")
        input("\n  Press Enter to exit...")
        sys.exit(1)


if __name__ == "__main__":
    main()
