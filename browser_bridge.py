"""
browser_bridge.py — Run this on YOUR PC
========================================
Connects your local browser to the Render server for GST automation.
Double-click RUN_BRIDGE.bat (or run: python browser_bridge.py)

Auto-installs required packages on first run.
"""

# ── Auto-install required packages ───────────────────────────────
import sys, subprocess

REQUIRED = ["websockets", "playwright"]

def _install(pkg):
    print(f"  Installing {pkg}...")
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", pkg, "-q", "--no-warn-script-location"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )

print()
print("  Checking required packages...")
for pkg in REQUIRED:
    try:
        __import__(pkg)
        print(f"  ✓ {pkg} already installed")
    except ImportError:
        print(f"  ✗ {pkg} missing — installing...")
        try:
            _install(pkg)
            print(f"  ✓ {pkg} installed")
        except Exception as e:
            print(f"  ✗ Failed to install {pkg}: {e}")
            print(f"\n  Please run manually:\n    pip install {pkg}")
            input("\n  Press Enter to exit...")
            sys.exit(1)

# ── Install Playwright browser if needed ──────────────────────────
import os
_pw_marker = os.path.join(os.path.expanduser("~"), ".playwright_chromium_installed")
if not os.path.exists(_pw_marker):
    print("\n  Installing Playwright Chromium browser (one-time, ~150 MB)...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        open(_pw_marker, "w").close()
        print("  ✓ Chromium installed")
    except Exception as e:
        print(f"  ✗ Chromium install failed: {e}")
        print("  Try manually:  playwright install chromium")
        input("\n  Press Enter to exit...")
        sys.exit(1)

# ── Now import everything ────────────────────────────────────────
import asyncio, json, base64, threading, time
import websockets
from playwright.sync_api import sync_playwright

# ════════════════════════════════════════════════════════════════════
#  ▼▼▼  UPDATE THIS LINE WITH YOUR RENDER APP URL  ▼▼▼
# ════════════════════════════════════════════════════════════════════
RENDER_SERVER = "https://gst-app-ut23.onrender.com"
#   Example: "https://gst-recon-abc123.onrender.com"
# ════════════════════════════════════════════════════════════════════


def _to_ws_url(url):
    """Convert https:// URL to wss:// WebSocket URL."""
    url = url.strip().rstrip("/")
    if url.startswith("https://"):
        url = "wss://" + url[8:]
    elif url.startswith("http://"):
        url = "ws://" + url[7:]
    elif not url.startswith(("ws://", "wss://")):
        url = "wss://" + url
    if not url.endswith("/ws"):
        url += "/ws"
    return url


# ── Shared state between sync playwright and async websocket ─────
_page    = None
_context = None

def _run_action(data):
    """Execute a browser action synchronously."""
    global _page
    action = data.get("action", "")

    if action == "goto":
        url = data["url"]
        print(f"  🌐 Opening: {url}")
        try:
            _page.goto(url, wait_until="domcontentloaded", timeout=60000)
            return {"status": "done", "url": _page.url}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "fill":
        sel   = data.get("selector", "")
        value = data.get("value", "")
        disp  = "*" * len(value) if "pass" in sel.lower() else value
        print(f"  ⌨  Fill [{sel}] = {disp}")
        try:
            _page.fill(sel, value)
            return {"status": "done"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "click":
        sel = data.get("selector", "")
        print(f"  🖱  Click [{sel}]")
        try:
            _page.click(sel, timeout=15000)
            return {"status": "done"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "screenshot":
        try:
            img = _page.screenshot()
            return {"status": "screenshot", "image": base64.b64encode(img).decode()}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "wait_for_selector":
        sel     = data.get("selector", "")
        timeout = data.get("timeout", 30000)
        print(f"  ⏳ Wait [{sel}]")
        try:
            _page.wait_for_selector(sel, timeout=timeout)
            return {"status": "found"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "get_text":
        sel = data.get("selector", "")
        try:
            return {"status": "text", "text": _page.inner_text(sel)}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "get_url":
        return {"status": "url", "url": _page.url}

    elif action == "select_option":
        sel   = data.get("selector", "")
        value = data.get("value", "")
        print(f"  🔽 Select [{sel}] = {value}")
        try:
            _page.select_option(sel, value)
            return {"status": "done"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "wait_for_navigation":
        try:
            _page.wait_for_load_state("networkidle", timeout=30000)
            return {"status": "done", "url": _page.url}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    elif action == "eval":
        script = data.get("script", "")
        try:
            result = _page.evaluate(script)
            return {"status": "result", "result": str(result)}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    else:
        return {"status": "error", "error": f"Unknown action: {action}"}


async def _ws_loop(ws_url):
    """Async WebSocket receive-send loop."""
    print(f"\n  Connecting to: {ws_url}")
    try:
        async with websockets.connect(
            ws_url,
            ping_interval=20,
            ping_timeout=10,
            open_timeout=30,
        ) as ws:
            print("  ✓ Connected to Render server!")
            print("  Waiting for commands...\n")
            print("  ─" * 33)

            while True:
                try:
                    raw = await ws.recv()
                    data = json.loads(raw)
                    result = _run_action(data)
                    await ws.send(json.dumps(result))
                except websockets.exceptions.ConnectionClosed:
                    print("\n  Connection closed by server.")
                    break
                except Exception as e:
                    print(f"\n  Error: {e}")
                    try:
                        await ws.send(json.dumps({"status": "error", "error": str(e)}))
                    except: pass

    except websockets.exceptions.InvalidURI:
        print(f"  ✗ Invalid URL: {ws_url}")
    except (ConnectionRefusedError, OSError) as e:
        print(f"  ✗ Cannot connect: {e}")
        print("    • Is your Render app running?")
        print("    • Is RENDER_SERVER set correctly?")
    except Exception as e:
        print(f"  ✗ Connection error: {e}")


def main():
    global _page, _context

    print()
    print("  " + "═" * 54)
    print("   🖥  GST Browser Bridge — PC Client")
    print("  " + "═" * 54)

    # ── Check RENDER_SERVER is configured ─────────────────────────
    if "YOUR-RENDER-APP" in RENDER_SERVER:
        print()
        print("  ⚠  RENDER_SERVER is not set!")
        print()
        print("  Open browser_bridge.py in Notepad and change:")
        print('    RENDER_SERVER = "https://YOUR-RENDER-APP-NAME.onrender.com"')
        print()
        print("  To your actual Render URL, e.g.:")
        print('    RENDER_SERVER = "https://my-gst-app.onrender.com"')
        print()
        input("  Press Enter to exit...")
        return

    ws_url = _to_ws_url(RENDER_SERVER)

    # ── Launch Playwright browser ──────────────────────────────────
    print("\n  Starting browser on your PC...")
    with sync_playwright() as pw:
        try:
            browser = pw.chromium.launch(
                headless=False,
                args=[
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ]
            )
        except Exception as e:
            print(f"\n  ✗ Browser failed to start: {e}")
            print("  Run:  playwright install chromium")
            input("\n  Press Enter to exit...")
            return

        _context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        _page = _context.new_page()
        print("  ✓ Browser ready!")
        print()
        print("  HOW TO USE:")
        print("  1. Go to your Render portal in a web browser")
        print("  2. Click the 'Auto-Download' tab")
        print("  3. Enter GSTIN, credentials, and click Start")
        print("  4. This browser will open GST portal automatically")
        print("  5. Type CAPTCHA when it appears here")
        print()
        print("  " + "─" * 54)

        try:
            asyncio.run(_ws_loop(ws_url))
        except KeyboardInterrupt:
            print("\n  Interrupted.")
        finally:
            print("\n  Closing browser...")
            try: browser.close()
            except: pass

    print("  Done. Goodbye!\n")
    input("  Press Enter to close...")


if __name__ == "__main__":
    main()
