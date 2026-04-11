"""
browser_bridge.py — Run this on YOUR PC
========================================
Connects your local browser to the Render server for GST automation.

HOW TO USE:
  1. Set RENDER_SERVER below to your Render app URL
  2. Double-click RUN_BRIDGE.bat   (installs everything automatically)
     OR run:  python browser_bridge.py
  3. Go to your Render portal → click Auto-Download tab
  4. Browser opens on your PC — type CAPTCHA when it appears
"""

# ── Auto-install required packages ───────────────────────────────
import sys, subprocess, os

REQUIRED = ["websockets", "playwright"]

def _pip(pkg):
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", pkg, "-q",
         "--no-warn-script-location"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

print("\n  Checking packages...")
for pkg in REQUIRED:
    try:
        __import__(pkg)
        print(f"  OK: {pkg}")
    except ImportError:
        print(f"  Installing {pkg}...")
        try:
            _pip(pkg)
            print(f"  OK: {pkg} installed")
        except Exception as e:
            print(f"  ERROR: Cannot install {pkg}: {e}")
            print(f"  Run manually:  pip install {pkg}")
            input("\n  Press Enter to exit..."); sys.exit(1)

# Install Chromium once
_marker = os.path.join(os.path.expanduser("~"), ".playwright_chromium_ok")
if not os.path.exists(_marker):
    print("  Installing Chromium browser (one-time, ~150 MB)...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "playwright", "install", "chromium"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        open(_marker, "w").close()
        print("  OK: Chromium installed")
    except Exception as e:
        print(f"  WARNING: Chromium install failed: {e}")
        print("  Run manually:  playwright install chromium")

# ── Now import everything ─────────────────────────────────────────
import asyncio, json, base64
import websockets
# IMPORTANT: use ASYNC playwright — sync API cannot run inside asyncio
from playwright.async_api import async_playwright


# ════════════════════════════════════════════════════════════════════
#  SET YOUR RENDER APP URL HERE
#  Just paste what you see in your browser — any format works
# ════════════════════════════════════════════════════════════════════
RENDER_SERVER = "https://gst-app-ut23.onrender.com"
#
#  Examples:
#    "https://gst-app-ut23.onrender.com"
#    "https://my-gst-portal.onrender.com"
# ════════════════════════════════════════════════════════════════════


def _to_ws_url(url: str) -> str:
    """
    Convert any form of Render URL to a correct wss:// WebSocket URL.
    Safely handles all typo combinations like wss://https://...
    """
    url = url.strip().rstrip("/")

    # Fix typos where scheme is doubled: wss://https://... etc.
    for bad, replace in [
        ("wss://https://", "https://"),
        ("wss://http://",  "http://"),
        ("ws://https://",  "https://"),
        ("ws://http://",   "http://"),
    ]:
        if url.lower().startswith(bad):
            url = replace + url[len(bad):]
            break

    # Convert http/https to ws/wss
    if url.startswith("https://"):
        url = "wss://" + url[8:]
    elif url.startswith("http://"):
        url = "ws://" + url[7:]
    elif not url.startswith(("ws://", "wss://")):
        url = "wss://" + url   # default: assume wss

    # Append /ws endpoint
    if not url.endswith("/ws"):
        url += "/ws"

    return url


async def main():
    print()
    print("  " + "=" * 56)
    print("   GST Browser Bridge - PC Client")
    print("  " + "=" * 56)

    ws_url = _to_ws_url(RENDER_SERVER)
    print(f"\n  Render URL   : {RENDER_SERVER}")
    print(f"  WebSocket URL: {ws_url}")

    print("\n  Starting browser on your PC...")

    # MUST use async_playwright inside asyncio
    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch(
                headless=False,  # Must be visible so you can type CAPTCHA
                args=[
                    "--start-maximized",
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                ]
            )
        except Exception as e:
            print(f"\n  ERROR: Browser failed to start: {e}")
            print("  Run:  playwright install chromium")
            input("\n  Press Enter to exit..."); return

        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()
        print("  OK: Browser is open and ready!")
        print()
        print("  HOW IT WORKS:")
        print("  1. Open your Render portal in any browser on this PC")
        print("  2. Click the Auto-Download tab")
        print("  3. Enter GSTIN + GST credentials → click Start")
        print("  4. THIS window will open GST portal automatically")
        print("  5. Type the CAPTCHA in THIS browser when it appears")
        print("  6. Press ENTER here to continue after CAPTCHA")
        print()
        print("  " + "-" * 56)

        # ── Action handler ─────────────────────────────────────────
        async def run_action(data: dict) -> dict:
            action = data.get("action", "")

            if action == "goto":
                url = data["url"]
                print(f"\n  >> Opening: {url}")
                try:
                    await page.goto(url, wait_until="domcontentloaded",
                                    timeout=60000)
                    return {"status": "done", "url": page.url}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "fill":
                sel   = data.get("selector", "")
                value = data.get("value", "")
                disp  = "*" * len(value) if "pass" in sel.lower() else value
                print(f"  >> Fill [{sel}] = {disp}")
                try:
                    await page.fill(sel, value)
                    return {"status": "done"}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "click":
                sel = data.get("selector", "")
                print(f"  >> Click [{sel}]")
                try:
                    await page.click(sel, timeout=15000)
                    return {"status": "done"}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "screenshot":
                try:
                    img = await page.screenshot()
                    return {"status": "screenshot",
                            "image": base64.b64encode(img).decode()}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "wait_for_selector":
                sel     = data.get("selector", "")
                timeout = data.get("timeout", 30000)
                print(f"  >> Wait for element [{sel}]")
                try:
                    await page.wait_for_selector(sel, timeout=timeout)
                    return {"status": "found"}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "get_text":
                sel = data.get("selector", "")
                try:
                    return {"status": "text",
                            "text": await page.inner_text(sel)}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "get_url":
                return {"status": "url", "url": page.url}

            elif action == "select_option":
                sel   = data.get("selector", "")
                value = data.get("value", "")
                print(f"  >> Select [{sel}] = {value}")
                try:
                    await page.select_option(sel, value)
                    return {"status": "done"}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "wait_for_navigation":
                try:
                    await page.wait_for_load_state("networkidle",
                                                   timeout=30000)
                    return {"status": "done", "url": page.url}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "eval":
                script = data.get("script", "")
                try:
                    result = await page.evaluate(script)
                    return {"status": "result", "result": str(result)}
                except Exception as e:
                    return {"status": "error", "error": str(e)}

            elif action == "wait_captcha":
                # Pause and let the user solve CAPTCHA manually
                print()
                print("  " + "!" * 56)
                print("  CAPTCHA REQUIRED — Look at the browser window")
                print("  1. Type the CAPTCHA letters in the browser")
                print("  2. Do NOT click Login yet")
                print("  3. Come back here and press ENTER")
                print("  " + "!" * 56)
                # Run blocking input in a thread so we don't block the event loop
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: input("  >> Press ENTER after typing CAPTCHA: ")
                )
                return {"status": "captcha_done"}

            else:
                return {"status": "error",
                        "error": f"Unknown action: {action}"}

        # ── WebSocket loop with auto-reconnect ─────────────────────
        attempt = 0
        while True:
            attempt += 1
            try:
                print(f"\n  Connecting to server (attempt {attempt})...")
                async with websockets.connect(
                    ws_url,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=30,
                ) as ws:
                    attempt = 0  # reset on success
                    print("  OK: Connected to Render server!")
                    print("  Waiting for commands... (keep this window open)")
                    print("  " + "-" * 56)

                    while True:
                        try:
                            raw    = await ws.recv()
                            data   = json.loads(raw)
                            # Handle server keep-alive ping
                            if data.get("action") == "ping":
                                await ws.send(json.dumps({"status": "pong"}))
                                continue
                            result = await run_action(data)
                            await ws.send(json.dumps(result))

                        except websockets.exceptions.ConnectionClosed:
                            print("\n  Server closed connection. Reconnecting...")
                            break

                        except Exception as e:
                            print(f"\n  Error: {e}")
                            try:
                                await ws.send(json.dumps(
                                    {"status": "error", "error": str(e)}))
                            except: pass

            except websockets.exceptions.InvalidURI:
                print(f"\n  ERROR: Invalid URL: {ws_url}")
                print("  Fix RENDER_SERVER in browser_bridge.py")
                break

            except (ConnectionRefusedError, OSError, TimeoutError) as e:
                wait = min(attempt * 5, 30)
                print(f"\n  Cannot connect: {e}")
                if attempt < 5:
                    print(f"  Retrying in {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    print()
                    print("  Could not connect after 5 attempts.")
                    print("  Check:")
                    print("  * Is your Render app running?")
                    print(f"  * RENDER_SERVER = {RENDER_SERVER!r}")
                    print(f"  * WebSocket URL = {ws_url}")
                    break

            except Exception as e:
                print(f"\n  Connection error: {e}")
                if attempt < 3:
                    await asyncio.sleep(5)
                else:
                    break

        print("\n  Closing browser...")
        await browser.close()
        print("  Goodbye!\n")
        input("  Press Enter to close...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n  Interrupted. Exiting...")
        sys.exit(0)
