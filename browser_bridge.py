"""
browser_bridge.py — Run this on YOUR PC
========================================
Uses plain HTTP polling — no WebSocket needed.

HOW TO USE:
  1. Double-click RUN_BRIDGE.bat  OR  run: python browser_bridge.py
  2. Open your Render portal → Auto-Download tab
  3. Fill GSTIN + credentials → click Start Auto Download
  4. This browser window opens GST portal automatically
  5. Solve CAPTCHA when it appears, press ENTER here
"""

import sys, subprocess, os

REQUIRED = ["requests", "playwright"]

def _pip(pkg):
    subprocess.check_call([sys.executable,"-m","pip","install",pkg,"-q",
        "--no-warn-script-location"],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)

print("\n  Checking packages...")
for pkg in REQUIRED:
    try:
        __import__(pkg); print(f"  OK: {pkg}")
    except ImportError:
        print(f"  Installing {pkg}...")
        try: _pip(pkg); print(f"  OK: {pkg} installed")
        except Exception as e:
            print(f"  ERROR: {e}"); input("\n  Press Enter to exit..."); sys.exit(1)

_marker = os.path.join(os.path.expanduser("~"), ".playwright_chromium_ok")
if not os.path.exists(_marker):
    print("  Installing Chromium (one-time ~150 MB)...")
    try:
        subprocess.check_call([sys.executable,"-m","playwright","install","chromium"],
            stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        open(_marker,"w").close(); print("  OK: Chromium installed")
    except Exception as e:
        print(f"  WARNING: {e}")

import asyncio, json, base64
import requests
from playwright.async_api import async_playwright

# ════════════════════════════════════════════════════
RENDER_SERVER = "https://gst-app-ut23.onrender.com"
# ════════════════════════════════════════════════════

POLL_URL    = RENDER_SERVER.rstrip("/") + "/api/bridge/poll"
RESPOND_URL = RENDER_SERVER.rstrip("/") + "/api/bridge/respond"

def _post(data):
    try: requests.post(RESPOND_URL, json=data, timeout=10)
    except Exception as e: print(f"  WARNING: respond failed: {e}")

def _poll():
    try:
        r = requests.get(POLL_URL, timeout=8)
        if r.status_code == 200: return r.json()
    except Exception as e: print(f"  WARNING: poll failed: {e}")
    return None

async def run_action(page, data):
    action = data.get("action","")
    if action == "goto":
        url = data["url"]; print(f"\n  >> Opening: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            return {"status":"done","url":page.url}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "fill":
        sel=data.get("selector",""); val=data.get("value","")
        disp="*"*len(val) if "pass" in sel.lower() else val
        print(f"  >> Fill [{sel}] = {disp}")
        try: await page.fill(sel,val); return {"status":"done"}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "click":
        sel=data.get("selector",""); print(f"  >> Click [{sel}]")
        try: await page.click(sel,timeout=15000); return {"status":"done"}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "screenshot":
        try:
            img=await page.screenshot()
            return {"status":"screenshot","image":base64.b64encode(img).decode()}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "wait_for_selector":
        sel=data.get("selector",""); timeout=data.get("timeout",30000)
        print(f"  >> Wait for [{sel}]")
        try: await page.wait_for_selector(sel,timeout=timeout); return {"status":"found"}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "get_text":
        sel=data.get("selector","")
        try: return {"status":"text","text":await page.inner_text(sel)}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "get_url":
        return {"status":"url","url":page.url}
    elif action == "select_option":
        sel=data.get("selector",""); val=data.get("value","")
        print(f"  >> Select [{sel}] = {val}")
        try: await page.select_option(sel,val); return {"status":"done"}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "wait_for_navigation":
        try:
            await page.wait_for_load_state("networkidle",timeout=30000)
            return {"status":"done","url":page.url}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "eval":
        script=data.get("script","")
        try:
            result=await page.evaluate(script)
            return {"status":"result","result":str(result)}
        except Exception as e: return {"status":"error","error":str(e)}
    elif action == "wait_captcha":
        print()
        print("  " + "="*56)
        print("  !!  CAPTCHA REQUIRED — DO THIS NOW:  !!")
        print()
        print("  1. Look at THIS Chromium browser window")
        print("  2. Type the CAPTCHA characters shown in the box")
        print("  3. Click the LOGIN button")
        print("  4. Come back to THIS cmd window and press ENTER")
        print("  " + "="*56)
        print()
        await asyncio.get_event_loop().run_in_executor(
            None, lambda: input("  >> Press ENTER AFTER you clicked LOGIN: "))
        return {"status":"captcha_done"}
    else:
        return {"status":"error","error":f"Unknown action: {action}"}

async def main():
    print()
    print("  " + "="*56)
    print("   GST Browser Bridge  (HTTP polling)")
    print("  " + "="*56)
    print(f"\n  Server: {RENDER_SERVER}")
    print("\n  Testing server connection...")
    try:
        r = requests.get(RENDER_SERVER, timeout=15)
        print(f"  OK: Server reachable (HTTP {r.status_code})")
    except Exception as e:
        print(f"\n  ERROR: Cannot reach server: {e}")
        input("\n  Press Enter to exit..."); return

    print("\n  Starting browser...")
    async with async_playwright() as pw:
        try:
            browser = await pw.chromium.launch(
                headless=False,
                args=["--start-maximized",
                      "--disable-blink-features=AutomationControlled",
                      "--no-sandbox"])
        except Exception as e:
            print(f"\n  ERROR: Browser failed: {e}")
            input("\n  Press Enter to exit..."); return

        context = await browser.new_context(
            viewport={"width":1366,"height":768},
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"))
        page = await context.new_page()

        print("  OK: Browser ready!")
        print()
        print("  NEXT STEPS:")
        print("  1. Open your Render portal in any browser")
        print("  2. Go to Auto-Download tab")
        print("  3. Enter details → click Start Auto Download")
        print("  4. This window will do the rest automatically")
        print()
        print("  Polling server for commands...")
        print("  " + "-"*56)

        errs = 0
        while True:
            cmd = await asyncio.get_event_loop().run_in_executor(None, _poll)
            if cmd is None:
                errs += 1
                if errs % 5 == 0: print(f"  WARNING: poll failing ({errs}x)")
                await asyncio.sleep(2); continue
            errs = 0
            action = cmd.get("action","idle")
            if action == "idle":
                await asyncio.sleep(1); continue
            result = await run_action(page, cmd)
            await asyncio.get_event_loop().run_in_executor(
                None, lambda r=result: _post(r))

        await browser.close()
    input("  Press Enter to close...")

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt:
        print("\n  Interrupted."); sys.exit(0)
