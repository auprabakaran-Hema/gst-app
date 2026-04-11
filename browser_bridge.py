"""
browser_bridge.py - Run this on YOUR PC
=======================================
Connects your local browser to the Render server for GST automation

HOW TO USE:
1. Update RENDER_SERVER below with your Render app URL
2. Install: pip install playwright websockets
3. Install browser: playwright install chromium
4. Run: python browser_bridge.py
5. Go to your Render app and click "Auto Download" tab
"""

import asyncio
import websockets
import json
import base64
import os
import sys
from playwright.sync_api import sync_playwright

# ═══════════════════════════════════════════════════════════════════
# UPDATE THIS WITH YOUR RENDER APP URL
# ═══════════════════════════════════════════════════════════════════
# Example: "wss://my-gst-app.onrender.com"
# Or: "wss://gst-recon-abc123.onrender.com"
RENDER_SERVER = "wss://YOUR-RENDER-APP-NAME.onrender.com"
# ═══════════════════════════════════════════════════════════════════


def get_websocket_url():
    """Convert Render URL to WebSocket URL"""
    url = RENDER_SERVER.strip()
    
    # Remove any trailing slashes
    url = url.rstrip('/')
    
    # Convert http/https to ws/wss
    if url.startswith("https://"):
        url = url.replace("https://", "wss://")
    elif url.startswith("http://"):
        url = url.replace("http://", "ws://")
    elif not url.startswith("ws://") and not url.startswith("wss://"):
        url = "wss://" + url
    
    # Add /ws path if not present
    if not url.endswith("/ws"):
        url += "/ws"
    
    return url


async def browser_handler():
    """Main browser bridge handler"""
    print("=" * 65)
    print("🖥️  GST Browser Bridge - PC Client")
    print("=" * 65)
    
    # Check if URL is configured
    if "YOUR-RENDER-APP" in RENDER_SERVER or not RENDER_SERVER:
        print("\n⚠️  ERROR: You need to update RENDER_SERVER!")
        print("\nOpen browser_bridge.py in a text editor and change:")
        print('  RENDER_SERVER = "wss://YOUR-RENDER-APP-NAME.onrender.com"')
        print("\nTo your actual Render URL, for example:")
        print('  RENDER_SERVER = "wss://my-gst-app.onrender.com"')
        print("\n" + "=" * 65)
        input("\nPress Enter to exit...")
        return
    
    ws_url = get_websocket_url()
    print(f"\n📱 Connecting to: {ws_url}")
    print("⏳ Starting browser...")
    
    with sync_playwright() as p:
        # Launch VISIBLE browser on YOUR PC
        try:
            browser = p.chromium.launch(
                headless=False,  # IMPORTANT: Must be False to see the browser!
                args=[
                    '--start-maximized',
                    '--disable-blink-features=AutomationControlled'
                ]
            )
        except Exception as e:
            print(f"\n❌ Failed to launch browser: {e}")
            print("\nMake sure you installed the browser:")
            print("  playwright install chromium")
            input("\nPress Enter to exit...")
            return
        
        context = browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = context.new_page()
        
        print("✅ Browser ready!")
        print("⏳ Waiting for commands from Render server...")
        print("=" * 65)
        
        try:
            async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                print("\n🔗 Connected to Render server!")
                print("🌐 GST Portal will open automatically when you start Auto Download")
                print("📝 You will type CAPTCHA when it appears on your screen")
                print("=" * 65)
                
                while True:
                    try:
                        message = await ws.recv()
                        data = json.loads(message)
                        action = data.get("action")
                        
                        # ── GOTO ─────────────────────────────────────────
                        if action == "goto":
                            url = data.get("url")
                            print(f"\n🌐 Navigating to: {url}")
                            try:
                                page.goto(url, wait_until="networkidle", timeout=60000)
                                await ws.send(json.dumps({"status": "done", "url": page.url}))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── FILL ─────────────────────────────────────────
                        elif action == "fill":
                            selector = data.get("selector")
                            value = data.get("value", "")
                            # Hide password in console output
                            display_value = "*" * len(value) if "pass" in selector.lower() else value
                            print(f"⌨️  Filling {selector}: {display_value}")
                            try:
                                page.fill(selector, value)
                                await ws.send(json.dumps({"status": "done"}))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── CLICK ────────────────────────────────────────
                        elif action == "click":
                            selector = data.get("selector")
                            print(f"🖱️  Clicking: {selector}")
                            try:
                                page.click(selector)
                                await ws.send(json.dumps({"status": "done"}))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── SCREENSHOT ───────────────────────────────────
                        elif action == "screenshot":
                            print("📸 Taking screenshot...")
                            try:
                                screenshot = page.screenshot(full_page=True)
                                encoded = base64.b64encode(screenshot).decode()
                                await ws.send(json.dumps({
                                    "status": "screenshot", 
                                    "image": encoded
                                }))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── WAIT FOR SELECTOR ────────────────────────────
                        elif action == "wait_for_selector":
                            selector = data.get("selector")
                            timeout = data.get("timeout", 30000)
                            print(f"⏳ Waiting for element: {selector}")
                            try:
                                page.wait_for_selector(selector, timeout=timeout)
                                await ws.send(json.dumps({"status": "found"}))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── GET TEXT ─────────────────────────────────────
                        elif action == "get_text":
                            selector = data.get("selector")
                            try:
                                text = page.inner_text(selector)
                                await ws.send(json.dumps({"status": "text", "text": text}))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── SELECT OPTION ────────────────────────────────
                        elif action == "select_option":
                            selector = data.get("selector")
                            value = data.get("value")
                            print(f"🔽 Selecting {value} in {selector}")
                            try:
                                page.select_option(selector, value)
                                await ws.send(json.dumps({"status": "done"}))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── GET URL ──────────────────────────────────────
                        elif action == "get_url":
                            await ws.send(json.dumps({
                                "status": "url", 
                                "url": page.url
                            }))
                        
                        # ── WAIT FOR NAVIGATION ──────────────────────────
                        elif action == "wait_for_navigation":
                            print("⏳ Waiting for page to load...")
                            try:
                                page.wait_for_load_state("networkidle", timeout=30000)
                                await ws.send(json.dumps({"status": "done", "url": page.url}))
                            except Exception as e:
                                await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        
                        # ── UNKNOWN ACTION ───────────────────────────────
                        else:
                            print(f"⚠️  Unknown action: {action}")
                            await ws.send(json.dumps({"status": "error", "error": f"Unknown action: {action}"}))
                            
                    except websockets.exceptions.ConnectionClosed:
                        print("\n❌ Connection to Render server closed")
                        break
                    except Exception as e:
                        print(f"\n❌ Error processing command: {e}")
                        try:
                            await ws.send(json.dumps({"status": "error", "error": str(e)}))
                        except:
                            pass
                        
        except websockets.exceptions.InvalidURI:
            print(f"\n❌ Invalid WebSocket URL: {ws_url}")
            print("\nPlease check your RENDER_SERVER setting.")
        except websockets.exceptions.ConnectionRefused:
            print(f"\n❌ Could not connect to: {ws_url}")
            print("\nPossible reasons:")
            print("  • The Render app is not running")
            print("  • The URL is incorrect")
            print("  • Your internet connection is down")
        except Exception as e:
            print(f"\n❌ Connection error: {e}")
        finally:
            print("\n🔒 Closing browser...")
            browser.close()
            print("✅ Browser closed. Goodbye!")


def main():
    """Entry point"""
    try:
        asyncio.run(browser_handler())
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user. Exiting...")
        sys.exit(0)


if __name__ == "__main__":
    main()
