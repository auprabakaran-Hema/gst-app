"""
GST Portal Downloader v3 — Server + Desktop Compatible
=======================================================
Browser priority:
  1. Edge (local Windows/Mac)
  2. Chrome/Chromium (local or Linux server via apt/snap)
  3. selenium-manager auto-download (selenium 4.10+)

Navigation: Services → Returns → Returns Dashboard (proper menu flow)
"""
import os, time, json, re, shutil, platform, logging
from pathlib import Path
from typing import Optional, Dict

log = logging.getLogger(__name__)

# ── Known browser binary locations ────────────────────────────────
_CHROME_BINS = [
    "/usr/bin/chromium-browser",      # Ubuntu/Debian apt
    "/usr/bin/chromium",              # Some distros
    "/usr/bin/google-chrome-stable",  # Chrome stable
    "/usr/bin/google-chrome",         # Chrome
    "/snap/bin/chromium",             # Snap
    "/usr/local/bin/chromium",        # Manual install
]
_EDGE_BINS = [
    "/usr/bin/microsoft-edge-stable",
    "/usr/bin/microsoft-edge",
    "/snap/bin/microsoft-edge",
]
_CHROMEDRIVER_BINS = [
    "/usr/bin/chromedriver",
    "/usr/lib/chromium-browser/chromedriver",
    "/usr/lib/chromium/chromedriver",
    "/snap/bin/chromium.chromedriver",
]


def _find_binary(candidates):
    """Return first existing executable from the list."""
    for b in candidates:
        if b.startswith("/"):
            if os.path.isfile(b) and os.access(b, os.X_OK):
                return b
        else:
            found = shutil.which(b)
            if found:
                return found
    return None


def _chrome_options(download_dir, binary=None, headless=True):
    from selenium.webdriver.chrome.options import Options
    opts = Options()
    if binary:
        opts.binary_location = binary
    opts.add_experimental_option("prefs", {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    if headless:
        opts.add_argument("--headless=new")
    for arg in [
        "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
        "--window-size=1920,1080", "--disable-blink-features=AutomationControlled",
        "--disable-extensions", "--disable-setuid-sandbox",
        "--remote-debugging-port=0", "--disable-background-networking",
    ]:
        opts.add_argument(arg)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return opts


def _edge_options(download_dir, binary=None, headless=True):
    from selenium.webdriver.edge.options import Options
    opts = Options()
    if binary:
        opts.binary_location = binary
    opts.add_experimental_option("prefs", {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    if headless:
        opts.add_argument("--headless=new")
    for arg in [
        "--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu",
        "--window-size=1920,1080", "--disable-blink-features=AutomationControlled",
        "--disable-extensions", "--disable-setuid-sandbox", "--remote-debugging-port=0",
    ]:
        opts.add_argument(arg)
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    return opts


def _make_driver(download_dir: str):
    """
    Start a browser. Tries 4 strategies in order:
      1. Edge (local)
      2. Chrome/Chromium with explicit binary path
      3. Chromium from apt/snap (Render/Ubuntu servers)
      4. selenium-manager auto-download (selenium 4.10+)

    On Linux servers (no DISPLAY), forces headless mode.
    Returns (driver, browser_name).
    """
    from selenium import webdriver as wd

    # Detect if we're on a headless server (Linux without GUI)
    on_server = (platform.system() == "Linux"
                 and not os.environ.get("DISPLAY", "")
                 and not os.environ.get("WAYLAND_DISPLAY", ""))
    headless_env = os.environ.get("GST_HEADLESS", "").lower()
    headless = True if on_server else (headless_env == "true")
    log.info(f"Platform: {platform.system()} | Server mode: {on_server} | Headless: {headless}")

    errors = []

    # ── Strategy 1: Edge ──────────────────────────────────────────
    if platform.system() != "Linux" or _find_binary(_EDGE_BINS):
        edge_bin = _find_binary(_EDGE_BINS)
        try:
            from selenium.webdriver.edge.service import Service as ES
            opts = _edge_options(download_dir, binary=edge_bin, headless=headless)
            try:
                from webdriver_manager.microsoft import EdgeChromiumDriverManager
                svc = ES(EdgeChromiumDriverManager().install())
            except Exception:
                svc = ES()
            driver = wd.Edge(service=svc, options=opts)
            driver.execute_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            driver.implicitly_wait(8)
            log.info(f"Started: Edge ({edge_bin or 'system'})")
            return driver, "Edge"
        except Exception as ex:
            errors.append(f"Edge: {ex}")
            log.warning(f"Edge failed: {ex}")

    # ── Strategy 2: Chrome with webdriver-manager ─────────────────
    chrome_bin = _find_binary(_CHROME_BINS)
    if chrome_bin:
        try:
            from selenium.webdriver.chrome.service import Service as CS
            opts = _chrome_options(download_dir, binary=chrome_bin, headless=headless)
            try:
                from webdriver_manager.chrome import ChromeDriverManager
                svc = CS(ChromeDriverManager().install())
            except Exception:
                # Try local chromedriver
                cd = _find_binary(_CHROMEDRIVER_BINS)
                svc = CS(executable_path=cd) if cd else CS()
            driver = wd.Chrome(service=svc, options=opts)
            driver.execute_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            driver.implicitly_wait(8)
            log.info(f"Started: Chrome ({chrome_bin})")
            return driver, "Chrome"
        except Exception as ex:
            errors.append(f"Chrome({chrome_bin}): {ex}")
            log.warning(f"Chrome({chrome_bin}) failed: {ex}")

    # ── Strategy 3: Chromium with local chromedriver (apt install) ─
    for cb in ["/usr/bin/chromium-browser", "/usr/bin/chromium", "/snap/bin/chromium"]:
        if not os.path.exists(cb):
            continue
        cd = _find_binary(_CHROMEDRIVER_BINS)
        try:
            from selenium.webdriver.chrome.service import Service as CS
            opts = _chrome_options(download_dir, binary=cb, headless=True)
            svc = CS(executable_path=cd) if cd else CS()
            driver = wd.Chrome(service=svc, options=opts)
            driver.execute_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
            driver.implicitly_wait(8)
            log.info(f"Started: Chromium ({cb}) with chromedriver={cd}")
            return driver, "Chromium"
        except Exception as ex:
            errors.append(f"Chromium({cb}+{cd}): {ex}")
            log.warning(f"Chromium({cb}) failed: {ex}")

    # ── Strategy 4: selenium-manager auto (selenium 4.10+) ─────────
    try:
        from selenium.webdriver.chrome.service import Service as CS
        opts = _chrome_options(download_dir, binary=None, headless=headless)
        driver = wd.Chrome(options=opts)  # selenium-manager auto-downloads
        driver.execute_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        driver.implicitly_wait(8)
        log.info("Started: Chrome (selenium-manager auto-download)")
        return driver, "Chrome-auto"
    except Exception as ex:
        errors.append(f"Chrome-auto: {ex}")
        log.warning(f"Chrome-auto failed: {ex}")

    # ── All failed ─────────────────────────────────────────────────
    err_list = "\n  ".join(errors)
    raise RuntimeError(
        f"No browser could be started.\n\n"
        f"For Render.com / Ubuntu server, add to build command:\n"
        f"  apt-get update && apt-get install -y chromium-browser chromium-driver\n\n"
        f"For local Windows, install Edge or Chrome.\n\n"
        f"Errors:\n  {err_list}"
    )


# ── Portal constants ───────────────────────────────────────────────
PORTAL_LOGIN   = "https://services.gst.gov.in/services/login"
PORTAL_WELCOME = "https://services.gst.gov.in/services/auth/fowelcome"
RETURNS_DASH   = "https://return.gst.gov.in/returns/auth/dashboard"

QUARTER_MAP = {
    "April":"Quarter 1 (Apr - Jun)","May":"Quarter 1 (Apr - Jun)","June":"Quarter 1 (Apr - Jun)",
    "July":"Quarter 2 (Jul - Sep)","August":"Quarter 2 (Jul - Sep)","September":"Quarter 2 (Jul - Sep)",
    "October":"Quarter 3 (Oct - Dec)","November":"Quarter 3 (Oct - Dec)","December":"Quarter 3 (Oct - Dec)",
    "January":"Quarter 4 (Jan - Mar)","February":"Quarter 4 (Jan - Mar)","March":"Quarter 4 (Jan - Mar)",
}


def _try_click(driver, xpaths, timeout=8):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    for xp in xpaths:
        try:
            el = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xp)))
            driver.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            continue
    return False


def _go_to_returns_dashboard(driver, page_wait=10):
    """Navigate via Services → Returns → Returns Dashboard."""
    from selenium.webdriver.common.by import By
    cur = driver.current_url
    if "return.gst.gov.in" in cur and "dashboard" in cur:
        log.info("    Already on Returns Dashboard ✓")
        return True

    log.info("    Navigating: Services → Returns → Returns Dashboard")

    if "gst.gov.in" not in cur:
        driver.get(PORTAL_WELCOME); time.sleep(page_wait)

    # Step 1: Services
    for _ in range(3):
        ok = _try_click(driver, [
            "//a[normalize-space(text())='Services']",
            "//nav//a[normalize-space()='Services']",
        ])
        if ok: log.info("    Services ✓"); break
        try:
            driver.execute_script("""
                var ll=document.querySelectorAll('a,li');
                for(var i=0;i<ll.length;i++){
                    var t=(ll[i].innerText||ll[i].textContent||'').trim();
                    if(t==='Services'){ll[i].dispatchEvent(new MouseEvent('mouseover',{bubbles:true}));ll[i].click();break;}}""")
        except: pass
        time.sleep(1.5)
    time.sleep(1.5)

    # Step 2: Returns
    for _ in range(3):
        ok = _try_click(driver, [
            "//a[normalize-space(text())='Returns']",
            "//*[contains(@class,'open')]//a[normalize-space()='Returns']",
        ])
        if ok: log.info("    Returns ✓"); break
        try:
            driver.execute_script("""
                var ll=document.querySelectorAll('a');
                for(var i=0;i<ll.length;i++){
                    var t=(ll[i].innerText||'').trim();
                    if(t==='Returns'&&ll[i].offsetParent!==null){ll[i].click();break;}}""")
        except: pass
        time.sleep(1.5)
    time.sleep(1.5)

    # Step 3: Returns Dashboard
    found = False
    for _ in range(3):
        ok = _try_click(driver, [
            "//a[contains(normalize-space(text()),'Returns Dashboard')]",
        ])
        if ok: log.info("    Returns Dashboard ✓"); found = True; break
        try:
            for el in driver.find_elements(By.TAG_NAME, "a"):
                if "Returns Dashboard" in (el.text or "") and el.is_displayed():
                    driver.execute_script("arguments[0].click();", el)
                    found = True; break
            if found: break
        except: pass
        time.sleep(1.5)

    time.sleep(page_wait)
    final_url = driver.current_url
    log.info(f"    URL: {final_url}")

    if "dashboard" not in final_url or "return.gst.gov.in" not in final_url:
        log.warning("    Direct URL fallback")
        driver.get(RETURNS_DASH); time.sleep(page_wait + 2)
        final_url = driver.current_url

    if "accessdenied" in final_url.lower():
        time.sleep(10); driver.get(RETURNS_DASH); time.sleep(page_wait)
        final_url = driver.current_url

    return "accessdenied" not in final_url.lower()


def _select_and_search(driver, fy_label, month_name, page_wait=10):
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select
    log.info(f"    FY={fy_label} | Quarter={QUARTER_MAP.get(month_name,'')} | Period={month_name}")
    time.sleep(3)

    sels = driver.find_elements(By.TAG_NAME, "select")
    # FY
    for sel_el in sels:
        try:
            s = Select(sel_el)
            opts = [o.text.strip() for o in s.options]
            if any("-" in o and len(o) <= 9 for o in opts):
                for opt in s.options:
                    if fy_label in opt.text:
                        s.select_by_visible_text(opt.text)
                        log.info(f"    FY: {opt.text} ✓"); break
                break
        except: continue
    time.sleep(1)

    sels = driver.find_elements(By.TAG_NAME, "select")
    qtr = QUARTER_MAP.get(month_name, "")
    for sel_el in sels:
        try:
            s = Select(sel_el)
            opts = [o.text.strip() for o in s.options]
            if any("quarter" in o.lower() for o in opts):
                for opt in s.options:
                    if qtr[:9].lower() in opt.text.lower():
                        s.select_by_visible_text(opt.text)
                        log.info(f"    Quarter: {opt.text} ✓"); break
                break
        except: continue
    time.sleep(1)

    sels = driver.find_elements(By.TAG_NAME, "select")
    MNS = ["january","february","march","april","may","june",
           "july","august","september","october","november","december"]
    for sel_el in sels:
        try:
            s = Select(sel_el)
            opts = [o.text.strip() for o in s.options]
            if any(m in " ".join(opts).lower() for m in MNS):
                for opt in s.options:
                    if month_name.lower() in opt.text.lower():
                        s.select_by_visible_text(opt.text)
                        log.info(f"    Period: {opt.text} ✓"); break
                break
        except: continue
    time.sleep(1)

    ok = _try_click(driver, [
        "//button[normalize-space()='SEARCH']",
        "//button[normalize-space()='Search']",
        "//button[contains(text(),'SEARCH')]",
    ])
    if not ok:
        try:
            driver.execute_script("""
                var bb=document.querySelectorAll('button,input[type=submit]');
                for(var i=0;i<bb.length;i++){
                    var t=(bb[i].innerText||bb[i].value||'').toUpperCase().trim();
                    if(t==='SEARCH'||t.endsWith('SEARCH')){bb[i].click();break;}}""")
        except: pass
    time.sleep(page_wait + 2)
    log.info("    Tiles loaded ✓")


def _click_tile(driver, rtype):
    from selenium.webdriver.common.by import By
    ALIASES = {
        "GSTR1": ["GSTR1","GSTR-1"],
        "GSTR2B": ["GSTR2B","GSTR-2B"],
        "GSTR2A": ["GSTR2A","GSTR-2A"],
        "GSTR3B": ["GSTR3B","GSTR-3B"],
    }
    targets = ALIASES.get(rtype.upper(), [rtype])
    try:
        tiles = driver.find_elements(By.CSS_SELECTOR,
            ".card,.tile,.return-tile,[class*='tile'],[class*='card']")
        for tile in tiles:
            txt = (tile.text or "").replace(" ","").replace("-","").upper()
            if any(t.replace("-","").upper() in txt for t in targets):
                for btn in tile.find_elements(By.TAG_NAME, "button"):
                    if "DOWNLOAD" in (btn.text or "").upper():
                        driver.execute_script("arguments[0].click();", btn)
                        log.info(f"    {rtype} DOWNLOAD ✓")
                        return True
    except Exception as ex:
        log.warning(f"    Tile error: {ex}")
    log.warning(f"    {rtype} tile not found")
    return False


def _wait_file(dl_dir, exts, before=None, timeout=120):
    before_set = set(before or [])
    t0 = time.time()
    while time.time() - t0 < timeout:
        for ext in exts:
            for f in Path(dl_dir).glob(f"*{ext}"):
                if str(f) not in before_set and not f.name.endswith(".crdownload"):
                    return f
        time.sleep(2)
    return None


def _mv(src: Path, dst: Path):
    try:
        if dst.exists(): dst.unlink()
        src.rename(dst)
    except OSError:
        shutil.copy2(str(src), str(dst))
        try: src.unlink()
        except: pass


# ════════════════════════════════════════════════════════════════
class GSTPortalDownloader:
    """
    Downloads GSTR-1/2A/2B/3B from GST India Portal.
    Browser: Edge first → Chrome → Chromium (server) → selenium-manager auto.
    Navigation: Services → Returns → Returns Dashboard → select period → SEARCH.
    """

    def __init__(self, username, password, download_dir=None,
                 page_wait=10, captcha_wait=30):
        self.username     = username
        self.password     = password
        self.download_dir = Path(download_dir or (Path.home()/"Downloads"/"GST_Downloads"))
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.page_wait    = page_wait
        self.captcha_wait = captcha_wait
        self.driver       = None
        self.browser_name = None
        self.fy_label     = "2025-26"

    def _ensure_driver(self):
        if not self.driver:
            self.driver, self.browser_name = _make_driver(str(self.download_dir))

    def login(self) -> bool:
        self._ensure_driver()
        try:
            log.info("Opening GST Portal...")
            self.driver.get(PORTAL_LOGIN)
            time.sleep(3)

            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            wait = WebDriverWait(self.driver, 20)

            # Username
            try:
                un = wait.until(EC.presence_of_element_located((By.ID, "username")))
            except:
                un = self.driver.find_element(By.NAME, "username")
            un.clear(); un.send_keys(self.username)
            log.info("Username ✓")

            # Password
            for pid in ("user_pass","password","passwd"):
                try: pw = self.driver.find_element(By.ID, pid); break
                except: continue
            else:
                pw = self.driver.find_element(By.NAME, "password")
            pw.clear(); pw.send_keys(self.password)
            log.info("Password ✓")

            log.info(f"Waiting {self.captcha_wait}s for CAPTCHA...")
            time.sleep(self.captcha_wait)

            ok = _try_click(self.driver, [
                "//button[@type='submit']",
                "//input[@type='submit']",
                "//button[contains(text(),'LOGIN')]",
                "//button[contains(text(),'Login')]",
            ])
            if not ok:
                try:
                    self.driver.execute_script("""
                        var bb=document.querySelectorAll('button,input[type=submit]');
                        for(var b of bb){
                            var t=(b.type||'').toLowerCase();
                            var txt=(b.innerText||'').toUpperCase();
                            if(t==='submit'||txt.includes('LOGIN')){b.click();break;}}""")
                except: pass

            time.sleep(self.page_wait)
            cur = self.driver.current_url
            log.info(f"Post-login URL: {cur}")
            if any(k in cur.lower() for k in ("fowelcome","dashboard","home","services/auth")):
                log.info("Login successful ✓"); return True
            log.error("Login failed — check credentials/CAPTCHA")
            return False
        except Exception as ex:
            log.error(f"Login error: {ex}"); return False

    def download_month(self, month_name, year, returns_todo=None):
        if returns_todo is None:
            returns_todo = ["GSTR1","GSTR2B","GSTR2A","GSTR3B"]
        results = {}
        for rtype in returns_todo:
            log.info(f"\n  [{rtype}] {month_name} {year}")
            before = {str(f) for f in self.download_dir.iterdir() if f.is_file()}
            try:
                _go_to_returns_dashboard(self.driver, self.page_wait)
                _select_and_search(self.driver, self.fy_label, month_name, self.page_wait)
                if not _click_tile(self.driver, rtype):
                    results[rtype] = None; continue
                time.sleep(5)

                if rtype in ("GSTR1","GSTR1A"):
                    _try_click(self.driver, [
                        "//button[contains(text(),'GENERATE JSON FILE TO DOWNLOAD')]",
                        "//button[contains(text(),'GENERATE JSON')]",
                        "//button[contains(text(),'GENERATE')]",
                    ])
                    time.sleep(3); exts = [".zip"]
                elif rtype == "GSTR2B":
                    _try_click(self.driver, [
                        "//button[contains(text(),'GENERATE EXCEL FILE TO DOWNLOAD')]",
                        "//button[contains(text(),'GENERATE EXCEL')]",
                    ])
                    time.sleep(3); exts = [".xlsx", ".zip"]
                elif rtype == "GSTR2A":
                    _try_click(self.driver, [
                        "//button[contains(text(),'GENERATE EXCEL')]",
                    ])
                    time.sleep(3); exts = [".xlsx", ".zip"]
                else:
                    exts = [".pdf"]

                f = _wait_file(self.download_dir, exts, before=before, timeout=120)
                if f:
                    std = {
                        "GSTR1":  f"GSTR1_{month_name}_{year}.zip",
                        "GSTR2B": f"GSTR2B_{month_name}_{year}.xlsx",
                        "GSTR2A": f"GSTR2A_{month_name}_{year}.zip",
                        "GSTR3B": f"GSTR3B_{month_name}_{year}.pdf",
                    }.get(rtype, f"{rtype}_{month_name}_{year}{f.suffix}")
                    dest = self.download_dir / std
                    _mv(f, dest)
                    log.info(f"  ✓ {dest.name}")
                    results[rtype] = dest
                else:
                    log.warning(f"  ✗ {rtype}: timeout")
                    results[rtype] = None
            except Exception as ex:
                log.error(f"  ✗ {rtype} error: {ex}")
                results[rtype] = None
        return results

    def _get_periods_for_fy(self, fy):
        """Compatibility shim for app.py."""
        s = int(fy.split("-")[0]); e = s+1
        months = [
            ("April","APR",s),("May","MAY",s),("June","JUN",s),
            ("July","JUL",s),("August","AUG",s),("September","SEP",s),
            ("October","OCT",s),("November","NOV",s),("December","DEC",s),
            ("January","JAN",e),("February","FEB",e),("March","MAR",e),
        ]
        return [(mn, f"{cd}{yr}", f"{str(i+4 if i<9 else i-8).zfill(2)}{yr}")
                for i,(mn,cd,yr) in enumerate(months)]

    def close(self):
        if self.driver:
            try: self.driver.quit()
            except: pass
            self.driver = None
