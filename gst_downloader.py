"""
GST Portal Downloader v2
========================
- Edge browser first, Chrome as fallback
- Proper navigation: Services → Returns → Returns Dashboard
- Robust FY/Quarter/Period selection
- Auto-retry on failures
- Headless mode support for server deployment
"""
import os, time, json, re, shutil, logging
from pathlib import Path
from typing import Optional, List, Dict

log = logging.getLogger(__name__)

# ── Browser setup ─────────────────────────────────────────────────
def _make_driver(download_dir: str):
    """
    Try Edge first, then Chrome.
    Returns (driver, browser_name) or raises.
    """
    from selenium.webdriver.support.ui import WebDriverWait

    headless = os.environ.get("GST_HEADLESS", "false").lower() == "true"

    def _edge_driver():
        from selenium import webdriver as wd
        from selenium.webdriver.edge.options import Options
        from selenium.webdriver.edge.service import Service
        try:
            from webdriver_manager.microsoft import EdgeChromiumDriverManager
            svc = Service(EdgeChromiumDriverManager().install())
        except Exception:
            svc = Service()
        opts = Options()
        opts.add_experimental_option("prefs", {
            "download.default_directory": str(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        })
        if headless:
            opts.add_argument("--headless=new")
        for arg in ("--no-sandbox","--disable-dev-shm-usage",
                    "--disable-gpu","--window-size=1920,1080",
                    "--disable-blink-features=AutomationControlled"):
            opts.add_argument(arg)
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        driver = wd.Edge(service=svc, options=opts)
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        return driver

    def _chrome_driver():
        from selenium import webdriver as wd
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            svc = Service(ChromeDriverManager().install())
        except Exception:
            svc = Service()
        opts = Options()
        opts.add_experimental_option("prefs", {
            "download.default_directory": str(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
        })
        if headless:
            opts.add_argument("--headless=new")
        for arg in ("--no-sandbox","--disable-dev-shm-usage",
                    "--disable-gpu","--window-size=1920,1080",
                    "--disable-blink-features=AutomationControlled"):
            opts.add_argument(arg)
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        driver = wd.Chrome(service=svc, options=opts)
        driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
        return driver

    # Try Edge first, fall back to Chrome
    for name, factory in [("Edge", _edge_driver), ("Chrome", _chrome_driver)]:
        try:
            driver = factory()
            driver.implicitly_wait(8)
            log.info(f"Browser started: {name}")
            return driver, name
        except Exception as ex:
            log.warning(f"{name} failed: {ex}")

    raise RuntimeError(
        "No browser available. Install Microsoft Edge or Google Chrome, "
        "and ensure webdriver-manager is installed (pip install webdriver-manager)."
    )


# ── Helper: try_click ─────────────────────────────────────────────
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


# ── Portal navigation ─────────────────────────────────────────────
PORTAL_LOGIN   = "https://services.gst.gov.in/services/login"
PORTAL_WELCOME = "https://services.gst.gov.in/services/auth/fowelcome"
RETURNS_DASH   = "https://return.gst.gov.in/returns/auth/dashboard"

QUARTER_MAP = {
    "April":"Quarter 1 (Apr - Jun)","May":"Quarter 1 (Apr - Jun)","June":"Quarter 1 (Apr - Jun)",
    "July":"Quarter 2 (Jul - Sep)","August":"Quarter 2 (Jul - Sep)","September":"Quarter 2 (Jul - Sep)",
    "October":"Quarter 3 (Oct - Dec)","November":"Quarter 3 (Oct - Dec)","December":"Quarter 3 (Oct - Dec)",
    "January":"Quarter 4 (Jan - Mar)","February":"Quarter 4 (Jan - Mar)","March":"Quarter 4 (Jan - Mar)",
}

def _go_to_returns_dashboard(driver, page_wait=10):
    """Navigate via Services → Returns → Returns Dashboard menu."""
    from selenium.webdriver.common.by import By
    cur = driver.current_url
    if "return.gst.gov.in" in cur and "dashboard" in cur:
        log.info("    Already on Returns Dashboard ✓"); return True

    log.info("    Navigating: Services → Returns → Returns Dashboard")

    # Step 0: ensure on portal
    if "gst.gov.in" not in cur:
        driver.get(PORTAL_WELCOME); time.sleep(page_wait)

    # Step 1: Click Services
    log.info("    Step 1: Services menu...")
    for attempt in range(3):
        ok = _try_click(driver, [
            "//a[normalize-space(text())='Services']",
            "//nav//a[normalize-space()='Services']",
            "//ul[contains(@class,'nav')]//a[contains(text(),'Services')]",
        ])
        if ok: log.info("    Services ✓"); break
        try:
            driver.execute_script("""
                var links=document.querySelectorAll('a,li');
                for(var i=0;i<links.length;i++){
                    var t=(links[i].innerText||links[i].textContent||'').trim();
                    if(t==='Services'){
                        links[i].dispatchEvent(new MouseEvent('mouseover',{bubbles:true}));
                        links[i].click(); break;}}""")
        except: pass
        time.sleep(1.5)
    time.sleep(1.5)

    # Step 2: Click Returns
    log.info("    Step 2: Returns...")
    for attempt in range(3):
        ok = _try_click(driver, [
            "//a[normalize-space(text())='Returns']",
            "//*[contains(@class,'dropdown-menu')]//a[normalize-space()='Returns']",
            "//*[contains(@class,'open')]//a[normalize-space()='Returns']",
        ])
        if ok: log.info("    Returns ✓"); break
        try:
            driver.execute_script("""
                var links=document.querySelectorAll('a');
                for(var i=0;i<links.length;i++){
                    var t=(links[i].innerText||'').trim();
                    if(t==='Returns'&&links[i].offsetParent!==null){links[i].click();break;}}""")
        except: pass
        time.sleep(1.5)
    time.sleep(1.5)

    # Step 3: Click Returns Dashboard
    log.info("    Step 3: Returns Dashboard...")
    found = False
    for attempt in range(3):
        ok = _try_click(driver, [
            "//a[contains(normalize-space(text()),'Returns Dashboard')]",
            "//li//a[contains(@href,'dashboard')]",
        ])
        if ok: log.info("    Returns Dashboard ✓"); found = True; break
        try:
            from selenium.webdriver.common.by import By
            for el in driver.find_elements(By.TAG_NAME, "a"):
                try:
                    if "Returns Dashboard" in (el.text or "") and el.is_displayed():
                        driver.execute_script("arguments[0].click();", el)
                        found = True; break
                except: continue
            if found: break
        except: pass
        time.sleep(1.5)

    time.sleep(page_wait)
    final_url = driver.current_url
    log.info(f"    URL: {final_url}")

    # Last resort
    if "dashboard" not in final_url or "return.gst.gov.in" not in final_url:
        log.warning("    Using direct URL (last resort)")
        driver.get(RETURNS_DASH); time.sleep(page_wait + 2)
        final_url = driver.current_url

    if "accessdenied" in final_url.lower():
        log.warning("    Access Denied — retrying after wait...")
        time.sleep(10)
        driver.get(RETURNS_DASH); time.sleep(page_wait)
        final_url = driver.current_url

    return "accessdenied" not in final_url.lower()


def _select_period_and_search(driver, fy_label, month_name, page_wait=10):
    """Select FY, Quarter, Period dropdowns then click SEARCH."""
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import Select
    log.info(f"    Setting FY={fy_label} Quarter={QUARTER_MAP.get(month_name,'')} Period={month_name}")
    time.sleep(3)

    sels = driver.find_elements(By.TAG_NAME, "select")
    log.info(f"    Found {len(sels)} dropdowns")

    # FY
    for sel_el in sels:
        try:
            s = Select(sel_el)
            opts = [o.text.strip() for o in s.options]
            if any("-" in o and len(o)<=9 for o in opts):
                for opt in s.options:
                    if fy_label in opt.text:
                        s.select_by_visible_text(opt.text)
                        log.info(f"    FY: {opt.text} ✓"); break
                break
        except: continue
    time.sleep(1)

    sels = driver.find_elements(By.TAG_NAME, "select")
    # Quarter
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
    # Period
    MONTHS_LOWER = ["january","february","march","april","may","june",
                    "july","august","september","october","november","december"]
    for sel_el in sels:
        try:
            s = Select(sel_el)
            opts = [o.text.strip() for o in s.options]
            if any(m in " ".join(opts).lower() for m in MONTHS_LOWER):
                for opt in s.options:
                    if month_name.lower() in opt.text.lower():
                        s.select_by_visible_text(opt.text)
                        log.info(f"    Period: {opt.text} ✓"); break
                break
        except: continue
    time.sleep(1)

    # SEARCH button
    ok = _try_click(driver, [
        "//button[normalize-space()='SEARCH']",
        "//button[normalize-space()='Search']",
        "//button[contains(text(),'SEARCH')]",
        "//input[@value='SEARCH']",
    ])
    if not ok:
        try:
            driver.execute_script("""
                var btns=document.querySelectorAll('button,input[type=submit]');
                for(var i=0;i<btns.length;i++){
                    var t=(btns[i].innerText||btns[i].value||'').toUpperCase().trim();
                    if(t==='SEARCH'||t.endsWith('SEARCH')){btns[i].click();break;}}""")
            log.info("    SEARCH clicked via JS")
        except: pass

    time.sleep(page_wait + 2)
    log.info("    Tiles loaded ✓")


def _click_tile_download(driver, return_type):
    """Find the tile for return_type and click its DOWNLOAD button."""
    from selenium.webdriver.common.by import By
    TILE_TEXTS = {
        "GSTR1":  ["GSTR1", "GSTR-1"],
        "GSTR2B": ["GSTR2B", "GSTR-2B"],
        "GSTR2A": ["GSTR2A", "GSTR-2A"],
        "GSTR3B": ["GSTR3B", "GSTR-3B"],
    }
    targets = TILE_TEXTS.get(return_type.upper(), [return_type])
    try:
        tiles = driver.find_elements(By.CSS_SELECTOR,
            ".card, .tile, .return-tile, [class*='tile'], [class*='card']")
        for tile in tiles:
            txt = (tile.text or "").replace(" ","").replace("-","").upper()
            if any(t.replace("-","").upper() in txt for t in targets):
                # Find DOWNLOAD button within tile
                for btn in tile.find_elements(By.TAG_NAME, "button"):
                    if "DOWNLOAD" in (btn.text or "").upper():
                        driver.execute_script("arguments[0].click();", btn)
                        log.info(f"    {return_type} DOWNLOAD clicked ✓")
                        return True
                # Try any link with DOWNLOAD text
                for a in tile.find_elements(By.TAG_NAME, "a"):
                    if "DOWNLOAD" in (a.text or "").upper():
                        driver.execute_script("arguments[0].click();", a)
                        return True
    except Exception as ex:
        log.warning(f"    Tile click error: {ex}")
    log.warning(f"    {return_type} tile not found")
    return False


def _wait_for_file(download_dir, extensions, before_files=None, timeout=90):
    """Wait for a new file with given extension to appear."""
    before = set(before_files or [])
    start = time.time()
    while time.time() - start < timeout:
        for ext in extensions:
            for f in Path(download_dir).glob(f"*{ext}"):
                if str(f) not in before and not f.name.endswith(".crdownload"):
                    return f
        time.sleep(2)
    return None


def _rename_file(src: Path, dest: Path):
    try:
        if dest.exists(): dest.unlink()
        src.rename(dest)
    except OSError:
        shutil.copy2(str(src), str(dest))
        try: src.unlink()
        except: pass


# ════════════════════════════════════════════════════════════════
# Main downloader class
# ════════════════════════════════════════════════════════════════
class GSTPortalDownloader:
    """
    Automates GSTR-1, GSTR-2B, GSTR-2A, GSTR-3B downloads from GST India Portal.
    Browser: Edge first, Chrome fallback.
    Navigation: Services → Returns → Returns Dashboard → select period → SEARCH → tile download.
    """

    def __init__(self, username: str, password: str, download_dir: str = None,
                 page_wait: int = 10, captcha_wait: int = 30):
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
        """Open GST Portal login page, fill credentials, wait for CAPTCHA, submit."""
        self._ensure_driver()
        try:
            log.info("Opening GST Portal login page...")
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
            log.info("Username entered ✓")

            # Password
            for pid in ("user_pass", "password", "passwd"):
                try:
                    pw = self.driver.find_element(By.ID, pid); break
                except: continue
            else:
                pw = self.driver.find_element(By.NAME, "password")
            pw.clear(); pw.send_keys(self.password)
            log.info("Password entered ✓")

            # CAPTCHA — wait for manual entry
            log.info(f"Waiting {self.captcha_wait}s for CAPTCHA entry...")
            time.sleep(self.captcha_wait)

            # Click Login
            ok = _try_click(self.driver, [
                "//button[@type='submit']",
                "//input[@type='submit']",
                "//button[contains(text(),'LOGIN')]",
                "//button[contains(text(),'Login')]",
            ])
            if not ok:
                try:
                    self.driver.execute_script("""
                        var btns=document.querySelectorAll('button,input[type=submit]');
                        for(var b of btns){
                            if((b.type||'').toLowerCase()==='submit'||(b.innerText||'').toUpperCase().includes('LOGIN')){
                                b.click();break;}}""")
                except: pass

            time.sleep(self.page_wait)
            cur = self.driver.current_url
            log.info(f"Post-login URL: {cur}")

            # Check success
            if any(kw in cur.lower() for kw in ("fowelcome","dashboard","home","services/auth")):
                log.info("Login successful ✓")
                return True

            # Check for error message
            try:
                from selenium.webdriver.common.by import By
                err = self.driver.find_element(By.CSS_SELECTOR, ".alert-danger,.error-msg,.err-msg")
                log.error(f"Login failed: {err.text}")
            except:
                log.error("Login may have failed — check credentials and CAPTCHA")
            return False

        except Exception as ex:
            log.error(f"Login error: {ex}")
            return False

    def _get_existing_files(self):
        return {str(f) for f in self.download_dir.iterdir() if f.is_file()}

    def _safe_navigate(self, month_name):
        """Navigate to Returns Dashboard then select period."""
        ok = _go_to_returns_dashboard(self.driver, self.page_wait)
        if not ok:
            log.warning("Dashboard nav failed, retrying once...")
            time.sleep(5)
            ok = _go_to_returns_dashboard(self.driver, self.page_wait)
        _select_period_and_search(self.driver, self.fy_label, month_name, self.page_wait)

    def download_month(self, month_name: str, year: str,
                       returns_todo=None) -> Dict[str, Optional[Path]]:
        """
        Download all (or selected) returns for one month.
        returns_todo: list like ["GSTR1","GSTR2B","GSTR2A","GSTR3B"] or None=all
        Returns dict: {return_type: Path or None}
        """
        if returns_todo is None:
            returns_todo = ["GSTR1", "GSTR2B", "GSTR2A", "GSTR3B"]

        results = {}

        for rtype in returns_todo:
            log.info(f"\n  -- {rtype} {month_name} {year} --")
            before = self._get_existing_files()
            try:
                # Navigate to dashboard + select period
                self._safe_navigate(month_name)

                # Click tile DOWNLOAD
                if not _click_tile_download(self.driver, rtype):
                    log.warning(f"  {rtype}: tile not found")
                    results[rtype] = None; continue

                time.sleep(5)

                # For GSTR-1/2A: click GENERATE JSON/EXCEL then poll for link
                if rtype in ("GSTR1", "GSTR1A"):
                    # Click GENERATE JSON
                    _try_click(self.driver, [
                        "//button[contains(text(),'GENERATE JSON FILE TO DOWNLOAD')]",
                        "//button[contains(text(),'GENERATE JSON')]",
                        "//button[contains(text(),'Generate JSON')]",
                        "//button[contains(text(),'GENERATE')]",
                    ])
                    time.sleep(3)
                    # Poll for download link (max 3 min)
                    ext = [".zip"]
                elif rtype == "GSTR2B":
                    # GSTR-2B: click GENERATE EXCEL
                    _try_click(self.driver, [
                        "//button[contains(text(),'GENERATE EXCEL FILE TO DOWNLOAD')]",
                        "//button[contains(text(),'GENERATE EXCEL')]",
                        "//button[contains(text(),'Generate Excel')]",
                    ])
                    time.sleep(3)
                    ext = [".xlsx", ".xls", ".zip"]
                elif rtype == "GSTR2A":
                    # GSTR-2A: generate excel
                    _try_click(self.driver, [
                        "//button[contains(text(),'GENERATE EXCEL')]",
                        "//button[contains(text(),'Generate Excel')]",
                    ])
                    time.sleep(3)
                    ext = [".xlsx", ".zip"]
                else:  # GSTR3B — PDF downloads directly
                    ext = [".pdf"]

                # Wait for file
                f = _wait_for_file(self.download_dir, ext, before_files=before, timeout=120)
                if f:
                    # Rename to standard name
                    std_name = {
                        "GSTR1":  f"GSTR1_{month_name}_{year}.zip",
                        "GSTR2B": f"GSTR2B_{month_name}_{year}.xlsx",
                        "GSTR2A": f"GSTR2A_{month_name}_{year}.zip",
                        "GSTR3B": f"GSTR3B_{month_name}_{year}.pdf",
                    }.get(rtype, f"{rtype}_{month_name}_{year}{f.suffix}")
                    dest = self.download_dir / std_name
                    _rename_file(f, dest)
                    log.info(f"  ✓ {rtype}: {dest.name}")
                    results[rtype] = dest
                else:
                    log.warning(f"  ✗ {rtype}: download timeout")
                    results[rtype] = None

            except Exception as ex:
                log.error(f"  ✗ {rtype} error: {ex}")
                results[rtype] = None

        return results

    def download_full_year(self, fy: str = "2025-26",
                           returns_todo=None) -> Dict[str, Dict]:
        """
        Download all returns for all 12 months of a financial year.
        Must call login() first.
        """
        self.fy_label = fy
        start_yr = int(fy.split("-")[0]); end_yr = start_yr + 1

        MONTHS = [
            ("April",str(start_yr)),("May",str(start_yr)),("June",str(start_yr)),
            ("July",str(start_yr)),("August",str(start_yr)),("September",str(start_yr)),
            ("October",str(start_yr)),("November",str(start_yr)),("December",str(start_yr)),
            ("January",str(end_yr)),("February",str(end_yr)),("March",str(end_yr)),
        ]
        all_results = {}
        for month_name, year in MONTHS:
            log.info(f"\n=== {month_name} {year} ===")
            all_results[f"{month_name}_{year}"] = self.download_month(
                month_name, year, returns_todo)
        return all_results

    def _get_periods_for_fy(self, fy: str):
        """Return list of (month_name, period_code, fp) for app.py compatibility."""
        start = int(fy.split("-")[0]); end = start + 1
        months = [
            ("April","APR",str(start)),("May","MAY",str(start)),("June","JUN",str(start)),
            ("July","JUL",str(start)),("August","AUG",str(start)),("September","SEP",str(start)),
            ("October","OCT",str(start)),("November","NOV",str(start)),("December","DEC",str(start)),
            ("January","JAN",str(end)),("February","FEB",str(end)),("March","MAR",str(end)),
        ]
        return [(mn, f"{code}{yr}", f"{'0'+str(i+1) if i+1<10 else str(i+1)}{yr}")
                for i,(mn,code,yr) in enumerate(months)]

    def close(self):
        if self.driver:
            try: self.driver.quit()
            except: pass
            self.driver = None
