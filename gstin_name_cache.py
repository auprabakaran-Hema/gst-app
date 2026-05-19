"""
================================================================================
  GSTIN NAME CACHE  v1.2  (Fixed: portal 403 tracking, Selenium fallback, Tally CSV seed)
  ===================================================================
  Shared module used by gstr1_fy_v5.py, gst_suite_v31.py, etc.

  CHANGES in v1.1:
  ─────────────────
  • _clean_name()  → strips _x000D_ Excel CR artifacts from all names
  • _seed_from_customer_master() now marks entries as "customer_master"
    but does NOT block portal re-fetch — portal names always win
  • get() / get_bulk() now always prefer portal data over master data
  • force_refresh() method added: re-fetches all non-portal entries
  • All names sanitised on load and on save

  HOW IT WORKS:
  ─────────────
  1. Keeps a local cache file  →  gstin_name_cache.json  (same folder as script)
  2. On first run: fetches missing names from GST portal (free, no key)
  3. On later runs: reads from cache instantly — NO portal call needed
  4. Also seeds from CustomerMaster.xlsx if present (portal data takes priority)
  5. Auto-saves new names back to cache after every run

  USAGE IN OTHER SCRIPTS:
  ────────────────────────
  from gstin_name_cache import GSTINNameCache

  cache = GSTINNameCache()                    # loads cache + CustomerMaster
  name  = cache.get("33AABCT1234C1ZX")        # instant if cached, else fetches
  names = cache.get_bulk(list_of_gstins)      # bulk fetch with progress
  cache.save()                                # persist new names to disk

  # To re-fetch all customer_master entries from portal:
  cache.force_refresh(source_filter="customer_master")
  cache.save()

================================================================================
"""

import json, ssl, time, re, warnings
from pathlib import Path
from datetime import datetime

# ── Suppress InsecureRequestWarning at module level ───────────────────────────
warnings.filterwarnings("ignore", message=".*InsecureRequestWarning.*")
warnings.filterwarnings("ignore", message=".*Unverified HTTPS request.*")
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

# ── Try requests, fall back to urllib ─────────────────────────────────────────
try:
    import requests as _req
    _USE_REQUESTS = True
except ImportError:
    import urllib.request, urllib.error
    _USE_REQUESTS = False

# ── Cache file location (same folder as this script) ─────────────────────────
_CACHE_FILE       = Path(__file__).parent / "gstin_name_cache.json"
_CUSTOMER_MASTER  = Path(__file__).parent / "CustomerMaster.xlsx"
_DELAY            = 0.35   # seconds between portal calls (polite rate limit)

# ── GST portal endpoints (tried in order) ─────────────────────────────────────
_ENDPOINTS = [
    "https://services.gst.gov.in/services/api/search/gstin?gstin={gstin}",
    "https://www.gst.gov.in/util/rest/toolkit/searchTax?gstin={gstin}",
]
_HEADERS = {
    "User-Agent":      ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/148.0.0.0 Safari/537.36"),
    "Accept":          "application/json, text/plain, */*",
    "Accept-Language": "en-IN,en-GB;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "Referer":         "https://services.gst.gov.in/services/searchtp",
    "Origin":          "https://services.gst.gov.in",
    "sec-ch-ua":       '"Chromium";v="148","Google Chrome";v="148"',
    "sec-fetch-dest":  "empty",
    "sec-fetch-mode":  "cors",
    "sec-fetch-site":  "same-origin",
}


# ══════════════════════════════════════════════════════════════════════════════
def _clean_name(name):
    """
    Sanitise a business name coming from Excel or the portal.

    Removes:
      • _x000D_  — Excel XML carriage-return escape (\\r encoded in xlsx)
      • \\r, \\n    — raw newlines that sneak in via openpyxl
      • leading/trailing whitespace
    """
    if not name:
        return ""
    # Remove Excel XML CR escape (_x000D_ or _X000D_)
    name = re.sub(r'_[xX]000[dD]_', '', name)
    # Remove raw CR / LF
    name = name.replace('\r', '').replace('\n', '')
    return name.strip()


# ══════════════════════════════════════════════════════════════════════════════
class GSTINNameCache:
    """
    GSTIN → Name lookup with persistent local cache.

    Priority order for names:
      portal  >  manual  >  customer_master

    Parameters
    ----------
    cache_file      : path to JSON cache  (default: gstin_name_cache.json)
    customer_master : path to xlsx seed   (default: CustomerMaster.xlsx)
    log_fn          : callable(msg) for progress output (default: print)
    auto_fetch      : fetch missing names from portal automatically (default: True)
    prefer_portal   : if True (default), portal fetch overrides customer_master
    """

    def __init__(self,
                 cache_file=None,
                 customer_master=None,
                 log_fn=None,
                 auto_fetch=True,
                 prefer_portal=True):
        self._cache_path  = Path(cache_file or _CACHE_FILE)
        self._cm_path     = Path(customer_master or _CUSTOMER_MASTER)
        self._log         = log_fn or (lambda m: None)   # silent by default
        self._auto_fetch  = auto_fetch
        self._prefer_portal = prefer_portal
        self._data        = {}     # gstin → {legal_name, trade_name, fetched_at, source}
        self._dirty       = False

        self._load_cache()
        self._seed_from_customer_master()
        self.seed_from_tally_csv()   # FIX v3.2: also seed from Tally all_parties CSV

    # ── Public API ─────────────────────────────────────────────────────────────

    def get(self, gstin, fallback=""):
        """
        Return the best display name for a GSTIN.

        Priority logic:
        • auto_fetch=False  → return cached name from ANY source instantly (no portal call).
        • auto_fetch=True + already portal-sourced → return immediately.
        • auto_fetch=True + prefer_portal + non-portal cached → try to upgrade via portal.
        • auto_fetch=True + not cached → fetch fresh from portal.
        Returns fallback string if nothing found.
        """
        gstin = self._clean_gstin(gstin)
        if not gstin:
            return fallback

        existing = self._data.get(gstin)

        # Fast path: auto_fetch disabled → return whatever is cached, no portal call
        if not self._auto_fetch:
            return self._best_name(existing or {}, fallback)

        # Already have a portal-sourced name → use it directly
        if existing and existing.get("source") in ("portal", "portal_selenium"):
            return self._best_name(existing, fallback)

        # Have a non-portal name and prefer_portal=True → try to upgrade
        if existing and self._prefer_portal:
            rec = self._fetch_one(gstin)
            if rec:
                self._data[gstin] = rec
                self._dirty = True
                return self._best_name(rec, fallback)
            # Portal failed → use existing cached name
            return self._best_name(existing, fallback)

        # Have a cached name, prefer_portal=False → return as-is
        if existing:
            return self._best_name(existing, fallback)

        # Nothing in cache → fetch from portal
        rec = self._fetch_one(gstin)
        if rec:
            self._data[gstin] = rec
            self._dirty = True
            return self._best_name(rec, fallback)

        return fallback

    def get_bulk(self, gstins, show_progress=True):
        """
        Fetch names for a list of GSTINs.
        Returns dict {gstin: name}.

        • auto_fetch=False  → returns all names from cache instantly (NO portal calls).
        • auto_fetch=True + portal-sourced → returned instantly.
        • auto_fetch=True + non-portal cached + prefer_portal=True → upgraded from portal.
        • auto_fetch=True + not in cache → fetched fresh from portal.
        """
        gstins = list(dict.fromkeys(
            self._clean_gstin(g) for g in gstins if self._clean_gstin(g)
        ))

        # Fast path: auto_fetch disabled → return all from cache, no portal calls
        if not self._auto_fetch:
            return {g: self._best_name(self._data.get(g, {})) for g in gstins}

        # Decide which ones need a portal call
        to_fetch = []
        for g in gstins:
            rec = self._data.get(g)
            if rec is None:
                to_fetch.append(g)                    # never seen
            elif rec.get("source") not in ("portal", "portal_selenium") and self._prefer_portal:
                to_fetch.append(g)                    # upgrade from master/manual

        if to_fetch:
            self._log(f"  📡 Fetching {len(to_fetch)} GSTIN name(s) from GST portal…")
            for i, gstin in enumerate(to_fetch, 1):
                if show_progress:
                    self._log(f"    [{i}/{len(to_fetch)}] {gstin} … ")
                rec = self._fetch_one(gstin)
                if rec:
                    self._data[gstin] = rec
                    self._dirty = True
                    if show_progress:
                        self._log(f"      ✓ {self._best_name(rec)}")
                else:
                    if show_progress:
                        existing = self._data.get(gstin)
                        fallback_name = self._best_name(existing or {}, "⚠ Not found")
                        self._log(f"      ⚠ Portal failed → using: {fallback_name}")
                if i < len(to_fetch):
                    time.sleep(_DELAY)

        return {g: self._best_name(self._data.get(g, {})) for g in gstins}

    def force_refresh(self, source_filter=None):
        """
        Re-fetch names from portal.

        Parameters
        ----------
        source_filter : if given (e.g. "customer_master"), only refresh entries
                        with that source.  If None, refreshes ALL entries.
        """
        if source_filter:
            targets = [g for g, r in self._data.items()
                       if r.get("source") == source_filter]
        else:
            targets = list(self._data.keys())

        self._log(f"  🔄 Force-refreshing {len(targets)} GSTIN(s) from portal…")
        refreshed = failed = 0
        for i, gstin in enumerate(targets, 1):
            self._log(f"    [{i}/{len(targets)}] {gstin} … ", )
            rec = self._fetch_one(gstin)
            if rec:
                self._data[gstin] = rec
                self._dirty = True
                refreshed += 1
                self._log(f"      ✓ {self._best_name(rec)}")
            else:
                failed += 1
                self._log(f"      ⚠ Portal failed — keeping existing name")
            if i < len(targets):
                time.sleep(_DELAY)

        self._log(f"  ✅ Refreshed: {refreshed}  ❌ Failed: {failed}")
        return refreshed, failed

    def set_manual(self, gstin, legal_name, trade_name=""):
        """Manually add / override a GSTIN name (won't be overwritten by portal fetch)."""
        gstin = self._clean_gstin(gstin)
        if gstin:
            self._data[gstin] = {
                "legal_name": _clean_name(legal_name),
                "trade_name": _clean_name(trade_name),
                "source":     "manual",
                "fetched_at": datetime.now().isoformat(),
            }
            self._dirty = True

    def save(self):
        """Persist cache to disk (only writes if new data was added)."""
        if not self._dirty:
            return
        # Sanitise all names before saving
        for rec in self._data.values():
            rec["legal_name"] = _clean_name(rec.get("legal_name", ""))
            rec["trade_name"] = _clean_name(rec.get("trade_name", ""))
        try:
            self._cache_path.write_text(
                json.dumps(self._data, ensure_ascii=False, indent=2),
                encoding="utf-8"
            )
            self._log(f"  💾 GSTIN name cache saved ({len(self._data)} entries) → {self._cache_path.name}")
            self._dirty = False
        except Exception as e:
            self._log(f"  ⚠ Cache save failed: {e}")

    def stats(self):
        """Return dict with counts by source."""
        sources = [r.get("source", "") for r in self._data.values()]
        return {
            "total":   len(self._data),
            "portal":  sources.count("portal"),
            "manual":  sources.count("manual"),
            "master":  sources.count("customer_master"),
        }

    def export_excel(self, out_path=None):
        """Export entire cache to Excel for reference / editing."""
        try:
            import openpyxl
            from openpyxl.styles import Font, PatternFill, Alignment
        except ImportError:
            self._log("  ⚠ openpyxl not installed — cannot export Excel")
            return None

        out = Path(out_path or (self._cache_path.parent / "GSTIN_Name_Master.xlsx"))
        wb = openpyxl.Workbook()
        ws = wb.active; ws.title = "GSTIN Master"

        hdrs = ["GSTIN", "PAN", "Legal Name", "Trade Name", "Source", "Fetched At"]
        wids = [20, 13, 45, 35, 15, 20]
        for ci, (h, w) in enumerate(zip(hdrs, wids), 1):
            c = ws.cell(row=1, column=ci, value=h)
            c.font = Font(bold=True, color="FFFFFF", name="Calibri")
            c.fill = PatternFill("solid", fgColor="1F3864")
            c.alignment = Alignment(horizontal="center")
            ws.column_dimensions[chr(64+ci)].width = w

        for ri, (gstin, rec) in enumerate(sorted(self._data.items()), 2):
            alt = "F2F2F2" if ri % 2 == 0 else "FFFFFF"
            vals = [
                gstin,
                gstin[2:12] if len(gstin) == 15 else "",
                _clean_name(rec.get("legal_name", "")),
                _clean_name(rec.get("trade_name", "")),
                rec.get("source", ""),
                rec.get("fetched_at", "")[:10] if rec.get("fetched_at") else "",
            ]
            for ci, v in enumerate(vals, 1):
                c = ws.cell(row=ri, column=ci, value=v)
                c.fill = PatternFill("solid", fgColor=alt)
                c.font = Font(name="Calibri", size=10)

        wb.save(str(out))
        self._log(f"  📊 Exported {len(self._data)} entries → {out.name}")
        return out

    # ── Private ───────────────────────────────────────────────────────────────

    def _clean_gstin(self, g):
        return re.sub(r'[^A-Z0-9]', '', str(g or "").strip().upper())

    def _best_name(self, rec, fallback=""):
        """Return trade name if available, else legal name, else fallback."""
        if not rec:
            return fallback
        name = rec.get("trade_name") or rec.get("legal_name") or ""
        return (_clean_name(name) or fallback).strip()

    def _load_cache(self):
        """Load existing cache JSON from disk, sanitising names on the way in."""
        if self._cache_path.exists():
            try:
                raw = json.loads(self._cache_path.read_text(encoding="utf-8"))
                # Sanitise names while loading
                dirty_fixed = 0
                for gstin, rec in raw.items():
                    orig_ln = rec.get("legal_name", "")
                    orig_tn = rec.get("trade_name", "")
                    rec["legal_name"] = _clean_name(orig_ln)
                    rec["trade_name"] = _clean_name(orig_tn)
                    if rec["legal_name"] != orig_ln or rec["trade_name"] != orig_tn:
                        dirty_fixed += 1
                self._data = raw
                if dirty_fixed:
                    self._dirty = True   # mark for re-save with clean names
                    self._log(f"  🧹 Cleaned {dirty_fixed} dirty name(s) in cache")
                self._log(f"  📂 Loaded {len(self._data)} cached GSTIN names")
            except Exception as e:
                self._log(f"  ⚠ Cache load failed: {e}")
                self._data = {}

    def _seed_from_customer_master(self):
        """
        Read CustomerMaster.xlsx and seed cache with those names.

        PRIORITY RULES (v1.1):
          • Portal-sourced entries are NEVER overwritten by customer_master.
          • customer_master entries that don't already exist → seeded.
          • When prefer_portal=True, seeded entries will be upgraded to portal
            names automatically on the next get() / get_bulk() call.
        """
        if not self._cm_path.exists():
            return
        try:
            import openpyxl
            wb   = openpyxl.load_workbook(str(self._cm_path), read_only=True, data_only=True)
            ws   = wb.active
            rows = list(ws.iter_rows(values_only=True))
            wb.close()

            if not rows or len(rows) < 2:
                return

            hdrs = [str(c or "").strip().upper() for c in rows[0]]

            def _col(*names):
                for n in names:
                    if n in hdrs: return hdrs.index(n)
                return -1

            ci_gstin = _col("GSTIN/UIN", "GSTIN", "GST NO", "GSTIN NO")
            ci_name  = _col("PARTICULARS", "NAME", "COMPANY NAME", "TRADE NAME", "LEGAL NAME")

            if ci_gstin == -1:
                return

            added = 0
            for row in rows[1:]:
                def _v(ci):
                    return str(row[ci] or "").strip() if ci != -1 and ci < len(row) else ""
                gstin = self._clean_gstin(_v(ci_gstin))
                name  = _clean_name(_v(ci_name)) if ci_name != -1 else ""
                if len(gstin) == 15 and name:
                    existing = self._data.get(gstin)
                    # Only seed if: (a) not in cache, or (b) existing is also customer_master
                    # Never overwrite a portal-sourced entry
                    if existing is None or existing.get("source") not in ("portal", "manual"):
                        self._data[gstin] = {
                            "legal_name": name,
                            "trade_name": "",
                            "source":     "customer_master",
                            "fetched_at": datetime.now().isoformat(),
                        }
                        self._dirty = True
                        added += 1

            if added:
                self._log(f"  📋 Seeded {added} names from CustomerMaster.xlsx")

        except ImportError:
            pass
        except Exception as e:
            self._log(f"  ⚠ CustomerMaster seed failed: {e}")

    def seed_from_tally_csv(self, csv_path=None):
        """
        FIX v3.2: Seed cache from Tally-generated all_parties_with_gstin CSV files.
        These CSVs are created by tally_extract_gst.py and contain clean Ledger names
        directly from Tally (more reliable than portal for registered customers).
        Searches script directory for *_all_parties_with_gstin.csv files.
        """
        import csv as _csv
        search_dirs = []
        if csv_path:
            search_dirs.append(Path(csv_path))
        # Auto-discover from script folder and common data folders
        for d in [self._cache_path.parent, Path.cwd()]:
            search_dirs.extend(d.glob("*_all_parties_with_gstin.csv"))
            search_dirs.extend(d.glob("*_all_ledgers.csv"))

        added = 0
        for path in search_dirs:
            path = Path(path)
            if not path.exists():
                continue
            try:
                with open(path, encoding="utf-8-sig", errors="replace") as f:
                    reader = _csv.DictReader(f)
                    for row in reader:
                        # Support both CSV formats (all_parties and all_ledgers)
                        gstin = ""
                        name  = ""
                        for k in row:
                            kl = k.strip().upper()
                            if "GSTIN" in kl and not gstin:
                                gstin = str(row[k] or "").strip()
                            if kl in ("LEDGER NAME", "PARTICULARS", "NAME") and not name:
                                name = _clean_name(str(row[k] or "").strip())
                        if len(gstin) == 15 and name:
                            existing = self._data.get(gstin)
                            # Only seed if not already from portal
                            if existing is None or existing.get("source") not in ("portal", "portal_selenium", "manual"):
                                self._data[gstin] = {
                                    "legal_name": name,
                                    "trade_name": "",
                                    "source":     "tally_csv",
                                    "fetched_at": datetime.now().isoformat(),
                                }
                                self._dirty = True
                                added += 1
            except Exception as e:
                self._log(f"  ⚠ Tally CSV seed failed ({path.name}): {e}")

        if added:
            self._log(f"  📋 Seeded {added} names from Tally CSV (all_parties_with_gstin)")

    # FIX v3.2: track API 403 failures to skip retrying dead endpoints in same session
    _api_failure_count = 0
    _API_FAILURE_LIMIT = 3   # stop trying after 3 consecutive 403/failures

    def _fetch_one(self, gstin):
        """
        Fetch a single GSTIN from the GST portal.
        Returns a sanitised record dict, or None on failure.

        FIX v3.3: Enhanced response parsing with better field detection.
        Handles multiple API response formats and extracts all available names.
        """
        if len(gstin) != 15:
            return None

        # Skip direct API if it's been consistently failing (403 blocked)
        if GSTINNameCache._api_failure_count < GSTINNameCache._API_FAILURE_LIMIT:
            for endpoint_tpl in _ENDPOINTS:
                url = endpoint_tpl.format(gstin=gstin)
                try:
                    raw  = self._http_get(url)
                    
                    # Handle nested response structures
                    info = raw
                    if isinstance(raw, dict):
                        # Try common response wrappers
                        if "taxpayerInfo" in raw:
                            info = raw["taxpayerInfo"]
                        elif "data" in raw:
                            info = raw["data"]
                        elif "result" in raw:
                            info = raw["result"]
                        elif "response" in raw:
                            info = raw["response"]
                    
                    # If info is a list, take first element
                    if isinstance(info, list) and len(info) > 0:
                        info = info[0]
                    
                    if not isinstance(info, dict):
                        continue

                    def _g(*keys):
                        """Get value from info dict, trying multiple key names."""
                        for k in keys:
                            v = info.get(k, "")
                            if v and isinstance(v, str):
                                cleaned = _clean_name(str(v).strip())
                                if cleaned:
                                    return cleaned
                            elif v and isinstance(v, dict):
                                # Handle nested dict (e.g., status object)
                                for subv in v.values():
                                    if subv:
                                        return _clean_name(str(subv).strip())
                        return ""

                    # Try multiple possible field names for legal name
                    legal = _g(
                        "lgnm",              # Endpoint 1
                        "legalName",         # Common
                        "legal_name",        # Common alternate
                        "tradeName",         # Fallback
                        "trade_name",        # Fallback alternate
                        "name",              # Generic
                        "taxpayerName",      # Some APIs
                        "tp_name"            # Some APIs
                    )
                    
                    # Try multiple field names for trade name
                    trade = _g(
                        "tradeNam",          # Endpoint 1
                        "tradeName",         # Common
                        "trade_name",        # Common alternate
                        "trade",             # Generic
                        "tp_trade_name"      # Some APIs
                    )
                    
                    # Ensure trade name is different from legal name
                    if trade and trade.upper() == legal.upper():
                        trade = ""
                    
                    # Try to get status
                    status = _g("sts", "status", "tp_status", "taxpayerStatus")
                    
                    if legal or trade:
                        GSTINNameCache._api_failure_count = 0   # reset on success
                        return {
                            "legal_name": legal,
                            "trade_name": trade,
                            "status":     status,
                            "source":     "portal",
                            "fetched_at": datetime.now().isoformat(),
                        }
                except Exception as e:
                    GSTINNameCache._api_failure_count += 1
                    self._log(f"      ⚠ API error: {str(e)[:50]}")
                    continue   # try next endpoint

        # ── Selenium fallback: scrape GST search page ────────────────────────
        # Works without login — public taxpayer search at services.gst.gov.in/services/searchtp
        return self._fetch_via_selenium(gstin)

    def _fetch_via_selenium(self, gstin):
        """
        FIX v3.3: Enhanced Selenium-based GSTIN name lookup.
        Uses Chrome to visit the public GST taxpayer search page.
        No login required — works even when the REST API is 403-blocked.
        
        IMPROVEMENTS:
        - Better name extraction from page structure
        - Handles multiple result formats
        - Extracts legal & trade names if available
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time as _time

            opts = Options()
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--log-level=3")
            opts.add_experimental_option("excludeSwitches", ["enable-logging"])

            driver = webdriver.Chrome(options=opts)
            try:
                driver.get("https://services.gst.gov.in/services/searchtp")
                # Wait for search input
                wait = WebDriverWait(driver, 15)
                inp = wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "input[placeholder*='GSTIN'], input[id*='gstin'], input[name*='gstin']")
                ))
                inp.clear()
                inp.send_keys(gstin)
                
                # Click search button
                btn = driver.find_element(By.CSS_SELECTOR,
                    "button[type='submit'], button.btn-search, button[class*='search']")
                btn.click()
                _time.sleep(2)

                # Try multiple selectors to find result data
                legal_name = ""
                trade_name = ""
                
                try:
                    # Try to find in structured elements
                    result_divs = driver.find_elements(By.CSS_SELECTOR, 
                        "div[class*='result'], div[class*='card'], table tbody tr")
                    
                    if result_divs:
                        for elem in result_divs:
                            try:
                                text = elem.text.strip()
                                if text and len(text) > 4:
                                    lines = text.split("\n")
                                    for line in lines:
                                        line = line.strip()
                                        # Skip GSTIN itself
                                        if gstin in line.upper():
                                            continue
                                        # Look for name (starts with letter, not number)
                                        if line and not line[0].isdigit():
                                            potential_name = _clean_name(line)
                                            if potential_name and len(potential_name) > 3:
                                                if not legal_name:
                                                    legal_name = potential_name
                                                elif legal_name != potential_name:
                                                    trade_name = potential_name
                                                    break
                            except Exception:
                                continue
                        
                        if legal_name:
                            return {
                                "legal_name": legal_name,
                                "trade_name": trade_name,
                                "source":     "portal_selenium",
                                "fetched_at": datetime.now().isoformat(),
                            }
                except Exception:
                    pass
                
                # Fallback: read entire body text
                body = driver.find_element(By.TAG_NAME, "body").text
                lines = body.split("\n")
                
                for i, line in enumerate(lines):
                    line = line.strip()
                    # Skip empty and short lines
                    if not line or len(line) < 4:
                        continue
                    # Skip GSTIN
                    if gstin in line.upper():
                        continue
                    # Skip lines that are mostly numbers or codes
                    if sum(c.isdigit() for c in line) / len(line) > 0.5:
                        continue
                    # Look for name-like strings
                    if any(c.isalpha() for c in line):
                        potential = _clean_name(line)
                        if potential and len(potential) > 3:
                            if not legal_name:
                                legal_name = potential
                            elif legal_name != potential and not trade_name:
                                trade_name = potential
                                break

                if legal_name and len(legal_name) > 2:
                    return {
                        "legal_name": legal_name,
                        "trade_name": trade_name,
                        "source":     "portal_selenium",
                        "fetched_at": datetime.now().isoformat(),
                    }
            finally:
                driver.quit()
        except Exception:
            pass   # Selenium not available or page changed

        return None   # all methods failed

    def _http_get(self, url):
        """HTTP GET → parsed JSON. Works with or without requests."""
        if _USE_REQUESTS:
            r = _req.get(url, headers=_HEADERS, timeout=12, verify=False)
            r.raise_for_status()
            return r.json()
        else:
            ctx = ssl.create_default_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            req = urllib.request.Request(url, headers=_HEADERS)
            with urllib.request.urlopen(req, timeout=12, context=ctx) as resp:
                return json.loads(resp.read().decode("utf-8"))
