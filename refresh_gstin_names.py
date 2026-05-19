"""
================================================================================
  REFRESH_GSTIN_NAMES.py
  ======================
  One-time utility to fetch REAL names from the GST portal for all GSTINs
  that are currently sourced only from CustomerMaster.xlsx.

  Run this ONCE after deploying the updated gstin_name_cache.py:

      python refresh_gstin_names.py

  What it does:
  ─────────────
  1. Loads gstin_name_cache.json  (must be in same folder)
  2. Finds all entries with source = "customer_master"  (or "manual")
  3. Hits the GST portal for each GSTIN (free, no API key needed)
  4. Saves verified names back to cache with source = "portal"
  5. Exports a summary Excel  →  GSTIN_Refresh_Report_<date>.xlsx

  Rate: ~3 GSTINs/second (0.35 s polite delay between calls)
  95 GSTINs ≈ 35 seconds total.

  Requirements:  pip install requests openpyxl   (already in INSTALL_REQUIREMENTS.bat)
================================================================================
"""

import sys, time
from pathlib import Path
from datetime import datetime

# ── Make sure gstin_name_cache.py is on the path ──────────────────────────────
HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))

try:
    from gstin_name_cache import GSTINNameCache
except ImportError:
    print("ERROR: gstin_name_cache.py not found in the same folder as this script.")
    sys.exit(1)

# ── Colour helpers (Windows-safe) ─────────────────────────────────────────────
import os
if sys.platform == "win32":
    os.system("color")
R = "\033[0m"; G = "\033[92m"; Y = "\033[93m"; C = "\033[96m"; B = "\033[1m"; RED = "\033[91m"


def banner():
    print(f"""
{C}╔══════════════════════════════════════════════════════════════╗
║    GSTIN Name Refresh — Fetching verified names from portal  ║
╚══════════════════════════════════════════════════════════════╝{R}
  Source : services.gst.gov.in  (free, no login needed)
  Cache  : gstin_name_cache.json  (same folder)
""")


def main():
    banner()

    # ── Load cache ────────────────────────────────────────────────────────────
    cache = GSTINNameCache(log_fn=print, auto_fetch=False)   # load only, no auto-fetch yet
    stats = cache.stats()
    print(f"\n  Cache stats before refresh:")
    print(f"    Total  : {stats['total']}")
    print(f"    Portal : {G}{stats['portal']}{R}")
    print(f"    Master : {Y}{stats['master']}{R}")
    print(f"    Manual : {stats['manual']}")

    to_refresh = stats['master'] + stats['manual']
    if to_refresh == 0 and stats['portal'] > 0:
        print(f"\n  {G}✓ All entries already sourced from portal. Nothing to do.{R}\n")
        return

    if to_refresh == 0:
        print(f"\n  {Y}⚠ No entries to refresh.{R}\n")
        return

    print(f"\n  {Y}Will fetch {to_refresh} GSTIN(s) from portal.{R}")
    est = to_refresh * 0.4
    print(f"  Estimated time: ~{est:.0f} seconds\n")

    proceed = input("  Proceed? [Y/n] ").strip().lower()
    if proceed not in ("", "y", "yes"):
        print("  Aborted.\n")
        return

    # ── Enable auto-fetch and run force_refresh ───────────────────────────────
    cache._auto_fetch    = True
    cache._prefer_portal = True

    print()
    refreshed, failed = cache.force_refresh(source_filter="customer_master")

    # Also refresh manual entries if any
    if stats['manual'] > 0:
        r2, f2 = cache.force_refresh(source_filter="manual")
        refreshed += r2; failed += f2

    # ── Save updated cache ────────────────────────────────────────────────────
    cache.save()

    # ── Print final stats ─────────────────────────────────────────────────────
    stats2 = cache.stats()
    print(f"\n  Cache stats after refresh:")
    print(f"    Total  : {stats2['total']}")
    print(f"    Portal : {G}{stats2['portal']}{R}  (was {stats['portal']})")
    print(f"    Master : {Y}{stats2['master']}{R}  (was {stats['master']})")
    print(f"\n  {G}✅ Refreshed: {refreshed}{R}   {RED}❌ Failed: {failed}{R}")

    # ── Export summary Excel ──────────────────────────────────────────────────
    ts  = datetime.now().strftime("%Y%m%d_%H%M")
    out = HERE / f"GSTIN_Refresh_Report_{ts}.xlsx"
    exported = cache.export_excel(out)
    if exported:
        print(f"\n  📊 Full report saved → {G}{exported.name}{R}")

    if failed > 0:
        print(f"\n  {Y}ℹ  {failed} GSTIN(s) could not be fetched from the portal.")
        print(f"     These entries retain their customer_master names.")
        print(f"     Possible reasons: GSTIN cancelled, typo in GSTIN, portal timeout.{R}")

    print(f"\n  Done.\n")


if __name__ == "__main__":
    main()
