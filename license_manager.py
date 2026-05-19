"""
================================================================================
  LICENSE MANAGER  v1.0
  ─────────────────────
  Drop this file alongside run_all.py, gst_suite_v31.py, it_suite_v6.py

  HOW IT WORKS:
  ─────────────
  1. On first run → asks user to enter their license key
  2. Saves key to  license.dat  (encrypted, in same folder)
  3. Every run → verifies key offline using machine ID + your secret salt
  4. Key is LOCKED to that PC — sharing does not work on another PC
  5. Key has expiry date built in — stops working after 31-Mar each year

  KEY FORMAT:   XXXX-XXXX-XXXX-XXXX  (16 hex chars in 4 groups)
  KEY ENCODES:  machine_id + expiry_month + client_limit + your_salt

  GENERATE KEYS:  Run  python license_manager.py --generate
                  or   python license_manager.py --generate --clients 10 --months 12

  VERIFY:         python license_manager.py --check ABCD-1234-EFGH-5678

================================================================================
"""

import hashlib, json, os, sys, uuid, argparse, base64, re
from pathlib import Path
from datetime import datetime, date
from typing import Optional

# ── YOUR SECRET SALT — change this to something only you know ─────────────────
# This makes your keys unique — nobody else can generate valid keys
# KEEP THIS PRIVATE — do not share this file with customers
_SALT = "FT_GST_AUTO_2025_RAJASEKARAN_SECRET_XK9"   # ← change this to your own secret

# ── License file location ─────────────────────────────────────────────────────
# CRITICAL FIX: Inside a PyInstaller EXE, __file__ points to a TEMP folder
# that is DELETED when the EXE exits → license.dat lost every run.
# When frozen (EXE), save license.dat next to the EXE (sys.executable parent).
_IS_FROZEN  = getattr(sys, "frozen", False)
_BASE_DIR   = Path(sys.executable).parent if _IS_FROZEN else Path(__file__).parent
_LIC_FILE   = _BASE_DIR / "license.dat"
_SOFT_NAME  = "FT GST Automation Suite"
_CONTACT   = "auprabakaran@gmail.com | WhatsApp: +91 7845998125"

# ── Current FY expiry (software stops working after 31-Mar) ───────────────────
_CURRENT_FY_END = date(2026, 3, 31)   # update this each year when you release new version


# ══════════════════════════════════════════════════════════════════════════════
#  MACHINE ID  — unique per PC, based on hardware, never changes
# ══════════════════════════════════════════════════════════════════════════════
def get_machine_id() -> str:
    """
    Returns a stable 12-char hex ID for this PC.
    Based on MAC address — survives reboots, reinstalls.
    Different on every machine — so keys are machine-locked.
    """
    raw = str(uuid.getnode())                          # MAC address as integer
    full = hashlib.sha256(raw.encode()).hexdigest()
    return full[:12].upper()                           # 12 hex chars


# ══════════════════════════════════════════════════════════════════════════════
#  KEY GENERATION  (you run this — customers never see this)
# ══════════════════════════════════════════════════════════════════════════════
def generate_key(machine_id: str,
                 expiry_date: date,
                 max_clients: int = 999,
                 plan_code: str = "PRO") -> str:
    """
    Generate a license key for a specific machine.

    machine_id  : from get_machine_id() — customer sends you this
    expiry_date : date(2026, 3, 31) for FY2025-26 license
    max_clients : 5 / 10 / 50 / 999
    plan_code   : "TRIAL" / "BASIC" / "PRO" / "UNLIMITED"

    Returns: "ABCD-1234-EFGH-5678"
    """
    expiry_str  = expiry_date.strftime("%Y%m%d")
    payload     = f"{machine_id}|{expiry_str}|{max_clients}|{_SALT}"
    digest      = hashlib.sha256(payload.encode()).hexdigest().upper()

    # Embed expiry + client limit into key so offline check works
    # Key = first 8 chars of hash + 4-char expiry code + 4-char client code
    base        = digest[:8]                           # 8 chars from hash
    exp_code    = expiry_date.strftime("%m%y")         # e.g. "0326" = Mar 2026
    cli_code    = f"{max_clients:04d}"[:4]             # e.g. "0010" or "0999"

    raw_key     = base + exp_code + cli_code           # 16 chars total
    # Format as XXXX-XXXX-XXXX-XXXX
    key         = "-".join([raw_key[i:i+4] for i in range(0, 16, 4)])
    return key


def _decode_key(key: str):
    """Extract expiry and client limit from key (without knowing machine_id)."""
    clean = key.replace("-", "").upper()
    if len(clean) != 16:
        return None, None
    try:
        exp_str  = clean[8:12]    # MMYY e.g. "0326"
        cli_str  = clean[12:16]   # e.g. "0010"
        month    = int(exp_str[:2])
        year     = 2000 + int(exp_str[2:])
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        expiry   = date(year, month, last_day)
        max_cli  = int(cli_str)
        return expiry, max_cli
    except Exception:
        return None, None


# ══════════════════════════════════════════════════════════════════════════════
#  KEY VERIFICATION  (runs on customer's machine every startup)
# ══════════════════════════════════════════════════════════════════════════════
def verify_key(key: str, machine_id: Optional[str] = None) -> dict:
    """
    Verify a license key on this machine.
    Returns dict: {valid, reason, expiry, max_clients, days_left}
    """
    clean = key.replace("-", "").strip().upper()
    if len(clean) != 16 or not re.match(r'^[A-F0-9]{16}$', clean):
        return {"valid": False, "reason": "Invalid key format"}

    mid = machine_id or get_machine_id()

    # Decode expiry and client limit from key
    expiry, max_clients = _decode_key(key)
    if expiry is None:
        return {"valid": False, "reason": "Cannot decode key"}

    # Reconstruct expected key from machine_id + decoded values
    plan_code = "PRO"   # unused - kept for compatibility
    expected  = generate_key(mid, expiry, max_clients, plan_code)

    if expected.replace("-","").upper() != clean:
        return {"valid": False, "reason": "Key not valid for this computer"}

    # Check expiry
    today = date.today()
    if today > expiry:
        days_over = (today - expiry).days
        return {
            "valid":       False,
            "reason":      f"License expired {days_over} day(s) ago (expired {expiry})",
            "expiry":      expiry,
            "max_clients": max_clients,
            "days_left":   -days_over,
        }

    days_left = (expiry - today).days
    return {
        "valid":       True,
        "reason":      "OK",
        "expiry":      expiry,
        "max_clients": max_clients,
        "days_left":   days_left,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  LICENSE FILE  — save/load key from encrypted dat file
# ══════════════════════════════════════════════════════════════════════════════
def _encode_dat(data: dict) -> str:
    """Simple obfuscation — not true encryption, but hides from casual viewing."""
    raw   = json.dumps(data)
    b64   = base64.b64encode(raw.encode()).decode()
    # XOR with salt
    key   = (_SALT * 100)[:len(b64)]
    xored = "".join(chr(ord(a) ^ ord(b) % 128) for a, b in zip(b64, key))
    return base64.b64encode(xored.encode("latin-1")).decode()


def _decode_dat(encoded: str) -> Optional[dict]:
    try:
        xored = base64.b64decode(encoded).decode("latin-1")
        key   = (_SALT * 100)[:len(xored)]
        b64   = "".join(chr(ord(a) ^ ord(b) % 128) for a, b in zip(xored, key))
        raw   = base64.b64decode(b64).decode()
        return json.loads(raw)
    except Exception:
        return None


def _save_license(key: str, result: dict):
    data = {
        "key":         key,
        "machine_id":  get_machine_id(),
        "expiry":      str(result["expiry"]),
        "max_clients": result["max_clients"],
        "activated":   str(date.today()),
    }
    _LIC_FILE.write_text(_encode_dat(data))


def _load_license() -> Optional[dict]:
    if not _LIC_FILE.exists():
        return None
    try:
        return _decode_dat(_LIC_FILE.read_text())
    except Exception:
        return None



# ══════════════════════════════════════════════════════════════════════════════
#  CONSOLE vs WINDOWED EXE  — safe input helpers
#  input() crashes in --windowed PyInstaller EXE (no console attached).
#  These helpers fall back to a tkinter dialog automatically.
# ══════════════════════════════════════════════════════════════════════════════
def _has_console() -> bool:
    """True if running with a real console (not a --windowed EXE)."""
    try:
        return sys.stdin is not None and sys.stdin.fileno() >= 0
    except Exception:
        return False


def _prompt_key(prompt: str) -> str:
    """Ask the user for a license key — console or GUI dialog."""
    if _has_console():
        try:
            return input(prompt).strip()
        except EOFError:
            pass
    # Windowed EXE fallback — tkinter dialog
    try:
        import tkinter as tk
        from tkinter import simpledialog
        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        val = simpledialog.askstring(
            "License Required",
            f"{_SOFT_NAME}\n\nEnter your License Key (XXXX-XXXX-XXXX-XXXX):\n"
            f"Contact: {_CONTACT}",
            parent=root,
        )
        root.destroy()
        return (val or "").strip()
    except Exception:
        return ""


def _press_enter_exit():
    """Show error and exit — console or GUI messagebox."""
    if _has_console():
        try:
            input("  Press Enter to exit...")
        except Exception:
            pass
    else:
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            messagebox.showerror(
                "License Error",
                f"License verification failed.\n\nContact: {_CONTACT}\n"
                f"Machine ID: {get_machine_id()}",
                parent=root,
            )
            root.destroy()
        except Exception:
            pass
    sys.exit(1)


def _show_license_prompt_header(mid: str):
    """Show the license prompt — console banner or GUI info dialog."""
    if _has_console():
        print("\n" + "═"*62)
        print(f"  {_SOFT_NAME}")
        print("═"*62)
        print(f"  Your Machine ID: {mid}")
        print(f"  Send this ID to get your license key.")
        print(f"  Contact: {_CONTACT}")
        print("═"*62)
    else:
        try:
            import tkinter as tk
            from tkinter import messagebox
            root = tk.Tk()
            root.withdraw()
            root.attributes("-topmost", True)
            messagebox.showinfo(
                "License Required",
                f"{_SOFT_NAME}\n\n"
                f"No valid license found on this computer.\n\n"
                f"Your Machine ID: {mid}\n\n"
                f"Send this Machine ID to get your license key.\n"
                f"Contact: {_CONTACT}",
                parent=root,
            )
            root.destroy()
        except Exception:
            pass


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRY POINT  — called at startup of run_all.py
# ══════════════════════════════════════════════════════════════════════════════
def check_license(n_clients: int = 0, silent: bool = False) -> dict:
    """
    Call this at the TOP of run_all.py main().
    Verifies license, prompts for key if missing, exits if invalid.

    n_clients : number of clients loaded (to check plan limit)
    silent    : if True, don't print the success banner

    Returns the result dict if valid. Calls sys.exit() if invalid.
    """
    mid = get_machine_id()

    # ── Try loading saved key ─────────────────────────────────────────────────
    saved = _load_license()
    if saved and saved.get("machine_id") == mid:
        key    = saved["key"]
        result = verify_key(key, mid)
        if result["valid"]:
            _print_license_ok(result, n_clients, silent)
            _check_client_limit(result, n_clients)
            return result
        else:
            # Key failed — ask for new key
            print(f"\n  ⚠  Saved license invalid: {result['reason']}")
            _LIC_FILE.unlink(missing_ok=True)

    # ── No valid saved key — prompt user ──────────────────────────────────────
    _show_license_prompt_header(mid)

    for attempt in range(3):
        key = _prompt_key("\n  Enter License Key (XXXX-XXXX-XXXX-XXXX): ")
        if not key:
            continue
        result = verify_key(key, mid)
        if result["valid"]:
            _save_license(key, result)
            _print_license_ok(result, n_clients, silent=False)
            _check_client_limit(result, n_clients)
            return result
        else:
            print(f"  ❌ {result['reason']}")
            if attempt < 2:
                print(f"     Try again ({2-attempt} attempt(s) left)...")

    if _has_console():
        print(f"\n  ❌ License verification failed.")
        print(f"  Contact: {_CONTACT}")
        print(f"  Your Machine ID: {mid}\n")
    _press_enter_exit()


def _print_license_ok(result: dict, n_clients: int, silent: bool):
    if silent:
        return
    exp    = result["expiry"]
    days   = result["days_left"]
    maxcli = result["max_clients"]
    warn   = f"  ⚠  Expires in {days} days — renew soon!" if days <= 30 else ""
    print(f"\n  ✅ License valid — expires {exp} ({days} days left)")
    if maxcli < 999:
        print(f"     Plan: up to {maxcli} clients  |  Loaded: {n_clients}")
    if warn:
        print(warn)


def _check_client_limit(result: dict, n_clients: int):
    maxcli = result.get("max_clients", 999)
    if maxcli >= 999:
        return
    if n_clients > maxcli:
        print(f"\n  ❌ Your license allows {maxcli} client(s).")
        print(f"     You have {n_clients} client(s) in your file.")
        print(f"     Please upgrade your plan: {_CONTACT}")
        _press_enter_exit()


# ══════════════════════════════════════════════════════════════════════════════
#  ADMIN TOOL  — python license_manager.py --generate
# ══════════════════════════════════════════════════════════════════════════════
def _admin_cli():
    parser = argparse.ArgumentParser(description="FT License Manager — Admin Tool")
    sub    = parser.add_subparsers(dest="cmd")

    # -- generate
    gen = sub.add_parser("--generate", help="Generate a new license key")
    gen.add_argument("--machine",  required=True, help="Customer machine ID (12 hex chars)")
    gen.add_argument("--expiry",   default="2026-03-31", help="Expiry date YYYY-MM-DD")
    gen.add_argument("--clients",  type=int, default=999, help="Max clients (default=999)")
    gen.add_argument("--plan",     default="PRO", help="Plan code: TRIAL/BASIC/PRO/UNLIMITED")

    # -- check
    chk = sub.add_parser("--check", help="Verify a key on this machine")
    chk.add_argument("key", help="License key to verify")

    # -- myid
    sub.add_parser("--myid", help="Show this machine's ID")

    # -- bulk
    blk = sub.add_parser("--bulk", help="Generate keys for multiple machines from a text file")
    blk.add_argument("file", help="Text file with one machine_id per line")
    blk.add_argument("--expiry",  default="2026-03-31")
    blk.add_argument("--clients", type=int, default=999)

    args = parser.parse_args()

    if args.cmd == "--myid" or not args.cmd:
        mid = get_machine_id()
        print(f"\n  Machine ID: {mid}")
        print(f"  Send this to your license provider.\n")
        return

    if args.cmd == "--generate":
        expiry = date.fromisoformat(args.expiry)
        key    = generate_key(args.machine.upper(), expiry, args.clients, args.plan)
        result = verify_key(key, args.machine.upper())
        print(f"\n  ┌─────────────────────────────────────┐")
        print(f"  │  LICENSE KEY GENERATED               │")
        print(f"  │                                      │")
        print(f"  │  Key      : {key}     │")
        print(f"  │  Machine  : {args.machine.upper()}         │")
        print(f"  │  Expiry   : {expiry}            │")
        print(f"  │  Clients  : {args.clients:<28} │")
        print(f"  │  Plan     : {args.plan:<28} │")
        print(f"  │  Valid    : {result['valid']}                         │")
        print(f"  └─────────────────────────────────────┘\n")
        return

    if args.cmd == "--check":
        mid    = get_machine_id()
        result = verify_key(args.key, mid)
        exp, maxcli = _decode_key(args.key)
        print(f"\n  Key     : {args.key}")
        print(f"  Machine : {mid}")
        print(f"  Valid   : {result['valid']}")
        print(f"  Reason  : {result['reason']}")
        print(f"  Expiry  : {exp}")
        print(f"  Clients : {maxcli}")
        print(f"  Days left: {result.get('days_left','N/A')}\n")
        return

    if args.cmd == "--bulk":
        expiry = date.fromisoformat(args.expiry)
        ids    = [l.strip().upper() for l in Path(args.file).read_text().splitlines() if l.strip()]
        print(f"\n  Generating {len(ids)} key(s)...\n")
        print(f"  {'Machine ID':<15}  {'License Key':<20}  Expiry")
        print("  " + "-"*55)
        for mid in ids:
            key = generate_key(mid, expiry, args.clients)
            print(f"  {mid:<15}  {key:<20}  {expiry}")
        print()
        return

    parser.print_help()


def enforce_license(n_clients: int = 0, silent: bool = False) -> dict:
    """
    Alias for check_license(). Call this at the TOP of run_all.py:

        from license_manager import enforce_license
        enforce_license(n_clients=len(clients))

    This will block execution if no valid license is found.
    """
    return check_license(n_clients=n_clients, silent=silent)


# ══════════════════════════════════════════════════════════════════════════════
#  UNIVERSAL AUTO-ENFORCEMENT
#  ─────────────────────────────────────────────────────────────────────────────
#  Simply add this ONE LINE at the very top of ANY script you want protected:
#
#      import license_manager   # LICENSE GATE
#
#  That's it. The moment any script imports this module, the license is checked.
#  No valid key  →  program exits immediately, no matter which script ran.
#  Works for .py files AND PyInstaller .exe bundles.
# ══════════════════════════════════════════════════════════════════════════════

# ── Scripts that are EXEMPT from the gate (admin/utility use only) ────────────
# These are YOUR internal tools — customers never have these filenames.
_EXEMPT_SCRIPTS = {
    "license_manager.py",   # admin key-generation tool itself
    "keygen.py",            # if you have a separate keygen helper
    "generate_keys.py",
}

def _auto_enforce_on_import():
    """
    Called automatically whenever license_manager is imported.
    Blocks execution unless a valid license is present.
    Exempt only: the admin tools listed in _EXEMPT_SCRIPTS.
    """
    # ── Skip ONLY during PyInstaller BUILD phase (not at runtime) ────────────
    # BUILD_EXE.bat sets this variable. Customers never have it set.
    if os.environ.get("SKIP_LICENSE_CHECK") == "1":
        return

    # ── Skip if PyInstaller collector/analysis is in the call stack ──────────
    # This catches the build-time import scan. At runtime (frozen EXE) these
    # paths are never present — so frozen EXEs are NOT skipped here.
    try:
        import inspect as _inspect2
        for _fi in _inspect2.stack():
            _full = str(_fi.filename).lower().replace("\\", "/")
            if "/pyinstaller/" in _full and (
                "collect" in _full or "analysis" in _full or "build" in _full
            ):
                return
    except Exception:
        pass

    # ── Determine the outermost calling script ────────────────────────────────
    # Inside a frozen EXE the stack has <frozen ...> entries; we look for the
    # actual .py or frozen bootstrap name to detect exempt admin scripts.
    import inspect as _inspect
    root_script = ""
    for frame_info in _inspect.stack():
        fname = Path(frame_info.filename).name
        # Skip internal / frozen bootstrap frames
        if fname in ("license_manager.py", "", "<string>"):
            continue
        root_script = fname   # keep walking — we want the outermost caller

    if root_script in _EXEMPT_SCRIPTS:
        return   # admin tool — skip gate

    # ── ALL other callers must have a valid license ───────────────────────────
    check_license()


# ── Fire on every import (not when running as admin tool directly) ─────────────
if __name__ != "__main__":
    _auto_enforce_on_import()


# ══════════════════════════════════════════════════════════════════════════════
#  ADMIN TOOL  — python license_manager.py --generate  (unchanged below)
# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    _admin_cli()
