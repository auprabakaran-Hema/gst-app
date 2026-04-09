"""
generate_license.py — GST Service License Manager
===================================================
Admin-only tool.  Run on your server or local machine.
Never share this file with customers.

Usage:
    python generate_license.py --name "ABC Traders" --email abc@mail.com --days 365
    python generate_license.py --list
    python generate_license.py --revoke GSTPRO-XXXXX-XXXXX-XXXXX
    python generate_license.py --info GSTPRO-XXXXX-XXXXX-XXXXX
"""

import os, sys, sqlite3, secrets, string, hashlib, argparse
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "licenses.db"

# ── DB setup ─────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE IF NOT EXISTS licenses (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            key_hash    TEXT UNIQUE NOT NULL,
            key_plain   TEXT UNIQUE NOT NULL,   -- stored only for admin display
            customer    TEXT NOT NULL,
            email       TEXT,
            plan        TEXT DEFAULT 'full',
            issued_at   TEXT NOT NULL,
            expires_at  TEXT,                   -- NULL = never expires
            is_active   INTEGER DEFAULT 1,
            note        TEXT
        )
    """)
    conn.commit()
    return conn

# ── Generate a readable license key ──────────────────────────────
def make_key():
    """Format: GSTPRO-XXXXX-XXXXX-XXXXX (letters+digits, no ambiguous chars)"""
    chars = string.ascii_uppercase.replace("O","").replace("I","") + string.digits.replace("0","")
    segments = [''.join(secrets.choice(chars) for _ in range(5)) for _ in range(3)]
    return "GSTPRO-" + "-".join(segments)

def hash_key(k: str) -> str:
    return hashlib.sha256(k.encode()).hexdigest()

# ── Create license ────────────────────────────────────────────────
def create_license(customer, email=None, days=365, plan="full", note=None):
    key = make_key()
    now = datetime.now()
    expires = (now + timedelta(days=days)).isoformat() if days else None

    conn = get_db()
    conn.execute("""
        INSERT INTO licenses (key_hash, key_plain, customer, email, plan,
                              issued_at, expires_at, note)
        VALUES (?,?,?,?,?,?,?,?)
    """, (hash_key(key), key, customer, email, plan,
          now.isoformat(), expires, note))
    conn.commit()
    conn.close()

    print()
    print("  ┌─────────────────────────────────────────────────────┐")
    print(f"  │  License created for: {customer:<30}│")
    print(f"  │  Key:    {key:<44}│")
    print(f"  │  Plan:   {plan:<44}│")
    print(f"  │  Issued: {now.strftime('%d-%b-%Y'):<44}│")
    exp_str = (now + timedelta(days=days)).strftime('%d-%b-%Y') if days else "Never"
    print(f"  │  Expires:{exp_str:<44}│")
    if email:
        print(f"  │  Email:  {email:<44}│")
    print("  └─────────────────────────────────────────────────────┘")
    print()
    print(f"  Send this key to the customer: {key}")
    print()
    return key

# ── List all licenses ─────────────────────────────────────────────
def list_licenses():
    conn = get_db()
    rows = conn.execute("""
        SELECT key_plain, customer, email, plan, issued_at, expires_at, is_active
        FROM licenses ORDER BY id DESC
    """).fetchall()
    conn.close()

    if not rows:
        print("  No licenses found.")
        return

    print()
    print(f"  {'KEY':<30} {'CUSTOMER':<22} {'PLAN':<7} {'EXPIRES':<12} {'STATUS'}")
    print("  " + "─"*90)
    for r in rows:
        exp = r["expires_at"][:10] if r["expires_at"] else "Never   "
        status = "✓ Active" if r["is_active"] else "✗ Revoked"
        if r["expires_at"]:
            if datetime.fromisoformat(r["expires_at"]) < datetime.now():
                status = "⚠ Expired"
        print(f"  {r['key_plain']:<30} {r['customer']:<22} {r['plan']:<7} {exp:<12} {status}")
    print()

# ── Revoke license ────────────────────────────────────────────────
def revoke_license(key):
    conn = get_db()
    c = conn.execute("UPDATE licenses SET is_active=0 WHERE key_plain=?", (key,))
    conn.commit()
    conn.close()
    if c.rowcount:
        print(f"  ✓ License revoked: {key}")
    else:
        print(f"  ✗ License not found: {key}")

# ── Show license info ─────────────────────────────────────────────
def show_info(key):
    conn = get_db()
    r = conn.execute("SELECT * FROM licenses WHERE key_plain=?", (key,)).fetchone()
    conn.close()
    if not r:
        print(f"  ✗ Not found: {key}")
        return
    print()
    for col in r.keys():
        if col == "key_hash": continue
        print(f"  {col:<12}: {r[col]}")
    print()

# ── Validate key (called by app.py) ──────────────────────────────
def validate_key(key: str) -> dict:
    """
    Returns:
        {"valid": True,  "plan": "full",  "customer": "...", "expires_at": "..."}
        {"valid": False, "reason": "..."}
    Called by app.py at /api/activate
    """
    conn = get_db()
    r = conn.execute(
        "SELECT * FROM licenses WHERE key_hash=? AND is_active=1",
        (hash_key(key),)
    ).fetchone()
    conn.close()

    if not r:
        return {"valid": False, "reason": "Key not found or revoked"}

    if r["expires_at"]:
        if datetime.fromisoformat(r["expires_at"]) < datetime.now():
            return {"valid": False, "reason": "License expired"}

    return {
        "valid":      True,
        "plan":       r["plan"],
        "customer":   r["customer"],
        "expires_at": r["expires_at"],
    }

# ── CLI ───────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="GST License Manager")
    p.add_argument("--name",   help="Customer name (for new license)")
    p.add_argument("--email",  help="Customer email")
    p.add_argument("--days",   type=int, default=365, help="Validity in days (0=never)")
    p.add_argument("--plan",   default="full", choices=["full","trial"])
    p.add_argument("--note",   help="Internal note")
    p.add_argument("--list",   action="store_true", help="List all licenses")
    p.add_argument("--revoke", metavar="KEY",  help="Revoke a license key")
    p.add_argument("--info",   metavar="KEY",  help="Show details for a key")
    args = p.parse_args()

    print()
    print("  GST License Manager")
    print("  DB:", DB_PATH)
    print()

    if args.list:
        list_licenses()
    elif args.revoke:
        revoke_license(args.revoke)
    elif args.info:
        show_info(args.info)
    elif args.name:
        create_license(
            customer=args.name,
            email=args.email,
            days=args.days if args.days else None,
            plan=args.plan,
            note=args.note,
        )
    else:
        p.print_help()

if __name__ == "__main__":
    main()
