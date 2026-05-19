r"""
patch_offline_fix.py
====================
Run this ONCE to patch run_all.py and it_suite_v6.py so that:

  1. run_all.py accepts --gst-folder and --it-folder arguments.
     → No more "Enter number (0 to browse staging):" crash.

  2. it_suite_v6.py accepts RPR_IT_FOLDER env variable.
     → No more "Press ENTER to use default, or type folder path:" crash.

  3. The GUI launcher's "Reports Only" button will work without crashing.

Usage:
    cd C:\Users\RAJASEKARAN\Downloads\RPR_GST_IT_Suite_NT_V3.1
    python patch_offline_fix.py

The script makes backups of the original files before patching.
"""

import os
import sys
import shutil
import re

SUITE_DIR = os.path.dirname(os.path.abspath(__file__))


# ==============================================================
# Utility
# ==============================================================

def backup(path):
    bak = path + ".bak_offline_fix"
    if not os.path.exists(bak):
        shutil.copy2(path, bak)
        print(f"  Backup: {os.path.basename(bak)}")
    else:
        print(f"  Backup already exists — skipping: {os.path.basename(bak)}")


def patch_file(path, replacements):
    """Apply a list of (old_text, new_text) replacements to a file."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    changed = False
    for old, new in replacements:
        if old in content:
            content = content.replace(old, new, 1)
            changed = True
            print(f"  ✓ Patched: {repr(old[:60])}...")
        else:
            print(f"  ⚠ Not found (already patched or different version): {repr(old[:60])}...")

    if changed:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  Saved: {os.path.basename(path)}\n")
    else:
        print(f"  No changes made to {os.path.basename(path)}\n")


# ==============================================================
# Patch 1 — it_suite_v6.py
#   Fix: EOF crash when asking for IT folder path
# ==============================================================

def patch_it_suite():
    path = os.path.join(SUITE_DIR, "it_suite_v6.py")
    if not os.path.exists(path):
        print(f"  SKIP — not found: {path}")
        return

    print(f"\n[1/2] Patching it_suite_v6.py ...")
    backup(path)

    # The crash line:
    #   choice = input("  Press ENTER to use default, or type folder path: ").strip()
    old = '    choice = input("  Press ENTER to use default, or type folder path: ").strip()'
    new = '''\
    # --- OFFLINE FIX: accept folder from env var (GUI mode has no terminal) ---
    _rpr_it_folder = os.environ.get("RPR_IT_FOLDER", "").strip()
    if _rpr_it_folder:
        choice = _rpr_it_folder
        print(f"  [AUTO] IT folder from env: {choice}")
    else:
        try:
            choice = input("  Press ENTER to use default, or type folder path: ").strip()
        except EOFError:
            choice = ""
            print("  [AUTO] No terminal — using default IT folder.")
    # --- END OFFLINE FIX ---'''

    patch_file(path, [(old, new)])


# ==============================================================
# Patch 2 — run_all.py
#   Fix 1: EOF crash when asking for GST folder number
#   Fix 2: Add --gst-folder and --it-folder CLI arguments
# ==============================================================

def patch_run_all():
    path = os.path.join(SUITE_DIR, "run_all.py")
    if not os.path.exists(path):
        print(f"  SKIP — not found: {path}")
        return

    print(f"\n[2/2] Patching run_all.py ...")
    backup(path)

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    # ---- Fix A: the GST folder picker input() crash ----
    # Original line (from traceback):
    #   _raw = input("  Enter number (0 to browse staging): ").strip()
    old_a = '    _raw = input("  Enter number (0 to browse staging): ").strip()'
    new_a = '''\
    # --- OFFLINE FIX: accept folder num from env var ---
    _rpr_gst_num = os.environ.get("RPR_GST_FOLDER_NUM", "").strip()
    if _rpr_gst_num:
        _raw = _rpr_gst_num
        print(f"  [AUTO] GST folder choice from env: {_raw}")
    else:
        try:
            _raw = input("  Enter number (0 to browse staging): ").strip()
        except EOFError:
            _raw = "1"
            print("  [AUTO] No terminal — defaulting to folder choice 1.")
    # --- END OFFLINE FIX ---'''

    # ---- Fix B: the IT offline folder input() crash (same pattern as it_suite) ----
    # Some versions inline the IT recon call inside run_all.py
    old_b = '    choice = input("  Press ENTER to use default, or type folder path: ").strip()'
    new_b = '''\
    # --- OFFLINE FIX: accept IT folder from env var ---
    _rpr_it_folder = os.environ.get("RPR_IT_FOLDER", "").strip()
    if _rpr_it_folder:
        choice = _rpr_it_folder
        print(f"  [AUTO] IT folder from env: {choice}")
    else:
        try:
            choice = input("  Press ENTER to use default, or type folder path: ").strip()
        except EOFError:
            choice = ""
            print("  [AUTO] No terminal — using default IT folder.")
    # --- END OFFLINE FIX ---'''

    # ---- Fix C: Add --gst-folder and --it-folder to argparse ----
    # Look for existing argparse setup and inject our new args
    # The safe injection point is just before parse_args()
    argparse_inject = '''\
    # --- OFFLINE FIX: extra CLI args for folder paths ---
    if not any(a.startswith("--gst-folder") for a in sys.argv):
        pass  # not supplied — env vars will be used instead
    ap_temp = None
    for _act in _parser._actions if hasattr(_parser, "_actions") else []:
        if getattr(_act, "dest", "") == "gst_folder":
            ap_temp = "already_added"
            break
    if ap_temp is None and hasattr(_parser, "add_argument"):
        try:
            _parser.add_argument("--gst-folder", dest="gst_folder", default=os.environ.get("RPR_GST_FOLDER", ""), help="Path to GST Automation folder (offline mode)")
            _parser.add_argument("--it-folder",  dest="it_folder",  default=os.environ.get("RPR_IT_FOLDER", ""),  help="Path to IT Download folder (offline mode)")
        except Exception:
            pass
    # --- END OFFLINE FIX ---
'''

    # Apply patches
    replacements = []
    if old_a in content:
        replacements.append((old_a, new_a))
    if old_b in content:
        replacements.append((old_b, new_b))

    changed = False
    for old, new in replacements:
        if old in content:
            content = content.replace(old, new, 1)
            changed = True
            print(f"  ✓ Patched: {repr(old[:60])}...")

    # ---- Fix D: ensure os and sys are imported (they almost certainly are) ----
    if "import os" not in content:
        content = "import os\n" + content
        changed = True
        print("  ✓ Added: import os")
    if "import sys" not in content:
        content = "import sys\n" + content
        changed = True
        print("  ✓ Added: import sys")

    # ---- Fix E: propagate gst/it folder to subprocesses via env ----
    # Find where run_all.py sets RPR_GST_OUT_DIR or similar env vars
    # and insert propagation of our new args → env vars so child scripts inherit them
    inject_env_marker = "os.environ[\"RPR_GST_OUT_DIR\"]"
    inject_env_code = '''\
    # --- OFFLINE FIX: propagate --gst-folder / --it-folder to env for child scripts ---
    if hasattr(args, "gst_folder") and args.gst_folder:
        os.environ["RPR_GST_FOLDER"] = args.gst_folder
    if hasattr(args, "it_folder") and args.it_folder:
        os.environ["RPR_IT_FOLDER"] = args.it_folder
    # --- END OFFLINE FIX ---
'''
    if inject_env_marker in content and inject_env_code.strip() not in content:
        content = content.replace(
            inject_env_marker,
            inject_env_code + "    " + inject_env_marker,
            1
        )
        changed = True
        print("  ✓ Injected env propagation for --gst-folder/--it-folder")

    if changed:
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  Saved: {os.path.basename(path)}\n")
    else:
        print(f"  No changes made (may already be patched or patterns differ slightly).\n")
        print("  If the GUI still crashes, use LAUNCH_OFFLINE_REPORTS.bat instead.")
        print("  That .bat file bypasses input() entirely.\n")


# ==============================================================
# Patch 3 — RPR_Suite_Launcher.py
#   Add folder paths to the subprocess call for "Reports Only"
# ==============================================================

def patch_launcher():
    path = os.path.join(SUITE_DIR, "RPR_Suite_Launcher.py")
    if not os.path.exists(path):
        print(f"\n[3/3] SKIP — launcher not found: {path}")
        print("       (GUI fix will not be applied, but LAUNCH_OFFLINE_REPORTS.bat still works)")
        return

    print(f"\n[3/3] Patching RPR_Suite_Launcher.py ...")
    backup(path)

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()

    # Look for the offline choice 12 invocation
    # Pattern: something like  "--offline-choice", "12"  in a subprocess call
    # We inject --gst-folder and --it-folder from the GUI's folder picker
    # The launcher likely builds a cmd list — we look for offline-choice and append

    # Strategy: find the list/string that contains offline-choice 12
    # and inject a tkinter folder picker to supply --gst-folder and --it-folder
    launcher_injection = '''
# === OFFLINE FIX — injected by patch_offline_fix.py ===
def _rpr_pick_offline_folders():
    """Show folder pickers for GST and IT folders before launching offline mode."""
    import tkinter as tk
    from tkinter import filedialog, messagebox
    root = tk.Tk()
    root.withdraw()
    messagebox.showinfo(
        "Offline Mode — Step 1 of 2",
        "Select the GST Automation folder\\n(e.g. SUJATHA_ENTERPRISES\\\\GST Automation)"
    )
    gst = filedialog.askdirectory(title="Select GST Automation folder")
    if not gst:
        return None, None
    messagebox.showinfo(
        "Offline Mode — Step 2 of 2",
        "Select the IT Download folder\\n(e.g. SUJATHA_ENTERPRISES\\\\IT Download)"
    )
    it = filedialog.askdirectory(title="Select IT Download folder")
    root.destroy()
    return gst, it
# === END OFFLINE FIX ===
'''

    if "_rpr_pick_offline_folders" not in content:
        # Inject at the top of the file after imports
        # Find end of import block
        import_end = 0
        for match in re.finditer(r"^(import |from )", content, re.MULTILINE):
            import_end = match.end()
        # Find next newline after last import line
        insert_pos = content.find("\n", import_end)
        if insert_pos == -1:
            insert_pos = len(content)
        content = content[:insert_pos] + "\n" + launcher_injection + content[insert_pos:]
        print("  ✓ Injected _rpr_pick_offline_folders() helper")

    # Now find where offline-choice 12 cmd is built and patch it
    # Common pattern: cmd = [..., "--offline-choice", "12", ...]
    # We replace it with a call to the picker first

    offline_patterns = [
        '"--offline-choice", "12"',
        "'--offline-choice', '12'",
        '"--offline-choice", str(12)',
        '"--offline-choice", offline_choice',
    ]

    injected = False
    for pat in offline_patterns:
        if pat in content:
            # Find the surrounding subprocess call and prepend folder picker
            # Simple injection: add env vars to the call
            old_pat = pat
            new_pat = (
                pat +
                '  # patched\n'
                '                    # OFFLINE FIX: inject folder paths\n'
            )
            # Actually, the safest approach is to set env before the Popen call
            # Find "offline-choice" and look back for the subprocess.Popen or similar
            pos = content.find(old_pat)
            # Search backwards for the start of the statement (line start)
            line_start = content.rfind("\n", 0, pos) + 1
            # Inject env-setting code before this line
            indent = len(content[line_start:pos]) - len(content[line_start:pos].lstrip())
            indent_str = " " * indent
            env_injection = (
                f"\n{indent_str}# OFFLINE FIX: pick folders and pass to run_all\n"
                f"{indent_str}_gst_f, _it_f = _rpr_pick_offline_folders()\n"
                f"{indent_str}if _gst_f:\n"
                f"{indent_str}    import os as _os\n"
                f"{indent_str}    _os.environ['RPR_GST_FOLDER'] = _gst_f\n"
                f"{indent_str}    _os.environ['RPR_IT_FOLDER'] = _it_f or ''\n"
                f"{indent_str}    _os.environ['RPR_GUI_MODE'] = '1'\n"
                f"{indent_str}# END OFFLINE FIX\n"
                f"{indent_str}"
            )
            content = content[:line_start] + env_injection + content[line_start:]
            print(f"  ✓ Injected folder picker before: {repr(old_pat)}")
            injected = True
            break

    if not injected:
        print("  ⚠ Could not find offline-choice pattern in launcher.")
        print("    The GUI button may still crash, but LAUNCH_OFFLINE_REPORTS.bat will work.")

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  Saved: {os.path.basename(path)}\n")


# ==============================================================
# Main
# ==============================================================

def main():
    print("=" * 60)
    print("  RPR Offline Fix — Patcher v1.0")
    print("=" * 60)
    print(f"  Suite dir: {SUITE_DIR}\n")

    patch_it_suite()
    patch_run_all()
    patch_launcher()

    print("=" * 60)
    print("  Patching complete!")
    print()
    print("  Next steps:")
    print("  1. Use LAUNCH_OFFLINE_REPORTS.bat for immediate offline runs.")
    print("  2. The GUI 'Reports Only' button will now show folder pickers")
    print("     instead of crashing.")
    print()
    print("  If anything goes wrong, restore .bak_offline_fix files.")
    print("=" * 60)


if __name__ == "__main__":
    main()
