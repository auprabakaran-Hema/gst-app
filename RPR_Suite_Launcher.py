"""
RPR GST + IT Suite — Launcher  v3.5 ADVANCED PRO
==================================================
Double-click to open the control panel.
No command prompt. No typing. Just click.

Place this file in the same folder as run_all.py

v3.5 ADVANCED PRO changes (app v14 build):
  • BUG FIX: app.py duplicate "created_at" key removed from job dict (Python drops first key silently)
  • BUG FIX: gst_suite_v35 added to _find_engine + _GST_PY_NAMES fallback lists
  • BUG FIX: global-dl-bar display:none/display:flex conflict fixed (bar couldn't hide on load)
  • BUG FIX: check_suite_files now accepts any gst_suite version (v31–v35/final), not only v32
  • BUG FIX: Navbar / footer / upgrade-banner version strings updated from stale "v32" label
  • BUG FIX: load_clients now actually includes AY2026-27 client manager (was missing despite v10.16 claim)
  • BUG FIX: gst_suite_v32.py route alias (/gst_suite_v32.py) removed — was always 403-blocked
  • BUG FIX: _find_engine now discovers gst_suite v33/v34/v35 (only v31/v32 were in alias list)
  • BUG FIX: _check_rate dict no longer grows unboundedly (stale IPs purged when > 5000)
  • BUG FIX: app.py startup banner now says v3.5 ADVANCED PRO (was showing v7)
  • NEW: /api/version endpoint — returns engine availability map + suite version info
  • NEW: /api/pipeline-health endpoint — deep check of engines, dirs, client manager
  • NEW: api_job now returns elapsed_seconds; progress bar shows ⏱ timer
  • run_all.py bumped to v10.17

v3.3 ADVANCED changes (previous):
  • Added "🔄 Refresh GSTIN Names" button in TOOLS
    → Fetches verified party names from services.gst.gov.in
    → Uses gstin_name_cache.py (official portal, free, no API key)
    → Shows live progress and cache stats in the log panel
  • check_suite_files() now also checks for gstin_name_cache.py
  • Health check reports cache status (portal vs master entries)
  • LAUNCH_SUITE.bat version bumped to v10.15
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import subprocess, sys, os, threading, json, shutil, re
from pathlib import Path
from datetime import datetime

# ── Frozen-EXE detection ───────────────────────────────────────────────────
_FROZEN = getattr(sys, "frozen", False)
if _FROZEN:
    SCRIPT_DIR = Path(sys._MEIPASS)
    EXE_DIR    = Path(sys.executable).parent.resolve()
else:
    SCRIPT_DIR = Path(__file__).parent.resolve()
    EXE_DIR    = SCRIPT_DIR


def _get_python_exe() -> str:
    """Return a real Python interpreter path — never a Windows Store stub."""

    def _is_real(path: str) -> bool:
        """Return True only if path points to a genuine python.exe (not a Store stub)."""
        if not path:
            return False
        p = Path(path)
        if not p.is_file():
            return False
        # Windows Store stubs live under WindowsApps — they open the Store, not Python.
        if "windowsapps" in str(p).lower():
            return False
        # Sanity-check: real interpreter is > 50 KB
        try:
            if p.stat().st_size < 50_000:
                return False
        except OSError:
            return False
        return True

    if not _FROZEN:
        # Normal source run — sys.executable IS the interpreter.
        return sys.executable

    # ── Inside a PyInstaller frozen EXE ──────────────────────────────────────
    # 1. Check next to the EXE (portable distribution)
    exe_dir = Path(sys.executable).parent
    for rel in ("python.exe", "python/python.exe", "Python/python.exe"):
        p = exe_dir / rel
        if _is_real(str(p)):
            return str(p)

    # 2. py.exe launcher (installed by official Python installer — most reliable)
    py_launcher = shutil.which("py")
    if py_launcher and _is_real(py_launcher):
        return py_launcher

    # 3. shutil.which — skip Store stubs
    for candidate in ("python", "python3"):
        found = shutil.which(candidate)
        if found and _is_real(found):
            return found

    # 4. Known installation directories (newest first)
    local_app = Path(os.environ.get("LOCALAPPDATA", ""))
    program_files = Path(os.environ.get("PROGRAMFILES", "C:/Program Files"))
    program_files_x86 = Path(os.environ.get("PROGRAMFILES(X86)", "C:/Program Files (x86)"))
    for ver in ("313", "312", "311", "310", "39", "38"):
        for base in (
            local_app / "Programs" / "Python" / f"Python{ver}",
            Path(f"C:/Python{ver}"),
            program_files / f"Python{ver}",
            program_files_x86 / f"Python{ver}",
            Path(f"C:/Program Files/Python{ver}"),
            Path(f"C:/Program Files (x86)/Python{ver}"),
        ):
            py = base / "python.exe"
            if _is_real(str(py)):
                return str(py)

    # 5. Last resort — hope it's on PATH
    return "python"


# ── Theme v3.5 ADVANCED PRO — Professional dark palette ───────────────────
BG        = "#050B12"   # True deep midnight
CARD      = "#0B1522"   # Rich navy surface
CARD2     = "#0F1D2E"   # Elevated surface
CARD3     = "#111F32"   # Hover / accent surface
PANEL     = "#08111C"   # Sidebar/panel bg
GREEN     = "#00E676"   # Vivid emerald
GREEN_DK  = "#00904A"   # Dark emerald (button)
GREEN_LT  = "#69F0AE"   # Light emerald text
BLUE      = "#00E5FF"   # Cyan accent (primary)
BLUE_DK   = "#0277BD"   # Mid blue
BLUE_LT   = "#80D8FF"   # Light cyan
AMBER     = "#FFB300"   # Warm amber
AMBER_LT  = "#FFE082"   # Light amber
RED       = "#FF1744"   # Alert red
RED_LT    = "#FF6B6B"   # Soft red
PURPLE    = "#7C3AED"   # Deep purple
PURPLE_LT = "#A78BFA"   # Light purple
TEAL      = "#00BCD4"   # Teal accent
TEAL_LT   = "#80DEEA"   # Light teal
WHITE     = "#EBF0FA"   # Off-white text
DIM       = "#3D5470"   # Muted foreground
DIM2      = "#607D9B"   # Lighter muted
BORDER    = "#13233A"   # Subtle border
SUCCESS   = "#B9F5D8"   # Light green text
WARN_BG   = "#1A1200"   # Dark amber bg
GOLD      = "#FFD700"   # Gold accent

# ── Typography ──────────────────────────────────────────────────────────────
F_TITLE  = ("Segoe UI", 19, "bold")
F_HEAD   = ("Segoe UI", 11, "bold")
F_SUB    = ("Segoe UI",  9)
F_BODY   = ("Segoe UI",  9)
F_SMALL  = ("Segoe UI",  8)
F_BTN    = ("Segoe UI", 10, "bold")
F_BTNLG  = ("Segoe UI", 12, "bold")
F_MONO   = ("Consolas",   9)
F_BADGE  = ("Segoe UI",   8, "bold")
F_TINY   = ("Segoe UI",   7)


def _is_tally_running():
    if os.name != "nt":
        return False
    try:
        out = subprocess.check_output(
            ["tasklist", "/FI", "IMAGENAME eq tally.exe"],
            stderr=subprocess.DEVNULL,
            creationflags=0x08000000,
            timeout=3,
        ).decode("utf-8", errors="ignore").lower()
        return "tally.exe" in out
    except Exception:
        return False


# ── Prompt detection ─────────────────────────────────────────────────────────
_PROMPT_PATTERNS = [
    r"password\s*[:\[]",
    r"enter\s+otp",
    r"otp\s*:",
    r"enter\s+captcha",
    r"captcha\s*[:\[]",
    r"press\s+enter\s+after",       # "Press ENTER after typing CAPTCHA"
    r"enter\s+your",
    r"enter\s+the",
    r"type\s+your",
    r"retype\s+the\s+credentials",
    r"please\s+retype",
    r"incorrect\s+password",
    r"wrong\s+password",
    r"invalid\s+password",
    r"login\s+(denied|failed)",
    r"access\s+denied",
    r"authentication\s+failed",
    r"username\s*[:\[]",
    r"gstin\s*[:\[]",
    r"press\s+enter\s+to\s+keep",   # "(Press ENTER to keep the current value)"
    r"\[1[-–]\d+\]",                # numbered menu  e.g. [1-5]:
    r"choice\s*[:\[]",
    r"select\s+company",
    r"which\s+client",
    r"confirm\s*[:\[]",
]
_PROMPT_RE = re.compile("|".join(_PROMPT_PATTERNS), re.IGNORECASE)

# Patterns that mean login FAILED — triggers credential dialog instead of bar
_CRED_FAIL_RE = re.compile(
    r"(access\s+denied|login\s+denied|wrong\s+password|incorrect\s+password"
    r"|retype\s+the\s+credentials|please\s+retype|authentication\s+failed"
    r"|attempt\s+\d+\s+of\s+\d+\s+failed)",
    re.IGNORECASE,
)

def _is_prompt_line(line):
    """Return True only if the line has real content AND matches a prompt pattern."""
    stripped = line.strip()
    # Ignore blank lines, lone arrow lines (►), pure separator lines
    if len(stripped) < 6:
        return False
    if stripped in ("►", "▶", "→", "─" * len(stripped), "═" * len(stripped)):
        return False
    return bool(_PROMPT_RE.search(stripped))

def _is_cred_fail_line(line):
    return bool(_CRED_FAIL_RE.search(line))


def run_script(args, log_widget, status_var, on_done=None, stdin_answers=None,
               input_bar=None):
    """
    Run a subprocess, stream its stdout to log_widget, keep stdin open so the
    user can type responses through input_bar at any time.

    Special handling:
    • Lone ► / blank lines — auto-fed as blank newlines (silent input() calls)
    • "Press ENTER to keep the current value" — fed a blank ONLY when we are
      NOT in credential-failure mode (so the dialog can supply real values)
    • Credential failure (ACCESS DENIED etc.) — blocks the reader thread and
      shows a modal dialog; worker resumes only after the user submits
    • Real prompts (OTP, CAPTCHA, menu) — shows the input bar
    """
    import queue as _queue
    import threading as _threading

    status_var.set("⏳  Running…")

    _stdin_q       = _queue.Queue()
    # Event set by the credential dialog when it is dismissed
    _cred_done_evt = _threading.Event()
    _cred_done_evt.set()   # start in "not waiting" state

    def worker():
        proc = None
        try:
            child_env = {**os.environ,
                         "PYTHONIOENCODING": "utf-8",
                         "PYTHONUTF8":       "1",
                         "PYTHONUNBUFFERED": "1"}

            proc = subprocess.Popen(
                [_get_python_exe()] + args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                text=True, encoding="utf-8", errors="replace",
                bufsize=1, cwd=str(EXE_DIR), env=child_env,
            )

            if input_bar:
                log_widget.after(0, lambda: input_bar.attach(proc))

            # ── Stdin writer: drains queue → proc.stdin ────────────────────────
            def _stdin_writer():
                while True:
                    try:
                        item = _stdin_q.get(timeout=0.5)
                    except _queue.Empty:
                        if proc.poll() is not None:
                            break
                        continue
                    if item is None:
                        break
                    try:
                        proc.stdin.write(item + "\n")
                        proc.stdin.flush()
                    except Exception:
                        break

            _threading.Thread(target=_stdin_writer, daemon=True).start()

            # Feed pre-baked answers
            for ans in (stdin_answers or []):
                _stdin_q.put(str(ans))

            # ── Stream stdout ──────────────────────────────────────────────────
            pending_blanks  = 0
            in_cred_failure = False

            # Matches "► Username: kgr_4719" and "► Password: ****" —
            # these are the actual input() prompts printed by gst_suite
            _CRED_FIELD_RE = re.compile(
                r"^►\s*(username|user|gstin|password|passwd|pwd)\s*[:\-]",
                re.IGNORECASE)

            for raw_line in proc.stdout:
                line    = raw_line.rstrip()
                stripped = line.strip()

                # ── ► Username: ... / ► Password: ... ────────────────────────
                # These are real input() prompts from gst_suite after a failure.
                # Block on the Username line until the credential dialog is done.
                if _CRED_FIELD_RE.match(stripped):
                    _log_line(log_widget, line, "prompt")
                    if in_cred_failure:
                        lo = stripped.lower()
                        is_user = any(w in lo for w in
                                      ("username","user ","gstin"))
                        if is_user:
                            # Block here — dialog already queued both answers
                            _cred_done_evt.wait(timeout=300)
                            in_cred_failure = False
                            pending_blanks  = 0
                        # Password line: answer already in queue from dialog
                    else:
                        # Not a failure re-prompt — keep existing (blank)
                        _stdin_q.put("")
                    continue

                # ── Lone ► / blank = silent input("") call ────────────────────
                if stripped in ("►", "▶", "→", ""):
                    if not in_cred_failure:
                        pending_blanks += 1
                    # In failure mode: discard — dialog feeds the answers
                    continue

                # ── Flush buffered blanks if safe ─────────────────────────────
                if pending_blanks > 0:
                    is_fail = _is_cred_fail_line(line)
                    is_keep = bool(re.search(r"press\s+enter\s+to\s+keep",
                                             line, re.IGNORECASE))
                    if not is_fail and not is_keep:
                        for _ in range(pending_blanks):
                            _stdin_q.put("")
                    pending_blanks = 0

                # ── Credential failure ─────────────────────────────────────────
                if _is_cred_fail_line(line):
                    _log_line(log_widget, line, "err")
                    if not in_cred_failure:
                        in_cred_failure = True
                        _cred_done_evt.clear()
                        if input_bar:
                            log_widget.after(0,
                                lambda l=line: input_bar.show_cred_dialog(
                                    _stdin_q, _cred_done_evt, hint=l))
                    continue

                # ── "Press ENTER to keep the current value" ───────────────────
                # Just log — blocking happens at the ► Username: line above
                if re.search(r"press\s+enter\s+to\s+keep", line, re.IGNORECASE):
                    _log_line(log_widget, line, "prompt")
                    continue

                # ── Real prompt (OTP, CAPTCHA, menu) ──────────────────────────
                if _is_prompt_line(line):
                    _log_line(log_widget, line, "prompt")
                    if input_bar:
                        log_widget.after(0, lambda l=line: input_bar.show(
                            hint=l, stdin_q=_stdin_q))
                    continue

                # ── Normal output ──────────────────────────────────────────────
                _log_line(log_widget, line, _classify_line(line))

            for _ in range(pending_blanks):
                _stdin_q.put("")
            _stdin_q.put(None)

            proc.wait()
            rc = proc.returncode
            log_widget.config(state="normal")
            if rc == 0:
                log_widget.insert(tk.END, "\n  ✅  Finished successfully!\n", "ok")
                status_var.set("✅  Done")
            else:
                log_widget.insert(tk.END,
                    f"\n  ⚠  Exited with code {rc} — check log above\n", "warn")
                status_var.set(f"⚠  Check log (code {rc})")
            log_widget.see(tk.END)
            log_widget.config(state="disabled")

        except Exception as ex:
            log_widget.config(state="normal")
            log_widget.insert(tk.END, f"\n  ❌  Could not start: {ex}\n", "err")
            log_widget.config(state="disabled")
            status_var.set("❌  Error — see log")
        finally:
            _cred_done_evt.set()   # unblock if still waiting
            if input_bar:
                log_widget.after(0, input_bar.detach)
            if on_done:
                log_widget.after(0, on_done)

    _threading.Thread(target=worker, daemon=True).start()


def _classify_line(line):
    """Return a log tag for a normal (non-prompt) output line."""
    lo = line.lower()
    if any(x in lo for x in ["✅","✓","ok ","done","success","complete"]):
        return "ok"
    if any(x in lo for x in ["⚠","warning","warn","skip","ℹ"]):
        return "warn"
    if any(x in lo for x in ["✗","error","fail","❌","exception","traceback"]):
        return "err"
    if any(x in lo for x in ["step","running","▶","━","═","─","==","--"]):
        return "head"
    if any(x in lo for x in ["→","•","downloading","uploading","connecting"]):
        return "info"
    return None


def _log_line(log_widget, line, tag=None):
    """Append one line to the log widget (thread-safe via after())."""
    def _do():
        log_widget.config(state="normal")
        log_widget.insert(tk.END, line + "\n", tag or "")
        log_widget.see(tk.END)
        log_widget.config(state="disabled")
        log_widget.update_idletasks()
    try:
        log_widget.after(0, _do)
    except Exception:
        pass


def check_suite_files(offline=False):
    # Find any available gst_suite version (v32 through v35, or canonical name)
    _GST_NAMES = ["gst_suite_v35.py", "gst_suite_v34.py", "gst_suite_v33.py",
                  "gst_suite_v32.py", "gst_suite_v31.py", "gst_suite_final.py"]
    _gst_found = any((SCRIPT_DIR / n).exists() for n in _GST_NAMES)

    missing = []
    for f in ["run_all.py", "it_suite_v6.py", "gstin_name_cache.py"]:
        if not (SCRIPT_DIR / f).exists():
            missing.append(f)
    if not offline and not _gst_found:
        missing.append("gst_suite_v32.py (or v33/v34/v35/final)")
    return missing


class Tip:
    def __init__(self, w, txt):
        self.w, self.txt, self.tw = w, txt, None
        w.bind("<Enter>", self.show)
        w.bind("<Leave>", self.hide)

    def show(self, _):
        try:
            x = self.w.winfo_rootx() + 20
            y = self.w.winfo_rooty() + self.w.winfo_height() + 4
            self.tw = tk.Toplevel(self.w)
            self.tw.wm_overrideredirect(True)
            self.tw.wm_geometry(f"+{x}+{y}")
            tk.Label(self.tw, text=self.txt, justify="left",
                     bg="#1E293B", fg="#CBD5E1", relief="solid",
                     borderwidth=1, font=("Segoe UI", 8),
                     padx=10, pady=8, wraplength=280).pack()
        except Exception:
            pass

    def hide(self, _):
        if self.tw:
            try:
                self.tw.destroy()
            except Exception:
                pass
            self.tw = None


# ═══════════════════════════════════════════════════════════════════════════════
# INTERACTIVE INPUT BAR
# Shown whenever the running subprocess prints a prompt line.
# Lets the user type passwords, OTPs, CAPTCHAs, menu choices etc. and sends
# them directly to the subprocess stdin — no console window needed.
# ═══════════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════════
# INTERACTIVE INPUT BAR
# ═══════════════════════════════════════════════════════════════════════════════
class InteractiveInputBar:
    """
    Collapsible amber panel below the log.
    Shown for OTPs, CAPTCHAs, menu choices.
    For credential failures, opens a proper modal dialog instead.
    """

    _PASSWORD_WORDS = {"password", "passwd", "pwd", "secret", "pin"}

    def __init__(self, parent, log_widget):
        self._parent   = parent
        self._log      = log_widget
        self._proc     = None
        self._stdin_q  = None   # set by show()
        self._visible  = False
        self._build()

    def _build(self):
        self._frame = tk.Frame(self._parent, bg=WARN_BG)

        # Amber top border
        tk.Frame(self._frame, bg=AMBER, height=2).pack(fill="x")

        # Header row
        hdr = tk.Frame(self._frame, bg=WARN_BG)
        hdr.pack(fill="x", padx=10, pady=(6, 2))
        tk.Label(hdr, text="⌨  Input needed:",
                 font=("Segoe UI", 9, "bold"),
                 bg=WARN_BG, fg=AMBER).pack(side="left", padx=(0, 6))
        self._hint_lbl = tk.Label(hdr, text="",
                                  font=("Segoe UI", 8), bg=WARN_BG, fg="#CBD5E1",
                                  anchor="w", wraplength=500, justify="left")
        self._hint_lbl.pack(side="left", fill="x", expand=True)

        # Entry row
        entry_row = tk.Frame(self._frame, bg=WARN_BG)
        entry_row.pack(fill="x", padx=10, pady=(2, 8))

        # Visible border frame around the entry
        entry_border = tk.Frame(entry_row, bg=AMBER, bd=1, relief="solid")
        entry_border.pack(side="left", fill="x", expand=True, padx=(0, 6))

        self._entry_var = tk.StringVar()
        self._entry = tk.Entry(
            entry_border,
            textvariable=self._entry_var,
            font=("Consolas", 11),
            bg="#0D1520", fg="#F1F5F9",
            insertbackground="#F59E0B",   # amber cursor
            relief="flat", bd=6,
            highlightthickness=0,
        )
        self._entry.pack(fill="x", ipady=5)
        self._entry.bind("<Return>",   lambda e: self._send())
        self._entry.bind("<KP_Enter>", lambda e: self._send())

        tk.Button(
            entry_row, text="Send  ↵",
            font=("Segoe UI", 9, "bold"),
            bg=AMBER, fg="#1A1A00",
            relief="flat", padx=14, pady=6,
            activebackground="#D97706",
            command=self._send,
        ).pack(side="left")

        tk.Button(
            entry_row, text="Skip",
            font=("Segoe UI", 8),
            bg=CARD2, fg=DIM2,
            relief="flat", padx=8, pady=6,
            activebackground=CARD,
            command=self._skip,
        ).pack(side="left", padx=(4, 0))

    # ── Public API ────────────────────────────────────────────────────────────

    def attach(self, proc):
        self._proc = proc

    def detach(self):
        self._proc    = None
        self._stdin_q = None
        self.hide()

    def show(self, hint="", stdin_q=None):
        """Show bar for a single-field prompt (OTP, CAPTCHA, menu choice)."""
        if stdin_q:
            self._stdin_q = stdin_q

        lo = hint.lower()
        is_pw = any(w in lo for w in self._PASSWORD_WORDS)
        self._entry.config(show="*" if is_pw else "")
        self._entry_var.set("")

        display = hint.strip()[:130]
        self._hint_lbl.config(text=display)

        if not self._visible:
            self._frame.pack(fill="x", before=self._log)
            self._visible = True

        self._entry.focus_set()

    def hide(self):
        if self._visible:
            try:
                self._frame.pack_forget()
            except Exception:
                pass
            self._visible = False

    def show_cred_dialog(self, stdin_q, done_event=None, hint=""):
        """
        Pop a modal credential dialog for username + password re-entry.
        Feeds username then password into stdin_q when submitted.
        Sets done_event so the blocked worker thread can resume.
        """
        self._stdin_q = stdin_q

        # Extract client name from hint if possible
        client = ""
        m = re.search(r"client[:\s]+(\S+)", hint, re.IGNORECASE)
        if not m:
            m = re.search(r"\b([a-z0-9_]{4,20}_\d{4})\b", hint, re.IGNORECASE)
        if m:
            client = m.group(1)

        dlg = tk.Toplevel(self._parent)
        dlg.title("Login Failed — Re-enter Credentials")
        dlg.configure(bg=CARD)
        dlg.resizable(False, False)
        dlg.grab_set()
        dlg.lift()
        dlg.attributes("-topmost", True)

        # Centre over parent
        pw, ph = 440, 310
        px = self._parent.winfo_rootx() + (self._parent.winfo_width()  - pw) // 2
        py = self._parent.winfo_rooty() + (self._parent.winfo_height() - ph) // 2
        dlg.geometry(f"{pw}x{ph}+{px}+{py}")

        # Header
        tk.Frame(dlg, bg=RED, height=3).pack(fill="x")
        hdr = tk.Frame(dlg, bg="#2A0808")
        hdr.pack(fill="x")
        tk.Label(hdr, text="❌  GST Login Failed — Wrong Password",
                 font=("Segoe UI", 10, "bold"),
                 bg="#2A0808", fg=RED, pady=10).pack(side="left", padx=14)

        body = tk.Frame(dlg, bg=CARD)
        body.pack(fill="both", expand=True, padx=20, pady=10)

        if client:
            tk.Label(body, text=f"Client: {client}",
                     font=("Segoe UI", 9, "bold"),
                     bg=CARD, fg=AMBER, anchor="w").pack(anchor="w", pady=(0, 4))

        tk.Label(body,
                 text="Username or Password is wrong.\n"
                      "Enter the correct values below. Leave blank to keep existing.",
                 font=("Segoe UI", 8), bg=CARD, fg=DIM2,
                 justify="left", anchor="w").pack(anchor="w", pady=(0, 10))

        # Username
        tk.Label(body, text="Username / GSTIN:", font=("Segoe UI", 9),
                 bg=CARD, fg=DIM2, anchor="w").pack(anchor="w")
        ub = tk.Frame(body, bg=BORDER, bd=1, relief="solid")
        ub.pack(fill="x", pady=(2, 8))
        user_var = tk.StringVar()
        user_ent = tk.Entry(ub, textvariable=user_var,
                            font=("Consolas", 10), bg=CARD2, fg=WHITE,
                            insertbackground=WHITE, relief="flat", bd=5)
        user_ent.pack(fill="x")

        # Password
        tk.Label(body, text="Password:", font=("Segoe UI", 9),
                 bg=CARD, fg=DIM2, anchor="w").pack(anchor="w")
        pb = tk.Frame(body, bg=AMBER, bd=1, relief="solid")
        pb.pack(fill="x", pady=(2, 12))
        pw_var = tk.StringVar()
        pw_ent = tk.Entry(pb, textvariable=pw_var, show="*",
                          font=("Consolas", 10), bg=CARD2, fg=WHITE,
                          insertbackground="#F59E0B", relief="flat", bd=5)
        pw_ent.pack(fill="x")

        def _submit():
            u = user_var.get().strip()
            p = pw_var.get().strip()
            self._log.config(state="normal")
            if u:
                self._log.insert(tk.END, f"  ▷  Username: {u}\n", "input")
            self._log.insert(tk.END,
                f"  ▷  Password: {'*' * len(p)}\n", "input")
            self._log.see(tk.END)
            self._log.config(state="disabled")
            stdin_q.put(u)   # username field (blank = keep existing)
            stdin_q.put(p)   # password field
            dlg.destroy()
            if done_event:
                done_event.set()   # unblock worker thread

        def _cancel():
            self._log.config(state="normal")
            self._log.insert(tk.END,
                "  ▷  Keeping existing credentials (blank sent)\n", "input")
            self._log.see(tk.END)
            self._log.config(state="disabled")
            stdin_q.put("")   # keep existing username
            stdin_q.put("")   # keep existing password
            dlg.destroy()
            if done_event:
                done_event.set()

        btn_row = tk.Frame(body, bg=CARD)
        btn_row.pack(fill="x")
        tk.Button(btn_row, text="✓  Submit",
                  font=("Segoe UI", 9, "bold"),
                  bg="#166534", fg=WHITE, relief="flat", padx=16, pady=7,
                  activebackground="#14532D",
                  command=_submit).pack(side="left")
        tk.Button(btn_row, text="Keep existing",
                  font=("Segoe UI", 8), bg=CARD2, fg=DIM2,
                  relief="flat", padx=10, pady=7,
                  activebackground=CARD,
                  command=_cancel).pack(side="left", padx=8)

        user_ent.focus_set()
        user_ent.bind("<Return>", lambda e: pw_ent.focus_set())
        pw_ent.bind("<Return>",   lambda e: _submit())
        dlg.bind("<Escape>",      lambda e: _cancel())
        # Safety: if user closes the window with X, treat as cancel
        dlg.protocol("WM_DELETE_WINDOW", _cancel)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _send(self):
        text = self._entry_var.get()
        self._entry_var.set("")
        lo_hint = self._hint_lbl.cget("text").lower()
        is_pw = any(w in lo_hint for w in self._PASSWORD_WORDS)
        echo = ("*" * len(text)) if is_pw else text
        self._log.config(state="normal")
        self._log.insert(tk.END, f"  ▷  {echo}\n", "input")
        self._log.see(tk.END)
        self._log.config(state="disabled")
        if self._stdin_q is not None:
            self._stdin_q.put(text)
        elif self._proc and self._proc.stdin:
            try:
                self._proc.stdin.write(text + "\n")
                self._proc.stdin.flush()
            except Exception:
                pass

    def _skip(self):
        self._entry_var.set("")
        self._send()


# ═══════════════════════════════════════════════════════════════════════════════
class RPRLauncher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RPR GST + IT Suite  v3.5 ADVANCED PRO")
        self.configure(bg=BG)
        self.resizable(True, True)
        self.minsize(1000, 700)
        w, h = 1160, 860
        x = (self.winfo_screenwidth()  - w) // 2
        y = (self.winfo_screenheight() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

        # ── Vars ──────────────────────────────────────────────────────────────
        self.status_var     = tk.StringVar(value="Ready — choose an action below")
        self.fy_var         = tk.StringVar(value="2026-27")
        self.client_var     = tk.StringVar(value="")
        self.skip_tally_var = tk.BooleanVar(value=False)
        self.gst_mode_var   = tk.StringVar(value="recommended")
        self.gst_fy_var     = tk.StringVar(value="2026-27")
        self.it_mode_var    = tk.StringVar(value="all")
        self.running        = False
        self._tally_status  = "unknown"

        # Advanced vars
        self.output_path_var = tk.StringVar(value="")
        self.dry_run_var    = tk.BooleanVar(value=False)
        self.debug_var      = tk.BooleanVar(value=False)
        self.skip_gst_var   = tk.BooleanVar(value=False)
        self.skip_it_var    = tk.BooleanVar(value=False)
        self.skip_bridge_var= tk.BooleanVar(value=False)
        self._STEPS = [
            ("1",   "Tally — Read Supplier Names",        "--only-tally"),
            ("2-3", "Download GST Returns",               "--only-gst"),
            ("4-5", "Download IT Documents",              "--only-it"),
            ("6",   "Build Reconciliation Reports",       "--only-bridge"),
            ("6b",  "GST vs IT Comparison",               "--only-compare"),
            ("6c",  "GSTR-2B Summary Extract",            "--only-2b"),
            ("6d",  "26AS vs Sales Return Match",         "--only-26as"),
            ("7",   "Final Consolidated Report",          "--only-final"),
        ]
        self.step_vars = {f: tk.BooleanVar(value=False) for _,_,f in self._STEPS}
        self._cmd_preview_var = tk.StringVar(value="python run_all.py")
        self._adv_body_ref    = None
        self._adv_chevron_ref = None

        self._build_ui()
        self._check_health()
        self._start_tally_poll()
        # ── Keyboard shortcuts ──────────────────────────────────────────────
        self.bind_all("<Control-r>", lambda e: self.run_full() if not self.running else None)
        self.bind_all("<Control-l>", lambda e: self.clear_log())
        self.bind_all("<Control-s>", lambda e: self.save_log())

    # ═══════════════════════════════════════════════════════════════════════════
    # TOP-LEVEL UI LAYOUT
    # ═══════════════════════════════════════════════════════════════════════════
    def _build_ui(self):
        # ── Header ──────────────────────────────────────────────────────────
        hdr = tk.Frame(self, bg=CARD, pady=0)
        hdr.pack(fill="x")

        hdr_inner = tk.Frame(hdr, bg=CARD)
        hdr_inner.pack(fill="x", padx=20, pady=14)

        left_hdr = tk.Frame(hdr_inner, bg=CARD)
        left_hdr.pack(side="left")

        # Title row with version badge
        title_row = tk.Frame(left_hdr, bg=CARD)
        title_row.pack(anchor="w", fill="x")
        tk.Label(title_row, text="⚡ RPR  GST + IT  Suite",
                 font=F_TITLE, bg=CARD, fg=WHITE).pack(side="left")
        tk.Label(title_row,
                 text=" v3.5 ADV PRO ",
                 font=("Segoe UI", 7, "bold"),
                 bg=GOLD, fg="#000", padx=3, pady=1,
                 relief="flat").pack(side="left", padx=(6,0), pady=(4,0), anchor="n")
        tk.Label(left_hdr,
                 text="Indian CA Tax Automation  ·  FY 2026-27 / 2027-28  ·  GST + Income Tax  ·  Professional Edition",
                 font=F_SMALL, bg=CARD, fg=DIM2).pack(anchor="w", pady=(2,0))

        right_hdr = tk.Frame(hdr_inner, bg=CARD)
        right_hdr.pack(side="right", anchor="e")
        self.health_label = tk.Label(right_hdr, text="Checking setup…",
                                     font=F_BODY, bg=CARD, fg=AMBER)
        self.health_label.pack(anchor="e")
        self.client_count_label = tk.Label(right_hdr, text="",
                                           font=F_SMALL, bg=CARD, fg=DIM)
        self.client_count_label.pack(anchor="e", pady=(3,0))

        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")

        # ── Body ────────────────────────────────────────────────────────────
        body = tk.Frame(self, bg=BG)
        body.pack(fill="both", expand=True)

        # Left sidebar (scrollable)
        left_outer = tk.Frame(body, bg=BG, width=370)
        left_outer.pack(side="left", fill="y")
        left_outer.pack_propagate(False)

        cv = tk.Canvas(left_outer, bg=BG, highlightthickness=0)
        sb = tk.Scrollbar(left_outer, orient="vertical", command=cv.yview)
        cv.configure(yscrollcommand=sb.set)
        sb.pack(side="right", fill="y")
        cv.pack(side="left", fill="both", expand=True)

        left = tk.Frame(cv, bg=BG)
        _win = cv.create_window((0,0), window=left, anchor="nw")

        left.bind("<Configure>",  lambda e: cv.configure(scrollregion=cv.bbox("all")))
        cv.bind("<Configure>",    lambda e: cv.itemconfig(_win, width=e.width))

        def _scroll(e):
            # Only scroll when the pointer is inside the left sidebar
            wx = left_outer.winfo_rootx()
            wy = left_outer.winfo_rooty()
            ww = left_outer.winfo_width()
            wh = left_outer.winfo_height()
            px, py = e.x_root, e.y_root
            if not (wx <= px <= wx + ww and wy <= py <= wy + wh):
                return
            delta = getattr(e, "delta", 0)
            if delta != 0:
                cv.yview_scroll(int(-1 * (delta / 120)), "units")
            elif getattr(e, "num", 0) == 4:
                cv.yview_scroll(-1, "units")
            elif getattr(e, "num", 0) == 5:
                cv.yview_scroll(1, "units")

        # Bind at the top-level window so ALL child widgets (including
        # buttons and labels added later) propagate scroll to the canvas.
        self.bind_all("<MouseWheel>", _scroll, add="+")
        self.bind_all("<Button-4>",   _scroll, add="+")
        self.bind_all("<Button-5>",   _scroll, add="+")

        # Right panel (log)
        right = tk.Frame(body, bg=BG)
        right.pack(side="right", fill="both", expand=True)

        self._build_left(left, _scroll)
        self._build_right(right)

        # ── Status bar ──────────────────────────────────────────────────────
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")
        bot = tk.Frame(self, bg=CARD, pady=6)
        bot.pack(fill="x")
        # Left: status text
        tk.Label(bot, textvariable=self.status_var,
                 font=F_SMALL, bg=CARD, fg=DIM2).pack(side="left", padx=14)
        # Right: session run counter + Python version + folder link
        self._run_count = 0
        self._run_count_var = tk.StringVar(value="0 runs this session")
        tk.Label(bot, textvariable=self._run_count_var,
                 font=F_TINY if hasattr(tk,'_default_root') else F_SMALL,
                 bg=CARD, fg=DIM).pack(side="right", padx=8)
        tk.Label(bot, text="·", bg=CARD, fg=BORDER).pack(side="right")
        pyv = sys.version.split()[0]
        tk.Label(bot, text=f"🐍 Python {pyv}",
                 font=F_SMALL, bg=CARD, fg=DIM).pack(side="right", padx=8)
        tk.Label(bot, text="·", bg=CARD, fg=BORDER).pack(side="right")
        fl = tk.Label(bot, text=f"📁  {EXE_DIR}",
                      font=F_SMALL, bg=CARD, fg=DIM, cursor="hand2")
        fl.pack(side="right", padx=8)
        fl.bind("<Button-1>", lambda e: self.open_output_folder())

    def _bind_scroll(self, widget, fn):
        widget.bind("<MouseWheel>", fn, add="+")
        widget.bind("<Button-4>",   fn, add="+")
        widget.bind("<Button-5>",   fn, add="+")
        for ch in widget.winfo_children():
            self._bind_scroll(ch, fn)

    # ═══════════════════════════════════════════════════════════════════════════
    # LEFT SIDEBAR
    # ═══════════════════════════════════════════════════════════════════════════
    def _build_left(self, parent, scroll_fn):

        # ── 1. SETUP CHECKLIST ──────────────────────────────────────────────
        self._build_setup_checklist(parent)

        # ── 2. TALLY STATUS ─────────────────────────────────────────────────
        self._build_tally_bar(parent)

        # ── 3. SETTINGS ─────────────────────────────────────────────────────
        sec = self._card(parent, "⚙  SETTINGS")

        # Financial Year
        fy_row = tk.Frame(sec, bg=CARD)
        fy_row.pack(fill="x", pady=2)
        tk.Label(fy_row, text="Financial Year", font=F_BODY,
                 bg=CARD, fg=DIM2, width=18, anchor="w").pack(side="left")
        fy_e = tk.Entry(fy_row, textvariable=self.fy_var, font=F_BODY,
                        bg=CARD2, fg=WHITE, insertbackground=WHITE,
                        relief="flat", bd=4, width=12)
        fy_e.pack(side="left", padx=6)
        fy_e.bind("<KeyRelease>", lambda e: self._refresh_cmd())
        Tip(fy_e, "The financial year you want to process.\nDefault: 2026-27  (1 Apr 2026 – 31 Mar 2027)")

        # Client filter
        cl_row = tk.Frame(sec, bg=CARD)
        cl_row.pack(fill="x", pady=2)
        tk.Label(cl_row, text="Process one client", font=F_BODY,
                 bg=CARD, fg=DIM2, width=18, anchor="w").pack(side="left")
        cl_e = tk.Entry(cl_row, textvariable=self.client_var, font=F_BODY,
                        bg=CARD2, fg=WHITE, insertbackground=WHITE,
                        relief="flat", bd=4, width=12)
        cl_e.pack(side="left", padx=6)
        cl_e.bind("<KeyRelease>", lambda e: self._refresh_cmd())
        Tip(cl_e, "Leave BLANK to process ALL clients at once.\n\nType a name here to test with just one client first.\nExample:  NURSERY GARDEN  or  ABC TRADERS")

        # Skip Tally
        skip_row = tk.Frame(sec, bg=CARD)
        skip_row.pack(fill="x", pady=(6,2))
        skip_cb = tk.Checkbutton(
            skip_row, text="Skip Tally step  (if Tally is not open)",
            variable=self.skip_tally_var,
            font=F_BODY, bg=CARD, fg=DIM2,
            activebackground=CARD, activeforeground=WHITE,
            selectcolor=CARD2,
            command=self._refresh_cmd)
        skip_cb.pack(side="left")
        Tip(skip_cb, "Tick this box if Tally is not open right now.\n\nTally is only needed in Step 1 to read your supplier names.\nIf unticked and Tally is closed, the program will open it automatically.")

        # Output Path (offline / reports)
        tk.Frame(sec, bg=BORDER, height=1).pack(fill="x", pady=(8,4))
        tk.Label(sec, text="Output / Save-to Folder  (optional)",
                 font=F_SMALL, bg=CARD, fg=DIM2, anchor="w").pack(anchor="w", pady=(0,3))

        out_row = tk.Frame(sec, bg=CARD)
        out_row.pack(fill="x", pady=2)

        out_e = tk.Entry(out_row, textvariable=self.output_path_var, font=F_SMALL,
                         bg=CARD2, fg=WHITE, insertbackground=WHITE,
                         relief="flat", bd=3)
        out_e.pack(side="left", fill="x", expand=True, padx=(0,4))
        out_e.bind("<KeyRelease>", lambda e: self._refresh_cmd())
        Tip(out_e, "Custom folder where reports/downloads are saved.\n\n"
                   "Leave blank to use the default folder next to the suite.\n"
                   "Useful when you want outputs on a shared drive or a specific path.")

        def _browse_out():
            folder = filedialog.askdirectory(
                title="Choose output / save-to folder",
                initialdir=self.output_path_var.get() or str(EXE_DIR),
            )
            if folder:
                self.output_path_var.set(folder)
                self._refresh_cmd()

        browse_btn = tk.Label(out_row, text="Browse",
                              font=F_SMALL, bg=BLUE_DK, fg="white",
                              padx=8, pady=4, cursor="hand2")
        browse_btn.pack(side="left", padx=(0,3))
        browse_btn.bind("<Button-1>", lambda e: _browse_out())
        Tip(browse_btn, "Browse for an output folder.")

        clear_out_btn = tk.Label(out_row, text="X",
                                 font=F_SMALL, bg=CARD2, fg=DIM2,
                                 padx=6, pady=4, cursor="hand2")
        clear_out_btn.pack(side="left")
        clear_out_btn.bind("<Button-1>",
                           lambda e: (self.output_path_var.set(""), self._refresh_cmd()))
        Tip(clear_out_btn, "Clear the custom path and use the default folder.")

        # ── 4. WHAT TO DOWNLOAD (GST) ───────────────────────────────────────
        gst_sec = self._card(parent, "🟢  WHAT TO DOWNLOAD  —  GST")

        # ── GST MODE DROPDOWN ────────────────────────────────────────────────
        self._gst_options = [
            # (value, display label, tooltip)
            # ── Fast Direct Downloads ──
            ("2b_only",       "⚡ Fast  |  GSTR-2B Only  (~5 min)",
             "Direct download of GSTR-2B (Purchase Return).\nNo generate step — instant portal download.\nQuick ITC verification."),
            ("3b_only",       "⚡ Fast  |  GSTR-3B Only  (~5 min)",
             "Direct download of GSTR-3B (Tax Statement).\nNo generate step — instant PDF download.\nFastest option for tax-only reconciliation."),
            ("2b_3b",         "⚡ Fast  |  GSTR-2B + GSTR-3B  (~10 min)",
             "Fast download of both purchase return and tax statement.\nNo generate wait — both are direct portal downloads.\nBest for quick monthly reconciliation."),
            # ── Recommended / Full Suites ──
            ("recommended",   "✅ Recommended  (GSTR-1, 2B, 3B + Tax Report)",
             "Downloads the 4 most important GST returns.\nBest for most offices — fastest & most complete."),
            ("full_optimized","🚀 Full Optimised  (all 5 returns, best order)",
             "Runs all 5 returns in optimal sequence:\nGenerate 1 & 2A → Download 2B → Download 3B → Tax Liability → Collect.\nFastest way to get everything."),
            ("full",          "📦 Full Download  (all returns including 2A)",
             "Downloads ALL GST returns including GSTR-2A.\nTakes longer but gives the most complete data."),
            # ── Individual Returns ──
            ("sales",         "📄 Individual  |  GSTR-1 Only  (Sales, JSON)",
             "Downloads only GSTR-1 (Sales Return).\nGenerates on portal first, then downloads JSON.\nAbout 5–8 minutes."),
            ("purchase",      "📄 Individual  |  GSTR-2A Only  (ITC, Excel)",
             "Downloads only GSTR-2A (ITC Return).\nGenerates on portal first, then downloads Excel.\nAbout 5–8 minutes."),
            # ── Combos ──
            ("1_3b",          "🔗 Combo  |  GSTR-1 + GSTR-3B",
             "Downloads GSTR-1 and GSTR-3B.\nNo purchase returns (no 2B or 2A).\nGood for sales + tax reconciliation only."),
            ("2b_2a",         "🔗 Combo  |  GSTR-2B + GSTR-2A",
             "Downloads both purchase returns: GSTR-2B and GSTR-2A.\nNo sales return (1) or tax statement (3B).\nComplete ITC view."),
            ("1_2b_2a",       "🔗 Combo  |  GSTR-1 + GSTR-2B + GSTR-2A  (no Tax)",
             "Downloads sales (1) and both purchase returns (2B, 2A).\nNo tax statement (3B).\nFull return data without 3B."),
            ("1_2b_3b",       "🔗 Combo  |  GSTR-1 + GSTR-2B + GSTR-3B  (no 2A)",
             "Most popular combo without ITC return.\nDownloads GSTR-1, 2B, and 3B — no GSTR-2A.\nFull reconciliation without waiting for 2A generate."),
            # ── Multi-year ──
            ("multi_fy",      "📅 Multi-Year  |  2026-27 + 2025-26  (Fast, all returns)",
             "Special 2-year optimized download sequence.\nGenerates GSTR-1 & 1A simultaneously for both FYs.\nDownloads 2B & 3B directly, then Tax Liability for both years.\nBest for year-end comprehensive filing."),
            # ── Utilities ──
            ("offline",       "💾 Tools  |  Offline Reconciliation  (no browser)",
             "Does NOT open the browser or GST portal.\nUse this when files are already downloaded.\nBuilds reconciliation Excel from existing PDFs/files."),
            ("retry_failed",  "🔁 Tools  |  Retry Failed Downloads",
             "Reads Master Report Excel from a previous run.\nIdentifies failed downloads (T1/T2/T3 timeouts).\nRetries only the failed months with better timing."),
            ("tax_only",      "📊 Tools  |  Tax Liability & ITC Comparison  (2-year)",
             "Downloads Tax Liability and ITC data for 2-year comparison.\nNo GST return downloads — tax comparison report only.\nCovers 2024-25 + 2023-24."),
            ("3b_summary",    "📋 Tools  |  GSTR-3B Summary Report  (PDF → Excel)",
             "Reads downloaded GSTR-3B PDF files.\nGenerates structured Summary Excel (XLSM format).\nSections: Sales | Purchase | Tax Liability | ITC | Cash Offset."),
            ("custom",        "⚙  Tools  |  Custom — Select exact returns manually",
             "Choose exactly which returns you need.\nAvailable: GSTR1, GSTR1A, GSTR2B, GSTR2A, GSTR3B, TAX_LIABILITY.\nType your own combination — maximum flexibility."),
        ]
        self._gst_val_map  = {label: val  for val, label, _ in self._gst_options}
        self._gst_tip_map  = {val: tip    for val, _, tip   in self._gst_options}
        self._gst_labels   = [label       for _, label, _   in self._gst_options]

        gst_dd_row = tk.Frame(gst_sec, bg=CARD)
        gst_dd_row.pack(fill="x", pady=(4, 2))

        style = ttk.Style()
        style.theme_use("default")
        style.configure("GST.TCombobox",
                        fieldbackground=CARD2, background=CARD2,
                        foreground=WHITE, selectbackground=BLUE_DK,
                        selectforeground=WHITE, borderwidth=0,
                        arrowcolor=DIM2, relief="flat")
        style.map("GST.TCombobox",
                  fieldbackground=[("readonly", CARD2)],
                  foreground=[("readonly", WHITE)],
                  selectbackground=[("readonly", BLUE_DK)])

        # Default to "recommended"
        default_gst_label = next(
            lbl for val, lbl, _ in self._gst_options if val == "recommended")
        self._gst_display_var = tk.StringVar(value=default_gst_label)

        self._gst_combo = ttk.Combobox(
            gst_dd_row,
            textvariable=self._gst_display_var,
            values=self._gst_labels,
            state="readonly",
            style="GST.TCombobox",
            font=F_BODY,
            width=46,
        )
        self._gst_combo.pack(fill="x", padx=0, pady=2, ipady=5)
        self._gst_combo.bind("<<ComboboxSelected>>", self._on_gst_combo)

        self._gst_tip_label = tk.Label(
            gst_sec, text=self._gst_tip_map.get("recommended", ""),
            font=F_SMALL, bg=CARD, fg=DIM2,
            justify="left", wraplength=310, anchor="w")
        self._gst_tip_label.pack(anchor="w", padx=2, pady=(2, 4))

        # GST FY row (compact)
        gfy_row = tk.Frame(gst_sec, bg=CARD)
        gfy_row.pack(fill="x", pady=(6,0))
        tk.Label(gfy_row, text="GST year:", font=F_SMALL,
                 bg=CARD, fg=DIM, width=10, anchor="w").pack(side="left")
        gfy_e = tk.Entry(gfy_row, textvariable=self.gst_fy_var, font=F_SMALL,
                         bg=CARD2, fg=WHITE, insertbackground=WHITE,
                         relief="flat", bd=3, width=10)
        gfy_e.pack(side="left", padx=4)
        gfy_e.bind("<KeyRelease>", lambda e: self._refresh_cmd())
        Tip(gfy_e, "Usually the same as the Financial Year above.\ne.g. 2026-27")

        # ── 5. WHAT TO DOWNLOAD (IT) ────────────────────────────────────────
        it_sec = self._card(parent, "🔵  WHAT TO DOWNLOAD  —  INCOME TAX")

        self._it_options = [
            ("all",     "📋 All IT Documents  (26AS + AIS + TIS + Recon Excel)",
             "Downloads everything from the Income Tax portal:\n• 26AS Annual Statement\n• AIS (Annual Information Statement)\n• TIS (Taxpayer Information Summary)\n• Builds Reconciliation Excel\n\nBest choice for most offices."),
            ("26as",    "📄 26AS Only",
             "Downloads only the 26AS Annual Tax Statement.\nQuick download — no other IT documents."),
            ("ais_tis", "📑 AIS + TIS Only",
             "Downloads AIS and TIS statements only.\nDoes not download 26AS."),
            ("recon",   "📊 Recon Excel Only  (from existing PDFs)",
             "Does NOT download anything from the portal.\nBuilds the IT Reconciliation Excel using\nPDFs that are already downloaded."),
        ]
        self._it_val_map  = {label: val  for val, label, _ in self._it_options}
        self._it_tip_map  = {val: tip    for val, _, tip   in self._it_options}
        self._it_labels   = [label       for _, label, _   in self._it_options]

        style = ttk.Style()
        style.configure("IT.TCombobox",
                        fieldbackground=CARD2, background=CARD2,
                        foreground=WHITE, selectbackground=BLUE_DK,
                        selectforeground=WHITE, borderwidth=0,
                        arrowcolor=DIM2, relief="flat")
        style.map("IT.TCombobox",
                  fieldbackground=[("readonly", CARD2)],
                  foreground=[("readonly", WHITE)],
                  selectbackground=[("readonly", BLUE_DK)])

        default_it_label = next(
            lbl for val, lbl, _ in self._it_options if val == "all")
        self._it_display_var = tk.StringVar(value=default_it_label)

        it_dd_row = tk.Frame(it_sec, bg=CARD)
        it_dd_row.pack(fill="x", pady=(4, 2))

        self._it_combo = ttk.Combobox(
            it_dd_row,
            textvariable=self._it_display_var,
            values=self._it_labels,
            state="readonly",
            style="IT.TCombobox",
            font=F_BODY,
            width=46,
        )
        self._it_combo.pack(fill="x", padx=0, pady=2, ipady=5)
        self._it_combo.bind("<<ComboboxSelected>>", self._on_it_combo)

        self._it_tip_label = tk.Label(
            it_sec, text=self._it_tip_map.get("all", ""),
            font=F_SMALL, bg=CARD, fg=DIM2,
            justify="left", wraplength=310, anchor="w")
        self._it_tip_label.pack(anchor="w", padx=2, pady=(2, 4))

        # ── 6. MAIN ACTIONS ─────────────────────────────────────────────────
        act_sec = self._card(parent, "🚀  START")

        # Hero button
        hero = tk.Frame(act_sec, bg=GREEN_DK, cursor="hand2")
        hero.pack(fill="x", pady=(2,4))
        tk.Label(hero, text="  🔄  RUN EVERYTHING",
                 font=("Segoe UI", 15, "bold"),
                 bg=GREEN_DK, fg="white", anchor="w",
                 pady=15, padx=10).pack(fill="x")
        tk.Label(hero, text="     Downloads GST + IT  →  Builds all Reports  →  Done  [Ctrl+R]",
                 font=F_SMALL, bg=GREEN_DK, fg="#86EFAC",
                 anchor="w", pady=0, padx=10).pack(fill="x", pady=(0,10))
        hero.bind("<Button-1>", lambda e: self.run_full())
        for ch in hero.winfo_children():
            ch.bind("<Button-1>", lambda e: self.run_full())
        Tip(hero, "Runs the complete pipeline:\n\n"
                  "Step 1  — Read supplier names from Tally\n"
                  "Step 2-3 — Download GST returns from GST portal\n"
                  "Step 4-5 — Download documents from IT portal\n"
                  "Step 6  — Build all reconciliation reports\n"
                  "Step 7  — Final consolidated Excel\n\n"
                  "This is the button most users should click every month.")

        # Secondary actions
        subs = [
            ("📂  Reports Only  (no internet needed)",
             "Use this when files are already downloaded.\nBuilds all Excel reconciliation reports\nwithout opening any browser or portal.",
             "#0F2744", self.run_offline),
            ("🟢  GST Downloads Only",
             "Opens the GST portal in a browser and\ndownloads your selected GST returns.\nYou may need to enter a CAPTCHA once.",
             "#0F2F1A", self.run_gst_only),
            ("🔵  IT Downloads Only",
             "Opens the Income Tax portal and downloads\nyour selected IT documents.\nYou will need to enter OTP per client.",
             "#0C1A3D", self.run_it_only),
            ("📥  GSTR-2B Only  (~5 min, no Tally needed)",
             "Quick shortcut — downloads ONLY the purchase\nreturn (GSTR-2B) from the GST portal.\nFastest option, about 5 minutes.",
             "#0F2F1A", self.run_2b_only),
        ]
        for label, tip, color, cmd in subs:
            f = tk.Frame(act_sec, bg=color, cursor="hand2")
            f.pack(fill="x", pady=1)
            lbl = tk.Label(f, text=f"  {label}", font=F_BTN,
                           bg=color, fg="white", anchor="w", pady=8, padx=6)
            lbl.pack(fill="x")
            for w in (f, lbl):
                w.bind("<Button-1>", lambda e, c=cmd: c())
            Tip(f, tip)

        # ── 7. QUICK TOOLS ──────────────────────────────────────────────────
        tools_sec = self._card(parent, "🛠  TOOLS")

        tools = [
            ("📦  Install required packages",
             "Run this ONCE before first use.\n"
             "Installs all Python packages needed by the suite.\n\n"
             "CORE: pandas, openpyxl, numpy, xlrd,\n"
             "pdfplumber, pypdf, PyPDF2, selenium,\n"
             "webdriver-manager, flask, requests,\n"
             "werkzeug, urllib3, packaging + more\n\n"
             "TALLY (optional, asked during install):\n"
             "pyodbc, pyautogui, pygetwindow, pywin32,\n"
             "Pillow, pytesseract\n\n"
             "Takes 5–10 minutes. Requires internet.",
             self.install_packages),
            ("🔄  Refresh GSTIN Names  (fetch from portal)",
             "Fetches verified party names from the official GST portal\n(services.gst.gov.in) for all clients.\n\nFree — no API key, no login needed.\nResults cached locally — next run is instant.\nRun this if party names are blank or incorrect in reports.",
             self.refresh_gstin_names),
            ("📋  Open client file  (clients.xlsx)",
             "Opens clients.xlsx so you can add or edit\nyour clients' GST and IT login details.",
             self.view_clients),
            ("📥  Open downloads / output folder",
             "Opens the folder where GSTR-2B and other\ndownloaded files are saved.\nUse this to find your downloaded files.",
             self.open_output_folder),
            ("📁  Open suite folder",
             "Opens the root folder where the suite\nand all files are stored.",
             self.open_folder),
            ("🗑  Clear log",
             "Clears the activity log on the right.",
             self.clear_log),
            ("💾  Save log to file",
             "Saves the current log output to a text file\nso you can share it for support.",
             self.save_log),
        ]
        for label, tip, cmd in tools:
            f = tk.Frame(tools_sec, bg=CARD2, cursor="hand2")
            f.pack(fill="x", pady=1)
            lbl = tk.Label(f, text=f"  {label}", font=F_BODY,
                           bg=CARD2, fg=WHITE, anchor="w", pady=7, padx=6)
            lbl.pack(fill="x")
            for w in (f, lbl):
                w.bind("<Button-1>", lambda e, c=cmd: c())
            Tip(f, tip)

        # ── 8. ADVANCED ─────────────────────────────────────────────────────
        self._build_advanced(parent, scroll_fn)

    # ═══════════════════════════════════════════════════════════════════════════
    # SETUP CHECKLIST  (first-time guidance, always visible)
    # ═══════════════════════════════════════════════════════════════════════════
    def _build_setup_checklist(self, parent):
        outer = tk.Frame(parent, bg=CARD2)
        outer.pack(fill="x", padx=6, pady=(8,4))

        tk.Label(outer, text="  ✅  QUICK START CHECKLIST",
                 font=("Segoe UI", 9, "bold"),
                 bg=CARD2, fg=TEAL_LT).pack(anchor="w", pady=(8,4))
        tk.Frame(outer, bg=BORDER, height=1).pack(fill="x", padx=10)

        steps = [
            ("1", "Install required packages",
             "Click  📦 Install required packages  (only once)",
             "install"),
            ("2", "Fill in client details",
             "Click  📋 Open client file  → add client logins",
             "clients"),
            ("3", "Open Tally (optional)",
             "Open Tally with your company loaded, OR tick 'Skip Tally'",
             "tally"),
            ("4", "Click  🔄 RUN EVERYTHING",
             "That's it! Browser opens automatically.",
             "run"),
        ]

        inner = tk.Frame(outer, bg=CARD2)
        inner.pack(fill="x", padx=10, pady=8)

        for num, heading, detail, _ in steps:
            row = tk.Frame(inner, bg=CARD2)
            row.pack(fill="x", pady=3)

            # Number badge
            badge = tk.Label(row, text=num, font=F_BADGE,
                             bg=BLUE_DK, fg="white",
                             width=2, pady=2)
            badge.pack(side="left", padx=(0,8))

            info = tk.Frame(row, bg=CARD2)
            info.pack(side="left", fill="x", expand=True)
            tk.Label(info, text=heading, font=("Segoe UI", 9, "bold"),
                     bg=CARD2, fg=WHITE, anchor="w").pack(anchor="w")
            tk.Label(info, text=detail, font=F_SMALL,
                     bg=CARD2, fg=DIM2, anchor="w").pack(anchor="w")

    # ═══════════════════════════════════════════════════════════════════════════
    # TALLY STATUS BAR
    # ═══════════════════════════════════════════════════════════════════════════
    def _build_tally_bar(self, parent):
        outer = tk.Frame(parent, bg=CARD)
        outer.pack(fill="x", padx=6, pady=2)

        tk.Label(outer, text="  🧾  TALLY STATUS",
                 font=("Segoe UI", 9, "bold"),
                 bg=CARD, fg=DIM2).pack(anchor="w", pady=(8,4))
        tk.Frame(outer, bg=BORDER, height=1).pack(fill="x", padx=10)

        row = tk.Frame(outer, bg=CARD)
        row.pack(fill="x", padx=10, pady=8)

        self._tally_cv = tk.Canvas(row, width=14, height=14,
                                   bg=CARD, highlightthickness=0)
        self._tally_cv.pack(side="left", padx=(0,8))
        self._tally_dot = self._tally_cv.create_oval(2,2,12,12,
                                                      fill="#334155", outline="")

        self._tally_lbl = tk.Label(row, text="Checking…",
                                   font=F_BODY, bg=CARD, fg=DIM2)
        self._tally_lbl.pack(side="left")

        open_btn = tk.Label(row, text="▶ Open Tally",
                            font=F_SMALL, bg=CARD2, fg=BLUE,
                            padx=8, pady=4, cursor="hand2")
        open_btn.pack(side="right", padx=4)
        open_btn.bind("<Button-1>", lambda e: self._open_tally())
        Tip(open_btn, "Try to find and launch Tally automatically.\nTally must be installed on this computer.")

    # ═══════════════════════════════════════════════════════════════════════════
    # TILE BUTTON HELPER
    # ═══════════════════════════════════════════════════════════════════════════
    def _tile(self, parent, label, selected, tip, cmd):
        color = BLUE_DK if selected else CARD2
        fg    = WHITE
        f = tk.Frame(parent, bg=color, cursor="hand2")
        f.pack(fill="x", pady=1)
        lbl = tk.Label(f, text=f"  {label}", font=F_BODY,
                       bg=color, fg=fg, anchor="w", pady=7, padx=6)
        lbl.pack(fill="x")
        for w in (f, lbl):
            w.bind("<Button-1>", lambda e, c=cmd: c())
        Tip(f, tip)
        return (f, lbl)

    def _on_gst_combo(self, event=None):
        label = self._gst_display_var.get()
        val   = self._gst_val_map.get(label, "recommended")
        self._set_gst_mode(val)

    def _set_gst_mode(self, val):
        self.gst_mode_var.set(val)
        if hasattr(self, "_gst_display_var"):
            label = next((lbl for v, lbl, _ in self._gst_options if v == val), None)
            if label:
                self._gst_display_var.set(label)
        if hasattr(self, "_gst_tip_label"):
            self._gst_tip_label.config(text=self._gst_tip_map.get(val, ""))
        self._refresh_cmd()

    def _on_it_combo(self, event=None):
        label = self._it_display_var.get()
        val   = self._it_val_map.get(label, "all")
        self._set_it_mode(val)

    def _set_it_mode(self, val):
        self.it_mode_var.set(val)
        if hasattr(self, "_it_display_var"):
            label = next((lbl for v, lbl, _ in self._it_options if v == val), None)
            if label:
                self._it_display_var.set(label)
        if hasattr(self, "_it_tip_label"):
            self._it_tip_label.config(text=self._it_tip_map.get(val, ""))
        self._refresh_cmd()

    # ═══════════════════════════════════════════════════════════════════════════
    # CARD helper
    # ═══════════════════════════════════════════════════════════════════════════
    def _card(self, parent, title):
        outer = tk.Frame(parent, bg=CARD)
        outer.pack(fill="x", padx=6, pady=4)
        tk.Label(outer, text=f"  {title}",
                 font=("Segoe UI", 9, "bold"),
                 bg=CARD, fg=DIM2).pack(anchor="w", pady=(8,4))
        tk.Frame(outer, bg=BORDER, height=1).pack(fill="x", padx=10)
        inner = tk.Frame(outer, bg=CARD, pady=6)
        inner.pack(fill="x", padx=10)
        return inner

    # ═══════════════════════════════════════════════════════════════════════════
    # ADVANCED PANEL
    # ═══════════════════════════════════════════════════════════════════════════
    def _build_advanced(self, parent, scroll_fn):
        outer = tk.Frame(parent, bg=CARD3)
        outer.pack(fill="x", padx=6, pady=4)

        # Header (toggle)
        hdr = tk.Frame(outer, bg=CARD3, cursor="hand2")
        hdr.pack(fill="x")
        tk.Label(hdr, text="  ⚙  ADVANCED  ·  RPR LAUNCHER",
                 font=("Segoe UI", 9, "bold"),
                 bg=CARD3, fg=PURPLE).pack(side="left", pady=8)
        self._adv_chevron_ref = tk.Label(hdr, text="▼  (click to expand)",
                                         font=F_SMALL, bg=CARD3, fg=DIM)
        self._adv_chevron_ref.pack(side="right", padx=12)
        tk.Frame(outer, bg=BORDER, height=1).pack(fill="x")

        # Body (hidden by default)
        body = tk.Frame(outer, bg=CARD3)
        self._adv_body_ref = body

        # ·· What these options do — plain English note ·····················
        note = tk.Frame(body, bg="#1A1F2E")
        note.pack(fill="x", padx=10, pady=(10,4))
        tk.Label(note, text="  ℹ  These options are for experienced users.\n"
                            "     Most users can ignore this section entirely.",
                 font=F_SMALL, bg="#1A1F2E", fg=DIM2,
                 justify="left", pady=6).pack(anchor="w")

        # ·· Run Specific Steps Only ·········································
        tk.Label(body, text="  RUN A SPECIFIC STEP ONLY",
                 font=("Segoe UI", 8, "bold"),
                 bg=CARD3, fg=DIM2).pack(anchor="w", padx=10, pady=(8,2))
        tk.Label(body, text="  Click a step to run it alone.  Leave all unselected to run normally.",
                 font=F_SMALL, bg=CARD3, fg=DIM).pack(anchor="w", padx=10)

        step_grid = tk.Frame(body, bg=CARD3)
        step_grid.pack(fill="x", padx=10, pady=6)
        step_grid.columnconfigure(0, weight=1)
        step_grid.columnconfigure(1, weight=1)

        self._step_cells = {}
        for idx, (num, name, flag) in enumerate(self._STEPS):
            r, c = divmod(idx, 2)
            cell = tk.Frame(step_grid, bg=CARD2, cursor="hand2")
            cell.grid(row=r, column=c, padx=2, pady=2, sticky="ew")
            nl = tk.Label(cell, text=num,  font=F_BADGE,
                          bg=CARD2, fg=DIM, padx=6, pady=5)
            tl = tk.Label(cell, text=name, font=F_SMALL,
                          bg=CARD2, fg=WHITE, pady=5)
            nl.pack(side="left")
            tl.pack(side="left")
            self._step_cells[flag] = (cell, nl, tl)

            def _tog(f=flag, ce=cell, n=nl, t=tl):
                v = not self.step_vars[f].get()
                self.step_vars[f].set(v)
                bg = "#1D3461" if v else CARD2
                fg = BLUE if v else DIM
                ce.config(bg=bg); n.config(bg=bg, fg=fg); t.config(bg=bg)
                self._refresh_cmd()
            for w in (cell, nl, tl):
                w.bind("<Button-1>", lambda e, fn=_tog: fn())

        clear_lbl = tk.Label(body, text="✕ Clear step selection",
                             font=F_SMALL, bg=CARD3, fg=DIM, cursor="hand2")
        clear_lbl.pack(anchor="w", padx=12)
        clear_lbl.bind("<Button-1>", lambda e: self._clear_steps())

        tk.Frame(body, bg=BORDER, height=1).pack(fill="x", padx=10, pady=8)

        # ·· Extra Flags ·····················································
        tk.Label(body, text="  EXTRA OPTIONS",
                 font=("Segoe UI", 8, "bold"),
                 bg=CARD3, fg=DIM2).pack(anchor="w", padx=10)

        flags = [
            ("Test run  (preview only — nothing actually runs)", self.dry_run_var,
             "Prints what would happen step by step\nbut does NOT actually run anything.\nSafe to use at any time."),
            ("Extra detail in log  (for troubleshooting)",      self.debug_var,
             "Shows much more detail in the log panel.\nUseful when something goes wrong."),
            ("Skip GST download steps",                         self.skip_gst_var,
             "Skips the GST portal steps.\nUseful if GST is already downloaded."),
            ("Skip IT download steps",                          self.skip_it_var,
             "Skips the Income Tax portal steps.\nUseful if IT documents are already downloaded."),
            ("Skip report building steps",                      self.skip_bridge_var,
             "Skips reconciliation and report steps.\nOnly runs the download steps."),
        ]

        flags_f = tk.Frame(body, bg=CARD3)
        flags_f.pack(fill="x", padx=10, pady=6)
        for label, var, tip in flags:
            cb = tk.Checkbutton(flags_f, text=f"  {label}", variable=var,
                                font=F_SMALL, bg=CARD3, fg=WHITE,
                                activebackground=CARD3, activeforeground=WHITE,
                                selectcolor=CARD2, command=self._refresh_cmd)
            cb.pack(anchor="w", pady=1)
            Tip(cb, tip)

        tk.Frame(body, bg=BORDER, height=1).pack(fill="x", padx=10, pady=6)

        # ·· Command Preview ·················································
        tk.Label(body, text="  WHAT WILL RUN  (command preview)",
                 font=("Segoe UI", 8, "bold"),
                 bg=CARD3, fg=DIM2).pack(anchor="w", padx=10)

        cmd_f = tk.Frame(body, bg="#0D1117")
        cmd_f.pack(fill="x", padx=10, pady=4)
        tk.Label(cmd_f, textvariable=self._cmd_preview_var,
                 font=F_MONO, bg="#0D1117", fg=GREEN,
                 anchor="w", padx=10, pady=8,
                 wraplength=300, justify="left").pack(fill="x")

        btn_row = tk.Frame(body, bg=CARD3)
        btn_row.pack(fill="x", padx=10, pady=(4,12))

        cp = tk.Label(btn_row, text="📋 Copy command",
                      font=F_SMALL, bg=CARD2, fg=BLUE,
                      padx=8, pady=5, cursor="hand2")
        cp.pack(side="left", padx=(0,4))
        cp.bind("<Button-1>", lambda e: self._copy_cmd())
        Tip(cp, "Copy the command to clipboard to run in a terminal.")

        run_adv = tk.Label(btn_row, text="▶ Run with these settings",
                           font=("Segoe UI", 8, "bold"),
                           bg=GREEN_DK, fg="white",
                           padx=8, pady=5, cursor="hand2")
        run_adv.pack(side="left", padx=4)
        run_adv.bind("<Button-1>", lambda e: self._run_advanced_cmd())
        Tip(run_adv, "Launch the pipeline using exactly the settings above.")

        rst = tk.Label(btn_row, text="↺ Reset",
                       font=F_SMALL, bg=CARD2, fg=DIM,
                       padx=8, pady=5, cursor="hand2")
        rst.pack(side="right")
        rst.bind("<Button-1>", lambda e: self._reset_advanced())
        Tip(rst, "Reset all Advanced settings to defaults.")

        # ── Toggle binding ────────────────────────────────────────────────
        def _toggle(e=None):
            if body.winfo_ismapped():
                body.pack_forget()
                self._adv_chevron_ref.config(text="▼  (click to expand)")
            else:
                body.pack(fill="x")
                self._adv_chevron_ref.config(text="▲  (click to collapse)")
                self._bind_scroll(body, scroll_fn)
        for w in (hdr, self._adv_chevron_ref):
            w.bind("<Button-1>", _toggle)

    # ═══════════════════════════════════════════════════════════════════════════
    # RIGHT PANEL — Log
    # ═══════════════════════════════════════════════════════════════════════════
    def _build_right(self, parent):
        # Progress track
        prog_outer = tk.Frame(parent, bg=CARD)
        prog_outer.pack(fill="x")
        tk.Label(prog_outer, text="  📊  Progress",
                 font=("Segoe UI", 9, "bold"),
                 bg=CARD, fg=DIM2).pack(side="left", padx=8, pady=8)
        self.prog_label = tk.Label(prog_outer, text="",
                                   font=F_BODY, bg=CARD, fg=GREEN)
        self.prog_label.pack(side="right", padx=12, pady=8)

        # Step indicator row
        step_bar = tk.Frame(parent, bg=PANEL)
        step_bar.pack(fill="x")
        self._step_indicators = {}
        step_labels = [
            ("Tally",   "Step 1"),
            ("GST",     "Step 2-3"),
            ("IT",      "Step 4-5"),
            ("Bridge",  "Step 6"),
            ("Reports", "Step 7"),
        ]
        for name, sub in step_labels:
            sf = tk.Frame(step_bar, bg=PANEL)
            sf.pack(side="left", expand=True, fill="x", padx=1)
            tk.Label(sf, text=name, font=F_SMALL,
                     bg=PANEL, fg=DIM, pady=6).pack()
            ind = tk.Frame(sf, bg="#1E293B", height=3)
            ind.pack(fill="x", padx=2)
            self._step_indicators[name] = ind

        tk.Frame(parent, bg=BORDER, height=1).pack(fill="x")

        # Log header
        log_hdr = tk.Frame(parent, bg=CARD2)
        log_hdr.pack(fill="x")
        tk.Label(log_hdr, text="  📟  Activity Log",
                 font=("Segoe UI", 10, "bold"),
                 bg=CARD2, fg=WHITE).pack(side="left", padx=8, pady=8)
        tk.Label(log_hdr,
                 text="Everything that happens appears here in real time",
                 font=F_SMALL, bg=CARD2, fg=DIM).pack(side="left")

        # Log area
        self.log = scrolledtext.ScrolledText(
            parent, font=("Consolas", 9),
            bg="#030810", fg="#8FAACC",
            insertbackground=WHITE,
            relief="flat", state="disabled",
            wrap="word", padx=16, pady=14,
            spacing1=1, spacing2=3,
        )
        self.log.pack(fill="both", expand=True)

        self.log.tag_config("ok",     foreground="#00E676", font=("Consolas", 9, "bold"))
        self.log.tag_config("warn",   foreground="#FFB300", font=("Consolas", 9))
        self.log.tag_config("err",    foreground="#FF4444", font=("Consolas", 9, "bold"),
                             background="#1A0000")
        self.log.tag_config("info",   foreground="#00D4FF")
        self.log.tag_config("head",   foreground="#A78BFA",
                             font=("Consolas", 9, "bold"))
        self.log.tag_config("prompt", foreground="#FFE082",
                             background="#1A1200",
                             font=("Consolas", 9, "bold"))
        self.log.tag_config("input",  foreground="#B9F5D8")

        # Interactive input bar — hidden until a prompt line is detected
        self.input_bar = InteractiveInputBar(parent, self.log)

        self._log_welcome()

    # ═══════════════════════════════════════════════════════════════════════════
    # TALLY POLLING
    # ═══════════════════════════════════════════════════════════════════════════
    def _start_tally_poll(self):
        def _poll():
            up = _is_tally_running()
            self.after(0, lambda: self._update_tally(up))
            self.after(2500, _spawn)
        def _spawn():
            threading.Thread(target=_poll, daemon=True).start()
        _spawn()

    def _update_tally(self, running):
        if not self._tally_cv.winfo_exists():
            return
        if running:
            self._tally_cv.itemconfig(self._tally_dot, fill=GREEN)
            self._tally_lbl.config(text="Tally is open and ready  ✓", fg=GREEN)
            if self._tally_status != "running":
                self.skip_tally_var.set(False)
                self._refresh_cmd()
            self._tally_status = "running"
        else:
            self._tally_cv.itemconfig(self._tally_dot, fill=AMBER)
            self._tally_lbl.config(text="Tally is not open", fg=AMBER)
            self._tally_status = "offline"

    def _open_tally(self):
        paths = [
            r"C:\Tally\tally.exe", r"C:\TallyPrime\tally.exe",
            r"C:\Program Files\Tally\tally.exe",
            r"C:\Program Files (x86)\Tally\tally.exe",
            r"C:\Tally.ERP9\tally.exe",
            r"D:\Tally\tally.exe", r"D:\TallyPrime\tally.exe",
        ]
        for p in paths:
            if Path(p).exists():
                try:
                    subprocess.Popen([p], cwd=str(Path(p).parent))
                    self._log(f"  ▶  Opening Tally from: {p}", "info")
                    self._log("     Please load your company, then run the suite.", "info")
                    return
                except Exception as ex:
                    self._log(f"  ⚠  Could not launch Tally: {ex}", "warn")
        self._log("  ⚠  Tally not found in common locations — please open it manually.", "warn")
        messagebox.showinfo("Open Tally Manually",
            "Tally was not found automatically.\n\n"
            "Please open Tally manually from your desktop,\n"
            "load your company, then come back and click\n"
            "🔄 RUN EVERYTHING.")

    # ═══════════════════════════════════════════════════════════════════════════
    # COMMAND BUILDING
    # ═══════════════════════════════════════════════════════════════════════════
    _GST_MODE_MAP = {
        # ── Fast Direct Downloads ──
        "2b_only":       "4",    # ⚡ GSTR-2B only (direct download)
        "3b_only":       "6",    # ⚡ GSTR-3B only (direct download)
        "2b_3b":         "15",   # ⚡ GSTR-2B + GSTR-3B (both direct)
        # ── Recommended / Full ──
        "recommended":   "17",   # ⚡ Recommended: 1+2B+3B+Tax (optimized)
        "full_optimized":"16",   # 🚀 Full Optimised: all 5 in best order
        "full":          "1",    # 📦 Full Suite: 1+2B+2A+3B+Tax (phased)
        # ── Individual Returns ──
        "sales":         "2",    # GSTR-1 only (generate first)
        "purchase":      "5",    # GSTR-2A only (generate first)
        # ── Combos ──
        "1_3b":          "7",    # GSTR-1 + GSTR-3B
        "2b_2a":         "8",    # GSTR-2B + GSTR-2A
        "1_2b_2a":       "9",    # GSTR-1 + GSTR-2B + GSTR-2A (no 3B)
        "1_2b_3b":       "13",   # GSTR-1 + GSTR-2B + GSTR-3B (no 2A)
        # ── Multi-FY ──
        "multi_fy":      "17",   # Multi-FY: 2025-26 + 2024-25 (1+1A+2B+3B+Tax)
        # ── Utilities ──
        "offline":       "11",   # OFFLINE — reconciliation from local files
        "retry_failed":  "12",   # RETRY FAILED — re-download failed months
        "tax_only":      "10",   # Tax Liability & ITC Comparison only
        "3b_summary":    "18",   # GSTR-3B Summary Report (PDF → Excel)
        "custom":        "C",    # Custom — type return names manually
    }
    _IT_MODE_MAP = {
        "all":     "1",
        "26as":    "2",
        "ais_tis": "3",
        "recon":   "4",
    }

    def _gst_option(self):
        return self._GST_MODE_MAP.get(self.gst_mode_var.get(), "17")

    def _it_option(self):
        return self._IT_MODE_MAP.get(self.it_mode_var.get(), "1")

    def _refresh_cmd(self):
        parts = ["python run_all.py"]
        fy = self.fy_var.get().strip()
        if fy and fy != "2026-27":
            parts.append(f"--fy {fy}")
        cl = self.client_var.get().strip()
        if cl:
            parts.append(f'--client "{cl}"')
        if self.skip_tally_var.get():
            parts.append("--skip-tally")
        if self.dry_run_var.get():     parts.append("--dry-run")
        if self.debug_var.get():       parts.append("--debug")
        if self.skip_gst_var.get():    parts.append("--skip-gst")
        if self.skip_it_var.get():     parts.append("--skip-it")
        if self.skip_bridge_var.get(): parts.append("--skip-bridge")
        custom_out = self.output_path_var.get().strip()
        if custom_out:
            parts.append(f'--output-dir "{custom_out}"')
        for flag, var in self.step_vars.items():
            if var.get():
                parts.append(flag)
        self._cmd_preview_var.set(" ".join(parts))

    def _copy_cmd(self):
        self._refresh_cmd()
        self.clipboard_clear()
        self.clipboard_append(self._cmd_preview_var.get())
        self._log("  📋  Command copied to clipboard.", "info")

    def _clear_steps(self):
        for flag, var in self.step_vars.items():
            var.set(False)
            cell, nl, tl = self._step_cells[flag]
            cell.config(bg=CARD2); nl.config(bg=CARD2, fg=DIM); tl.config(bg=CARD2)
        self._refresh_cmd()

    def _reset_advanced(self):
        self._clear_steps()
        for v in (self.dry_run_var, self.debug_var, self.skip_gst_var,
                  self.skip_it_var, self.skip_bridge_var):
            v.set(False)
        self._refresh_cmd()
        self._log("  ↺  Advanced settings reset.", "info")

    def _build_args(self, extra=None):
        args = [str(SCRIPT_DIR / "run_all.py")]
        fy = self.fy_var.get().strip()
        if fy and fy != "2026-27":
            args += ["--fy", fy]
        cl = self.client_var.get().strip()
        if cl:
            args += ["--client", cl]
        if self.skip_tally_var.get():
            args += ["--skip-tally"]
        custom_out = self.output_path_var.get().strip()
        if custom_out:
            args += ["--output-dir", custom_out]
        if extra:
            args += extra
        return args

    def _inject_env(self):
        os.environ["GST_MENU_CHOICE"] = self._gst_option()
        os.environ["GST_MENU_FY"]     = self.gst_fy_var.get().strip() or "2026-27"
        os.environ["IT_MENU_CHOICE"]  = self._it_option()
        os.environ["IT_WORKERS"]      = "1"
        custom_out = self.output_path_var.get().strip()
        if custom_out:
            os.environ["RPR_OUTPUT_DIR"] = custom_out
        else:
            os.environ.pop("RPR_OUTPUT_DIR", None)

    def _run_advanced_cmd(self):
        if not self._guard(): return
        self._inject_env()
        args = self._build_args()
        if self.dry_run_var.get():     args.append("--dry-run")
        if self.debug_var.get():       args.append("--debug")
        if self.skip_gst_var.get():    args.append("--skip-gst")
        if self.skip_it_var.get():     args.append("--skip-it")
        if self.skip_bridge_var.get(): args.append("--skip-bridge")
        for flag, var in self.step_vars.items():
            if var.get(): args.append(flag)
        self._start_run(args, "Custom (Advanced)")

    # ═══════════════════════════════════════════════════════════════════════════
    # LOG HELPERS
    # ═══════════════════════════════════════════════════════════════════════════
    def _log(self, msg, tag=None):
        self.log.config(state="normal")
        if tag:
            self.log.insert(tk.END, msg + "\n", tag)
        else:
            self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)
        self.log.config(state="disabled")

    def _log_welcome(self):
        ts  = datetime.now().strftime("%d %b %Y  %H:%M")
        pyv = sys.version.split()[0]
        self._log("=" * 66, "head")
        self._log(f"  ⚡ RPR GST + IT Suite  v3.5 ADVANCED PRO  —  {ts}", "head")
        self._log("=" * 66, "head")
        self._log("")
        self._log(f"  📁  Folder : {EXE_DIR}", "info")
        self._log(f"  🐍  Python : {pyv}  |  Platform: {sys.platform}", "info")
        self._log("")
        self._log("  👋  Welcome!  First time here?", "head")
        self._log("  Follow the Quick Start Checklist on the left.", "info")
        self._log("")
        self._log("  Keyboard shortcuts:  Ctrl+R = Run Everything", "info")
        self._log("                       Ctrl+L = Clear Log", "info")
        self._log("                       Ctrl+S = Save Log", "info")
        self._log("")
        self._log("  ─── All activity appears here in real time ───", "head")
        self._log("")

    def clear_log(self):
        self.log.config(state="normal")
        self.log.delete("1.0", tk.END)
        self.log.config(state="disabled")
        self._log_welcome()

    def save_log(self):
        p = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files","*.txt"),("All files","*.*")],
            initialfile=f"RPR_Log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
        )
        if p:
            Path(p).write_text(self.log.get("1.0", tk.END), encoding="utf-8")
            self._log(f"  💾  Log saved: {p}", "ok")

    # ═══════════════════════════════════════════════════════════════════════════
    # HEALTH CHECK
    # ═══════════════════════════════════════════════════════════════════════════
    def _check_health(self):
        missing = check_suite_files()
        clients_ok = any((EXE_DIR / f).exists() for f in [
            "clients.xlsx",
            "Client_Manager_Secure_AY2027-28.xlsx",
            "Client_Manager_Secure_AY2026-27.xlsx",
            "Client_Manager_Secure_AY2025-26.xlsx",
            "Client_Manager_Secure_AY2024-25.xlsx",
        ])
        count = 0
        if clients_ok:
            try:
                import openpyxl as ox
                wb = ox.load_workbook(str(EXE_DIR / "clients.xlsx"), read_only=True)
                count = max(0, sum(1 for _ in wb.active.iter_rows()) - 1)
                wb.close()
            except Exception:
                pass

        # Check GSTIN name cache status
        cache_note = ""
        cache_json = EXE_DIR / "gstin_name_cache.json"
        if cache_json.exists():
            try:
                import json as _j
                d = _j.loads(cache_json.read_text(encoding="utf-8"))
                portal_ct  = sum(1 for v in d.values() if v.get("source") == "portal")
                master_ct  = sum(1 for v in d.values() if v.get("source") == "customer_master")
                total_ct   = len(d)
                tally_ct = sum(1 for v in d.values() if v.get("source") in ("tally_csv", "gstr1_zip"))
                unverified = total_ct - portal_ct
                if portal_ct == 0 and total_ct > 0:
                    cache_note = f"  ⚠ GSTIN names: {total_ct} cached — click 🔄 Refresh to fetch from portal"
                elif unverified > 0:
                    cache_note = (f"  ℹ GSTIN names: {portal_ct} portal-verified"
                                  f" + {unverified} unverified — click 🔄 Refresh")
                else:
                    cache_note = f"  ✅ GSTIN names: {total_ct} portal-verified (all good)"
            except Exception:
                pass

        if missing:
            self.health_label.config(
                text=f"⚠  Missing files: {', '.join(missing)}", fg=RED)
            self.client_count_label.config(text="Place this file in the suite folder", fg=AMBER)
        elif not clients_ok:
            self.health_label.config(text="⚠  clients.xlsx not found", fg=AMBER)
            self.client_count_label.config(
                text="Click  📋 Open client file  to set up clients", fg=DIM2)
        else:
            self.health_label.config(text="✅  Suite is ready", fg=GREEN)
            detail = f"{count} client(s) loaded  |  FY 2026-27"  # FIX v12
            if cache_note:
                detail += f"\n{cache_note}"
            self.client_count_label.config(text=detail, fg=DIM2)

    # ═══════════════════════════════════════════════════════════════════════════
    # GUARD
    # ═══════════════════════════════════════════════════════════════════════════
    def _guard(self):
        if self.running:
            messagebox.showwarning("Already Running",
                "Something is already running.\n\nPlease wait for it to finish before starting another action.")
            return False
        missing = check_suite_files()
        if missing:
            messagebox.showerror("Files Missing",
                f"These required files are missing:\n\n"
                + "\n".join(f"  • {f}" for f in missing)
                + "\n\nPlease make sure RPR_Launcher.exe is in the\nsame folder as run_all.py and all suite files.")
            return False
        return True

    def _start_run(self, args, label, stdin_answers=None, after_hook=None):
        self.running = True
        self._run_count += 1
        self._run_count_var.set(f"{self._run_count} run(s) this session")
        ts = datetime.now().strftime("%H:%M:%S")
        self.status_var.set(f"⏳  {label}…")
        self.prog_label.config(text=f"⚙  {label}", fg=AMBER)
        self._log("")
        self._log(f"▶  [{ts}]  {label}", "head")
        self._log("─" * 66, "head")
        self._log("")

        def on_done():
            self.running = False
            ts_end = datetime.now().strftime("%H:%M:%S")
            self.prog_label.config(text=f"✅ Finished at {ts_end}", fg=GREEN)
            self._log("")
            self._log(f"✅  [{ts_end}]  {label}  —  Done", "ok")
            self._log("─" * 66, "head")
            self._check_health()
            if after_hook:
                after_hook()
        run_script(args, self.log, self.status_var, on_done,
                   stdin_answers, input_bar=self.input_bar)

    # ═══════════════════════════════════════════════════════════════════════════
    # RUN ACTIONS
    # ═══════════════════════════════════════════════════════════════════════════
    def run_full(self):
        if not self._guard(): return
        gst_name = {
            "2b_only":       "GSTR-2B Only (Direct Download)",
            "3b_only":       "GSTR-3B Only (Direct Download)",
            "2b_3b":         "GSTR-2B + GSTR-3B (Fast Combo)",
            "recommended":   "Recommended (GST-1, 2B, 3B, Tax Report)",
            "full_optimized":"Full Optimised (all 5 returns, best order)",
            "full":          "Full Suite (all returns including 2A)",
            "sales":         "GSTR-1 Only (Sales Return)",
            "purchase":      "GSTR-2A Only (ITC Return)",
            "1_3b":          "GSTR-1 + GSTR-3B (Sales + Tax)",
            "2b_2a":         "GSTR-2B + GSTR-2A (Both Purchase Returns)",
            "1_2b_2a":       "GSTR-1 + GSTR-2B + GSTR-2A (no Tax)",
            "1_2b_3b":       "GSTR-1 + GSTR-2B + GSTR-3B (no 2A)",
            "multi_fy":      "Multi-FY Optimised (2026-27 + 2025-26)",
            "offline":       "Offline Reconciliation (no browser)",
            "retry_failed":  "Retry Failed Downloads",
            "tax_only":      "Tax Liability & ITC Comparison Only",
            "3b_summary":    "GSTR-3B Summary Report (PDF → Excel)",
            "custom":        "Custom Returns (manual selection)",
        }.get(self.gst_mode_var.get(), "Recommended")
        it_name = {
            "all":     "All IT Documents (26AS + AIS + TIS + Recon)",
            "26as":    "26AS Only",
            "ais_tis": "AIS + TIS Only",
            "recon":   "Recon Excel Only",
        }.get(self.it_mode_var.get(), "All IT Documents")

        skip_note = "" if self.skip_tally_var.get() \
                    else "• Step 1: Tally must be open (or tick Skip Tally)\n"

        if not messagebox.askyesno("Start Full Pipeline?",
            "This will run everything:\n\n"
            + skip_note +
            f"• GST Download: {gst_name}\n"
            "   Browser will open — enter CAPTCHA if asked\n"
            f"• IT Download: {it_name}\n"
            "   Enter OTP per client when asked\n"
            "• Build all reconciliation reports\n\n"
            "Ready to start?"):
            return
        self._inject_env()
        self._start_run(self._build_args(), "Full Pipeline  (Steps 1 → 7)")

    def run_offline(self):
        # Offline mode doesn't need gst_suite_v32.py — use offline guard
        missing = check_suite_files(offline=True)
        if self.running:
            messagebox.showwarning("Already Running",
                "Something is already running.\n\nPlease wait for it to finish before starting another action.")
            return
        if missing:
            messagebox.showerror("Files Missing",
                f"These required files are missing:\n\n"
                + "\n".join(f"  • {f}" for f in missing)
                + "\n\nPlease make sure RPR_Launcher.exe is in the\nsame folder as run_all.py and all suite files.")
            return
        dlg = tk.Toplevel(self)
        dlg.title("Reports Only — Which Steps?")
        dlg.configure(bg=CARD)
        dlg.resizable(True, True)
        dlg.grab_set()
        w, h = 560, 680
        dlg.geometry(f"{w}x{h}+{(dlg.winfo_screenwidth()-w)//2}+{(dlg.winfo_screenheight()-h)//2}")
        dlg.minsize(520, 500)

        tk.Label(dlg, text="  📂  Build Reports Without Downloading",
                 font=F_HEAD, bg=CARD, fg=WHITE).pack(anchor="w", padx=14, pady=(14,2))
        tk.Label(dlg,
                 text="  Choose what to build.  Files must already be downloaded.",
                 font=F_SMALL, bg=CARD, fg=DIM2).pack(anchor="w", padx=14, pady=(0,6))
        tk.Frame(dlg, bg=BORDER, height=1).pack(fill="x")

        # ── FOLDER PICKERS ────────────────────────────────────────────────────
        folder_frame = tk.Frame(dlg, bg=CARD2)
        folder_frame.pack(fill="x", padx=14, pady=(10, 4))

        tk.Label(folder_frame, text="  📁  GST Automation folder:",
                 font=F_BODY, bg=CARD2, fg=DIM2).pack(anchor="w", padx=6, pady=(8,2))
        gst_row = tk.Frame(folder_frame, bg=CARD2)
        gst_row.pack(fill="x", padx=6, pady=(0,4))

        # Auto-detect GST folder
        _auto_gst = ""
        try:
            for _ch in sorted(EXE_DIR.iterdir()):
                _gd = _ch / "GST Automation"
                if _gd.exists():
                    _auto_gst = str(_gd)
                    break
        except Exception:
            pass


        # ── Subfolder resolver ──────────────────────────────────────────────────
        def _resolve_subfolder(base_path, file_patterns, folder_hints):
            """Walk up to 4 levels into base_path to find the best matching subfolder."""
            bp = Path(base_path)

            def _has_files(d):
                return any(any(d.glob(pat)) for pat in file_patterns)

            if _has_files(bp):
                return str(bp)

            hint_matches, file_matches = [], []
            try:
                for root, dirs, _ in os.walk(str(bp)):
                    depth = len(Path(root).relative_to(bp).parts)
                    if depth > 4:
                        dirs.clear()
                        continue
                    rp = Path(root)
                    if rp == bp:
                        continue
                    if _has_files(rp):
                        if any(h.lower() in rp.name.lower() for h in folder_hints):
                            hint_matches.append(rp)
                        else:
                            file_matches.append(rp)
            except Exception:
                pass

            if hint_matches:
                return str(hint_matches[0])
            if file_matches:
                return str(file_matches[0])
            return str(bp)

        _GST_PATS  = ["*.xlsx", "GSTR*.pdf", "GSTR*.json"]
        _GST_HINTS = ["GST Automation", "GST_Automation", "Raw Data"]
        _IT_PATS   = ["*.xlsx", "26AS*.pdf", "AIS*.pdf", "TIS*.pdf"]
        _IT_HINTS  = ["IT Download", "IT_Download", "Raw Data"]

        gst_var = tk.StringVar(value=_auto_gst)
        gst_entry = tk.Entry(gst_row, textvariable=gst_var, font=F_SMALL,
                             bg=CARD, fg=WHITE, insertbackground=WHITE,
                             relief="flat", bd=3)
        gst_entry.pack(side="left", fill="x", expand=True, padx=(0,4))

        def _browse_gst():
            start = gst_var.get() or str(EXE_DIR)
            folder = filedialog.askdirectory(
                title="Select GST Automation folder (or any parent — subfolders scanned automatically)",
                initialdir=start)
            if folder:
                resolved = _resolve_subfolder(folder, _GST_PATS, _GST_HINTS)
                gst_var.set(resolved)

        gst_browse_btn = tk.Label(gst_row, text="Browse", font=F_SMALL,
                                  bg=BLUE_DK, fg="white", padx=8, pady=4, cursor="hand2")
        gst_browse_btn.pack(side="left")
        gst_browse_btn.bind("<Button-1>", lambda e: _browse_gst())

        tk.Label(folder_frame, text="  📁  IT Download folder:",
                 font=F_BODY, bg=CARD2, fg=DIM2).pack(anchor="w", padx=6, pady=(6,2))
        it_row = tk.Frame(folder_frame, bg=CARD2)
        it_row.pack(fill="x", padx=6, pady=(0,8))

        # Auto-detect IT folder
        _auto_it = ""
        try:
            for _ch in sorted(EXE_DIR.iterdir()):
                _id = _ch / "IT Download"
                if _id.exists():
                    _auto_it = str(_id)
                    break
        except Exception:
            pass

        it_var = tk.StringVar(value=_auto_it)
        it_entry = tk.Entry(it_row, textvariable=it_var, font=F_SMALL,
                            bg=CARD, fg=WHITE, insertbackground=WHITE,
                            relief="flat", bd=3)
        it_entry.pack(side="left", fill="x", expand=True, padx=(0,4))

        def _browse_it():
            start = it_var.get() or str(EXE_DIR)
            folder = filedialog.askdirectory(
                title="Select IT Download folder (or any parent — subfolders scanned automatically)",
                initialdir=start)
            if folder:
                resolved = _resolve_subfolder(folder, _IT_PATS, _IT_HINTS)
                it_var.set(resolved)

        it_browse_btn = tk.Label(it_row, text="Browse", font=F_SMALL,
                                 bg=BLUE_DK, fg="white", padx=8, pady=4, cursor="hand2")
        it_browse_btn.pack(side="left")
        it_browse_btn.bind("<Button-1>", lambda e: _browse_it())

        tk.Label(folder_frame,
                 text="  Tip: Browse any parent folder — subfolders are scanned automatically.\n"
                      "  Direct paths: ClientName\\GST Automation   and   ClientName\\IT Download",
                 font=F_SMALL, bg=CARD2, fg=DIM, pady=2, justify="left", wraplength=500).pack(anchor="w", padx=6, pady=(0,6))

        tk.Frame(dlg, bg=BORDER, height=1).pack(fill="x")

        opts = [
            ("Most Common ✦",      "Steps 6 → 7 — Bridge + All Reports",    "10"),
            ("All Recon + Reports", "Steps 3 → 7 — IT Recon + Bridge + Reports","11"),
            ("Full Offline",        "Steps 2 → 7 — GST + IT + Bridge + Reports","12"),
            ("─────────────────", "Individual steps:", None),
            ("Bridge Only",         "Step 6 — Build reconciliation Excel",    "4"),
            ("GST vs IT Compare",   "Step 6b — GST vs IT comparison report",  "5"),
            ("GSTR-2B Summary",     "Step 6c — GSTR-2B consolidated extract", "6"),
            ("26AS vs Sales Match", "Step 6d — 26AS vs GSTR-1 comparison",    "7"),
            ("GST Tax Report",      "Step 6e — GST comparison report",        "8"),
            ("Final Report Only",   "Step 7 — Final consolidated Excel",      "9"),
        ]

        chosen = tk.StringVar(value="10")

        # ── Scrollable container for option rows ─────────────────────────────
        list_outer = tk.Frame(dlg, bg=CARD)
        list_outer.pack(fill="both", expand=True, padx=14, pady=8)

        list_cv  = tk.Canvas(list_outer, bg=CARD, highlightthickness=0)
        list_sb  = tk.Scrollbar(list_outer, orient="vertical", command=list_cv.yview)
        list_cv.configure(yscrollcommand=list_sb.set)
        list_sb.pack(side="right", fill="y")
        list_cv.pack(side="left", fill="both", expand=True)

        lb_frame = tk.Frame(list_cv, bg=CARD)
        _lwin = list_cv.create_window((0, 0), window=lb_frame, anchor="nw")
        lb_frame.bind("<Configure>",
                      lambda e: list_cv.configure(scrollregion=list_cv.bbox("all")))
        list_cv.bind("<Configure>",
                     lambda e: list_cv.itemconfig(_lwin, width=e.width))

        def _dlg_scroll(e):
            delta = getattr(e, "delta", 0)
            if delta != 0:
                list_cv.yview_scroll(int(-1 * (delta / 120)), "units")
            elif getattr(e, "num", 0) == 4:
                list_cv.yview_scroll(-1, "units")
            elif getattr(e, "num", 0) == 5:
                list_cv.yview_scroll(1, "units")

        for _w in (list_cv, lb_frame):
            _w.bind("<MouseWheel>", _dlg_scroll)
            _w.bind("<Button-4>",   _dlg_scroll)
            _w.bind("<Button-5>",   _dlg_scroll)

        for title, desc, val in opts:
            if val is None:
                tk.Label(lb_frame, text=f"  {title}", font=F_SMALL,
                         bg=CARD, fg=DIM, pady=4).pack(anchor="w")
                continue
            row = tk.Frame(lb_frame, bg=CARD2, cursor="hand2")
            row.pack(fill="x", pady=1)
            is_rec = "Most Common" in title
            bg_ = GREEN_DK if is_rec else CARD2
            row.config(bg=bg_)

            def _sel(v=val, r=row):
                chosen.set(v)
                for ch in lb_frame.winfo_children():
                    if isinstance(ch, tk.Frame):
                        nm = ch.cget("bg")
                        if nm not in (GREEN_DK, BLUE_DK):
                            ch.config(bg=CARD2)
                            for gc in ch.winfo_children():
                                try: gc.config(bg=CARD2)
                                except Exception: pass
                r.config(bg=BLUE_DK)
                for gc in r.winfo_children():
                    try: gc.config(bg=BLUE_DK)
                    except Exception: pass

            tl = tk.Label(row, text=f"  {title}", font=("Segoe UI", 9, "bold"),
                          bg=bg_, fg=WHITE, anchor="w", pady=5, padx=6)
            tl.pack(fill="x")
            dl = tk.Label(row, text=f"     {desc}", font=F_SMALL,
                          bg=bg_, fg=DIM2, anchor="w", pady=2, padx=6)
            dl.pack(fill="x")
            for w in (row, tl, dl):
                w.bind("<Button-1>",  lambda e, fn=_sel: fn())
                w.bind("<MouseWheel>", _dlg_scroll)
                w.bind("<Button-4>",   _dlg_scroll)
                w.bind("<Button-5>",   _dlg_scroll)

        def _ok():
            gst_path = gst_var.get().strip()
            it_path  = it_var.get().strip()
            if not gst_path:
                messagebox.showerror("Missing Folder",
                    "Please enter or browse the GST Automation folder path.", parent=dlg)
                return
            if not it_path:
                messagebox.showerror("Missing Folder",
                    "Please enter or browse the IT Download folder path.", parent=dlg)
                return
            if not Path(gst_path).exists():
                messagebox.showerror("Folder Not Found",
                    f"GST folder does not exist:\n{gst_path}", parent=dlg)
                return
            if not Path(it_path).exists():
                messagebox.showerror("Folder Not Found",
                    f"IT folder does not exist:\n{it_path}", parent=dlg)
                return
            dlg.destroy()
            v = chosen.get()
            # Offline mode — force env vars to recon-only values
            os.environ["GST_MENU_CHOICE"] = "11"
            os.environ["GST_MENU_FY"]     = self.gst_fy_var.get().strip() or "2026-27"
            os.environ["IT_MENU_CHOICE"]  = "4"
            os.environ["IT_WORKERS"]      = "1"
            custom_out = self.output_path_var.get().strip()
            if custom_out:
                os.environ["RPR_OUTPUT_DIR"] = custom_out
            else:
                os.environ.pop("RPR_OUTPUT_DIR", None)
            # Offline never needs Tally
            self.skip_tally_var.set(True)
            # Pass --gst-folder and --it-folder so run_all.py never calls input()
            self._start_run(
                self._build_args([
                    "--offline",
                    "--offline-choice", v,
                    "--gst-folder", gst_path,
                    "--it-folder",  it_path,
                ]),
                "Reports Only (offline)")

        def _cancel():
            dlg.destroy()

        btn_r = tk.Frame(dlg, bg=CARD)
        btn_r.pack(fill="x", pady=10, padx=14)
        tk.Button(btn_r, text="▶  Build Selected Reports",
                  font=F_BTN, bg=GREEN_DK, fg="white",
                  relief="flat", padx=14, pady=8, command=_ok).pack(side="left", padx=4)
        tk.Button(btn_r, text="Cancel",
                  font=F_BTN, bg=CARD2, fg=DIM2,
                  relief="flat", padx=10, pady=8, command=_cancel).pack(side="left", padx=4)

    def run_gst_only(self):
        if not self._guard(): return
        self._inject_env()
        # GST-only never needs Tally (Step 1); add --skip-tally automatically
        # unless the user has already ticked it (to avoid passing the flag twice)
        extra = ["--only-gst"]
        if not self.skip_tally_var.get():
            extra.append("--skip-tally")
        self._start_run(self._build_args(extra), "GST Downloads Only (Steps 2-3)")

    def run_it_only(self):
        if not self._guard(): return
        self._inject_env()
        # IT-only never needs Tally (Step 1); add --skip-tally automatically
        extra = ["--only-it"]
        if not self.skip_tally_var.get():
            extra.append("--skip-tally")
        self._start_run(self._build_args(extra), "IT Downloads Only (Steps 4-5)")

    def run_2b_only(self):
        if not self._guard(): return
        os.environ["GST_MENU_CHOICE"] = "4"
        os.environ["GST_MENU_FY"]     = self.gst_fy_var.get().strip() or "2026-27"
        os.environ["IT_MENU_CHOICE"]  = "1"
        os.environ["IT_WORKERS"]      = "1"
        self.skip_tally_var.set(True)

        def _after_2b():
            out = self._find_output_folder()
            self._log("", )
            self._log("─" * 62, "head")
            self._log(f"  📁  Download folder:  {out}", "info")
            self._log("  Click the button below OR the path above to open it.", "info")
            self._log("─" * 62, "head")
            # Show a popup with the folder path and an open button
            self._show_output_ready(out)

        self._start_run(self._build_args(["--only-gst"]),
                        "GSTR-2B Download Only (~5 min)",
                        after_hook=_after_2b)

    def _show_output_ready(self, folder):
        """Show a small popup telling the user where files are saved."""
        win = tk.Toplevel(self)
        win.title("Download Complete")
        win.configure(bg=CARD)
        win.resizable(False, False)
        win.grab_set()
        w, h = 480, 200
        win.geometry(f"{w}x{h}+{(win.winfo_screenwidth()-w)//2}+{(win.winfo_screenheight()-h)//2}")

        tk.Label(win, text="  ✅  GSTR-2B Download Complete",
                 font=F_HEAD, bg=CARD, fg=GREEN).pack(anchor="w", padx=16, pady=(16,4))
        tk.Label(win, text="  Files are saved here:",
                 font=F_BODY, bg=CARD, fg=DIM2).pack(anchor="w", padx=16)
        path_lbl = tk.Label(win, text=f"  {folder}",
                            font=("Consolas", 9), bg=CARD2, fg=WHITE,
                            wraplength=440, justify="left",
                            anchor="w", padx=10, pady=8)
        path_lbl.pack(fill="x", padx=16, pady=6)

        btn_row = tk.Frame(win, bg=CARD)
        btn_row.pack(fill="x", padx=16, pady=8)
        tk.Button(btn_row, text="📁  Open Folder", font=F_BTN,
                  bg=BLUE_DK, fg="white", relief="flat",
                  padx=14, pady=8,
                  command=lambda: (self._open_path_in_explorer(str(folder)), win.destroy())
                  ).pack(side="left", padx=(0,6))
        tk.Button(btn_row, text="OK", font=F_BTN,
                  bg=CARD2, fg=DIM2, relief="flat",
                  padx=14, pady=8,
                  command=win.destroy).pack(side="left")

    # ═══════════════════════════════════════════════════════════════════════════
    # REFRESH GSTIN NAMES  — fetch verified names from GST portal
    # ═══════════════════════════════════════════════════════════════════════════
    def refresh_gstin_names(self):
        """Fetch/update all GSTIN party names — scans GSTR-1 ZIPs then fetches from portal."""
        if self.running:
            messagebox.showwarning("Already Running",
                "Something is already running.\n\nPlease wait for it to finish.")
            return

        # Check gstin_name_cache.py is present
        cache_module = EXE_DIR / "gstin_name_cache.py"
        if not cache_module.exists():
            messagebox.showerror("File Missing",
                "gstin_name_cache.py not found in the suite folder.\n\n"
                "Please download the latest version of the suite.")
            return

        # Auto-detect GSTR-1 ZIP folders so we can scan new supplier/receiver GSTINs
        _gstr1_zip_folders = []
        try:
            import os as _os
            for _child in EXE_DIR.iterdir():
                if not _child.is_dir():
                    continue
                # Check common layout paths: ClientName/GST Automation/ or direct
                for _sub in [_child / "GST Automation", _child / "Raw Data", _child]:
                    if _sub.is_dir() and any(_sub.glob("*.zip")):
                        _gstr1_zip_folders.append(_sub)
                        break
            # Also check script dir directly
            if any(EXE_DIR.glob("*.zip")):
                _gstr1_zip_folders.append(EXE_DIR)
        except Exception:
            pass

        # Peek at cache to give the user a count
        total_ct  = 0
        portal_ct = 0
        master_ct = 0
        cache_json = EXE_DIR / "gstin_name_cache.json"
        if cache_json.exists():
            try:
                import json as _j
                d = _j.loads(cache_json.read_text(encoding="utf-8"))
                total_ct  = len(d)
                portal_ct = sum(1 for v in d.values() if v.get("source") == "portal")
                master_ct = sum(1 for v in d.values() if v.get("source") == "customer_master")
            except Exception:
                pass

        to_refresh = master_ct + (total_ct - portal_ct - master_ct)  # non-portal entries
        est_sec = max(5, to_refresh) * 0.5

        zip_note = (f"\n  📂 {len(_gstr1_zip_folders)} folder(s) with GSTR-1 ZIPs found\n"
                    f"     New supplier/receiver GSTINs will be extracted first."
                    if _gstr1_zip_folders else
                    "\n  ℹ  No GSTR-1 ZIP folders found — only cache refresh will run.")

        msg = (
            f"This will fetch verified party names from the official GST portal\n"
            f"(services.gst.gov.in) — free, no API key, no login needed.\n\n"
            f"STEP 1: Scan GSTR-1 ZIPs for all supplier/receiver GSTINs{zip_note}\n\n"
            f"STEP 2: Fetch names from portal for any new/unverified GSTINs\n"
            f"Current cache: {total_ct} entries\n"
            f"  ✅ Already portal-verified : {portal_ct}\n"
            f"  ⚠  Unverified (from Excel) : {master_ct}\n\n"
        )
        if master_ct == 0 and portal_ct > 0 and not _gstr1_zip_folders:
            msg += "All names are already portal-verified. Run anyway to check for new GSTINs?\n\n"
        else:
            msg += f"Will fetch ~{to_refresh}+ name(s) from portal.\n"
            msg += f"Estimated time: ~{est_sec:.0f}+ seconds.\n\n"
        msg += "Continue?"

        if not messagebox.askyesno("Refresh GSTIN Names", msg):
            return

        self.running = True
        self.status_var.set("⏳  Fetching GSTIN names from portal…")
        self.prog_label.config(text="⚙  Refreshing GSTIN names…", fg=AMBER)
        self._log("")
        self._log("▶  Refresh GSTIN Names — services.gst.gov.in", "head")
        self._log("─" * 62, "head")
        self._log(f"  Cache before: {total_ct} entries  "
                  f"({portal_ct} portal-verified, {master_ct} from Excel)", "info")
        self._log("")

        def _work():
            try:
                import sys as _sys
                _sys.path.insert(0, str(EXE_DIR))
                from gstin_name_cache import GSTINNameCache

                def _log_fn(m):
                    self._log(f"  {m}", "info")

                # ── STEP 1: Scan GSTR-1 ZIPs for supplier/receiver GSTINs ────────
                # This extracts all party GSTINs from downloaded GSTR-1 JSON ZIPs
                # and seeds them into the cache BEFORE the portal fetch.
                # Uses extract_gst_names_once.py if available, else inline scan.
                new_gstins_found = 0
                if _gstr1_zip_folders:
                    self._log("", "info")
                    self._log("  ── STEP 1: Scanning GSTR-1 ZIPs for party GSTINs ──", "head")
                    extractor_script = EXE_DIR / "extract_gst_names_once.py"
                    if extractor_script.exists():
                        # Run as subprocess so it uses correct sys.path
                        import subprocess as _sp
                        for _folder in _gstr1_zip_folders:
                            self._log(f"    📂 {_folder.name}", "info")
                            try:
                                _r = _sp.run(
                                    [_get_python_exe(), str(extractor_script),
                                     str(_folder), "--no-export"],
                                    capture_output=True, text=True, timeout=300,
                                    cwd=str(EXE_DIR),
                                )
                                for _line in (_r.stdout + _r.stderr).splitlines():
                                    if _line.strip():
                                        self._log(f"    {_line}", "info")
                                        if "GSTIN(s)" in _line and "Found" in _line:
                                            try:
                                                new_gstins_found += int(_line.split()[2])
                                            except Exception:
                                                pass
                            except Exception as _se:
                                self._log(f"    ⚠  Scan error: {_se}", "warn")
                    else:
                        # Inline fallback: extract GSTINs directly without the script
                        self._log("    ℹ  extract_gst_names_once.py not found — using inline scan", "info")
                        import zipfile as _zf, json as _jj, re as _re
                        def _cg(g):
                            return _re.sub(r'[^A-Z0-9]', '', str(g or '').strip().upper())
                        def _scan_zip(zp):
                            gs = set()
                            try:
                                with _zf.ZipFile(zp) as z:
                                    for n in z.namelist():
                                        if not n.lower().endswith('.json'): continue
                                        try:
                                            d = _jj.loads(z.read(n).decode('utf-8', errors='replace'))
                                            if isinstance(d, dict):
                                                inner = d.get('data') or d.get('result') or d
                                                if isinstance(inner, str):
                                                    try: inner = _jj.loads(inner)
                                                    except Exception: inner = d
                                                for sec in ('b2b', 'cdnr', 'b2ba', 'cdnra'):
                                                    for e in inner.get(sec, []):
                                                        g = _cg(e.get('ctin', ''))
                                                        if len(g) == 15: gs.add(g)
                                        except Exception: pass
                            except Exception: pass
                            return gs
                        _cache_tmp = GSTINNameCache(
                            cache_file=str(EXE_DIR / "gstin_name_cache.json"),
                            log_fn=_log_fn, auto_fetch=False,
                        )
                        for _folder in _gstr1_zip_folders:
                            _zips = list(_folder.glob("*.zip"))
                            self._log(f"    📂 {_folder.name} — {len(_zips)} ZIP(s)", "info")
                            for _zp in _zips:
                                _gs = _scan_zip(_zp)
                                for _g in _gs:
                                    if _g not in _cache_tmp._data:
                                        _cache_tmp._data[_g] = {
                                            "legal_name": "", "trade_name": "",
                                            "source": "gstr1_zip",
                                            "fetched_at": "",
                                        }
                                        _cache_tmp._dirty = True
                                        new_gstins_found += 1
                        _cache_tmp.save()
                        self._log(f"    ✓ {new_gstins_found} new GSTIN(s) added from GSTR-1 ZIPs", "ok")

                    self._log(f"  ✓ STEP 1 done — {new_gstins_found} new GSTIN(s) discovered", "ok")

                # ── STEP 2: Fetch names from portal for all unverified entries ───
                self._log("", "info")
                self._log("  ── STEP 2: Fetching names from GST portal ──", "head")
                cache = GSTINNameCache(
                    cache_file=str(EXE_DIR / "gstin_name_cache.json"),
                    customer_master=str(EXE_DIR / "CustomerMaster.xlsx"),
                    log_fn=_log_fn,
                    auto_fetch=True,
                    prefer_portal=True,
                )

                # Refresh all non-portal entries (customer_master + gstr1_zip + tally_csv)
                refreshed, failed = cache.force_refresh(source_filter=None)
                # Note: force_refresh with source_filter=None refreshes all non-portal.
                # We implement this correctly by only refreshing non-portal sources:
                non_portal = [g for g, r in cache._data.items()
                              if r.get("source") not in ("portal", "portal_selenium")]
                if non_portal:
                    self._log(f"  📡 Fetching {len(non_portal)} unverified GSTIN(s) from portal…", "info")
                    refreshed = failed = 0
                    import time as _time
                    for i, g in enumerate(non_portal, 1):
                        self._log(f"    [{i}/{len(non_portal)}] {g} …", "info")
                        rec = cache._fetch_one(g)
                        if rec:
                            cache._data[g] = rec
                            cache._dirty = True
                            refreshed += 1
                            self._log(f"      ✓ {cache._best_name(rec)}", "info")
                        else:
                            failed += 1
                            existing = cache._data.get(g)
                            fb = cache._best_name(existing or {}, "⚠ Not found")
                            self._log(f"      ⚠ Portal failed → keeping: {fb}", "warn")
                        if i < len(non_portal):
                            _time.sleep(0.35)
                else:
                    refreshed = 0
                    failed = 0
                    self._log("  ✓ All entries already portal-verified — nothing to fetch", "ok")

                cache.save()

                stats = cache.stats()
                self._log("", "info")
                self._log(f"  ✅  Refreshed: {refreshed}   ❌  Failed: {failed}", "ok")
                self._log(f"  Cache after: {stats['total']} entries  "
                          f"({stats['portal']} portal-verified, {stats['master']} from Excel)",
                          "info")

                if failed > 0:
                    self._log("", "info")
                    self._log(f"  ℹ  {failed} GSTIN(s) could not be fetched — "
                              "portal blocked/timeout or cancelled GSTIN.", "warn")
                    self._log("     Those entries keep their existing names.", "warn")

                self._log("")
                self._log("✅  All party names updated.  GSTR-1 reports will now show", "ok")
                self._log("    supplier and receiver names from cache (no portal calls during reports).", "ok")
                self.status_var.set(f"✅  {refreshed} GSTIN names fetched from portal")

            except ImportError as ie:
                self._log(f"  ❌  Could not import gstin_name_cache: {ie}", "err")
                self._log("     Make sure gstin_name_cache.py is in the suite folder.", "err")
                self.status_var.set("❌  Refresh failed — module not found")
            except Exception as ex:
                self._log(f"  ❌  Error: {ex}", "err")
                import traceback as _tb
                self._log(_tb.format_exc(), "err")
                self.status_var.set("❌  Refresh failed")
            finally:
                self.running = False
                self.prog_label.config(text="", fg=GREEN)
                self._check_health()

        threading.Thread(target=_work, daemon=True).start()

    def install_packages(self):
        if not self._guard(): return

        # ── Ask whether to include Tally packages ───────────────────────────
        include_tally = messagebox.askyesno(
            "Install Packages — Tally Support?",
            "Do you use Tally integration?\n\n"
            "Click YES  to install ALL packages including Tally ODBC,\n"
            "           OCR, and window-automation libraries.\n\n"
            "Click NO   to install core packages only\n"
            "           (GST downloads, IT downloads, reports).\n\n"
            "You can always re-run this later to add Tally support."
        )

        # ── Package tiers ────────────────────────────────────────────────────
        # TIER 1 — Core Excel & Data
        tier1 = [
            ("pandas",            "Excel data processing"),
            ("openpyxl",          "Read/write .xlsx files"),
            ("numpy",             "Numerical operations"),
            ("xlrd",              "Legacy .xls file support"),
        ]
        # TIER 2 — PDF Processing (3-tier fallback chain)
        tier2 = [
            ("pdfplumber",        "Primary PDF extractor"),
            ("pypdf",             "Secondary PDF reader"),
            ("PyPDF2",            "Fallback PDF reader"),
        ]
        # TIER 3 — Browser Automation
        tier3 = [
            ("selenium",          "Browser automation"),
            ("webdriver-manager", "Auto-download ChromeDriver"),
        ]
        # TIER 4 — Web / HTTP
        tier4 = [
            ("flask",             "Web server for demo portal"),
            ("requests",          "HTTP requests & GSTIN lookup"),
            ("werkzeug",          "Flask utilities"),
            ("urllib3",           "HTTP connection pool"),
            ("gunicorn",          "Production WSGI server"),
        ]
        # TIER 5 — Tally Integration (optional)
        tier5 = [
            ("pyodbc",            "Tally ODBC connection"),
            ("pyautogui",         "Tally GUI automation"),
            ("pygetwindow",       "Tally window detection"),
            ("pywin32",           "Win32 API (win32gui/win32con)"),
            ("Pillow",            "Image capture for OCR"),
            ("pytesseract",       "OCR — reads Tally screenshots"),
        ]
        # TIER 6 — Utilities
        tier6 = [
            ("setuptools",        "pip build utilities"),
            ("packaging",         "Version comparison"),
            ("cryptography",      "Secure data handling"),
            ("python-dotenv",     "Environment variable loader"),
        ]

        core_pkgs  = tier1 + tier2 + tier3 + tier4 + tier6
        tally_pkgs = tier5

        all_pkgs = core_pkgs + (tally_pkgs if include_tally else [])
        total    = len(all_pkgs)

        # ── Confirmation dialog ──────────────────────────────────────────────
        pkg_names = [p for p, _ in core_pkgs]
        msg = (
            "This will install the following Python packages:\n\n"
            f"  CORE ({len(core_pkgs)} packages):\n"
            "  pandas, openpyxl, numpy, xlrd, pdfplumber,\n"
            "  pypdf, PyPDF2, selenium, webdriver-manager,\n"
            "  flask, requests, werkzeug, urllib3, gunicorn,\n"
            "  setuptools, packaging, cryptography, python-dotenv\n"
        )
        if include_tally:
            msg += (
                "\n  TALLY (6 packages):\n"
                "  pyodbc, pyautogui, pygetwindow, pywin32,\n"
                "  Pillow, pytesseract\n"
                "\n  ⚠  NOTE: After install, also:\n"
                "  • Enable ODBC in TallyPrime: Gateway of Tally\n"
                "    → F12 → ODBC Server → Yes  (port 9000)\n"
                "  • Install Tesseract OCR binary from:\n"
                "    github.com/UB-Mannheim/tesseract/wiki\n"
            )
        msg += f"\n  Total: {total} packages\n"
        msg += "\nThis takes 5–10 minutes and requires internet.\nYou only need to do this once.\n\nContinue?"

        if not messagebox.askyesno("Confirm Package Installation", msg):
            return

        self.running = True
        self.status_var.set("⏳  Installing packages…")
        self.prog_label.config(text="⚙  Installing…", fg=AMBER)
        self._log("")
        self._log("▶  Installing required packages", "head")
        self._log(f"   {total} packages total — please wait, this takes 5–10 minutes…", "info")
        self._log("─" * 62, "head")
        self._log("")

        def _work():
            env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
            py  = _get_python_exe()

            # ── Step 0: Upgrade pip first ────────────────────────────────────
            self._log("  Upgrading pip, setuptools, wheel…", "info")
            try:
                r = subprocess.run(
                    [py, "-m", "pip", "install", "--upgrade",
                     "pip", "setuptools", "wheel", "--quiet"],
                    capture_output=True, text=True,
                    encoding="utf-8", errors="replace", env=env)
                if r.returncode == 0:
                    self._log("  ✅  pip upgraded", "ok")
                else:
                    self._log(f"  ⚠  pip upgrade: {r.stderr.strip()[:120]}", "warn")
            except Exception as ex:
                self._log(f"  ⚠  pip upgrade skipped: {ex}", "warn")
            self._log("")

            # ── Install core packages tier by tier ───────────────────────────
            tier_labels = [
                ("TIER 1 — Excel & Data",         tier1),
                ("TIER 2 — PDF Processing",        tier2),
                ("TIER 3 — Browser Automation",    tier3),
                ("TIER 4 — Web / HTTP",            tier4),
                ("TIER 6 — Utilities",             tier6),
            ]
            if include_tally:
                tier_labels.append(("TIER 5 — Tally Integration", tier5))

            failed_pkgs = []
            installed   = 0

            for tier_label, pkg_list in tier_labels:
                self._log(f"  ── {tier_label} ──", "head")
                for pkg, desc in pkg_list:
                    self._log(f"  Installing {pkg}  ({desc})…", "info")
                    try:
                        r = subprocess.run(
                            [py, "-m", "pip", "install", pkg, "--quiet"],
                            capture_output=True, text=True,
                            encoding="utf-8", errors="replace", env=env)
                        if r.returncode == 0:
                            self._log(f"  ✅  {pkg}", "ok")
                            installed += 1
                        else:
                            err_msg = r.stderr.strip()[:160]
                            self._log(f"  ⚠  {pkg}: {err_msg}", "warn")
                            failed_pkgs.append(pkg)
                    except Exception as ex:
                        self._log(f"  ❌  {pkg}: {ex}", "err")
                        failed_pkgs.append(pkg)
                self._log("")

            # ── Summary ──────────────────────────────────────────────────────
            self._log("─" * 62, "head")
            if not failed_pkgs:
                self._log(f"✅  All {installed} packages installed successfully!", "ok")
                self._log("   You can now click  🔄 RUN EVERYTHING  to start.", "ok")
            else:
                self._log(f"⚠  Installed {installed}/{total}  —  "
                          f"{len(failed_pkgs)} failed:", "warn")
                for fp in failed_pkgs:
                    self._log(f"     • {fp}", "warn")
                self._log("", "warn")
                self._log("   To fix failed packages, run INSTALL_REQUIREMENTS.bat", "warn")
                self._log("   as Administrator (right-click → Run as Administrator).", "warn")
                if "pyodbc" in failed_pkgs:
                    self._log("", "warn")
                    self._log("   pyodbc failed — this usually means Visual C++ Build", "warn")
                    self._log("   Tools are missing. Download from:", "warn")
                    self._log("   visualstudio.microsoft.com/visual-cpp-build-tools/", "warn")

            if include_tally and "pytesseract" in [p for p, _ in tally_pkgs]:
                ok = "pytesseract" not in failed_pkgs
                if ok:
                    self._log("", "info")
                    self._log("  ℹ  pytesseract installed — but also install the", "info")
                    self._log("     Tesseract OCR binary for OCR to work:", "info")
                    self._log("     github.com/UB-Mannheim/tesseract/wiki", "info")

            self._log("")
            self.running = False
            self.status_var.set(
                f"✅  {installed}/{total} packages installed"
                if not failed_pkgs else
                f"⚠  {len(failed_pkgs)} package(s) failed — see log"
            )
            self.prog_label.config(text="", fg=GREEN)

        threading.Thread(target=_work, daemon=True).start()

    def _find_output_folder(self):
        """Return the most-likely output/download folder, falling back to EXE_DIR."""
        # Prefer the user-specified custom path when set and valid
        custom_out = self.output_path_var.get().strip()
        if custom_out:
            p = Path(custom_out)
            p.mkdir(parents=True, exist_ok=True)
            return p
        candidates = [
            EXE_DIR / "GST_Downloads",
            EXE_DIR / "gst_downloads",
            EXE_DIR / "output",
            EXE_DIR / "Output",
            EXE_DIR / "downloads",
            EXE_DIR / "Downloads",
            EXE_DIR / "GSTR2B",
            EXE_DIR / "gstr2b",
            EXE_DIR / "GST",
            EXE_DIR / "Reports",
            EXE_DIR / "reports",
        ]
        for c in candidates:
            if c.exists() and c.is_dir():
                return c
        return EXE_DIR

    def open_folder(self):
        folder = str(EXE_DIR)
        self._open_path_in_explorer(folder)

    def _start_spinner(self):
        """Animate the status bar with a spinner while a job is running."""
        _frames = ["⣾","⣽","⣻","⢿","⡿","⣟","⣯","⣷"]
        _idx = [0]
        def _tick():
            if not self.running:
                return
            cur = self.status_var.get()
            # Extract message after the spinner char
            for f in _frames:
                if cur.startswith(f + " "):
                    cur = cur[len(f)+1:]
                    break
            self.status_var.set(_frames[_idx[0]] + "  " + cur)
            _idx[0] = (_idx[0] + 1) % len(_frames)
            self.after(120, _tick)
        self.after(0, _tick)

    def open_output_folder(self):
        folder = str(self._find_output_folder())
        self._open_path_in_explorer(folder)

    def _open_path_in_explorer(self, folder):
        try:
            if os.name == "nt":
                subprocess.Popen(f'explorer "{folder}"', shell=True)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", folder])
            else:
                subprocess.Popen(["xdg-open", folder])
            self._log(f"  📁  Opened folder: {folder}", "info")
        except Exception:
            messagebox.showinfo("Folder", f"Folder:\n\n{folder}")

    def view_clients(self):
        for fname in ["Client_Manager_Secure_AY2027-28.xlsx",
                      "Client_Manager_Secure_AY2026-27.xlsx",
                      "Client_Manager_Secure_AY2025-26.xlsx",
                      "Client_Manager_Secure_AY2024-25.xlsx",
                      "clients.xlsx"]:
            f = EXE_DIR / fname
            if f.exists():
                if os.name == "nt":
                    os.startfile(str(f))
                else:
                    subprocess.Popen(["xdg-open", str(f)])
                self._log(f"  📋  Opened: {fname}", "info")
                return
        messagebox.showinfo("Client File Not Found",
            "clients.xlsx was not found.\n\n"
            "Create a file called  clients.xlsx  in the suite folder with these columns:\n\n"
            "  Client Name | PAN | GSTIN | IT Password | GST Password\n\n"
            "Contact RPR support if you need a template.")


if __name__ == "__main__":
    _py_found = _get_python_exe()
    print(f"[Launcher] Python interpreter : {_py_found}")
    print(f"[Launcher] Frozen EXE mode    : {_FROZEN}")
    print(f"[Launcher] Script dir         : {SCRIPT_DIR}")
    print(f"[Launcher] EXE dir            : {EXE_DIR}")
    if _FROZEN and _py_found in ("python", "python3"):
        _warn_root = tk.Tk()
        _warn_root.withdraw()
        messagebox.showwarning(
            "Python Not Found",
            "The launcher could not locate a Python interpreter on this PC.\n\n"
            "Please install Python 3.10 or newer from:\n"
            "  https://www.python.org/downloads/\n\n"
            "Tick  'Add Python to PATH'  during installation, then restart the launcher."
        )
        _warn_root.destroy()
    app = RPRLauncher()
    app.mainloop()
