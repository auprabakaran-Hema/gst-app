"""
RPR GST + IT Suite — Beautiful GUI Launcher
============================================
Double-click this file to open the control panel.
No command prompt. No typing. Just click buttons.

Place this file in the SAME folder as run_all.py
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import subprocess, sys, os, threading, json
from pathlib import Path
from datetime import datetime

# ── Find the suite folder ──────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).parent.resolve()

# Colors & Fonts
BG_DARK     = "#0D1117"
BG_CARD     = "#161B22"
BG_CARD2    = "#1C2128"
ACCENT      = "#238636"
ACCENT_BLUE = "#1F6FEB"
ACCENT_RED  = "#DA3633"
ACCENT_ORG  = "#E3B341"
TEXT_WHITE  = "#E6EDF3"
TEXT_DIM    = "#8B949E"
TEXT_GREEN  = "#3FB950"
TEXT_BLUE   = "#79C0FF"
BORDER      = "#30363D"

FONT_TITLE  = ("Segoe UI", 22, "bold")
FONT_HEAD   = ("Segoe UI", 13, "bold")
FONT_SUB    = ("Segoe UI", 10)
FONT_LABEL  = ("Segoe UI", 9)
FONT_MONO   = ("Consolas", 9)
FONT_BTN    = ("Segoe UI", 10, "bold")
FONT_BTNLG  = ("Segoe UI", 12, "bold")


def run_script(args, log_widget, status_var, on_done=None, stdin_answers=None):
    """
    Run a Python script in a background thread, stream output to the log widget.

    stdin_answers: list of strings to auto-send as keyboard input responses.
      Each string is sent as a line (with newline).  This lets us answer all
      interactive input() prompts without the user needing to type anything.
      e.g. ["16", "2025-26", "YES", "1"] answers the GST menu automatically.
    """
    status_var.set("⏳  Running...")

    def worker():
        try:
            child_env = {
                **os.environ,
                "PYTHONIOENCODING": "utf-8",
                "PYTHONUTF8":       "1",
                "PYTHONUNBUFFERED": "1",
            }
            # Build stdin pipe content: each answer on its own line.
            # Extra blank lines at end ensure any unexpected input() calls get
            # an empty-string response (which is usually the safe default).
            if stdin_answers:
                stdin_text = "\n".join(str(a) for a in stdin_answers) + "\n" * 10
            else:
                # No answers needed — send many newlines so any unexpected
                # input() gets the default (empty/Enter) and doesn't hang.
                stdin_text = "\n" * 20

            proc = subprocess.Popen(
                [sys.executable] + args,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                cwd=str(SCRIPT_DIR),
                env=child_env,
            )

            # Feed stdin answers in a separate thread so stdout streaming
            # doesn't block (avoids deadlock on large output + small pipe).
            def _feed_stdin():
                try:
                    proc.stdin.write(stdin_text)
                    proc.stdin.flush()
                    proc.stdin.close()
                except Exception:
                    pass
            threading.Thread(target=_feed_stdin, daemon=True).start()

            for line in proc.stdout:
                log_widget.config(state="normal")
                log_widget.insert(tk.END, line)
                log_widget.see(tk.END)
                log_widget.config(state="disabled")
            proc.wait()
            if proc.returncode == 0:
                status_var.set("✅  Completed successfully")
            else:
                status_var.set(f"⚠️  Finished with warnings (code {proc.returncode})")
        except Exception as e:
            log_widget.config(state="normal")
            log_widget.insert(tk.END, f"\n[ERROR] {e}\n")
            log_widget.config(state="disabled")
            status_var.set("❌  Error occurred")
        if on_done:
            on_done()

    threading.Thread(target=worker, daemon=True).start()


def check_suite_files():
    """Return list of missing critical files."""
    needed = ["run_all.py", "it_suite_v6.py"]
    missing = [f for f in needed if not (SCRIPT_DIR / f).exists()]
    return missing


class Tooltip:
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tip = None
        widget.bind("<Enter>", self.show)
        widget.bind("<Leave>", self.hide)

    def show(self, _):
        x = self.widget.winfo_rootx() + 20
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 5
        self.tip = tk.Toplevel(self.widget)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f"+{x}+{y}")
        lbl = tk.Label(self.tip, text=self.text, background="#2D333B",
                       foreground=TEXT_WHITE, font=FONT_LABEL, padx=8, pady=4,
                       relief="flat", justify="left")
        lbl.pack()

    def hide(self, _):
        if self.tip:
            self.tip.destroy()
            self.tip = None


class RPRLauncher(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("RPR GST + IT Suite — Control Panel")
        self.configure(bg=BG_DARK)
        self.resizable(True, True)
        self.minsize(860, 640)

        # Center on screen
        self.update_idletasks()
        w, h = 980, 740
        x = (self.winfo_screenwidth() - w) // 2
        y = (self.winfo_screenheight() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

        self.status_var = tk.StringVar(value="Ready")
        self.fy_var = tk.StringVar(value="2025-26")
        self.client_var = tk.StringVar(value="")
        self.skip_tally_var  = tk.BooleanVar(value=False)
        self.gst_option_var   = tk.StringVar(value="17")   # default: Fast Combo (RECOMMENDED)
        self.gst_fy_var       = tk.StringVar(value="2025-26")
        self.it_option_var    = tk.StringVar(value="1")    # default: ALL
        self.it_workers_var   = tk.StringVar(value="1")    # parallel browsers
        self.running = False

        self._build_ui()
        self._check_health()

    # ── UI BUILD ──────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── TOP HEADER ────────────────────────────────────────────────────────
        hdr = tk.Frame(self, bg=BG_DARK, pady=0)
        hdr.pack(fill="x", padx=0, pady=0)

        hdr_inner = tk.Frame(hdr, bg=BG_CARD, pady=14)
        hdr_inner.pack(fill="x")

        # Logo / Title row
        title_row = tk.Frame(hdr_inner, bg=BG_CARD)
        title_row.pack(fill="x", padx=24)

        tk.Label(title_row, text="⚡", font=("Segoe UI Emoji", 26),
                 bg=BG_CARD, fg=ACCENT_ORG).pack(side="left")
        tk.Label(title_row, text="  RPR GST + Income Tax Suite",
                 font=FONT_TITLE, bg=BG_CARD, fg=TEXT_WHITE).pack(side="left")

        tk.Label(title_row, text="v10.12  |  AY 2026-27",
                 font=FONT_LABEL, bg=BG_CARD, fg=TEXT_DIM).pack(side="right", padx=4)

        # Status bar
        self.health_label = tk.Label(hdr_inner, text="",
                                     font=FONT_LABEL, bg=BG_CARD, fg=TEXT_GREEN)
        self.health_label.pack(anchor="w", padx=28, pady=(2, 0))

        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")

        # ── MAIN BODY ─────────────────────────────────────────────────────────
        body = tk.Frame(self, bg=BG_DARK)
        body.pack(fill="both", expand=True, padx=16, pady=12)

        # Left panel: scrollable canvas wrapper
        left_outer = tk.Frame(body, bg=BG_DARK, width=310)
        left_outer.pack(side="left", fill="y", padx=(0, 12))
        left_outer.pack_propagate(False)

        left_canvas = tk.Canvas(left_outer, bg=BG_DARK, width=292,
                                highlightthickness=0, bd=0)
        left_vsb = tk.Scrollbar(left_outer, orient="vertical",
                                command=left_canvas.yview)
        left_canvas.configure(yscrollcommand=left_vsb.set)

        # Scrollbar only visible when needed; packed on right
        left_vsb.pack(side="right", fill="y")
        left_canvas.pack(side="left", fill="both", expand=True)

        left = tk.Frame(left_canvas, bg=BG_DARK)
        left_window = left_canvas.create_window((0, 0), window=left,
                                                anchor="nw")

        def _on_left_configure(event):
            left_canvas.configure(scrollregion=left_canvas.bbox("all"))

        def _on_canvas_resize(event):
            left_canvas.itemconfig(left_window, width=event.width)

        left.bind("<Configure>", _on_left_configure)
        left_canvas.bind("<Configure>", _on_canvas_resize)

        # Mouse-wheel scroll (Windows + Linux)
        def _on_mousewheel(event):
            if event.num == 4:
                left_canvas.yview_scroll(-1, "units")
            elif event.num == 5:
                left_canvas.yview_scroll(1, "units")
            else:
                left_canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")

        left_canvas.bind("<MouseWheel>", _on_mousewheel)
        left_canvas.bind("<Button-4>", _on_mousewheel)
        left_canvas.bind("<Button-5>", _on_mousewheel)
        left.bind("<MouseWheel>", _on_mousewheel)
        left.bind("<Button-4>", _on_mousewheel)
        left.bind("<Button-5>", _on_mousewheel)

        # Right panel: log
        right = tk.Frame(body, bg=BG_DARK)
        right.pack(side="left", fill="both", expand=True)

        self._build_left(left)
        self._build_right(right)

        # Propagate mouse-wheel scroll to all children of the left panel
        def _bind_children_scroll(widget):
            widget.bind("<MouseWheel>", _on_mousewheel, add="+")
            widget.bind("<Button-4>", _on_mousewheel, add="+")
            widget.bind("<Button-5>", _on_mousewheel, add="+")
            for child in widget.winfo_children():
                _bind_children_scroll(child)
        _bind_children_scroll(left)

        # ── BOTTOM STATUS BAR ─────────────────────────────────────────────────
        tk.Frame(self, bg=BORDER, height=1).pack(fill="x")
        bot = tk.Frame(self, bg=BG_CARD, pady=6)
        bot.pack(fill="x")
        tk.Label(bot, textvariable=self.status_var, font=FONT_LABEL,
                 bg=BG_CARD, fg=TEXT_DIM, anchor="w").pack(side="left", padx=16)
        _folder_str = str(SCRIPT_DIR)
        _max_len = 60
        _folder_display = ("…" + _folder_str[-(_max_len-1):]) if len(_folder_str) > _max_len else _folder_str
        folder_lbl = tk.Label(bot, text=f"📁 {_folder_display}", font=FONT_LABEL,
                              bg=BG_CARD, fg=ACCENT_BLUE, cursor="hand2")
        folder_lbl.pack(side="right", padx=16)
        folder_lbl.bind("<Button-1>", lambda e: self.open_folder())
        Tooltip(folder_lbl, f"Click to open folder:\n{SCRIPT_DIR}")

    def _section(self, parent, title):
        """Returns a card frame with a section title."""
        outer = tk.Frame(parent, bg=BG_CARD, bd=0)
        outer.pack(fill="x", pady=(0, 8))

        tk.Label(outer, text=title, font=("Segoe UI", 9, "bold"),
                 bg=BG_CARD, fg=TEXT_DIM, anchor="w").pack(
            fill="x", padx=12, pady=(10, 6))
        tk.Frame(outer, bg=BORDER, height=1).pack(fill="x", padx=12)

        inner = tk.Frame(outer, bg=BG_CARD, pady=8)
        inner.pack(fill="x", padx=10)
        return inner

    def _big_btn(self, parent, text, icon, color, tip, cmd):
        """Large action button."""
        f = tk.Frame(parent, bg=color, cursor="hand2")
        f.pack(fill="x", pady=3)

        lbl = tk.Label(f, text=f"  {icon}  {text}",
                       font=FONT_BTN, bg=color, fg="white",
                       anchor="w", pady=9, padx=6)
        lbl.pack(fill="x")

        for w in (f, lbl):
            w.bind("<Button-1>", lambda e: cmd())
        Tooltip(f, tip)
        return f

    def _small_btn(self, parent, text, color, cmd, tip=""):
        b = tk.Label(parent, text=text, font=FONT_LABEL,
                     bg=color, fg="white", cursor="hand2",
                     padx=8, pady=4, relief="flat")
        b.pack(side="left", padx=3, pady=4)
        b.bind("<Button-1>", lambda e: cmd())
        if tip:
            Tooltip(b, tip)
        return b

    def _build_left(self, parent):
        # ── FY Settings ───────────────────────────────────────────────────────
        sec = self._section(parent, "⚙  SETTINGS")

        tk.Label(sec, text="Financial Year", font=FONT_LABEL,
                 bg=BG_CARD, fg=TEXT_DIM).grid(row=0, column=0, sticky="w", padx=4)
        fy_entry = tk.Entry(sec, textvariable=self.fy_var, font=FONT_SUB,
                            bg=BG_CARD2, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                            relief="flat", bd=4, width=14)
        fy_entry.grid(row=0, column=1, sticky="w", padx=6, pady=2)

        tk.Label(sec, text="Client Filter (optional)", font=FONT_LABEL,
                 bg=BG_CARD, fg=TEXT_DIM).grid(row=1, column=0, sticky="w", padx=4)
        cl_entry = tk.Entry(sec, textvariable=self.client_var, font=FONT_SUB,
                            bg=BG_CARD2, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                            relief="flat", bd=4, width=14)
        cl_entry.grid(row=1, column=1, sticky="w", padx=6, pady=2)
        Tooltip(cl_entry, "Leave blank for ALL clients.\nType a name to process just one client.")

        skip_cb = tk.Checkbutton(sec, text="Skip Tally Step (if Tally not open)",
                                  variable=self.skip_tally_var,
                                  onvalue=True, offvalue=False,
                                  font=FONT_LABEL, bg=BG_CARD, fg=TEXT_DIM,
                                  activebackground=BG_CARD, activeforeground=TEXT_WHITE,
                                  selectcolor=BG_CARD2)
        skip_cb.grid(row=2, column=0, columnspan=2, sticky="w", padx=4, pady=2)
        skip_cb.deselect()  # Force visual state — fixes Tkinter Canvas render glitch
        Tooltip(skip_cb, "Tick this if Tally is not running.\nSkips Step 1 only — GST/IT downloads still work.")


        # ── GST OPTIONS ───────────────────────────────────────────────────────
        sec_gst = self._section(parent, "🟢  GST DOWNLOAD OPTIONS")

        tk.Label(sec_gst, text="What to download:", font=FONT_LABEL,
                 bg=BG_CARD, fg=TEXT_DIM).grid(row=0, column=0, sticky="w", padx=4)

        gst_opts = [
            ("16 — Full Optimised  (all returns, fastest)",                        "16"),
            ("17 — ⚡ Fast Combo   (1+1A+2B+3B+Tax Liability — RECOMMENDED)",      "17"),
            ("1  — Full Suite      (all returns, phased)",                          "1"),
            ("15 — GSTR-2B + GSTR-3B only (fast, ~10 min)",                       "15"),
            ("13 — GSTR-1 + GSTR-2B + GSTR-3B  (no 2A)",                          "13"),
            ("9  — GSTR-1 + GSTR-2B + GSTR-2A  (no 3B)",                          "9"),
            ("8  — GSTR-2B + GSTR-2A  (both purchase returns)",                    "8"),
            ("7  — GSTR-1 + GSTR-3B",                                              "7"),
            ("4  — GSTR-2B only  (Excel, direct, ~5 min)",                        "4"),
            ("6  — GSTR-3B only  (PDF, direct, ~5 min)",                          "6"),
            ("2  — GSTR-1 only",                                                    "2"),
            ("5  — GSTR-2A only  (generate first)",                                "5"),
            ("10 — Tax Liability & ITC Comparison only",                           "10"),
            ("11 — Offline  (no download, recon from files)",                     "11"),
            ("12 — Retry Failed  (re-download from Master Report)",               "12"),
            ("18 — GSTR-3B Summary Report  (PDF → Excel)",                       "18"),
        ]
        gst_cb = ttk.Combobox(sec_gst, textvariable=self.gst_option_var,
                              values=[o[0] for o in gst_opts],
                              font=FONT_LABEL, state="readonly", width=46)
        gst_cb.current(1)   # default = Option 17 (Fast Combo — RECOMMENDED)
        gst_cb.grid(row=1, column=0, columnspan=2, sticky="w", padx=4, pady=2)
        # Keep option number in sync + auto-manage Skip Tally
        # GST options that download directly from portal — Tally not needed
        _GST_PORTAL_ONLY_OPTIONS = {"4", "6", "2", "5", "15", "10"}

        def _gst_sel(e):
            sel = self.gst_option_var.get()
            num = sel.split(" — ")[0].strip()
            self.gst_option_var.set(num)
            # Auto-tick Skip Tally for portal-only options
            if num in _GST_PORTAL_ONLY_OPTIONS:
                self.skip_tally_var.set(True)
                self._log(f"  ℹ  Option {num} selected — Skip Tally auto-enabled (not needed for this download)", "info")
        gst_cb.bind("<<ComboboxSelected>>", _gst_sel)
        Tooltip(gst_cb, "Select which GST returns to download from the portal.\n"
                        "Option 17 = RECOMMENDED: 1+1A+2B+3B+Tax Liability.\n"
                        "Option 16 = Full Optimised (all returns incl. 2A).\n"
                        "Option 11 = Offline recon only (no browser needed).\n"
                        "Options 12/18 = Utility tools (Retry / 3B Summary).")

        tk.Label(sec_gst, text="GST Financial Year:", font=FONT_LABEL,
                 bg=BG_CARD, fg=TEXT_DIM).grid(row=2, column=0, sticky="w", padx=4, pady=(4,0))
        gst_fy = tk.Entry(sec_gst, textvariable=self.gst_fy_var, font=FONT_LABEL,
                          bg=BG_CARD2, fg=TEXT_WHITE, insertbackground=TEXT_WHITE,
                          relief="flat", bd=4, width=14)
        gst_fy.grid(row=2, column=1, sticky="w", padx=6, pady=(4,0))
        Tooltip(gst_fy, "e.g. 2025-26  or range: 2024-25 to 2025-26")

        # ── IT OPTIONS ────────────────────────────────────────────────────────
        sec_it = self._section(parent, "🔵  IT DOWNLOAD OPTIONS")

        tk.Label(sec_it, text="What to download:", font=FONT_LABEL,
                 bg=BG_CARD, fg=TEXT_DIM).grid(row=0, column=0, sticky="w", padx=4)

        it_opts = [
            ("1 — ALL: 26AS + AIS + TIS + IT Recon Excel",  "1"),
            ("2 — 26AS only",                                 "2"),
            ("3 — AIS + TIS only",                            "3"),
            ("4 — IT Recon Excel only (from existing PDFs)",  "4"),
        ]
        it_cb = ttk.Combobox(sec_it, textvariable=self.it_option_var,
                             values=[o[0] for o in it_opts],
                             font=FONT_LABEL, state="readonly", width=38)
        it_cb.current(0)
        it_cb.grid(row=1, column=0, columnspan=2, sticky="w", padx=4, pady=2)
        def _it_sel(e):
            sel = self.it_option_var.get()
            num = sel.split(" — ")[0].strip()
            self.it_option_var.set(num)
        it_cb.bind("<<ComboboxSelected>>", _it_sel)
        Tooltip(it_cb, "Select what to download from the IT portal.")

        tk.Label(sec_it, text="Parallel browsers:", font=FONT_LABEL,
                 bg=BG_CARD, fg=TEXT_DIM).grid(row=2, column=0, sticky="w", padx=4, pady=(4,0))
        workers_spin = tk.Spinbox(sec_it, from_=1, to=5, width=5,
                                  textvariable=self.it_workers_var,
                                  font=FONT_LABEL, bg=BG_CARD2, fg=TEXT_WHITE,
                                  insertbackground=TEXT_WHITE, relief="flat")
        workers_spin.grid(row=2, column=1, sticky="w", padx=6, pady=(4,0))
        Tooltip(workers_spin, "Number of client browsers to open at once.\n1 = one at a time (safest).\n2-3 = faster but needs a strong PC.")

        # ── MAIN ACTIONS ──────────────────────────────────────────────────────
        sec2 = self._section(parent, "🚀  FULL PIPELINE")

        self._big_btn(sec2,
                      "RUN EVERYTHING", "🔄",
                      "#1A472A",
                      "Full Pipeline: Tally → GST Downloads → IT Downloads → Bridge → Reports\n(Most common — runs all 7 steps)",
                      self.run_full)

        self._big_btn(sec2,
                      "OFFLINE / BRIDGE ONLY", "📂",
                      "#1B3A4B",
                      "No internet downloads needed.\nUse existing files on disk to build reconciliation reports.",
                      self.run_offline)

        # ── INDIVIDUAL STEPS ──────────────────────────────────────────────────
        sec3 = self._section(parent, "🔧  INDIVIDUAL STEPS")

        steps = [
            ("GST Downloads Only",   "🟢", "#1E3A1E", "Downloads GSTR-1, 2B, 3B for all clients from GST Portal.\nBrowser opens. Enter CAPTCHA once.", self.run_gst_only),
            ("GSTR-2B Only (Fast)",  "📥", "#1A3A2A", "Downloads ONLY GSTR-2B for all clients — NO Tally needed.\nSkips all other steps. ~5 min.", self.run_2b_only),
            ("IT Downloads Only",    "🔵", "#1B2A4A", "Downloads 26AS, AIS, TIS for all clients from IT Portal.\nBrowser opens. Enter OTP per client.", self.run_it_only),
            ("Bridge / Reports Only","🟡", "#3A2E00", "Runs reconciliation & all Excel reports.\nNeeds GST + IT files already downloaded.", self.run_bridge_only),
            ("Install Packages",     "📦", "#2D1B69", "Installs all required Python packages.\nRun this ONCE before first use.", self.install_packages),
        ]

        for label, icon, color, tip, cmd in steps:
            f = tk.Frame(sec3, bg=BG_CARD)
            f.pack(fill="x", pady=2)

            inner = tk.Frame(f, bg=color, cursor="hand2")
            inner.pack(fill="x")

            lbl = tk.Label(inner, text=f" {icon}  {label}",
                           font=("Segoe UI", 9, "bold"),
                           bg=color, fg="white", anchor="w", pady=7, padx=8)
            lbl.pack(fill="x")

            for w in (inner, lbl):
                w.bind("<Button-1>", lambda e, c=cmd: c())
            Tooltip(inner, tip)

        # ── QUICK TOOLS ───────────────────────────────────────────────────────
        sec4 = self._section(parent, "🛠  QUICK TOOLS")

        row1 = tk.Frame(sec4, bg=BG_CARD)
        row1.pack(fill="x")
        self._small_btn(row1, "📁 Open Folder", "#2D333B", self.open_folder,
                        "Opens the suite folder in Windows Explorer")
        self._small_btn(row1, "📋 View Clients", "#2D333B", self.view_clients,
                        "Opens clients.xlsx to add/edit client details")

        row2 = tk.Frame(sec4, bg=BG_CARD)
        row2.pack(fill="x")
        self._small_btn(row2, "🗑 Clear Log", "#3A2020", self.clear_log)
        self._small_btn(row2, "💾 Save Log", "#1B3020", self.save_log)

    def _build_right(self, parent):
        # Header
        log_hdr = tk.Frame(parent, bg=BG_CARD2, pady=8)
        log_hdr.pack(fill="x")
        tk.Label(log_hdr, text="  📟  Live Output Log",
                 font=("Segoe UI", 10, "bold"),
                 bg=BG_CARD2, fg=TEXT_WHITE).pack(side="left", padx=8)

        self.prog_label = tk.Label(log_hdr, text="",
                                   font=FONT_LABEL, bg=BG_CARD2, fg=TEXT_GREEN)
        self.prog_label.pack(side="right", padx=12)

        # Log area
        log_frame = tk.Frame(parent, bg=BG_DARK, bd=1, relief="flat")
        log_frame.pack(fill="both", expand=True, pady=(4, 0))

        self.log = scrolledtext.ScrolledText(
            log_frame,
            font=FONT_MONO,
            bg="#0D1117",
            fg="#E6EDF3",
            insertbackground=TEXT_WHITE,
            relief="flat",
            state="disabled",
            wrap="word",
            padx=10,
            pady=8,
        )
        self.log.pack(fill="both", expand=True)

        # Tag colors for log
        self.log.tag_config("ok",    foreground=TEXT_GREEN)
        self.log.tag_config("warn",  foreground=ACCENT_ORG)
        self.log.tag_config("err",   foreground="#FF6B6B")
        self.log.tag_config("info",  foreground=TEXT_BLUE)
        self.log.tag_config("head",  foreground="#C9D1D9", font=("Consolas", 9, "bold"))

        self._log_welcome()

    # ── LOG HELPERS ───────────────────────────────────────────────────────────

    def _log(self, msg, tag=None):
        self.log.config(state="normal")
        if tag:
            self.log.insert(tk.END, msg + "\n", tag)
        else:
            self.log.insert(tk.END, msg + "\n")
        self.log.see(tk.END)
        self.log.config(state="disabled")

    def _log_welcome(self):
        ts = datetime.now().strftime("%d %b %Y  %H:%M")
        self._log("=" * 62, "head")
        self._log(f"  RPR GST + IT Suite Launcher  —  {ts}", "head")
        self._log("=" * 62, "head")
        self._log("")
        self._log(f"  Suite folder: {SCRIPT_DIR}", "info")
        self._log("")
        self._log("  Quick Start:", "head")
        self._log("  1. First time? → Click  📦 Install Packages", "info")
        self._log("  2. Fill clients.xlsx with client credentials", "info")
        self._log("  3. Click  🔄 RUN EVERYTHING  to start", "info")
        self._log("")

    def clear_log(self):
        self.log.config(state="normal")
        self.log.delete("1.0", tk.END)
        self.log.config(state="disabled")
        self._log_welcome()

    def save_log(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
            initialfile=f"RPR_Log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
        )
        if path:
            content = self.log.get("1.0", tk.END)
            Path(path).write_text(content, encoding="utf-8")
            self._log(f"  Log saved → {path}", "ok")

    # ── HEALTH CHECK ─────────────────────────────────────────────────────────

    def _check_health(self):
        missing = check_suite_files()
        clients_ok = (SCRIPT_DIR / "clients.xlsx").exists() or \
                     (SCRIPT_DIR / "Client_Manager_Secure_AY2025-26.xlsx").exists()

        if missing:
            self.health_label.config(
                text=f"⚠  Missing files: {', '.join(missing)} — make sure this launcher is in the suite folder",
                fg=ACCENT_ORG)
        elif not clients_ok:
            self.health_label.config(
                text="⚠  clients.xlsx not found — please add your client list before running",
                fg=ACCENT_ORG)
        else:
            count = 0
            try:
                import openpyxl as ox
                wb = ox.load_workbook(str(SCRIPT_DIR / "clients.xlsx"), read_only=True)
                ws = wb.active
                count = max(0, sum(1 for _ in ws.iter_rows()) - 1)
                wb.close()
            except Exception:
                pass
            label = f"{count} client(s) loaded" if count else "clients.xlsx found"
            self.health_label.config(
                text=f"✅  Suite ready  |  {label}  |  FY 2025-26 / AY 2026-27",
                fg=TEXT_GREEN)

    # ── ACTIONS ───────────────────────────────────────────────────────────────

    def _guard(self):
        """Returns True if safe to run."""
        if self.running:
            messagebox.showwarning("Already Running",
                                   "A process is already running.\nPlease wait for it to finish.")
            return False
        missing = check_suite_files()
        if missing:
            messagebox.showerror("Files Missing",
                                 f"These required files are missing:\n{chr(10).join(missing)}\n\n"
                                 "Please place this launcher in the same folder as run_all.py")
            return False
        return True

    def _build_args(self, extra=None):
        args = [str(SCRIPT_DIR / "run_all.py")]
        fy = self.fy_var.get().strip()
        if fy and fy != "2025-26":
            args += ["--fy", fy]
        client = self.client_var.get().strip()
        if client:
            args += ["--client", client]
        if self.skip_tally_var.get():
            args += ["--skip-tally"]
        if extra:
            args += extra
        return args

    def _inject_menu_env(self):
        """
        Inject GST_MENU_CHOICE, GST_MENU_FY, IT_MENU_CHOICE, IT_WORKERS into
        os.environ BEFORE launching run_all.  run_all passes os.environ to its
        subprocess calls, so gst_suite and it_suite inherit these and auto-
        answer their interactive menus without any keyboard input.
        """
        gst_choice = self.gst_option_var.get().split()[0]
        gst_fy     = self.gst_fy_var.get().strip() or "2025-26"
        it_choice  = self.it_option_var.get().split()[0]
        it_workers = self.it_workers_var.get().strip() or "1"

        os.environ["GST_MENU_CHOICE"] = gst_choice
        os.environ["GST_MENU_FY"]     = gst_fy
        os.environ["IT_MENU_CHOICE"]  = it_choice
        os.environ["IT_WORKERS"]      = it_workers

        # Log what we're sending
        return (f"  Auto-answers: GST={gst_choice} | FY={gst_fy} | "
                f"IT={it_choice} | Workers={it_workers}")

    def _build_stdin_answers(self):
        """
        Build the list of stdin answers that will be auto-piped to run_all.py
        subprocesses via env vars.  run_all.py itself has no interactive menus —
        it passes GST_MENU / IT_MENU env vars to gst_suite and it_suite so those
        scripts skip their menus automatically.  We set those env vars here.
        """
        # These are read by gst_suite_v32.py and it_suite_v6.py when set
        gst_choice  = self.gst_option_var.get().split()[0]   # e.g. "16"
        gst_fy      = self.gst_fy_var.get().strip() or "2025-26"
        it_choice   = self.it_option_var.get().split()[0]    # e.g. "1"
        it_workers  = self.it_workers_var.get().strip() or "1"

        # Store as instance vars so _build_args can pass them as env vars
        self._gst_menu_choice = gst_choice
        self._gst_menu_fy     = gst_fy
        self._it_menu_choice  = it_choice
        self._it_workers      = it_workers

        # stdin answers for run_all.py itself (offline mode menu: 1-12)
        # In online mode run_all has no input() — only offline mode does.
        return []   # run_all online mode needs no stdin

    def _start_run(self, args, label, stdin_answers=None):
        self.running = True
        self.status_var.set(f"⏳  {label}…")
        self.prog_label.config(text=f"⚙ {label}", fg=ACCENT_ORG)
        self._log("")
        self._log(f"▶  {label}", "head")
        self._log(f"   Command: python {' '.join(args[0:1] + args[1:])}", "info")
        self._log("─" * 62, "head")
        self._log("")

        def on_done():
            self.running = False
            self.prog_label.config(text="", fg=TEXT_GREEN)
            self._check_health()

        run_script(args, self.log, self.status_var, on_done, stdin_answers)

    def run_full(self):
        if not self._guard():
            return
        skip = self.skip_tally_var.get()
        tally_line = "" if skip else "• Step 1: Tally must be open (supplier name cache)\n"
        gst_choice = self.gst_option_var.get().split()[0]
        it_choice  = self.it_option_var.get().split()[0]
        if not messagebox.askyesno("Start Full Pipeline",
                                   "This will:\n\n"
                                   + tally_line +
                                   f"• GST Portal: Option {gst_choice} (auto-answered)\n"
                                   "  Enter CAPTCHA in browser when it appears\n"
                                   f"• IT Portal: Option {it_choice} (auto-answered)\n"
                                   "  Enter OTP in browser when it appears\n"
                                   "• Build all reconciliation Excel reports\n\nContinue?"):
            return
        env_msg = self._inject_menu_env()
        self._log(env_msg, "info")
        self._start_run(self._build_args(), "Full Pipeline (Steps 1->7)", stdin_answers=[])

    def run_offline(self):
        if not self._guard():
            return
        # Offline mode shows a 1-12 menu in run_all.py.
        # We ask the user which option they want via a dialog.
        offline_opts = [
            ("1",  "Step 1 only  — Tally GST Extract"),
            ("2",  "Step 2+3    — GST Recon from existing files"),
            ("3",  "Step 4+5    — IT Recon from existing PDFs"),
            ("4",  "Step 6      — Master Bridge only"),
            ("5",  "Step 6b     — GST-IT Comparison Excel"),
            ("6",  "Step 6c     — GSTR-2B Consolidated Extractor"),
            ("7",  "Step 6d     — GSTR-1 vs 26AS Comparison"),
            ("8",  "Step 7      — Final Consolidated Report"),
            ("9",  "Steps 6→7   — Bridge + All Reports  ✦ Most common"),
            ("10", "Steps 3→7   — IT Recon + Bridge + Reports"),
            ("11", "Steps 2→7   — GST + IT Recon + Bridge + Reports"),
            ("12", "ALL STEPS   — Full offline pipeline"),
        ]
        dlg = tk.Toplevel(self)
        dlg.title("Offline Mode — Select Steps")
        dlg.configure(bg=BG_DARK)
        dlg.resizable(False, False)
        dlg.grab_set()
        w, h = 500, 420
        dlg.geometry(f"{w}x{h}+{(dlg.winfo_screenwidth()-w)//2}+{(dlg.winfo_screenheight()-h)//2}")

        tk.Label(dlg, text="No browser downloads — use existing files on disk.",
                 font=FONT_LABEL, bg=BG_DARK, fg=TEXT_DIM).pack(pady=(12,4))

        choice_var = tk.StringVar(value="9")
        lb_frame = tk.Frame(dlg, bg=BG_DARK)
        lb_frame.pack(fill="both", expand=True, padx=16)
        lb = tk.Listbox(lb_frame, font=FONT_LABEL, bg=BG_CARD2, fg=TEXT_WHITE,
                        selectbackground=ACCENT_BLUE, relief="flat",
                        height=len(offline_opts))
        for num, label in offline_opts:
            lb.insert(tk.END, f"  [{num:>2}]  {label}")
        lb.selection_set(8)   # default: option 9
        lb.pack(fill="both", expand=True)

        def _ok():
            sel = lb.curselection()
            if sel:
                choice_var.set(offline_opts[sel[0]][0])
            dlg.destroy()
        def _cancel():
            choice_var.set("")
            dlg.destroy()

        btn_row = tk.Frame(dlg, bg=BG_DARK)
        btn_row.pack(pady=10)
        tk.Button(btn_row, text="▶  Run Selected",
                  font=FONT_BTN, bg=ACCENT, fg="white",
                  relief="flat", padx=16, pady=6, command=_ok).pack(side="left", padx=6)
        tk.Button(btn_row, text="Cancel",
                  font=FONT_BTN, bg="#3A2020", fg="white",
                  relief="flat", padx=10, pady=6, command=_cancel).pack(side="left", padx=6)

        self.wait_window(dlg)
        chosen = choice_var.get()
        if not chosen:
            return
        env_msg = self._inject_menu_env()
        self._log(env_msg, "info")
        # stdin answer = the offline menu number
        self._start_run(self._build_args(["--offline"]),
                        f"Offline Mode — Option {chosen}",
                        stdin_answers=[chosen])

    def run_2b_only(self):
        """Download GSTR-2B only — forces GST option 4, always skips Tally."""
        if not self._guard():
            return
        # Force option 4 (GSTR-2B only) and skip Tally
        self.gst_option_var.set("4")
        self.skip_tally_var.set(True)
        os.environ["GST_MENU_CHOICE"] = "4"
        os.environ["GST_MENU_FY"]     = self.gst_fy_var.get().strip() or "2025-26"
        os.environ["IT_MENU_CHOICE"]  = self.it_option_var.get().split()[0]
        os.environ["IT_WORKERS"]      = self.it_workers_var.get().strip() or "1"
        self._log("  📥 GSTR-2B Only mode — GST Option 4, Skip Tally = ON", "info")
        self._log("  ⚠  Tally extract is SKIPPED (not needed for 2B download)", "warn")
        self._start_run(self._build_args(["--only-gst"]), "GSTR-2B Only Download", stdin_answers=[])

    def run_gst_only(self):
        if not self._guard():
            return
        env_msg = self._inject_menu_env()
        self._log(env_msg, "info")
        self._start_run(self._build_args(["--only-gst"]), "GST Downloads Only", stdin_answers=[])

    def run_it_only(self):
        if not self._guard():
            return
        env_msg = self._inject_menu_env()
        self._log(env_msg, "info")
        self._start_run(self._build_args(["--only-it"]), "IT Downloads Only", stdin_answers=[])

    def run_bridge_only(self):
        if not self._guard():
            return
        env_msg = self._inject_menu_env()
        self._log(env_msg, "info")
        self._start_run(self._build_args(["--only-bridge"]), "Bridge & Reports Only", stdin_answers=[])

    def install_packages(self):
        if not self._guard():
            return
        if not messagebox.askyesno("Install Packages",
                                   "This will install all required Python packages:\n\n"
                                   "pandas, openpyxl, numpy, pdfplumber,\n"
                                   "pypdf, selenium, webdriver-manager, flask\n\n"
                                   "This may take 3–5 minutes.\nContinue?"):
            return

        self.running = True
        self.status_var.set("⏳  Installing packages…")
        self.prog_label.config(text="⚙ Installing…", fg=ACCENT_ORG)
        self._log("")
        self._log("▶  Installing Packages", "head")
        self._log("─" * 62, "head")

        pkgs = ["pandas", "openpyxl", "numpy", "pdfplumber",
                "pypdf", "selenium", "webdriver-manager", "flask", "requests"]

        def worker():
            _enc_env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUNBUFFERED": "1"}
            for pkg in pkgs:
                self._log(f"  Installing {pkg}...", "info")
                try:
                    result = subprocess.run(
                        [sys.executable, "-m", "pip", "install", pkg, "--quiet"],
                        capture_output=True, text=True,
                        encoding="utf-8", errors="replace",
                        env=_enc_env,
                    )
                    if result.returncode == 0:
                        self._log(f"  ✅  {pkg} installed", "ok")
                    else:
                        self._log(f"  ⚠  {pkg}: {result.stderr.strip()}", "warn")
                except Exception as e:
                    self._log(f"  ❌  {pkg} failed: {e}", "err")

            self._log("")
            self._log("✅  All packages processed!", "ok")
            self._log("   You can now run the suite.", "info")
            self.running = False
            self.status_var.set("✅  Packages installed")
            self.prog_label.config(text="", fg=TEXT_GREEN)

        threading.Thread(target=worker, daemon=True).start()

    def open_folder(self):
        # Always resolve from __file__ so we never open a subprocess cwd by mistake
        folder = str(Path(__file__).parent.resolve())
        try:
            if os.name == "nt":
                subprocess.Popen(f'explorer "{folder}"', shell=True)
            elif sys.platform == "darwin":
                subprocess.Popen(["open", folder])
            else:
                subprocess.Popen(["xdg-open", folder])
            self._log(f"  📁 Opening suite folder: {folder}", "info")
        except Exception as e:
            self._log(f"  ❌ Could not open folder: {e}", "err")
            messagebox.showinfo("Suite Folder",
                                f"Suite folder path:\n\n{folder}\n\nCopy and paste into Windows Explorer.")

    def view_clients(self):
        candidates = [
            SCRIPT_DIR / "Client_Manager_Secure_AY2025-26.xlsx",
            SCRIPT_DIR / "clients.xlsx",
        ]
        for f in candidates:
            if f.exists():
                if os.name == "nt":
                    os.startfile(str(f))
                else:
                    subprocess.Popen(["xdg-open", str(f)])
                self._log(f"  Opened: {f.name}", "info")
                return
        messagebox.showinfo("Not Found",
                            "clients.xlsx not found.\n\n"
                            "Create a file named 'clients.xlsx' in the suite folder with columns:\n"
                            "Client Name | PAN | GSTIN | DOB | IT Username | IT Password | GST Username | GST Password")


# ── ENTRY POINT ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    app = RPRLauncher()
    app.mainloop()
