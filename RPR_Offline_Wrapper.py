r"""
RPR_Offline_Wrapper.py
======================
Place this file in: C:\Users\RAJASEKARAN\Downloads\RPR_GST_IT_Suite_NT_V3.1\

Double-click or run from the launcher as "Reports Only (offline)".

What it does:
  1. Shows a GUI popup asking for GST folder path
  2. Shows a GUI popup asking for IT folder path
  3. Runs run_all.py --offline-choice 12 with those folders
  4. No input() crashes — fully GUI driven
"""

import os
import sys
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RUN_ALL    = os.path.join(SCRIPT_DIR, "run_all.py")

# ── remembered last-used folders (saved in a small file next to this script)
PREFS_FILE = os.path.join(SCRIPT_DIR, ".rpr_offline_prefs.txt")

def load_prefs():
    prefs = {"gst": "", "it": ""}
    try:
        with open(PREFS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                k, _, v = line.strip().partition("=")
                if k in prefs:
                    prefs[k] = v
    except Exception:
        pass
    return prefs

def save_prefs(gst, it):
    try:
        with open(PREFS_FILE, "w", encoding="utf-8") as f:
            f.write(f"gst={gst}\nit={it}\n")
    except Exception:
        pass

# ── main picker window
class FolderPickerApp:
    def __init__(self, root):
        self.root = root
        self.root.title("RPR Suite — Offline Reports Only")
        self.root.resizable(False, False)
        self.root.configure(bg="#1e1e2e")

        prefs = load_prefs()

        pad = dict(padx=12, pady=6)

        tk.Label(root, text="RPR GST + IT Suite  —  Reports Only (Offline)",
                 bg="#1e1e2e", fg="#cdd6f4", font=("Segoe UI", 13, "bold")).grid(
                 row=0, column=0, columnspan=3, pady=(16,4), padx=16)

        tk.Label(root, text="No internet. No browser. Reads files already on disk.",
                 bg="#1e1e2e", fg="#a6adc8", font=("Segoe UI", 9)).grid(
                 row=1, column=0, columnspan=3, pady=(0,14))

        # ── GST folder
        tk.Label(root, text="GST Automation folder:",
                 bg="#1e1e2e", fg="#cdd6f4", font=("Segoe UI", 10)).grid(
                 row=2, column=0, sticky="w", **pad)

        self.gst_var = tk.StringVar(value=prefs["gst"])
        gst_entry = tk.Entry(root, textvariable=self.gst_var,
                             width=55, bg="#313244", fg="#cdd6f4",
                             insertbackground="white", relief="flat",
                             font=("Segoe UI", 9))
        gst_entry.grid(row=2, column=1, **pad)

        tk.Button(root, text="Browse", bg="#45475a", fg="#cdd6f4",
                  relief="flat", cursor="hand2",
                  command=self.browse_gst).grid(row=2, column=2, **pad)

        # ── IT folder
        tk.Label(root, text="IT Download folder:",
                 bg="#1e1e2e", fg="#cdd6f4", font=("Segoe UI", 10)).grid(
                 row=3, column=0, sticky="w", **pad)

        self.it_var = tk.StringVar(value=prefs["it"])
        it_entry = tk.Entry(root, textvariable=self.it_var,
                            width=55, bg="#313244", fg="#cdd6f4",
                            insertbackground="white", relief="flat",
                            font=("Segoe UI", 9))
        it_entry.grid(row=3, column=1, **pad)

        tk.Button(root, text="Browse", bg="#45475a", fg="#cdd6f4",
                  relief="flat", cursor="hand2",
                  command=self.browse_it).grid(row=3, column=2, **pad)

        # ── hint
        tk.Label(root,
                 text="Tip: GST folder = ClientName\\GST Automation   |   IT folder = ClientName\\IT Download",
                 bg="#1e1e2e", fg="#6c7086", font=("Segoe UI", 8)).grid(
                 row=4, column=0, columnspan=3, pady=(2,10))

        # ── Run button
        tk.Button(root, text="▶  Run Reports Only (Steps 2 → 7)",
                  bg="#89b4fa", fg="#1e1e2e",
                  font=("Segoe UI", 11, "bold"),
                  relief="flat", cursor="hand2", padx=20, pady=8,
                  command=self.run).grid(
                  row=5, column=0, columnspan=3, pady=(4, 16))

        # ── log output area
        tk.Label(root, text="Output log:", bg="#1e1e2e", fg="#a6adc8",
                 font=("Segoe UI", 9)).grid(row=6, column=0, sticky="w", padx=12)

        self.log_box = tk.Text(root, width=80, height=20,
                               bg="#11111b", fg="#a6e3a1",
                               font=("Consolas", 8), relief="flat",
                               state="disabled")
        self.log_box.grid(row=7, column=0, columnspan=3, padx=12, pady=(0,12))

        sb = tk.Scrollbar(root, command=self.log_box.yview)
        sb.grid(row=7, column=3, sticky="ns", pady=(0,12))
        self.log_box.config(yscrollcommand=sb.set)

        self.process = None

    def browse_gst(self):
        start = self.gst_var.get() or os.path.expanduser("~\\Downloads")
        folder = filedialog.askdirectory(title="Select GST Automation folder", initialdir=start)
        if folder:
            self.gst_var.set(folder)

    def browse_it(self):
        start = self.it_var.get() or os.path.expanduser("~\\Downloads")
        folder = filedialog.askdirectory(title="Select IT Download folder", initialdir=start)
        if folder:
            self.it_var.set(folder)

    def log(self, text):
        self.log_box.config(state="normal")
        self.log_box.insert("end", text)
        self.log_box.see("end")
        self.log_box.config(state="disabled")
        self.root.update_idletasks()

    def run(self):
        gst = self.gst_var.get().strip()
        it  = self.it_var.get().strip()

        if not gst:
            messagebox.showerror("Missing folder", "Please select or type the GST Automation folder path.")
            return
        if not it:
            messagebox.showerror("Missing folder", "Please select or type the IT Download folder path.")
            return
        if not os.path.isdir(gst):
            messagebox.showerror("Folder not found", f"GST folder does not exist:\n{gst}")
            return
        if not os.path.isdir(it):
            messagebox.showerror("Folder not found", f"IT folder does not exist:\n{it}")
            return
        if not os.path.isfile(RUN_ALL):
            messagebox.showerror("Error", f"run_all.py not found in:\n{SCRIPT_DIR}")
            return

        save_prefs(gst, it)

        self.log(f"GST folder : {gst}\n")
        self.log(f"IT  folder : {it}\n")
        self.log("─" * 60 + "\n")

        env = os.environ.copy()
        env["PYTHONIOENCODING"]  = "utf-8"
        env["PYTHONUNBUFFERED"]  = "1"
        env["RPR_GST_FOLDER"]    = gst
        env["RPR_IT_FOLDER"]     = it
        env["RPR_GUI_MODE"]      = "1"
        env["RPR_GST_FOLDER_NUM"]= "1"

        cmd = [
            sys.executable, RUN_ALL,
            "--offline-choice", "12",
            "--gst-folder", gst,
            "--it-folder",  it,
            "--skip-tally",
        ]

        def stream():
            try:
                self.process = subprocess.Popen(
                    cmd,
                    cwd=SCRIPT_DIR,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,   # <-- KEY: no terminal input possible
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                )
                for line in self.process.stdout:
                    self.root.after(0, self.log, line)
                self.process.wait()
                rc = self.process.returncode
                if rc == 0:
                    self.root.after(0, self.log, "\n✅  ALL DONE — check your Downloads folder.\n")
                    self.root.after(0, messagebox.showinfo,
                                   "Done", "Reports generated successfully!\nCheck your Downloads folder.")
                else:
                    self.root.after(0, self.log, f"\n❌  Exited with code {rc}\n")
                    self.root.after(0, messagebox.showerror,
                                   "Error", f"run_all.py exited with code {rc}.\nSee log above.")
            except Exception as e:
                self.root.after(0, self.log, f"\nERROR: {e}\n")

        threading.Thread(target=stream, daemon=True).start()


def main():
    root = tk.Tk()
    app = FolderPickerApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
