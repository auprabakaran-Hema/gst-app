"""
_make_rpr_icon.py
─────────────────
Generates RPR_icon.ico in the same folder as this script.
Called automatically by LAUNCH_SUITE.bat on first run.

Manual run:  python _make_rpr_icon.py
Requires:    pip install Pillow
"""
import os, sys

def _ensure_pillow():
    try:
        from PIL import Image
        return True
    except ImportError:
        import subprocess
        subprocess.call([sys.executable, "-m", "pip", "install",
                         "Pillow", "--quiet"])
        try:
            from PIL import Image   # noqa: F401
            return True
        except ImportError:
            return False

if not _ensure_pillow():
    print("Pillow install failed — icon will not be generated.")
    sys.exit(0)

from PIL import Image, ImageDraw, ImageFont

def make_rpr_icon(size):
    img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    d   = ImageDraw.Draw(img)

    BG   = (15,  25,  50,  255)   # deep navy
    GRN  = (0,  200, 120, 255)    # emerald green  (matches bat color 0A)
    GOLD = (255, 215,  80, 255)   # gold accent
    WHT  = (255, 255, 255, 255)
    BLK  = (0,   0,   0,  200)

    # ── Rounded background ────────────────────────────────────────────────
    r   = max(4, size // 7)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).rounded_rectangle(
        [0, 0, size-1, size-1], radius=r, fill=255)
    bg = Image.new("RGBA", (size, size), BG)
    img.paste(bg, mask=mask)

    # ── Green top bar ─────────────────────────────────────────────────────
    bar_h = max(6, size // 8)
    bar   = Image.new("RGBA", (size, size), (0,0,0,0))
    ImageDraw.Draw(bar).rounded_rectangle(
        [0, 0, size-1, bar_h + r], radius=r, fill=GRN)
    ImageDraw.Draw(bar).rectangle(
        [0, r, size-1, bar_h + r], fill=GRN)
    img.alpha_composite(bar)

    # ── Bottom green accent line ──────────────────────────────────────────
    lw  = max(2, size // 20)
    pad = size // 12
    ImageDraw.Draw(img).rectangle(
        [pad, size-pad-lw, size-pad, size-pad], fill=GRN)

    # ── "RPR" text ────────────────────────────────────────────────────────
    ts = max(8, int(size * 0.38))
    FONT_PATH = r"C:\Windows\Fonts\arialbd.ttf"       # Windows
    FALLBACKS = [
        r"C:\Windows\Fonts\Arial Bold.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",  # Linux
        "/System/Library/Fonts/Helvetica.ttc",                   # macOS
    ]
    font = None
    for fp in [FONT_PATH] + FALLBACKS:
        if os.path.exists(fp):
            try:
                font = ImageFont.truetype(fp, ts)
                break
            except Exception:
                pass
    if font is None:
        font = ImageFont.load_default()

    td = ImageDraw.Draw(img)
    cy = int(size * 0.51)
    # drop-shadow
    for dx, dy in [(-1,-1),(1,-1),(-1,1),(1,1)]:
        td.text((size//2+dx, cy+dy), "RPR", font=font, fill=BLK, anchor="mm")
    td.text((size//2, cy), "RPR", font=font, fill=WHT, anchor="mm")

    # ── "GST • IT" subtitle (≥48 px only) ────────────────────────────────
    if size >= 48:
        ss = max(6, int(size * 0.13))
        sfont = None
        for fp in [FONT_PATH] + FALLBACKS:
            if os.path.exists(fp):
                try:
                    sfont = ImageFont.truetype(fp, ss)
                    break
                except Exception:
                    pass
        if sfont is None:
            sfont = ImageFont.load_default()
        td.text((size//2, int(size*0.80)), "GST  \u2022  IT",
                font=sfont, fill=GOLD, anchor="mm")

    return img


if __name__ == "__main__":
    out_dir = os.path.dirname(os.path.abspath(__file__))
    out_ico = os.path.join(out_dir, "RPR_icon.ico")

    sizes  = [16, 24, 32, 48, 64, 128, 256]
    frames = [make_rpr_icon(s) for s in sizes]

    frames[0].save(
        out_ico, format="ICO",
        sizes=[(s, s) for s in sizes],
        append_images=frames[1:],
    )
    print(f"RPR_icon.ico saved → {out_ico}")
