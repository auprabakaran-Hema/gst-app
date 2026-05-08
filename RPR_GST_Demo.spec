# -*- mode: python ; coding: utf-8 -*-
from PyInstaller.utils.hooks import collect_submodules

hiddenimports = ['flask', 'werkzeug', 'pandas', 'openpyxl', 'numpy', 'pdfplumber', 'pypdf']
hiddenimports += collect_submodules('flask')
hiddenimports += collect_submodules('werkzeug')


a = Analysis(
    ['demo_app.py'],
    pathex=[],
    binaries=[],
    datas=[('gst_suite_v32.py', '.'), ('gstin_name_cache.json', '.')],
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='RPR_GST_Demo',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
