# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['gst_suite_v32.py'],
    pathex=[],
    binaries=[],
    datas=[('license_manager.py', '.'), ('gstin_lookup.py', '.'), ('gstin_name_cache.py', '.'), ('gstin_name_cache.json', '.'), ('gstr1_fy_v5.py', '.'), ('clients.xlsx', '.'), ('Client_Manager_Secure_AY2025-26.xlsx', '.')],
    hiddenimports=['license_manager', 'pandas', 'openpyxl', 'pdfplumber', 'pypdf', 'selenium', 'selenium.webdriver', 'selenium.webdriver.chrome', 'selenium.webdriver.chrome.webdriver', 'selenium.webdriver.chrome.service', 'selenium.webdriver.chrome.options', 'selenium.webdriver.edge.service', 'selenium.webdriver.edge.options', 'selenium.webdriver.common.by', 'selenium.webdriver.support.ui', 'selenium.webdriver.support.expected_conditions', 'selenium.webdriver.common.action_chains', 'webdriver_manager', 'webdriver_manager.chrome'],
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
    name='gst_suite',
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
