# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['run_all.py'],
    pathex=[],
    binaries=[],
    datas=[('gst_suite_v32.py', '.'), ('it_suite_v6.py', '.'), ('master_bridge.py', '.'), ('build_gst_it_comparison.py', '.'), ('gstr2b_extractor_v2.py', '.'), ('it_recon_engine.py', '.'), ('gstr1_fy_v5.py', '.'), ('gstin_lookup.py', '.'), ('gstin_name_cache.py', '.'), ('gstr1_tally_vs_json.py', '.'), ('gstin_name_cache.json', '.'), ('clients.xlsx', '.'), ('Client_Manager_Secure_AY2025-26.xlsx', '.')],
    hiddenimports=['pandas', 'openpyxl', 'numpy', 'pdfplumber', 'pypdf', 'selenium', 'selenium.webdriver', 'selenium.webdriver.chrome', 'selenium.webdriver.chrome.webdriver', 'selenium.webdriver.chrome.service', 'selenium.webdriver.chrome.options', 'selenium.webdriver.edge.service', 'selenium.webdriver.edge.options', 'selenium.webdriver.common.by', 'selenium.webdriver.support.ui', 'selenium.webdriver.support.expected_conditions', 'selenium.webdriver.common.action_chains', 'webdriver_manager', 'webdriver_manager.chrome'],
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
    name='run_all',
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
