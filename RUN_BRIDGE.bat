@echo off
title GST Browser Bridge — Auto-Installer
color 1F

echo.
echo  ============================================================
echo   GST Browser Bridge — PC Client
echo   Connects your browser to the Render server for downloads
echo  ============================================================
echo.

:: ── Check Python ─────────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python is not installed!
    echo.
    echo  Download Python from: https://www.python.org/downloads/
    echo  IMPORTANT: Check "Add Python to PATH" during install.
    echo.
    pause & exit /b 1
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo   Python: %%v

:: ── Upgrade pip silently ──────────────────────────────────────────
echo.
echo  Checking pip...
python -m pip install --upgrade pip -q --no-warn-script-location 2>nul

:: ── Install websockets ────────────────────────────────────────────
echo  Installing websockets...
python -m pip install websockets -q --no-warn-script-location
if errorlevel 1 (
    echo  WARNING: websockets install may have had issues
) else (
    echo  OK: websockets ready
)

:: ── Install playwright ────────────────────────────────────────────
echo  Installing playwright...
python -m pip install playwright -q --no-warn-script-location
if errorlevel 1 (
    echo  WARNING: playwright install may have had issues
) else (
    echo  OK: playwright ready
)

:: ── Install Chromium browser (only if marker file missing) ────────
set MARKER=%USERPROFILE%\.playwright_chromium_installed
if not exist "%MARKER%" (
    echo.
    echo  Installing Chromium browser (one-time, ~150 MB^)...
    echo  Please wait — this may take 2-5 minutes...
    python -m playwright install chromium
    if errorlevel 1 (
        echo  WARNING: Chromium install failed.
        echo  Try manually: playwright install chromium
    ) else (
        echo "" > "%MARKER%"
        echo  OK: Chromium installed
    )
) else (
    echo  OK: Chromium already installed
)

:: ── Check browser_bridge.py exists ───────────────────────────────
echo.
if not exist "%~dp0browser_bridge.py" (
    echo  ERROR: browser_bridge.py not found!
    echo  Make sure browser_bridge.py is in the same folder as this bat file.
    echo  Folder: %~dp0
    pause & exit /b 1
)

:: ── Run the bridge ────────────────────────────────────────────────
echo  ============================================================
echo   All dependencies ready. Starting Browser Bridge...
echo  ============================================================
echo.

cd /d "%~dp0"
python browser_bridge.py
set EXIT=%errorlevel%

echo.
echo  ============================================================
if %EXIT% equ 0 (
    echo  Bridge closed normally.
) else (
    echo  Bridge exited with code: %EXIT%
)
echo  ============================================================
echo.
pause
