@echo off
title RPR GST + IT Suite Launcher
color 0A

REM ── Set UTF-8 so Python Unicode characters don't crash ───────────────────
chcp 65001 >nul 2>&1
set PYTHONIOENCODING=utf-8
set PYTHONUNBUFFERED=1

REM ── Check Python is installed ─────────────────────────────────────────────
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo  ERROR: Python is not installed!
    echo.
    echo  Please install Python first:
    echo    1. Go to https://www.python.org/downloads/
    echo    2. Download Python 3.11 or newer
    echo    3. During install, tick "Add Python to PATH"
    echo    4. Then double-click this file again
    echo.
    pause
    exit /b 1
)

REM ── Install tkinter if missing (usually included with Python) ─────────────
python -c "import tkinter" >nul 2>&1
if errorlevel 1 (
    echo  Note: tkinter not found. Installing...
    pip install tk --quiet
)

REM ── Generate RPR.ico if it does not exist yet ─────────────────────────────
if not exist "%~dp0RPR_icon.ico" (
    echo  Generating RPR icon...
    python "%~dp0_make_rpr_icon.py" >nul 2>&1
)

REM ── Create Desktop shortcut with RPR icon (runs once) ─────────────────────
if not exist "%USERPROFILE%\Desktop\RPR GST-IT Suite.lnk" (
    echo  Creating Desktop shortcut with RPR icon...
    powershell -NoProfile -ExecutionPolicy Bypass -Command ^
        "$ws = New-Object -ComObject WScript.Shell;" ^
        "$sc = $ws.CreateShortcut([Environment]::GetFolderPath('Desktop') + '\RPR GST-IT Suite.lnk');" ^
        "$sc.TargetPath  = '%~f0';" ^
        "$sc.IconLocation = '%~dp0RPR_icon.ico,0';" ^
        "$sc.WorkingDirectory = '%~dp0';" ^
        "$sc.Description = 'RPR GST and IT Suite Launcher';" ^
        "$sc.Save()"
    echo  Shortcut created on Desktop.
)

REM ── Launch the GUI ────────────────────────────────────────────────────────
echo  Starting RPR Suite Launcher...
python "%~dp0RPR_Suite_Launcher.py"

REM If there was an error, show it
if errorlevel 1 (
    echo.
    echo  Something went wrong. Please check the error above.
    pause
)
