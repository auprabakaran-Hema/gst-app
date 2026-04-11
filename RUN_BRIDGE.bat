@echo off
title GST Browser Bridge
color 1F

echo.
echo  ============================================================
echo   GST Browser Bridge - PC Client Launcher
echo  ============================================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found!
    echo.
    echo  Download from: https://www.python.org/downloads/
    echo  IMPORTANT: Check "Add Python to PATH" during install
    echo.
    pause & exit /b 1
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo  Python: %%v

:: Check browser_bridge.py exists
if not exist "%~dp0browser_bridge.py" (
    echo.
    echo  ERROR: browser_bridge.py not found in this folder!
    echo  Folder: %~dp0
    echo.
    pause & exit /b 1
)

echo.
echo  Starting browser bridge...
echo  (Required packages will be installed automatically)
echo.
echo  ============================================================

cd /d "%~dp0"
python browser_bridge.py

echo.
echo  ============================================================
echo  Bridge closed.
echo  ============================================================
echo.
pause
