@echo off
title GST Suite FINAL - AY 2025-26
color 1F

echo.
echo  ============================================================
echo   GST COMPLETE SUITE - FINAL - AY 2025-26
echo   Downloads : GSTR-1, GSTR-1A, GSTR-2B, GSTR-2A, GSTR-3B
echo   Tax Liab  : FY 2024-25 AND FY 2025-26 (single navigation)
echo   Recon     : GSTR3B-vs-R2A  and  GSTR3B-vs-R1
echo   Monthly   : Separate sheet per month (Apr-24 to Mar-25)
echo   Summary   : Company Overall Total sheet
echo  ============================================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found!
    echo  Download: https://www.python.org/downloads/
    echo  IMPORTANT: Tick "Add Python to PATH" during install.
    pause & exit /b 1
)
for /f "tokens=*" %%v in ('python --version 2^>^&1') do echo   Python: %%v

:: Install packages
echo.
echo  Installing required packages...
python -m pip install --upgrade pip --quiet --no-warn-script-location 2>nul
python -m pip install selenium webdriver-manager pandas openpyxl pdfplumber --quiet --no-warn-script-location 2>nul
echo  Done: selenium, webdriver-manager, pandas, openpyxl, pdfplumber

:: Check Edge WebDriver
echo.
if exist "%~dp0msedgedriver.exe" (
    echo  Edge WebDriver : FOUND
) else (
    echo  Edge WebDriver : NOT found
    echo.
    echo  Steps to get Edge WebDriver:
    echo    1. Open Edge - go to: edge://settings/help
    echo    2. Note your Edge version (e.g. 146.0.xxxx)
    echo    3. Download msedgedriver.exe from:
    echo       https://developer.microsoft.com/microsoft-edge/tools/webdriver/
    echo    4. Copy msedgedriver.exe to THIS folder (same as RUN_ME.bat)
    echo.
)

echo.
echo  HOW IT WORKS:
echo  ─────────────────────────────────────────────────────────
echo    1. Browser opens - username and password auto-filled
echo    2. PAUSE - you type CAPTCHA - press ENTER here
echo    3. Script downloads selected returns automatically
echo    4. Tax Liability: navigates ONCE, downloads FY 2024-25
echo       then FY 2025-26 (staying on same page, just re-select FY)
echo    5. Annual Reconciliation Excel is generated:
echo       - Summary_Report (12-month overview)
echo       - GSTR1_Sales_Detail + Invoice_Detail + CDNR
echo       - GSTR2B_ITC_Detail + GSTR2A_Purchase_Detail
echo       - GSTR3B_Status (PDF extraction)
echo       - GSTR3B_vs_R1_Recon + GSTR3B_vs_R2A_Recon
echo       - Monthwise_Reconciliation + Annual_Summary
echo       - Apr-24...Mar-25 (12 separate monthly sheets)
echo       - Company_Overall_Total (grand summary)
echo    6. Creates Master Report across all clients
echo  ─────────────────────────────────────────────────────────
echo.

cd /d "%~dp0"
python gst_suite_final.py
set EXIT_CODE=%errorlevel%

echo.
echo  ============================================================
if %EXIT_CODE% equ 0 (
    echo  All done successfully!
) else (
    echo  Exited with code: %EXIT_CODE%
    echo  Check log in: Downloads\GST_Automation\
)
echo  ============================================================
echo.
pause
