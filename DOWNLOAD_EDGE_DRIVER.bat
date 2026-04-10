@echo off
title Download Edge WebDriver
echo.
echo  ══════════════════════════════════════════════════
echo   EDGE WEBDRIVER DOWNLOAD HELPER
echo  ══════════════════════════════════════════════════
echo.
echo  Step 1: Finding your Edge version...
for /f "tokens=*" %%i in ('reg query "HKEY_CURRENT_USER\Software\Microsoft\Edge\BLBeacon" /v version 2^>nul') do (
    echo  Your Edge version: %%i
)
echo.
echo  Step 2: Open this URL in your browser to download:
echo  https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/
echo.
echo  Step 3: Download the version matching YOUR Edge version
echo.
echo  Step 4: Extract msedgedriver.exe
echo.
echo  Step 5: Copy msedgedriver.exe to this folder:
echo  %~dp0
echo.
echo  Step 6: Run RUN_ME.bat again
echo.
start https://developer.microsoft.com/en-us/microsoft-edge/tools/webdriver/
pause
