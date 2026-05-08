@echo off
title FT Suite - Install All Requirements
color 0A
echo.
echo ============================================================
echo   FT GST + IT Automation Suite v10.6
echo   Installing all required packages
echo   Please wait... (may take 3-5 minutes first time)
echo ============================================================
echo.

REM Check Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo   ERROR: Python is not installed!
    echo.
    echo   Please install Python first:
    echo   1. Go to https://www.python.org/downloads/
    echo   2. Download Python 3.11 or newer
    echo   3. During install, tick "Add Python to PATH"
    echo   4. Then run this file again
    echo.
    pause
    exit /b 1
)

echo   Python found:
python --version
echo.

REM Upgrade pip first
echo   [Step 1/8] Upgrading pip...
python -m pip install --upgrade pip --quiet
echo   Done.

REM Install each package
echo   [Step 2/8] Installing pandas  (Excel data handling)...
pip install pandas --quiet
echo   Done.

echo   [Step 3/8] Installing openpyxl  (Excel file reading/writing)...
pip install openpyxl --quiet
echo   Done.

echo   [Step 4/8] Installing numpy  (Number calculations)...
pip install numpy --quiet
echo   Done.

echo   [Step 5/8] Installing pdfplumber + pypdf  (PDF reading/unlocking)...
pip install pdfplumber pypdf --quiet
echo   Done.

echo   [Step 6/8] Installing selenium  (Browser automation)...
pip install selenium --quiet
echo   Done.

echo   [Step 7/8] Installing webdriver-manager  (Auto Chrome driver)...
pip install webdriver-manager --quiet
echo   Done.

echo   [Step 8/8] Installing flask + requests  (Web app support)...
pip install flask requests --quiet
echo   Done.

echo.
echo ============================================================
echo   ALL PACKAGES INSTALLED SUCCESSFULLY
echo ============================================================
echo.
echo   You can now run:
echo     - run_all.py    (full pipeline)
echo     - gst_suite_v31.py    (GST downloads)
echo     - it_suite_v6.py      (IT downloads)
echo.
echo   Or if you compiled to EXE already, just run the EXE files.
echo.

REM Verify key packages installed correctly
echo   Verifying installation...
python -c "import pandas; print('   pandas        OK  version:', pandas.__version__)"
python -c "import openpyxl; print('   openpyxl      OK  version:', openpyxl.__version__)"
python -c "import pdfplumber; print('   pdfplumber    OK')"
python -c "import pypdf; print('   pypdf         OK')"
python -c "import selenium; print('   selenium      OK  version:', selenium.__version__)"
python -c "import webdriver_manager; print('   webdriver-mgr OK')"
python -c "import flask; print('   flask         OK  version:', flask.__version__)"
echo.
echo   All checks done!
echo.
pause
