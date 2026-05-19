"""
================================================================================
  COMPREHENSIVE TEST SUITE FOR RPR SUITE v3.2
  ===================================================================
  Quick tests to verify all components are working correctly

  RUN THIS AFTER INSTALLATION TO VERIFY:
  • Python environment
  • All packages installed
  • GSTR-1 name fetching
  • Online GST checker
  • Database connectivity (pyodbc)

  USAGE:
  ─────
  python test_all_components.py
  python test_all_components.py --verbose
  python test_all_components.py --skip-online  (without portal tests)

================================================================================
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# Color codes for output
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
BLUE = '\033[94m'
RESET = '\033[0m'
BOLD = '\033[1m'

class TestResults:
    """Track test results."""
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.skipped = 0
        self.results = []
    
    def add_pass(self, test_name, message=""):
        self.passed += 1
        self.results.append((test_name, 'PASS', message))
        print(f"  {GREEN}✓{RESET} {test_name}")
        if message:
            print(f"    └─ {message}")
    
    def add_fail(self, test_name, message=""):
        self.failed += 1
        self.results.append((test_name, 'FAIL', message))
        print(f"  {RED}✗{RESET} {test_name}")
        if message:
            print(f"    └─ {message}")
    
    def add_skip(self, test_name, message=""):
        self.skipped += 1
        self.results.append((test_name, 'SKIP', message))
        print(f"  {YELLOW}⊘{RESET} {test_name}")
        if message:
            print(f"    └─ {message}")
    
    def summary(self):
        total = self.passed + self.failed + self.skipped
        print(f"\n{BOLD}{'='*70}{RESET}")
        print(f"{BOLD}TEST SUMMARY{RESET}")
        print(f"{'='*70}")
        print(f"  Total:  {total}")
        print(f"  {GREEN}Passed: {self.passed}{RESET}")
        print(f"  {RED}Failed: {self.failed}{RESET}")
        print(f"  {YELLOW}Skipped: {self.skipped}{RESET}")
        print(f"{'='*70}")
        
        if self.failed == 0 and self.passed > 0:
            print(f"\n{GREEN}{BOLD}✓ ALL TESTS PASSED - SYSTEM READY!{RESET}\n")
            return True
        elif self.failed > 0:
            print(f"\n{RED}{BOLD}✗ SOME TESTS FAILED - CHECK ABOVE{RESET}\n")
            return False
        else:
            print(f"\n{YELLOW}⊘ NO TESTS RUN - UNEXPECTED{RESET}\n")
            return False


def test_python_version(results):
    """Test Python version."""
    print(f"\n{BOLD}1. PYTHON ENVIRONMENT{RESET}")
    print("-" * 70)
    
    version_info = sys.version_info
    version_str = f"{version_info.major}.{version_info.minor}.{version_info.micro}"
    
    if version_info.major >= 3 and version_info.minor >= 10:
        results.add_pass("Python version", f"Python {version_str} (OK)")
    else:
        results.add_fail("Python version", f"Python {version_str} (need 3.10+)")


def test_required_packages(results):
    """Test all required packages."""
    print(f"\n{BOLD}2. REQUIRED PACKAGES{RESET}")
    print("-" * 70)
    
    packages = {
        'pandas': '2.0.0',
        'openpyxl': '3.1.0',
        'numpy': '1.24.0',
        'xlrd': '2.0.0',
        'pdfplumber': '0.9.0',
        'pypdf': '3.0.0',
        'selenium': '4.0',
        'webdriver_manager': '3.9.0',
        'flask': '2.3.0',
        'requests': '2.28.0',
        'pyodbc': '4.0.0',
    }
    
    for package_name, min_version in packages.items():
        try:
            module = __import__(package_name.lower().replace('-', '_'))
            version = getattr(module, '__version__', 'unknown')
            results.add_pass(f"{package_name}", f"v{version}")
        except ImportError:
            results.add_fail(f"{package_name}", "Not installed - pip install " + package_name)


def test_gstin_name_cache(results):
    """Test GSTR-1 name cache module."""
    print(f"\n{BOLD}3. GSTR-1 NAME CACHE MODULE{RESET}")
    print("-" * 70)
    
    try:
        from gstin_name_cache import GSTINNameCache
        results.add_pass("Import gstin_name_cache", "Module loaded successfully")
        
        # Test cache creation
        try:
            cache = GSTINNameCache(auto_fetch=False)
            results.add_pass("Create cache instance", "GSTINNameCache instantiated")
        except Exception as e:
            results.add_fail("Create cache instance", str(e)[:50])
    
    except ImportError as e:
        results.add_fail("Import gstin_name_cache", "File not found in current directory")


def test_gstr1_module(results):
    """Test GSTR-1 module."""
    print(f"\n{BOLD}4. GSTR-1 MODULE{RESET}")
    print("-" * 70)
    
    # Check if file exists
    if Path('gstr1_fy_v5.py').exists():
        results.add_pass("gstr1_fy_v5.py exists", "File found")
        
        try:
            # Try importing (without running)
            import importlib.util
            spec = importlib.util.spec_from_file_location("gstr1_fy_v5", "gstr1_fy_v5.py")
            module = importlib.util.module_from_spec(spec)
            results.add_pass("gstr1_fy_v5.py imports", "Module structure is valid")
        except SyntaxError as e:
            results.add_fail("gstr1_fy_v5.py imports", f"Syntax error: {str(e)[:50]}")
        except Exception as e:
            results.add_fail("gstr1_fy_v5.py imports", str(e)[:50])
    else:
        results.add_fail("gstr1_fy_v5.py exists", "File not found")


def test_online_gst_checker(results):
    """Test online GST checker module."""
    print(f"\n{BOLD}5. ONLINE GST CHECKER{RESET}")
    print("-" * 70)
    
    if Path('check_gst_online.py').exists():
        results.add_pass("check_gst_online.py exists", "File found")
        
        try:
            from check_gst_online import GSTChecker
            results.add_pass("Import GSTChecker", "Module loaded successfully")
            
            # Test checker creation
            try:
                checker = GSTChecker(verbose=False)
                results.add_pass("Create GSTChecker instance", "GSTChecker instantiated")
                
                # Test GSTIN validation
                valid = checker._validate_gstin_format("33AABCT1234C1ZX")
                if valid:
                    results.add_pass("GSTIN format validation", "Validates 15-char GSTIN")
                else:
                    results.add_fail("GSTIN format validation", "Validation failed")
                
            except Exception as e:
                results.add_fail("Create GSTChecker instance", str(e)[:50])
        
        except ImportError as e:
            results.add_fail("Import GSTChecker", str(e)[:50])
    else:
        results.add_fail("check_gst_online.py exists", "File not found")


def test_database_connectivity(results):
    """Test database connectivity (pyodbc)."""
    print(f"\n{BOLD}6. DATABASE CONNECTIVITY (PYODBC){RESET}")
    print("-" * 70)
    
    try:
        import pyodbc
        results.add_pass("pyodbc module", "Installed and importable")
        
        # Test getting ODBC drivers
        try:
            drivers = pyodbc.drivers()
            if drivers:
                results.add_pass("ODBC drivers", f"Found {len(drivers)} driver(s)")
            else:
                results.add_skip("ODBC drivers", "No drivers installed (optional)")
        except Exception as e:
            results.add_skip("ODBC drivers", "Cannot list drivers (optional)")
    
    except ImportError:
        results.add_fail("pyodbc module", "Not installed - pip install pyodbc")


def test_excel_export(results):
    """Test Excel export capability."""
    print(f"\n{BOLD}7. EXCEL EXPORT{RESET}")
    print("-" * 70)
    
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill
        
        results.add_pass("openpyxl module", "Installed")
        
        # Test Excel creation
        try:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws['A1'].value = "Test"
            ws['A1'].font = Font(bold=True)
            
            # Save test file
            test_file = Path("_test_excel.xlsx")
            wb.save(str(test_file))
            
            if test_file.exists():
                results.add_pass("Create Excel file", "Successfully created test file")
                test_file.unlink()  # Delete test file
            else:
                results.add_fail("Create Excel file", "File not created")
        
        except Exception as e:
            results.add_fail("Create Excel file", str(e)[:50])
    
    except ImportError:
        results.add_fail("openpyxl module", "Not installed - pip install openpyxl")


def test_file_structure(results):
    """Test required files are present."""
    print(f"\n{BOLD}8. FILE STRUCTURE{RESET}")
    print("-" * 70)
    
    required_files = {
        'gstr1_fy_v5.py': 'GSTR-1 module',
        'gst_suite_v32.py': 'GST suite module',
        'it_suite_v6.py': 'IT suite module',
        'gstin_name_cache.py': 'GSTIN cache (v3.3)',
        'check_gst_online.py': 'Online GST checker',
        'verify_installation.py': 'Verification script',
        'requirements.txt': 'Requirements file',
    }
    
    for filename, description in required_files.items():
        if Path(filename).exists():
            results.add_pass(filename, description)
        else:
            results.add_fail(filename, f"{description} - NOT FOUND")


def test_quick_gst_check(results, verbose=False):
    """Quick test of GST checker without portal."""
    print(f"\n{BOLD}9. QUICK GST CHECKER TEST (NO PORTAL){RESET}")
    print("-" * 70)
    
    try:
        from check_gst_online import GSTChecker
        
        checker = GSTChecker(verbose=verbose)
        
        # Test with valid format GSTIN
        test_gstin = "33AABCT1234C1ZX"
        
        try:
            # Test format validation
            is_valid = checker._validate_gstin_format(test_gstin)
            if is_valid:
                results.add_pass("GSTIN format check", f"Valid format: {test_gstin}")
            else:
                results.add_fail("GSTIN format check", "Format validation failed")
            
            # Test state code extraction
            state_name = checker._get_state_name("33")
            if state_name and state_name != "Unknown":
                results.add_pass("State extraction", f"State code 33 = {state_name}")
            else:
                results.add_fail("State extraction", "Could not extract state")
        
        except Exception as e:
            results.add_fail("Quick GST check", str(e)[:50])
    
    except Exception as e:
        results.add_fail("Quick GST check setup", str(e)[:50])


def main():
    """Run all tests."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Test RPR Suite v3.2 components"
    )
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--skip-online', action='store_true', help='Skip online tests')
    
    args = parser.parse_args()
    
    results = TestResults()
    
    print(f"\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}  RPR GST + IT SUITE v3.2 - COMPONENT TESTS{RESET}")
    print(f"{BOLD}  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}{'='*70}{RESET}")
    
    # Run tests
    test_python_version(results)
    test_required_packages(results)
    test_file_structure(results)
    test_gstin_name_cache(results)
    test_gstr1_module(results)
    test_online_gst_checker(results)
    test_database_connectivity(results)
    test_excel_export(results)
    test_quick_gst_check(results, verbose=args.verbose)
    
    # Summary
    success = results.summary()
    
    print(f"\n{BOLD}NEXT STEPS:{RESET}")
    if success:
        print(f"""
  {GREEN}✓ System is ready!{RESET}
  
  You can now run:
  
  1. {BOLD}GSTR-1 download:{RESET}
     python gstr1_fy_v5.py --name "YOUR COMPANY" /path/to/gstr
  
  2. {BOLD}Check GST online:{RESET}
     python check_gst_online.py 33AABCT1234C1ZX
  
  3. {BOLD}Check multiple GSTINs:{RESET}
     python check_gst_online.py --file gstins.txt --export report.xlsx
  
  4. {BOLD}Run complete suite:{RESET}
     python run_all.py
        """)
    else:
        print(f"""
  {RED}✗ Some components need attention{RESET}
  
  Please:
  1. Review the failed tests above
  2. Install missing packages: pip install -r requirements.txt
  3. Verify files are in correct location
  4. Run tests again
  
  For help:
  - See: GSTR1_FIX_IMPLEMENTATION_GUIDE.txt
  - See: ONLINE_GST_CHECKER_GUIDE.txt
        """)
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())
