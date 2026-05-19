"""
================================================================================
  ONLINE GST NUMBER CHECKER v1.0
  ===================================================================
  Verify GST numbers online using GST portal
  Get company details: Name, PAN, Status, Address, etc.

  FEATURES:
  • Check if GST number is valid
  • Fetch company legal name
  • Fetch trade name
  • Get registration status
  • Get PAN from GSTIN
  • Get address details (if available)
  • Bulk check multiple GSTINs
  • Export results to Excel

  USAGE:
  ─────
  python check_gst_online.py 33AABCT1234C1ZX
  python check_gst_online.py --file gstin_list.txt
  python check_gst_online.py --bulk 33AABCT1234C1ZX 33AABCT1234C1ZY

================================================================================
"""

import json
import sys
import time
import argparse
from pathlib import Path
from datetime import datetime

try:
    import requests
except ImportError:
    print("ERROR: requests module not found")
    print("Install with: pip install requests")
    sys.exit(1)

# Try to import optional modules
try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    EXCEL_AVAILABLE = True
except ImportError:
    EXCEL_AVAILABLE = False


class GSTChecker:
    """Check GST numbers online using GST portal API."""
    
    # GST Portal API endpoints
    ENDPOINTS = [
        "https://services.gst.gov.in/services/api/search/gstin?gstin={gstin}",
        "https://www.gst.gov.in/util/rest/toolkit/searchTax?gstin={gstin}",
    ]
    
    # HTTP headers (mimic browser)
    HEADERS = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/148.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-IN,en-GB;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://services.gst.gov.in/services/searchtp",
    }
    
    def __init__(self, verbose=False):
        """Initialize GST checker."""
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.timeout = 15
    
    def check_gstin(self, gstin):
        """
        Check a single GSTIN online.
        
        Returns:
            dict: {
                'gstin': string,
                'valid': bool,
                'legal_name': string,
                'trade_name': string,
                'status': string,
                'pan': string,
                'state': string,
                'address': string,
                'error': string (if failed),
                'source': 'api' or 'error'
            }
        """
        gstin = str(gstin).strip().upper()
        
        # Validate GSTIN format
        if not self._validate_gstin_format(gstin):
            return {
                'gstin': gstin,
                'valid': False,
                'error': 'Invalid GSTIN format (should be 15 alphanumeric)',
                'source': 'validation'
            }
        
        # Try each endpoint
        for endpoint in self.ENDPOINTS:
            try:
                url = endpoint.format(gstin=gstin)
                if self.verbose:
                    print(f"  Trying: {endpoint[:50]}...")
                
                response = self.session.get(url, timeout=self.timeout, verify=False)
                
                if response.status_code == 200:
                    data = response.json()
                    result = self._parse_response(gstin, data)
                    if result and result.get('legal_name'):
                        if self.verbose:
                            print(f"    ✓ Found: {result['legal_name']}")
                        return result
            
            except Exception as e:
                if self.verbose:
                    print(f"    ⚠ Error: {str(e)[:50]}")
                continue
        
        return {
            'gstin': gstin,
            'valid': False,
            'error': 'GST number not found or portal unreachable',
            'source': 'api_failed'
        }
    
    def check_bulk(self, gstins, show_progress=True):
        """
        Check multiple GSTINs.
        
        Parameters:
            gstins: list of GSTIN strings
            show_progress: show progress for each GSTIN
        
        Returns:
            list of result dicts
        """
        results = []
        total = len(gstins)
        
        for i, gstin in enumerate(gstins, 1):
            if show_progress:
                print(f"[{i}/{total}] Checking {gstin}... ", end="", flush=True)
            
            result = self.check_gstin(gstin)
            results.append(result)
            
            if show_progress:
                if result.get('legal_name'):
                    print(f"✓ {result['legal_name']}")
                else:
                    print(f"⚠ {result.get('error', 'Not found')}")
            
            # Rate limiting (be polite to portal)
            if i < total:
                time.sleep(0.5)
        
        return results
    
    def _validate_gstin_format(self, gstin):
        """Validate GSTIN format (15 chars, alphanumeric)."""
        if not gstin or len(gstin) != 15:
            return False
        return all(c.isalnum() or c.isspace() for c in gstin.replace(' ', ''))
    
    def _parse_response(self, gstin, raw_response):
        """Parse GST portal API response."""
        try:
            # Handle different response structures
            info = raw_response
            
            if isinstance(raw_response, dict):
                if "taxpayerInfo" in raw_response:
                    info = raw_response["taxpayerInfo"]
                elif "data" in raw_response:
                    info = raw_response["data"]
                elif "result" in raw_response:
                    info = raw_response["result"]
            
            # Handle list response
            if isinstance(info, list) and len(info) > 0:
                info = info[0]
            
            if not isinstance(info, dict):
                return None
            
            # Extract fields (try multiple names)
            legal_name = self._get_field(info, 
                "lgnm", "legalName", "legal_name", "tradeName", "trade_name",
                "name", "taxpayerName", "tp_name")
            
            trade_name = self._get_field(info,
                "tradeNam", "tradeName", "trade_name", "trade", "tp_trade_name")
            
            status = self._get_field(info,
                "sts", "status", "tp_status", "taxpayerStatus")
            
            # Extract PAN (first 10 chars of GSTIN are PAN+1 state code char)
            pan = ""
            if len(gstin) >= 12:
                pan = gstin[2:12]  # Extract PAN portion
            
            # Extract state code (chars 0-1)
            state_code = gstin[0:2] if len(gstin) >= 2 else ""
            state_name = self._get_state_name(state_code)
            
            # Get address if available
            address = self._get_field(info,
                "address", "addr", "shop_number", "location")
            
            return {
                'gstin': gstin,
                'valid': True,
                'legal_name': legal_name,
                'trade_name': trade_name if trade_name != legal_name else "",
                'status': status,
                'pan': pan,
                'state_code': state_code,
                'state_name': state_name,
                'address': address,
                'source': 'api',
                'fetched_at': datetime.now().isoformat()
            }
        
        except Exception as e:
            return None
    
    def _get_field(self, info, *field_names):
        """Try to get field from info dict, trying multiple names."""
        for field in field_names:
            value = info.get(field, "")
            if value and isinstance(value, str):
                return value.strip()
        return ""
    
    def _get_state_name(self, state_code):
        """Get state name from 2-digit code."""
        states = {
            '01': 'ANDAMAN AND NICOBAR',
            '02': 'ANDHRA PRADESH',
            '03': 'ARUNACHAL PRADESH',
            '04': 'ASSAM',
            '05': 'BIHAR',
            '06': 'CHANDIGARH',
            '07': 'CHHATTISGARH',
            '08': 'DADRA AND NAGAR HAVELI',
            '09': 'DAMAN AND DIU',
            '10': 'DELHI',
            '11': 'PUDUCHERRY',
            '12': 'GOA',
            '13': 'GUJARAT',
            '14': 'HARYANA',
            '15': 'HIMACHAL PRADESH',
            '16': 'JHARKHAND',
            '17': 'JAMMU AND KASHMIR',
            '18': 'KARNATAKA',
            '19': 'KERALA',
            '20': 'LAKSHADWEEP',
            '21': 'MADHYA PRADESH',
            '22': 'MAHARASHTRA',
            '23': 'MANIPUR',
            '24': 'MEGHALAYA',
            '25': 'MIZORAM',
            '26': 'NAGALAND',
            '27': 'ODISHA',
            '28': 'PUDUCHERRY',
            '29': 'PUNJAB',
            '30': 'RAJASTHAN',
            '31': 'SIKKIM',
            '32': 'TAMIL NADU',
            '33': 'TELANGANA',
            '34': 'TRIPURA',
            '35': 'UTTAR PRADESH',
            '36': 'UTTARAKHAND',
            '37': 'WEST BENGAL',
            '38': 'OTHER TERRITORY',
        }
        return states.get(state_code, 'Unknown')
    
    def export_excel(self, results, output_file=None):
        """Export results to Excel file."""
        if not EXCEL_AVAILABLE:
            print("ERROR: openpyxl not installed")
            print("Install with: pip install openpyxl")
            return None
        
        output_file = output_file or Path("GST_Check_Results.xlsx")
        
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "GST Results"
        
        # Headers
        headers = ["GSTIN", "Valid", "Legal Name", "Trade Name", "Status", 
                   "PAN", "State", "Address", "Fetched At", "Error/Notes"]
        
        # Header styling
        header_font = Font(bold=True, color="FFFFFF", name="Calibri", size=11)
        header_fill = PatternFill(start_color="1F3864", end_color="1F3864", fill_type="solid")
        header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        
        for col, header in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col)
            cell.value = header
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = header_alignment
        
        # Column widths
        widths = [18, 8, 35, 30, 12, 13, 18, 30, 20, 25]
        for col, width in enumerate(widths, 1):
            ws.column_dimensions[chr(64 + col)].width = width
        
        # Data rows
        border = Border(
            left=Side(style='thin'),
            right=Side(style='thin'),
            top=Side(style='thin'),
            bottom=Side(style='thin')
        )
        
        for row_idx, result in enumerate(results, 2):
            values = [
                result.get('gstin', ''),
                'Yes' if result.get('valid') else 'No',
                result.get('legal_name', ''),
                result.get('trade_name', ''),
                result.get('status', ''),
                result.get('pan', ''),
                result.get('state_name', ''),
                result.get('address', ''),
                result.get('fetched_at', '').split('T')[0] if result.get('fetched_at') else '',
                result.get('error', '')
            ]
            
            # Alternate row colors
            row_fill = PatternFill(start_color="F2F2F2", end_color="F2F2F2", fill_type="solid") \
                if row_idx % 2 == 0 else PatternFill(start_color="FFFFFF", end_color="FFFFFF", fill_type="solid")
            
            for col, value in enumerate(values, 1):
                cell = ws.cell(row=row_idx, column=col)
                cell.value = value
                cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
                cell.border = border
                cell.fill = row_fill
        
        # Freeze header row
        ws.freeze_panes = "A2"
        
        # Auto-filter
        ws.auto_filter.ref = f"A1:{chr(64 + len(headers))}{len(results) + 1}"
        
        wb.save(str(output_file))
        return output_file


def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="Check GST numbers online using GST portal",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python check_gst_online.py 33AABCT1234C1ZX
  python check_gst_online.py --file gstin_list.txt
  python check_gst_online.py --bulk 33AABCT1234C1ZX 33AABCT1234C1ZY
  python check_gst_online.py --bulk 33AABCT1234C1ZX --export results.xlsx
        """
    )
    
    parser.add_argument('gstin', nargs='?', help='Single GSTIN to check')
    parser.add_argument('--file', help='File with list of GSTINs (one per line)')
    parser.add_argument('--bulk', nargs='+', help='Check multiple GSTINs')
    parser.add_argument('--export', help='Export results to Excel file')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    args = parser.parse_args()
    
    # Disable SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    checker = GSTChecker(verbose=args.verbose)
    
    # Determine what to check
    gstins_to_check = []
    
    if args.bulk:
        gstins_to_check = args.bulk
    elif args.file:
        try:
            with open(args.file, 'r') as f:
                gstins_to_check = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"ERROR: File {args.file} not found")
            sys.exit(1)
    elif args.gstin:
        gstins_to_check = [args.gstin]
    else:
        parser.print_help()
        sys.exit(1)
    
    if not gstins_to_check:
        print("ERROR: No GSTINs to check")
        sys.exit(1)
    
    print(f"\n{'='*80}")
    print(f"  ONLINE GST NUMBER CHECKER")
    print(f"  Checking {len(gstins_to_check)} GSTIN(s)")
    print(f"{'='*80}\n")
    
    # Check all
    results = checker.check_bulk(gstins_to_check, show_progress=True)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*80}")
    
    valid_count = sum(1 for r in results if r.get('valid'))
    invalid_count = len(results) - valid_count
    
    print(f"  Total checked: {len(results)}")
    print(f"  Valid GSTINs: {valid_count}")
    print(f"  Invalid/Not found: {invalid_count}")
    
    # Show details
    print(f"\n{'GSTIN':<20} {'Valid':<8} {'Name':<40} {'Status':<12}")
    print("-" * 80)
    
    for result in results:
        gstin = result.get('gstin', '')
        valid = 'Yes' if result.get('valid') else 'No'
        name = result.get('legal_name', result.get('error', 'Not found'))[:37]
        status = result.get('status', '')[:10]
        
        print(f"{gstin:<20} {valid:<8} {name:<40} {status:<12}")
    
    # Export if requested
    if args.export:
        print(f"\n  Exporting to Excel... ", end="", flush=True)
        excel_file = checker.export_excel(results, args.export)
        if excel_file:
            print(f"✓ Saved: {excel_file}")
    
    print(f"\n{'='*80}\n")
    
    return 0 if valid_count > 0 else 1


if __name__ == '__main__':
    sys.exit(main())
