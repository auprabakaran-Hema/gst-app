"""
GSTR-1 Extractor - Extracts GSTR-1 data from ZIP files to Excel
Compatible with GST India JSON format
"""
import json
import zipfile
import os
from pathlib import Path
from datetime import datetime
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import re


def extract_gstr1_to_excel(input_dir: str, output_file: str, customer_names: dict = None):
    """
    Extract GSTR-1 data from ZIP files in input_dir and save to Excel
    
    Args:
        input_dir: Directory containing GSTR-1 ZIP files
        output_file: Path to output Excel file
        customer_names: Optional dict mapping GSTIN to customer names
    """
    input_path = Path(input_dir)
    customer_names = customer_names or {}
    
    # Find all GSTR-1 ZIP files
    gstr1_zips = sorted(input_path.glob("GSTR1_*.zip"))
    if not gstr1_zips:
        gstr1_zips = sorted(input_path.glob("*GSTR*.zip"))
    
    if not gstr1_zips:
        raise FileNotFoundError("No GSTR-1 ZIP files found in input directory")
    
    print(f"Found {len(gstr1_zips)} GSTR-1 ZIP files")
    
    # Data containers for different sections
    b2b_invoices = []
    b2b_items = []
    hsn_summary = []
    b2cs_data = []
    b2cl_data = []
    credit_notes = []
    debit_notes = []
    exports = []
    nil_rated = []
    amendments = []
    doc_summary = []
    
    for zip_file in gstr1_zips:
        month_year = _extract_month_year(zip_file.name)
        print(f"Processing: {zip_file.name} ({month_year})")
        
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                for json_name in zf.namelist():
                    if json_name.endswith('.json'):
                        with zf.open(json_name) as jf:
                            data = json.load(jf)
                            _process_gstr1_data(data, month_year, customer_names,
                                               b2b_invoices, b2b_items, hsn_summary,
                                               b2cs_data, b2cl_data, credit_notes,
                                               debit_notes, exports, nil_rated,
                                               amendments, doc_summary)
        except Exception as e:
            print(f"  Warning: Error processing {zip_file.name}: {e}")
            continue
    
    # Create Excel workbook with all sheets
    print(f"Creating Excel file: {output_file}")
    wb = openpyxl.Workbook()
    wb.remove(wb.active)  # Remove default sheet
    
    # Create sheets
    _create_sheet(wb, "B2B Invoices", b2b_invoices, [
        "Month", "GSTIN", "Customer Name", "Invoice No", "Invoice Date",
        "Invoice Value", "Taxable Value", "IGST", "CGST", "SGST", "Total Tax"
    ])
    
    _create_sheet(wb, "B2B Item Detail", b2b_items, [
        "Month", "GSTIN", "Customer Name", "Invoice No", "Item Description",
        "HSN Code", "Quantity", "UQC", "Taxable Value", "IGST Rate", "CGST Rate",
        "SGST Rate", "IGST Amt", "CGST Amt", "SGST Amt"
    ])
    
    _create_sheet(wb, "HSN Summary", hsn_summary, [
        "Month", "HSN Code", "Description", "UQC", "Quantity",
        "Taxable Value", "IGST Amt", "CGST Amt", "SGST Amt", "Total Tax"
    ])
    
    _create_sheet(wb, "B2CS", b2cs_data, [
        "Month", "POS", "Supply Type", "Taxable Value", "IGST", "CGST", "SGST"
    ])
    
    _create_sheet(wb, "B2CL", b2cl_data, [
        "Month", "POS", "Invoice No", "Invoice Date", "Invoice Value",
        "Taxable Value", "IGST", "CGST", "SGST"
    ])
    
    _create_sheet(wb, "Credit Notes", credit_notes, [
        "Month", "GSTIN", "Customer Name", "Note No", "Note Date",
        "Original Invoice", "Note Value", "Taxable Value", "IGST", "CGST", "SGST"
    ])
    
    _create_sheet(wb, "Debit Notes", debit_notes, [
        "Month", "GSTIN", "Customer Name", "Note No", "Note Date",
        "Original Invoice", "Note Value", "Taxable Value", "IGST", "CGST", "SGST"
    ])
    
    _create_sheet(wb, "Exports", exports, [
        "Month", "Export Type", "Invoice No", "Invoice Date",
        "Invoice Value", "Taxable Value", "Port Code", "Shipping Bill No"
    ])
    
    _create_sheet(wb, "Nil Rated", nil_rated, [
        "Month", "Supply Type", "Nil Rated", "Exempted", "Non-GST"
    ])
    
    _create_sheet(wb, "Amendments", amendments, [
        "Month", "Section", "Original No", "Original Date",
        "Revised No", "Revised Date", "Taxable Value", "IGST", "CGST", "SGST"
    ])
    
    _create_sheet(wb, "Doc Summary", doc_summary, [
        "Month", "Doc Type", "From Serial", "To Serial", "Total Count", "Cancelled"
    ])
    
    # Create Master Summary sheet
    _create_master_summary(wb, "Master Summary", {
        "B2B Invoices": b2b_invoices,
        "Credit Notes": credit_notes,
        "Exports": exports,
        "HSN Summary": hsn_summary
    })
    
    wb.save(output_file)
    print(f"Excel file saved: {output_file}")
    return output_file


def _extract_month_year(filename):
    """Extract month and year from filename"""
    months = ["January", "February", "March", "April", "May", "June",
              "July", "August", "September", "October", "November", "December"]
    
    for month in months:
        if month.lower() in filename.lower():
            # Try to find year
            year_match = re.search(r'20\d{2}', filename)
            if year_match:
                return f"{month} {year_match.group()}"
            return month
    
    # Try pattern GSTR1_MMYYYY
    match = re.search(r'GSTR1[_-]?(\d{2})(\d{4})', filename, re.IGNORECASE)
    if match:
        month_num = int(match.group(1))
        year = match.group(2)
        if 1 <= month_num <= 12:
            return f"{months[month_num-1]} {year}"
    
    return "Unknown"


def _process_gstr1_data(data, month_year, customer_names, b2b_invoices, b2b_items,
                        hsn_summary, b2cs_data, b2cl_data, credit_notes, debit_notes,
                        exports, nil_rated, amendments, doc_summary):
    """Process GSTR-1 JSON data and populate lists"""
    
    # Process B2B invoices
    if 'b2b' in data:
        for b2b in data['b2b']:
            ctin = b2b.get('ctin', '')
            cust_name = customer_names.get(ctin, '')
            for inv in b2b.get('inv', []):
                inv_data = _process_invoice(inv, month_year, ctin, cust_name)
                b2b_invoices.append(inv_data)
                
                # Process items
                for item in inv.get('itms', []):
                    item_data = _process_item(item, month_year, ctin, cust_name, inv.get('inum', ''))
                    b2b_items.append(item_data)
    
    # Process HSN summary
    if 'hsnsum' in data:
        for hsn in data['hsnsum'].get('hsn', []):
            hsn_data = {
                'Month': month_year,
                'HSN Code': hsn.get('hsn_sc', ''),
                'Description': hsn.get('desc', ''),
                'UQC': hsn.get('uqc', ''),
                'Quantity': hsn.get('qty', 0),
                'Taxable Value': float(hsn.get('val', 0)),
                'IGST Amt': float(hsn.get('iamt', 0)),
                'CGST Amt': float(hsn.get('camt', 0)),
                'SGST Amt': float(hsn.get('samt', 0)),
                'Total Tax': float(hsn.get('iamt', 0)) + float(hsn.get('camt', 0)) + float(hsn.get('samt', 0))
            }
            hsn_summary.append(hsn_data)
    
    # Process B2CS (B2C Small)
    if 'b2cs' in data:
        for b2c in data['b2cs']:
            b2cs_data.append({
                'Month': month_year,
                'POS': b2c.get('pos', ''),
                'Supply Type': b2c.get('sply_ty', ''),
                'Taxable Value': float(b2c.get('txval', 0)),
                'IGST': float(b2c.get('iamt', 0)),
                'CGST': float(b2c.get('camt', 0)),
                'SGST': float(b2c.get('samt', 0))
            })
    
    # Process B2CL (B2C Large)
    if 'b2cl' in data:
        for b2c in data['b2cl']:
            for inv in b2c.get('inv', []):
                b2cl_data.append({
                    'Month': month_year,
                    'POS': b2c.get('pos', ''),
                    'Invoice No': inv.get('inum', ''),
                    'Invoice Date': inv.get('idt', ''),
                    'Invoice Value': float(inv.get('val', 0)),
                    'Taxable Value': sum(float(it.get('txval', 0)) for it in inv.get('itms', [])),
                    'IGST': sum(float(it.get('iamt', 0)) for it in inv.get('itms', [])),
                    'CGST': sum(float(it.get('camt', 0)) for it in inv.get('itms', [])),
                    'SGST': sum(float(it.get('samt', 0)) for it in inv.get('itms', []))
                })
    
    # Process Credit Notes
    if 'cdnr' in data:
        for cdnr in data['cdnr']:
            ctin = cdnr.get('ctin', '')
            cust_name = customer_names.get(ctin, '')
            for note in cdnr.get('nt', []):
                credit_notes.append({
                    'Month': month_year,
                    'GSTIN': ctin,
                    'Customer Name': cust_name,
                    'Note No': note.get('nt_num', ''),
                    'Note Date': note.get('nt_dt', ''),
                    'Original Invoice': note.get('inum', ''),
                    'Note Value': float(note.get('val', 0)),
                    'Taxable Value': sum(float(it.get('txval', 0)) for it in note.get('itms', [])),
                    'IGST': sum(float(it.get('iamt', 0)) for it in note.get('itms', [])),
                    'CGST': sum(float(it.get('camt', 0)) for it in note.get('itms', [])),
                    'SGST': sum(float(it.get('samt', 0)) for it in note.get('itms', []))
                })
    
    # Process Exports
    if 'exp' in data:
        for exp in data['exp']:
            for inv in exp.get('inv', []):
                exports.append({
                    'Month': month_year,
                    'Export Type': exp.get('exp_typ', ''),
                    'Invoice No': inv.get('inum', ''),
                    'Invoice Date': inv.get('idt', ''),
                    'Invoice Value': float(inv.get('val', 0)),
                    'Taxable Value': sum(float(it.get('txval', 0)) for it in inv.get('itms', [])),
                    'Port Code': exp.get('port_code', ''),
                    'Shipping Bill No': exp.get('sbnum', '')
                })
    
    # Process Nil Rated
    if 'nil' in data:
        for nil in data['nil'].get('inv', []):
            nil_rated.append({
                'Month': month_year,
                'Supply Type': nil.get('sply_ty', ''),
                'Nil Rated': float(nil.get('nil_amt', 0)),
                'Exempted': float(nil.get('expt_amt', 0)),
                'Non-GST': float(nil.get('ngsup_amt', 0))
            })


def _process_invoice(inv, month_year, ctin, cust_name):
    """Process a single invoice"""
    items = inv.get('itms', [])
    taxable_value = sum(float(it.get('txval', 0)) for it in items)
    igst = sum(float(it.get('iamt', 0)) for it in items)
    cgst = sum(float(it.get('camt', 0)) for it in items)
    sgst = sum(float(it.get('samt', 0)) for it in items)
    
    return {
        'Month': month_year,
        'GSTIN': ctin,
        'Customer Name': cust_name,
        'Invoice No': inv.get('inum', ''),
        'Invoice Date': inv.get('idt', ''),
        'Invoice Value': float(inv.get('val', 0)),
        'Taxable Value': taxable_value,
        'IGST': igst,
        'CGST': cgst,
        'SGST': sgst,
        'Total Tax': igst + cgst + sgst
    }


def _process_item(item, month_year, ctin, cust_name, inv_no):
    """Process a single item"""
    itm_det = item.get('itm_det', {})
    return {
        'Month': month_year,
        'GSTIN': ctin,
        'Customer Name': cust_name,
        'Invoice No': inv_no,
        'Item Description': itm_det.get('desc', ''),
        'HSN Code': itm_det.get('hsn_sc', ''),
        'Quantity': itm_det.get('qty', 0),
        'UQC': itm_det.get('uqc', ''),
        'Taxable Value': float(itm_det.get('txval', 0)),
        'IGST Rate': itm_det.get('rt', 0),
        'CGST Rate': itm_det.get('rt', 0) / 2 if itm_det.get('rt') else 0,
        'SGST Rate': itm_det.get('rt', 0) / 2 if itm_det.get('rt') else 0,
        'IGST Amt': float(itm_det.get('iamt', 0)),
        'CGST Amt': float(itm_det.get('camt', 0)),
        'SGST Amt': float(itm_det.get('samt', 0))
    }


def _create_sheet(wb, title, data, headers):
    """Create a worksheet with data"""
    ws = wb.create_sheet(title)
    
    # Header styling
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_align = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Write headers
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
    
    # Write data
    for row_idx, row_data in enumerate(data, 2):
        for col_idx, header in enumerate(headers, 1):
            value = row_data.get(header, '') if isinstance(row_data, dict) else row_data[col_idx-1]
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.border = thin_border
            
            # Format numbers
            if isinstance(value, (int, float)) and header not in ['Quantity', 'HSN Code']:
                cell.number_format = '#,##0.00'
    
    # Auto-adjust column widths
    for col in range(1, len(headers) + 1):
        max_length = 0
        for row in range(1, min(len(data) + 2, 100)):  # Limit to first 100 rows for performance
            cell = ws.cell(row=row, column=col)
            if cell.value:
                max_length = max(max_length, len(str(cell.value)))
        ws.column_dimensions[get_column_letter(col)].width = min(max_length + 2, 30)
    
    # Freeze header row
    ws.freeze_panes = "A2"


def _create_master_summary(wb, title, data_dict):
    """Create master summary sheet"""
    ws = wb.create_sheet(title, 0)  # Insert at beginning
    
    header_font = Font(bold=True, color="FFFFFF", size=12)
    header_fill = PatternFill(start_color="1F4E78", end_color="1F4E78", fill_type="solid")
    subheader_font = Font(bold=True, color="FFFFFF")
    subheader_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    
    row = 1
    
    # Title
    ws.cell(row=row, column=1, value="GSTR-1 MASTER SUMMARY")
    ws.cell(row=row, column=1).font = Font(bold=True, size=16)
    row += 2
    
    # Summary by section
    for section_name, data in data_dict.items():
        # Section header
        ws.cell(row=row, column=1, value=section_name)
        ws.cell(row=row, column=1).font = header_font
        ws.cell(row=row, column=1).fill = header_fill
        row += 1
        
        # Count and totals
        count = len(data)
        total_value = sum(float(r.get('Invoice Value', r.get('Taxable Value', 0))) for r in data) if data else 0
        total_tax = sum(float(r.get('Total Tax', r.get('IGST', 0) + r.get('CGST', 0) + r.get('SGST', 0))) for r in data) if data else 0
        
        ws.cell(row=row, column=1, value="Count:")
        ws.cell(row=row, column=2, value=count)
        ws.cell(row=row, column=2).number_format = '#,##0'
        row += 1
        
        ws.cell(row=row, column=1, value="Total Value:")
        ws.cell(row=row, column=2, value=total_value)
        ws.cell(row=row, column=2).number_format = '#,##0.00'
        row += 1
        
        ws.cell(row=row, column=1, value="Total Tax:")
        ws.cell(row=row, column=2, value=total_tax)
        ws.cell(row=row, column=2).number_format = '#,##0.00'
        row += 2
    
    # Auto-adjust column widths
    ws.column_dimensions['A'].width = 25
    ws.column_dimensions['B'].width = 20


# For direct execution
if __name__ == "__main__":
    import sys
    if len(sys.argv) >= 3:
        extract_gstr1_to_excel(sys.argv[1], sys.argv[2])
    else:
        print("Usage: python gstr1_extract.py <input_dir> <output_file>")
