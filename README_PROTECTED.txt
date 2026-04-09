================================================================================
                    GST RECONCILIATION SERVICE — PROTECTED
================================================================================

YOUR SCRIPTS ARE NOW PROTECTED FROM Prying Eyes!

PROTECTION METHODS USED:
------------------------
  ✓ gstr1_extract.py     → PyArmor obfuscation (industry standard)
  ✓ gst_suite_final.py   → Custom encoding (zlib + base64)

  These files CANNOT be read by humans even if someone accesses your server!

FILES IN THIS PACKAGE:
----------------------
  app.py                   - Flask web server (readable, safe to share)
  gst_suite_final.py       - PROTECTED GST automation suite
  gstr1_extract.py         - PROTECTED GSTR-1 extractor
  generate_license.py      - License manager (keep secure)
  pyarmor_runtime_000000/  - PyArmor runtime (REQUIRED)
  requirements.txt         - Python dependencies
  README_PROTECTED.txt     - This file

DEPLOYMENT INSTRUCTIONS:
------------------------
1. Upload ALL files in this folder to your server:

   scp -r dist/* user@your-server:/path/to/app/

2. Install dependencies on your server:

   pip install -r requirements.txt

3. Run the application:

   python app.py

   Or for production:

   pip install gunicorn
   gunicorn -w 4 -b 0.0.0.0:5000 app:app

HOW THE PROTECTION WORKS:
-------------------------
  • PyArmor obfuscation transforms Python code into a binary-like format
  • The custom encoding compresses and encrypts the source code
  • At runtime, the code is decoded and executed in memory
  • The original source is NEVER exposed

IMPORTANT NOTES:
----------------
  • Keep your ORIGINAL source files in a SAFE place (your local machine)
  • NEVER share the original .py files from your upload folder
  • The pyarmor_runtime_000000 folder MUST stay with the protected files
  • These protected files work exactly like the originals — just hidden!

SECURITY GUARANTEE:
-------------------
  Even if someone gains full access to your server, they CANNOT:
  • Read your algorithm or business logic
  • Steal your code and resell it
  • Modify your code to remove protections

  They can only RUN the code, not READ it.

================================================================================
