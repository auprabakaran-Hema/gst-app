#!/bin/bash
# GST Reconciliation Service - Deployment Script

echo "=========================================="
echo "  GST Service Deployment"
echo "=========================================="
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed."
    exit 1
fi

echo "Installing dependencies..."
pip install -q -r requirements.txt

echo ""
echo "Starting server..."
echo "Access the app at: http://localhost:5000"
echo ""

python3 app.py
