#!/bin/bash
cd "$(dirname "$0")"
echo "📦 Installing dependencies..."
pip3 install flask requests pandas "multitasking==0.0.9" yfinance --user -q 2>/dev/null \
  || pip install flask requests pandas "multitasking==0.0.9" yfinance --user -q 2>/dev/null
echo "🚀 Starting dashboard — your browser will open automatically..."
python3 stock_dashboard.py || python stock_dashboard.py
