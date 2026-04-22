@echo off
cd /d "%~dp0"
echo Installing dependencies...
pip install flask requests yfinance pandas -q
echo Starting dashboard...
python stock_dashboard.py
pause
