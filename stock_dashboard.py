#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════╗
║   📊  Jaclyn's Live Stock Dashboard                  ║
║   Powered by Yahoo Finance  •  Auto-refreshes 5min  ║
╠══════════════════════════════════════════════════════╣
║  SETUP (one time):                                   ║
║    pip install flask requests                        ║
║                                                      ║
║  RUN:                                                ║
║    python stock_dashboard.py                         ║
║                                                      ║
║  Then open:  http://localhost:8080                   ║
╚══════════════════════════════════════════════════════╝
"""

import sys, subprocess

# ── Auto-install dependencies ─────────────────────────────────────────────────
# Install in correct order — multitasking must be pinned for Python 3.8 compat
_DEPS = [
    ("flask",            "flask"),
    ("requests",         "requests"),
    ("pandas",           "pandas"),
    ("multitasking",     "multitasking==0.0.9"),
    ("yfinance",         "yfinance"),
]
for import_name, install_spec in _DEPS:
    try:
        __import__(import_name)
    except (ImportError, TypeError):
        print(f"Installing {install_spec}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", install_spec,
                               "--user", "-q"])

from flask import Flask, jsonify, request, render_template_string
import json, time, threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import yfinance as yf

app = Flask(__name__)

# ── Tickers & Metadata ────────────────────────────────────────────────────────

TICKERS = [
    "NVDA", "META", "NFLX", "GOOG", "MSFT", "AMZN", "AAPL",
    "BLK", "KKR", "BX", "JPM", "GS",
    "XOM", "RCL", "OTF",
    "L.TO", "DOL.TO",
    "VFV.TO", "ZBAL.TO", "ZEQT.TO", "MGQE.TO",
    "VDE", "ITA", "VDC", "IVV", "VTV", "QQQ", "DXJ",
    "GLD", "SLV",
]

ETF_INFO = {
    "VFV.TO":  {"name": "Vanguard S&P 500 ETF (CAD)",          "benchmark": "S&P 500"},
    "ZBAL.TO": {"name": "BMO Balanced ETF",                     "benchmark": "Balanced (60/40)"},
    "ZEQT.TO": {"name": "BMO All-Equity ETF",                   "benchmark": "Global Equity"},
    "MGQE.TO": {"name": "Mackenzie Global Quality ETF",         "benchmark": "Global Quality"},
    "VDE":     {"name": "Vanguard Energy ETF",                  "benchmark": "Energy Sector"},
    "ITA":     {"name": "iShares Aerospace & Defense ETF",      "benchmark": "Aerospace & Defense"},
    "VDC":     {"name": "Vanguard Consumer Staples ETF",        "benchmark": "Consumer Staples"},
    "IVV":     {"name": "iShares Core S&P 500 ETF",             "benchmark": "S&P 500"},
    "VTV":     {"name": "Vanguard Value ETF",                   "benchmark": "US Large-Cap Value"},
    "QQQ":     {"name": "Invesco QQQ ETF",                      "benchmark": "Nasdaq-100"},
    "DXJ":     {"name": "WisdomTree Japan Hedged Equity ETF",   "benchmark": "Japan Equity (USD-hedged)"},
    "GLD":     {"name": "SPDR Gold Shares",                     "benchmark": "Gold Spot Price"},
    "SLV":     {"name": "iShares Silver Trust",                 "benchmark": "Silver Spot Price"},
}

IS_ETF = set(ETF_INFO.keys())

SECTOR_MAP = {
    "NVDA": "Technology",      "META": "Technology",     "NFLX": "Technology",
    "GOOG": "Technology",      "MSFT": "Technology",     "AMZN": "Technology",
    "AAPL": "Technology",      "BLK": "Financials",      "KKR": "Financials",
    "BX":   "Financials",      "JPM": "Financials",      "GS":  "Financials",
    "XOM":  "Energy",          "RCL": "Consumer Discret.","OTF": "Financials",
    "L.TO": "Consumer Staples","DOL.TO": "Consumer Discret.",
}

# Target P/E multiples per sector (for Claude's price target)
PE_TARGETS = {
    "Technology": 27, "Financials": 14, "Energy": 12,
    "Consumer Discret.": 22, "Consumer Staples": 18, "default": 18,
}

# ── yfinance helpers ──────────────────────────────────────────────────────────

def _wrap(val):
    """Wrap a raw value in Yahoo Finance's {raw, fmt} format expected by analysis fns."""
    return {"raw": val} if val is not None else {}

def _info_to_summary(info):
    """Convert flat yfinance info dict → nested quoteSummary format used by analysis fns."""
    return {
        "financialData": {k: _wrap(info.get(k)) for k in (
            "revenueGrowth","grossMargins","operatingMargins","profitMargins",
            "earningsGrowth","totalRevenue","totalCash","totalDebt","debtToEquity",
            "currentRatio","returnOnEquity","returnOnAssets","freeCashflow",
            "targetMeanPrice","targetLowPrice","targetHighPrice",
        )},
        "defaultKeyStatistics": {k: _wrap(info.get(k)) for k in (
            "forwardEps","trailingEps","bookValue","priceToBook",
            "enterpriseToEbitda","enterpriseToRevenue","pegRatio",
            "trailingPE","sharesOutstanding","shortPercentOfFloat",
        )},
        "summaryDetail": {k: _wrap(info.get(k)) for k in (
            "dividendYield","beta","expenseRatio",
        )},
        "assetProfile": {
            "longBusinessSummary": info.get("longBusinessSummary",""),
            "sector":   info.get("sector",""),
            "industry": info.get("industry",""),
        },
        "recommendationTrend": {"trend": [{
            "strongBuy":  info.get("strongBuy", 0) or 0,
            "buy":        info.get("buy", 0) or 0,
            "hold":       info.get("hold", 0) or 0,
            "sell":       info.get("sell", 0) or 0,
            "strongSell": info.get("strongSell", 0) or 0,
        }]},
    }

def fetch_quotes(tickers):
    """Fetch live quote data for all tickers in parallel using yfinance fast_info."""
    result = {}
    def _one(sym):
        try:
            t  = yf.Ticker(sym)
            fi = t.fast_info
            info = t.info or {}
            price = fi.last_price
            prev  = fi.previous_close or fi.regular_market_previous_close
            chg   = (price - prev) if (price and prev) else None
            chgp  = (chg / prev * 100) if (chg is not None and prev) else None
            return sym, {
                "symbol": sym,
                "regularMarketPrice":         price,
                "regularMarketChange":         chg,
                "regularMarketChangePercent":  chgp,
                "regularMarketVolume":         getattr(fi, "last_volume", None) or info.get("volume"),
                "regularMarketOpen":           getattr(fi, "open", None),
                "regularMarketDayHigh":        getattr(fi, "day_high", None),
                "regularMarketDayLow":         getattr(fi, "day_low", None),
                "regularMarketPreviousClose":  prev,
                "marketCap":                   getattr(fi, "market_cap", None),
                "fiftyTwoWeekHigh":            getattr(fi, "year_high", None),
                "fiftyTwoWeekLow":             getattr(fi, "year_low", None),
                "trailingPE":                  info.get("trailingPE"),
                "forwardPE":                   info.get("forwardPE"),
                "averageDailyVolume3Month":    getattr(fi, "three_month_average_volume", None),
                "shortName":                   info.get("shortName", sym),
                "longName":                    info.get("longName", sym),
                "currency":                    getattr(fi, "currency", "USD"),
            }
        except Exception as e:
            print(f"[YF] quote {sym}: {e}")
            return sym, {}

    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(_one, sym): sym for sym in tickers}
        for future in as_completed(futures):
            sym, data = future.result()
            result[sym] = data
    return result

def fetch_chart(symbol, interval="1d", range_="1y"):
    """Return historical price data as {timestamp:[...], indicators:{quote:[{close:[...]}]}}"""
    period_map = {"5d":"5d","1mo":"1mo","3mo":"3mo","1y":"1y","2y":"2y","5y":"5y"}
    period = period_map.get(range_, "1y")
    try:
        hist = yf.Ticker(symbol).history(period=period, interval=interval, auto_adjust=True)
        if hist.empty:
            return None
        # Convert timezone-aware index to Unix timestamps safely
        timestamps = (hist.index.astype("int64") // 10**9).tolist()
        closes     = [round(float(v), 4) if v == v else None for v in hist["Close"]]
        return {"timestamp": timestamps, "indicators": {"quote": [{"close": closes}]}}
    except Exception as e:
        print(f"[YF] chart {symbol}: {e}")
        return None

def fetch_summary(symbol):
    """Fetch fundamental data and return in nested quoteSummary format."""
    try:
        info = yf.Ticker(symbol).info or {}
        if not info or info.get("trailingPE") is None and info.get("regularMarketPrice") is None:
            return None
        return _info_to_summary(info)
    except Exception as e:
        print(f"[YF] summary {symbol}: {e}")
        return None

def fetch_news(symbol):
    """Fetch latest news items."""
    try:
        raw = yf.Ticker(symbol).news or []
        return [_normalize_news(n) for n in raw]
    except Exception as e:
        print(f"[YF] news {symbol}: {e}")
        return []

def _normalize_news(item):
    """Handle both old (flat) and new (nested content) yfinance news formats."""
    if "content" not in item:
        return item   # old format — already correct
    c = item["content"]
    # Parse ISO pubDate → Unix timestamp
    ts = None
    try:
        from datetime import datetime, timezone
        ts = int(datetime.fromisoformat(
            c.get("pubDate","").replace("Z","+00:00")).timestamp())
    except Exception:
        pass
    url = ((c.get("canonicalUrl") or {}).get("url")
        or (c.get("clickThroughUrl") or {}).get("url","#"))
    return {
        "title":               c.get("title",""),
        "publisher":           (c.get("provider") or {}).get("displayName",""),
        "link":                url,
        "providerPublishTime": ts,
        "summary":             c.get("summary",""),
    }

# ── Analysis: Claude's Price Target ──────────────────────────────────────────

def claude_price_target(symbol, summary, quote):
    if symbol in IS_ETF:
        return None, "ETF — intrinsic value analysis not applicable."
    if not summary or not quote:
        return None, "Insufficient data available."

    fd  = summary.get("financialData", {})
    ks  = summary.get("defaultKeyStatistics", {})
    r   = lambda d, k: (d.get(k) or {}).get("raw")

    price        = quote.get("regularMarketPrice", 0) or 0
    fwd_eps      = r(ks, "forwardEps")
    trail_eps    = r(ks, "trailingEps")
    rev_growth   = r(fd, "revenueGrowth") or 0
    profit_mgn   = r(fd, "profitMargins") or 0
    fcf          = r(fd, "freeCashflow")
    shares       = r(ks, "sharesOutstanding")

    sector  = SECTOR_MAP.get(symbol, "default")
    base_pe = PE_TARGETS.get(sector, 18)

    # Growth-adjust the P/E
    if   rev_growth > 0.30: base_pe *= 1.45
    elif rev_growth > 0.15: base_pe *= 1.20
    elif rev_growth < 0.02: base_pe *= 0.85

    # Margin-adjust
    if   profit_mgn > 0.25: base_pe *= 1.10
    elif profit_mgn < 0.05: base_pe *= 0.90

    base_pe = round(base_pe, 1)

    def upside(t):
        return round((t - price) / price * 100, 1) if price else None

    if fwd_eps and fwd_eps > 0:
        target = round(fwd_eps * base_pe, 2)
        note = (f"Forward EPS ${fwd_eps:.2f} × {base_pe}x P/E "
                f"(sector: {sector}, adjusted for growth/margin profile)")
        return {"target": target, "upside": upside(target), "methodology": note,
                "current": price}, note

    if trail_eps and trail_eps > 0:
        target = round(trail_eps * base_pe * 1.10, 2)   # +10% growth haircut
        note = (f"Trailing EPS ${trail_eps:.2f} × {base_pe}x P/E "
                f"+ 10% forward growth assumption (sector: {sector})")
        return {"target": target, "upside": upside(target), "methodology": note,
                "current": price}, note

    if fcf and fcf > 0 and shares:
        fcf_ps = fcf / shares
        target = round(fcf_ps / 0.03, 2)   # 3% FCF yield target
        note = (f"FCF/share ${fcf_ps:.2f} ÷ 3% target FCF yield "
                f"(growth-stage valuation, sector: {sector})")
        return {"target": target, "upside": upside(target), "methodology": note,
                "current": price}, note

    return None, "Insufficient earnings/FCF data for a price target estimate."

# ── Analysis: Bull / Bear Case ────────────────────────────────────────────────

def bull_bear_case(symbol, summary, quote):
    if symbol in IS_ETF:
        info = ETF_INFO[symbol]
        bm   = info["benchmark"]
        return (
            [f"Broad diversified exposure to {bm} with automatic rebalancing",
             "Low-cost passive structure — no manager risk or style drift",
             "Long-term compounding through reinvested distributions",
             "Transparent holdings, daily liquidity, and no redemption gates"],
            ["No alpha generation — returns mirror the benchmark by design",
             "Full participation in market downside with no defensive positioning",
             f"Currency risk for foreign-denominated ETFs (USD/CAD exposure)",
             "Sector/geographic concentration risk depending on benchmark"]
        )

    if not summary:
        return ["Data loading — check back shortly."], ["Data loading — check back shortly."]

    fd      = summary.get("financialData", {})
    ks      = summary.get("defaultKeyStatistics", {})
    profile = summary.get("assetProfile", {})
    rec     = (summary.get("recommendationTrend", {}).get("trend") or [{}])[0]
    r       = lambda d, k: (d.get(k) or {}).get("raw")

    bull, bear = [], []
    sector = SECTOR_MAP.get(symbol, "default")

    rev_g   = r(fd, "revenueGrowth")
    earn_g  = r(fd, "earningsGrowth")
    g_mgn   = r(fd, "grossMargins")
    p_mgn   = r(fd, "profitMargins")
    dte     = r(fd, "debtToEquity")
    fcf     = r(fd, "freeCashflow")
    roe     = r(fd, "returnOnEquity")
    fwd_pe  = quote.get("forwardPE") if quote else None
    sp      = PE_TARGETS.get(sector, 18)

    # Revenue growth
    if rev_g is not None:
        if rev_g > 0.20:
            bull.append(f"Strong revenue growth of {rev_g*100:.1f}% YoY — significantly above sector peers, pointing to durable demand expansion")
        elif rev_g > 0.06:
            bull.append(f"Steady revenue growth of {rev_g*100:.1f}% YoY demonstrates consistent execution against strategic plan")
        else:
            bear.append(f"Decelerating top-line growth ({rev_g*100:.1f}% YoY) raises questions about addressable market saturation and pricing power")

    # Gross / net margins
    if g_mgn is not None:
        if g_mgn > 0.55:
            bull.append(f"Exceptional gross margin of {g_mgn*100:.1f}% reflects deep competitive moat, strong IP, and pricing power resistant to cost inflation")
        elif g_mgn > 0.30:
            bull.append(f"Healthy gross margin of {g_mgn*100:.1f}% provides meaningful buffer to invest in R&D and growth initiatives")
        elif g_mgn < 0.15:
            bear.append(f"Thin gross margins ({g_mgn*100:.1f}%) leave limited room to absorb supply chain shocks, wage inflation, or competitive price cuts")

    if p_mgn is not None:
        if p_mgn < 0:
            bear.append(f"Currently unprofitable (net margin {p_mgn*100:.1f}%) — path to profitability depends on unproven scale economics and may require dilutive financing")
        elif p_mgn > 0.20:
            bull.append(f"High net profit margin ({p_mgn*100:.1f}%) demonstrates best-in-class operating leverage and disciplined cost management")

    # Balance sheet
    if dte is not None:
        if dte < 0.4:
            bull.append(f"Conservative balance sheet (D/E: {dte:.2f}x) provides financial flexibility for M&A, buybacks, and weathering economic downturns")
        elif dte > 2.0:
            bear.append(f"Elevated leverage (D/E: {dte:.2f}x) amplifies downside risk in a higher-for-longer rate environment and limits strategic flexibility")

    # Free cash flow
    if fcf is not None:
        if fcf > 0:
            bull.append(f"Positive FCF of ${fcf/1e9:.1f}B gives management capital allocation optionality — dividends, buybacks, bolt-on acquisitions")
        else:
            bear.append(f"Negative FCF (${fcf/1e9:.1f}B) indicates the business is currently cash-consumptive; external financing or asset sales may be needed")

    # Analyst sentiment
    sb = rec.get("strongBuy", 0); b = rec.get("buy", 0)
    h  = rec.get("hold", 0);      s = rec.get("sell", 0) + rec.get("strongSell", 0)
    total = sb + b + h + s
    if total >= 5:
        pos_pct = (sb + b) / total * 100
        if pos_pct > 65:
            bull.append(f"Strong analyst conviction: {pos_pct:.0f}% of {total} covering analysts rate Buy or Strong Buy, reflecting confidence in earnings trajectory")
        elif pos_pct < 35:
            bear.append(f"Weak Street sentiment: only {pos_pct:.0f}% of {total} analysts hold Buy ratings, suggesting limited near-term catalysts")

    # Valuation
    if fwd_pe:
        if fwd_pe > sp * 1.6:
            bear.append(f"Premium valuation (forward P/E {fwd_pe:.1f}x vs ~{sp}x sector) leaves little margin of safety — any guidance miss risks a sharp de-rating")
        elif fwd_pe < sp * 0.80:
            bull.append(f"Attractive relative valuation (forward P/E {fwd_pe:.1f}x vs ~{sp}x sector average) — potential mean-reversion upside if sentiment improves")

    # ROE
    if roe and roe > 0.20:
        bull.append(f"High return on equity ({roe*100:.1f}%) indicates capital is being deployed efficiently, compounding shareholder value over time")

    # Sector-specific colour
    _sector_notes = {
        frozenset(["NVDA","META","GOOG","MSFT","AMZN","AAPL","NFLX"]): (
            "Dominant network-effect moats, platform lock-in, and massive R&D budgets create durable competitive advantages",
            "Intensifying antitrust scrutiny (DOJ/EU/FTC) could impose structural remedies or fine-based headwinds on margins"
        ),
        frozenset(["JPM","GS","BLK","KKR","BX"]): (
            "Benefits from elevated-rate environment boosting NII/carried interest; strong fee income diversification",
            "Credit cycle sensitivity — a hard landing or commercial real-estate stress could spike provisions and reduce AUM"
        ),
        frozenset(["XOM"]): (
            "Vertically integrated energy major with downstream refining providing earnings stability through commodity cycles",
            "Long-duration energy transition risk: accelerating EV adoption and renewables reduce long-term hydrocarbon demand"
        ),
        frozenset(["RCL"]): (
            "Record advance bookings and robust pricing power signal multi-year demand runway in post-pandemic leisure travel",
            "High debt load from COVID disruption plus sensitivity to fuel costs, consumer confidence, and geopolitical events"
        ),
        frozenset(["L.TO","DOL.TO"]): (
            "Resilient Canadian consumer-staples/discount franchise with strong domestic market share and pricing discipline",
            "Exposure to Canadian consumer slowdown; FX risk on USD-denominated cost inputs; limited offshore growth runway"
        ),
        frozenset(["OTF"]): (
            "BDC structure provides high dividend yield and direct exposure to middle-market credit with floating-rate loans",
            "NAV and dividend at risk in a credit downturn; leverage amplifies losses; liquidity less than public equities"
        ),
    }
    for key, (b_note, br_note) in _sector_notes.items():
        if symbol in key:
            bull.append(b_note)
            bear.append(br_note)
            break

    # Padding
    defaults_b = [
        "Management track record of consistent execution on stated strategic priorities",
        "Shareholder return programme (buybacks/dividends) provides a floor on valuation",
    ]
    defaults_br = [
        "Macro uncertainty: potential slowdown in consumer spending and elevated real interest rates",
        "Currency and geopolitical risks in key international revenue markets",
    ]
    while len(bull) < 4: bull.append(defaults_b[len(bull) % len(defaults_b)])
    while len(bear) < 4: bear.append(defaults_br[len(bear) % len(defaults_br)])

    return bull[:6], bear[:6]

# ── In-memory cache ───────────────────────────────────────────────────────────

_cache, _cache_ts = {}, {}
CACHE_TTL = 300   # 5 min default

def cached(key, fn, ttl=CACHE_TTL):
    now = time.time()
    if key in _cache and (now - _cache_ts.get(key, 0)) < ttl:
        return _cache[key]
    result = fn()
    _cache[key]    = result
    _cache_ts[key] = now
    return result

# ── Flask Routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/tickers")
def api_tickers():
    return jsonify(TICKERS)

@app.route("/api/update-tickers", methods=["POST"])
def api_update_tickers():
    global TICKERS
    data = request.get_json(force=True) or {}
    new_tickers = data.get("tickers", [])
    if new_tickers and isinstance(new_tickers, list):
        TICKERS = [t.strip().upper() for t in new_tickers if t.strip()]
        # Clear quote cache so new tickers are fetched
        _cache.pop("quotes", None)
        _cache_ts.pop("quotes", None)
        print(f"[SYNC] Tickers updated: {len(TICKERS)} tickers")
    return jsonify({"ok": True, "count": len(TICKERS)})

@app.route("/api/quotes")
def api_quotes():
    return jsonify(cached("quotes", lambda: fetch_quotes(TICKERS)))

@app.route("/api/chart/<symbol>")
def api_chart(symbol):
    iv  = request.args.get("interval", "1d")
    rng = request.args.get("range",    "1y")
    key = f"chart_{symbol}_{iv}_{rng}"
    ttl = 60 if iv in ("1m", "5m") else 300
    return jsonify(cached(key, lambda: fetch_chart(symbol, iv, rng), ttl))

@app.route("/api/details/<symbol>")
def api_details(symbol):
    def build():
        summary = fetch_summary(symbol)
        qs      = fetch_quotes([symbol])
        quote   = qs.get(symbol, {})
        target, methodology = claude_price_target(symbol, summary, quote)
        bull, bear          = bull_bear_case(symbol, summary, quote)
        fd = (summary or {}).get("financialData", {})
        r  = lambda k: (fd.get(k) or {}).get("raw")
        analyst = {"mean": r("targetMeanPrice"),
                   "low":  r("targetLowPrice"),
                   "high": r("targetHighPrice")} if summary else None
        return {"summary": summary, "quote": quote,
                "claude_target": target, "bull_case": bull, "bear_case": bear,
                "analyst_target": analyst}
    return jsonify(cached(f"details_{symbol}", build, 300))

@app.route("/api/news/<symbol>")
def api_news(symbol):
    return jsonify(cached(f"news_{symbol}", lambda: fetch_news(symbol), 600))

# ── HTML / JS / CSS ───────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>📊 Jaclyn's Live Stock Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3.0.0/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
:root{--bg:#0d0f16;--card:#161924;--card2:#1e2235;--border:#252a3e;--text:#ffffff;--muted:#b0bcd4;--green:#22c55e;--red:#ef4444;--blue:#3b82f6;--purple:#a855f7;--gold:#f59e0b;--teal:#14b8a6}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;font-size:14px;line-height:1.5}
/* scrollbar */
::-webkit-scrollbar{width:5px;height:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}

/* header */
.hdr{background:var(--card);border-bottom:1px solid var(--border);padding:10px 20px;display:flex;align-items:center;justify-content:space-between;position:sticky;top:0;z-index:200}
.hdr-logo{font-size:17px;font-weight:800;background:linear-gradient(135deg,#60a5fa,#a78bfa);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.live-dot{display:inline-flex;align-items:center;gap:5px;background:rgba(34,197,94,.12);color:var(--green);font-size:10px;font-weight:700;padding:3px 9px;border-radius:20px;text-transform:uppercase;letter-spacing:.05em}
.live-dot::before{content:'';width:6px;height:6px;border-radius:50%;background:var(--green);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.hdr-right{display:flex;align-items:center;gap:10px}
.upd{font-size:11px;color:var(--muted)}
.btn{background:var(--blue);color:#fff;border:none;padding:6px 14px;border-radius:7px;cursor:pointer;font-size:12px;font-weight:700;transition:opacity .2s}
.btn:hover{opacity:.8}
.btn-ghost{background:transparent;color:var(--muted);border:1px solid var(--border);padding:6px 12px;border-radius:7px;cursor:pointer;font-size:12px;font-weight:600;transition:all .2s}
.btn-ghost:hover{border-color:var(--blue);color:var(--blue)}

/* layout */
.layout{display:flex;height:calc(100vh - 49px);overflow:hidden}
.sidebar{width:252px;min-width:252px;background:var(--card);border-right:1px solid var(--border);overflow-y:auto;display:flex;flex-direction:column}
.sb-hdr{padding:10px 14px;font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);border-bottom:1px solid var(--border);flex-shrink:0}
.tk{padding:9px 14px;cursor:pointer;border-bottom:1px solid rgba(37,42,62,.6);display:flex;justify-content:space-between;align-items:center;transition:background .12s}
.tk:hover{background:var(--card2)}
.tk.active{background:rgba(59,130,246,.12);border-left:3px solid var(--blue)}
.tk .s{font-weight:700;font-size:13px}
.tk .n{font-size:10px;color:var(--muted);margin-top:1px;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.tk .p{text-align:right;flex-shrink:0}
.tk .pv{font-weight:600;font-size:12px}
.tk .pc{font-size:11px}
.green{color:var(--green)}.red{color:var(--red)}

/* main */
.main{flex:1;overflow-y:auto;padding:20px}

/* overview */
.ov-title{font-size:22px;font-weight:800;margin-bottom:4px}
.ov-sub{color:var(--muted);font-size:13px;margin-bottom:16px}
.ov-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(178px,1fr));gap:12px}
.ov-card{background:var(--card);border:1px solid var(--border);border-radius:11px;padding:14px;cursor:pointer;transition:all .15s}
.ov-card:hover{border-color:var(--blue);transform:translateY(-2px);box-shadow:0 8px 24px rgba(0,0,0,.3)}
.ov-sym{font-weight:800;font-size:15px}
.ov-name{font-size:10px;color:var(--muted);margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ov-price{font-size:21px;font-weight:800;margin-top:8px}
.ov-chg{font-size:12px;margin-top:2px}
.ov-foot{display:flex;justify-content:space-between;margin-top:8px;font-size:10px;color:var(--muted)}
.badge{display:inline-block;font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px;text-transform:uppercase;letter-spacing:.04em}
.badge-etf{background:rgba(168,85,247,.14);color:var(--purple)}
.badge-ca{background:rgba(20,184,166,.14);color:var(--teal)}
.chg-pill{font-size:11px;font-weight:700;padding:2px 7px;border-radius:5px}

/* detail */
#detail-view{display:none}
.d-hdr{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;margin-bottom:18px;flex-wrap:wrap}
.d-sym{font-size:30px;font-weight:900;display:flex;align-items:center;gap:10px}
.d-full{color:var(--muted);font-size:14px;margin-top:3px}
.d-sector{font-size:12px;color:var(--muted);margin-top:5px}
.d-price{text-align:right}
.d-pv{font-size:34px;font-weight:900}
.d-chg{font-size:15px;margin-top:3px}
.d-vol{font-size:11px;color:var(--muted);margin-top:4px}

/* tabs */
.tabs{display:flex;gap:2px;border-bottom:1px solid var(--border);margin-bottom:18px;overflow-x:auto}
.tab{padding:8px 18px;cursor:pointer;color:var(--muted);font-weight:600;font-size:13px;border-bottom:2px solid transparent;white-space:nowrap;transition:color .15s;user-select:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--blue);border-bottom-color:var(--blue)}
.tc{display:none}.tc.active{display:block}

/* chart */
.chart-wrap{background:var(--card);border:1px solid var(--border);border-radius:11px;padding:16px;margin-bottom:14px}
.chart-controls{display:flex;gap:5px;margin-bottom:14px;flex-wrap:wrap}
.cb{padding:4px 13px;border-radius:6px;border:1px solid var(--border);background:transparent;color:var(--muted);cursor:pointer;font-size:12px;font-weight:700;transition:all .15s}
.cb:hover,.cb.active{background:var(--blue);color:#fff;border-color:var(--blue)}
canvas{max-height:300px}

/* stat cards */
.sg{display:grid;grid-template-columns:repeat(auto-fill,minmax(165px,1fr));gap:10px;margin-bottom:6px}
.sc{background:var(--card);border:1px solid var(--border);border-radius:9px;padding:12px 14px}
.sl{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:var(--muted)}
.sv{font-size:19px;font-weight:800;margin-top:4px}
.ss{font-size:10px;color:var(--muted);margin-top:2px}
.sec-title{font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin:18px 0 10px}

/* 52w range */
.rbar{height:4px;background:var(--card2);border-radius:2px;position:relative;margin-top:6px}
.rbar-fill{position:absolute;top:0;left:0;width:100%;height:100%;background:linear-gradient(90deg,var(--red),var(--gold),var(--green));border-radius:2px}
.rbar-dot{position:absolute;top:-4px;width:12px;height:12px;background:#fff;border-radius:50%;transform:translateX(-50%);box-shadow:0 0 6px rgba(0,0,0,.5)}
.rbar-labs{display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-top:3px}

/* bull/bear */
.bb-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media(max-width:640px){.bb-grid{grid-template-columns:1fr}}
.bb{background:var(--card);border:1px solid var(--border);border-radius:11px;padding:16px}
.bb.bull{border-top:3px solid var(--green)}.bb.bear{border-top:3px solid var(--red)}
.bb h3{font-size:14px;font-weight:700;margin-bottom:12px;display:flex;align-items:center;gap:7px}
.bb ul{list-style:none;display:flex;flex-direction:column;gap:7px}
.bb ul li{padding:8px 11px;border-radius:7px;font-size:12.5px;line-height:1.55}
.bb.bull ul li{background:rgba(34,197,94,.07);border-left:2px solid rgba(34,197,94,.6)}
.bb.bear ul li{background:rgba(239,68,68,.07);border-left:2px solid rgba(239,68,68,.6)}
.bb-note{font-size:11px;color:var(--muted);margin-bottom:14px;line-height:1.5;padding:10px 12px;background:var(--card);border-radius:8px;border:1px solid var(--border)}

/* price targets */
.pt-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:16px}
@media(max-width:640px){.pt-grid{grid-template-columns:1fr}}
.pt-card{background:var(--card);border:1px solid var(--border);border-radius:11px;padding:18px}
.pt-card.claude{border-top:3px solid var(--purple)}
.pt-card.analyst{border-top:3px solid var(--blue)}
.pt-lbl{font-size:10px;font-weight:800;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);margin-bottom:12px}
.pt-price{font-size:30px;font-weight:900}
.pt-range{font-size:12px;color:var(--muted);margin-top:3px}
.pt-upside{display:inline-block;font-size:13px;font-weight:700;margin-top:10px;padding:4px 11px;border-radius:6px}
.up-pill{background:rgba(34,197,94,.14);color:var(--green)}
.dn-pill{background:rgba(239,68,68,.14);color:var(--red)}
.method{font-size:11.5px;color:var(--muted);margin-top:10px;line-height:1.55;padding:9px 11px;background:var(--card2);border-radius:7px;border:1px solid var(--border)}
.warn{font-size:11px;color:var(--muted);margin-top:10px;font-style:italic;line-height:1.5}

/* ratings bar */
.rat-bar{display:flex;height:7px;border-radius:4px;overflow:hidden;margin-bottom:6px;margin-top:12px}
.rat-labs{display:flex;font-size:11px;color:var(--muted);gap:12px;flex-wrap:wrap}
.rat-labs span{display:flex;align-items:center;gap:4px}
.dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}

/* news */
.news-list{display:flex;flex-direction:column;gap:11px}
.ni{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px 16px}
.ni a{color:var(--text);text-decoration:none;font-weight:700;font-size:14px;line-height:1.4;display:block}
.ni a:hover{color:var(--blue)}
.ni-meta{font-size:11px;color:var(--muted);margin-top:5px;display:flex;gap:12px;flex-wrap:wrap}
.ni-sum{font-size:12px;color:var(--muted);margin-top:6px;line-height:1.55}

/* loading */
.loading{text-align:center;padding:40px 20px;color:var(--muted)}
.spinner{display:inline-block;width:22px;height:22px;border:2px solid var(--border);border-top-color:var(--blue);border-radius:50%;animation:spin .7s linear infinite;margin-bottom:8px}
@keyframes spin{to{transform:rotate(360deg)}}

/* description box */
.desc-box{background:var(--card);border:1px solid var(--border);border-radius:9px;padding:14px 16px;font-size:13px;color:var(--muted);line-height:1.65}

/* drag-and-drop */
.tk[draggable=true]{cursor:grab}
.tk.dragging{opacity:.3;background:var(--card2)}
.tk.drag-over{border-top:2px solid var(--blue);padding-top:7px}
.drag-handle{color:var(--muted);font-size:13px;opacity:.35;cursor:grab;flex-shrink:0;user-select:none;line-height:1}
.drag-handle:hover{opacity:.8}

/* ticker edit modal */
.modal-bg{position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:999;display:flex;align-items:center;justify-content:center;backdrop-filter:blur(4px)}
.modal{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:24px;width:480px;max-width:90vw;max-height:80vh;overflow-y:auto;box-shadow:0 20px 50px rgba(0,0,0,.5)}
.modal h2{font-size:17px;font-weight:800;margin-bottom:4px}
.modal-sub{font-size:12px;color:var(--muted);margin-bottom:16px}
.modal textarea{width:100%;min-height:100px;background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:10px;color:var(--text);font-family:monospace;font-size:13px;resize:vertical}
.modal textarea:focus{outline:none;border-color:var(--blue)}
.modal-actions{display:flex;gap:8px;margin-top:14px;justify-content:flex-end}
.modal input[type=text]{width:100%;background:var(--card2);border:1px solid var(--border);border-radius:8px;padding:8px 10px;color:var(--text);font-size:13px}
.modal input[type=text]:focus{outline:none;border-color:var(--blue)}

/* sync buttons in sidebar header */
.sb-actions{display:flex;gap:5px;padding:8px 14px;border-bottom:1px solid var(--border)}
.sb-btn{flex:1;background:var(--card2);border:1px solid var(--border);color:var(--muted);padding:5px 8px;border-radius:6px;cursor:pointer;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.04em;transition:all .15s;text-align:center}
.sb-btn:hover{border-color:var(--blue);color:var(--blue)}
.sb-btn.primary{background:rgba(59,130,246,.12);border-color:rgba(59,130,246,.3);color:var(--blue)}
</style>
</head>
<body>

<!-- ── Header ──────────────────────────────────────────────────── -->
<div class="hdr">
  <div style="display:flex;align-items:center;gap:14px">
    <span class="hdr-logo">📊 Jaclyn's Stock Dashboard</span>
    <span class="live-dot">Live</span>
  </div>
  <div class="hdr-right">
    <span class="upd" id="upd">Loading…</span>
    <button class="btn" onclick="hardRefresh()">↻ Refresh</button>
  </div>
</div>

<!-- ── Layout ──────────────────────────────────────────────────── -->
<div class="layout">

  <!-- Sidebar -->
  <div class="sidebar">
    <div class="sb-hdr">Watchlist — <span id="tk-count">30</span> tickers</div>
    <div class="sb-actions">
      <button class="sb-btn primary" onclick="syncFromYF()">↻ Sync YF</button>
      <button class="sb-btn" onclick="openEditModal()">✎ Edit List</button>
    </div>
    <div id="sb"></div>
  </div>

  <!-- Edit Tickers Modal -->
  <div id="edit-modal" class="modal-bg" style="display:none" onclick="if(event.target===this)closeModal()">
    <div class="modal">
      <h2>Edit Watchlist</h2>
      <div class="modal-sub">Drag tickers in the sidebar to reorder. Edit below to add/remove.</div>
      <label style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.05em">Tickers (one per line or comma-separated)</label>
      <textarea id="modal-tickers" spellcheck="false" style="margin-top:6px"></textarea>
      <div class="modal-actions">
        <button class="btn-ghost" onclick="closeModal()">Cancel</button>
        <button class="btn" onclick="saveTickers()">Save & Reload</button>
      </div>
      <div style="margin-top:18px;padding-top:14px;border-top:1px solid var(--border)">
        <div style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin-bottom:8px">One-Click Yahoo Finance Sync</div>
        <p style="font-size:12px;color:var(--muted);line-height:1.55;margin-bottom:10px">Drag this button to your bookmark bar. Then go to your Yahoo Finance watchlist and click it to sync instantly:</p>
        <a id="bookmarklet-link" href="#" onclick="return false"
           style="display:inline-block;background:var(--purple);color:#fff;padding:7px 14px;border-radius:7px;font-size:12px;font-weight:700;text-decoration:none;cursor:grab">
           📊 Sync to Dashboard
        </a>
        <p style="font-size:11px;color:var(--muted);margin-top:6px;font-style:italic">Tip: drag this purple button up into your Chrome bookmark bar</p>
      </div>
    </div>
  </div>

  <!-- Main -->
  <div class="main" id="main">

    <!-- Overview -->
    <div id="ov">
      <div class="ov-title">Portfolio Overview</div>
      <div class="ov-sub">Click any card for detailed analysis → price trends, stats, bull/bear thesis, targets, news</div>
      <div class="ov-grid" id="ov-grid">
        <div class="loading"><div class="spinner"></div><br>Fetching live quotes…</div>
      </div>
    </div>

    <!-- Detail -->
    <div id="detail-view">
      <button class="btn-ghost" onclick="showOv()" style="margin-bottom:14px">← Back to Overview</button>

      <div class="d-hdr">
        <div>
          <div class="d-sym" id="d-sym"></div>
          <div class="d-full" id="d-full"></div>
          <div class="d-sector" id="d-sector"></div>
        </div>
        <div class="d-price">
          <div class="d-pv" id="d-pv"></div>
          <div class="d-chg" id="d-chg"></div>
          <div class="d-vol" id="d-vol"></div>
        </div>
      </div>

      <div class="tabs">
        <div class="tab active" onclick="swTab('chart')">📈 Price Trends</div>
        <div class="tab" onclick="swTab('stats')">📊 Key Statistics</div>
        <div class="tab" onclick="swTab('bb')">⚖️ Bull vs Bear</div>
        <div class="tab" onclick="swTab('targets')">🎯 Price Targets</div>
        <div class="tab" onclick="swTab('news')">📰 Latest News</div>
      </div>

      <!-- A: Chart -->
      <div class="tc active" id="tc-chart">
        <div class="chart-wrap">
          <div class="chart-controls">
            <button class="cb" onclick="loadChart('5d','5m')">5D</button>
            <button class="cb" onclick="loadChart('1mo','1d')">1M</button>
            <button class="cb" onclick="loadChart('3mo','1d')">3M</button>
            <button class="cb active" onclick="loadChart('1y','1d')">1Y</button>
            <button class="cb" onclick="loadChart('2y','1wk')">2Y</button>
            <button class="cb" onclick="loadChart('5y','1mo')">5Y</button>
          </div>
          <canvas id="pc"></canvas>
        </div>
        <div class="sg" id="chart-meta"></div>
      </div>

      <!-- B: Stats -->
      <div class="tc" id="tc-stats">
        <div id="stats-body" class="loading"><div class="spinner"></div><br>Loading fundamentals…</div>
      </div>

      <!-- C: Bull/Bear -->
      <div class="tc" id="tc-bb">
        <div id="bb-body" class="loading"><div class="spinner"></div><br>Generating analysis…</div>
      </div>

      <!-- D: Targets -->
      <div class="tc" id="tc-targets">
        <div id="tgt-body" class="loading"><div class="spinner"></div><br>Loading price targets…</div>
      </div>

      <!-- E: News -->
      <div class="tc" id="tc-news">
        <div id="news-body" class="loading"><div class="spinner"></div><br>Fetching latest news…</div>
      </div>
    </div>

  </div><!-- /main -->
</div><!-- /layout -->

<script>
// ── Config ────────────────────────────────────────────────────────────────────
const TICKERS = """ + json.dumps(TICKERS) + r""";
const IS_ETF  = """ + json.dumps(list(IS_ETF)) + r""";
const ETF_INFO= """ + json.dumps(ETF_INFO) + r""";

let quotes = {}, cur = null, chart = null, dcache = {}, refreshTimer = null;

// ── Formatters ────────────────────────────────────────────────────────────────
const fmtN = (n,d=2,pre='$') => n==null?'N/A':`${pre}${Number(n).toLocaleString('en-US',{minimumFractionDigits:d,maximumFractionDigits:d})}`;
const fmtP = (n,d=1) => n==null?'N/A':`${n>0?'+':''}${(n*100).toFixed(d)}%`;
const fmtB = n => {
  if(n==null)return'N/A';
  const a=Math.abs(n);
  if(a>=1e12)return`$${(n/1e12).toFixed(2)}T`;
  if(a>=1e9) return`$${(n/1e9).toFixed(2)}B`;
  if(a>=1e6) return`$${(n/1e6).toFixed(2)}M`;
  return `$${n.toLocaleString()}`;
};
const fmtV = n => {
  if(n==null)return'N/A';
  if(n>=1e9)return`${(n/1e9).toFixed(2)}B`;
  if(n>=1e6)return`${(n/1e6).toFixed(2)}M`;
  if(n>=1e3)return`${(n/1e3).toFixed(0)}K`;
  return String(n);
};
const ts2date = t => new Date(t*1000).toLocaleDateString('en-US',{month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'});
const cur$ = q => q?.currency==='CAD'?'C$':'$';
const isEtf = s => IS_ETF.includes(s);
const isCA  = s => s.endsWith('.TO');

// ── Data fetching ─────────────────────────────────────────────────────────────
async function fetchQuotes(){
  try{
    const r=await fetch('/api/quotes'); quotes=await r.json();
    renderSB(); renderOv();
    document.getElementById('upd').textContent='Updated '+new Date().toLocaleTimeString();
  }catch(e){console.error(e);}
}

async function loadDetails(sym){
  if(dcache[sym]){renderDetails(sym,dcache[sym]);return;}
  ['stats-body','bb-body','tgt-body'].forEach(id=>{
    document.getElementById(id).innerHTML='<div class="loading"><div class="spinner"></div><br>Loading…</div>';
  });
  try{
    const r=await fetch(`/api/details/${sym}`);
    dcache[sym]=await r.json();
    if(cur===sym) renderDetails(sym,dcache[sym]);
  }catch(e){console.error(e);}
}

async function loadNews(sym){
  document.getElementById('news-body').innerHTML='<div class="loading"><div class="spinner"></div><br>Fetching news…</div>';
  try{
    const r=await fetch(`/api/news/${sym}`);
    const news=await r.json();
    if(cur!==sym)return;
    if(!news||!news.length){
      document.getElementById('news-body').innerHTML='<div style="color:var(--muted);text-align:center;padding:40px">No recent news found</div>';
      return;
    }
    document.getElementById('news-body').innerHTML=`<div class="news-list">${news.map(n=>`
      <div class="ni">
        <a href="${n.link||'#'}" target="_blank" rel="noopener">${n.title||'Untitled'}</a>
        <div class="ni-meta">
          <span>${n.publisher||''}</span>
          ${n.providerPublishTime?`<span>${ts2date(n.providerPublishTime)}</span>`:''}
        </div>
        ${n.summary?`<div class="ni-sum">${n.summary.substring(0,200)}…</div>`:''}
      </div>`).join('')}</div>`;
  }catch(e){
    document.getElementById('news-body').innerHTML='<div style="color:var(--muted);text-align:center;padding:40px">Could not load news</div>';
  }
}

// ── Chart ─────────────────────────────────────────────────────────────────────
async function loadChart(range,interval){
  document.querySelectorAll('.cb').forEach(b=>{
    const map={'5d':'5D','1mo':'1M','3mo':'3M','1y':'1Y','2y':'2Y','5y':'5Y'};
    b.classList.toggle('active', map[range]===b.textContent);
  });
  if(!cur)return;
  try{
    const r=await fetch(`/api/chart/${cur}?range=${range}&interval=${interval}`);
    const d=await r.json(); if(!d)return;
    const ts=d.timestamp||[];
    const cl=d.indicators?.quote?.[0]?.close||[];
    const pts=ts.map((t,i)=>({x:new Date(t*1000),y:cl[i]})).filter(p=>p.y!=null);
    if(!pts.length)return;
    const prices=pts.map(p=>p.y);
    const isUp=prices[prices.length-1]>=prices[0];
    const col=isUp?'#22c55e':'#ef4444';
    if(chart)chart.destroy();
    const ctx=document.getElementById('pc').getContext('2d');
    const grad=ctx.createLinearGradient(0,0,0,280);
    grad.addColorStop(0,isUp?'rgba(34,197,94,.18)':'rgba(239,68,68,.18)');
    grad.addColorStop(1,'rgba(0,0,0,0)');
    chart=new Chart(ctx,{
      type:'line',
      data:{datasets:[{label:cur,data:pts,borderColor:col,backgroundColor:grad,
        borderWidth:2,pointRadius:0,fill:true,tension:0.1}]},
      options:{responsive:true,maintainAspectRatio:true,
        interaction:{intersect:false,mode:'index'},
        scales:{
          x:{type:'time',
             time:{unit:range==='5d'?'hour':range==='1mo'||range==='3mo'?'week':'month'},
             grid:{color:'rgba(37,42,62,.6)'},
             ticks:{color:'#7a8499',maxTicksLimit:8}},
          y:{grid:{color:'rgba(37,42,62,.6)'},
             ticks:{color:'#7a8499',callback:v=>'$'+v.toFixed(0)}}
        },
        plugins:{
          legend:{display:false},
          tooltip:{backgroundColor:'#161924',borderColor:'#252a3e',borderWidth:1,
            titleColor:'#e2e8f0',bodyColor:'#7a8499',
            callbacks:{label:c=>`$${c.parsed.y.toFixed(2)}`}}
        }
      }
    });
  }catch(e){console.error('Chart:',e);}
}

// ── Render Sidebar ────────────────────────────────────────────────────────────
// ── Drag & Drop ──────────────────────────────────────────────────────────────
let dragSym=null;
function dgStart(e){
  dragSym=e.currentTarget.dataset.sym;
  e.currentTarget.classList.add('dragging');
  e.dataTransfer.effectAllowed='move';
  e.dataTransfer.setData('text/plain',dragSym);
}
function dgOver(e){
  e.preventDefault();
  e.dataTransfer.dropEffect='move';
  const t=e.currentTarget;
  document.querySelectorAll('.drag-over').forEach(el=>el.classList.remove('drag-over'));
  if(t.dataset.sym!==dragSym) t.classList.add('drag-over');
}
function dgDrop(e){
  e.preventDefault();
  const targetSym=e.currentTarget.dataset.sym;
  if(dragSym&&targetSym&&dragSym!==targetSym){
    const fi=TICKERS.indexOf(dragSym), ti=TICKERS.indexOf(targetSym);
    TICKERS.splice(fi,1); TICKERS.splice(ti,0,dragSym);
    persistOrder();
    renderSB(); renderOv();
  }
  document.querySelectorAll('.drag-over').forEach(el=>el.classList.remove('drag-over'));
}
function dgEnd(e){
  document.querySelectorAll('.dragging,.drag-over').forEach(el=>el.classList.remove('dragging','drag-over'));
  dragSym=null;
}
function persistOrder(){
  try{localStorage.setItem('dash_tickers',JSON.stringify(TICKERS))}catch(e){}
  fetch('/api/update-tickers',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({tickers:TICKERS})}).catch(()=>{});
  document.getElementById('tk-count').textContent=TICKERS.length;
}

function renderSB(){
  document.getElementById('tk-count').textContent=TICKERS.length;
  document.getElementById('sb').innerHTML=TICKERS.map(s=>{
    const q=quotes[s]||{};
    const p=q.regularMarketPrice;
    const c=q.regularMarketChangePercent;
    const isP=c>=0;
    const cc=cur$({currency:q.currency});
    return `<div class="tk${cur===s?' active':''}" draggable="true" data-sym="${s}"
      ondragstart="dgStart(event)" ondragover="dgOver(event)" ondrop="dgDrop(event)" ondragend="dgEnd(event)"
      onclick="showDetail('${s}')">
      <div style="display:flex;align-items:center;gap:7px">
        <span class="drag-handle">⠿</span>
        <div><div class="s">${s}</div>
          <div class="n">${(q.shortName||q.longName||s).substring(0,25)}</div></div>
      </div>
      <div class="p">
        <div class="pv">${p!=null?cc+p.toFixed(2):'—'}</div>
        <div class="pc ${isP?'green':'red'}">${c!=null?(isP?'+':'')+c.toFixed(2)+'%':'—'}</div>
      </div></div>`;
  }).join('');
}

// ── Render Overview ───────────────────────────────────────────────────────────
function renderOv(){
  document.getElementById('ov-grid').innerHTML=TICKERS.map(s=>{
    const q=quotes[s]||{};
    const p=q.regularMarketPrice;
    const c=q.regularMarketChangePercent;
    const isP=c>=0;
    const cc=cur$({currency:q.currency});
    const etf=isEtf(s);
    const ca=isCA(s);
    const name=(q.shortName||q.longName||s);
    return `<div class="ov-card" onclick="showDetail('${s}')">
      <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:4px">
        <div style="min-width:0">
          <div class="ov-sym">${s}${etf?`<span class="badge badge-etf" style="margin-left:5px">ETF</span>`:''}${ca&&!etf?`<span class="badge badge-ca" style="margin-left:5px">CA</span>`:''}</div>
          <div class="ov-name">${name}</div>
        </div>
        <div class="chg-pill ${isP?'up-pill':'dn-pill'}" style="flex-shrink:0">
          ${c!=null?(isP?'+':'')+c.toFixed(2)+'%':'—'}
        </div>
      </div>
      <div class="ov-price ${isP?'green':p!=null?'red':''}">${p!=null?cc+p.toFixed(2):'Loading…'}</div>
      <div class="ov-foot">
        <span>Vol ${fmtV(q.regularMarketVolume)}</span>
        <span>${fmtB(q.marketCap)}</span>
      </div>
    </div>`;
  }).join('');
}

// ── Show Detail ───────────────────────────────────────────────────────────────
function showDetail(sym){
  cur=sym;
  document.getElementById('ov').style.display='none';
  document.getElementById('detail-view').style.display='block';
  renderSB();

  const q=quotes[sym]||{};
  const p=q.regularMarketPrice;
  const chg=q.regularMarketChange;
  const chgP=q.regularMarketChangePercent;
  const isP=chgP>=0;
  const cc=cur$({currency:q.currency});
  const etf=isEtf(sym);
  const etfInfo=ETF_INFO[sym]||{};

  document.getElementById('d-sym').innerHTML=`${sym}${etf?`<span class="badge badge-etf">ETF</span>`:''}`;
  document.getElementById('d-full').textContent=etf?(etfInfo.name||q.longName||sym):(q.longName||q.shortName||sym);
  document.getElementById('d-sector').textContent=etf?`Benchmark: ${etfInfo.benchmark||'—'}`:`Sector: ${({'NVDA':'Technology','META':'Technology','NFLX':'Technology','GOOG':'Technology','MSFT':'Technology','AMZN':'Technology','AAPL':'Technology','BLK':'Financials','KKR':'Financials','BX':'Financials','JPM':'Financials','GS':'Financials','XOM':'Energy','RCL':'Consumer Discretionary','OTF':'Financials (BDC)','L.TO':'Consumer Staples','DOL.TO':'Consumer Discretionary'})[sym]||'—'}`;
  document.getElementById('d-pv').textContent=p!=null?cc+p.toFixed(2):'—';
  document.getElementById('d-pv').className='d-pv '+(isP?'green':'red');
  document.getElementById('d-chg').textContent=chg!=null?`${isP?'+':''}${chg.toFixed(2)} (${isP?'+':''}${chgP.toFixed(2)}%)`:'';
  document.getElementById('d-chg').className='d-chg '+(isP?'green':'red');
  document.getElementById('d-vol').textContent=`Vol: ${fmtV(q.regularMarketVolume)} • Avg: ${fmtV(q.averageDailyVolume3Month)} • 52W: ${cc}${q.fiftyTwoWeekLow?.toFixed(2)||'—'} – ${cc}${q.fiftyTwoWeekHigh?.toFixed(2)||'—'}`;

  // Quick meta under chart
  const hi=q.fiftyTwoWeekHigh, lo=q.fiftyTwoWeekLow;
  const pct=(p&&hi&&lo)?((p-lo)/(hi-lo)*100):null;
  document.getElementById('chart-meta').innerHTML=`
    <div class="sc"><div class="sl">Open</div><div class="sv">${cc}${q.regularMarketOpen?.toFixed(2)||'—'}</div></div>
    <div class="sc"><div class="sl">Day Range</div><div class="sv" style="font-size:14px">${cc}${q.regularMarketDayLow?.toFixed(2)||'—'} – ${cc}${q.regularMarketDayHigh?.toFixed(2)||'—'}</div>
      ${pct!=null?`<div class="rbar"><div class="rbar-fill"></div><div class="rbar-dot" style="left:${pct.toFixed(1)}%"></div></div>
      <div class="rbar-labs"><span>${cc}${lo.toFixed(2)}</span><span>52W Range</span><span>${cc}${hi.toFixed(2)}</span></div>`:''}
    </div>
    <div class="sc"><div class="sl">Market Cap</div><div class="sv" style="font-size:15px">${fmtB(q.marketCap)}</div></div>
    <div class="sc"><div class="sl">Trailing P/E</div><div class="sv">${q.trailingPE?.toFixed(1)||'—'}</div></div>
    <div class="sc"><div class="sl">Forward P/E</div><div class="sv">${q.forwardPE?.toFixed(1)||'—'}</div></div>
    <div class="sc"><div class="sl">Volume</div><div class="sv">${fmtV(q.regularMarketVolume)}</div></div>
  `;

  swTab('chart');
  loadChart('1y','1d');
  loadDetails(sym);
  loadNews(sym);
}

// ── Render Details ────────────────────────────────────────────────────────────
function renderDetails(sym, data){
  const {summary,quote,claude_target,bull_case,bear_case,analyst_target}=data;
  if(!summary&&!quote){
    ['stats-body','bb-body','tgt-body'].forEach(id=>{
      document.getElementById(id).innerHTML='<div style="color:var(--muted);text-align:center;padding:40px">No data returned from Yahoo Finance. Market may be closed or ticker delisted.</div>';
    });
    return;
  }
  const fd=(summary||{}).financialData||{};
  const ks=(summary||{}).defaultKeyStatistics||{};
  const sd=(summary||{}).summaryDetail||{};
  const prof=(summary||{}).assetProfile||{};
  const recT=((summary||{}).recommendationTrend||{}).trend||[];
  const rec=recT[0]||{};
  const cc=cur$({currency:quote?.currency});
  const rv=o=>o?.raw??null;
  const vf=(o,d=2)=>rv(o)!=null?rv(o).toFixed(d):'N/A';
  const vp=o=>rv(o)!=null?(rv(o)*100).toFixed(1)+'%':'N/A';
  const vc=o=>rv(o)!=null?cc+rv(o).toFixed(2):'N/A';
  const fwdPE=(rv(ks.forwardEps)&&quote?.regularMarketPrice)?(quote.regularMarketPrice/rv(ks.forwardEps)).toFixed(1):'N/A';

  // ── B: Stats ──
  const etf=isEtf(sym);
  let statsHtml='';
  if(etf){
    statsHtml=`
      <div class="sec-title">Price Statistics</div>
      <div class="sg">
        <div class="sc"><div class="sl">Current Price</div><div class="sv">${cc}${quote?.regularMarketPrice?.toFixed(2)||'—'}</div></div>
        <div class="sc"><div class="sl">52-Week High</div><div class="sv">${cc}${quote?.fiftyTwoWeekHigh?.toFixed(2)||'—'}</div></div>
        <div class="sc"><div class="sl">52-Week Low</div><div class="sv">${cc}${quote?.fiftyTwoWeekLow?.toFixed(2)||'—'}</div></div>
        <div class="sc"><div class="sl">Market Cap (AUM)</div><div class="sv" style="font-size:15px">${fmtB(quote?.marketCap)}</div></div>
        <div class="sc"><div class="sl">Avg Volume (3M)</div><div class="sv">${fmtV(quote?.averageDailyVolume3Month)}</div></div>
        <div class="sc"><div class="sl">Dividend Yield</div><div class="sv">${vp(sd.dividendYield)}</div></div>
      </div>
      <div class="sec-title">ETF Details</div>
      <div class="sg">
        <div class="sc"><div class="sl">Benchmark</div><div class="sv" style="font-size:14px">${ETF_INFO[sym]?.benchmark||'—'}</div></div>
        <div class="sc"><div class="sl">Expense Ratio</div><div class="sv">${vf(sd.expenseRatio)!=='N/A'?(rv(sd.expenseRatio)*100).toFixed(2)+'%':'See prospectus'}</div></div>
      </div>`;
  } else {
    statsHtml=`
      <div class="sec-title">Valuation Multiples</div>
      <div class="sg">
        <div class="sc"><div class="sl">Trailing P/E</div><div class="sv">${vf(ks.trailingPE)}</div></div>
        <div class="sc"><div class="sl">Forward P/E</div><div class="sv">${fwdPE}</div></div>
        <div class="sc"><div class="sl">Price / Book</div><div class="sv">${vf(ks.priceToBook)}</div></div>
        <div class="sc"><div class="sl">EV / EBITDA</div><div class="sv">${vf(ks.enterpriseToEbitda)}</div></div>
        <div class="sc"><div class="sl">EV / Revenue</div><div class="sv">${vf(ks.enterpriseToRevenue)}</div></div>
        <div class="sc"><div class="sl">PEG Ratio</div><div class="sv">${vf(ks.pegRatio)}</div></div>
      </div>
      <div class="sec-title">Profitability & Growth</div>
      <div class="sg">
        <div class="sc"><div class="sl">Revenue (TTM)</div><div class="sv" style="font-size:15px">${fmtB(rv(fd.totalRevenue))}</div></div>
        <div class="sc"><div class="sl">Revenue Growth</div><div class="sv ${rv(fd.revenueGrowth)>0?'green':'red'}">${vp(fd.revenueGrowth)}</div></div>
        <div class="sc"><div class="sl">Gross Margin</div><div class="sv">${vp(fd.grossMargins)}</div></div>
        <div class="sc"><div class="sl">Operating Margin</div><div class="sv">${vp(fd.operatingMargins)}</div></div>
        <div class="sc"><div class="sl">Net Margin</div><div class="sv ${rv(fd.profitMargins)>0?'green':'red'}">${vp(fd.profitMargins)}</div></div>
        <div class="sc"><div class="sl">Earnings Growth</div><div class="sv ${rv(fd.earningsGrowth)>0?'green':'red'}">${vp(fd.earningsGrowth)}</div></div>
      </div>
      <div class="sec-title">Financial Health</div>
      <div class="sg">
        <div class="sc"><div class="sl">Total Cash</div><div class="sv" style="font-size:15px">${fmtB(rv(fd.totalCash))}</div></div>
        <div class="sc"><div class="sl">Total Debt</div><div class="sv" style="font-size:15px">${fmtB(rv(fd.totalDebt))}</div></div>
        <div class="sc"><div class="sl">Debt / Equity</div><div class="sv ${rv(fd.debtToEquity)>2?'red':rv(fd.debtToEquity)<0.5?'green':''}">${vf(fd.debtToEquity)}</div></div>
        <div class="sc"><div class="sl">Current Ratio</div><div class="sv">${vf(fd.currentRatio)}</div></div>
        <div class="sc"><div class="sl">Return on Equity</div><div class="sv ${rv(fd.returnOnEquity)>0?'green':'red'}">${vp(fd.returnOnEquity)}</div></div>
        <div class="sc"><div class="sl">Free Cash Flow</div><div class="sv" style="font-size:15px">${fmtB(rv(fd.freeCashflow))}</div></div>
      </div>
      <div class="sec-title">Per Share</div>
      <div class="sg">
        <div class="sc"><div class="sl">EPS (TTM)</div><div class="sv">${vc(ks.trailingEps)}</div></div>
        <div class="sc"><div class="sl">Forward EPS</div><div class="sv">${vc(ks.forwardEps)}</div></div>
        <div class="sc"><div class="sl">Book Value / Share</div><div class="sv">${vc(ks.bookValue)}</div></div>
        <div class="sc"><div class="sl">Dividend Yield</div><div class="sv">${vp(sd.dividendYield)}</div></div>
        <div class="sc"><div class="sl">Beta</div><div class="sv">${vf(sd.beta)}</div></div>
        <div class="sc"><div class="sl">Short % Float</div><div class="sv">${vp(ks.shortPercentOfFloat)}</div></div>
      </div>
      ${prof.longBusinessSummary?`<div class="sec-title">Business Description</div>
      <div class="desc-box">${prof.longBusinessSummary.substring(0,700)}${prof.longBusinessSummary.length>700?'…':''}</div>`:''}
    `;
  }
  document.getElementById('stats-body').innerHTML=statsHtml;

  // ── C: Bull / Bear ──
  document.getElementById('bb-body').innerHTML=`
    <p class="bb-note">Analysis based on financial statements, valuation multiples, analyst sentiment, and competitive positioning as of ${new Date().toLocaleDateString('en-US',{year:'numeric',month:'long',day:'numeric'})}. Not financial advice.</p>
    <div class="bb-grid">
      <div class="bb bull"><h3><span style="color:var(--green)">▲</span> Bull Case</h3>
        <ul>${(bull_case||[]).map(p=>`<li>${p}</li>`).join('')}</ul></div>
      <div class="bb bear"><h3><span style="color:var(--red)">▼</span> Bear Case</h3>
        <ul>${(bear_case||[]).map(p=>`<li>${p}</li>`).join('')}</ul></div>
    </div>`;

  // ── D: Price Targets ──
  const price=quote?.regularMarketPrice;
  let analystHtml='<div style="color:var(--muted);font-size:13px;padding:10px 0">No analyst price target data available.</div>';
  if(analyst_target?.mean){
    const m=analyst_target.mean, lo=analyst_target.low, hi=analyst_target.high;
    const up=price?((m-price)/price*100):null;
    const isU=up>=0;
    const sb=rec.strongBuy||0,b=rec.buy||0,h=rec.hold||0,s=(rec.sell||0)+(rec.strongSell||0);
    const tot=sb+b+h+s;
    analystHtml=`
      <div class="pt-price ${isU?'green':'red'}">${cc}${m.toFixed(2)}</div>
      <div class="pt-range">Low ${cc}${lo?.toFixed(2)||'—'} &nbsp;/&nbsp; High ${cc}${hi?.toFixed(2)||'—'}</div>
      ${up!=null?`<div class="pt-upside ${isU?'up-pill':'dn-pill'}">${isU?'▲':'▼'} ${Math.abs(up).toFixed(1)}% ${isU?'upside':'downside'} from current ${cc}${price.toFixed(2)}</div>`:''}
      ${tot>5?`<div>
        <div style="font-size:11px;font-weight:800;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);margin-top:14px;margin-bottom:6px">Analyst Ratings (${tot} analysts)</div>
        <div class="rat-bar">
          <div style="width:${(sb/tot*100).toFixed(0)}%;background:#15803d"></div>
          <div style="width:${(b/tot*100).toFixed(0)}%;background:#22c55e"></div>
          <div style="width:${(h/tot*100).toFixed(0)}%;background:#f59e0b"></div>
          <div style="width:${(s/tot*100).toFixed(0)}%;background:#ef4444"></div>
        </div>
        <div class="rat-labs">
          <span><span class="dot" style="background:#15803d"></span>Strong Buy: ${sb}</span>
          <span><span class="dot" style="background:#22c55e"></span>Buy: ${b}</span>
          <span><span class="dot" style="background:#f59e0b"></span>Hold: ${h}</span>
          <span><span class="dot" style="background:#ef4444"></span>Sell: ${s}</span>
        </div>
      </div>`:''}
    `;
  }

  let claudeHtml='<div style="color:var(--muted);font-size:13px;padding:10px 0">ETF — intrinsic value analysis does not apply to passive index funds.</div>';
  if(claude_target){
    const ct=claude_target;
    const isU2=ct.upside>=0;
    claudeHtml=`
      <div class="pt-price ${isU2?'green':'red'}">${cc}${ct.target?.toFixed(2)||'—'}</div>
      ${ct.upside!=null?`<div class="pt-upside ${isU2?'up-pill':'dn-pill'}">${isU2?'▲':'▼'} ${Math.abs(ct.upside).toFixed(1)}% ${isU2?'upside':'downside'}</div>`:''}
      <div class="method"><strong>Methodology:</strong> ${ct.methodology}</div>
      <div class="warn">⚠️ Claude's estimate is a simplified quantitative model based on public financial data (forward EPS / FCF × sector P/E target, adjusted for growth and margins). It is not financial advice and should not be used as the sole basis for investment decisions.</div>
    `;
  }

  document.getElementById('tgt-body').innerHTML=`
    <div class="pt-grid">
      <div class="pt-card analyst"><div class="pt-lbl">🏦 Wall Street Analyst Consensus</div>${analystHtml}</div>
      <div class="pt-card claude"><div class="pt-lbl">🤖 Claude's Quantitative Estimate</div>${claudeHtml}</div>
    </div>`;
}

// ── Tab switching ─────────────────────────────────────────────────────────────
const TAB_NAMES=['chart','stats','bb','targets','news'];
function swTab(name){
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',TAB_NAMES[i]===name));
  document.querySelectorAll('.tc').forEach((t,i)=>t.classList.toggle('active',TAB_NAMES[i]===name));
}

function showOv(){
  cur=null;
  document.getElementById('ov').style.display='block';
  document.getElementById('detail-view').style.display='none';
  renderSB();
}

async function hardRefresh(){
  dcache={};
  await fetchQuotes();
  if(cur){await loadDetails(cur);await loadNews(cur);loadChart('1y','1d');}
}

// ── Ticker Edit Modal ─────────────────────────────────────────────────────────
function openEditModal(){
  document.getElementById('modal-tickers').value=TICKERS.join(', ');
  document.getElementById('edit-modal').style.display='flex';
  // Set bookmarklet href
  const bm=`javascript:void(function(){var s=new Set;document.querySelectorAll('a[href*="/quote/"]').forEach(function(a){var m=a.href.match(/\\/quote\\/([^/?]+)/);m&&s.add(m[1])});var t=[...s];fetch('http://localhost:8080/api/update-tickers',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({tickers:t})}).then(function(r){return r.json()}).then(function(d){alert('Synced '+d.count+' tickers to dashboard! Refresh the dashboard page.')}).catch(function(e){alert('Error: '+e)})})()`;
  document.getElementById('bookmarklet-link').href=bm;
}
function closeModal(){
  document.getElementById('edit-modal').style.display='none';
}
function saveTickers(){
  const raw=document.getElementById('modal-tickers').value;
  const parsed=raw.split(/[,\n]+/).map(s=>s.trim().toUpperCase()).filter(s=>s.length>0&&s.length<12);
  if(parsed.length===0){alert('No valid tickers found');return;}
  TICKERS.length=0; parsed.forEach(t=>TICKERS.push(t));
  persistOrder();
  closeModal();
  dcache={};
  fetchQuotes();
}

// ── Yahoo Finance Sync ───────────────────────────────────────────────────────
function syncFromYF(){
  const confirmed=confirm(
    'Sync options:\\n\\n'+
    '1. BOOKMARKLET (recommended): Click "Edit List" → drag the purple "Sync to Dashboard" button to your bookmark bar → go to Yahoo Finance watchlist → click the bookmarklet\\n\\n'+
    '2. MANUAL: Click OK, then paste your tickers in the edit box\\n\\n'+
    'Click OK to open the Edit List dialog.'
  );
  if(confirmed) openEditModal();
}

// ── Load saved ticker order ──────────────────────────────────────────────────
function loadSavedOrder(){
  try{
    const saved=localStorage.getItem('dash_tickers');
    if(saved){
      const arr=JSON.parse(saved);
      if(Array.isArray(arr)&&arr.length>0){
        TICKERS.length=0; arr.forEach(t=>TICKERS.push(t));
        // Also push to server
        fetch('/api/update-tickers',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({tickers:TICKERS})}).catch(()=>{});
      }
    }
  }catch(e){}
}

// ── Init ──────────────────────────────────────────────────────────────────────
loadSavedOrder();
fetchQuotes();
// Auto-refresh quotes every 5 minutes
setInterval(()=>{
  fetchQuotes();
  if(cur){delete dcache[cur];loadDetails(cur);}
}, 5*60*1000);
</script>
</body>
</html>"""

# ── Launch ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "━"*54)
    print("   📊  Jaclyn's Live Stock Dashboard")
    print("━"*54)
    print(f"   Tickers   : {len(TICKERS)} stocks & ETFs loaded")
    print(f"   Dashboard : http://localhost:8080")
    print(f"   Refresh   : auto every 5 minutes")
    print("━"*54 + "\n")
    print("   Warming up data connection…")
    # Pre-fetch a single ticker in background to warm up yfinance
    threading.Thread(target=lambda: fetch_quotes(["AAPL"]), daemon=True).start()
    # Auto-open browser
    import webbrowser
    threading.Timer(2.0, lambda: webbrowser.open("http://localhost:8080")).start()
    app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)
