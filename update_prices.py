#!/usr/bin/env python3
"""
Fetch delayed Bursa Malaysia prices for the Top-50 dashboard and write prices.json.

Run it anywhere Yahoo Finance is reachable: Google Colab, your own computer, or GitHub Actions.
    pip install yfinance requests pandas
    python update_prices.py                      -> writes prices.json (and downloads it in Colab)
    python update_prices.py --inject dashboard.html
                                                 -> also writes a copy of the dashboard with the prices saved inside

How it works
  1. yfinance downloads 1 month of daily bars and today's 1-minute bars for the 50 symbols.
  2. Anything yfinance misses is fetched from Yahoo's chart endpoint with requests.
  3. Every symbol is checked: the Yahoo close for the valuation date must equal the dashboard's close
     (this proves the symbol and the data source), and the price must be a sensible positive number.
  4. prices.json is written in Yahoo's chart format, which the dashboard re-validates in your browser.
Prices from Yahoo for Bursa Malaysia are delayed (normally by at least 15 minutes).
"""
import argparse, concurrent.futures as cf, datetime as dt, json, os, re, sys, time

BASE_DATE = "2026-10-02"
BASE = {
 "1155.KL": {
  "tk": "MAY MK",
  "name": "Malayan Banking Bhd",
  "close": 10.02,
  "mcap": 120.35,
  "px": 10.04
 },
 "1295.KL": {
  "tk": "PBK MK",
  "name": "Public Bank Bhd",
  "close": 4.66,
  "mcap": 90.65,
  "px": 4.71
 },
 "1023.KL": {
  "tk": "CIMB MK",
  "name": "CIMB Group Holdings Bhd",
  "close": 7.65,
  "mcap": 82.97,
  "px": 7.68
 },
 "5347.KL": {
  "tk": "TNB MK",
  "name": "Tenaga Nasional Bhd",
  "close": 12.96,
  "mcap": 75.2,
  "px": 13.06
 },
 "5225.KL": {
  "tk": "IHH MK",
  "name": "IHH Healthcare Bhd",
  "close": 7.8,
  "mcap": 68.04,
  "px": 7.81
 },
 "8869.KL": {
  "tk": "PMAH MK",
  "name": "Press Metal Aluminium Holdings",
  "close": 7.17,
  "mcap": 59,
  "px": 7.19
 },
 "6742.KL": {
  "tk": "YTLP MK",
  "name": "YTL Power International Bhd",
  "close": 5.68,
  "mcap": 50.25,
  "px": 5.59
 },
 "5819.KL": {
  "tk": "HLBK MK",
  "name": "Hong Leong Bank Bhd",
  "close": 22.64,
  "mcap": 49.51,
  "px": 22.76
 },
 "5285.KL": {
  "tk": "SDG MK",
  "name": "SD Guthrie Bhd",
  "close": 6.1,
  "mcap": 41.84,
  "px": 6.09
 },
 "3816.KL": {
  "tk": "MISC MK",
  "name": "MISC Bhd",
  "close": 7.53,
  "mcap": 34.28,
  "px": 7.57
 },
 "6033.KL": {
  "tk": "PTG MK",
  "name": "Petronas Gas Bhd",
  "close": 17.2,
  "mcap": 34.27,
  "px": 17.24
 },
 "5183.KL": {
  "tk": "PCHEM MK",
  "name": "Petronas Chemicals Group Bhd",
  "close": 4.21,
  "mcap": 33.04,
  "px": 4.26
 },
 "1066.KL": {
  "tk": "RHBBANK MK",
  "name": "RHB Bank Bhd",
  "close": 7.43,
  "mcap": 32.61,
  "px": 7.45
 },
 "5398.KL": {
  "tk": "GAM MK",
  "name": "Gamuda Bhd",
  "close": 5.1,
  "mcap": 30.98,
  "px": 5.1
 },
 "4863.KL": {
  "tk": "T MK",
  "name": "Telekom Malaysia Bhd",
  "close": 7.83,
  "mcap": 30.13,
  "px": 7.85
 },
 "6947.KL": {
  "tk": "CDB MK",
  "name": "CelcomDigi Bhd",
  "close": 2.52,
  "mcap": 29.44,
  "px": 2.51
 },
 "5211.KL": {
  "tk": "SWB MK",
  "name": "Sunway Bhd",
  "close": 4.3,
  "mcap": 29.41,
  "px": 4.3
 },
 "1961.KL": {
  "tk": "IOI MK",
  "name": "IOI Corp Bhd",
  "close": 4.69,
  "mcap": 29.35,
  "px": 4.69
 },
 "6012.KL": {
  "tk": "MAXIS MK",
  "name": "Maxis Bhd",
  "close": 3.51,
  "mcap": 27.13,
  "px": 3.5
 },
 "4677.KL": {
  "tk": "YTL MK",
  "name": "YTL Corp Bhd",
  "close": 2.24,
  "mcap": 26.52,
  "px": 2.22
 },
 "5326.KL": {
  "tk": "99SMART MK",
  "name": "99 Speed Mart Retail Holdings",
  "close": 3.06,
  "mcap": 25.54,
  "px": 3.06
 },
 "2445.KL": {
  "tk": "KLK MK",
  "name": "Kuala Lumpur Kepong Bhd",
  "close": 21,
  "mcap": 23.85,
  "px": 20.96
 },
 "5555.KL": {
  "tk": "SUNMED MK",
  "name": "Sunway Healthcare Holdings Bhd",
  "close": 2.06,
  "mcap": 23.81,
  "px": 2.07
 },
 "5246.KL": {
  "tk": "WPRTS MK",
  "name": "Westports Holdings Bhd",
  "close": 6.75,
  "mcap": 23.3,
  "px": 6.77
 },
 "1015.KL": {
  "tk": "AMM MK",
  "name": "AMMB Holdings Bhd",
  "close": 6.39,
  "mcap": 21.16,
  "px": 6.4
 },
 "1082.KL": {
  "tk": "HLFG MK",
  "name": "Hong Leong Financial Group Bhd",
  "close": 18.46,
  "mcap": 21.14,
  "px": 18.48
 },
 "4707.KL": {
  "tk": "NESZ MK",
  "name": "Nestle Malaysia Bhd",
  "close": 89.52,
  "mcap": 21.11,
  "px": 89.8
 },
 "2089.KL": {
  "tk": "UPL MK",
  "name": "United Plantations Bhd",
  "close": 31.38,
  "mcap": 19.26,
  "px": 31.32
 },
 "5681.KL": {
  "tk": "PETD MK",
  "name": "Petronas Dagangan Bhd",
  "close": 19.2,
  "mcap": 18.99,
  "px": 19.14
 },
 "0097.KL": {
  "tk": "VITRO MK",
  "name": "ViTrox Corp Bhd",
  "close": 10.46,
  "mcap": 18.95,
  "px": 10.34
 },
 "5249.KL": {
  "tk": "IOIPG MK",
  "name": "IOI Properties Group Bhd",
  "close": 3.4,
  "mcap": 18.72,
  "px": 3.43
 },
 "4197.KL": {
  "tk": "SIME MK",
  "name": "Sime Darby Bhd",
  "close": 2.49,
  "mcap": 16.49,
  "px": 2.5
 },
 "5235SS.KL": {
  "tk": "KLCCSS MK",
  "name": "KLCCP Stapled Group",
  "close": 8.32,
  "mcap": 15,
  "px": 8.3
 },
 "6888.KL": {
  "tk": "AXIATA MK",
  "name": "Axiata Group Bhd",
  "close": 1.64,
  "mcap": 14.89,
  "px": 1.65
 },
 "7084.KL": {
  "tk": "QLG MK",
  "name": "QL Resources Bhd",
  "close": 3.89,
  "mcap": 14.17,
  "px": 3.85
 },
 "4065.KL": {
  "tk": "PEP MK",
  "name": "PPB Group Bhd",
  "close": 9.15,
  "mcap": 12.89,
  "px": 9.12
 },
 "5296.KL": {
  "tk": "MRDIY MK",
  "name": "MR DIY Group M Bhd",
  "close": 1.27,
  "mcap": 11.85,
  "px": 1.28
 },
 "5227.KL": {
  "tk": "IGBREIT MK",
  "name": "IGB Real Estate Investment Trust",
  "close": 2.81,
  "mcap": 11.83,
  "px": 2.77
 },
 "5878.KL": {
  "tk": "KPJ MK",
  "name": "KPJ Healthcare Bhd",
  "close": 2.59,
  "mcap": 11.51,
  "px": 2.63
 },
 "5031.KL": {
  "tk": "TDC MK",
  "name": "TIME dotCom Bhd",
  "close": 5.65,
  "mcap": 10.61,
  "px": 5.72
 },
 "7277.KL": {
  "tk": "DLG MK",
  "name": "Dialog Group Bhd",
  "close": 1.85,
  "mcap": 10.61,
  "px": 1.84
 },
 "3336.KL": {
  "tk": "IJM MK",
  "name": "IJM Corp Bhd",
  "close": 2.81,
  "mcap": 10.25,
  "px": 2.82
 },
 "5263.KL": {
  "tk": "SCGB MK",
  "name": "Sunway Construction Group Bhd",
  "close": 7.57,
  "mcap": 10.06,
  "px": 7.51
 },
 "0166.KL": {
  "tk": "INRI MK",
  "name": "Inari Amertron Bhd",
  "close": 2.58,
  "mcap": 9.7,
  "px": 2.57
 },
 "3794.KL": {
  "tk": "LMC MK",
  "name": "Malayan Cement Bhd",
  "close": 5.52,
  "mcap": 9.13,
  "px": 5.6
 },
 "0128.KL": {
  "tk": "FRCB MK",
  "name": "Frontken Corp Bhd",
  "close": 5.06,
  "mcap": 9.06,
  "px": 5.04
 },
 "4715.KL": {
  "tk": "GENM MK",
  "name": "Genting Malaysia Bhd",
  "close": 1.47,
  "mcap": 8.44,
  "px": 1.47
 },
 "5337.KL": {
  "tk": "ECOSHOP MK",
  "name": "ECO-Shop Marketing Bhd",
  "close": 1.45,
  "mcap": 8.35,
  "px": 1.43
 },
 "3867.KL": {
  "tk": "MPI MK",
  "name": "Malaysian Pacific Industries Bhd",
  "close": 42.4,
  "mcap": 8.2,
  "px": 41.96
 },
 "5288.KL": {
  "tk": "SDPR MK",
  "name": "Sime Darby Property Bhd",
  "close": 1.24,
  "mcap": 8.16,
  "px": 1.24
 }
}          # symbol -> {"tk": ticker, "name": company, "close": dashboard close on BASE_DATE}
CHART_BASE = os.environ.get("YF_CHART_BASE", "https://query1.finance.yahoo.com")
MYT = dt.timezone(dt.timedelta(hours=8))
TOL = 0.006              # half a sen: Yahoo's close for BASE_DATE must equal the dashboard's close


def log(*a):
    print(*a, flush=True)


def in_session(now):
    """Approximate Bursa hours, Mon-Fri 09:00-12:30 and 14:30-17:00 MYT (with a little grace)."""
    n = now.astimezone(MYT)
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return (540 <= m <= 755) or (870 <= m <= 1025)


def epoch(date_iso, hh, mm):
    y, mo, d = map(int, date_iso.split("-"))
    return int(dt.datetime(y, mo, d, hh, mm, tzinfo=MYT).timestamp())


def chart_json(code, price, qtime, hist, day_h=None, day_l=None, vol=None, source="yfinance", highs=None, lows=None, vols=None):
    dates = sorted(hist)
    vols = vols or {}
    highs, lows = highs or {}, lows or {}
    return {"chart": {"error": None, "result": [{
        "meta": {"currency": "MYR", "symbol": code, "exchangeName": "KLS", "regularMarketPrice": price,
                 "regularMarketTime": qtime, "regularMarketDayHigh": day_h, "regularMarketDayLow": day_l,
                 "regularMarketVolume": vol, "dataSource": source},
        "timestamp": [epoch(d, 9, 0) for d in dates],
        "indicators": {"quote": [{"close": [hist[d] for d in dates],
                                  "high": [highs.get(d) for d in dates],
                                  "low": [lows.get(d) for d in dates],
                                  "volume": [vols.get(d) for d in dates]}]}}]}}


# ---------------------------------------------------------------- yfinance route
def _sub(df, code):
    import pandas as pd
    if df is None or len(df) == 0:
        return None
    cols = df.columns
    if isinstance(cols, pd.MultiIndex):
        if code in cols.get_level_values(0):
            return df[code]
        if code in cols.get_level_values(1):
            return df.xs(code, axis=1, level=1)
        return None
    return df


def _download(yf, codes, **kw):
    for attempt in range(3):
        try:
            df = yf.download(codes, auto_adjust=False, group_by="ticker", progress=False, threads=True, **kw)
            if df is not None and len(df):
                return df
        except Exception as e:                                   # rate limits, network blips
            log("  yfinance attempt", attempt + 1, "failed:", repr(e)[:140])
        time.sleep(3 * (attempt + 1))
    return None


def _intraday(same, day):
    """1-minute bars (time, close, high, low) of the latest trading day, for the dashboard's 1D chart."""
    import pandas as pd
    try:
        x = same.dropna(subset=["Close"])
        if not len(x):
            return None
        idx = pd.DatetimeIndex(x.index)
        idx = idx.tz_localize(MYT) if idx.tz is None else idx.tz_convert(MYT)
        x = x.set_axis(idx)
        g = x.resample("1min", label="left", closed="left")
        c, h, l = g["Close"].last().dropna(), g["High"].max(), g["Low"].min()
        t = [int(i.timestamp()) for i in c.index]
        if len(t) < 1:
            return None
        return {"day": day, "t": t, "c": [round(float(v), 3) for v in c],
                "h": [round(float(h[i]), 3) for i in c.index], "l": [round(float(l[i]), 3) for i in c.index]}
    except Exception as e:
        log("  intraday series skipped:", repr(e)[:100])
        return None


def via_yfinance(codes, now):
    import pandas as pd
    import yfinance as yf
    daily = _download(yf, codes, period="3mo", interval="1d")
    intra = _download(yf, codes, period="1d", interval="1m")
    # The batch download sometimes comes back empty for a thinly traded stock. Ask again, one stock at a time, so that a real
    # trade is not missed (a stock with no trade yet today simply stays empty).
    extra = {}
    for code in codes:
        try:
            cur = _sub(intra, code)
            if cur is not None and len(cur["Close"].dropna()):
                continue
            one = yf.Ticker(code).history(period="1d", interval="1m", auto_adjust=False)
            if one is not None and len(one) and len(one["Close"].dropna()):
                extra[code] = one
                log("  recovered 1-minute data for", code, "on a second try")
        except Exception as e:
            log("  second try failed for", code, repr(e)[:80])
    out = {}
    for code in codes:
        try:
            d = _sub(daily, code)
            if d is None:
                continue
            closes = d["Close"].dropna()
            hist = {pd.Timestamp(i).strftime("%Y-%m-%d"): round(float(v), 3) for i, v in closes.items() if float(v) > 0}
            if not hist:
                continue
            highs, lows = {}, {}                                  # each day's intraday high and low (the chart "shadow")
            if "High" in d and "Low" in d:
                for i in d.index:
                    day = pd.Timestamp(i).strftime("%Y-%m-%d")
                    try:
                        hi, lo = float(d.loc[i, "High"]), float(d.loc[i, "Low"])
                    except (TypeError, ValueError):
                        continue
                    c_ = hist.get(day)
                    if c_ and hi == hi and lo == lo and lo > 0 and lo <= hi and lo - 0.0051 <= c_ <= hi + 0.0051:
                        highs[day], lows[day] = round(hi, 3), round(lo, 3)
            vols = {}                                              # daily share volume (for the 1-30 day average traded value)
            if "Volume" in d:
                for i in d.index:
                    day = pd.Timestamp(i).strftime("%Y-%m-%d")
                    try:
                        v_ = float(d.loc[i, "Volume"])
                    except (TypeError, ValueError):
                        continue
                    if day in hist and v_ == v_ and v_ >= 0:
                        vols[day] = int(v_)
            price = qtime = day_h = day_l = vol = None
            intraday = None
            src = "yfinance daily bar"
            it = extra.get(code)
            if it is None:
                it = _sub(intra, code)
            if it is not None:
                c = it["Close"].dropna()
                if len(c):
                    ts = pd.Timestamp(c.index[-1])
                    ts = ts.tz_localize(MYT) if ts.tzinfo is None else ts
                    ts_myt = ts.tz_convert(MYT)
                    day = ts_myt.strftime("%Y-%m-%d")
                    price, qtime, src = float(c.iloc[-1]), int(ts.timestamp()), "yfinance 1-minute bar"
                    idx = [pd.Timestamp(i).tz_convert(MYT).strftime("%Y-%m-%d") if pd.Timestamp(i).tzinfo else
                           pd.Timestamp(i).strftime("%Y-%m-%d") for i in it.index]
                    same = it[[x == day for x in idx]]
                    day_h = round(float(same["High"].dropna().max()), 3) if "High" in same else None
                    day_l = round(float(same["Low"].dropna().min()), 3) if "Low" in same else None
                    vol = int(same["Volume"].fillna(0).sum()) if "Volume" in same else None
                    intraday = _intraday(same, day)
                    # after the close, the official daily close is the right "last price"
                    if not in_session(now) and day in hist:
                        price = hist[day]
            if price is None:                                    # no intraday data: use the latest daily bar
                last = max(hist)
                price, qtime = hist[last], epoch(last, 17, 0)
            out[code] = chart_json(code, round(price, 3), qtime, hist, day_h, day_l, vol, src, highs, lows, vols)
            if intraday:
                out[code]["chart"]["result"][0]["intraday"] = intraday
        except Exception as e:
            log("  could not read", code, "from yfinance:", repr(e)[:120])
    return out


# ---------------------------------------------------------------- direct chart-API route
STATE = {"ok": 0, "net": 0}


def via_chart_api(code):
    import requests
    last = None
    if STATE["ok"] == 0 and STATE["net"] >= 8:               # nothing has worked: Yahoo is unreachable from here
        raise RuntimeError("Yahoo is not reachable from this network")
    for attempt in range(3):
        try:
            r = requests.get(f"{CHART_BASE}/v8/finance/chart/{code}?range=3mo&interval=1d&includePrePost=false",
                             headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
            if r.status_code == 429:
                time.sleep(3 * (attempt + 1)); last = "HTTP 429"; continue
            r.raise_for_status()
            STATE["ok"] += 1
            return r.json()
        except requests.exceptions.ConnectionError as e:
            STATE["net"] += 1
            raise RuntimeError("could not connect: " + repr(e)[:100])
        except Exception as e:
            last = repr(e)[:120]
            time.sleep(1.5 * (attempt + 1))
    raise RuntimeError(last)



# ---------------------------------------------------------------- keep every trading day
def heal_history(charts, old_feed, seed, now):
    """Yahoo's daily history sometimes leaves out a day that was already fetched (it dropped Monday 12 Oct on Tuesday morning),
    which would make every "today" change use the wrong previous close. Days that were fetched before are kept; a day Yahoo
    returns now always replaces the kept copy. Nothing is ever invented: days come only from earlier Yahoo downloads."""
    restored = 0
    for code, j in charts.items():
        try:
            r = j["chart"]["result"][0]
            q = r["indicators"]["quote"][0]
            qd = now.astimezone(MYT).strftime("%Y-%m-%d")                  # only finished days are restored, never today's
            bars = {}
            for src in (seed.get(code, {}), _bars_of(old_feed, code)):      # kept copies first, fresh data overrides them below
                for day, v in src.items():
                    bars[day] = v
            have = {}
            for i, t in enumerate(r["timestamp"]):
                day = dt.datetime.fromtimestamp(t, MYT).strftime("%Y-%m-%d")
                if isinstance(q["close"][i], (int, float)) and q["close"][i] == q["close"][i] and q["close"][i] > 0:
                    have[day] = [q["close"][i], q["high"][i], q["low"][i], q["volume"][i]]
            first = min(have) if have else None
            add = [d for d in bars if d not in have and first and d >= first and d < qd and bars[d][0] and bars[d][0] > 0]
            if not add:
                continue
            for d in add:
                have[d] = bars[d]; restored += 1
            days = sorted(have)
            m = r["meta"]
            if dt.datetime.fromtimestamp(m["regularMarketTime"], MYT).strftime("%Y-%m-%d") < days[-1] and days[-1] < qd:
                m["regularMarketPrice"], m["regularMarketTime"] = round(have[days[-1]][0], 3), epoch(days[-1], 17, 0)
                m["regularMarketDayHigh"], m["regularMarketDayLow"], m["regularMarketVolume"] = have[days[-1]][1], have[days[-1]][2], have[days[-1]][3]
                m["dataSource"] = "yfinance daily bar"
            r["timestamp"] = [epoch(d, 9, 0) for d in days]
            q["close"] = [have[d][0] for d in days]; q["high"] = [have[d][1] for d in days]
            q["low"] = [have[d][2] for d in days]; q["volume"] = [have[d][3] for d in days]
        except Exception as e:
            log("  history merge skipped for", code, repr(e)[:80])
    if restored:
        log("Restored %d earlier trading day(s) that Yahoo left out of today's download." % restored)


def _bars_of(feed, code):
    out = {}
    try:
        r = feed["charts"][code]["chart"]["result"][0]; q = r["indicators"]["quote"][0]
        for i, t in enumerate(r["timestamp"]):
            c = q["close"][i]
            if isinstance(c, (int, float)) and c == c and c > 0:
                out[dt.datetime.fromtimestamp(t, MYT).strftime("%Y-%m-%d")] = [c, q["high"][i], q["low"][i], q["volume"][i]]
    except Exception:
        pass
    return out


def _load(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}

# ---------------------------------------------------------------- validation
def check(code, j, now):
    """Return (ok, base_status, message). Mirrors the checks the dashboard repeats in the browser."""
    try:
        r = j["chart"]["result"][0]
        m = r["meta"]
        if str(m.get("symbol", "")).upper() != code.upper():
            return False, "na", "wrong symbol returned (%s)" % m.get("symbol")
        if m.get("currency") and str(m["currency"]).upper() != "MYR":
            return False, "na", "unexpected currency (%s)" % m["currency"]
        px, t = m.get("regularMarketPrice"), m.get("regularMarketTime")
        if not isinstance(px, (int, float)) or px != px or px <= 0:
            return False, "na", "no valid price"
        if not t or t > now.timestamp() + 600:
            return False, "na", "bad quote time"
        hist = {}
        q = r["indicators"]["quote"][0]["close"]
        for ts_, c in zip(r.get("timestamp", []), q):
            if isinstance(c, (int, float)) and c == c and c > 0:
                hist[dt.datetime.fromtimestamp(ts_, MYT).strftime("%Y-%m-%d")] = c
        if BASE_DATE in hist:
            return True, ("ok" if abs(hist[BASE_DATE] - BASE[code]["close"]) <= TOL else "diff"), ""
        return True, "na", ""
    except Exception as e:
        return False, "na", "unreadable response (%s)" % repr(e)[:80]


def write_xlsx(path, feed, now):
    """Excel copy of the live table: prices from the feed, formulas for the % changes and market cap."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils import get_column_letter as L
    wb = Workbook(); ws = wb.active; ws.title = "Live Prices"
    hd = ["#", "Ticker", "Company", "Yahoo code", "Close " + BASE_DATE, "Last price", "Previous close", "Quote time (MYT)",
          "Today %", "Since " + BASE_DATE + " %", "Day low", "Day high", "Volume", "Mkt cap RM bn (dashboard)",
          "Dashboard price snapshot", "Mkt cap RM bn (live)", "Check vs dashboard"]
    ws.append(["Malaysia Top 50 - Live Prices (delayed). Data written " + now.astimezone(MYT).strftime("%d %b %Y %H:%M") + " MYT via " + feed["source"]])
    ws.append([]); ws.append(hd)
    for c in ws[3]:
        c.font = Font(bold=True, color="FFFFFF"); c.fill = PatternFill("solid", fgColor="1F3A5F")
        c.alignment = Alignment(wrap_text=True, vertical="center")
    r0 = 4
    for i, (code, b) in enumerate(BASE.items()):
        r = r0 + i
        ch = feed["charts"].get(code)
        if ch:
            res = ch["chart"]["result"][0]; m = res["meta"]
            hist = {}
            for t_, c_ in zip(res["timestamp"], res["indicators"]["quote"][0]["close"]):
                if c_: hist[dt.datetime.fromtimestamp(t_, MYT).strftime("%Y-%m-%d")] = c_
            qd = dt.datetime.fromtimestamp(m["regularMarketTime"], MYT).strftime("%Y-%m-%d")
            prev = [hist[d] for d in sorted(hist) if d < qd]
            chk = "OK" if abs(hist.get(BASE_DATE, -1) - b["close"]) <= TOL else ("differs" if BASE_DATE in hist else "n/a")
            row = [i + 1, b["tk"], b["name"], code, b["close"], m["regularMarketPrice"], prev[-1] if prev else None,
                   dt.datetime.fromtimestamp(m["regularMarketTime"], MYT).replace(tzinfo=None),
                   "=IF(G%d>0,F%d/G%d-1,\"\")" % (r, r, r), "=F%d/E%d-1" % (r, r), m.get("regularMarketDayLow"),
                   m.get("regularMarketDayHigh"), m.get("regularMarketVolume"), b["mcap"], b["px"],
                   "=N%d*F%d/O%d" % (r, r, r), chk]
        else:
            row = [i + 1, b["tk"], b["name"], code, b["close"]] + [None] * 8 + [b["mcap"], b["px"], None, "no data"]
        ws.append(row)
    rl = r0 + len(BASE) - 1
    ws.append([]); t = rl + 2
    ws.cell(t, 3, "Total (stocks with prices)").font = Font(bold=True)
    ws.cell(t, 14, "=SUMIF(F%d:F%d,\">0\",N%d:N%d)" % (r0, rl, r0, rl)); ws.cell(t, 16, "=SUM(P%d:P%d)" % (r0, rl))
    ws.cell(t, 10, "=IF(N%d>0,P%d/N%d-1,\"\")" % (t, t, t))
    for c in (14, 16): ws.cell(t, c).number_format = "#,##0.0"; ws.cell(t, c).font = Font(bold=True)
    ws.cell(t, 10).number_format = "+0.00%;-0.00%"; ws.cell(t, 10).font = Font(bold=True)
    for r in range(r0, rl + 1):
        for c, f in ((5, "0.000"), (6, "0.000"), (7, "0.000"), (8, "dd mmm hh:mm"), (9, "+0.00%;-0.00%"), (10, "+0.00%;-0.00%"),
                     (11, "0.000"), (12, "0.000"), (13, "#,##0"), (14, "#,##0.0"), (15, "0.000"), (16, "#,##0.0")):
            ws.cell(r, c).number_format = f
    for i, w in enumerate([5, 10, 34, 11, 11, 11, 11, 15, 10, 12, 9, 9, 13, 16, 14, 16, 14], 1):
        ws.column_dimensions[L(i)].width = w
    ws.freeze_panes = "D4"; ws.row_dimensions[3].height = 42; ws.auto_filter.ref = "A3:Q%d" % rl
    from openpyxl.formatting.rule import CellIsRule
    ws.conditional_formatting.add("I%d:J%d" % (r0, rl), CellIsRule(operator="greaterThan", formula=["0"], font=Font(color="1A7F37")))
    ws.conditional_formatting.add("I%d:J%d" % (r0, rl), CellIsRule(operator="lessThan", formula=["0"], font=Font(color="C62828")))
    n = wb.create_sheet("Notes"); rp = feed["report"]
    for line in ["Source: Yahoo Finance via " + feed["source"] + " (delayed at least 15 minutes, unofficial).",
                 "Fetched %d of %d symbols; %d failed." % (rp["fetched"], len(BASE), rp["failed"]),
                 "Accuracy check: Yahoo's %s close equals the dashboard's for %d stocks (%d differ, %d not comparable)." % (BASE_DATE, rp["base_match"], rp["base_diff"], rp["base_na"]),
                 "Last price after the close = the official daily close. Today % compares it with the previous trading day's close.",
                 "Mkt cap (live) = dashboard market cap x last price / dashboard price snapshot, same method as the Live Prices tab.",
                 "To refresh: run update_prices.py again; it writes a new prices.json and prices_live.xlsx."]:
        n.append([line])
    n.column_dimensions["A"].width = 120
    wb.save(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="prices.json")
    ap.add_argument("--inject", help="dashboard .html file to copy with the prices saved inside")
    ap.add_argument("--xlsx", default="prices_live.xlsx", help="Excel copy of the live table ('' to skip)")
    ap.add_argument("--no-yfinance", action="store_true", help="skip yfinance and use the chart endpoint only")
    a = ap.parse_args(sys.argv[1:] if "ipykernel" not in sys.modules else [])
    now = dt.datetime.now(dt.timezone.utc)
    codes = list(BASE)
    log("Fetching %d symbols at %s MYT ..." % (len(codes), now.astimezone(MYT).strftime("%Y-%m-%d %H:%M")))
    charts, failed, used = {}, {}, "yfinance"
    if not a.no_yfinance:
        try:
            charts = via_yfinance(codes, now)
        except ImportError:
            log("yfinance is not installed; using the chart endpoint instead (pip install yfinance)")
        except Exception as e:
            log("yfinance route failed:", repr(e)[:160])
    todo = [c for c in codes if c not in charts or not check(c, charts[c], now)[0]]
    if todo:
        log("Using the chart endpoint for %d symbol(s) ..." % len(todo))
        used = "yfinance + chart endpoint" if charts else "chart endpoint"
        with cf.ThreadPoolExecutor(5) as ex:
            futs = {ex.submit(via_chart_api, c): c for c in todo}
            for f in cf.as_completed(futs):
                c = futs[f]
                try:
                    charts[c] = f.result()
                except Exception as e:
                    charts.pop(c, None)
                    failed[c] = str(e)
    heal_history(charts, _load(a.out), _load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "history_seed.json")), now)
    ok = diff = na = 0
    for c in list(charts):
        good, base, msg = check(c, charts[c], now)
        if not good:
            failed[c] = msg; charts.pop(c); continue
        ok += base == "ok"; diff += base == "diff"; na += base == "na"
        if base == "diff":
            log("  WARNING %s %s: Yahoo close for %s differs from the dashboard" % (c, BASE[c]["tk"], BASE_DATE))
    report = {"fetched": len(charts), "failed": len(failed), "base_match": ok, "base_diff": diff, "base_na": na}
    # one common "prices as of" time for every stock: the newest trade in the feed (stocks that have not traded since then keep their last trade)
    as_of = max([charts[c]["chart"]["result"][0]["meta"]["regularMarketTime"] for c in charts] or [0])
    feed = {"generated_at": now.isoformat(timespec="seconds"), "generated_ms": int(now.timestamp() * 1000), "as_of": as_of,
            "source": used, "base_date": BASE_DATE, "report": report, "charts": charts,
            "failed": {c: failed[c] for c in failed}}
    with open(a.out, "w", encoding="utf-8") as fh:
        json.dump(feed, fh, separators=(",", ":"))
    log("Wrote %s: %d fetched, %d failed; %d of %d match the dashboard's %s close (%d differ, %d not comparable)." %
        (a.out, len(charts), len(failed), ok, len(charts), BASE_DATE, diff, na))
    for c, m in failed.items():
        log("  FAILED %s %s: %s" % (c, BASE[c]["tk"], m))
    if a.xlsx:
        try:
            write_xlsx(a.xlsx, feed, now); log("Wrote", a.xlsx)
        except ImportError:
            log("Excel file skipped (pip install openpyxl)"); a.xlsx = ""
    if a.inject:
        h = open(a.inject, encoding="utf-8").read()
        pat = re.compile(r'(<script id="livefeed" type="application/json">)(.*?)(</script>)', re.S)
        if not pat.search(h):
            sys.exit("This dashboard file has no Live Prices tab (no livefeed block).")
        blob = json.dumps(feed, separators=(",", ":")).replace("</", "<\\/")
        h = pat.sub(lambda m: m.group(1) + blob + m.group(3), h, 1)
        stem = re.sub(r"\.html$", "", a.inject)
        outp = "%s (updated %s).html" % (stem, now.astimezone(MYT).strftime("%Y-%m-%d %H%M"))
        open(outp, "w", encoding="utf-8").write(h)
        log("Wrote", outp)
    if "google.colab" in sys.modules:
        from google.colab import files
        files.download(a.out)
        if a.xlsx:
            files.download(a.xlsx)
        if a.inject:
            files.download(outp)
    if not charts:
        sys.exit("No prices could be fetched.")


if __name__ == "__main__":
    main()
