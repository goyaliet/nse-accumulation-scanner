#!/usr/bin/env python3
"""
NSE Institutional Accumulation Scanner
=======================================
Detects institutional buying via delivery % + volume surge analysis.
Runs daily after market close, generates a mobile-responsive HTML report,
and pushes it to GitHub Pages.

GitHub Pages URL: https://goyaliet.github.io/nse-accumulation-scanner/

Usage:
    python scanner.py              # Run for today
    python scanner.py --date 2026-03-21  # Run for specific date
    python scanner.py --no-push    # Skip git push (local testing)
"""

import os
import sys
import time
import argparse
import subprocess
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────
SCANNER_DIR = Path(__file__).parent
REPO_DIR    = SCANNER_DIR.parent
CACHE_DIR   = REPO_DIR / "cache"
REPORTS_DIR = REPO_DIR / "reports"
STREAK_FILE = SCANNER_DIR / "streak_tracker.csv"
INDEX_FILE  = REPO_DIR / "index.html"

CACHE_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# ── Config ────────────────────────────────────────────────────────────────────
GITHUB_USER   = "goyaliet"
GITHUB_REPO   = "nse-accumulation-scanner"
PAGES_URL     = f"https://{GITHUB_USER}.github.io/{GITHUB_REPO}/"
CACHE_DAYS    = 30
HISTORY_DAYS  = 22     # ~20 trading days window

# Pre-score filters
MIN_TURNOVER_CR  = 1.0    # Rs. 1 Crore daily turnover
MIN_PRICE        = 30.0   # Rs. 30 minimum price
MIN_AVG_VOLUME   = 20000  # 20-day avg volume
MIN_HISTORY_DAYS = 10     # Need at least 10 days to compute averages

# Signal thresholds
STRONG_SCORE   = 70
MODERATE_SCORE = 50

NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Referer":         "https://www.nseindia.com",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-IN,en-US;q=0.9,en;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection":      "keep-alive",
    "DNT":             "1",
}

# ── Logging ───────────────────────────────────────────────────────────────────
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {level:5s} {msg}", flush=True)

def warn(msg):  log(msg, "WARN")
def err(msg):   log(msg, "ERROR")

# ── NSE Data Download ─────────────────────────────────────────────────────────
def get_nse_session():
    """Create a browser-like requests session for NSE."""
    session = requests.Session()
    session.headers.update(NSE_HEADERS)
    try:
        log("Establishing NSE session (fetching cookies)...")
        session.get("https://www.nseindia.com", timeout=15)
        time.sleep(1.5)
    except Exception as e:
        warn(f"Could not warm up NSE session: {e}")
    return session


def download_bhav(session, trade_date, retry=True):
    """
    Download sec_bhavdata_full CSV for a given date.
    Returns (bytes_content, error_string).
    """
    date_str = trade_date.strftime("%d%b%Y").upper()
    url = (
        f"https://nsearchives.nseindia.com/products/content/"
        f"sec_bhavdata_full_{date_str}.csv"
    )
    log(f"Downloading NSE data for {trade_date.strftime('%Y-%m-%d')} ...")
    try:
        resp = session.get(url, timeout=45)
        if resp.status_code == 404:
            return None, f"404 — Market holiday or data not yet published for {trade_date.strftime('%d-%b-%Y')}"
        if resp.status_code != 200:
            return None, f"HTTP {resp.status_code} from NSE"
        if len(resp.content) < 2000:
            return None, "Response too small — likely error page (NSE anti-scraping)"
        log(f"Downloaded {len(resp.content)//1024} KB")
        return resp.content, None
    except requests.Timeout:
        if retry:
            warn("Timeout — retrying in 15 seconds...")
            time.sleep(15)
            return download_bhav(session, trade_date, retry=False)
        return None, "Request timed out"
    except Exception as e:
        return None, str(e)


def parse_bhav(content):
    """Parse NSE sec_bhavdata_full CSV into a clean DataFrame."""
    df = pd.read_csv(StringIO(content.decode("utf-8", errors="replace")))
    df.columns = df.columns.str.strip().str.upper()

    # Keep EQ series only (exclude BE/BZ/SME etc.)
    if "SERIES" in df.columns:
        df = df[df["SERIES"].str.strip() == "EQ"].copy()
    else:
        log("Warning: SERIES column not found in NSE data")

    # Standardise column names
    col_map = {
        "SYMBOL":        "symbol",
        "OPEN_PRICE":    "open",
        "HIGH_PRICE":    "high",
        "LOW_PRICE":     "low",
        "CLOSE_PRICE":   "close",
        "PREV_CLOSE":    "prev_close",
        "TTL_TRD_QNTY":  "volume",
        "DELIV_QTY":     "deliv_qty",
        "DELIV_PER":     "deliv_pct",
        "TURNOVER_LACS": "turnover_lacs",
        "NO_OF_TRADES":  "trades",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # Numeric cleanup
    for col in ["open", "high", "low", "close", "prev_close",
                 "volume", "deliv_qty", "deliv_pct", "turnover_lacs", "trades"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ""), errors="coerce"
            )

    df["turnover_cr"] = df.get("turnover_lacs", pd.Series(dtype=float)) / 100
    df["symbol"] = df["symbol"].str.strip()

    df = df.dropna(subset=["symbol", "close", "volume"])
    df = df[df["close"] > 0]
    log(f"Parsed {len(df)} EQ stocks from NSE data")
    return df


# ── Cache Management ──────────────────────────────────────────────────────────
def save_to_cache(df, trade_date):
    cache_path = CACHE_DIR / f"{trade_date.strftime('%Y%m%d')}.csv"
    df.to_csv(cache_path, index=False)
    log(f"Saved to cache: {cache_path.name}")


def load_cache(exclude_date_str=None):
    """Load all cached daily CSVs. Returns dict {date_str: df}."""
    cache = {}
    for f in sorted(CACHE_DIR.glob("????????.csv")):
        if exclude_date_str and f.stem == exclude_date_str:
            continue
        try:
            cache[f.stem] = pd.read_csv(f)
        except Exception as e:
            warn(f"Could not load {f.name}: {e}")
    return cache


def clean_old_cache():
    cutoff = datetime.now() - timedelta(days=CACHE_DAYS + 5)
    removed = 0
    for f in CACHE_DIR.glob("????????.csv"):
        try:
            if datetime.strptime(f.stem, "%Y%m%d") < cutoff:
                f.unlink()
                removed += 1
        except:
            pass
    if removed:
        log(f"Removed {removed} old cache files")


def build_history(cache):
    """Compute per-symbol 20-day avg volume and avg close from cache."""
    if not cache:
        return pd.DataFrame(columns=["symbol", "avg_volume", "avg_close", "days"])

    dfs = []
    for date_str, df in sorted(cache.items())[-HISTORY_DAYS:]:
        subset = df[["symbol"]].copy()
        for col in ["volume", "close"]:
            if col in df.columns:
                subset[col] = pd.to_numeric(df[col], errors="coerce")
        subset["date"] = date_str
        dfs.append(subset)

    hist = pd.concat(dfs, ignore_index=True)
    stats = hist.groupby("symbol").agg(
        avg_volume=("volume", "mean"),
        avg_close=("close",  "mean"),
        days=("date", "count"),
    ).reset_index()
    return stats


# ── Scoring Engine ────────────────────────────────────────────────────────────
def score_stocks(today_df, history_df):
    """
    Score each stock on 4 signals (total 100 pts):
      1. Delivery %      — 35 pts  (high delivery = institutional, not intraday)
      2. Volume Surge    — 30 pts  (vol vs 20-day avg)
      3. Price Strength  — 20 pts  (close vs day high — buying with conviction)
      4. Trend Alignment — 15 pts  (close vs 20-day moving average)
    """
    df = today_df.merge(history_df, on="symbol", how="left")

    # ── Filters ───────────────────────────────────────────────────────────────
    df = df[df["turnover_cr"].fillna(0)   >= MIN_TURNOVER_CR]
    df = df[df["close"].fillna(0)         >= MIN_PRICE]
    df = df[df["days"].fillna(0)          >= MIN_HISTORY_DAYS]
    df = df[df["avg_volume"].fillna(0)    >= MIN_AVG_VOLUME]

    results = []
    for _, r in df.iterrows():
        deliv_pct     = float(r.get("deliv_pct",    0) or 0)
        volume        = float(r.get("volume",        0) or 0)
        avg_vol       = float(r.get("avg_volume",    1) or 1)
        c,ose         = float(r.get("close",         0) or 0)
        high          = float(r.get("high",       close) or close)
        avg_close     = float(r.get("avg_close",  close) or close)

        vol_surge       = volume / avg_vol      if avg_vol   > 0 else 0
        price_vs_high   = close  / high         if high      > 0 else 1
        price_vs_20dma  = close  / avg_close    if avg_close > 0 else 1

        # ── Signal 1: Delivery % (35 pts) ─────────────────────────────────────
        if   deliv_pct >= 80: d = 35
        elif deliv_pct >= 70: d = 28
        elif deliv_pct >= 60: d = 21
        elif deliv_pct >= 50: d = 14
        else:                  d = 0

        # ── Signal 2: Volume Surge (30 pts) ───────────────────────────────────
        if   vol_surge >= 3.0: v = 30
        elif vol_surge >= 2.0: v = 22
        elif vol_surge >= 1.5: v = 15
        elif vol_surge >= 1.2: v = 8
        else:                   v = 0

        # ── Signal 3: Price Strength / Close vs Day High (20 pts) ─────────────
        if   price_vs_high >= 0.97: p = 20
        elif price_vs_high >= 0.93: p = 15
        elif price_vs_high >= 0.88: p = 8
        else:                        p = 0

        # ── Signal 4: Trend Alignment — Close vs 20 DMA (15 pts) ──────────────
        if   price_vs_20dma >= 1.00: t = 15
        elif price_vs_20dma >= 0.97: t = 8
        else:                         t = 0

        total = d + v + p + t

        results.append({
            "symbol":         r["symbol"],
            "cmp":            round(close, 2),
            "open":           round(float(r.get("open", 0) or 0), 2),
            "high":           round(high, 2),
            "low":            round(float(r.get("low",  0) or 0), 2),
            "volume":         int(volume),
            "avg_volume":     int(avg_vol),
            "vol_surge":      round(vol_surge, 2),
            "deliv_pct":      round(deliv_pct, 1),
            "turnover_cr":    round(float(r.get("turnover_cr", 0) or 0), 2),
            "vs_20dma_pct":   round((price_vs_20dma - 1) * 100, 1),
            "days_history":   int(r.get("days", 0) or 0),
            "d_score":        d,
            "v_score":        v,
            "p_score":        p,
            "t_score":        t,
            "total_score":    total,
        })

    result_df = pd.DataFrame(results).sort_values("total_score", ascending=False)
    log(f"Scored {len(result_df)} stocks. "
        f"Strong: {(result_df['total_score'] >= STRONG_SCORE).sum()}, "
        f"Moderate: {((result_df['total_score'] >= MODERATE_SCORE) & (result_df['total_score'] < STRONG_SCORE)).sum()}")
    return result_df


# ── Streak Tracker ────────────────────────────────────────────────────────────
def load_streaks():
    if STREAK_FILE.exists():
        try:
            return pd.read_csv(STREAK_FILE)
        except:
            pass
    return pd.DataFrame(columns=["symbol", "streak", "last_date"])


def update_and_save_streaks(scored_df, today_str):
    """Update multi-day accumulation streaks. Returns df with streak column added."""
    old = load_streaks()
    streak_map = {}
    if not old.empty:
        for _, row in old.iterrows():
            streak_map[str(row["symbol"])] = {
                "streak":    int(row["streak"]),
                "last_date": str(row["last_date"]),
            }

    today_dt = datetime.strptime(today_str, "%Y%m%d")
    new_rows  = []
    streak_out = {}

    for _, row in scored_df.iterrows():
        sym   = row["symbol"]
        score = row["total_score"]
        prev  = streak_map.get(sym)

        if score >= MODERATE_SCORE:
            if prev:
                try:
                    gap = (today_dt - datetime.strptime(prev["last_date"], "%Y%m%d")).days
                    streak = prev["streak"] + 1 if gap <= 5 else 1
                except:
                    streak = 1
            else:
                streak = 1
        else:
            streak = 0

        if streak > 0:
            new_rows.append({"symbol": sym, "streak": streak, "last_date": today_str})
            streak_out[sym] = streak

    new_streak_df = pd.DataFrame(new_rows)
    if not new_streak_df.empty:
        new_streak_df.to_csv(STREAK_FILE, index=False)

    scored_df["streak"] = scored_df["symbol"].map(streak_out).fillna(0).astype(int)
    return scored_df


# ── HTML Report Generator ─────────────────────────────────────────────────────
def score_color(score):
    if score >= STRONG_SCORE:   return "#1D6F42"   # green
    if score >= MODERATE_SCORE: return "#E67E00"   # amber
    return "#888888"                                # grey


def score_label(score):
    if score >= STRONG_SCORE:   return "STRONG"
    if score >= MODERATE_SCORE: return "MODERATE"
    return "WEAK"


def fmt_vol(n):
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.0f}K"
    return str(n)


def make_bar(value, max_val, color):
    pct = min(100, int(value / max_val * 100))
    return (
        f'<div style="background:#e0e0e0;border-radius:3px;height:6px;width:100%;margin-top:4px">'
        f'<div style="background:{color};width:{pct}%;height:6px;border-radius:3px"></div></div>'
    )


def build_card(row, rank):
    """Build a mobile stock card."""
    score     = int(row["total_score"])
    sc        = score_color(score)
    streak    = int(row.get("streak", 0))
    streak_html = f'<span class="streak-badge">🔥 {streak}d streak</span>' if streak >= 3 else ""
    vs20      = row["vs_20dma_pct"]
    vs20_col  = "#1D6F42" if vs20 >= 0 else "#CC0000"
    vs20_str  = f"+{vs20:.1f}%" if vs20 >= 0 else f"{vs20:.1f}%"

    return f"""
    <div class="card">
      <div class="card-header">
        <div>
          <span class="rank">#{rank}</span>
          <span class="symbol">{row['symbol']}</span>
          {streak_html}
        </div>
        <div class="score-badge" style="background:{sc}">
          <span class="score-num">{score}</span>
          <span class="score-lbl">{score_label(score)}</span>
        </div>
      </div>
      <div class="card-metrics">
        <div class="metric"><span class="mlabel">CMP</span><span class="mval">₹{row['cmp']:,.2f}</span></div>
        <div class="metric"><span class="mlabel">Delivery</span><span class="mval">{row['deliv_pct']:.1f}%</span></div>
        <div class="metric"><span class="mlabel">Vol Surge</span><span class="mval">{row['vol_surge']:.2f}x</span></div>
        <div class="metric"><span class="mlabel">vs 20DMA</span><span class="mval" style="color:{vs20_col}">{vs20_str}</span></div>
        <div class="metric"><span class="mlabel">Volume</span><span class="mval">{fmt_vol(row['volume'])}</span></div>
        <div class="metric"><span class="mlabel">Turnover</span><span class="mval">₹{row['turnover_cr']:.1f}Cr</span></div>
      </div>
      <div class="score-breakdown">
        <div class="sb-item">
          <span class="sb-label">Delivery ({row['d_score']}/35)</span>
          {make_bar(row['d_score'], 35, '#1C6B3A')}
        </div>
        <div class="sb-item">
          <span class="sb-label">Volume ({row['v_score']}/30)</span>
          {make_bar(row['v_score'], 30, '#1C4B8A')}
        </div>
        <div class="sb-item">
          <span class="sb-label">Price ({row['p_score']}/20)</span>
          {make_bar(row['p_score'], 20, '#8A6B1C')}
        </div>
        <div class="sb-item">
          <span class="sb-label">Trend ({row['t_score']}/15)</span>
          {make_bar(row['t_score'], 15, '#6B1C8A')}
        </div>
      </div>
    </div>"""


def build_table_row(row, rank):
    score  = int(row["total_score"])
    sc     = score_color(score)
    streak = int(row.get("streak", 0))
    streak_html = f'<span class="streak-badge">🔥{streak}d</span>' if streak >= 3 else ""
    vs20   = row["vs_20dma_pct"]
    vs20_col = "#1D6F42" if vs20 >= 0 else "#CC0000"
    vs20_str = f"+{vs20:.1f}%" if vs20 >= 0 else f"{vs20:.1f}%"
    return f"""
    <tr>
      <td class="rank-cell">#{rank}</td>
      <td class="sym-cell"><strong>{row['symbol']}</strong>{streak_html}</td>
      <td>₹{row['cmp']:,.2f}</td>
      <td><strong>{row['deliv_pct']:.1f}%</strong></td>
      <td><strong>{row['vol_surge']:.2f}x</strong></td>
      <td style="color:{vs20_col}">{vs20_str}</td>
      <td>{fmt_vol(row['volume'])}</td>
      <td>₹{row['turnover_cr']:.1f}Cr</td>
      <td>
        <span class="score-pill" style="background:{sc}">{score}</span>
        <div style="display:flex;gap:2px;margin-top:4px">
          <div title="Delivery" style="background:#1C6B3A;height:4px;width:{int(row['d_score']/35*40)}px;border-radius:2px"></div>
          <div title="Volume"   style="background:#1C4B8A;height:4px;width:{int(row['v_score']/30*40)}px;border-radius:2px"></div>
          <div title="Price"    style="background:#8A6B1C;height:4px;width:{int(row['p_score']/20*40)}px;border-radius:2px"></div>
          <div title="Trend"    style="background:#6B1C8A;height:4px;width:{int(row['t_score']/15*40)}px;border-radius:2px"></div>
        </div>
      </td>
    </tr>"""


def generate_html(scored_df, report_date, report_archive, stats):
    """
    Generate a fully self-contained, mobile-first HTML report.
    scored_df      — DataFrame filtered to signals worth showing
    report_date    — datetime object for today
    report_archive — list of (date_str, filename) for past reports
    stats          — dict with summary stats
    """
    date_display = report_date.strftime("%A, %d %B %Y")
    strong  = scored_df[scored_df["total_score"] >= STRONG_SCORE]
    watchlist = scored_df[scored_df["total_score"] >= MODERATE_SCORE]

    # Build cards (top 20)
    top_cards = ""
    for rank, (_, row) in enumerate(watchlist.head(20).iterrows(), 1):
        top_cards += build_card(row, rank)

    # Build table rows (all watchlist)
    table_rows = ""
    for rank, (_, row) in enumerate(watchlist.iterrows(), 1):
        table_rows += build_table_row(row, rank)

    # Archive dropdown options
    archive_opts = ""
    for ds, fname in sorted(report_archive, reverse=True)[:30]:
        label = datetime.strptime(ds, "%Y%m%d").strftime("%d %b %Y")
        archive_opts += f'<option value="reports/{fname}">{label}</option>'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta name="description" content="NSE Institutional Accumulation Scanner — Daily stock watchlist by delivery % and volume surge">
<title>NSE Accumulation Scanner — {report_date.strftime('%d %b %Y')}</title>
<style>
  :root {{
    --navy:  #1C2A3A;
    --orange:#D4541A;
    --green: #1D6F42;
    --amber: #E67E00;
    --card:  #ffffff;
    --bg:    #F0F4F8;
    --text:  #1a1a2e;
    --muted: #666;
    --border:#e0e0e0;
  }}
  @media (prefers-color-scheme: dark) {{
    :root {{
      --card: #1e2535;
      --bg:   #111827;
      --text: #e8eaf0;
      --muted:#aab;
      --border:#2a3550;
    }}
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
          background: var(--bg); color: var(--text); font-size: 14px; }}

  /* ── Header ── */
  .header {{ background: var(--navy); color: #fff; padding: 16px 20px; }}
  .header-top {{ display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }}
  .site-title {{ font-size: 18px; font-weight: 700; letter-spacing: 0.5px; }}
  .site-title span {{ color: var(--orange); }}
  .header-date {{ font-size: 12px; opacity: 0.75; }}
  .stat-bar {{ display: flex; gap: 16px; margin-top: 12px; flex-wrap: wrap; }}
  .stat-item {{ text-align: center; }}
  .stat-num {{ font-size: 22px; font-weight: 700; }}
  .stat-num.green {{ color: #4ade80; }}
  .stat-num.amber {{ color: #fbbf24; }}
  .stat-lbl {{ font-size: 10px; opacity: 0.65; text-transform: uppercase; letter-spacing: 0.5px; }}

  /* ── Nav / Archive ── */
  .nav-bar {{ background: var(--navy); border-top: 1px solid rgba(255,255,255,0.1);
              padding: 8px 20px; display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }}
  .nav-bar label {{ color: #ccc; font-size: 12px; }}
  .archive-select {{ background: #2a3a4e; color: #fff; border: 1px solid rgba(255,255,255,0.2);
                     border-radius: 6px; padding: 5px 10px; font-size: 12px; cursor: pointer; }}
  .pages-link {{ color: var(--orange); font-size: 12px; text-decoration: none; margin-left: auto; }}

  /* ── Section titles ── */
  .section {{ padding: 16px 16px 8px; }}
  .section-title {{ font-size: 13px; font-weight: 700; text-transform: uppercase;
                    letter-spacing: 0.8px; color: var(--muted); margin-bottom: 12px;
                    display: flex; align-items: center; gap: 8px; }}
  .section-title::after {{ content: ''; flex: 1; height: 1px; background: var(--border); }}

  /* ── Score legend ── */
  .legend {{ display: flex; gap: 16px; padding: 0 16px 12px; flex-wrap: wrap; }}
  .legend-item {{ display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--muted); }}
  .legend-dot {{ width: 10px; height: 10px; border-radius: 50%; }}

  /* ── Cards (mobile default) ── */
  .cards-grid {{ display: grid; grid-template-columns: 1fr; gap: 12px; padding: 0 16px 16px; }}
  @media (min-width: 600px)  {{ .cards-grid {{ grid-template-columns: repeat(2, 1fr); }} }}
  @media (min-width: 900px)  {{ .cards-grid {{ grid-template-columns: repeat(3, 1fr); }} }}
  @media (min-width: 1200px) {{ .cards-grid {{ grid-template-columns: repeat(4, 1fr); }} }}

  .card {{ background: var(--card); border-radius: 12px;
           box-shadow: 0 2px 8px rgba(0,0,0,0.08); padding: 14px; }}
  .card-header {{ display: flex; justify-content: space-between; align-items: flex-start;
                  margin-bottom: 10px; }}
  .rank {{ font-size: 11px; color: var(--muted); margin-right: 4px; }}
  .symbol {{ font-size: 16px; font-weight: 700; }}
  .streak-badge {{ display: inline-block; background: #fff3cd; color: #856404;
                   border-radius: 10px; padding: 2px 7px; font-size: 10px;
                   font-weight: 600; margin-left: 6px; white-space: nowrap; }}
  @media (prefers-color-scheme: dark) {{
    .streak-badge {{ background: #3d2d00; color: #fbbf24; }}
  }}

  .score-badge {{ display: flex; flex-direction: column; align-items: center;
                  border-radius: 10px; padding: 6px 12px; min-width: 64px; color: #fff; }}
  .score-num {{ font-size: 22px; font-weight: 800; line-height: 1; }}
  .score-lbl {{ font-size: 9px; opacity: 0.85; text-transform: uppercase; letter-spacing: 0.5px; }}

  .card-metrics {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px;
                   margin-bottom: 10px; }}
  .metric {{ display: flex; flex-direction: column; }}
  .mlabel {{ font-size: 9px; text-transform: uppercase; color: var(--muted);
             letter-spacing: 0.5px; }}
  .mval {{ font-size: 13px; font-weight: 600; margin-top: 1px; }}

  .score-breakdown {{ border-top: 1px solid var(--border); padding-top: 8px;
                      display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; }}
  .sb-item {{ font-size: 10px; color: var(--muted); }}
  .sb-label {{ display: block; margin-bottom: 2px; }}

  /* ── Table (hidden on mobile, visible on md+) ── */
  .table-wrap {{ overflow-x: auto; padding: 0 16px 24px; display: none; }}
  @media (min-width: 768px) {{ .table-wrap {{ display: block; }} .cards-grid {{ display: none; }} }}

  table {{ width: 100%; border-collapse: collapse; background: var(--card);
           border-radius: 12px; overflow: hidden;
           box-shadow: 0 2px 8px rgba(0,0,0,0.08); }}
  th {{ background: var(--navy); color: #fff; padding: 10px 12px;
        text-align: left; font-size: 11px; text-transform: uppercase;
        letter-spacing: 0.5px; cursor: pointer; white-space: nowrap; }}
  th:hover {{ background: #253648; }}
  td {{ padding: 10px 12px; border-bottom: 1px solid var(--border);
        font-size: 13px; vertical-align: middle; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: rgba(212,84,26,0.04); }}
  tr:nth-child(even) td {{ background: rgba(0,0,0,0.015); }}
  @media (prefers-color-scheme: dark) {{
    tr:nth-child(even) td {{ background: rgba(255,255,255,0.02); }}
  }}

  .rank-cell {{ color: var(--muted); font-size: 12px; width: 36px; }}
  .sym-cell {{ white-space: nowrap; }}
  .score-pill {{ display: inline-block; color: #fff; font-weight: 700; font-size: 13px;
                 border-radius: 6px; padding: 3px 8px; min-width: 36px; text-align: center; }}

  /* ── Score filter bar ── */
  .filter-bar {{ padding: 12px 16px; display: flex; gap: 10px; flex-wrap: wrap;
                 align-items: center; }}
  .filter-btn {{ border: 1px solid var(--border); background: var(--card); color: var(--text);
                 border-radius: 20px; padding: 5px 14px; font-size: 12px; cursor: pointer;
                 transition: all .15s; }}
  .filter-btn.active, .filter-btn:hover {{ background: var(--navy); color: #fff; border-color: var(--navy); }}

  /* ── Score legend bar ── */
  .score-key {{ display: flex; gap: 20px; padding: 8px 16px 0; flex-wrap: wrap; }}
  .sk-item {{ display: flex; align-items: center; gap: 6px; font-size: 11px; color: var(--muted); }}
  .sk-box {{ width: 28px; height: 10px; border-radius: 3px; }}
  .sk-key {{ display: inline-block; width: 10px; height: 10px; border-radius: 2px; }}

  /* ── Footer ── */
  .footer {{ text-align: center; padding: 20px 16px 36px; font-size: 11px;
             color: var(--muted); border-top: 1px solid var(--border); margin-top: 8px; }}
</style>
</head>
<body>

<div class="header">
  <div class="header-top">
    <div class="site-title">NSE <span>Accumulation</span> Scanner</div>
    <div class="header-date">{date_display}</div>
  </div>
  <div class="stat-bar">
    <div class="stat-item">
      <div class="stat-num">{stats['total_scanned']:,}</div>
      <div class="stat-lbl">Stocks Scanned</div>
    </div>
    <div class="stat-item">
      <div class="stat-num green">{stats['strong_count']}</div>
      <div class="stat-lbl">Strong Signals</div>
    </div>
    <div class="stat-item">
      <div class="stat-num amber">{stats['moderate_count']}</div>
      <div class="stat-lbl">Moderate Signals</div>
    </div>
    <div class="stat-item">
      <div class="stat-num">{stats['streak_3plus']}</div>
      <div class="stat-lbl">3+ Day Streaks 🔥</div>
    </div>
  </div>
</div>

<div class="nav-bar">
  <label for="archive">Past Reports:</label>
  <select class="archive-select" id="archive" onchange="if(this.value)window.location.href=this.value">
    <option value="">— Select date —</option>
    {archive_opts}
  </select>
  <a class="pages-link" href="{PAGES_URL}" target="_blank">🔗 Share Link</a>
</div>

<div class="score-key">
  <div class="sk-item"><span class="sk-key" style="background:#1C6B3A"></span> Delivery (35)</div>
  <div class="sk-item"><span class="sk-key" style="background:#1C4B8A"></span> Volume (30)</div>
  <div class="sk-item"><span class="sk-key" style="background:#8A6B1C"></span> Price (20)</div>
  <div class="sk-item"><span class="sk-key" style="background:#6B1C8A"></span> Trend (15)</div>
  <div class="sk-item"><span class="sk-key" style="background:#1D6F42"></span> Strong ≥70</div>
  <div class="sk-item"><span class="sk-key" style="background:#E67E00"></span> Moderate 50-69</div>
</div>

<div class="filter-bar">
  <button class="filter-btn active" onclick="filterScore(0, this)">All ({len(watchlist)})</button>
  <button class="filter-btn" onclick="filterScore(70, this)">Strong ≥70 ({len(strong)})</button>
  <button class="filter-btn" onclick="filterScore(3, this)">🔥 3+ Streak ({stats['streak_3plus']})</button>
</div>

<!-- Mobile: Cards -->
<div class="cards-grid" id="cardsGrid">
{top_cards}
</div>

<!-- Desktop: Table -->
<div class="table-wrap">
  <table id="mainTable">
    <thead>
      <tr>
        <th>#</th>
        <th onclick="sortTable(1)">Symbol ↕</th>
        <th onclick="sortTable(2)">CMP ↕</th>
        <th onclick="sortTable(3)">Delivery% ↕</th>
        <th onclick="sortTable(4)">Vol Surge ↕</th>
        <th onclick="sortTable(5)">vs 20DMA ↕</th>
        <th onclick="sortTable(6)">Volume ↕</th>
        <th onclick="sortTable(7)">Turnover ↕</th>
        <th onclick="sortTable(8)">Score ↕</th>
      </tr>
    </thead>
    <tbody id="tableBody">
{table_rows}
    </tbody>
  </table>
</div>

<div class="footer">
  <p>Data source: NSE India (sec_bhavdata_full). Scores are algorithmic signals, not investment advice.</p>
  <p style="margin-top:6px">Delivery 35pts · Volume Surge 30pts · Price Strength 20pts · Trend 15pts</p>
  <p style="margin-top:6px">Generated {datetime.now().strftime('%d %b %Y %H:%M IST')} · <a href="{PAGES_URL}" style="color:var(--orange)">NSE Accumulation Scanner</a></p>
</div>

<script>
// ── Sort table ──
let sortDir = {{}};
function sortTable(col) {{
  const tb = document.getElementById('tableBody');
  const rows = Array.from(tb.querySelectorAll('tr'));
  sortDir[col] = !sortDir[col];
  rows.sort((a, b) => {{
    let av = a.cells[col].textContent.replace(/[₹,%x🔥d]/g,'').trim();
    let bv = b.cells[col].textContent.replace(/[b��,%x🔥d]/g,'').trim();
    let an = parseFloat(av), bn = parseFloat(bv);
    if (!isNaN(an) && !isNaN(bn)) return sortDir[col] ? bn-an : an-bn;
    return sortDir[col] ? bv.localeCompare(av) : av.localeCompare(bv);
  }});
  rows.forEach(r => tb.appendChild(r));
}}

// ── Filter ──
function filterScore(minScore, btn) {{
  document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const rows = document.querySelectorAll('#tableBody tr');
  rows.forEach(r => {{
    if (minScore === 3) {{
      r.style.display = r.innerHTML.includes('🔥') ? '' : 'none';
    }} else {{
      const score = parseInt(r.querySelector('.score-pill')?.textContent || '0');
      r.style.display = score >= minScore ? '' : 'none';
    }}
  }});
}}
</script>

</body>
</html>"""
    return html


# ── Git Push ──────────────────────────────────────────────────────────────────
def git_push(report_date):
    """Commit and push the updated report to GitHub."""
    try:
        date_str = report_date.strftime("%Y-%m-%d")
        subprocess.run(["git", "-C", str(REPO_DIR), "add", "-A"], check=True)
        result = subprocess.run(
            ["git", "-C", str(REPO_DIR), "diff", "--cached", "--quiet"],
            capture_output=True
        )
        if result.returncode == 0:
            log("No changes to commit — report unchanged")
            return True
        subprocess.run(
            ["git", "-C", str(REPO_DIR), "commit",
             "-m", f"NSE Scanner: {date_str} accumulation report"],
            check=True
        )
        subprocess.run(
            ["git", "-C", str(REPO_DIR), "push", "origin", "main"],
            check=True
        )
        log(f"✅ Pushed to GitHub — live at {PAGES_URL}")
        return True
    except subprocess.CalledProcessError as e:
        err(f"Git push failed: {e}")
        return False


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NSE Institutional Accumulation Scanner")
    parser.add_argument("--date",     default=None, help="Date to run for (YYYY-MM-DD). Default: today.")
    parser.add_argument("--no-push",  action="store_true", help="Skip git push (local testing)")
    parser.add_argument("--no-cache", action="store_true", help="Re-download even if cached")
    args = parser.parse_args()

    if args.date:
        trade_date = datetime.strptime(args.date, "%Y-%m-%d")
    else:
        trade_date = datetime.now()
        # If market not closed yet (before 6:30 PM IST), warn
        if trade_date.hour < 18:
            warn("It's before 6:30 PM IST — NSE data may not be published yet. Proceeding anyway.")

    today_str = trade_date.strftime("%Y%m%d")
    log(f"=== NSE Accumulation Scanner for {trade_date.strftime('%d %b %Y')} ===")

    # ── Step 1: Download today's data ─────────────────────────────────────────
    cache_file = CACHE_DIR / f"{today_str}.csv"
    if cache_file.exists() and not args.no_cache:
        log("Using cached data for today")
        today_df = pd.read_csv(cache_file)
    else:
        session = get_nse_session()
        content, error = download_bhav(session, trade_date)
        if error:
            err(error)
            # Retry for previous trading day if today's data unavailable
            prev_day = trade_date - timedelta(days=1)
            log(f"Trying previous trading day: {prev_day.strftime('%Y-%m-%d')}")
            content, error2 = download_bhav(session, prev_day)
            if error2:
                err(f"Also failed for {prev_day.strftime('%Y-%m-%d')}: {error2}")
                log("EXITING: No NSE data available. Try again after 6:30 PM IST.")
                sys.exit(0)
            trade_date = prev_day
            today_str  = trade_date.strftime("%Y%m%d")
        today_df = parse_bhav(content)
        save_to_cache(today_df, trade_date)

    # ── Step 2: Load historical cache + build averages ─────────────────────────
    clean_old_cache()
    cache = load_cache(exclude_date_str=today_str)
    log(f"Loaded {len(cache)} days of historical cache")
    history_df = build_history(cache)

    # ── Step 3: Score stocks ───────────────────────────────────────────────────
    scored_df = score_stocks(today_df, history_df)

    # ── Step 4: Update streaks ─────────────────────────────────────────────────
    scored_df = update_and_save_streaks(scored_df, today_str)

    # ── Step 5: Stats ──────────────────────────────────────────────────────────
    watchlist = scored_df[scored_df["total_score"] >= MODERATE_SCORE]
    stats = {
        "total_scanned":  len(scored_df),
        "strong_count":   int((scored_df["total_score"] >= STRONG_SCORE).sum()),
        "moderate_count": int(((scored_df["total_score"] >= MODERATE_SCORE) &
                               (scored_df["total_score"] < STRONG_SCORE)).sum()),
        "streak_3plus":   int((scored_df.get("streak", 0) >= 3).sum()),
    }

    # ── Step 6: Generate HTML ──────────────────────────────────────────────────
    # Build archive list from existing reports
    archive = [(f.stem, f.name) for f in REPORTS_DIR.glob("????????.html")]

    report_filename = f"{today_str}.html"
    report_path     = REPORTS_DIR / report_filename

    html = generate_html(watchlist, trade_date, archive, stats)
    report_path.write_text(html, encoding="utf-8")
    INDEX_FILE.write_text(html, encoding="utf-8")
    log(f"Generated report: {report_path}")
    log(f"Updated: {INDEX_FILE}")

    # ── Step 7: Git push ───────────────────────────────────────────────────────
    if not args.no_push:
        git_push(trade_date)
    else:
        log("Skipping git push (--no-push flag set)")

    log(f"=== Done. Watchlist: {len(watchlist)} stocks. Strong signals: {stats['strong_count']} ===")


if __name__ == "__main__":
    main()
