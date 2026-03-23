"""
NSE Institutional Accumulation Scanner — Vercel Serverless (zero dependencies)
===============================================================================
Triggered by Vercel Cron at 13:30 UTC (7:00 PM IST) Mon-Fri.
Uses ONLY Python standard library — no pip packages needed.

Required env var:  GITHUB_TOKEN  (PAT with repo contents:write)
"""

from http.server import BaseHTTPRequestHandler
import json, os, base64, csv
from io import StringIO
from urllib.request import Request, urlopen, build_opener, HTTPCookieProcessor
from urllib.error import HTTPError
from urllib.parse import urlparse, parse_qs
from http.cookiejar import CookieJar
from datetime import datetime, timedelta

# ── Config ───────────────────────────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO  = "goyaliet/nse-accumulation-scanner"
PAGES_URL    = "https://goyaliet.github.io/nse-accumulation-scanner/"
GITHUB_API   = "https://api.github.com"

STRONG_SCORE    = 70
MODERATE_SCORE  = 50
MIN_TURNOVER_CR = 1.0
MIN_PRICE       = 30.0
MIN_AVG_VOLUME  = 20000
MIN_HISTORY_DAYS = 10
HISTORY_DAYS    = 22

NSE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ── GitHub helpers ───────────────────────────────────────────────────────────
def _gh_headers():
    return {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "nse-scanner-bot",
    }

def gh_read(path):
    """Read file from GitHub repo.  Returns (text, sha) or (None, None).
    Falls back to raw URL for files >1 MB (GitHub API omits content field)."""
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{path}"
    req = Request(url, headers=_gh_headers())
    try:
        with urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
        sha = d.get("sha")
        content = d.get("content", "")
        if content and d.get("encoding") == "base64":
            return base64.b64decode(content).decode(), sha
        # File >1 MB: API returns metadata only — fetch content via raw URL
        raw_url = (f"https://raw.githubusercontent.com/"
                   f"{GITHUB_REPO}/main/{path}")
        raw_req = Request(raw_url,
                          headers={"Authorization": f"token {GITHUB_TOKEN}"})
        with urlopen(raw_req, timeout=30) as r2:
            return r2.read().decode(), sha
    except HTTPError as e:
        if e.code == 404:
            return None, None
        raise

def gh_write(path, text, msg, sha=None):
    """Create or update a file in the repo."""
    url = f"{GITHUB_API}/repos/{GITHUB_REPO}/contents/{path}"
    body = {"message": msg, "content": base64.b64encode(text.encode()).decode()}
    if sha:
        body["sha"] = sha
    data = json.dumps(body).encode()
    req = Request(url, data=data, headers=_gh_headers(), method="PUT")
    with urlopen(req, timeout=30) as r:
        return json.loads(r.read())

# ── NSE bhavcopy download ───────────────────────────────────────────────────
def _nse_opener():
    """Build a urllib opener that keeps NSE session cookies."""
    jar = CookieJar()
    opener = build_opener(HTTPCookieProcessor(jar))
    opener.addheaders = [
        ("User-Agent", NSE_UA),
        ("Referer", "https://www.nseindia.com"),
        ("Accept", "text/html,application/xhtml+xml,*/*"),
    ]
    # warm-up: hit homepage to get cookies
    try:
        opener.open("https://www.nseindia.com", timeout=10)
    except Exception:
        pass
    return opener

def download_bhav(date=None, opener=None):
    """Download NSE bhavcopy CSV.  Tries today then previous 5 trading days."""
    if opener is None:
        opener = _nse_opener()
    d = date or datetime.utcnow() + timedelta(hours=5, minutes=30)  # IST
    for offset in range(6):
        day = d - timedelta(days=offset)
        if day.weekday() >= 5:
            continue
        ds = day.strftime("%d%m%Y")
        url = (
            f"https://nsearchives.nseindia.com/products/content/"
            f"sec_bhavdata_full_{ds}.csv"
        )
        try:
            resp = opener.open(url, timeout=30)
            raw = resp.read().decode("utf-8", errors="replace")
            if len(raw) > 1000:
                return raw, day.strftime("%Y-%m-%d")
        except Exception:
            continue
    return None, None

def _trading_days_back(n_days):
    """Return list of last n_days weekday dates (newest first), going back 60 cal days."""
    ist = datetime.utcnow() + timedelta(hours=5, minutes=30)
    days = []
    d = ist
    for _ in range(60):
        d -= timedelta(days=1)
        if d.weekday() < 5:
            days.append(d)
        if len(days) >= n_days:
            break
    return days  # newest first

def run_backfill(target_days=16):
    """Download last target_days trading days, build history, score, and commit all inline.
    Completely self-contained — no dependency on gh_read working for history afterward.
    """
    log = []
    opener = _nse_opener()
    log.append(f"Backfill: targeting {target_days} trading days")

    # Collect newest-first so collected_dates[0] = most recent trading day (e.g. Mar 20)
    # Use 2x buffer for Indian market holidays
    candidates = _trading_days_back(target_days * 2)
    history = {}
    collected_dates = []      # newest first
    stocks_by_date = {}

    for day in candidates:  # newest first
        raw, date_str = download_bhav(date=day, opener=opener)
        if not raw:
            log.append(f"Skip {day.strftime('%Y-%m-%d')} (no data / holiday)")
            continue
        stocks = parse_bhav(raw)
        if not stocks:
            log.append(f"Skip {day.strftime('%Y-%m-%d')} (0 EQ stocks parsed)")
            continue
        history = update_history(history, stocks, date_str)
        collected_dates.append(date_str)
        stocks_by_date[date_str] = stocks
        log.append(f"Got {date_str}: {len(stocks)} stocks")
        if len(collected_dates) >= target_days:
            break

    log.append(f"Collected {len(collected_dates)} days, {len(history)} symbols")

    if not collected_dates:
        return dict(ok=False, error="Backfill: no data collected", log=log)

    # collected_dates[0] = most recent day (to score); [-1] = oldest (for commit msg)
    scan_date = collected_dates[0]
    today_stocks = stocks_by_date[scan_date]

    # Commit full history (all collected days)
    oldest, newest = collected_dates[-1], collected_dates[0]
    _, hist_sha = gh_read("cache/rolling_history.csv")
    gh_write("cache/rolling_history.csv",
             history_to_csv(history),
             f"backfill {oldest} to {newest}", hist_sha)
    log.append("Wrote rolling_history.csv")

    # Score scan_date against averages built from the OTHER days (cleaner signal)
    hist_excl = {sym: [r for r in rows if r["date"] != scan_date]
                 for sym, rows in history.items()}
    hist_excl = {sym: rows for sym, rows in hist_excl.items() if rows}
    avgs = build_averages(hist_excl)
    log.append(f"Averages: {len(avgs)} symbols with >={MIN_HISTORY_DAYS} days")
    scored = score_stocks(today_stocks, avgs)
    log.append(f"Scored: {len(scored)} hits on {scan_date}")

    # Apply streaks, generate HTML, write all outputs
    strk_csv, strk_sha = gh_read("scanner/streak_tracker.csv")
    new_strk = apply_streaks(scored, strk_csv)
    mx = max((len(r) for r in history.values()), default=0)
    html = generate_html(scored, scan_date, dict(total=len(today_stocks), hist_days=mx))

    gh_write("scanner/streak_tracker.csv", new_strk, f"streaks {scan_date}", strk_sha)
    ds = scan_date.replace("-", "")
    _, rep_sha = gh_read(f"reports/{ds}.html")
    gh_write(f"reports/{ds}.html", html, f"report {scan_date}", rep_sha)
    _, idx_sha = gh_read("index.html")
    gh_write("index.html", html, f"index {scan_date}", idx_sha)
    arch_txt, arch_sha = gh_read("cache/archive_index.json")
    archive = json.loads(arch_txt) if arch_txt else []
    entry = dict(date=scan_date, file=f"reports/{ds}.html",
                 strong=sum(1 for s in scored if s["grade"] == "STRONG"),
                 moderate=sum(1 for s in scored if s["grade"] == "MODERATE"))
    archive = [a for a in archive if a["date"] != scan_date] + [entry]
    archive.sort(key=lambda x: x["date"], reverse=True)
    gh_write("cache/archive_index.json", json.dumps(archive[:90], indent=2),
             f"archive {scan_date}", arch_sha)
    log.append(f"Result: {entry['strong']} strong, {entry['moderate']} moderate")

    return dict(ok=True, backfill=True, days_collected=len(collected_dates),
                date=scan_date, url=PAGES_URL,
                strong=entry["strong"], moderate=entry["moderate"],
                total=len(today_stocks), log=log)

def parse_bhav(raw):
    """Parse bhavcopy into list of stock dicts (EQ series only)."""
    reader = csv.DictReader(StringIO(raw))
    out = []
    for row in reader:
        try:
            if row.get(" SERIES", row.get("SERIES", "")).strip() != "EQ":
                continue
            sym   = row.get(" SYMBOL", row.get("SYMBOL", "")).strip()
            close = float(row.get(" CLOSE_PRICE", row.get("CLOSE_PRICE", 0)))
            prev  = float(row.get(" PREV_CLOSE", row.get("PREV_CLOSE", close)))
            vol   = int(float(row.get(" TTL_TRD_QNTY", row.get("TTL_TRD_QNTY", 0))))
            dq    = int(float(row.get(" DELIV_QTY", row.get("DELIV_QTY", 0))))
            dp    = float(row.get(" DELIV_PER", row.get("DELIV_PER", 0)))
            if close < MIN_PRICE or vol < 1000:
                continue
            if close * vol / 1e7 < MIN_TURNOVER_CR:
                continue
            out.append(dict(
                symbol=sym, close=close, prev_close=prev,
                volume=vol, deliv_qty=dq, deliv_pct=dp,
            ))
        except (ValueError, KeyError, TypeError):
            continue
    return out

# ── Rolling history ──────────────────────────────────────────────────────────
def load_history(text):
    h = {}
    if not text:
        return h
    for row in csv.DictReader(StringIO(text)):
        sym = row["symbol"]
        h.setdefault(sym, []).append(dict(
            date=row["date"], close=float(row["close"]),
            volume=int(float(row["volume"])),
            deliv_qty=int(float(row["deliv_qty"])),
            deliv_pct=float(row["deliv_pct"]),
        ))
    return h

def update_history(h, stocks, today):
    for s in stocks:
        sym = s["symbol"]
        rows = h.setdefault(sym, [])
        rows[:] = [r for r in rows if r["date"] != today]
        rows.append(dict(
            date=today, close=s["close"], volume=s["volume"],
            deliv_qty=s["deliv_qty"], deliv_pct=s["deliv_pct"],
        ))
        rows.sort(key=lambda r: r["date"])
        if len(rows) > HISTORY_DAYS:
            rows[:] = rows[-HISTORY_DAYS:]
    return h

def history_to_csv(h):
    buf = StringIO()
    w = csv.writer(buf)
    w.writerow(["symbol","date","close","volume","deliv_qty","deliv_pct"])
    for sym in sorted(h):
        for r in h[sym]:
            w.writerow([sym, r["date"], r["close"], r["volume"],
                        r["deliv_qty"], r["deliv_pct"]])
    return buf.getvalue()

def build_averages(h):
    avgs = {}
    for sym, rows in h.items():
        if len(rows) < MIN_HISTORY_DAYS:
            continue
        recent = rows[-20:]
        n = len(recent)
        avgs[sym] = dict(
            avg_vol   = sum(r["volume"]    for r in recent) / n,
            avg_deliv = sum(r["deliv_pct"] for r in recent) / n,
            avg_close = sum(r["close"]     for r in recent) / n,
            high_20d  = max(r["close"]     for r in recent),
            low_20d   = min(r["close"]     for r in recent),
            days      = len(rows),
        )
    return avgs

# ── Scoring ──────────────────────────────────────────────────────────────────
def score_stocks(stocks, avgs):
    scored = []
    for s in stocks:
        sym = s["symbol"]
        a = avgs.get(sym)
        if not a or a["avg_vol"] < MIN_AVG_VOLUME:
            continue

        score, signals = 0, []
        dp, avg_dp = s["deliv_pct"], a["avg_deliv"]
        vol, avg_v = s["volume"],    a["avg_vol"]
        close      = s["close"]

        # 1) Delivery % — 35 pts
        if avg_dp > 0:
            ratio = dp / avg_dp
            if dp >= 60 and ratio >= 1.3:
                score += 35; signals.append(f"Deliv {dp:.0f}% vs avg {avg_dp:.0f}%")
            elif dp >= 50 and ratio >= 1.15:
                score += 25; signals.append(f"Deliv {dp:.0f}% vs avg {avg_dp:.0f}%")
            elif dp >= 40 and ratio >= 1.05:
                score += 15; signals.append(f"Deliv {dp:.0f}%")

        # 2) Volume surge — 30 pts
        if avg_v > 0:
            vr = vol / avg_v
            if   vr >= 3.0: score += 30; signals.append(f"Vol {vr:.1f}x")
            elif vr >= 2.0: score += 22; signals.append(f"Vol {vr:.1f}x")
            elif vr >= 1.5: score += 15; signals.append(f"Vol {vr:.1f}x")

        # 3) Price strength — 20 pts
        h20 = a["high_20d"]
        if h20 > 0:
            off = (h20 - close) / h20 * 100
            if   off <= 3: score += 20; signals.append("Near 20d high")
            elif off <= 7: score += 12; signals.append(f"{off:.1f}% from high")
        if close > a["avg_close"]:
            score += 5

        # 4) Trend — 15 pts
        if close > a["avg_close"] * 1.02:
            score += 10; signals.append("Uptrend")
        elif close > a["avg_close"]:
            score += 5

        if score < MODERATE_SCORE:
            continue

        chg = (close - s["prev_close"]) / s["prev_close"] * 100 if s["prev_close"] else 0
        scored.append(dict(
            symbol=sym, close=close, chg_pct=round(chg, 2),
            volume=vol, vol_ratio=round(vol/avg_v, 2) if avg_v else 0,
            deliv_pct=round(dp, 1), avg_deliv=round(avg_dp, 1),
            score=score,
            grade="STRONG" if score >= STRONG_SCORE else "MODERATE",
            signals=signals,
        ))

    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored

# ── Streaks ──────────────────────────────────────────────────────────────────
def apply_streaks(scored, csv_text):
    prev = {}
    if csv_text:
        for row in csv.DictReader(StringIO(csv_text)):
            prev[row["symbol"]] = dict(
                streak=int(row.get("streak", 1)),
                last_date=row.get("last_date", ""),
            )
    today = datetime.utcnow().strftime("%Y-%m-%d")
    yest  = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    new = {}
    for s in scored:
        sym = s["symbol"]
        p = prev.get(sym, {"streak": 0, "last_date": ""})
        s["streak"] = (p["streak"] + 1) if p["last_date"] >= yest else 1
        new[sym] = dict(streak=s["streak"], last_date=today)

    buf = StringIO()
    w = csv.writer(buf)
    w.writerow(["symbol","streak","last_date"])
    for sym in sorted(new):
        w.writerow([sym, new[sym]["streak"], new[sym]["last_date"]])
    return buf.getvalue()

# ── HTML generation ──────────────────────────────────────────────────────────
def generate_html(scored, scan_date, stats):
    strong  = sum(1 for s in scored if s["grade"] == "STRONG")
    moderate = sum(1 for s in scored if s["grade"] == "MODERATE")
    display = datetime.strptime(scan_date, "%Y-%m-%d").strftime("%d %b %Y")

    rows = ""
    for i, s in enumerate(scored[:50], 1):
        chg_c = "#4caf50" if s["chg_pct"] >= 0 else "#f44336"
        vol_c = "#4caf50" if s["vol_ratio"] >= 2 else "#ff9800" if s["vol_ratio"] >= 1.5 else "#888"
        gr_bg = "#4caf50" if s["grade"] == "STRONG" else "#ff9800"
        badge = (f' <span style="background:#ff9800;color:#fff;padding:1px 6px;'
                 f'border-radius:8px;font-size:11px">{s["streak"]}d</span>'
                 if s.get("streak", 1) > 1 else "")
        sigs  = ", ".join(s["signals"])
        rows += (
            f'<tr>'
            f'<td class="c">{i}</td>'
            f'<td><b>{s["symbol"]}</b>{badge}</td>'
            f'<td class="r">{s["close"]:.2f}</td>'
            f'<td class="r" style="color:{chg_c}">{s["chg_pct"]:+.2f}%</td>'
            f'<td class="c"><span class="pill" style="background:{gr_bg}">'
            f'{s["score"]}</span></td>'
            f'<td class="r">{s["deliv_pct"]:.0f}%</td>'
            f'<td class="r">{s["avg_deliv"]:.0f}%</td>'
            f'<td class="r" style="color:{vol_c}">{s["vol_ratio"]:.1f}x</td>'
            f'<td class="r">{s["volume"]:,}</td>'
            f'<td class="sig">{sigs}</td>'
            f'</tr>\n'
        )

    empty = ('<tr><td colspan="10" class="empty">No stocks meeting '
             'accumulation criteria today</td></tr>')

    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NSE Accumulation Scanner {display}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;
  background:#0a0a0a;color:#e0e0e0;padding:20px}}
.wrap{{max-width:1400px;margin:0 auto}}
h1{{font-size:22px;color:#fff;margin-bottom:4px}}
.sub{{color:#888;font-size:13px;margin-bottom:16px}}
.bar{{display:flex;gap:12px;margin-bottom:20px;flex-wrap:wrap}}
.st{{background:#1a1a2e;border-radius:8px;padding:12px 18px;min-width:110px}}
.st .n{{font-size:22px;font-weight:700;color:#4fc3f7}}
.st .l{{font-size:11px;color:#888;margin-top:2px}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#1a1a2e;color:#aaa;padding:8px 10px;text-align:left;
  position:sticky;top:0;font-size:11px;text-transform:uppercase}}
td{{padding:7px 10px;border-bottom:1px solid #1a1a2e}}
tr:hover{{background:#111133}}
.c{{text-align:center}}.r{{text-align:right}}
.sig{{font-size:12px;color:#aaa}}
.pill{{color:#fff;padding:2px 8px;border-radius:4px;font-size:12px}}
.empty{{text-align:center;padding:30px;color:#666}}
.leg{{margin-top:20px;padding:14px;background:#1a1a2e;border-radius:8px;
  font-size:12px;color:#888}}
.leg b{{color:#ccc}}
a{{color:#4fc3f7;text-decoration:none}}
</style></head><body>
<div class="wrap">
<h1>NSE Institutional Accumulation Scanner</h1>
<p class="sub">Scan date: {display} &middot; Auto-refreshes 7 PM IST Mon-Fri</p>
<div class="bar">
<div class="st"><div class="n">{strong}</div><div class="l">Strong (&ge;{STRONG_SCORE})</div></div>
<div class="st"><div class="n">{moderate}</div><div class="l">Moderate (&ge;{MODERATE_SCORE})</div></div>
<div class="st"><div class="n">{stats['total']}</div><div class="l">Scanned</div></div>
<div class="st"><div class="n">{stats['hist_days']}</div><div class="l">History Days</div></div>
</div>
<table><thead><tr>
<th>#</th><th>Symbol</th><th>Close</th><th>Chg%</th><th>Score</th>
<th>Deliv%</th><th>Avg Deliv%</th><th>Vol Ratio</th><th>Volume</th>
<th>Signals</th>
</tr></thead><tbody>
{rows if rows else empty}
</tbody></table>
<div class="leg">
<b>Scoring:</b> Delivery % (35) + Volume Surge (30) + Price Strength (20) + Trend (15) = 100 max<br>
<b>Strong &ge;{STRONG_SCORE}</b> &middot; <b>Moderate &ge;{MODERATE_SCORE}</b> &middot;
Streak badge = consecutive signal days<br>
<b>Source:</b> NSE Bhavcopy &middot; Zero dependencies &middot; Vercel Serverless
</div></div></body></html>"""

# ── Main pipeline ────────────────────────────────────────────────────────────
def run_scan():
    log = []

    # 1 — Download bhavcopy
    raw, scan_date = download_bhav()
    if not raw:
        return dict(ok=False, error="Could not download bhavcopy", log=log)
    log.append(f"Bhavcopy for {scan_date}")

    # 2 — Parse
    stocks = parse_bhav(raw)
    log.append(f"Parsed {len(stocks)} EQ stocks")

    # 3 — Load rolling history from GitHub
    hist_csv, hist_sha = gh_read("cache/rolling_history.csv")
    history = load_history(hist_csv)
    log.append(f"History: {len(history)} symbols")

    # 4 — Append today + trim
    history = update_history(history, stocks, scan_date)

    # 5 — Averages
    avgs = build_averages(history)
    log.append(f"Averages: {len(avgs)} symbols")

    # 6 — Score
    scored = score_stocks(stocks, avgs)
    log.append(f"Scored: {len(scored)} hits")

    # 7 — Streaks
    strk_csv, strk_sha = gh_read("scanner/streak_tracker.csv")
    new_strk = apply_streaks(scored, strk_csv)

    # 8 — HTML
    mx = max((len(r) for r in history.values()), default=0)
    html = generate_html(scored, scan_date, dict(total=len(stocks), hist_days=mx))

    # 9 — Write to GitHub
    gh_write("cache/rolling_history.csv",
             history_to_csv(history),
             f"history {scan_date}", hist_sha)
    log.append("Wrote history")

    gh_write("scanner/streak_tracker.csv",
             new_strk, f"streaks {scan_date}", strk_sha)
    log.append("Wrote streaks")

    ds = scan_date.replace("-", "")
    _, rep_sha = gh_read(f"reports/{ds}.html")
    gh_write(f"reports/{ds}.html", html, f"report {scan_date}", rep_sha)
    log.append(f"Wrote reports/{ds}.html")

    _, idx_sha = gh_read("index.html")
    gh_write("index.html", html, f"index {scan_date}", idx_sha)
    log.append("Wrote index.html")

    # Archive index
    arch_txt, arch_sha = gh_read("cache/archive_index.json")
    archive = json.loads(arch_txt) if arch_txt else []
    entry = dict(date=scan_date, file=f"reports/{ds}.html",
                 strong=sum(1 for s in scored if s["grade"]=="STRONG"),
                 moderate=sum(1 for s in scored if s["grade"]=="MODERATE"))
    archive = [a for a in archive if a["date"] != scan_date] + [entry]
    archive.sort(key=lambda x: x["date"], reverse=True)
    gh_write("cache/archive_index.json",
             json.dumps(archive[:90], indent=2),
             f"archive {scan_date}", arch_sha)
    log.append("Wrote archive index")

    return dict(
        ok=True, date=scan_date, url=PAGES_URL,
        strong=entry["strong"], moderate=entry["moderate"],
        total=len(stocks), log=log,
    )

# ── Vercel handler ───────────────────────────────────────────────────────────
class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            qs_str = urlparse(self.path).query or os.environ.get("QUERY_STRING", "")
            qs = parse_qs(qs_str)
            backfill_param = qs.get("backfill", ["0"])[0]
            if backfill_param.isdigit() and int(backfill_param) > 0:
                result = run_backfill(int(backfill_param))
            else:
                result = run_scan()
            code = 200 if result.get("ok") else 500
        except Exception as e:
            import traceback
            result = dict(ok=False, error=str(e), trace=traceback.format_exc())
            code = 500
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(result, indent=2).encode())
