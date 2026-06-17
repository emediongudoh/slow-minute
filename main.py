import asyncio
import json
import os
import traceback
from typing import TYPE_CHECKING, Any
import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
from dotenv import load_dotenv

if TYPE_CHECKING:
    from bot import EmilianoBot, AccountService, MarketWorker

try:
    from bot import AccountService as _AccountService  # type: ignore
    from bot import MarketWorker as _MarketWorker  # type: ignore
except Exception as _bot_import_err:
    print(f"❌ Failed to import AccountService/MarketWorker: {_bot_import_err}")
    traceback.print_exc()
    _AccountService = None  # type: ignore
    _MarketWorker = None  # type: ignore

load_dotenv()


def _bot_name_from_url(url: str) -> str:
    """Derive a display name from the Render external URL.
    e.g. 'https://slow-minute.onrender.com' → 'Slow Minute'
    Falls back to 'Emiliano' if the URL is missing or unparseable.
    """
    if not url:
        return "Emiliano"
    try:
        host = url.replace("https://", "").replace("http://", "").split(".")[0]
        return host.replace("-", " ").title()
    except Exception:
        return "Emiliano"


BOT_DISPLAY_NAME = _bot_name_from_url(os.getenv("RENDER_EXTERNAL_URL", ""))

app = FastAPI(title=f"{BOT_DISPLAY_NAME} Dashboard")

try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
except Exception:
    pass  # static dir is optional

bots: list[Any] = []
account: Any = None
active_connections: list[WebSocket] = []


@app.on_event("startup")
async def startup_event():
    global bots, account
    print("🚀 Emiliano Dashboard Starting on Render...")
    bots = []

    if _AccountService is None or _MarketWorker is None:
        print("❌ Cannot start bots — AccountService/MarketWorker failed to import.")
        return

    try:
        # ── Global, account-level, ONE-TIME startup work ──────────────────
        # This single AccountService owns the Web3 connection, the ClobClient,
        # the wallet audit, and the periodic PnL-merge scheduler. It is built
        # exactly once here, regardless of how many assets are tracked below,
        # so the wallet audit and the global PnL job never run more than once.
        print("Initializing AccountService (wallet audit runs once here)...")
        account = _AccountService()
        if not account.run_wallet_audit():
            print("❌ Wallet audit failed — aborting bot startup.")
            return
        account.start_pnl_merge_scheduler()
        print("✅ AccountService ready — wallet audited once, PnL scheduler running.")

        # ── Per-asset market workers — share the single AccountService ────
        for asset in ["btc", "eth", "sol"]:
            print(f"Initializing {asset.upper()} Bot...")
            bots.append(_MarketWorker(asset, account))
            print(f"✅ {asset.upper()} Bot initialized")

        asyncio.create_task(run_all_bots())
        print("🎉 All bots started successfully!")

    except Exception as e:
        print(f"❌ Bot init failed: {e}")
        traceback.print_exc()


async def run_all_bots():
    if not bots:
        return
    try:
        await asyncio.gather(
            *[bot.start() for bot in bots],
            broadcast_loop(),
            keep_alive_heartbeat(),
        )
    except Exception as e:
        print(f"Background tasks error: {e}")
        traceback.print_exc()


async def keep_alive_heartbeat():
    render_url = os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    ping_interval = 10 * 60
    heartbeat_interval = 25
    seconds_since_ping = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                loop_time = asyncio.get_running_loop().time()
                print(f"💓 Heartbeat [{loop_time:.0f}] — {len(bots)} bots active")
                seconds_since_ping += heartbeat_interval
                if render_url and seconds_since_ping >= ping_interval:
                    seconds_since_ping = 0
                    try:
                        async with session.get(
                            f"{render_url}/api/status",
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as resp:
                            print(f"🌐 Self-ping → {resp.status}")
                    except Exception as ping_err:
                        print(f"⚠️  Self-ping failed: {ping_err}")
            except Exception:
                pass
            await asyncio.sleep(heartbeat_interval)


async def broadcast_loop():
    while True:
        try:
            bot_data = []
            for bot in bots:
                try:
                    bot_data.append(bot.get_dashboard_data())
                except Exception as e:
                    bot_data.append({
                        "asset": getattr(bot, "asset_type", "ERROR").upper(),
                        "status": "ERROR",
                        "position": str(e)[:80],
                    })

            data = {
                "bots": bot_data,
                "global_stats": get_global_stats(),
                "timestamp": asyncio.get_running_loop().time(),
            }

            message = json.dumps(data)
            for ws in active_connections[:]:
                try:
                    await ws.send_text(message)
                except Exception:
                    if ws in active_connections:
                        active_connections.remove(ws)

            await asyncio.sleep(1.0)
        except Exception as e:
            print(f"Broadcast error: {e}")
            await asyncio.sleep(3)


def get_global_stats() -> dict:
    if not bots:
        return {
            "total_bots": 0, "active_bots": 0, "total_pnl": 0.0,
            "in_profit": 0, "in_loss": 0, "total_trades": 0, "total_wins": 0,
            "total_losses": 0, "win_rate": 0.0,
        }

    from bot import TradeState  # local import to avoid circular issues at module level

    total_pnl = 0.0
    active_count = 0
    total_wins = 0
    total_losses = 0
    in_profit_count = 0
    in_loss_count = 0

    for bot in bots:
        try:
            total_pnl    += getattr(bot, "cumulative_pnl", 0.0)
            total_wins   += getattr(bot, "wins", 0)
            total_losses += getattr(bot, "losses", 0)
            in_position   = getattr(bot, "trade_state", None) == TradeState.FILLED
            if bot.active_market or in_position:
                active_count += 1
            if in_position:
                pnl_dollars, _, _ = bot.get_current_pnl()
                if pnl_dollars > 0:
                    in_profit_count += 1
                elif pnl_dollars < 0:
                    in_loss_count += 1
        except Exception:
            continue

    total_trades = total_wins + total_losses
    win_rate = round((total_wins / total_trades) * 100, 1) if total_trades > 0 else 0.0

    return {
        "total_bots":   len(bots),
        "active_bots":  active_count,
        "total_pnl":    round(total_pnl, 2),
        "in_profit":    in_profit_count,
        "in_loss":      in_loss_count,
        "total_trades": total_trades,
        "total_wins":   total_wins,
        "total_losses": total_losses,
        "win_rate":     win_rate,
    }


HTML_CONTENT = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>__BOT_NAME__ • Live</title>

  <!-- JetBrains Mono + Inter (Polymarket-style) -->
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">

  <script src="https://cdn.tailwindcss.com"></script>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css">

  <style>
    /* ── Base typography ───────────────────────────────────────── */
    *, *::before, *::after { box-sizing: border-box; }

    body {
      font-family: 'Inter', system-ui, sans-serif;
      -webkit-font-smoothing: antialiased;
    }

    /* Monospace: JetBrains Mono with tabular digits everywhere */
    .mono, .font-mono,
    [class*="font-mono"] {
      font-family: 'JetBrains Mono', 'Consolas', 'Menlo', monospace;
      font-variant-numeric: tabular-nums;
    }

    /* Make Tailwind's font-mono utility use JetBrains Mono */
    .font-mono {
      font-family: 'JetBrains Mono', 'Consolas', 'Menlo', monospace !important;
      font-variant-numeric: tabular-nums;
    }

    /* ── Card hover ────────────────────────────────────────────── */
    .card { transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1); }
    .card:hover { transform: translateY(-3px); }

    /* ── Signal colour helpers ─────────────────────────────────── */
    .sig-strong-bull  { color: #22c55e; font-weight: 700; }
    .sig-strong-bear  { color: #ef4444; font-weight: 700; }
    .sig-mild-bull    { color: #86efac; }
    .sig-mild-bear    { color: #fca5a5; }
    .sig-neutral      { color: #71717a; }
    .pill-strong-bull { background: rgba(34,197,94,.15);   color: #22c55e; }
    .pill-strong-bear { background: rgba(239,68,68,.15);   color: #ef4444; }
    .pill-mild-bull   { background: rgba(134,239,172,.12); color: #86efac; }
    .pill-mild-bear   { background: rgba(252,165,165,.12); color: #fca5a5; }
    .pill-neutral     { background: rgba(113,113,122,.15); color: #71717a; }

    /* ── P&L chart card ────────────────────────────────────────── */
    .pm-card {
      background: #18181b;
      border-radius: 24px;
      overflow: hidden;
      margin-bottom: 24px;
    }

    /* header row: triangle + label  |  period tabs */
    .pm-card-hdr {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 18px 20px 4px;
    }
    .pm-label-grp { display: flex; align-items: center; gap: 7px; }
    .pm-tri { font-size: 11px; font-weight: 700; line-height: 1; }
    .pm-tri.pos { color: #22c55e; }
    .pm-tri.neg { color: #ef4444; }
    .pm-lbl-txt {
      font-size: 12px; font-weight: 500;
      color: #71717a; letter-spacing: .04em;
      text-transform: uppercase;
    }

    /* period tab buttons */
    .pm-per-tabs { display: flex; gap: 1px; }
    .pm-per-btn {
      background: none; border: none; color: #52525b;
      font-family: 'Inter', system-ui, sans-serif;
      font-size: 11px; font-weight: 600;
      padding: 5px 8px; border-radius: 6px;
      cursor: pointer; transition: all .15s;
      letter-spacing: .03em;
    }
    .pm-per-btn.act { background: #22c55e; color: #0a0a0a; }

    /* big portfolio value */
    .pm-val-block {
      padding: 6px 20px 0;
      display: flex;
      flex-direction: column;   /* ← stacked, not side-by-side */
      align-items: flex-start;
      gap: 4px;
    }
    .pm-big-val {
      font-family: 'Inter', system-ui, sans-serif;
      font-size: 40px; font-weight: 800;
      line-height: 1; letter-spacing: -.04em;
    }
    .pm-big-val.pos { color: #22c55e; }
    .pm-big-val.neg { color: #ef4444; }
    .pm-big-val.neu { color: #e4e4e7; }

    /* period change label — NOW BELOW the big number */
    .pm-change-lbl {
      font-family: 'JetBrains Mono', 'Consolas', monospace;
      font-variant-numeric: tabular-nums;
      font-size: 12px; font-weight: 600;
      color: #71717a;
      letter-spacing: .01em;
    }
    .pm-change-lbl.pos { color: #22c55e; }
    .pm-change-lbl.neg { color: #ef4444; }

    /* chart box */
    .pm-chart-box {
      height: 160px;
      position: relative;
      overflow: hidden;
      margin-top: 12px;
    }
    .pm-chart-box svg {
      display: block; width: 100%; height: 100%; pointer-events: none;
    }
    .pm-no-data {
      position: absolute; top: 50%; left: 50%;
      transform: translate(-50%, -50%);
      font-size: 12px; color: #52525b;
      white-space: nowrap; pointer-events: none;
    }
    #pm-chart-overlay {
      position: absolute; inset: 0; z-index: 10;
      cursor: crosshair; touch-action: pan-y;
    }

    /* stats row below chart */
    .pm-stats-row {
      display: flex;
      border-top: 1px solid rgba(255,255,255,0.06);
      padding: 12px 20px;
      align-items: center;
      justify-content: space-between;
    }
    .pm-stat-item { display: flex; flex-direction: column; align-items: center; }
    .pm-stat-num  {
      font-family: 'JetBrains Mono', 'Consolas', monospace;
      font-variant-numeric: tabular-nums;
      font-size: 15px; font-weight: 700; line-height: 1.2;
    }
    .pm-stat-lbl  {
      font-size: 9px; color: #52525b;
      text-transform: uppercase; letter-spacing: .1em;
      margin-top: 3px;
    }
  </style>
</head>

<body class="bg-zinc-950 text-zinc-100 min-h-screen p-4 md:p-6">
<div class="max-w-7xl mx-auto">

  <!-- ── Header ───────────────────────────j────────────────────── -->
  <div class="flex items-center justify-between mb-5">
    <h1 class="text-4xl font-bold" style="font-family:'JetBrains Mono','Consolas',monospace;letter-spacing:-.03em;line-height: 1;">
      __BOT_NAME__
    </h1>
    <span id="conn-dot" class="w-2.5 h-2.5 rounded-full bg-zinc-600 inline-block" title="WebSocket status"></span>
  </div>
  <p id="math-quote" class="text-[#22c55e] mb-5 text-sm font-bold" style="font-family:'JetBrains Mono','Consolas',monospace;letter-spacing:-.03em;line-height: 1;transition:opacity .6s ease;"></p>
  <script>
  (function () {
    var quotes = [
      '"The only way to learn mathematics is to do mathematics." — Paul Halmos',
      '"Mathematics is the language in which God has written the universe." — Galileo Galilei',
      '"In mathematics you don\'t understand things. You just get used to them." — John von Neumann',
      '"God made the integers; all else is the work of man." — Leopold Kronecker',
      '"Pure mathematics is, in its way, the poetry of logical ideas." — Albert Einstein',
      '"Mathematics is not about numbers, equations, computations, or algorithms: it is about understanding." — William Paul Thurston',
      '"Do not worry about your difficulties in mathematics. I can assure you mine are still greater." — Albert Einstein',
      '"A mathematician is a machine for turning coffee into theorems." — Paul Erdős',
      '"Without mathematics, there\'s nothing you can do. Everything around you is mathematics." — Shakuntala Devi',
      '"The essence of mathematics lies in its freedom." — Georg Cantor',
      '"If people do not believe that mathematics is simple, it is only because they do not realize how complicated life is." — John von Neumann',
      '"An equation for me has no meaning unless it represents a thought of God." — Srinivasa Ramanujan',
      '"It is not knowledge, but the act of learning, not possession but the act of getting there, which grants the greatest enjoyment." — Carl Friedrich Gauss',
      '"Mathematics is the queen of the sciences and number theory is the queen of mathematics." — Carl Friedrich Gauss',
      '"No human investigation can be called real science if it cannot be demonstrated mathematically." — Leonardo da Vinci',
    ];
    var el = document.getElementById('math-quote');
    var idx = Math.floor(Math.random() * quotes.length);
    function showNext() {
      el.style.opacity = '0';
      setTimeout(function () {
        idx = (idx + 1) % quotes.length;
        el.textContent = quotes[idx];
        el.style.opacity = '1';
      }, 600);
    }
    el.textContent = quotes[idx];
    el.style.opacity = '1';
    setInterval(showNext, 12000);
  })();
  </script>

  <!-- ── P&L Chart Card ──────────────────────────────────────── -->
  <div class="pm-card">

    <!-- header: label + period tabs -->
    <div class="pm-card-hdr">
      <div class="pm-label-grp">
        <span class="pm-tri neg" id="pm-tri">▼</span>
        <span class="pm-lbl-txt">Portfolio P&amp;L</span>
      </div>
      <div class="pm-per-tabs">
        <button class="pm-per-btn act" id="ppb-1D"  onclick="setPeriod('1D')">1D</button>
        <button class="pm-per-btn"     id="ppb-1W"  onclick="setPeriod('1W')">1W</button>
        <button class="pm-per-btn"     id="ppb-1M"  onclick="setPeriod('1M')">1M</button>
        <button class="pm-per-btn"     id="ppb-1Y"  onclick="setPeriod('1Y')">1Y</button>
      </div>
    </div>

    <!-- big value + change label stacked vertically below it -->
    <div class="pm-val-block">
      <div class="pm-big-val neu" id="pm-bigval">$0.00</div>
      <!-- change label sits directly under the big number -->
      <span class="pm-change-lbl" id="pm-change-lbl"></span>
    </div>

    <!-- chart -->
    <div class="pm-chart-box" id="pm-chart-box">
      <div class="pm-no-data" id="pm-no-data">No trade data yet</div>
      <div id="pm-chart-overlay"></div>
    </div>

    <!-- stats row -->
    <div class="pm-stats-row" id="pm-stats-row">
      <div class="pm-stat-item">
        <span class="pm-stat-num text-zinc-200" id="pm-st-trades">0</span>
        <span class="pm-stat-lbl">Trades</span>
      </div>
      <div class="pm-stat-item">
        <span class="pm-stat-num text-emerald-400" id="pm-st-wins">0</span>
        <span class="pm-stat-lbl">Wins</span>
      </div>
      <div class="pm-stat-item">
        <span class="pm-stat-num text-red-400" id="pm-st-losses">0</span>
        <span class="pm-stat-lbl">Losses</span>
      </div>
      <div class="pm-stat-item">
        <span class="pm-stat-num text-yellow-400" id="pm-st-wr">--%</span>
        <span class="pm-stat-lbl">Win Rate</span>
      </div>
    </div>
  </div>

  <!-- ── Bot Cards ────────────────────────────────────────────── -->
  <div id="bots-container" class="grid grid-cols-1 md:grid-cols-2 gap-4 md:gap-6"></div>

</div>

<script>
// ═══════════════════════════════════════════════════════════════
// P&L CHART ENGINE
// ═══════════════════════════════════════════════════════════════
const _pnlPts  = [];   // { t: ms, v: cumulative_pnl }
let _pnlPeriod = '1D';
let _lastPushed = 0;

// How far back each period looks (ms). 1Y is the longest kept.
function _periodMs(p) {
  if (p === '1D') return 24 * 3600 * 1000;
  if (p === '1W') return 7  * 24 * 3600 * 1000;
  if (p === '1M') return 30 * 24 * 3600 * 1000;
  if (p === '1Y') return 365 * 24 * 3600 * 1000;
  return 365 * 24 * 3600 * 1000;
}

// Cutoff timestamp for a period (points older than this are outside the window).
function _periodCutoff(p) {
  return Date.now() - _periodMs(p);
}

function _periodLabel(p) {
  return { '1D': 'Past 24h', '1W': 'Past 7 Days', '1M': 'Past 30 Days', '1Y': 'Past Year' }[p] || p;
}

// Compact old points to save memory while preserving long-period history.
// Strategy:
//   data older than 30d  → keep 1 point per hour  (max resolution needed for 1Y)
//   data older than 7d   → keep 1 point per 10min (enough for 1M)
//   data older than 1d   → keep 1 point per 1min  (enough for 1W)
//   data within 1d       → keep every point        (full resolution for 1D)
function _compactOldPoints() {
  const now = Date.now();
  const MS_1D  = 24 * 3600 * 1000;
  const MS_7D  = 7  * MS_1D;
  const MS_30D = 30 * MS_1D;
  const MS_1Y  = 365 * MS_1D;

  // Drop anything older than 1 year
  const cutoff1Y = now - MS_1Y;
  let i = 0;
  while (i < _pnlPts.length && _pnlPts[i].t < cutoff1Y) i++;
  if (i > 0) _pnlPts.splice(0, i);

  // Downsample each zone in a single pass (keep the LAST point in each bucket)
  function downsample(startIdx, endIdx, bucketMs) {
    if (endIdx <= startIdx) return;
    const keep = [];
    let bucketStart = Math.floor(_pnlPts[startIdx].t / bucketMs) * bucketMs;
    let lastInBucket = startIdx;
    for (let j = startIdx + 1; j <= endIdx; j++) {
      const pt = _pnlPts[j];
      if (!pt) break;
      const thisBucket = Math.floor(pt.t / bucketMs) * bucketMs;
      if (thisBucket !== bucketStart) {
        keep.push(lastInBucket); // keep last point of previous bucket
        bucketStart = thisBucket;
      }
      lastInBucket = j;
    }
    keep.push(lastInBucket); // keep last point of final bucket
    // Replace range [startIdx, endIdx] with kept points
    const replacement = keep.map(k => _pnlPts[k]);
    _pnlPts.splice(startIdx, endIdx - startIdx + 1, ...replacement);
    return replacement.length;
  }

  const cutoff30D = now - MS_30D;
  const cutoff7D  = now - MS_7D;
  const cutoff1D  = now - MS_1D;

  // Find boundary indices (binary-search style)
  function findIdx(ts) {
    let lo = 0, hi = _pnlPts.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (_pnlPts[mid].t < ts) lo = mid + 1; else hi = mid;
    }
    return lo;
  }

  const idx30D = findIdx(cutoff30D);
  const idx7D  = findIdx(cutoff7D);
  const idx1D  = findIdx(cutoff1D);

  // Downsample oldest zone first (indices shift after each splice)
  // Zone: [0, idx30D) → hourly (3600s buckets)
  if (idx30D > 1) downsample(0, idx30D - 1, 3600 * 1000);

  // Re-find boundaries after first splice
  const idx7D2  = findIdx(cutoff7D);
  const idx1D2  = findIdx(cutoff1D);

  // Zone: [idx7D2_start, idx7D2) — between 7d and 30d → 10-min buckets
  const idx30D2 = findIdx(cutoff30D);
  if (idx7D2 > idx30D2 + 1) downsample(idx30D2, idx7D2 - 1, 10 * 60 * 1000);

  // Zone: [idx1D, idx7D) — between 1d and 7d → 1-min buckets
  const idx7D3  = findIdx(cutoff7D);
  const idx1D3  = findIdx(cutoff1D);
  if (idx1D3 > idx7D3 + 1) downsample(idx7D3, idx1D3 - 1, 60 * 1000);
}

let _compactCounter = 0;

function setPeriod(p) {
  _pnlPeriod = p;
  document.querySelectorAll('.pm-per-btn').forEach(b => {
    b.classList.toggle('act', b.id === 'ppb-' + p);
  });
  updatePnlChart();
}

function pushPnlPoint(totalPnl) {
  const now = Date.now();
  // Throttle: within 800ms, just update the last point's value (prevents duplicate ticks)
  if (now - _lastPushed < 800 && _pnlPts.length > 0) {
    _pnlPts[_pnlPts.length - 1].v = totalPnl;
  } else {
    _pnlPts.push({ t: now, v: totalPnl });
    _lastPushed = now;
  }
  // Compact every 300 pushes (~5 min at 1s interval) to save memory
  _compactCounter++;
  if (_compactCounter >= 300) {
    _compactCounter = 0;
    _compactOldPoints();
  }
  updatePnlChart();
}

function updatePnlChart() {
  const now    = Date.now();
  const cutoff = _periodCutoff(_pnlPeriod);

  // Filter to the selected time window
  const filtered = _pnlPts.filter(p => p.t >= cutoff);

  // Current (latest) cumulative PnL — always from the very last point
  const totalPnl = _pnlPts.length > 0 ? _pnlPts[_pnlPts.length - 1].v : 0;

  // Period PnL = change from the first point inside the window to the last.
  // If no data exists in the window yet, treat period start value as 0.
  const periodStartVal = filtered.length > 0 ? filtered[0].v : totalPnl;
  const periodEndVal   = filtered.length > 0 ? filtered[filtered.length - 1].v : totalPnl;
  const periodChange   = periodEndVal - periodStartVal;  // ← FIXED: true delta for the period

  // Percentage change relative to the absolute value at period start.
  // Guard against division by zero (period started at $0).
  const periodChangePct = periodStartVal !== 0
    ? (periodChange / Math.abs(periodStartVal)) * 100
    : null;

  // Build chart points: anchor the chart to the start of the period at the
  // actual value there (not forced to 0), so the line truly represents history.
  // We add a synthetic anchor point at the exact period cutoff if filtered is
  // not empty, so the chart always fills the full selected width.
  let pts = [];
  if (filtered.length > 0) {
    // Synthetic left-edge point at the cutoff timestamp with the period-start value
    pts.push({ t: cutoff, v: filtered[0].v });
    for (const p of filtered) pts.push({ t: p.t, v: p.v });
  }

  // ── Big number (current total cumulative PnL) ─────────────────
  const bigEl = document.getElementById('pm-bigval');
  if (bigEl) {
    bigEl.textContent = (totalPnl >= 0 ? '' : '-') + '$' + Math.abs(totalPnl).toFixed(2);
    bigEl.className   = 'pm-big-val ' + (totalPnl > 0 ? 'pos' : totalPnl < 0 ? 'neg' : 'neu');
  }

  // ── Triangle ─────────────────────────────────────────────────
  const triEl = document.getElementById('pm-tri');
  if (triEl) {
    triEl.textContent = periodChange >= 0 ? '▲' : '▼';
    triEl.className   = 'pm-tri ' + (periodChange >= 0 ? 'pos' : 'neg');
  }

  // ── Change label — BELOW big number ──────────────────────────
  // Shows: "+$2.34  (+12.5%)  ▲  Past 24h"
  const chEl = document.getElementById('pm-change-lbl');
  if (chEl) {
    if (filtered.length > 1) {
      const arrow  = periodChange >= 0 ? '▲' : '▼';
      const sign   = periodChange >= 0 ? '+' : '';
      const pctStr = periodChangePct !== null
        ? ` (${periodChange >= 0 ? '+' : ''}${periodChangePct.toFixed(1)}%)`
        : '';
      chEl.textContent = `${sign}$${Math.abs(periodChange).toFixed(2)}${pctStr}  ${arrow}  ${_periodLabel(_pnlPeriod)}`;
      chEl.className   = 'pm-change-lbl ' + (periodChange >= 0 ? 'pos' : 'neg');
    } else {
      chEl.textContent = _periodLabel(_pnlPeriod);
      chEl.className   = 'pm-change-lbl';
    }
  }

  const chartEl = document.getElementById('pm-chart-box');
  if (!chartEl) return;

  if (pts.length < 2) {
    const old = chartEl.querySelector('svg');
    if (old) old.remove();
    const nd = chartEl.querySelector('#pm-no-data');
    if (nd) nd.style.display = '';
    window._pmChart = null;
    return;
  }

  // ── Build SVG ────────────────────────────────────────────────
  const W = 400, H = 160, PX = 20, PY = 10;
  const vals   = pts.map(p => p.v);
  const minV   = Math.min(...vals);
  const maxV   = Math.max(...vals);
  const padV   = (maxV - minV) * 0.15 || 0.05;
  const lo = minV - padV, hi = maxV + padV;
  const vRange = hi - lo;
  const drawW  = W - PX * 2;
  const drawH  = H - PY * 2;

  const mx = i => (PX + (i / Math.max(pts.length - 1, 1)) * drawW).toFixed(2);
  const my = v  => (PY + drawH - ((v - lo) / vRange * drawH)).toFixed(2);

  const lineColor  = periodChange >= 0 ? '#22c55e' : '#ef4444';
  const lineColor2 = periodChange >= 0 ? '#16a34a' : '#dc2626';

  const dLine = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${mx(i)},${my(p.v)}`).join(' ');
  const zeroY = parseFloat(my(0));
  const clampedZeroY = Math.max(PY, Math.min(H - PY, zeroY));
  const dArea = dLine
    + ` L${parseFloat(mx(pts.length - 1)).toFixed(2)},${clampedZeroY.toFixed(2)}`
    + ` L${PX},${clampedZeroY.toFixed(2)} Z`;

  // x-axis time labels
  let xLabels = '';
  if (filtered.length > 0) {
    const step      = Math.max(1, Math.floor((pts.length - 1) / 4));
    const labelIdxs = [0];
    for (let i = step; i < pts.length - 1; i += step) labelIdxs.push(i);
    if (labelIdxs[labelIdxs.length - 1] !== pts.length - 1) labelIdxs.push(pts.length - 1);
    xLabels = labelIdxs.map(i => {
      const x  = parseFloat(mx(i));
      const pt = pts[i];
      const timeStr = pt.t
        ? (_pnlPeriod === '1D'
            ? new Date(pt.t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
            : _pnlPeriod === '1W'
              ? new Date(pt.t).toLocaleString([], { weekday: 'short', hour: '2-digit', minute: '2-digit' })
              : new Date(pt.t).toLocaleDateString([], { month: 'short', day: 'numeric' }))
        : '';
      const anchor = i === 0 ? 'start' : i === pts.length - 1 ? 'end' : 'middle';
      return `<text x="${x}" y="${H - 3}" text-anchor="${anchor}"
        font-size="8" fill="rgba(255,255,255,0.28)"
        font-family="'JetBrains Mono','Consolas',monospace">${timeStr}</text>`;
    }).join('');
  }

  // zero crossing line
  let zeroLine = '';
  if (minV < 0 && maxV > 0) {
    zeroLine = `<line x1="${PX}" y1="${clampedZeroY.toFixed(2)}"
      x2="${W - PX}" y2="${clampedZeroY.toFixed(2)}"
      stroke="rgba(255,255,255,0.12)" stroke-width="1" stroke-dasharray="3,3"/>`;
  }

  const lastX = parseFloat(mx(pts.length - 1));
  const lastY = parseFloat(my(pts[pts.length - 1].v));

  // store chart state for crosshair restore
  window._pmChart = {
    pts, PX, PY, drawW, drawH, W, H,
    period: _pnlPeriod, lo, vRange, lineColor,
    origVal: totalPnl,
    origPeriodChange: periodChange,
    origChangeTxt: chEl ? chEl.textContent : '',
    origChangeCls: chEl ? chEl.className   : '',
  };

  const ndEl = chartEl.querySelector('#pm-no-data');
  if (ndEl) ndEl.style.display = 'none';
  const oldSvg = chartEl.querySelector('svg');
  if (oldSvg) oldSvg.remove();

  const svgEl = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  svgEl.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svgEl.setAttribute('preserveAspectRatio', 'none');
  svgEl.style.cssText = 'display:block;width:100%;height:100%;pointer-events:none';
  svgEl.innerHTML = `
    <defs>
      <linearGradient id="pnlGrd" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%"   stop-color="${lineColor}"  stop-opacity="0.38"/>
        <stop offset="65%"  stop-color="${lineColor}"  stop-opacity="0.07"/>
        <stop offset="100%" stop-color="${lineColor}"  stop-opacity="0"/>
      </linearGradient>
      <linearGradient id="lineGrd" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%"   stop-color="${lineColor2}"/>
        <stop offset="100%" stop-color="${lineColor}"/>
      </linearGradient>
      <filter id="glow">
        <feGaussianBlur stdDeviation="1.5" result="coloredBlur"/>
        <feMerge><feMergeNode in="coloredBlur"/><feMergeNode in="SourceGraphic"/></feMerge>
      </filter>
      <clipPath id="chartClip">
        <rect x="${PX}" y="${PY}" width="${drawW}" height="${drawH + PY}"/>
      </clipPath>
    </defs>
    ${zeroLine}
    <g clip-path="url(#chartClip)">
      <path d="${dArea}" fill="url(#pnlGrd)"/>
      <path d="${dLine}" fill="none" stroke="url(#lineGrd)"
            stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"
            filter="url(#glow)"/>
    </g>
    ${xLabels}
    <circle id="chart-end-dot" cx="${lastX}" cy="${lastY}"
            r="3.5" fill="${lineColor}" filter="url(#glow)"/>
    <line id="xhair-line"
          x1="${PX}" y1="${PY}" x2="${PX}" y2="${H - 14}"
          stroke="rgba(255,255,255,0.6)" stroke-width="1.2"
          stroke-dasharray="3,3" display="none"/>
    <circle id="xhair-dot" cx="${PX}" cy="${PY}" r="4.5"
            fill="${lineColor}" stroke="#111" stroke-width="1.5"
            display="none" filter="url(#glow)"/>`;

  const overlay = chartEl.querySelector('#pm-chart-overlay');
  chartEl.insertBefore(svgEl, overlay);
}

// ── Crosshair overlay ────────────────────────────────────────
function _initChartOverlay() {
  const overlay = document.getElementById('pm-chart-overlay');
  if (!overlay) return;

  function _handleMove(clientX) {
    const c = window._pmChart;
    if (!c || !c.pts || c.pts.length < 2) return;
    const chartBox = document.getElementById('pm-chart-box');
    if (!chartBox) return;
    const rect = chartBox.getBoundingClientRect();
    const frac = Math.max(0, Math.min(1, (clientX - rect.left) / rect.width));
    const idx  = Math.round(frac * (c.pts.length - 1));
    const pt   = c.pts[idx];
    const svgX = c.PX + (idx / Math.max(c.pts.length - 1, 1)) * c.drawW;
    const svgY = c.PY + c.drawH - ((pt.v - c.lo) / c.vRange * c.drawH);

    const line = document.getElementById('xhair-line');
    const dot  = document.getElementById('xhair-dot');
    if (line) {
      line.setAttribute('x1', svgX.toFixed(2));
      line.setAttribute('x2', svgX.toFixed(2));
      line.setAttribute('y1', c.PY);
      line.setAttribute('y2', c.H - 14);
      line.removeAttribute('display');
    }
    if (dot) {
      dot.setAttribute('cx', svgX.toFixed(2));
      dot.setAttribute('cy', svgY.toFixed(2));
      dot.removeAttribute('display');
    }

    const bigEl = document.getElementById('pm-bigval');
    if (bigEl) {
      bigEl.textContent = (pt.v >= 0 ? '' : '-') + '$' + Math.abs(pt.v).toFixed(2);
      bigEl.className   = 'pm-big-val ' + (pt.v > 0 ? 'pos' : pt.v < 0 ? 'neg' : 'neu');
    }
    const triEl = document.getElementById('pm-tri');
    if (triEl) {
      triEl.textContent = pt.v >= 0 ? '▲' : '▼';
      triEl.className   = 'pm-tri ' + (pt.v >= 0 ? 'pos' : 'neg');
    }
    // change label shows hovered timestamp while scrubbing
    const chEl = document.getElementById('pm-change-lbl');
    if (chEl && pt.t) {
      chEl.textContent = c.period === '1D'
        ? new Date(pt.t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
        : new Date(pt.t).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
      chEl.className = 'pm-change-lbl';
    }
  }

  function _handleLeave() {
    const c    = window._pmChart;
    const line = document.getElementById('xhair-line');
    const dot  = document.getElementById('xhair-dot');
    if (line) line.setAttribute('display', 'none');
    if (dot)  dot.setAttribute('display',  'none');
    if (!c) return;
    const bigEl = document.getElementById('pm-bigval');
    if (bigEl) {
      bigEl.textContent = (c.origVal >= 0 ? '' : '-') + '$' + Math.abs(c.origVal).toFixed(2);
      bigEl.className   = 'pm-big-val ' + (c.origVal > 0 ? 'pos' : c.origVal < 0 ? 'neg' : 'neu');
    }
    const triEl = document.getElementById('pm-tri');
    if (triEl) {
      triEl.textContent = c.origPeriodChange >= 0 ? '▲' : '▼';
      triEl.className   = 'pm-tri ' + (c.origPeriodChange >= 0 ? 'pos' : 'neg');
    }
    const chEl = document.getElementById('pm-change-lbl');
    if (chEl && c.origChangeTxt) {
      chEl.textContent = c.origChangeTxt;
      chEl.className   = c.origChangeCls;
    }
  }

  overlay.addEventListener('mousemove',   e => _handleMove(e.clientX));
  overlay.addEventListener('mouseleave',  _handleLeave);
  overlay.addEventListener('touchstart',  e => _handleMove(e.touches[0].clientX), { passive: true });
  overlay.addEventListener('touchmove',   e => _handleMove(e.touches[0].clientX), { passive: true });
  overlay.addEventListener('touchend',    _handleLeave);
  overlay.addEventListener('touchcancel', _handleLeave);
}

// ═══════════════════════════════════════════════════════════════
// WEBSOCKET
// ═══════════════════════════════════════════════════════════════
const connDot = document.getElementById('conn-dot');
let ws;
function connect() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen  = () => connDot.className = 'w-2.5 h-2.5 rounded-full bg-emerald-400 inline-block';
  ws.onclose = () => {
    connDot.className = 'w-2.5 h-2.5 rounded-full bg-red-500 inline-block';
    setTimeout(connect, 2500);
  };
  ws.onerror = () => ws.close();
  ws.onmessage = e => {
    const d = JSON.parse(e.data);
    renderGlobalStats(d.global_stats);
    pushPnlPoint(d.global_stats.total_pnl ?? 0);
    renderBots(d.bots);
  };
}
connect();

// ═══════════════════════════════════════════════════════════════
// GLOBAL STATS — four cells in the pm-stats-row
// ═══════════════════════════════════════════════════════════════
function renderGlobalStats(g) {
  const el = id => document.getElementById(id);
  if (el('pm-st-trades')) el('pm-st-trades').textContent = g.total_trades ?? 0;
  if (el('pm-st-wins'))   el('pm-st-wins').textContent   = g.total_wins   ?? 0;
  if (el('pm-st-losses')) el('pm-st-losses').textContent = g.total_losses ?? 0;
  if (el('pm-st-wr'))     el('pm-st-wr').textContent     = (g.win_rate ?? 0).toFixed(1) + '%';
}

// ═══════════════════════════════════════════════════════════════
// SIGNAL HELPERS
// ═══════════════════════════════════════════════════════════════
function sigClass(s) {
  if (!s) return 'sig-neutral';
  const u = s.toUpperCase();
  if (u.includes('STRONGLY') && u.includes('BULL')) return 'sig-strong-bull';
  if (u.includes('STRONGLY') && u.includes('BEAR')) return 'sig-strong-bear';
  if (u.includes('MILDLY')   && u.includes('BULL')) return 'sig-mild-bull';
  if (u.includes('MILDLY')   && u.includes('BEAR')) return 'sig-mild-bear';
  return 'sig-neutral';
}
function pillClass(s) {
  if (!s) return 'pill-neutral';
  const u = s.toUpperCase();
  if (u.includes('STRONGLY') && u.includes('BULL')) return 'pill-strong-bull';
  if (u.includes('STRONGLY') && u.includes('BEAR')) return 'pill-strong-bear';
  if (u.includes('MILDLY')   && u.includes('BULL')) return 'pill-mild-bull';
  if (u.includes('MILDLY')   && u.includes('BEAR')) return 'pill-mild-bear';
  return 'pill-neutral';
}
function sigIcon(s) {
  if (!s) return '—';
  const u = s.toUpperCase();
  if (u.includes('STRONGLY') && u.includes('BULL')) return '🚀';
  if (u.includes('STRONGLY') && u.includes('BEAR')) return '🔻';
  if (u.includes('MILDLY')   && u.includes('BULL')) return '📈';
  if (u.includes('MILDLY')   && u.includes('BEAR')) return '📉';
  return '➖';
}

function formatMarketWindow(startIso, endIso) {
  if (!startIso || !endIso) return 'Waiting for market…';
  try {
    const opts = { timeZone: 'America/New_York' };
    const startDate = new Date(startIso).toLocaleString('en-US', { ...opts, month: 'short', day: 'numeric' });
    const startTime = new Date(startIso).toLocaleString('en-US', { ...opts, hour: 'numeric', minute: '2-digit', hour12: true });
    const endTime   = new Date(endIso).toLocaleString('en-US',   { ...opts, hour: 'numeric', minute: '2-digit', hour12: true });
    return `${startDate}, ${startTime} – ${endTime} ET`;
  } catch (e) { return 'Waiting for market…'; }
}

// ═══════════════════════════════════════════════════════════════
// BOT CARDS
// ═══════════════════════════════════════════════════════════════
function renderBots(bots) {
  const container = document.getElementById('bots-container');
  bots.forEach((bot, i) => {
    let card = document.getElementById(`bot-card-${i}`);
    if (!card) {
      card = document.createElement('div');
      card.id = `bot-card-${i}`;
      card.className = 'card bg-zinc-900 rounded-3xl p-5 md:p-6';
      container.appendChild(card);
    }
    card.innerHTML = renderCard(bot);
  });
}

function renderCard(bot) {
  const hasPos   = bot.position && bot.position !== '-';
  const pnlVal   = bot.pnl_dollars || 0;
  const pnlPos   = pnlVal >= 0;
  const cumPos   = (bot.cumulative_pnl || 0) >= 0;
  const inProfit = hasPos && pnlVal > 0;
  const inLoss   = hasPos && pnlVal < 0;
  const border   = inProfit ? 'border-l-4 border-emerald-500'
                 : inLoss   ? 'border-l-4 border-red-500'
                 : hasPos   ? 'border-l-4 border-sky-500' : '';

  const signal   = bot.imbalance_signal || 'NEUTRAL';
  const ratio    = (bot.imbalance_ratio    || 0).toFixed(3);
  const momentum = (bot.imbalance_momentum || 0).toFixed(3);
  const asset    = bot.asset || 'BOT';

  const wins   = bot.wins        ?? 0;
  const losses = bot.losses      ?? 0;
  const trades = bot.trade_count ?? 0;
  const wr     = bot.win_rate    ?? 0;

  const marketWindow = formatMarketWindow(bot.market_start_iso, bot.market_end_iso);

  const statusBadge = inProfit
    ? `<span class="text-xs bg-emerald-950 text-emerald-400 px-2 py-0.5 rounded-full font-semibold">🟢 IN PROFIT</span>`
    : inLoss
      ? `<span class="text-xs bg-red-950 text-red-400 px-2 py-0.5 rounded-full font-semibold">🔴 IN LOSS</span>`
      : hasPos
        ? `<span class="text-xs bg-sky-900 text-sky-300 px-2 py-0.5 rounded-full">⚡ IN POSITION</span>`
        : `<span class="text-xs bg-zinc-800 text-zinc-400 px-2 py-0.5 rounded-full">WAITING</span>`;

  return `
    <div class="${border} rounded-2xl pl-3">

      <!-- header -->
      <div class="flex items-center justify-between mb-1">
        <div class="flex items-center gap-2 flex-wrap">
          <span class="text-xl font-black tracking-tight"
                style="font-family:'Inter',system-ui,sans-serif;letter-spacing:-.02em;">
            ${asset}
          </span>
          ${statusBadge}
        </div>
        <span class="text-zinc-500 text-xs font-mono">${bot.timer || '--:--'}</span>
      </div>

      <!-- market window -->
      <div class="text-zinc-500 text-xs mb-3 font-mono">${marketWindow}</div>

      <!-- YES / NO prices -->
      <div class="flex gap-3 mb-3">
        <div class="flex-1 bg-zinc-800 rounded-xl p-2 text-center">
          <div class="text-zinc-400 text-xs mb-0.5">YES</div>
          <div class="text-lg font-bold text-emerald-400 font-mono"
               style="font-variant-numeric:tabular-nums;">${bot.yes || 0}¢</div>
        </div>
        <div class="flex-1 bg-zinc-800 rounded-xl p-2 text-center">
          <div class="text-zinc-400 text-xs mb-0.5">NO</div>
          <div class="text-lg font-bold text-red-400 font-mono"
               style="font-variant-numeric:tabular-nums;">${bot.no || 0}¢</div>
        </div>
      </div>

      <!-- signal row -->
      <div class="bg-zinc-800 rounded-xl px-3 py-2 mb-3 flex items-center justify-between gap-2 flex-wrap">
        <span class="inline-flex items-center gap-1.5 text-sm font-semibold px-2.5 py-1 rounded-full ${pillClass(signal)}">
          ${sigIcon(signal)} ${signal}
        </span>
        <div class="text-xs text-zinc-400 font-mono">
          Imb <span class="${sigClass(signal)}">${ratio}</span>
          &nbsp;Mom <span class="${sigClass(signal)}">${momentum}</span>
        </div>
      </div>

      <!-- position / pnl rows -->
      <div class="space-y-1.5 text-sm mb-3">
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Position</span>
          <span class="font-mono text-zinc-200">${bot.position || '-'}</span>
        </div>
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Trade PnL</span>
          <span class="font-mono ${pnlPos ? 'text-emerald-400' : 'text-red-400'}"
                style="font-variant-numeric:tabular-nums;">
            ${pnlPos ? '+' : ''}$${(bot.pnl_dollars || 0).toFixed(2)}
            (${(bot.pnl_pct || 0).toFixed(1)}%)
          </span>
        </div>
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Cumulative PnL</span>
          <span class="font-mono font-semibold ${cumPos ? 'text-emerald-300' : 'text-red-300'}"
                style="font-variant-numeric:tabular-nums;">
            ${cumPos ? '+' : ''}$${(bot.cumulative_pnl || 0).toFixed(2)}
          </span>
        </div>
        <div class="flex items-center justify-between">
          <span class="text-zinc-400">Outcome</span>
          <span class="font-mono
            ${bot.outcome === 'YES' ? 'text-emerald-400' : bot.outcome === 'NO' ? 'text-red-400' : 'text-zinc-500'}">
            ${bot.outcome || 'PENDING'}
          </span>
        </div>
      </div>

      <!-- per-asset stats -->
      <div class="bg-zinc-800/60 rounded-xl px-3 py-2 mb-3
                  flex items-center justify-between text-xs">
        <div class="flex flex-col items-center">
          <span class="font-bold text-zinc-200 font-mono">${trades}</span>
          <span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Trades</span>
        </div>
        <div class="flex flex-col items-center">
          <span class="font-bold text-emerald-400 font-mono">${wins}</span>
          <span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Wins</span>
        </div>
        <div class="flex flex-col items-center">
          <span class="font-bold text-red-400 font-mono">${losses}</span>
          <span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Losses</span>
        </div>
        <div class="flex flex-col items-center">
          <span class="font-bold text-yellow-400 font-mono">${wr.toFixed(1)}%</span>
          <span class="text-zinc-500 uppercase" style="font-size:9px;letter-spacing:.08em">Win Rate</span>
        </div>
      </div>

      <!-- listener countdown -->
      <div class="pt-2 border-t border-zinc-800 flex items-center justify-between
                  text-xs text-zinc-500">
        <span>Listener in</span>
        <span class="font-mono">${bot.listener || '--:--'}</span>
      </div>
    </div>
  `;
}

// Boot
_initChartOverlay();
</script>
</body>
</html>"""


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTML_CONTENT.replace("__BOT_NAME__", BOT_DISPLAY_NAME)


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    try:
        while True:
            await asyncio.sleep(30)
    except WebSocketDisconnect:
        if websocket in active_connections:
            active_connections.remove(websocket)


@app.get("/api/status")
async def api_status():
    return {
        "bots": [bot.get_dashboard_data() for bot in bots] if bots else [],
        "global_stats": get_global_stats(),
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")