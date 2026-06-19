import asyncio
import json
import logging
import math
import os
import re
import threading
import requests  # type: ignore
import websockets  # type: ignore
import time as t
import aiohttp
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
from typing import Deque, Dict, Any, Optional, cast, List, Tuple
from dotenv import load_dotenv  # type: ignore
from web3 import Web3  # type: ignore
from web3.types import TxParams, Wei  # type: ignore
from collections import deque
from zoneinfo import ZoneInfo

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.clob_types import OrderArgs, OrderPayload, OrderType, ApiCreds
from py_clob_client_v2.order_builder.constants import BUY, SELL
from py_clob_client_v2 import Side, SignatureTypeV2

from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich.live import Live
from rich import box
from rich.text import Text

console = Console()
load_dotenv()

# ── Structured trade logger ──────────────────────────────────────────────────
_exec_logger = logging.getLogger("emiliano.execution")
_exec_logger.setLevel(logging.DEBUG)
if not _exec_logger.handlers:
    _fh = logging.FileHandler("emiliano_execution.log", encoding="utf-8")
    _fh.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
    _exec_logger.addHandler(_fh)

def exec_log(event: str, **kwargs):
    payload = {"event": event, "ts": t.time(), **kwargs}
    _exec_logger.info(json.dumps(payload))


# ── Configuration ────────────────────────────────────────────────────────────

DRY_MODE = os.getenv("DRY_MODE", "True").lower() == "true"
LOG_FILE  = "emiliano_trades.txt"

HOST        = "https://clob.polymarket.com"
WS_URL      = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_URL   = "https://gamma-api.polymarket.com/markets"
POLYGON_RPC = os.getenv("POLYGON_RPC")

PUSD_ADDRESS      = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"
USDC_E            = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
CTF_CONTRACT      = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
CTF_EXCHANGE      = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
STANDARD_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_ADAPTER  = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"

BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RED    = "\033[91m"
RESET  = "\033[0m"

LISTENER_ACTIVATE_SECONDS = int(os.getenv("LISTENER_ACTIVATE_SECONDS", "300"))
ENTRY_SECONDS_LEFT        = int(os.getenv("ENTRY_SECONDS_LEFT", "300"))

STOP_LOSS_PCT   = float(os.getenv("STOP_LOSS_PCT",   "35"))
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "35"))

# Entry gate: only enter when a side's ask is at or above this price.
MIN_ENTRY_PRICE = float(os.getenv("MIN_ENTRY_PRICE", "0.90"))

# Locked-price sentinels. Entry is skipped when the price equals either value.
LOCKED_LOW  = 0.01  # 1c  — market resolved or buy-side liquidity exhausted
LOCKED_HIGH = 1.00  # 100c — market fully resolved

# Absolute buy ceiling — the +0.01 slippage bump in confirmed_execute can never
# reach exactly $1.00.
MAX_BUY_PRICE = float(os.getenv("MAX_BUY_PRICE", "0.99"))

FINAL_PRICE   = float(os.getenv("FINAL_PRICE", "0.70"))
POSITION_SIZE = float(os.getenv("POSITION_SIZE", "5.10"))

BINANCE_PRIME_THRESHOLD   = float(os.getenv("BINANCE_PRIME_THRESHOLD",   "0.20"))
BINANCE_STALE_CUTOFF_SECS = float(os.getenv("BINANCE_STALE_CUTOFF_SECS", "5.0"))
BINANCE_DEPTH_LIMIT       = int(os.getenv("BINANCE_DEPTH_LIMIT",          "20"))

MIN_FILL_DELTA   = float(os.getenv("MIN_FILL_DELTA",   "0.05"))
FILL_TIMEOUT_SEC = float(os.getenv("FILL_TIMEOUT_SEC", "15.0"))


# ═════════════════════════════════════════════════════════════════════════════
# TRADE STATE MACHINE
# ═════════════════════════════════════════════════════════════════════════════

class OrderState(Enum):
    CREATED          = auto()
    SUBMITTED        = auto()
    OPEN             = auto()
    PARTIALLY_FILLED = auto()
    FILLED           = auto()
    CANCEL_PENDING   = auto()
    CANCELLED        = auto()
    REJECTED         = auto()
    FAILED           = auto()


class TradeState(Enum):
    # No position open. Bot monitors YES and NO for a qualifying ≥90c entry.
    IDLE    = auto()
    # Single directional position is held. Entry logic is fully suppressed.
    # Only TP/SL monitoring runs from this state.
    FILLED  = auto()
    # TP or SL triggered. Sell order is in flight.
    EXITING = auto()
    # Trade cycle complete. reset_state() returns to IDLE for the next market.
    CLOSED  = auto()
    # Unexpected failure. Logged; no further orders until reset_state().
    ERROR   = auto()


# ─────────────────────────────────────────────────────────────────────────────
# LOCKED-PRICE GUARD
# ─────────────────────────────────────────────────────────────────────────────

def is_locked_price(price: float) -> bool:
    # Round to 4 d.p. to absorb floating-point noise from the WebSocket feed.
    rounded = round(price, 4)
    return rounded <= LOCKED_LOW or rounded >= LOCKED_HIGH


# ═════════════════════════════════════════════════════════════════════════════
# REDIS PERSISTENCE LAYER
# ═════════════════════════════════════════════════════════════════════════════

UPSTASH_URL   = os.getenv("UPSTASH_REDIS_REST_URL", "").rstrip("/")
UPSTASH_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
_redis_available = bool(UPSTASH_URL and UPSTASH_TOKEN)

def _redis_headers() -> dict:
    return {"Authorization": f"Bearer {UPSTASH_TOKEN}"}

def redis_get(key: str) -> Optional[str]:
    if not _redis_available:
        return None
    try:
        resp = requests.get(f"{UPSTASH_URL}/get/{key}", headers=_redis_headers(), timeout=4)
        return resp.json().get("result")
    except Exception as e:
        print(f"⚠️ Redis GET error ({key}): {e}")
        return None

def redis_set(key: str, value: str) -> bool:
    if not _redis_available:
        return False
    try:
        resp = requests.get(
            f"{UPSTASH_URL}/set/{key}/{requests.utils.quote(value, safe='')}",  # type: ignore
            headers=_redis_headers(), timeout=4,
        )
        return resp.json().get("result") == "OK"
    except Exception as e:
        print(f"⚠️ Redis SET error ({key}): {e}")
        return False

def redis_set_json(key: str, obj: Any) -> bool:
    return redis_set(key, json.dumps(obj))

def redis_get_json(key: str) -> Optional[Any]:
    raw = redis_get(key)
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════════════
# PORTFOLIO HISTORY — persistent equity-curve storage
#
# Redis key  : emiliano:portfolio:history
# Value      : JSON list of {"t": unix_ms, "v": portfolio_pnl_dollars}
#              sorted oldest-first, capped at MAX_HISTORY_POINTS entries.
#
# Backfill   : On startup (called from main.py's startup_event) we inspect
#              all emiliano:{asset}:trades keys already in Redis, reconstruct
#              the full cross-asset portfolio equity curve from those trade
#              records, and persist it to emiliano:portfolio:history.
#              This means the chart shows real history from the very first
#              trade — not just from the day this feature was deployed.
#
# Idempotent : The backfill compares the number of trade records in Redis
#              against the existing history length. If history already covers
#              every trade it does nothing. If new trades exist beyond what is
#              already stored it merges only the missing tail. Safe to rerun.
#
# Live feed  : log_pnl() calls portfolio_history_snapshot() immediately after
#              every completed trade so the curve updates in real time without
#              waiting for any background flush interval.
# ═════════════════════════════════════════════════════════════════════════════

PORTFOLIO_HISTORY_KEY = "emiliano:portfolio:history"
MAX_HISTORY_POINTS    = 60_000          # ~1 year at 1 pt per 10 min

# All assets the bot tracks across every worker.
_ALL_ASSETS = ["btc", "eth", "sol", "xrp"]

# ── Process-wide worker registry ──────────────────────────────────────────
# ROOT CAUSE OF CHART SPIKES (see notes above portfolio_history_snapshot):
# log_pnl() used to push *one asset's* cumulative_pnl into the shared,
# cross-asset portfolio history key. That single-asset value would land
# right next to genuinely correct cross-asset totals (written every 60 s by
# main.py's portfolio_snapshot_loop, and by the startup backfill), producing
# a sharp up/down spike on every single trade close.
#
# Fix: every MarketWorker registers itself here at construction time. Any
# code that needs "the total portfolio PnL right now" sums cumulative_pnl
# across every registered worker instead of using one worker's own value.
_worker_registry: List["MarketWorker"] = []
_worker_registry_lock = threading.Lock()


def _register_worker(worker: "MarketWorker") -> None:
    with _worker_registry_lock:
        if worker not in _worker_registry:
            _worker_registry.append(worker)


def _portfolio_total_pnl() -> float:
    """Sum cumulative_pnl across every live MarketWorker in this process.

    This is the single source of truth for 'total portfolio PnL right now' —
    every write into emiliano:portfolio:history must go through this (or the
    equivalent get_global_stats()-based total in main.py) rather than any
    one worker's own self.cumulative_pnl.
    """
    with _worker_registry_lock:
        workers = list(_worker_registry)
    total = 0.0
    for w in workers:
        try:
            v = getattr(w, "cumulative_pnl", 0.0)
            if _is_finite_number(v):
                total += v
        except Exception:
            continue
    return round(total, 4)


# ── Value / timestamp validation ──────────────────────────────────────────

def _is_finite_number(v: Any) -> bool:
    """True only for real, finite int/float values. Rejects None, NaN, Inf,
    bool, strings, etc. Used to keep corrupted values out of persisted
    history and to reject them again at read time as a second line of
    defense."""
    if isinstance(v, bool):
        return False
    if not isinstance(v, (int, float)):
        return False
    try:
        return math.isfinite(float(v))
    except Exception:
        return False


def sanitize_portfolio_history(points: List[Dict], *, drop_isolated_spikes: bool = True) -> List[Dict]:
    """
    Single source of truth for cleaning a portfolio-history point list before
    it is either persisted to Redis or served to the chart.

    Guarantees on the returned list:
      • Every point has a finite numeric 't' (int, unix ms) and 'v' (float).
      • No two points share the same 't' (last-write-wins on duplicates).
      • Strictly increasing 't' (i.e. chronological order is enforced, not
        just sorted — true duplicates are already gone by the time we sort).
      • Optionally drops "isolated spikes": a single point whose value jumps
        far away from both neighbors and then jumps right back, which is the
        exact signature of a one-off corrupted write landing between two
        otherwise-correct totals.

    This function is intentionally conservative about the spike filter —
    genuine, large, sustained portfolio swings are never removed, only
    single-point spike-and-revert artifacts.
    """
    cleaned: Dict[int, float] = {}
    for p in points:
        if not isinstance(p, dict):
            continue
        t_raw = p.get("t")
        v_raw = p.get("v")
        if t_raw is None or v_raw is None:
            continue
        try:
            t_ms = int(t_raw)
        except (TypeError, ValueError):
            continue
        if not _is_finite_number(v_raw):
            continue
        if t_ms <= 0:
            continue
        v = round(float(v_raw), 4)
        # Last write wins for exact-duplicate timestamps (e.g. a retried
        # write, or a 60-s loop tick that lands on the same millisecond as a
        # trade-close write).
        cleaned[t_ms] = v

    ordered = [{"t": ts, "v": v} for ts, v in sorted(cleaned.items())]

    if drop_isolated_spikes and len(ordered) >= 3:
        ordered = _drop_isolated_spikes(ordered)

    return ordered


def _drop_isolated_spikes(points: List[Dict]) -> List[Dict]:
    """
    Remove single-point spikes: a point whose value jumps far from BOTH
    neighbors, where the neighbors themselves are close to each other (i.e.
    the series jumps away and immediately jumps back). This is the exact
    shape produced by a stray bad write landing between two correct points,
    and it is conservative enough to leave real, sustained PnL moves intact.
    """
    if len(points) < 3:
        return points

    values = [p["v"] for p in points]
    diffs  = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    diffs_sorted = sorted(diffs)
    mid = len(diffs_sorted) // 2
    median_step = diffs_sorted[mid] if diffs_sorted else 0.0

    # Floor so that a perfectly flat or near-flat series doesn't make the
    # spike threshold collapse to (near) zero.
    floor = max(5.0, median_step * 4)

    keep = [True] * len(points)
    for i in range(1, len(points) - 1):
        prev_v, cur_v, next_v = values[i - 1], values[i], values[i + 1]
        jump_in  = abs(cur_v - prev_v)
        jump_out = abs(next_v - cur_v)
        settle   = abs(next_v - prev_v)
        if jump_in < floor or jump_out < floor:
            continue
        # Both surrounding jumps are large, but the series basically returns
        # to where it started → this point is an isolated spike.
        if settle <= max(jump_in, jump_out) * 0.35:
            keep[i] = False

    return [p for p, k in zip(points, keep) if k]

# Timestamp format written by log_pnl() via datetime.now().strftime(...)
_TS_PRIMARY = "%Y-%m-%d %H:%M:%S"

# Additional formats found in older records or alternative paths.
_TS_FORMATS: List[str] = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
]


def _parse_ts(ts_str: str) -> Optional[int]:
    """Parse a trade timestamp string → Unix milliseconds. Returns None on failure."""
    if not ts_str:
        return None
    for fmt in _TS_FORMATS:
        try:
            dt = datetime.strptime(ts_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _load_trades_for_asset(asset: str) -> List[Dict]:
    """
    Return the full trade list for one asset from Redis (primary) or the
    local JSON fallback.  Each item must have 'timestamp' and 'cumulative_pnl'.
    """
    if _redis_available:
        trades = redis_get_json(f"emiliano:{asset}:trades")
        if trades and isinstance(trades, list) and len(trades) > 0:
            return trades
    # Local JSON fallback
    fp = f"{asset}_pnl_history.json"
    try:
        if os.path.exists(fp):
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            trades = data.get("trades", [])
            if trades:
                return trades
    except Exception as e:
        print(f"⚠️ [backfill] Could not read {fp}: {e}")
    return []


def _build_equity_curve(all_trades_by_asset: Dict[str, List[Dict]]) -> List[Dict]:
    """
    Given {asset: [trade, ...]}, build a time-sorted portfolio equity curve.

    Algorithm:
      1. Tag every trade with its asset and parse its timestamp.
      2. Sort all events globally by timestamp.
      3. Walk the events, maintaining each asset's last known cumulative PnL.
      4. At each event emit {t: unix_ms, v: sum_of_all_asset_pnls}.

    The result is a list of {"t": unix_ms, "v": float} dicts, oldest-first.
    Duplicate timestamps are de-duplicated (last value wins).
    """
    events: List[Tuple[int, str, float]] = []  # (unix_ms, asset, cumulative_pnl)

    for asset, trades in all_trades_by_asset.items():
        for trade in trades:
            cum = trade.get("cumulative_pnl")
            if cum is None or not _is_finite_number(cum):
                continue
            ts_ms = _parse_ts(trade.get("timestamp", ""))
            if ts_ms is None:
                continue
            events.append((ts_ms, asset, float(cum)))

    if not events:
        return []

    events.sort(key=lambda e: e[0])

    asset_pnl: Dict[str, float] = {a: 0.0 for a in _ALL_ASSETS}
    seen_ts: Dict[int, float]   = {}

    for ts_ms, asset, cum_pnl in events:
        asset_pnl[asset] = cum_pnl
        total = round(sum(asset_pnl.values()), 4)
        seen_ts[ts_ms] = total          # last-write-wins for duplicate timestamps

    curve = [{"t": ts, "v": v} for ts, v in sorted(seen_ts.items())]
    curve = sanitize_portfolio_history(curve, drop_isolated_spikes=False)
    return curve


def portfolio_history_backfill() -> int:
    """
    Idempotent backfill: reads all existing emiliano:{asset}:trades data from
    Redis and reconstructs emiliano:portfolio:history.

    Returns the number of backfill points written (0 = nothing to do / no data).

    Idempotency guarantee
    ─────────────────────
    • Count total trade records across all assets in Redis (N_trades).
    • Read existing portfolio history length (N_hist).
    • If N_hist >= N_trades → history already covers every trade → skip.
    • Otherwise rebuild the full curve and MERGE:
        – Points whose timestamps already exist in history are NOT overwritten
          so any manually-entered or live-snapshot points are preserved.
        – New trade-derived points are inserted into the correct chronological
          position.
    """
    if not _redis_available:
        print("ℹ️  [backfill] Redis not available — skipping portfolio history backfill.")
        return 0

    print("🔄 [backfill] Inspecting existing Redis trade data...")

    # Step 1: load all trade records
    all_trades: Dict[str, List[Dict]] = {}
    total_trade_count = 0
    for asset in _ALL_ASSETS:
        trades = _load_trades_for_asset(asset)
        all_trades[asset] = trades
        print(f"  [{asset.upper()}] {len(trades)} trade records found")
        total_trade_count += len(trades)

    if total_trade_count == 0:
        print("ℹ️  [backfill] No trade records found in Redis — nothing to backfill.")
        return 0

    # Step 2: check how many history points already exist
    existing_history: List[Dict] = redis_get_json(PORTFOLIO_HISTORY_KEY) or []
    existing_count = len(existing_history)

    if existing_count >= total_trade_count:
        print(f"✅ [backfill] Portfolio history already has {existing_count} points "
              f"covering {total_trade_count} trades — skipping rebuild.")
        return 0

    print(f"📊 [backfill] History has {existing_count} pts, trades have {total_trade_count} "
          f"records — rebuilding equity curve...")

    # Step 3: build the full curve from trade records
    backfill_curve = _build_equity_curve(all_trades)
    if not backfill_curve:
        print("⚠️  [backfill] Could not build equity curve (no parseable timestamps).")
        return 0

    # Step 4: merge with existing (already-sanitized) history
    # Existing points that post-date the last backfill point (live snapshots
    # recorded since previous deploy) are preserved; backfill replaces older pts.
    existing_history = sanitize_portfolio_history(existing_history, drop_isolated_spikes=False)
    last_backfill_t = backfill_curve[-1]["t"] if backfill_curve else 0
    live_tail = [p for p in existing_history if p["t"] > last_backfill_t]

    merged: Dict[int, float] = {}
    for p in backfill_curve:
        merged[p["t"]] = p["v"]
    for p in live_tail:
        merged[p["t"]] = p["v"]    # live points win on any overlap

    final_curve = sanitize_portfolio_history(
        [{"t": ts, "v": v} for ts, v in sorted(merged.items())],
        drop_isolated_spikes=False,
    )

    # Cap to max points
    if len(final_curve) > MAX_HISTORY_POINTS:
        final_curve = final_curve[-MAX_HISTORY_POINTS:]

    # Step 5: write back (best-effort backup of the pre-backfill state so a
    # bad rebuild can always be rolled back manually)
    if existing_history:
        redis_set_json(f"{PORTFOLIO_HISTORY_KEY}:backup:pre_backfill", existing_history)

    ok = redis_set_json(PORTFOLIO_HISTORY_KEY, final_curve)
    if ok:
        print(f"✅ [backfill] Wrote {len(final_curve)} portfolio history points to Redis "
              f"({len(backfill_curve)} from trades + {len(live_tail)} live tail).")
    else:
        print("❌ [backfill] Redis write failed.")
        return 0

    return len(backfill_curve)


# Serializes all read-modify-write cycles against PORTFOLIO_HISTORY_KEY within
# this process. log_pnl() (per-trade writes) and portfolio_snapshot_loop in
# main.py (60-s heartbeat writes) both call portfolio_history_snapshot(), and
# without this lock two near-simultaneous calls could each read the same
# "existing" list, then write back, with one call's point silently lost
# (a classic lost-update race). A threading.Lock (not asyncio.Lock) is used
# deliberately: log_pnl is a plain synchronous function and may be called
# from sync contexts (e.g. the cleanup/migration script) as well as from
# inside async methods, so the lock must work in both.
#
# Note on scope: this protects against races *within a single process*. If
# this dashboard is ever scaled to more than one Render instance writing the
# same Redis key, a true cross-process lock (Redis MULTI/Lua, or a proper
# distributed mutex) would be required — the lightweight Upstash REST client
# used here only exposes plain GET/SET, not atomic compare-and-swap.
_history_write_lock = threading.Lock()

# Tracks the most recent round-key written per (asset, slug) so a duplicate
# log_pnl() call for the same market round (e.g. a retried exit handler)
# cannot double-write a snapshot for that round.
_recent_round_writes: Dict[str, float] = {}
_ROUND_DEDUP_TTL_SEC = 600  # 10 minutes — far longer than one 5-min round


def _round_already_written(round_key: Optional[str]) -> bool:
    if not round_key:
        return False
    now = t.time()
    # Opportunistically prune old entries so this dict never grows unbounded.
    expired = [k for k, ts in _recent_round_writes.items() if now - ts > _ROUND_DEDUP_TTL_SEC]
    for k in expired:
        _recent_round_writes.pop(k, None)
    return round_key in _recent_round_writes


def _mark_round_written(round_key: Optional[str]) -> None:
    if round_key:
        _recent_round_writes[round_key] = t.time()


def portfolio_history_snapshot(total_pnl: float, round_key: Optional[str] = None) -> bool:
    """
    Append one live {t, v} snapshot to Redis immediately.

    Called by MarketWorker.log_pnl() right after every completed trade so the
    chart updates the moment a market round closes — no background flush delay.

    Also called by main.py's portfolio_snapshot_loop every 60 s during normal
    operation so the curve stays continuous even in idle periods with no trades.

    Parameters
    ──────────
    total_pnl : the TOTAL portfolio PnL across every asset right now — never
                a single asset's own cumulative_pnl. Callers must use
                _portfolio_total_pnl() (or main.py's get_global_stats()
                total) to compute this.
    round_key : optional unique key (e.g. "{asset}:{slug}") identifying the
                market round this write corresponds to. When provided, a
                second call with the same round_key within
                _ROUND_DEDUP_TTL_SEC is ignored — this is the "only one valid
                snapshot per market round" safeguard.

    Returns True on success (including a successful no-op skip for a
    duplicate round_key — there is nothing left to do, which counts as
    success), False on a real failure.
    """
    if not _is_finite_number(total_pnl):
        print(f"⚠️ portfolio_history_snapshot rejected non-finite value: {total_pnl!r}")
        return False

    if _round_already_written(round_key):
        print(f"ℹ️ portfolio_history_snapshot skipped duplicate round write for {round_key!r}")
        return True

    if not _redis_available:
        _mark_round_written(round_key)
        return False

    with _history_write_lock:
        try:
            existing: List[Dict] = redis_get_json(PORTFOLIO_HISTORY_KEY) or []
            existing = sanitize_portfolio_history(existing, drop_isolated_spikes=False)
            now_ms = int(t.time() * 1000)
            new_v  = round(float(total_pnl), 4)

            # Avoid near-duplicate timestamps: if the last point is within
            # 2 s just update it in place rather than adding a near-zero-
            # width vertical segment.
            if existing and (now_ms - existing[-1]["t"]) < 2000:
                existing[-1]["v"] = new_v
            else:
                existing.append({"t": now_ms, "v": new_v})

            existing = sanitize_portfolio_history(existing, drop_isolated_spikes=False)

            if len(existing) > MAX_HISTORY_POINTS:
                existing = existing[-MAX_HISTORY_POINTS:]

            ok = redis_set_json(PORTFOLIO_HISTORY_KEY, existing)
            if ok:
                _mark_round_written(round_key)
            return ok
        except Exception as e:
            print(f"⚠️ portfolio_history_snapshot error: {e}")
            return False


def portfolio_history_get(period: str = "ALL") -> List[Dict]:
    """
    Fetch portfolio history filtered to the requested period.
    period: '1D' | '1W' | '1M' | '1Y' | 'ALL'
    Returns list of {t: unix_ms, v: pnl} dicts, oldest-first, fully
    sanitized (validated, de-duplicated, strictly chronological, with
    isolated single-point spikes removed).
    Always returns at least the most recent point as a baseline.
    """
    raw_pts: List[Dict] = redis_get_json(PORTFOLIO_HISTORY_KEY) or []
    all_pts = sanitize_portfolio_history(raw_pts, drop_isolated_spikes=True)

    if not all_pts or period.upper() == "ALL":
        return all_pts

    period_ms: Optional[int] = {
        "1D":  24 * 3600 * 1000,
        "1W":  7  * 24 * 3600 * 1000,
        "1M":  30 * 24 * 3600 * 1000,
        "1Y":  365 * 24 * 3600 * 1000,
    }.get(period.upper())

    if period_ms is None:
        return all_pts

    cutoff   = int(t.time() * 1000) - period_ms
    filtered = [p for p in all_pts if p["t"] >= cutoff]

    # Always return at least one point so the chart has a left-edge anchor
    if not filtered and all_pts:
        filtered = [all_pts[-1]]

    return filtered


# ═════════════════════════════════════════════════════════════════════════════
# BINANCE DEPTH SIGNAL  (display / context only — not a trade gate)
# ═════════════════════════════════════════════════════════════════════════════

class BinanceDepthSignal:
    _instances: Dict[str, "BinanceDepthSignal"] = {}
    _instance_lock = asyncio.Lock()

    @classmethod
    async def get_or_create(cls, symbol: str) -> "BinanceDepthSignal":
        async with cls._instance_lock:
            sym = symbol.upper()
            if sym not in cls._instances:
                inst = cls(sym)
                cls._instances[sym] = inst
                asyncio.create_task(inst._run())
            return cls._instances[sym]

    def __init__(self, symbol: str):
        self.symbol      = symbol
        self.imbalance   = 0.0
        self.momentum    = 0.0
        self.last_update = 0.0
        self._history: deque = deque(maxlen=8)
        self._running    = False

    @property
    def is_fresh(self) -> bool:
        return (t.time() - self.last_update) < BINANCE_STALE_CUTOFF_SECS

    @property
    def is_primed(self) -> bool:
        return self.is_fresh and abs(self.imbalance) >= BINANCE_PRIME_THRESHOLD

    @property
    def signal_label(self) -> str:
        if not self.is_fresh:
            return "STALE"
        if abs(self.imbalance) >= BINANCE_PRIME_THRESHOLD:
            return "STRONGLY BULL ↑" if self.imbalance > 0 else "STRONGLY BEAR ↓"
        if abs(self.imbalance) >= 0.10:
            return "MILDLY BULL ↑"   if self.imbalance > 0 else "MILDLY BEAR ↓"
        return "NEUTRAL"

    async def _run(self):
        if self._running:
            return
        self._running = True
        stream = f"{self.symbol.lower()}usdt@depth{BINANCE_DEPTH_LIMIT}@100ms"
        url    = f"wss://fstream.binance.com/stream?streams={stream}"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=15) as ws:
                    print(f"📡 [Binance WS] Connected: {stream}")
                    async for raw in ws:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        bids = data.get("b", [])
                        asks = data.get("a", [])
                        if bids and asks:
                            self._process(bids, asks)
            except Exception as e:
                print(f"⚠️ [Binance WS] {self.symbol}: {e} — reconnecting in 3s")
                await asyncio.sleep(3)

    def _process(self, bids: list, asks: list):
        def weighted_vol(levels: list) -> float:
            tw = tv = 0.0
            for i, item in enumerate(levels[:20]):
                qty = float(item[1])
                w   = 1.0 / (i + 1) ** 0.6
                tv += qty * w
                tw += w
            return tv / tw if tw > 0 else 0.0

        bid_v = weighted_vol(bids)
        ask_v = weighted_vol(asks)
        total = bid_v + ask_v
        if total <= 0:
            return
        raw = (bid_v - ask_v) / total
        self._history.append(raw)
        self.last_update = t.time()
        if len(self._history) >= 4:
            recent         = list(self._history)[-4:]
            self.imbalance = sum(recent) / len(recent)
            self.momentum  = recent[-1] - recent[0]
        else:
            self.imbalance = raw
            self.momentum  = 0.0


# ═════════════════════════════════════════════════════════════════════════════
# ACCOUNT SERVICE — GLOBAL, SINGLE-INSTANCE, BOT-LEVEL
# ═════════════════════════════════════════════════════════════════════════════
#
# Everything in this class represents work that is tied to the WALLET / ACCOUNT,
# not to any individual market. There is exactly one AccountService for the
# entire bot process, constructed once in main() and shared by reference into
# every MarketWorker. Nothing in here is duplicated per asset.
#
# Responsibilities:
#   • Single Web3 connection + single ClobClient (auth derived/created once)
#   • One-time startup wallet audit (balances + on-chain approvals)
#   • Shared order-placement / order-status / cancel helpers used by all workers
#   • Shared on-chain share-balance lookups used by all workers
#   • Global periodic PnL merge background task (+ on-demand merge_now())
#
# Per-market logic (price listening, entry/exit, TP/SL, per-asset PnL bookkeeping)
# stays in MarketWorker — see below.
# ═════════════════════════════════════════════════════════════════════════════

class AccountService:
    def __init__(self):
        pk     = os.getenv("PRIVATE_KEY")
        funder = os.getenv("FUNDER_ADDRESS")
        if not pk or not funder:
            raise ValueError("Missing PRIVATE_KEY or FUNDER_ADDRESS in .env")

        self.w3             = Web3(Web3.HTTPProvider(POLYGON_RPC))
        self.wallet_address = self.w3.to_checksum_address(funder)
        self.signer_address = self.w3.eth.account.from_key(pk).address
        self.private_key    = pk

        print(f"Signer : {self.signer_address}")
        print(f"Funder : {self.wallet_address}")

        _l1_client = ClobClient(
            host=HOST, key=pk, chain_id=137, funder=funder, signature_type=3  # type: ignore
        )

        print("🔑 Authenticating with Polymarket (V2)...")
        try:
            raw_creds = _l1_client.derive_api_key()
            if raw_creds is None or not getattr(raw_creds, 'api_key', None):
                print("⚠️  No existing key found — creating new one...")
                raw_creds = _l1_client.create_api_key()
            print("✅ API Authentication Successful")
        except Exception as e:
            print(f"❌ Authentication Failed: {e}")
            raise

        # Single shared ClobClient — used by every MarketWorker for order
        # placement / status / cancellation. There is only ever one of these
        # for the whole process, regardless of how many assets are tracked.
        self.client = ClobClient(
            host=HOST, key=pk, chain_id=137, funder=funder,
            signature_type=3, creds=raw_creds,  # type: ignore
        )

        # Guards so audit/init work can never accidentally run twice even if
        # something calls these methods more than once.
        self._audited = False
        self._merge_task: Optional[asyncio.Task] = None

    # ── On-chain approvals (account-level — run once for the whole wallet) ──

    def set_approvals(self, operator_address: str, label: str):
        print(f"⏳ Sending approval for {label}...")
        ctf_abi = [
            {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
             "name": "setApprovalForAll", "outputs": [], "type": "function"}
        ]
        ctf_contract   = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=ctf_abi)
        signer_account = self.w3.eth.account.from_key(os.getenv("PRIVATE_KEY"))
        signer_address = signer_account.address
        try:
            gas_balance = self.w3.eth.get_balance(signer_address)
            if gas_balance < self.w3.to_wei(0.01, 'ether'):
                print(f"❌ Signer ({signer_address}) needs at least 0.01 POL for gas.")
                return False
            current_gas_price   = self.w3.eth.gas_price
            increased_gas_price = Wei(int(current_gas_price * 1.2))
            tx_params: TxParams = {
                'from':     signer_address,
                'nonce':    self.w3.eth.get_transaction_count(signer_address, "pending"),
                'gas':      100000,
                'gasPrice': increased_gas_price,
                'chainId':  137,
            }
            tx = ctf_contract.functions.setApprovalForAll(
                self.w3.to_checksum_address(operator_address), True
            ).build_transaction(tx_params)
            signed_tx = self.w3.eth.account.sign_transaction(tx, os.getenv("PRIVATE_KEY"))
            tx_hash   = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"✅ Approval sent! Hash: {tx_hash.hex()}")
            self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120, poll_latency=2.0)
            return True
        except Exception as e:
            print(f"❌ Approval failed for {label}: {e}")
            return False

    def get_pol_balance(self):
        balance_wei = self.w3.eth.get_balance(self.wallet_address)
        return float(self.w3.from_wei(balance_wei, 'ether'))

    def check_and_approve_pusd(self, spender_address: str, label: str):
        pusd_abi = [
            {"inputs": [{"name": "owner",   "type": "address"}, {"name": "spender", "type": "address"}],
             "name": "allowance", "outputs": [{"name": "", "type": "uint256"}], "type": "function"},
            {"inputs": [{"name": "spender", "type": "address"}, {"name": "amount",  "type": "uint256"}],
             "name": "approve",   "outputs": [{"name": "", "type": "bool"}],    "type": "function"},
        ]
        pusd_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(PUSD_ADDRESS), abi=pusd_abi)
        spender           = self.w3.to_checksum_address(spender_address)
        current_allowance = pusd_contract.functions.allowance(self.wallet_address, spender).call()
        if current_allowance < 1_000_000:
            print(f"🔓 [GAS] Approving pUSD for {label}...")
            tx_params = cast(TxParams, {
                'from':     self.signer_address,
                'nonce':    self.w3.eth.get_transaction_count(self.signer_address, "pending"),
                'gas':      60000,
                'gasPrice': int(self.w3.eth.gas_price * 1.2),
                'chainId':  137,
            })
            raw_tx    = pusd_contract.functions.approve(spender, 2**256 - 1).build_transaction(tx_params)
            signed_tx = self.w3.eth.account.sign_transaction(raw_tx, os.getenv("FUNDER_PRIVATE_KEY"))
            tx_hash   = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            self.w3.eth.wait_for_transaction_receipt(tx_hash, poll_latency=2.0)
            print(f"✅ {label} pUSD: Approved.")
        else:
            print(f"✅ {label} pUSD: Already Approved")

    def check_and_approve_shares(self, operator_address: str, label: str):
        ctf_abi = [
            {"inputs": [{"name": "account",  "type": "address"}, {"name": "operator", "type": "address"}],
             "name": "isApprovedForAll", "outputs": [{"name": "", "type": "bool"}], "type": "function"},
            {"inputs": [{"name": "operator", "type": "address"}, {"name": "approved", "type": "bool"}],
             "name": "setApprovalForAll", "outputs": [], "type": "function"},
        ]
        ctf_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=ctf_abi)
        operator    = self.w3.to_checksum_address(operator_address)
        is_approved = ctf_contract.functions.isApprovedForAll(self.wallet_address, operator).call()
        if not is_approved:
            print(f"🔓 [POL TX] Funder granting {label} permission to handle shares...")
            funder_pk = os.getenv("FUNDER_PRIVATE_KEY")
            if not funder_pk:
                print(f"❌ Cannot approve {label}. Add FUNDER_PRIVATE_KEY to .env.")
                return
            tx_params: TxParams = {
                'from':     self.signer_address,
                'nonce':    self.w3.eth.get_transaction_count(self.signer_address, "pending"),
                'gas':      120000,
                'gasPrice': Wei(int(self.w3.eth.gas_price * 1.5)),
                'chainId':  137,
            }
            raw_tx    = ctf_contract.functions.setApprovalForAll(operator, True).build_transaction(tx_params)
            signed_tx = self.w3.eth.account.sign_transaction(raw_tx, funder_pk)
            tx_hash   = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"⏳ Confirming {label} approval... Hash: {tx_hash.hex()}")
            self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
            print(f"✅ {label} Shares Enabled.")
        else:
            print(f"✅ {label} Shares: Already Approved")

    def run_wallet_audit(self) -> bool:
        """
        Account-level wallet audit: balance check + (if not DRY_MODE) on-chain
        approvals. This runs EXACTLY ONCE for the entire bot process — at
        startup, before any MarketWorker begins trading — regardless of how
        many assets are being tracked. It must never be called per-asset.
        """
        if self._audited:
            print("ℹ️  Wallet audit already completed this session — skipping duplicate run.")
            return True

        min_abi = [
            {"constant": True, "inputs": [{"name": "_owner", "type": "address"}],
             "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"},
            {"constant": True, "inputs": [], "name": "decimals",
             "outputs": [{"name": "", "type": "uint8"}], "type": "function"},
        ]
        pusd_contract  = self.w3.eth.contract(
            address=self.w3.to_checksum_address(PUSD_ADDRESS), abi=min_abi)
        usdce_contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(USDC_E), abi=min_abi)

        signer_pusd  = pusd_contract.functions.balanceOf(self.signer_address).call() / 10**6
        funder_pusd  = pusd_contract.functions.balanceOf(self.wallet_address).call() / 10**6
        funder_usdce = usdce_contract.functions.balanceOf(self.wallet_address).call() / 10**6

        print(f"\n💵 Signer pUSD Balance : {signer_pusd:.2f} pUSD")
        print(f"💵 Funder pUSD Balance : {funder_pusd:.2f} pUSD")
        print(f"💵 Funder USDC.e       : {funder_usdce:.2f} USDC.e  (legacy — not used as collateral)")

        if not DRY_MODE:
            operators = [
                (STANDARD_EXCHANGE, "Main Exchange"),
                (NEG_RISK_EXCHANGE, "Neg-Risk Exchange"),
                (NEG_RISK_ADAPTER,  "Neg-Risk Adapter"),
            ]
            for addr, label in operators:
                self.check_and_approve_pusd(addr, label)
                self.check_and_approve_shares(addr, label)

        self._audited = True
        return True

    # ── Shared on-chain balance helpers (used by every MarketWorker) ────────

    async def get_onchain_share_balance_async(self, token_id: str, retries: int = 3) -> float:
        abi = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
                "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}]
        contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=abi)
        for attempt in range(retries):
            try:
                raw_balance = contract.functions.balanceOf(
                    self.wallet_address, int(token_id)).call()
                return float(raw_balance / 10**6)
            except Exception as e:
                if attempt < retries - 1:
                    wait = (attempt + 1) * 2
                    print(f"⚠️ RPC Glitch: {e}. Retrying in {wait}s...")
                    await asyncio.sleep(wait)
                else:
                    print(f"❌ CRITICAL: On-chain balance check failed after {retries} attempts.")
                    return -1.0
        return -1.0

    def get_onchain_share_balance(self, token_id: str, retries: int = 3) -> float:
        """Synchronous version kept for non-async call sites (approvals, audits)."""
        abi = [{"inputs": [{"name": "account", "type": "address"}, {"name": "id", "type": "uint256"}],
                "name": "balanceOf", "outputs": [{"name": "", "type": "uint256"}], "type": "function"}]
        contract = self.w3.eth.contract(
            address=self.w3.to_checksum_address(CTF_CONTRACT), abi=abi)
        for attempt in range(retries):
            try:
                raw_balance = contract.functions.balanceOf(
                    self.wallet_address, int(token_id)).call()
                return float(raw_balance / 10**6)
            except Exception as e:
                if attempt < retries - 1:
                    wait = (attempt + 1) * 2
                    print(f"⚠️ RPC Glitch: {e}. Retrying in {wait}s...")
                    t.sleep(wait)
                else:
                    print(f"❌ CRITICAL: On-chain balance check failed after {retries} attempts.")
                    return -1.0
        return -1.0

    async def merge_shares(self, active_market: Optional[Dict[str, Any]], amount_to_merge: float):
        """Merge YES+NO shares back into pUSD on-chain. Account-level operation —
        takes the market dict explicitly since the position itself is per-market."""
        if active_market is None:
            print("❌ Merge aborted: No active market metadata found.")
            return
        CTF_MAIN = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
        ctf_abi = [{
            "name": "mergePositions", "type": "function",
            "inputs": [
                {"name": "collateralToken",    "type": "address"},
                {"name": "parentCollectionId", "type": "bytes32"},
                {"name": "conditionId",        "type": "bytes32"},
                {"name": "partition",          "type": "uint256[]"},
                {"name": "amount",             "type": "uint256"},
            ],
            "outputs": [],
        }]
        target_address = self.w3.to_checksum_address(CTF_MAIN)
        contract       = self.w3.eth.contract(address=target_address, abi=ctf_abi)
        raw_amount     = int(amount_to_merge * 10**6)
        try:
            parent_id = "0x" + "0" * 64
            partition = [1, 2]
            cond_id   = active_market.get('condition_id')
            if not cond_id:
                print("❌ Market metadata missing condition_id.")
                return
            nonce = self.w3.eth.get_transaction_count(self.signer_address, "pending")
            tx = contract.functions.mergePositions(
                PUSD_ADDRESS, parent_id, cond_id, partition, raw_amount
            ).build_transaction({
                'from':     self.signer_address,
                'gas':      180000,
                'gasPrice': self.w3.eth.gas_price,
                'nonce':    nonce,
            })
            signed  = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed.rawTransaction)
            print(f"♻️ Capital Recycled (pUSD)! Hash: {tx_hash.hex()}")
            return tx_hash
        except Exception as e:
            print(f"❌ On-chain Merge Error: {e}")

    # ── Shared order execution helpers (used by every MarketWorker) ─────────

    def create_and_post_order(self, side_str: str, price: float, size: float, token_id: str):
        """Build + submit an order through the single shared ClobClient."""
        order_args   = OrderArgs(price=price, size=size, side=side_str, token_id=token_id)
        signed_order = self.client.create_order(order_args)
        resp         = self.client.post_order(signed_order, cast(OrderType, OrderType.GTC))
        return resp

    def get_order_status(self, order_id: str):
        return self.client.get_order(order_id)

    def cancel_order(self, order_id: str):
        payload = OrderPayload(orderID=order_id)
        return self.client.cancel_order(payload)

    # ── Global periodic PnL merge (background scheduler, single instance) ───

    def start_pnl_merge_scheduler(self):
        """
        Launches the recurring PnL-merge background task exactly once for the
        whole bot. This task runs on a fixed interval (MERGE_INTERVAL_SECONDS)
        independent of any individual trade completing — it is the bot-level
        equivalent of "account/PnL updates" and must not be created per asset.
        """
        if self._merge_task is not None and not self._merge_task.done():
            print("ℹ️  PnL merge scheduler already running — skipping duplicate start.")
            return self._merge_task
        self._merge_task = asyncio.create_task(self._pnl_merge_loop())
        return self._merge_task

    async def _pnl_merge_loop(self):
        while True:
            await asyncio.sleep(MERGE_INTERVAL_SECONDS)
            try:
                merge_all_pnl(send_telegram_notify=False)
            except Exception as e:
                print(f"⚠️ Scheduled PnL merge failed: {e}")

    def merge_now(self, send_telegram_notify: bool = False):
        """On-demand merge, called by a MarketWorker right after a trade closes."""
        merge_all_pnl(send_telegram_notify=send_telegram_notify)

# ═════════════════════════════════════════════════════════════════════════════
# MARKET WORKER — ONE INSTANCE PER TRACKED ASSET/MARKET
# ═════════════════════════════════════════════════════════════════════════════
#
# Everything in this class is scoped to a single asset (e.g. "btc", "eth").
# All wallet/account-level concerns (Web3 connection, ClobClient, wallet audit,
# on-chain approvals, the global PnL-merge scheduler) have been moved OUT of
# this class and into AccountService, which is constructed once in main() and
# passed in here by reference (`account`). MarketWorker never creates its own
# Web3 connection, never calls derive_api_key/create_api_key, and never runs
# the wallet audit — it borrows the shared AccountService for all of that.
#
# What legitimately stays per-instance here:
#   • Order book / price-listener WebSocket subscription for this asset
#   • Entry signal generation, TP/SL exit logic for this asset's position
#   • Per-asset PnL bookkeeping (own Redis keys / own JSON history file)
#   • Per-asset dashboard state
# ═════════════════════════════════════════════════════════════════════════════

class MarketWorker:
    def __init__(self, asset_type: str, account: "AccountService"):
        # Shared, single-instance account/wallet service — injected, not created.
        self.account = account

        # Convenience references so existing per-asset logic that reads
        # self.w3 / self.client / self.wallet_address keeps working unchanged,
        # while the underlying objects are still owned (singly) by AccountService.
        self.w3             = account.w3
        self.client         = account.client
        self.wallet_address = account.wallet_address
        self.signer_address = account.signer_address
        self.private_key    = account.private_key

        self.asset_type    = asset_type.lower()
        self.active_market: Optional[Dict[str, Any]] = None
        self.prices: Dict[str, float] = {"YES": 0.0, "NO": 0.0}
        self.transitioning = False

        self.session_profit = 0.0
        self.trade_count    = 0

        _saved              = self._load_pnl_stats(asset_type.lower())
        self.cumulative_pnl: float = _saved["total_pnl"]
        self.wins: int             = _saved["wins"]
        self.losses: int           = _saved["losses"]

        # Single-leg position tracking
        self.trade_state: TradeState = TradeState.IDLE
        self.position_side: Optional[str] = None   # "YES" or "NO"
        self.position_size: float = 0.0            # shares held
        self.entry_price:   float = 0.0            # actual fill price

        self.exited            = False
        self.processed_markets = set()
        self.entry_timestamp   = None
        self.start_delay_met   = False
        self.market_start_time: Optional[float] = None

        self.dummy_balance  = POSITION_SIZE
        self.last_trade_time = 0
        self.logged_markets  = set()

        self.last_yes_update = 0.0
        self.last_no_update  = 0.0

        self.price_history: deque = deque(maxlen=30)
        self.binance: Optional[BinanceDepthSignal] = None

        self.market_outcome  = None
        self.final_yes_price = 0.0
        self.final_no_price  = 0.0

        self.market_slug = None
        self.seen_markets = set()

        self.market_exit_reasons: Dict[str, str] = {}

        # Single-flight lock — prevents duplicate orders on the same tick.
        self._order_lock = asyncio.Lock()

        self.dashboard = {
            "asset":              asset_type.upper(),
            "yes":                0,
            "no":                 0,
            "timer":              "--:--",
            "listener":           "--:--",
            "status":             "WAITING",
            "bought_side":        "-",
            "entry_price":        0.0,
            "outcome":            "PENDING",
            "profit":             0.0,
            "imbalance_ratio":    0.0,
            "imbalance_momentum": 0.0,
        }
        self.recent_logs: Deque[str] = deque(maxlen=4)

        # Last market slug for which a trade has been logged. Used by
        # log_pnl() as a per-round dedup guard so a retried exit handler
        # cannot double-log (and double-snapshot) the same market round.
        self._last_logged_slug: Optional[str] = None

        # Register with the process-wide worker registry so
        # _portfolio_total_pnl() can sum every asset's PnL when computing
        # the cross-asset portfolio total for the equity chart.
        _register_worker(self)

    # ═════════════════════════════════════════════════════════════════════
    # REDIS-BACKED PnL PERSISTENCE  (per-asset — each market keeps its own
    # trade history / win-loss record, which is intentionally NOT shared)
    # ═════════════════════════════════════════════════════════════════════

    def _redis_stats_key(self) -> str:
        return f"emiliano:{self.asset_type}:stats"

    def _redis_trades_key(self) -> str:
        return f"emiliano:{self.asset_type}:trades"

    @staticmethod
    def _load_pnl_stats(asset_type: str) -> dict:
        if _redis_available:
            data = redis_get_json(f"emiliano:{asset_type}:stats")
            if data:
                print(f"✅ [{asset_type.upper()}] Loaded PnL stats from Redis: "
                      f"PnL=${data.get('total_pnl', 0):.2f} "
                      f"W{data.get('wins', 0)}/L{data.get('losses', 0)}")
                return {
                    "total_pnl": float(data.get("total_pnl", 0.0)),
                    "wins":      int(data.get("wins", 0)),
                    "losses":    int(data.get("losses", 0)),
                }

        file_path = f"{asset_type}_pnl_history.json"
        try:
            if os.path.exists(file_path):
                with open(file_path, "r") as f:
                    data = json.load(f)
                result = {
                    "total_pnl": float(data.get("total_pnl", 0.0)),
                    "wins":      int(data.get("wins", 0)),
                    "losses":    int(data.get("losses", 0)),
                }
                if _redis_available:
                    print(f"📤 [{asset_type.upper()}] Migrating local stats → Redis...")
                    redis_set_json(f"emiliano:{asset_type}:stats", result)
                return result
        except Exception:
            pass

        return {"total_pnl": 0.0, "wins": 0, "losses": 0}

    def _save_stats_to_redis(self):
        if not _redis_available:
            return
        payload = {
            "total_pnl":    round(self.cumulative_pnl, 4),
            "wins":         self.wins,
            "losses":       self.losses,
            "win_rate":     (f"{round((self.wins / (self.wins + self.losses)) * 100, 2)}%"
                             if (self.wins + self.losses) > 0 else "0%"),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        ok = redis_set_json(self._redis_stats_key(), payload)
        if ok:
            print(f"💾 [{self.asset_type.upper()}] Stats saved to Redis: "
                  f"PnL=${self.cumulative_pnl:.2f} W{self.wins}/L{self.losses}")
        else:
            print(f"⚠️ [{self.asset_type.upper()}] Redis save failed — in-memory state preserved.")

    def _append_trade_to_redis(self, entry: dict):
        if not _redis_available:
            return
        existing = redis_get_json(self._redis_trades_key()) or []
        existing.append(entry)
        if len(existing) > 500:
            existing = existing[-500:]
        redis_set_json(self._redis_trades_key(), existing)

    # ═════════════════════════════════════════════════════════════════════
    # DASHBOARD HELPERS
    # ═════════════════════════════════════════════════════════════════════

    def add_log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.recent_logs.append(f"[{timestamp}] {message}")

    def update_dashboard(self):
        self.dashboard["bought_side"] = self.position_side or "-"
        self.dashboard["entry_price"] = self.entry_price
        if self.market_outcome:
            self.dashboard["outcome"] = self.market_outcome

    def get_listener_countdown(self) -> str:
        if not self.active_market:
            return "--:--"
        now          = datetime.now(timezone.utc)
        remaining    = self.active_market["expiry"] - now
        seconds_left = int(remaining.total_seconds())
        if seconds_left <= LISTENER_ACTIVATE_SECONDS:
            return "00:00"
        wait_seconds = seconds_left - LISTENER_ACTIVATE_SECONDS
        mins, secs   = divmod(wait_seconds, 60)
        return f"{mins:02d}:{secs:02d}"

    def get_current_pnl(self) -> Tuple[float, float, str]:
        if not self.position_side or self.entry_price <= 0:
            return 0.0, 0.0, "white"
        current_price = self.prices.get(self.position_side, 0.0)
        if current_price <= 0 or self.position_size <= 0:
            return 0.0, 0.0, "white"
        pnl_pct     = ((current_price - self.entry_price) / self.entry_price) * 100
        pnl_dollars = (current_price - self.entry_price) * self.position_size
        color       = "red" if pnl_dollars < 0 else "green"
        return round(pnl_dollars, 2), round(pnl_pct, 2), color

    def log_to_file(self, message: str):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(f"{timestamp} {message}\n")
        except Exception:
            pass

    def log_trade(self, side: str, price: float, action: str = "BUY",
                  size: float = 0.0, order_id: Optional[str] = None):
        mode_label  = "[DRY]" if DRY_MODE else "[LIVE]"
        market_name = self.active_market['question'] if self.active_market else "Unknown Market"
        price_cents = f"{round(price * 100)}c"
        msg = (f"{mode_label} {action}: {side} @ {price_cents} "
               f"size={size:.4f} | Market: {market_name}")
        self.log_to_file(msg)
        exec_log(
            "trade", mode=mode_label, action=action, side=side,
            price=price, size=size, order_id=order_id, market=market_name,
        )

    # ── Market fetching ────────────────────────────────────────────────

    def fetch_target_market(self, url: str) -> bool:
        try:
            match = re.search(r"/(?:event|market)/([^/?#]+)", url)
            if not match:
                print(f"❌ Could not parse slug from URL: {url}")
                return False
            slug = match.group(1)
            if slug in self.processed_markets:
                print(f"⏭️  BLOCKED: Already processed market: {slug}")
                return False
            resp = requests.get(f"{GAMMA_URL}?slug={slug}").json()
            if not resp:
                print(f"❌ API returned no data for slug: {slug}")
                return False
            m            = resp[0]
            end_date_str = m.get("endDate")
            end_dt       = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            now          = datetime.now(timezone.utc)
            if now >= end_dt:
                print(f"🛑 Market EXPIRED.")
                return False
            clob_ids = m.get("clobTokenIds")
            if isinstance(clob_ids, str):
                clob_ids = json.loads(clob_ids)
            self.active_market = {
                "question": m.get('question'),
                "yes_id":   str(clob_ids[0]),
                "no_id":    str(clob_ids[1]),
                "expiry":   end_dt,
                "slug":     slug,
            }
            self.market_slug = slug
            print(f"🎯 Market: {self.active_market['question']} | Slug: {slug}")
            return True
        except Exception as e:
            print(f"Market Fetch Error: {e}")
            return False

    # ── WebSocket price listener ───────────────────────────────────────

    async def price_listener(self):
        if not self.active_market:
            return
        while True:
            now          = datetime.now(timezone.utc)
            remaining    = self.active_market["expiry"] - now
            seconds_left = int(remaining.total_seconds())
            if seconds_left <= LISTENER_ACTIVATE_SECONDS:
                break
            sleep_time = min(5, seconds_left - LISTENER_ACTIVATE_SECONDS)
            print(f"⏳ Waiting for WS window: {seconds_left}s left...", end="\r")
            await asyncio.sleep(max(1, sleep_time))

        print(f"\n\n📡 Starting WebSocket listener in final {LISTENER_ACTIVATE_SECONDS}s window...")

        async for ws in websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=10):
            try:
                sub_msg = {
                    "operation":  "subscribe",
                    "type":       "market",
                    "assets_ids": [self.active_market["yes_id"], self.active_market["no_id"]],
                }
                await ws.send(json.dumps(sub_msg))
                print(f"\n📡 Subscription active for: {self.active_market['question']}")
                if self.binance is None:
                    sym          = self.asset_type.split("-")[0].upper()
                    self.binance = await BinanceDepthSignal.get_or_create(sym)

                async for message in ws:
                    if self.exited:
                        await ws.close()
                        break
                    now          = datetime.now(timezone.utc)
                    remaining    = self.active_market["expiry"] - now
                    seconds_left = int(remaining.total_seconds())
                    if seconds_left <= 0:
                        print("\n⌛ Market expired. Closing listener...")
                        await asyncio.sleep(3)
                        self.print_final_summary()
                        await ws.close()
                        return
                    mins, secs = divmod(seconds_left, 60)
                    timer_str  = f"{mins:02d}:{secs:02d}"

                    data   = json.loads(message)
                    events = data if isinstance(data, list) else [data]
                    for ev in events:
                        e_type = ev.get("event_type")
                        if e_type in ["book", "initial_state"]:
                            for asset in ev.get("assets", []):
                                aid = asset.get("asset_id")
                                p   = float(asset.get("best_ask", 0))
                                if aid == self.active_market["yes_id"]:
                                    self.prices["YES"] = p
                                elif aid == self.active_market["no_id"]:
                                    self.prices["NO"] = p
                        elif e_type == "price_change":
                            for change in ev.get("price_changes", []):
                                aid = change.get("asset_id")
                                p   = float(change.get("best_ask") or change.get("price", 0))
                                if aid == self.active_market["yes_id"]:
                                    self.prices["YES"] = p
                                elif aid == self.active_market["no_id"]:
                                    self.prices["NO"] = p
                    if self.prices["YES"] > 0 and self.prices["NO"] > 0:
                        self.price_history.append({
                            "ts":  t.time(),
                            "YES": self.prices["YES"],
                            "NO":  self.prices["NO"],
                        })
                        await self.check_logic(timer_str)

            except websockets.exceptions.ConnectionClosed:
                print("\n⚠️ Connection lost. Reconnecting in 3s...")
                await asyncio.sleep(3)
                continue
            except Exception as e:
                print(f"\n❌ Listener Error: {e}")
                await asyncio.sleep(5)
                continue

    # ═════════════════════════════════════════════════════════════════════
    # CORE ENTRY LOGIC — SINGLE-LEG DIRECTIONAL
    # ═════════════════════════════════════════════════════════════════════

    async def check_logic(self, timer: str):
        """
        Single-leg entry logic. Called on every WebSocket price tick.

        Entry conditions (ALL must be true):
          1. trade_state == IDLE  (no position already open)
          2. price >= MIN_ENTRY_PRICE  (>= 90c)
          3. not is_locked_price(price)  (not exactly 1c or 100c)
          4. _order_lock is not held  (no concurrent order in flight)

        YES is evaluated first. If YES qualifies, it is purchased and the
        function returns — NO is never evaluated in the same tick.
        If YES does not qualify, NO is evaluated under the same conditions.

        Once FILLED, check_logic only runs TP/SL evaluation via
        _check_single_side_exit(). No second entry is ever attempted.
        """
        y = self.prices.get("YES", 0.0)
        n = self.prices.get("NO",  0.0)
        if y <= 0 or n <= 0:
            return

        imbalance = self.binance.imbalance if (self.binance and self.binance.is_fresh) else 0.0
        momentum  = self.binance.momentum  if (self.binance and self.binance.is_fresh) else 0.0
        self.dashboard["imbalance_ratio"]    = imbalance
        self.dashboard["imbalance_momentum"] = momentum

        y_c = round(y * 100)
        n_c = round(n * 100)
        self.dashboard["yes"]   = y_c
        self.dashboard["no"]    = n_c
        self.dashboard["timer"] = timer
        self.update_dashboard()

        # Position is open — only monitor TP/SL, never enter a new trade.
        if self.trade_state == TradeState.FILLED:
            await self._check_single_side_exit(self.position_side)
            return

        # Exit/error states — nothing to do until reset.
        if self.trade_state in (TradeState.EXITING, TradeState.CLOSED, TradeState.ERROR):
            return

        # Suppress re-entrant tick processing when an order is already in flight.
        if self._order_lock.locked():
            print(f"⏳ Order in flight — skipping check_logic tick")
            return

        # ── IDLE: look for first qualifying side ─────────────────────────

        # Evaluate YES first.
        if y >= MIN_ENTRY_PRICE:
            if is_locked_price(y):
                print(f"🔒 [SKIP] YES @ {y_c}c is a locked price (1c or 100c) — skipping entry.")
                self.add_log(f"🔒 YES locked @ {y_c}c — skip")
            else:
                print(f"[ENTRY] YES @ {y_c}c >= {round(MIN_ENTRY_PRICE*100)}c threshold — buying YES")
                self.log_to_file(f"[ENTRY] YES @ {y_c}c >= threshold {round(MIN_ENTRY_PRICE*100)}c")
                await self.execute_order("YES", y, Side.BUY)
                return  # re-evaluate next tick after state update

        # YES did not qualify — evaluate NO.
        if n >= MIN_ENTRY_PRICE:
            if is_locked_price(n):
                print(f"🔒 [SKIP] NO @ {n_c}c is a locked price (1c or 100c) — skipping entry.")
                self.add_log(f"🔒 NO locked @ {n_c}c — skip")
            else:
                print(f"[ENTRY] NO @ {n_c}c >= {round(MIN_ENTRY_PRICE*100)}c threshold — buying NO")
                self.log_to_file(f"[ENTRY] NO @ {n_c}c >= threshold {round(MIN_ENTRY_PRICE*100)}c")
                await self.execute_order("NO", n, Side.BUY)
                return

        # Neither side qualifies yet.
        print(f"⏳ [IDLE] YES={y_c}c | NO={n_c}c | "
              f"need >={round(MIN_ENTRY_PRICE*100)}c | "
              f"Binance: {self.binance.signal_label if self.binance else 'N/A'}")

    async def _check_single_side_exit(self, side: Optional[str]) -> bool:
        """
        Evaluate take-profit / stop-loss on the single open position.
        Returns True if an exit was triggered (caller should return immediately).
        """
        if not side:
            return False
        entry_price   = self.entry_price
        current_price = self.prices.get(side, 0.0)
        if entry_price <= 0 or current_price <= 0:
            return False

        loss_pct = (entry_price - current_price) / entry_price
        if loss_pct >= STOP_LOSS_PCT / 100:
            print(f"\n\n⚠️ STOP LOSS TRIGGERED: {side} -{loss_pct*100:.1f}%")
            await self.market_exit(loss_pct, is_profit=False)
            return True

        return False

    # ── Order execution ────────────────────────────────────────────────

    async def execute_order(self, side: str, price: float, direction: Side):
        """
        Entry point for placing a single order.

        Guards (checked in order):
          1. active_market must be set.
          2. _order_lock prevents re-entrant concurrent orders.
          3. MIN_ENTRY_PRICE gate (BUY only): price must be >= 0.90.
          4. Locked-price guard (BUY only): second independent check against
             rapid price movement between check_logic and order placement.
          5. MAX_BUY_PRICE ceiling (BUY only): price must not exceed 0.99.
          6. Duplicate-entry guard: trade_state must be IDLE for BUY.

        On success, confirmed_execute() calls update_state() with the actual
        submitted price and confirmed fill size.
        """
        if not self.active_market:
            return
        if self._order_lock.locked():
            print(f"⏳ [LOCK] Order in flight — skipping {direction} {side}")
            return

        if direction == Side.BUY:
            # Prevent entering if a position is already open.
            if self.trade_state != TradeState.IDLE:
                print(f"⚠️ [ABORT] Trade state is {self.trade_state.name} — entry suppressed.")
                return
            if price < MIN_ENTRY_PRICE:
                print(f"⚠️ [ABORT] Price {round(price*100)}c is below MIN_ENTRY_PRICE "
                      f"({round(MIN_ENTRY_PRICE*100)}c) — skipping BUY")
                exec_log("execute_order_below_min_entry", side=side, price=price,
                         min_entry=MIN_ENTRY_PRICE)
                return
            if is_locked_price(price):
                print(f"🔒 [ABORT] Price {round(price*100)}c is a locked price — refusing BUY.")
                exec_log("execute_order_locked_price", side=side, price=price)
                return
            if price > MAX_BUY_PRICE:
                print(f"⚠️ [ABORT] Price {round(price*100)}c exceeds MAX_BUY_PRICE "
                      f"({round(MAX_BUY_PRICE*100)}c) — skipping")
                return

        async with self._order_lock:
            t_id = (self.active_market["yes_id"] if side == "YES"
                    else self.active_market["no_id"])

            if direction == Side.BUY:
                calculated_size = POSITION_SIZE
            else:
                print(f"🔍 [CHAIN] Verifying shares for {side} (ID: {t_id})...")
                actual_balance = await self.account.get_onchain_share_balance_async(t_id)
                if actual_balance <= 0:
                    reason = "RPC Error" if actual_balance < 0 else "0 Shares Found"
                    print(f"🛑 [ABORT] Cannot Sell: {reason}. Check wallet.")
                    return
                calculated_size = actual_balance
                if calculated_size < 0.05:
                    print(f"⚠️ [DUST TRAP] Balance {calculated_size:.4f} is dust — skipping sell")
                    return

            if DRY_MODE:
                print(f"\n🧪 [DRY] {direction} {calculated_size} {side} @ {round(price*100)}c")
                self.update_state(side, price, direction, calculated_size)
                return

            success, actual_size = await self.confirmed_execute(
                side, price, direction, timeout_sec=FILL_TIMEOUT_SEC
            )
            if success:
                print(f"✅ execute_order complete: {direction} {side} "
                      f"actual_size={actual_size:.4f} @ {round(self.entry_price*100)}c")
            else:
                print(f"❌ execute_order: no confirmed fill for {direction} {side}")
                exec_log("execute_order_no_fill", side=side,
                         direction=str(direction), price=price)
                self.trade_state = TradeState.ERROR

    def update_state(self, side: str, price: float, direction: Side, size: float):
        """
        Update position tracking after a confirmed fill.
        price is the actual submitted price — never the pre-slippage requested price.
        """
        if direction == Side.BUY:
            self.entry_timestamp = t.time()
            self.position_side   = side
            self.entry_price     = price
            self.position_size   = size
            self.trade_state     = TradeState.FILLED
            self.dashboard["status"] = f"{side} FILLED"
            self.log_trade(side, price, "BUY", size=size)
            print(f"[ENTRY] Bought {side} @ {round(price*100)}c | "
                  f"Size: {size:.4f} | State → FILLED")
        else:
            # SELL — clear the position, return to IDLE.
            self.log_trade(side, price, "SELL_EXIT", size=size)
            self.position_side   = None
            self.position_size   = 0.0
            self.entry_price     = 0.0
            self.entry_timestamp = None
            self.trade_state     = TradeState.IDLE

    # ── Exit logic ─────────────────────────────────────────────────────

    async def market_exit(self, pct: float, is_profit: bool = False):
        """
        Take-profit or stop-loss exit on the single open position.
        Sells only self.position_side — the one held leg.
        """
        self.exited      = True
        self.trade_state = TradeState.EXITING
        label = "TAKE PROFIT" if is_profit else "STOP LOSS"
        emoji = "💰" if is_profit else "🛑"
        color = GREEN if is_profit else RED
        print(f"\n{BOLD}{color}{emoji} Executing {label} Exit...{RESET}")
        exec_log("exit_start", label=label, pct=pct)

        side           = self.position_side
        total_spent    = 0.0
        total_received = 0.0
        exit_price     = 0.0

        if side and self.position_size > 0:
            spent        = self.entry_price * self.position_size
            total_spent  = spent
            exit_price   = max(0.01, self.prices.get(side, self.entry_price) - 0.02)
            price_cents  = f"{round(exit_price * 100)}c"
            print(f"📡 Attempting confirmed SELL {self.position_size:.2f} {side} @ {price_cents}...")
            position_size = self.position_size
            if DRY_MODE:
                actual_size     = position_size
                received        = actual_size * exit_price
                total_received  = received
                print(f"  → 🧪 DRY SELL {actual_size:.2f} @ {price_cents}")
            else:
                success, sold_size = await self.confirmed_execute(
                    side, exit_price, Side.SELL, timeout_sec=20.0)
                actual_size     = sold_size if (success and sold_size > 0) else position_size
                received        = actual_size * exit_price
                total_received  = received

        net_payout   = total_received - total_spent
        result_color = GREEN if net_payout > 0 else RED
        print(f"\n{BOLD}{color}--- {label} SUMMARY ---{RESET}")
        print(f"📉 Total Invested: ${total_spent:.3f}")
        print(f"💵 Total Received: ${total_received:.3f}")
        print(f"📊 Net Payout:     {result_color}${net_payout:.3f} ({pct*100:.1f}%){RESET}")
        self.log_to_file(f"{emoji} {label} EXIT | Spent: ${total_spent:.2f} | "
                         f"Recv: ${total_received:.2f} | Net: ${net_payout:.2f}")
        exec_log("exit_complete", label=label, total_spent=total_spent,
                 total_received=total_received, net_payout=net_payout)

        outcome_type = "TAKE_PROFIT" if is_profit else "STOP_LOSS"
        entry_val    = self.entry_price
        actual_pct   = round(((exit_price - entry_val) / entry_val) * 100, 1) if entry_val > 0 else 0.0
        pct_key      = "profit_pct" if is_profit else "loss_pct"
        details = {
            "side":        side,
            "entry_price": entry_val,
            "exit_price":  exit_price,
            pct_key:       abs(actual_pct),
            "duration_seconds": round(t.time() - (self.entry_timestamp or t.time()), 2),
        }
        self.log_pnl(outcome_type, net_payout, details)
        self.trade_state = TradeState.CLOSED

        if self.active_market:
            slug = self.active_market.get("slug") or self.market_slug
            if slug:
                self.processed_markets.add(slug)
                self.market_exit_reasons[slug] = outcome_type
                print(f"🚫 MARKET BLACKLISTED: {slug} (after {label})")

        print(f"\n{BOLD}{GREEN}♻️  Trade cycle completed. Resetting state for next market...{RESET}")
        self.exited = False
        await self.reset_state()
        await asyncio.sleep(4.0)

    # ── Confirmed order execution ──────────────────────────────────────

    async def confirmed_execute(
        self,
        side: str,
        price: float,
        direction: Side,
        timeout_sec: float = 15.0,
    ) -> Tuple[bool, float]:
        """
        Place a limit order via the shared (account-level) V2 CLOB client and
        wait for fill confirmation.

        Confirmation uses two independent signals:
          1. CLOB order status endpoint (filled / matched / partially_filled)
          2. On-chain share balance delta (ground truth, MIN_FILL_DELTA threshold)

        update_state() is only called after a confirmed fill — never optimistically.
        The actual submitted price (clean_price, with slippage bump) is passed to
        update_state(), not the pre-slippage requested price.
        Stale or unmatched orders are cancelled on timeout.

        All order placement / status / cancel calls go through self.account
        (the single shared AccountService), never through a per-worker client.
        """
        if not self.active_market:
            print("❌ No active market for confirmed_execute")
            return False, 0.0

        token_id = (self.active_market["yes_id"] if side == "YES"
                    else self.active_market["no_id"])

        if direction == Side.BUY:
            requested_size = POSITION_SIZE
        else:
            print(f"🔍 [CHAIN] Verifying shares for {side} sell (ID: {token_id})...")
            current_balance = await self.account.get_onchain_share_balance_async(token_id)
            if current_balance <= 0:
                print(f"🛑 Cannot sell: balance {current_balance}")
                return False, 0.0
            requested_size = current_balance
            if requested_size < 0.05:
                print(f"⚠️ Dust trap: only {requested_size:.4f} — skipping sell")
                return False, 0.0

        if DRY_MODE:
            print(f"\n🧪 [DRY CONFIRMED] {direction} {requested_size:.2f} {side} @ {round(price*100)}c")
            self.update_state(side, price, direction, requested_size)
            return True, requested_size

        # Slippage bump: buy slightly higher, sell slightly lower.
        if direction == Side.BUY:
            clean_price = round(price + 0.01, 2)
        else:
            clean_price = round(price - 0.01, 2)
        clean_price = max(0.01, min(0.99, clean_price))

        order_id    = None
        order_state = OrderState.CREATED

        try:
            side_str = "BUY" if direction == Side.BUY else "SELL"
            resp     = self.account.create_and_post_order(
                side_str, clean_price, requested_size, token_id)
            order_state = OrderState.SUBMITTED

            exec_log("order_submit",
                     side=side, direction=side_str,
                     requested_price=price, submitted_price=clean_price,
                     size=requested_size, token_id=token_id)

            if isinstance(resp, dict):
                order_id = resp.get('orderID') or resp.get('order_id')
            elif isinstance(resp, str):
                try:
                    parsed   = json.loads(resp)
                    order_id = parsed.get('orderID') or parsed.get('order_id')
                except Exception:
                    pass

            exec_log("order_accepted",
                     side=side, direction=side_str,
                     submitted_price=clean_price, size=requested_size,
                     order_id=order_id, raw_resp=str(resp)[:200])

            print(f"\n🚀 [LIVE] {direction} {requested_size:.2f} {side} "
                  f"@ {round(clean_price*100)}c | OrderID: {order_id or 'unknown'}")

        except Exception as e:
            exec_log("order_failed", side=side, error=str(e))
            print(f"❌ Order placement failed: {e}")
            return False, 0.0

        start_time      = t.time()
        initial_balance = await self.account.get_onchain_share_balance_async(token_id)
        print(f"⏳ Confirming {direction} ({timeout_sec}s timeout) | "
              f"Initial balance: {initial_balance:.4f}")

        while t.time() - start_time < timeout_sec:
            await asyncio.sleep(2.5)
            confirmed      = False
            confirmed_size = 0.0

            # Check 1: CLOB order status
            if order_id:
                try:
                    order_info = self.account.get_order_status(order_id)
                    if isinstance(order_info, str):
                        try:
                            order_info = json.loads(order_info)
                        except json.JSONDecodeError:
                            order_info = {}
                    if isinstance(order_info, dict):
                        clob_status  = str(order_info.get('status', '')).lower()
                        size_matched = float(order_info.get('size_matched', 0) or 0)
                        size_remain  = float(order_info.get('size_remaining', requested_size) or requested_size)

                        exec_log("order_status_check",
                                 order_id=order_id, clob_status=clob_status,
                                 size_matched=size_matched, size_remaining=size_remain,
                                 elapsed=round(t.time() - start_time, 1))

                        if clob_status in ['filled', 'matched', 'closed']:
                            confirmed_size = size_matched if size_matched > 0 else requested_size
                            confirmed      = True
                            order_state    = OrderState.FILLED
                            print(f"✅ CLOB confirms {direction} FILLED: {confirmed_size:.4f}")
                        elif clob_status in ['partially_filled', 'partial']:
                            confirmed_size = size_matched
                            if size_matched >= MIN_FILL_DELTA:
                                confirmed   = True
                                order_state = OrderState.PARTIALLY_FILLED
                                print(f"⚠️ CLOB partial fill: {size_matched:.4f} / {requested_size:.4f}")
                        elif clob_status in ['cancelled', 'rejected']:
                            order_state = (OrderState.CANCELLED if clob_status == 'cancelled'
                                           else OrderState.REJECTED)
                            exec_log("order_terminal", order_id=order_id, status=clob_status)
                            print(f"🚫 Order {clob_status}: {order_id}")
                            return False, 0.0

                except Exception as e:
                    print(f"CLOB status check failed: {e} — falling back to on-chain")
                    exec_log("clob_check_error", order_id=order_id, error=str(e))

            # Check 2: on-chain balance delta (ground truth)
            current_balance = await self.account.get_onchain_share_balance_async(token_id)
            if direction == Side.BUY:
                delta = current_balance - initial_balance
                if delta >= MIN_FILL_DELTA:
                    print(f"✅ On-chain BUY confirmed: +{delta:.4f} (now {current_balance:.4f})")
                    exec_log("onchain_fill_confirmed",
                             direction="BUY", delta=delta,
                             initial=initial_balance, current=current_balance,
                             order_id=order_id)
                    if not confirmed:
                        confirmed_size = delta
                    confirmed = True
                    if delta > confirmed_size:
                        confirmed_size = delta
            else:
                sold = initial_balance - current_balance
                if sold >= MIN_FILL_DELTA:
                    print(f"✅ On-chain SELL confirmed: sold {sold:.4f} (remaining {current_balance:.4f})")
                    exec_log("onchain_fill_confirmed",
                             direction="SELL", sold=sold,
                             initial=initial_balance, current=current_balance,
                             order_id=order_id)
                    if not confirmed:
                        confirmed_size = sold
                    confirmed = True
                    if sold > confirmed_size:
                        confirmed_size = sold

            if confirmed:
                self.update_state(side, clean_price, direction, confirmed_size)
                exec_log("state_updated",
                         side=side, direction=str(direction),
                         actual_price=clean_price, actual_size=confirmed_size,
                         order_id=order_id)
                return True, confirmed_size

            print(f"  Still waiting... balance now {current_balance:.4f}")

        # ── Timeout ──────────────────────────────────────────────────────
        final_balance     = await self.account.get_onchain_share_balance_async(token_id)
        delta_at_timeout  = ((final_balance - initial_balance) if direction == Side.BUY
                             else (initial_balance - final_balance))

        exec_log("order_timeout",
                 order_id=order_id, timeout_sec=timeout_sec,
                 initial_balance=initial_balance, final_balance=final_balance,
                 delta_at_timeout=delta_at_timeout)

        print(f"⌛ Confirmation timeout | Final balance: {final_balance:.4f} "
              f"(started {initial_balance:.4f}) | delta={delta_at_timeout:.4f}")

        if delta_at_timeout >= MIN_FILL_DELTA:
            print(f"⚠️ Recording partial fill at timeout: "
                  f"{delta_at_timeout:.4f} @ {round(clean_price*100)}c")
            self.update_state(side, clean_price, direction, delta_at_timeout)
            exec_log("partial_fill_at_timeout",
                     side=side, delta=delta_at_timeout,
                     price=clean_price, order_id=order_id)
            if order_id:
                self._try_cancel_order(order_id)
            return True, delta_at_timeout

        if order_id:
            self._try_cancel_order(order_id)
        return False, 0.0

    def _try_cancel_order(self, order_id: str):
        try:
            result = self.account.cancel_order(order_id)
            print(f"🚫 Cancel sent for {order_id}: {result}")
            exec_log("cancel_sent", order_id=order_id, result=str(result)[:100])
        except Exception as e:
            print(f"⚠️ Cancel failed for {order_id}: {e}")
            exec_log("cancel_failed", order_id=order_id, error=str(e))

    # ── On-chain balance helpers (thin pass-throughs to AccountService) ────

    async def get_onchain_share_balance_async(self, token_id: str, retries: int = 3) -> float:
        return await self.account.get_onchain_share_balance_async(token_id, retries=retries)

    def get_onchain_share_balance(self, token_id: str, retries: int = 3) -> float:
        return self.account.get_onchain_share_balance(token_id, retries=retries)

    async def merge_shares(self, amount_to_merge: float):
        """Merge YES+NO shares back into pUSD on-chain for this worker's active market."""
        return await self.account.merge_shares(self.active_market, amount_to_merge)

    # ── State reset ────────────────────────────────────────────────────

    async def reset_state(self):
        self.trade_state     = TradeState.IDLE
        self.position_side   = None
        self.position_size   = 0.0
        self.entry_price     = 0.0
        self.entry_timestamp = None
        self.market_start_time = None
        self.price_history.clear()
        self.market_outcome    = None
        self.final_yes_price   = 0.0
        self.final_no_price    = 0.0
        self.exited            = False
        self.market_slug       = None
        self.prices            = {"YES": 0.0, "NO": 0.0}
        self.dashboard["yes"]               = 0
        self.dashboard["no"]                = 0
        self.dashboard["timer"]             = "--:--"
        self.dashboard["listener"]          = "--:--"
        self.dashboard["outcome"]           = "PENDING"
        self.dashboard["bought_side"]       = "-"
        self.dashboard["entry_price"]       = 0.0
        self.dashboard["status"]            = "WAITING"
        self.dashboard["profit"]            = 0.0
        self.dashboard["imbalance_ratio"]   = 0.0
        self.dashboard["imbalance_momentum"] = 0.0
        self.recent_logs.clear()
        self.update_dashboard()
        print("\n♻️ Full state reset after trade/exit. Ready for next market.")

    # ── PnL logging ────────────────────────────────────────────────────

    def log_pnl(self, outcome_type: str, pnl_amount: float, details: dict):
        file_path       = f"{self.asset_type}_pnl_history.json"
        slug            = ((self.active_market.get("slug") if self.active_market else None)
                           or self.market_slug or "unknown")
        market_question = ((self.active_market.get("question", "Unknown")
                            if self.active_market else "Unknown Market"))

        # ── Validation: reject corrupted PnL values outright ─────────────
        # A NaN/Inf/None pnl_amount (e.g. from a divide-by-zero upstream, or
        # a malformed fill response) must never be allowed to corrupt
        # cumulative_pnl or the persisted history — once a bad value is
        # added in, every later point is wrong forever.
        if not _is_finite_number(pnl_amount):
            print(f"❌ [{self.asset_type.upper()}] log_pnl rejected non-finite pnl_amount "
                  f"({pnl_amount!r}) for market {slug} — trade NOT recorded.")
            return

        # ── Per-round dedup guard ──────────────────────────────────────────
        # "Only one valid snapshot per market round": if this exact slug was
        # already logged (e.g. a retried exit path firing twice), skip the
        # second call entirely rather than double-counting the trade.
        round_key = f"{self.asset_type}:{slug}"
        if slug != "unknown" and slug == self._last_logged_slug:
            print(f"ℹ️ [{self.asset_type.upper()}] log_pnl skipped duplicate call for "
                  f"already-logged market {slug}.")
            return

        self.cumulative_pnl = round(self.cumulative_pnl + pnl_amount, 4)
        if pnl_amount > 0:
            self.wins   += 1
        else:
            self.losses += 1
        self.trade_count = self.wins + self.losses
        total_trades     = self.trade_count
        win_rate_str     = (f"{round((self.wins / total_trades) * 100, 2)}%"
                            if total_trades > 0 else "0%")

        duration = round(t.time() - (self.entry_timestamp or t.time()), 2)
        entry = {
            "timestamp":      datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "market":         market_question,
            "slug":           slug,
            "type":           outcome_type,
            "pnl":            round(pnl_amount, 4),
            "cumulative_pnl": self.cumulative_pnl,
            "details":        {**details, "duration_seconds": duration},
        }

        self._save_stats_to_redis()
        self._append_trade_to_redis(entry)
        self._last_logged_slug = slug

        # ── Live portfolio history snapshot ──────────────────────────────
        # Push a portfolio-total point to emiliano:portfolio:history right
        # now so the chart updates the moment this trade closes, without
        # waiting for any background flush interval.
        #
        # IMPORTANT (this was the root cause of the chart spikes): this MUST
        # be the TOTAL PnL across every asset, never self.cumulative_pnl for
        # this asset alone. Using a single asset's own PnL here produced a
        # sharp spike/drop on every trade close, because that value would be
        # written right next to genuinely correct cross-asset totals from
        # the 60-s background loop and the startup backfill.
        portfolio_history_snapshot(_portfolio_total_pnl(), round_key=round_key)

        default_data = {"total_pnl": 0.0, "wins": 0, "losses": 0,
                        "win_rate": "0%", "trades": []}
        try:
            if os.path.exists(file_path):
                with open(file_path, 'r') as f:
                    data = json.load(f)
            else:
                data = default_data
        except Exception:
            data = default_data

        data["total_pnl"] = self.cumulative_pnl
        data["wins"]      = self.wins
        data["losses"]    = self.losses
        data["win_rate"]  = win_rate_str
        data["trades"].append(entry)

        try:
            temp_file = file_path + ".tmp"
            with open(temp_file, 'w') as f:
                json.dump(data, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, file_path)
        except Exception as e:
            print(f"⚠️ Local JSON write failed (non-fatal, Redis has the data): {e}")

    # ── Dashboard data ─────────────────────────────────────────────────

    def get_dashboard_data(self) -> dict:
        pnl_dollars, pnl_pct, _ = self.get_current_pnl()

        state_labels = {
            TradeState.IDLE:    self.dashboard.get("status", "WAITING"),
            TradeState.FILLED:  f"{self.position_side} FILLED",
            TradeState.EXITING: "EXITING",
            TradeState.CLOSED:  "CLOSED",
            TradeState.ERROR:   "ERROR",
        }
        status = state_labels.get(self.trade_state, "WAITING")

        if self.position_side and self.entry_price > 0:
            entry         = round(self.entry_price * 100, 1)
            sz            = round(self.position_size, 2)
            position_text = f"{self.position_side} @ {entry}c ×{sz}"
        else:
            position_text = "-"

        market_start_iso = None
        market_end_iso   = None
        if self.active_market and self.active_market.get("expiry"):
            expiry           = self.active_market["expiry"]
            start            = expiry - timedelta(minutes=5)
            market_end_iso   = expiry.isoformat()
            market_start_iso = start.isoformat()

        return {
            "asset":              self.asset_type.upper(),
            "yes":                round(self.prices.get("YES", 0) * 100),
            "no":                 round(self.prices.get("NO",  0) * 100),
            "timer":              self.dashboard.get("timer",    "--:--"),
            "listener":           self.get_listener_countdown(),
            "status":             status,
            "position":           position_text,
            "outcome":            self.dashboard.get("outcome", "PENDING"),
            "trade_state":        self.trade_state.name,
            "imbalance_ratio":    round(self.dashboard.get("imbalance_ratio",    0.0), 3),
            "imbalance_momentum": round(self.dashboard.get("imbalance_momentum", 0.0), 3),
            "imbalance_signal":   self.binance.signal_label if self.binance else "NO SIGNAL",
            "pnl_dollars":        round(pnl_dollars,         2),
            "pnl_pct":            round(pnl_pct,             2),
            "cumulative_pnl":     round(self.cumulative_pnl, 2),
            "wins":               self.wins,
            "losses":             self.losses,
            "trade_count":        self.trade_count,
            "win_rate":           (round((self.wins / self.trade_count) * 100, 1)
                                   if self.trade_count > 0 else 0.0),
            "market_start_iso":   market_start_iso,
            "market_end_iso":     market_end_iso,
            "yes_threshold":      round(MIN_ENTRY_PRICE * 100),
            "no_threshold":       round(MIN_ENTRY_PRICE * 100),
            "locked_low_c":       round(LOCKED_LOW  * 100),
            "locked_high_c":      round(LOCKED_HIGH * 100),
        }

    def print_final_summary(self):
        self.final_yes_price = self.prices.get("YES", 0.0)
        self.final_no_price  = self.prices.get("NO",  0.0)
        if self.final_yes_price > FINAL_PRICE:
            self.market_outcome = "YES"
        elif self.final_no_price > FINAL_PRICE:
            self.market_outcome = "NO"
        else:
            self.market_outcome = "UNKNOWN"
        self.dashboard["outcome"] = self.market_outcome

        if not self.position_side or self.position_size <= 0:
            print(f"\n{BOLD}{YELLOW}ℹ️  No position taken this market.{RESET}")
            self.account.merge_now()
            return

        total_spent   = self.entry_price * self.position_size
        actual_profit = (self.position_size - total_spent
                         if self.market_outcome == self.position_side else -total_spent)
        price_in_cents = lambda p: f"{round(p * 100)}c"
        print(f"\n\n{BOLD}{GREEN}--- 🚀 SINGLE SIDE RESULT ---{RESET}")
        print(f"⏱️ Market Outcome:      {self.market_outcome}")
        print(f"💰 Bought Side:         {self.position_side} @ "
              f"{price_in_cents(self.entry_price)}")
        print(f"📊 Shares Bought:       {self.position_size:.4f}")
        print(f"📉 Total Invested:      ${total_spent:.4f}")
        print(f"📈 Net Profit:          {GREEN if actual_profit >= 0 else RED}"
              f"${actual_profit:.4f}{RESET}")
        self.log_pnl("HODL", actual_profit, {
            "bought_side":    self.position_side,
            "entry_price":    round(self.entry_price * 100),
            "shares":         self.position_size,
            "outcome":        self.market_outcome,
            "duration_seconds": round(t.time() - (self.entry_timestamp or t.time()), 2),
        })

        global completed_markets
        completed_markets += 1
        if completed_markets >= TOTAL_BOTS:
            self.account.merge_now(send_telegram_notify=True)
            completed_markets = 0
        else:
            self.account.merge_now(send_telegram_notify=False)

    # ── Market scanner ─────────────────────────────────────────────────

    def get_candidate_markets(self, asset="eth") -> list:
        now       = int(datetime.now(timezone.utc).timestamp())
        intervals = [(now // 300) * 300, (now // 300) * 300 + 300]
        markets   = []
        for ts in intervals:
            slug = f"{asset}-updown-5m-{ts}"
            try:
                resp = requests.get(f"{GAMMA_URL}?slug={slug}").json()
                if resp:
                    markets.append({
                        "slug":     slug,
                        "url":      f"https://polymarket.com/event/{resp[0]['slug']}",
                        "start_ts": ts,
                    })
            except Exception:
                pass
        return markets

    def pick_next_market(self, markets: list) -> Optional[dict]:
        now_ts  = int(datetime.now(timezone.utc).timestamp())
        current = [m for m in markets if m["start_ts"] <= now_ts < m["start_ts"] + 300]
        if current:
            return current[0]
        future = [m for m in markets if m["start_ts"] > now_ts]
        if not future:
            return None
        return min(future, key=lambda x: x["start_ts"])

    async def start(self):
        """
        Per-asset trading loop. The wallet audit is NOT run here — it already
        ran exactly once, account-wide, in main() before any MarketWorker was
        started. This method only scans for and trades this worker's market.
        """
        print(f"🤖 EmilianoBot (V2) — 90-Cent Single-Leg Entry → "
              f"Monitoring {self.asset_type} 5m markets...")
        print(f"  Entry threshold : ≥{round(MIN_ENTRY_PRICE*100)}c | "
              f"Locked-price skip: {round(LOCKED_LOW*100)}c & {round(LOCKED_HIGH*100)}c | "
              f"Size: {POSITION_SIZE}")
        session = requests.Session()
        while True:
            try:
                now_ts     = int(datetime.now(timezone.utc).timestamp())
                intervals  = [(now_ts // 300) * 300, (now_ts // 300) * 300 + 300]
                candidates = []
                for ts in intervals:
                    slug = f"{self.asset_type}-updown-5m-{ts}"
                    try:
                        resp = session.get(f"{GAMMA_URL}?slug={slug}", timeout=2).json()
                        if resp:
                            candidates.append({
                                "url":      f"https://polymarket.com/event/{resp[0]['slug']}",
                                "start_ts": ts,
                                "slug":     slug,
                            })
                    except Exception:
                        continue

                current = [m for m in candidates if m["start_ts"] <= now_ts < m["start_ts"] + 300]
                if current:
                    target = current[0]
                else:
                    future = [m for m in candidates if m["start_ts"] > now_ts]
                    if not future:
                        print("⏳ No market → retrying...")
                        await asyncio.sleep(2)
                        continue
                    target = min(future, key=lambda x: x["start_ts"])

                print(f"\n🎯 Target: {target['slug']}")
                if not self.fetch_target_market(target["url"]):
                    print("❌ Market fetch failed → retry loop")
                    await asyncio.sleep(1)
                    continue

                await self.reset_state()
                now_ts = int(datetime.now(timezone.utc).timestamp())
                start  = target["start_ts"]
                if start <= now_ts < start + 300:
                    print("⚡ Already inside active market → starting immediately")

                print("📡 Starting price listener...")
                await self.price_listener()
                print("🏁 Market complete → instant rescan")

            except Exception as e:
                print(f"❌ Scheduler Error: {e}")
                await asyncio.sleep(2)


# Backward-compatible alias: existing external code/imports that reference
# `EmilianoBot` (e.g. main.py) keep working without modification.
EmilianoBot = MarketWorker

# ═════════════════════════════════════════════════════════════════════════════
# TERMINAL DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════

def create_dashboard(bots):
    layout = Layout()
    layout.split_column(
        Layout(
            Panel(
                "[bold cyan]EMILIANO BOT — 90-Cent Single-Leg Entry | "
                "Binance WS Signal (Display)[/bold cyan]",
                style="bold green", box=box.ROUNDED,
            ),
            size=3,
        ),
        Layout(name="main"),
    )
    layout["main"].split_row(
        Layout(name="col1", ratio=1),
        Layout(name="col2", ratio=1),
    )

    for i, bot in enumerate(bots):
        d           = bot.dashboard
        pnl_dollars, pnl_pct, pnl_color = bot.get_current_pnl()
        listener_cd = bot.get_listener_countdown()

        # Status label
        if bot.trade_state == TradeState.FILLED and bot.position_side:
            display_status = f"{bot.position_side} FILLED"
        elif bot.trade_state == TradeState.EXITING:
            display_status = "EXITING"
        elif bot.trade_state == TradeState.CLOSED:
            display_status = "CLOSED"
        elif bot.trade_state == TradeState.ERROR:
            display_status = "ERROR"
        else:
            display_status = d.get('status', 'WAITING')

        # Binance signal display
        ratio    = d.get('imbalance_ratio',    0.0)
        momentum = d.get('imbalance_momentum', 0.0)
        if ratio > 0.22 or (ratio > 0.15 and momentum > 0.07):
            ratio_text = f"[bold green]↑ {ratio:+.3f} STRONG BULLISH[/bold green]"
        elif ratio > 0.12:
            ratio_text = f"[green]↑ {ratio:+.3f} Bullish[/green]"
        elif ratio < -0.22 or (ratio < -0.15 and momentum < -0.07):
            ratio_text = f"[bold red]↓ {ratio:+.3f} STRONG BEARISH[/bold red]"
        elif ratio < -0.12:
            ratio_text = f"[red]↓ {ratio:+.3f} Bearish[/red]"
        else:
            ratio_text = f"[white]{ratio:+.3f} Neutral[/white]"

        momentum_text = ""
        if abs(momentum) > 0.04:
            mom_color     = "green" if momentum > 0 else "red"
            momentum_text = f" | Mom: [{mom_color}]{momentum:+.3f}[/{mom_color}]"

        # Card border color based on trade state
        if bot.trade_state == TradeState.FILLED:
            card_color = "cyan"
        elif bot.trade_state in (TradeState.EXITING, TradeState.CLOSED):
            card_color = "green"
        else:
            card_color = "blue"

        # Time window label
        if bot.active_market and bot.active_market.get("expiry"):
            expiry_utc  = bot.active_market["expiry"]
            et_zone     = ZoneInfo("America/New_York")
            expiry_et   = expiry_utc.astimezone(et_zone)
            start_et    = expiry_et - timedelta(minutes=5)
            time_window = (f"{start_et.strftime('%b %d')}, "
                           f"{start_et.strftime('%I:%M%p')}-{expiry_et.strftime('%I:%M%p')} ET")
        else:
            time_window = "Waiting for market..."

        # Position text
        if bot.position_side and bot.entry_price > 0:
            bought_text = (
                f"[b]{bot.position_side} filled:[/] {bot.entry_price*100:.1f}c "
                f"×{bot.position_size:.2f}"
            )
        else:
            bought_text = (
                f"[dim]Entry: ≥{round(MIN_ENTRY_PRICE*100)}c | "
                f"Skip locked: {round(LOCKED_LOW*100)}c & {round(LOCKED_HIGH*100)}c[/dim]"
            )

        card = Panel(
            Text.from_markup(
                f"""[yellow]YES:[/] {d.get('yes', 0):>3}c    [yellow]NO:[/] {d.get('no', 0):>3}c
[cyan]Timer:[/] {d.get('timer', '--:--')}
[cyan]Listener:[/] {listener_cd}
[magenta]Status:[/] {display_status}
{bought_text}
[bold]ROI:[/] [{pnl_color}]+${pnl_dollars:.2f} ({pnl_pct:+.2f}%)[/{pnl_color}]
[bold]Binance Imb:[/] {ratio_text} {momentum_text}
[bold]Outcome:[/] [bold {'green' if d.get('outcome') == 'YES' else 'red' if d.get('outcome') == 'NO' else 'white'}]{d.get('outcome', 'PENDING')}[/]"""
            ),
            title=f"{d.get('asset', 'UNKNOWN')} · {time_window}",
            border_style=card_color, box=box.HEAVY, padding=(1, 2),
        )
        if i == 0:
            layout["main"]["col1"].update(card)
        elif i == 1:
            layout["main"]["col2"].update(card)
        elif i == 2:
            if layout["main"]["col1"].renderable is None:
                layout["main"]["col1"].update(card)
            else:
                layout["main"]["col1"].split_column(
                    layout["main"]["col1"].renderable, Layout(card, ratio=1))
        elif i == 3:
            if layout["main"]["col2"].renderable is None:
                layout["main"]["col2"].update(card)
            else:
                layout["main"]["col2"].split_column(
                    layout["main"]["col2"].renderable, Layout(card, ratio=1))
    return layout


async def dashboard_loop(bots):
    with Live(create_dashboard(bots), console=console,
              refresh_per_second=2, screen=True) as live:
        while True:
            try:
                live.update(create_dashboard(bots))
            except Exception as e:
                console.print(f"[red]Dashboard error: {e}[/red]")
            await asyncio.sleep(0.8)


# ═════════════════════════════════════════════════════════════════════════════
# PNL MERGE + TELEGRAM
# ═════════════════════════════════════════════════════════════════════════════

PNL_FILES              = ["btc_pnl_history.json", "eth_pnl_history.json",
                           "sol_pnl_history.json", "xrp_pnl_history.json"]
OUTPUT_FILE            = "bot_pnl.json"
MERGE_INTERVAL_SECONDS = 300
TELEGRAM_BOT_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID")

completed_markets      = 0
TOTAL_BOTS             = 4
last_notification_time = 0


def send_telegram(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ Telegram credentials not set.")
        return
    try:
        url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "parse_mode": "HTML", "text": message}
        requests.post(url, json=payload, timeout=10)
        print("📨 Telegram notification sent.")
    except Exception as e:
        print(f"❌ Failed to send Telegram: {e}")


def get_pnl_emoji(pnl: float) -> str:
    return "🟢" if pnl >= 0 else "🔴"


def merge_all_pnl(send_telegram_notify: bool = False):
    global completed_markets, last_notification_time
    all_trades: List[Dict[str, Any]] = []
    total_pnl    = 0.0
    total_wins   = 0
    total_losses = 0
    asset_stats: Dict[str, Dict] = {}
    print(f"\n🔄 [{datetime.now().strftime('%H:%M:%S')}] Merging PNL files...")
    for file_path in PNL_FILES:
        if not os.path.exists(file_path):
            continue
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data   = json.load(f)
            trades     = data.get("trades", [])
            file_pnl   = data.get("total_pnl", 0.0)
            wins       = data.get("wins", 0)
            losses     = data.get("losses", 0)
            asset_name = file_path.replace("_pnl_history.json", "").upper()
            asset_stats[asset_name] = {
                "wins":         wins,
                "losses":       losses,
                "total_trades": wins + losses,
                "pnl":          file_pnl,
                "win_rate":     (round((wins / (wins + losses) * 100), 2)
                                 if (wins + losses) > 0 else 0.0),
            }
            all_trades.extend(trades)
            total_pnl    += file_pnl
            total_wins   += wins
            total_losses += losses
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")

    total_trades     = total_wins + total_losses
    overall_win_rate = (round((total_wins / total_trades) * 100, 2)
                        if total_trades > 0 else 0.0)
    combined_data = {
        "total_pnl":    round(total_pnl, 4),
        "wins":         total_wins,
        "losses":       total_losses,
        "win_rate":     f"{overall_win_rate}%",
        "total_trades": total_trades,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "assets":       asset_stats,
    }
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(combined_data, f, indent=2)
    except Exception:
        pass

    ranked = sorted(asset_stats.items(), key=lambda x: x[1]["pnl"], reverse=True)
    print("\n" + "=" * 80)
    print(f"{BOLD}{CYAN}💰 PNL SUMMARY{RESET}")
    print("=" * 80)
    print(f"Total PNL       : ${total_pnl:.2f}")
    print(f"Total Trades    : {total_trades} ({total_wins}W | {total_losses}L)")
    print(f"Overall Win Rate: {overall_win_rate:.1f}%\n")
    print(f"{BOLD}Asset Ranking:{RESET}")
    for i, (asset, stats) in enumerate(ranked, 1):
        color = GREEN if stats["pnl"] >= 0 else RED
        print(f"{i}. {asset:<6} | {stats['total_trades']:>2} trades | "
              f"{stats['wins']:>3}W {stats['losses']:>3}L | "
              f"{stats['win_rate']:>5.1f}% | {color}${stats['pnl']:.2f}{RESET}")

    if send_telegram_notify:
        current_time = t.time()
        if current_time - last_notification_time > 60:
            telegram_msg = (
                f"<b>💰 EMILIANO PNL Summary</b>\n\n"
                f"<b>Total PNL:</b> {get_pnl_emoji(total_pnl)} "
                f"<b>${total_pnl:.2f}</b>\n"
                f"<b>Total Trades:</b> {total_trades} "
                f"(<b>{total_wins}W</b> - <b>{total_losses}L</b>)\n"
                f"<b>Overall Win Rate:</b> {overall_win_rate:.1f}%\n\n"
                f"<b>Ranking:</b>\n"
            )
            for i, (asset, stats) in enumerate(ranked, 1):
                emoji = get_pnl_emoji(stats["pnl"])
                telegram_msg += (
                    f"{i}. <b>{asset}</b>: {emoji} ${stats['pnl']:.2f} | "
                    f"{stats['total_trades']} trades "
                    f"(<b>{stats['wins']}W</b> - {stats['losses']}L) | "
                    f"{stats['win_rate']:.1f}%\n"
                )
            telegram_msg += f"\nLast Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            send_telegram(telegram_msg)
            last_notification_time = current_time


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════
#
# Startup order (this is the fix for the duplicate-global-task issue):
#   1. Construct ONE AccountService — single Web3 connection, single ClobClient,
#      auth derived/created exactly once for the whole process.
#   2. Run the wallet audit ONCE, here, before any market worker exists.
#   3. Start the global PnL-merge scheduler ONCE.
#   4. Construct N MarketWorker instances (one per tracked asset), each
#      sharing the same AccountService by reference. Adding more assets here
#      never creates a second wallet audit, a second ClobClient, or a second
#      PnL-merge scheduler — those only ever exist once per process.
# ═════════════════════════════════════════════════════════════════════════════

async def main():
    account = AccountService()

    # Global, account-level, one-time startup work — never duplicated below.
    if not account.run_wallet_audit():
        console.print("[bold red]Wallet audit failed — aborting startup.[/bold red]")
        return
    account.start_pnl_merge_scheduler()

    # Per-asset market workers — concurrent, but each is purely market-scoped.
    bots = [MarketWorker(asset, account) for asset in ["btc", "eth", "sol", "xrp"]]

    await asyncio.gather(*[bot.start() for bot in bots], dashboard_loop(bots))


if __name__ == "__main__":
    try:
        print("🚀 Starting EmilianoBot — 90-Cent Single-Leg Directional Mode...")
        print(f"   Entry: ≥{round(MIN_ENTRY_PRICE*100)}c | "
              f"Skip locked: {round(LOCKED_LOW*100)}c & {round(LOCKED_HIGH*100)}c | "
              f"Size: {POSITION_SIZE} shares")
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[bold yellow]👋 EmilianoBot shutting down...[/bold yellow]")
    except Exception as e:
        console.print(f"[bold red]Fatal Error: {e}[/bold red]")