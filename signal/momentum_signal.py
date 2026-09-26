"""
ETF Momentum Rotation - SIGNAL job (runs once a day, before the open).

Every decision the strategy makes is made here; the momentum job places the
orders and decides nothing. They never call each other - they meet at two fields
in Mongo: marked_for_exit (this job sets it, momentum sells it) and plan.target
(this job writes it, momentum buys whatever is missing).

  1. Trailing stops - EVERY session.
  2. Ranking, rotations and the target - ONLY when a rebalance is due.

Parameters are constants below; the universe is read from etf_universe in Mongo.

See README.md for the design - why the two steps run on different clocks, why
rotations and stops share one flag, and why the target is a state rather than an
instruction.
"""
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from pymongo import MongoClient
from dotenv import find_dotenv, load_dotenv
from slack_sdk import WebClient
from tamingnifty import connect_dhan as edge
from tamingnifty import utils as util

load_dotenv(find_dotenv())

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
user_name = os.environ.get("user_name", "sugam")
MONGO_DB = os.environ.get("MONGO_DB", "Bots")
slack_channel = "etf-momentum-investing"
slack_client = WebClient(token=os.environ.get("slack_token"))

mongo_client = MongoClient(CONNECTION_STRING)
db = mongo_client[MONGO_DB]
universe_coll = db["etf_universe"]
positions = db[f"etf_positions_{user_name}"]   # one doc per position, nothing else
state = db[f"etf_state_{user_name}"]           # the singletons: accounts / meta / plan

# --- strategy parameters (1:1 from the validated engine bt_v2.py, FINALCFG) ---
LOOKBACK = 21          # momentum window, in trading days
N_HOLD = 4             # target number of holdings
TOP_RETAIN = 7         # keep a holding while its rank is <= this
STOP_PCT = 0.08        # trailing stop, below the peak daily close
MOMENTUM_MIN = 0.0     # eligibility: momentum must be STRICTLY above this

# The freshness guard asks this one liquid name which session is the newest.
CALENDAR_REF = "NIFTYBEES"


def notify(message):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}", flush=True)
    try:
        util.notify(message=str(message), slack_channel=slack_channel, slack_client=slack_client)
    except Exception as exc:
        print(f"[notify] slack post failed: {exc}", flush=True)


def today():
    return datetime.now().date()


def is_trading_day():
    return today().weekday() < 5      # Mon-Fri only (live)


def parse_date(s):
    return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()


def month_key():
    d = today()
    return f"{d.year:04d}-{d.month:02d}"


# --- strategy math (transcribed 1:1 from the validated engine bt_v2.py) -------
def momentum(closes, lookback):
    """Close-to-close return over `lookback` trading days: c_now / c_then - 1."""
    if closes is None or len(closes) < lookback + 1:
        return None
    c_now = closes[-1]
    c_then = closes[-1 - lookback]
    if c_now is None or c_then is None or c_then <= 0:
        return None
    return c_now / c_then - 1.0


def rank_universe(closes_by_symbol, lookback, momentum_min):
    """Rank eligible ETFs (momentum > momentum_min) by momentum, best first."""
    mom_of = {}
    for sym, closes in closes_by_symbol.items():
        m = momentum(closes, lookback)
        if m is not None and m > momentum_min:
            mom_of[sym] = m
    ranked = sorted(mom_of, key=lambda s: mom_of[s], reverse=True)
    rank_of = {s: i + 1 for i, s in enumerate(ranked)}
    return ranked, rank_of, mom_of


def update_peak(entry_price, closes_since_entry):
    """Highest daily close since entry, seeded at the entry price (never drops)."""
    peak = entry_price
    for c in closes_since_entry:
        if c is not None and c > peak:
            peak = c
    return peak


def stop_hit(current_close, peak, stop_pct):
    """Trailing-stop breach on the close: close <= peak * (1 - stop_pct)."""
    if current_close is None or peak is None:
        return False
    return current_close <= peak * (1.0 - stop_pct)


def select_rebalance(held, ranked, rank_of, buckets, n_hold, top_retain):
    """
    Monthly rebalance selection (bt_v2 lines 284-300):
      retained    = held names still ranked <= top_retain, one per bucket
      new_entries = best-ranked eligible names filling the remaining slots,
                    one per bucket, skipping buckets already used

    Anything held that is NOT retained is a rotation exit; the caller derives it
    as "held and not retained".
    """
    retained, used = [], set()
    for sym in held:
        r = rank_of.get(sym)
        if r is not None and r <= top_retain and buckets[sym] not in used:
            retained.append(sym)
            used.add(buckets[sym])

    slots = n_hold - len(retained)
    new_entries = []
    for sym in ranked:
        if slots <= 0:
            break
        if sym in retained:
            continue
        b = buckets[sym]
        if b in used:
            continue
        new_entries.append(sym)
        used.add(b)
        slots -= 1

    return retained, new_entries


# --- market data (Dhan daily candles via tamingnifty) -------------------------
# Dhan rate-limits the data API - without a pause most calls come back DH-904
# (measured 2026-09-25: 17 of 27 failed at no sleep, 0 failed at 0.25s).
PAUSE_BETWEEN_CALLS = 0.5


def daily_closes(conn, secid, lookback_days=400):
    """(dates, closes) ascending; ~400 calendar days covers the 21d + peak window."""
    time.sleep(PAUSE_BETWEEN_CALLS)
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    df = edge.fetch_equity_data(conn, secid, start, end, "day")
    if df is None or len(df) == 0 or "close" not in df.columns:
        return [], []
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).sort_values("datetime")
    dates = [d.date() for d in df["datetime"]]
    closes = [float(c) for c in df["close"]]
    return dates, closes


def fetch_into(conn, series_of, wanted):
    """Fetch the daily series for every (symbol, secid) in `wanted` we do not
    already have, adding them to `series_of` in place. Skipping what is already
    there is what keeps a name that is both held and ranked to one Dhan call.
    """
    for sym, secid in wanted:
        if sym in series_of or not secid:
            continue
        series_of[sym] = daily_closes(conn, secid)
    return series_of


def holdings_to_fetch(active, secid_of):
    """(symbol, secid) for each active holding - its own secid first, so a name
    that has since left etf_universe still gets its stop maintained."""
    return [(p["symbol"], p.get("secid") or secid_of.get(p["symbol"])) for p in active]


def newest_candle_date(series_of):
    """The most recent daily-bar date Dhan returned, across the whole fetch."""
    newest = None
    for dates, closes in series_of.values():
        if dates:
            newest = dates[-1] if newest is None else max(newest, dates[-1])
    return newest


# --- universe (read from Mongo; fail loudly if unseeded) ----------------------
def get_meta():
    doc = state.find_one({"_id": "meta"})
    return doc or {}


def set_meta(**fields):
    state.update_one({"_id": "meta"}, {"$set": fields}, upsert=True)


def drop_from_target(sym):
    """Take a stopped name out of the plan's target.

    Momentum buys whatever the target says is missing, so removing the name here
    IS the no-mid-month-refill rule. See README.md.
    """
    plan = state.find_one({"_id": "plan"})
    if not plan:
        return
    target = [t for t in (plan.get("target") or []) if t["symbol"] != sym]
    state.update_one({"_id": "plan"}, {"$set": {"target": target}})


def load_universe():
    docs = list(universe_coll.find({"is_park": {"$ne": True}}))
    if not docs:
        notify("SIGNAL ABORT: etf_universe is empty - seed etf_universe in Mongo first.")
        raise SystemExit(1)
    # secid is the NSE exchange token, which is also Dhan's securityId for cash
    # equity.
    secid_of = {d["symbol"]: d["secid"] for d in docs}
    tsym_of = {d["symbol"]: d["tsym"] for d in docs}
    bucket_of = {d["symbol"]: d["bucket"] for d in docs}
    candidates = [d["symbol"] for d in docs]
    return candidates, secid_of, tsym_of, bucket_of


def main():
    if not is_trading_day():
        notify("SIGNAL: not a trading day - nothing to do.")
        return

    candidates, secid_of, tsym_of, bucket_of = load_universe()

    conn = edge.login_to_dhan()
    notify(f"SIGNAL started (NSE, {len(candidates)} ETFs, lookback={LOOKBACK}, stop={STOP_PCT*100:.0f}%)")
    notify(f"SIGNAL public IP: {util.get_public_ip()}")

    # 1. Freshness guard, on a SINGLE candle: if the newest bar Dhan has is the
    #    one the last run already acted on, there is nothing new to decide.
    ref = CALENDAR_REF if CALENDAR_REF in secid_of else candidates[0]
    series_of = {}
    fetch_into(conn, series_of, [(ref, secid_of[ref])])
    signal_date = newest_candle_date(series_of)
    if signal_date is None:
        notify(f"SIGNAL ABORT: Dhan returned no daily candles for the calendar "
               f"reference {ref} - cannot tell which session this is, so not "
               f"touching anything.")
        raise SystemExit(1)
    last_done = get_meta().get("last_candle_date")
    notify(f"SIGNAL: newest daily candle = {signal_date} (via {ref}), seen at "
           f"{datetime.now():%H:%M:%S} | last processed = {last_done or 'never'}")
    if str(signal_date) == str(last_done):
        notify("SIGNAL: no new candle since the last run - nothing to do.")
        return

    # 2. Trailing stops - EVERY day. A stop can break on any session.
    active = list(positions.find({"status": "active"}))
    fetch_into(conn, series_of, holdings_to_fetch(active, secid_of))
    stopped_now = []
    for pos in active:
        sym = pos["symbol"]
        dates, all_closes = series_of.get(sym, ([], []))
        if not all_closes:
            notify(f"SIGNAL: no candles for {sym}; skipping peak update")
            continue
        entry_dt = parse_date(pos["entry_date"])
        closes = [c for d, c in zip(dates, all_closes) if d >= entry_dt]
        if not closes:
            notify(f"SIGNAL: no candles since entry for {sym}; skipping peak update")
            continue
        peak = update_peak(float(pos["entry_price"]), closes)
        last = closes[-1]
        fields = {
            "peak": round(peak, 4),
            "last_close": round(last, 4),
            "ltp": round(last, 4),
            "shadow_pnl": round((last - float(pos["entry_price"])) * pos["quantity"], 2),
        }
        if stop_hit(last, peak, STOP_PCT) and not pos.get("marked_for_exit"):
            fields["marked_for_exit"] = True
            fields["exit_reason"] = "trailing_stop"
            stopped_now.append(sym)
            drop_from_target(sym)                 # no refill: see drop_from_target
            notify(f"SIGNAL: STOP flagged {sym} close={last:.2f} peak={peak:.2f} "
                   f"({(last/peak-1)*100:.1f}% from peak) -> sell at next open")
        positions.update_one({"symbol": sym, "status": "active"}, {"$set": fields})

    # 3. Ranking - ONLY when a rebalance is due. The rank is read at exactly one
    #    moment, so computing it daily would cost 28 calls for nothing.
    current_month = month_key()
    if current_month == get_meta().get("last_rebalanced_month"):
        set_meta(last_candle_date=str(signal_date), last_signal_date=str(today()))
        notify(f"SIGNAL done. signal_date={signal_date} | held={[p['symbol'] for p in active]} "
               f"| stops={stopped_now} | {current_month} already rebalanced - ranking skipped "
               f"({len(series_of)} of {len(candidates)} candles fetched).")
        return

    notify(f"SIGNAL: rebalance due for {current_month} - ranking the full universe.")
    fetch_into(conn, series_of, [(s, secid_of[s]) for s in candidates])

    closes_by_symbol = {}
    for sym in candidates:
        dates, closes = series_of.get(sym, ([], []))
        if closes:
            closes_by_symbol[sym] = closes

    # Never plan off a universe that rate limiting has eaten half of.
    if len(closes_by_symbol) < len(candidates):
        missing = [s for s in candidates if s not in closes_by_symbol]
        notify(f"SIGNAL: WARNING - no candles for {len(missing)} candidate(s): {missing}")
    if len(closes_by_symbol) < len(candidates) * 0.8:
        notify(f"SIGNAL ABORT: only {len(closes_by_symbol)} of {len(candidates)} candidates "
               f"returned candles - refusing to write a rebalance plan from a partial "
               f"universe. Re-run once Dhan is answering.")
        raise SystemExit(1)

    ranked, rank_of, mom_of = rank_universe(closes_by_symbol, LOOKBACK, MOMENTUM_MIN)

    # Stops sell at the open, before the rebalance is evaluated (engine order).
    held_after_stops = [p["symbol"] for p in active if p["symbol"] not in stopped_now]
    retained, new_entries = select_rebalance(
        held_after_stops, ranked, rank_of, bucket_of, N_HOLD, TOP_RETAIN)

    # Rotations: held but not retained. Same flag a trailing stop sets, so the
    # momentum job never has to tell the two apart.
    rotations = [p["symbol"] for p in active
                 if p["symbol"] not in retained and p["symbol"] not in stopped_now
                 and not p.get("marked_for_exit")]
    for sym in rotations:
        positions.update_one({"symbol": sym, "status": "active"},
                             {"$set": {"marked_for_exit": True,
                                       "exit_reason": "rebalance"}})

    # target = the book we want to end up holding. It carries everything the
    # momentum job needs to place the order, so that job never has to read
    # etf_universe - a new entry has no position document to look it up on, and
    # this is the moment we already have it resolved. Everything else in the plan
    # is there to be read by a human in Mongo or Slack.
    target = [{"symbol": sym, "secid": secid_of[sym], "tsym": tsym_of[sym],
               "bucket": bucket_of[sym]} for sym in retained + new_entries]
    plan = {
        "signal_date": str(signal_date),
        "generated_on": str(today()),
        "for_month": current_month,
        "target": target,
        "retained": retained,
        "new_entries": new_entries,
        "rotated_out": rotations,
        "held_before": [p["symbol"] for p in active],
        "stop_exits": stopped_now,
        "ranked_top": ranked[:10],
        "rank_of": {s: rank_of[s] for s in ranked[:12]},
        "mom_of": {s: round(mom_of[s], 4) for s in ranked[:12]},
        "n_hold": N_HOLD,
        "top_retain": TOP_RETAIN,
        "lookback": LOOKBACK,
    }
    state.update_one({"_id": "plan"}, {"$set": plan}, upsert=True)

    # This job decided the rebalance, so this job records that it is decided.
    # last_candle_date is the CANDLE it was decided on, not the day we ran.
    set_meta(last_candle_date=str(signal_date), last_signal_date=str(today()),
             last_rebalanced_month=current_month)

    notify("SIGNAL done. signal_date={sd} | month={m} | held={hb} | stops={st} | "
           "retain={rt} | rotate={ro} | new={nw} | target={tg}".format(
               sd=plan["signal_date"], m=current_month, hb=plan["held_before"],
               st=stopped_now, rt=retained, ro=rotations, nw=new_entries,
               tg=[t["symbol"] for t in target]))
    if ranked:
        notify("SIGNAL ranking (top): " + ", ".join(
            f"{s}#{rank_of[s]}({mom_of[s]*100:.1f}%)" for s in ranked[:6]))


if __name__ == "__main__":
    main()
