"""
ETF Momentum Rotation - SIGNAL job (runs once a day, after the close).

Analysis only - it never places an order. It does two things:

  1. Trailing stop: for every active holding, update the peak (highest daily
     close since entry) and flag it for exit if the close has dropped stop_pct
     below that peak. The momentum job sells the flagged names at the next open.
  2. Ranking + plan: rank the 28 ETFs by momentum and write a provisional
     monthly-rebalance plan (stamped with the signal date). The momentum job
     uses the plan on the first session of a new month.

Universe + parameters are read from MongoDB (etf_universe, etf_params).
Like the other strategies: one self-contained file, tamingnifty for the broker
and Slack, MongoDB as the ledger.
"""
import os
from datetime import datetime, timedelta

import pandas as pd
from pymongo import MongoClient
from dotenv import find_dotenv, load_dotenv
from slack_sdk import WebClient
from tamingnifty import connect_definedge as edge
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
params_coll = db["etf_params"]
positions = db[f"etf_positions_{user_name}"]


def notify(message):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {message}", flush=True)
    try:
        util.notify(message=str(message), slack_channel=slack_channel, slack_client=slack_client)
    except Exception as exc:
        print(f"[notify] slack post failed: {exc}", flush=True)


def today():
    return datetime.now().date()


def is_trading_day():
    return today().weekday() < 5


def parse_date(s):
    return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()


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
      rotate_out  = held names not retained (sold at the open)
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

    rotate_out = [s for s in held if s not in retained]
    return retained, new_entries, rotate_out


# --- market data (Definedge daily candles via tamingnifty) --------------------
def daily_closes(conn, exchange, tsym, lookback_days=400):
    """(dates, closes) ascending; ~400 calendar days covers the 21d + peak window."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    df = edge.fetch_historical_data(conn, exchange, tsym, start, end, "day")
    if df is None or len(df) == 0 or "close" not in df.columns:
        return [], []
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.dropna(subset=["datetime"]).sort_values("datetime")
    dates = [d.date() for d in df["datetime"]]
    closes = [float(c) for c in df["close"]]
    return dates, closes


def closes_since(conn, exchange, tsym, start_date):
    dates, closes = daily_closes(conn, exchange, tsym)
    return [c for d, c in zip(dates, closes) if d >= start_date]


# --- universe + params (read from Mongo; fail loudly if unseeded) -------------
def load_params():
    doc = params_coll.find_one({"_id": "params"})
    if not doc:
        notify("SIGNAL ABORT: etf_params is empty - seed etf_params in Mongo first.")
        raise SystemExit(1)
    return doc


def load_universe():
    docs = list(universe_coll.find({"is_park": {"$ne": True}}))
    if not docs:
        notify("SIGNAL ABORT: etf_universe is empty - seed etf_universe in Mongo first.")
        raise SystemExit(1)
    tsym_of = {d["symbol"]: d["tsym"] for d in docs}
    bucket_of = {d["symbol"]: d["bucket"] for d in docs}
    candidates = [d["symbol"] for d in docs]
    return candidates, tsym_of, bucket_of


def main():
    if not is_trading_day():
        notify("SIGNAL: not a trading day - nothing to do.")
        return

    params = load_params()
    exchange = params["exchange"]
    lookback = int(params["lookback"])
    n_hold = int(params["n_hold"])
    top_retain = int(params["top_retain"])
    stop_pct = float(params["stop_pct"])
    momentum_min = float(params.get("momentum_min", 0.0))
    series = params.get("series", "EQ")
    candidates, tsym_of, bucket_of = load_universe()

    conn = edge.login_to_integrate()
    notify(f"SIGNAL started ({exchange}, {len(candidates)} ETFs, lookback={lookback}, stop={stop_pct*100:.0f}%)")

    # 1. trailing-stop maintenance on active holdings
    active = list(positions.find({"status": "active"}))
    stopped_now = []
    for pos in active:
        sym = pos["symbol"]
        tsym = tsym_of.get(sym, f"{sym}-{series}")
        entry_dt = parse_date(pos["entry_date"])
        closes = closes_since(conn, exchange, tsym, entry_dt)
        if not closes:
            notify(f"SIGNAL: no candles for {sym}; skipping peak update")
            continue
        peak = update_peak(float(pos["entry_price"]), closes)
        last = closes[-1]
        fields = {
            "peak": round(peak, 4),
            "last_close": round(last, 4),
            "ltp": round(last, 4),
            "shadow_pnl": round((last - float(pos["entry_price"])) * pos["quantity"], 2),
        }
        if stop_hit(last, peak, stop_pct) and not pos.get("marked_for_exit"):
            fields["marked_for_exit"] = True
            fields["exit_reason"] = "trailing_stop"
            stopped_now.append(sym)
            notify(f"SIGNAL: STOP flagged {sym} close={last:.2f} peak={peak:.2f} "
                   f"({(last/peak-1)*100:.1f}% from peak) -> sell at next open")
        positions.update_one({"symbol": sym, "status": "active"}, {"$set": fields})

    # 2. tonight's ranking + provisional rebalance plan
    closes_by_symbol = {}
    signal_date = None
    for sym in candidates:
        dates, closes = daily_closes(conn, exchange, tsym_of[sym])
        if closes:
            closes_by_symbol[sym] = closes
            if dates:
                signal_date = dates[-1] if signal_date is None else max(signal_date, dates[-1])

    ranked, rank_of, mom_of = rank_universe(closes_by_symbol, lookback, momentum_min)

    # plan is computed on the holdings that survive tonight's stops (they sell at
    # the open before the rebalance is evaluated - matches the engine's ordering)
    held_after_stops = [p["symbol"] for p in active if p["symbol"] not in stopped_now]
    retained, new_entries, rotate_out = select_rebalance(
        held_after_stops, ranked, rank_of, bucket_of, n_hold, top_retain)

    plan = {
        "signal_date": str(signal_date or today()),
        "generated_on": str(today()),
        "ranked_top": ranked[:10],
        "rank_of": {s: rank_of[s] for s in ranked[:12]},
        "mom_of": {s: round(mom_of[s], 4) for s in ranked[:12]},
        "held_before": [p["symbol"] for p in active],
        "stop_exits": stopped_now,
        "retained": retained,
        "new_entries": new_entries,
        "rotate_out": rotate_out,
        "n_hold": n_hold,
        "top_retain": top_retain,
        "lookback": lookback,
    }
    positions.update_one({"_id": "plan"}, {"$set": plan}, upsert=True)
    positions.update_one({"_id": "meta"}, {"$set": {"last_signal_date": str(today())}}, upsert=True)

    notify("SIGNAL done. signal_date={sd} | held={hb} | stops={st} | "
           "retain={rt} | new={nw} | rotate_out={ro}".format(
               sd=plan["signal_date"], hb=plan["held_before"], st=stopped_now,
               rt=retained, nw=new_entries, ro=rotate_out))
    if ranked:
        notify("SIGNAL ranking (top): " + ", ".join(
            f"{s}#{rank_of[s]}({mom_of[s]*100:.1f}%)" for s in ranked[:6]))


if __name__ == "__main__":
    main()
