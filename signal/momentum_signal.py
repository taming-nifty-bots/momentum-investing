"""
ETF Momentum Rotation - SIGNAL job (runs once a day, before the open).

EVERY decision the strategy makes is made here. This job never places an order;
the momentum job places orders and decides nothing. The two never call each
other - they meet only at two fields in Mongo:

    marked_for_exit   this job sets it, the momentum job sells whatever carries it
    plan.target       this job writes it, the momentum job buys whatever is missing

  1. Trailing stops - EVERY session. Update each holding's peak (highest daily
     close since entry) and flag it marked_for_exit if the close has fallen
     STOP_PCT below that peak. A stopped name is also removed from the target,
     which is what stops the momentum job buying it straight back.
  2. Ranking, rotations and the target - ONLY when a rebalance is due. Rank the
     universe, work out what to keep and what to buy, flag the holdings that fell
     out of the band with the SAME marked_for_exit flag, and write the target
     book. Then stamp the month, so this step does not run again until the next
     one.

Rotations and stops share one flag on purpose. To the momentum job an exit is an
exit; the reason is a label for the ledger, not something it acts on.

The target is a desired STATE, not an instruction. The momentum job compares what
is held against it and buys the difference, so re-reading it is harmless, a
missed run costs nothing but time, and a half-finished one is picked up by the
next session. That is also what makes T+1 work without any bookkeeping: the
momentum job sells on day 1 and stops, and buys the still-missing names on day 2.

The two steps run on different clocks because a stop can break on ANY session
while a rotation happens once a month. Skipping step 2 changes no decision - the
rank is read only at the rebalance - and saves ~24 of the ~28 Dhan calls a day.

The universe is read from MongoDB (etf_universe); the strategy parameters are
constants below. Like the other strategies: one self-contained file, tamingnifty
for the broker and Slack, MongoDB as the ledger.

SCHEDULING: everything here is driven by DAILY candles, so this job must not act
on a session until that session's daily bar has been published. It runs the
MORNING AFTER the session it is acting on (before the momentum job), which is
early enough to be safe regardless of how soon after the close Dhan publishes.
How soon that actually is has NOT been measured - the one observation we have is
that the 2026-09-25 bar was absent at 22:57 that night and present by 12:03 the
next day, which brackets it to a 13-hour window and proves nothing narrower.
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

# --- strategy parameters ------------------------------------------------------
# A 1:1 transcription of the validated engine (bt_v2.py, FINALCFG). These used to
# sit in an etf_params document in Mongo, which meant the strategy could be
# changed without a code change and without leaving a trace. They are constants
# here instead, so git history IS the audit trail: changing one is a commit, a
# review and a redeploy, which is the correct amount of friction for numbers that
# define the strategy. The 28-ETF universe stays in Mongo - that is data, not a
# rule, and it is edited far more often.
LOOKBACK = 21          # momentum window, in trading days
N_HOLD = 4             # target number of holdings
TOP_RETAIN = 7         # keep a holding while its rank is <= this
STOP_PCT = 0.08        # trailing stop, below the peak daily close
MOMENTUM_MIN = 0.0     # eligibility: momentum must be STRICTLY above this

# Which session is "now" is a property of the MARKET, not of what we happen to
# hold, so the freshness guard asks one liquid name for the latest daily bar
# rather than inferring it from the bulk fetch. NIFTYBEES is the most heavily
# traded ETF on the NSE and will have a bar for every session; if it is ever
# missing from etf_universe we fall back to the first candidate.
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
      retained    = held names still ranked <= top_retain, one per bucket (kept)
      new_entries = best-ranked eligible names filling the remaining slots,
                    one per bucket, skipping buckets already used (bought)

    Anything held that is NOT retained is a rotation exit. There is no separate
    rotate_out return value: the caller derives it as "held and not retained" and
    flags those positions marked_for_exit, the same flag a trailing stop sets.
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
# Dhan rate-limits the data API. Walking all 28 ETFs back to back with no pause
# gets most of the calls refused with DH-904 (measured 2026-09-25: 17 of 27 failed
# at no sleep, 0 failed at 0.25s), so every candle request waits first. At half a
# second the whole universe still takes under 20 seconds.
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
    already have, adding them to `series_of` in place.

    The run builds `series_of` up in stages - the calendar reference first, then
    the holdings, then (only on a rebalance) the rest of the universe - and each
    stage skips what an earlier one already fetched. So a name that is both held
    and ranked still costs exactly one Dhan call, and an ordinary day never pays
    for the 24-odd candidates nothing is going to read.

    A symbol with no secid is skipped rather than fetched: a holding that has
    dropped out of etf_universe carries its own secid on the position document,
    which is what the caller passes in.
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

    The momentum job buys whatever the target says is missing, so this one line
    IS the no-mid-month-refill rule: remove the name and there is no longer a
    gap for it to fill. Leave it in and the next session would buy it straight
    back, which the strategy explicitly forbids - the freed cash has to sit idle
    until the next rebalance.
    """
    plan = state.find_one({"_id": "plan"})
    if not plan:
        return
    target = [s for s in (plan.get("target") or []) if s != sym]
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

    # 1. Freshness guard, on a SINGLE candle. Everything below decides one thing:
    #    what to do about the session whose daily bar is the newest one Dhan has.
    #    If that is the bar the previous run already acted on there is nothing new
    #    to decide - so stop, rather than redo the stops or (the real hazard)
    #    re-decide on a stale bar because the latest one is not published yet.
    #    This makes the cron slot a performance question instead of a correctness
    #    one, and makes the job safe to re-run by hand.
    #
    #    Asking ONE liquid name costs a single Dhan call, so the common "not
    #    published yet" case - the very case this guard exists for - now exits in
    #    about a second instead of walking the whole universe first. The candle
    #    date and the wall clock are logged every run, so the Slack history
    #    measures Dhan's real publish lag for free.
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

    # 2. Trailing-stop maintenance - EVERY day. A stop can break on any session,
    #    so this runs unconditionally, and it only needs the holdings' candles.
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

    # 3. Ranking - ONLY when a rebalance is actually due. Holdings rotate once a
    #    month, and the rank is read at exactly one moment: that rebalance, for
    #    the retain band and the new entries. Ranking all 28 ETFs on the other ~20
    #    sessions computes a number nothing consumes, at 28 rate-limited calls a
    #    run. Skipping it changes no decision the strategy makes.
    #
    #    last_rebalanced_month is stamped by the MOMENTUM job when it finishes a
    #    rebalance, so this condition stays true until the plan has actually been
    #    consumed: if momentum does not run on the 1st, signal keeps refreshing
    #    the plan every session until it does. That is also what keeps the plan
    #    young enough for momentum's PLAN_MAX_AGE_DAYS check.
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

    # The rebalance is decided on this ranking, so a run that silently lost half
    # the universe to rate limiting must not be allowed to write a plan from it.
    if len(closes_by_symbol) < len(candidates):
        missing = [s for s in candidates if s not in closes_by_symbol]
        notify(f"SIGNAL: WARNING - no candles for {len(missing)} candidate(s): {missing}")
    if len(closes_by_symbol) < len(candidates) * 0.8:
        notify(f"SIGNAL ABORT: only {len(closes_by_symbol)} of {len(candidates)} candidates "
               f"returned candles - refusing to write a rebalance plan from a partial "
               f"universe. Re-run once Dhan is answering.")
        raise SystemExit(1)

    ranked, rank_of, mom_of = rank_universe(closes_by_symbol, LOOKBACK, MOMENTUM_MIN)

    # The plan is computed on the holdings that survive tonight's stops (they sell
    # at the open before the rebalance is evaluated - matches the engine's order).
    held_after_stops = [p["symbol"] for p in active if p["symbol"] not in stopped_now]
    retained, new_entries = select_rebalance(
        held_after_stops, ranked, rank_of, bucket_of, N_HOLD, TOP_RETAIN)

    # Flag the rotations. Anything held that the ranking did not retain has fallen
    # out of the band, so it goes out - marked with the SAME flag a trailing stop
    # uses, because the momentum job is not supposed to know the difference. It
    # sells what is flagged; deciding what deserves flagging is this job's work.
    rotations = [p["symbol"] for p in active
                 if p["symbol"] not in retained and p["symbol"] not in stopped_now
                 and not p.get("marked_for_exit")]
    for sym in rotations:
        positions.update_one({"symbol": sym, "status": "active"},
                             {"$set": {"marked_for_exit": True,
                                       "exit_reason": "rebalance"}})

    # target = the book we want to be holding once this rebalance has played out.
    # The momentum job buys whatever of it is missing, so it is a desired STATE,
    # not an instruction: re-reading it is harmless, and a run that is skipped or
    # half-finished is simply picked up by the next one. Everything else in the
    # plan is there to be read by a human in Mongo or Slack.
    plan = {
        "signal_date": str(signal_date),
        "generated_on": str(today()),
        "for_month": current_month,
        "target": retained + new_entries,
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

    # Stamp the month HERE, not in the momentum job. This job decided the
    # rebalance, so this job records that it is decided; tomorrow it goes back to
    # stops-only and the target stays frozen for the momentum job to work through
    # over however many sessions T+1 needs. last_candle_date is the CANDLE this
    # was decided on, not the day the job happened to run.
    set_meta(last_candle_date=str(signal_date), last_signal_date=str(today()),
             last_rebalanced_month=current_month)

    notify("SIGNAL done. signal_date={sd} | month={m} | held={hb} | stops={st} | "
           "retain={rt} | rotate={ro} | new={nw} | target={tg}".format(
               sd=plan["signal_date"], m=current_month, hb=plan["held_before"],
               st=stopped_now, rt=retained, ro=rotations, nw=new_entries,
               tg=plan["target"]))
    if ranked:
        notify("SIGNAL ranking (top): " + ", ".join(
            f"{s}#{rank_of[s]}({mom_of[s]*100:.1f}%)" for s in ranked[:6]))


if __name__ == "__main__":
    main()
