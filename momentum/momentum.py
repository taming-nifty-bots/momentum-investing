"""
ETF Momentum Rotation - MOMENTUM job (runs once a day, at/just after the open).

The only job that places orders, and it makes no decisions. It reads two fields
the signal job left in Mongo and acts on them:

    marked_for_exit   sell it, whatever the reason
    plan.target       the book we should hold; buy whatever is missing

  1. Sell every holding flagged marked_for_exit.
  2. Sold anything? STOP - Dhan settles T+1, so that cash is not usable today.
  3. Otherwise buy whatever of the target we are not already holding.

See README.md for the design - why the target is a state rather than a shopping
list, and how that makes T+1 and every retry fall out for free.

SAFETY: orders are DRY RUN unless live_trading=true.
"""
import os
import time
from datetime import datetime

from pymongo import MongoClient
from dotenv import find_dotenv, load_dotenv
from slack_sdk import WebClient
from tamingnifty import connect_dhan as edge
from tamingnifty import utils as util

load_dotenv(find_dotenv())

live_trading = os.environ.get("live_trading", "false").lower() == "true"
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
orders = db[f"etf_orders_{user_name}"]

# The signal job owns the ranking/stop parameters; this job owns only the money.
START_CAPITAL = 400000.0


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


# --- universe (read from Mongo; fail loudly if unseeded) ----------------------
def load_universe():
    docs = list(universe_coll.find({"is_park": {"$ne": True}}))
    if not docs:
        notify("MOMENTUM ABORT: etf_universe is empty - seed etf_universe in Mongo first.")
        raise SystemExit(1)
    # secid is the NSE exchange token, which is also Dhan's securityId for cash
    # equity.
    secid_of = {d["symbol"]: d["secid"] for d in docs}
    tsym_of = {d["symbol"]: d["tsym"] for d in docs}
    bucket_of = {d["symbol"]: d["bucket"] for d in docs}
    return secid_of, tsym_of, bucket_of


# --- ledger -------------------------------------------------------------------
# Positions and the accounts / meta / plan singletons live in separate
# collections: the ledger is DERIVED from find({"status": "active"}), so a stray
# status field on a singleton would silently skew the P&L.
def strip(doc):
    if doc is None:
        return None
    doc = dict(doc)
    doc.pop("_id", None)
    return doc


def compute_accounts():
    """Work the whole ledger out from the position documents themselves.

    Nothing is incremented, so the ledger cannot drift: a run that dies midway
    is simply recomputed by the next one.
      invested       = capital tied up in open positions
      total_pnl      = realised P&L over closed positions
      unused_balance = start + pnl - invested
    """
    start = START_CAPITAL
    active = list(positions.find({"status": "active"}))
    closed = list(positions.find({"status": "closed"}))
    pnls = [float(p.get("pnl") or 0.0) for p in closed]

    invested = round(sum(float(p["capital_deployed"]) for p in active), 2)
    total_pnl = round(sum(pnls), 2)
    return {
        "start_capital": start,
        "total_balance": round(start + total_pnl, 2),
        "unused_balance": round(start + total_pnl - invested, 2),
        "invested": invested,
        "total_trades": len(active) + len(closed),
        "active_trades": len(active),
        "closed_trades": len(closed),
        "winning_trades": sum(1 for p in pnls if p > 0),
        "losing_trades": sum(1 for p in pnls if p < 0),
        "total_pnl": total_pnl,
    }


def save_accounts():
    """Recompute the ledger and store it. Mongo keeps a snapshot for reading;
    the positions remain the single source of truth."""
    acc = compute_accounts()
    state.update_one({"_id": "accounts"}, {"$set": acc}, upsert=True)
    return acc


def get_meta():
    return strip(state.find_one({"_id": "meta"})) or {}


def set_meta(**fields):
    state.update_one({"_id": "meta"}, {"$set": fields}, upsert=True)


def get_plan():
    return strip(state.find_one({"_id": "plan"}))


def active_positions():
    return [strip(p) for p in positions.find({"status": "active"})]


# --- broker (LTP + order placement, DRY-RUN gated) ----------------------------
# Dhan keys everything by securityId; the tsym is carried only for readability.
def last_daily_close(conn, secid):
    from datetime import timedelta
    end = datetime.now()
    df = edge.fetch_equity_data(conn, secid, end - timedelta(days=15), end, "day")
    if df is None or len(df) == 0 or "close" not in df.columns:
        return None
    return float(df["close"].iloc[-1])


def ltp(conn, secid):
    try:
        price = edge.get_equity_ltp(conn, secid)
        if price:
            return float(price)
    except Exception:
        pass
    return last_daily_close(conn, secid)


def simulated_order(tsym, side, qty, price):
    # Same shape as a real Dhan order dict, so callers need not care which.
    return {
        "orderId": f"DRYRUN-{side}-{tsym}-{datetime.now():%H%M%S}",
        "orderStatus": "DRYRUN", "transactionType": side, "tradingSymbol": tsym,
        "quantity": qty, "filledQty": qty,
        "averageTradedPrice": round(price, 2) if price else 0.0,
        "orderType": "MARKET", "productType": "CNC",
        "message": "dry-run: no order sent to broker",
        "createTime": datetime.now().strftime("%d-%m-%Y %H:%M:%S"),
    }


def place_market(conn, secid, tsym, side, qty):
    """CNC MARKET order for `qty` units of `tsym`. DRY RUN unless live_trading=true."""
    side = side.upper()
    if qty is None or qty <= 0:
        raise ValueError(f"place_market: non-positive qty {qty} for {tsym}")

    if not live_trading:
        return simulated_order(tsym, side, qty, ltp(conn, secid))

    try:
        order = edge.place_equity_order(conn, secid, side, int(qty))
    except Exception as exc:
        # A rejection is an expected outcome (unsettled funds), and Dhan raises it
        # as an HTTP error - hand the caller a dict instead of killing the job.
        return {"orderStatus": "REJECTED", "message": str(exc)}
    return edge.wait_for_fill(conn, order["orderId"])


def filled(order):
    return order.get("orderStatus") in ("TRADED", "DRYRUN")


def why_failed(order):
    return order.get("omsErrorDescription") or order.get("message") or order.get("orderStatus")


# --- trade actions (accounting invariants preserved exactly from the engine) --
def sell(conn, secid_of, pos, reason):
    """Market-sell a full position, realize P&L and update the ledger."""
    sym, qty = pos["symbol"], pos["quantity"]
    tsym = pos.get("tsym") or sym
    secid = pos.get("secid") or secid_of.get(sym)
    if not secid:
        notify(f"MOMENTUM: SELL SKIPPED {sym} - no secid on the position or in etf_universe.")
        return 0.0
    order = place_market(conn, secid, tsym, "SELL", qty)
    orders.insert_one(dict(order))
    if not filled(order):
        notify(f"MOMENTUM: SELL FAILED {sym} - {why_failed(order)}")
        return 0.0
    avg = float(order["averageTradedPrice"])
    entry = float(pos["entry_price"])
    cap = float(pos["capital_deployed"])
    pnl = round((avg - entry) * qty, 2)
    roi = round(pnl / cap * 100, 2) if cap else 0.0

    positions.update_one(
        {"symbol": sym, "status": "active"},
        {"$set": {"status": "closed", "exit_price": avg, "exit_date": str(today()),
                  "pnl": pnl, "roi": roi, "exit_reason": reason}})
    save_accounts()             # derived from the positions, so recompute AFTER the write
    notify(f"MOMENTUM: SOLD {sym} x{qty} @ {avg:.2f} ({reason}) pnl=Rs{pnl:.0f} roi={roi:.1f}%")
    return round(avg * qty, 2)


def buy(conn, sym, secid, tsym, bucket, alloc):
    """Market-buy `alloc` rupees of `sym`, floored to whole units, and record it."""
    price = ltp(conn, secid)
    if not price or price <= 0:
        notify(f"MOMENTUM: no price for {sym}; skipping buy")
        return None                                     # local skip (not a broker rejection)
    qty = int(alloc / price)                            # whole ETF units only
    if qty <= 0:                                        # skip if the slice buys 0 units
        notify(f"MOMENTUM: alloc Rs{alloc:.0f} buys 0 units of {sym}; skipping")
        return None                                     # local skip (not a broker rejection)
    order = place_market(conn, secid, tsym, "BUY", qty)
    orders.insert_one(dict(order))
    if not filled(order):
        notify(f"MOMENTUM: BUY FAILED {sym} - {why_failed(order)}")
        return False                                    # broker rejected (e.g. funds unsettled)
    avg = float(order["averageTradedPrice"])
    spend = round(avg * qty, 2)

    positions.insert_one({
        "symbol": sym, "secid": secid, "tsym": tsym, "bucket": bucket,
        "entry_price": avg, "quantity": qty, "capital_deployed": spend,
        "entry_date": str(today()), "peak": avg, "status": "active",
        "marked_for_exit": False, "exit_reason": "",
        "ltp": avg, "last_close": avg, "shadow_pnl": 0.0,
        "exit_price": "", "exit_date": "", "pnl": "", "roi": 0.0,
    })
    save_accounts()             # derived from the positions, so recompute AFTER the write
    notify(f"MOMENTUM: BOUGHT {sym} x{qty} @ {avg:.2f} (Rs{spend:.0f})")
    return True


def place_new_entries(conn, secid_of, tsym_of, bucket_of, new_entries, available):
    """Buy `new_entries` now, splitting `available` cash equally across them, each
    slice floored to whole units (matches the engine's equal-weight snapshot).

    Stops at a broker REJECTION; the skipped names stay missing from the target,
    so a later session buys them. A local skip (no price / 0 units) is not a
    rejection and does not stop the loop.
    """
    alloc_each = available / len(new_entries)
    for sym in new_entries:
        time.sleep(1)
        secid = secid_of.get(sym)
        if not secid:
            notify(f"MOMENTUM: {sym} is not in etf_universe (no secid); skipping")
            continue
        tsym = tsym_of.get(sym, sym)
        result = buy(conn, sym, secid, tsym, bucket_of.get(sym, ""), alloc_each)
        if result is False:                             # broker rejected the order
            notify(f"MOMENTUM: buy REJECTED - broker declined (e.g. funds unsettled). "
                   f"Skipping the remaining entries; they stay missing from the target "
                   f"so the next session tries again.")
            return False
    return True


def summary():
    acc = save_accounts()
    holdings = [p["symbol"] for p in active_positions()]
    equity = round(acc["unused_balance"] + acc["invested"], 2)
    notify("MOMENTUM summary | holdings={h} | cash=Rs{c:.0f} | invested=Rs{i:.0f} | "
           "equity=Rs{e:.0f} | realized_pnl=Rs{p:.0f} | W/L={w}/{l}".format(
               h=holdings, c=acc["unused_balance"], i=acc["invested"], e=equity,
               p=acc["total_pnl"], w=acc.get("winning_trades", 0),
               l=acc.get("losing_trades", 0)))


def main():
    if not is_trading_day():
        notify("MOMENTUM: not a trading day - nothing to do.")
        return

    mode = "LIVE" if live_trading else "DRY-RUN"
    secid_of, tsym_of, bucket_of = load_universe()

    conn = edge.login_to_dhan()
    notify(f"MOMENTUM started [{mode}] (NSE)")
    notify(f"MOMENTUM public IP: {util.get_public_ip()}")   # must be whitelisted

    # 1. Sell everything the signal job flagged - stops and rotations alike.
    sold_today = []
    for pos in active_positions():
        if pos.get("marked_for_exit"):
            time.sleep(1)
            sell(conn, secid_of, pos, pos.get("exit_reason") or "trailing_stop")
            sold_today.append(pos["symbol"])

    # 2. Sold something? Stop - Dhan settles T+1. Nothing is written down for
    #    tomorrow: the missing names will still be missing tomorrow.
    if sold_today:
        notify(f"MOMENTUM: sold {sold_today} today - T+1 means that cash is not usable "
               f"until the next session, so no entries today.")
        summary()
        return

    # 3. Nothing sold, so our cash is settled - buy what the target is missing.
    plan = get_plan()
    if not plan:
        notify("MOMENTUM: no plan in Mongo yet - run the signal job first.")
        summary()
        return

    held = {p["symbol"] for p in active_positions()}
    missing = [s for s in (plan.get("target") or []) if s not in held]
    available = compute_accounts()["unused_balance"]
    if missing and available > 1e-9:
        notify(f"MOMENTUM: target={plan.get('target')} | held={sorted(held)} | "
               f"buying {missing} with settled cash Rs{available:.0f}")
        place_new_entries(conn, secid_of, tsym_of, bucket_of, missing, available)
    elif missing:
        notify(f"MOMENTUM: {missing} missing from the target but cash is "
               f"Rs{available:.0f} - nothing to buy with.")

    set_meta(last_momentum_date=str(today()))
    summary()


if __name__ == "__main__":
    main()
