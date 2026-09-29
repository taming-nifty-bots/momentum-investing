"""
ETF Momentum Rotation - MOMENTUM job (runs once a day, at/just after the open).

The only job that places orders, and it makes no decisions. It reads two fields
the signal job left in Mongo and acts on them:

    marked_for_exit   sell it, whatever the reason
    plan.target       the book we should hold, each entry carrying its own
                      secid/tsym/bucket; buy whatever is missing

  1. Sell every holding flagged marked_for_exit.
  2. Sold anything? STOP - Dhan settles T+1, so that cash is not usable today.
  3. Otherwise buy whatever of the target we are not already holding.
  4. Once nothing is missing, stamp meta.last_rebalanced_month. That is the one
     field this job WRITES for the other one: a rebalance is "done" when the
     money moved, not when it was decided, and only this job knows that. Once
     stamped, no new entry goes on the book until the next month - so a stop
     that sells mid-month leaves the cash idle, which is the validated rule.

See README.md for the design - why the target is a state rather than a shopping
list, and how that makes T+1 and every retry fall out for free.

SAFETY: orders are DRY RUN unless live_trading=true.
"""
import os
import time
import traceback
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
positions = db[f"etf_positions_{user_name}"]   # one doc per position, nothing else
state = db[f"etf_state_{user_name}"]           # the singletons: accounts / meta / plan
orders = db[f"etf_orders_{user_name}"]

# The signal job owns the ranking/stop parameters; this job owns only the money.
START_CAPITAL = 400000.0


def notify(message):
    # No print here. util.notify prints the same line itself, with a timestamp and the
    # channel name, before it posts - keeping a second print would double every log line.
    try:
        util.notify(message=str(message), slack_channel=slack_channel, slack_client=slack_client)
    except Exception as exc:
        print(f"[notify] slack post failed, message was '{message}': {util.exception_detail(exc)}", flush=True)


def today():
    return datetime.now().date()


def month_key():
    # Same format signal uses, so the two jobs compare the same string.
    d = today()
    return f"{d.year:04d}-{d.month:02d}"


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
    except Exception as exc:
        # The daily-close fallback below is the right behaviour, but swallowing this
        # silently meant a broken LTP feed looked identical to a healthy one that
        # simply had no quote - and the price used for sizing was quietly stale.
        print(f"[ltp] live quote for {secid} unavailable, falling back to the last "
              f"daily close: {util.exception_detail(exc)}", flush=True)
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
        # exception_detail rather than str(exc) because str() on a requests HTTPError
        # is "400 Client Error:  for url: ..." and drops Dhan's actual reason, which
        # then gets written to the order log and Slack as the explanation.
        return {"orderStatus": "REJECTED", "message": util.exception_detail(exc)}
    return edge.wait_for_fill(conn, order["orderId"])


def filled(order):
    return order.get("orderStatus") in ("TRADED", "DRYRUN")


def why_failed(order):
    return order.get("omsErrorDescription") or order.get("message") or order.get("orderStatus")


# --- trade actions (accounting invariants preserved exactly from the engine) --
def sell(conn, pos, reason):
    """Market-sell a full position, realize P&L and update the ledger."""
    sym, qty = pos["symbol"], pos["quantity"]
    tsym = pos.get("tsym") or sym
    secid = pos.get("secid")
    if not secid:
        notify(f"MOMENTUM: SELL SKIPPED {sym} - no secid on the position document.")
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


def place_new_entries(conn, new_entries, available):
    """Buy `new_entries` - target entries, each carrying its own secid/tsym/bucket
    - splitting `available` cash equally across them, each slice floored to whole
    units (matches the engine's equal-weight snapshot).

    Stops at a broker REJECTION; the skipped names stay missing from the target,
    so a later session buys them. A local skip (no price / 0 units) is not a
    rejection and does not stop the loop.
    """
    alloc_each = available / len(new_entries)
    for entry in new_entries:
        time.sleep(1)
        sym, secid = entry["symbol"], entry.get("secid")
        if not secid:
            notify(f"MOMENTUM: no secid on the target entry for {sym}; skipping")
            continue
        result = buy(conn, sym, secid, entry.get("tsym") or sym,
                     entry.get("bucket", ""), alloc_each)
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
    # No calendar check here. Which days this job runs is decided by the Azure
    # cron schedule, so weekends and market holidays are handled there.
    mode = "LIVE" if live_trading else "DRY-RUN"
    conn = edge.login_to_dhan()
    notify(f"MOMENTUM started [{mode}] (NSE)")
    notify(f"MOMENTUM public IP: {util.get_public_ip()}")   # must be whitelisted

    # 1. Sell everything the signal job flagged - stops and rotations alike.
    sold_today = []
    for pos in active_positions():
        if pos.get("marked_for_exit"):
            time.sleep(1)
            sell(conn, pos, pos.get("exit_reason") or "trailing_stop")
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

    this_month = month_key()
    target = plan.get("target") or []
    held = {p["symbol"] for p in active_positions()}
    missing = [t for t in target if t["symbol"] not in held]
    names = [t["symbol"] for t in missing]

    # This month's entries are already placed, so nothing new goes on the book
    # until the next rebalance. If a name is missing from the target now, a stop
    # sold it mid-month - and the validated rule is that the freed cash sits idle
    # until the next rebalance rather than refilling the slot.
    if get_meta().get("last_rebalanced_month") == this_month:
        if missing:
            notify(f"MOMENTUM: {names} missing from the target, but {this_month} "
                   f"entries are already done - no mid-month refill.")
        set_meta(last_momentum_date=str(today()))
        summary()
        return

    available = compute_accounts()["unused_balance"]
    if missing and available > 1e-9:
        notify(f"MOMENTUM: target={[t['symbol'] for t in target]} | held={sorted(held)} | "
               f"buying {names} with settled cash Rs{available:.0f}")
        place_new_entries(conn, missing, available)
    elif missing:
        notify(f"MOMENTUM: {names} missing from the target but cash is "
               f"Rs{available:.0f} - nothing to buy with.")

    # 4. Stamping the month is THIS job's call, because this job is the one that
    #    knows whether the money actually moved. signal only decided it.
    #    Two conditions, and both matter:
    #      - the plan must be for THIS month, or we would be marking a month
    #        rebalanced that signal has not even ranked for;
    #      - nothing may still be missing, so a rejected buy leaves the month
    #        open and the next session retries it instead of skipping it.
    still_missing = [t["symbol"] for t in target
                     if t["symbol"] not in {p["symbol"] for p in active_positions()}]
    if plan.get("for_month") != this_month:
        notify(f"MOMENTUM: the plan is for {plan.get('for_month')}, not {this_month} - "
               f"not stamping the month; signal has not ranked for it yet.")
    elif still_missing:
        notify(f"MOMENTUM: {this_month} rebalance NOT complete - {still_missing} still "
               f"missing, so the month stays open and the next session tries again.")
    else:
        set_meta(last_rebalanced_month=this_month)
        notify(f"MOMENTUM: {this_month} rebalance executed - holding "
               f"{[t['symbol'] for t in target]}. No new entries until next month.")

    set_meta(last_momentum_date=str(today()))
    summary()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Without this the job dies with a traceback on stdout and says NOTHING in
        # Slack. This one places real orders, so a silent crash can leave the book
        # half-rebalanced - some legs sent, the rest never attempted - and the only
        # clue would be the next session's "rebalance NOT complete" line.
        #
        # SystemExit is a BaseException, not an Exception, so the deliberate
        # `raise SystemExit(1)` aborts elsewhere in this file still pass straight
        # through here untouched and stay quiet.
        traceback.print_exc()
        notify(f"MOMENTUM CRASHED: {util.exception_detail(e)}")
        raise
