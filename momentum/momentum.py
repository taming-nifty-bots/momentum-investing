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
import inspect
import math
import time
import traceback
from uuid import uuid4
from datetime import datetime

from pymongo import MongoClient
from dotenv import find_dotenv, load_dotenv
from slack_sdk import WebClient
from tamingnifty import connect_dhan as edge
from tamingnifty import utils as util
from tamingnifty.charges import etf_order_charges, etf_tax_segment, sum_charges
from tamingnifty.safety import assert_execution_clear, check_trading_day, guarded_execution

load_dotenv(find_dotenv())

def live_mode():
    mode = os.environ.get("live_trading", "false").strip().lower()
    if mode not in ("true", "false"):
        raise ValueError("live_trading must be true or false")
    return mode == "true"


live_trading = live_mode()
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
user_name = os.environ.get("user_name", "sugam")
MONGO_DB = os.environ.get("MONGO_DB", "Bots")
slack_channel = "etf-momentum-investing"
slack_client = WebClient(token=os.environ.get("slack_token"))

mongo_client = MongoClient(CONNECTION_STRING)
db = mongo_client[MONGO_DB]
ledger_user = user_name + "_live" if live_trading else user_name
positions = db[f"etf_positions_{ledger_user}"]
state = db[f"etf_state_{ledger_user}"]
orders = db[f"etf_orders_{ledger_user}"]
EXECUTION_SCOPE = f"{MONGO_DB}.etf_positions_{ledger_user}"

# The signal job owns the ranking/stop parameters; this job owns only the money.
START_CAPITAL = 200000.0 if live_trading else 400000.0
LIVE_START_DATE = "2026-12-01"


def notify(message):
    # No print here. util.notify prints the same line itself, with a timestamp and the
    # channel name, before it posts - keeping a second print would double every log line.
    try:
        util.notify(message=str(message), slack_channel=slack_channel, slack_client=slack_client)
    except Exception as exc:
        print(f"[notify] slack post failed, message was '{message}': {util.exception_detail(exc)}", flush=True)


def today():
    return util.ist_now().date()


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
      total_pnl      = gross realised P&L over closed positions
      net_pnl        = realised P&L after round-trip charges
      unused_balance = start + gross pnl - all incurred charges - invested
    """
    start = START_CAPITAL
    active = list(positions.find({"status": "active"}))
    closed = list(positions.find({"status": "closed"}))
    pnls = []
    net_pnls = []
    all_charges = []
    for pos in closed:
        pnl = float(pos["pnl"])
        charges = pos.get("charges")
        if charges is None:
            charges = sum_charges(
                etf_order_charges(pos["symbol"], "BUY", pos["capital_deployed"]),
                etf_order_charges(pos["symbol"], "SELL", float(pos["exit_price"]) * pos["quantity"]))
        pnls.append(pnl)
        net_pnls.append(round(pnl - charges["total"], 2))
        all_charges.append(charges)
    for pos in active:
        charges = pos.get("charges")
        if charges is None:
            charges = etf_order_charges(pos["symbol"], "BUY", pos["capital_deployed"])
        all_charges.append(charges)

    invested = round(sum(float(p["capital_deployed"]) for p in active), 2)
    total_pnl = round(sum(pnls), 2)
    charges = sum_charges(*all_charges)
    balance = round(start + total_pnl - charges["total"], 2)
    return {
        "start_capital": start,
        "total_balance": balance,
        "unused_balance": round(balance - invested, 2),
        "invested": invested,
        "total_trades": len(active) + len(closed),
        "active_trades": len(active),
        "closed_trades": len(closed),
        "winning_trades": sum(1 for p in net_pnls if p > 0),
        "losing_trades": sum(1 for p in net_pnls if p < 0),
        "total_pnl": total_pnl,
        "net_pnl": round(sum(net_pnls), 2),
        "charges": charges,
    }


def save_accounts():
    """Recompute the ledger and store it. Mongo keeps a snapshot for reading;
    the positions remain the single source of truth."""
    acc = compute_accounts()
    check_write(state.update_one({"_id": "accounts"}, {"$set": acc}, upsert=True),
                "accounts snapshot")
    return acc


def check_write(result, action, require_match=False):
    if not result.acknowledged or (require_match and result.matched_count != 1):
        raise RuntimeError("Unconfirmed Mongo persistence: " + action)


def get_meta():
    return strip(state.find_one({"_id": "meta"})) or {}


def set_meta(**fields):
    check_write(state.update_one({"_id": "meta"}, {"$set": fields}, upsert=True),
                "executor metadata")


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
        if live_trading:
            raise
        # The daily-close fallback below is the right behaviour, but swallowing this
        # silently meant a broken LTP feed looked identical to a healthy one that
        # simply had no quote - and the price used for sizing was quietly stale.
        print(f"[ltp] live quote for {secid} unavailable, falling back to the last "
              f"daily close: {util.exception_detail(exc)}", flush=True)
    if live_trading:
        raise ValueError("No fresh live price for " + str(secid))
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


def place_market(conn, secid, tsym, side, qty, alloc=None):
    """CNC depth-priced LIMIT in live mode; LTP simulation in paper mode."""
    side = side.upper()
    if not isinstance(qty, int) or isinstance(qty, bool) or qty <= 0:
        raise ValueError(f"place_market: non-positive qty {qty} for {tsym}")

    if not live_trading:
        return simulated_order(tsym, side, qty, ltp(conn, secid))

    timeline = []
    def event(message):
        text = util.ist_now().strftime("%H:%M:%S") + " | " + message
        print(text, flush=True)
        timeline.append(text)

    order_key = "LIVE-" + uuid4().hex[:25]
    try:
        event(f"{side} {tsym} x{qty} | preparing fresh depth")
        prepared = edge.prepare_order(
            conn, secid, side, qty, "NSE_EQ", "CNC",
            entry_deadline=time.time() + 15 if side == "BUY" else None,
            correlation_id=order_key, on_event=event)
        if side == "BUY":
            if alloc is None or not math.isfinite(alloc) or alloc <= 0:
                raise ValueError("Live BUY needs a finite positive allocation")
            tick = prepared["policy"]["tick"]
            reserve = etf_order_charges(tsym.removesuffix("-EQ"), "BUY", alloc)["total"]
            limit = edge.round_execution_price((alloc - reserve) / qty, tick, "SELL")
            if limit < prepared["request"]["price"]:
                raise ValueError("Fresh depth exceeds allocated cash; no order submitted")
            prepared["policy"]["max_price"] = min(prepared["policy"]["max_price"], limit)
        check_write(orders.insert_one(dict(
            _id=order_key, prepared=prepared, orderStatus="INTENT",
            tradingSymbol=tsym, transactionType=side, quantity=qty)), "live order intent")
        event("Intent persisted | submitting LIMIT")
        order = edge.submit_prepared_order(conn, prepared)
        check_write(orders.update_one(
            {"_id": order_key}, {"$set": dict(order)}, upsert=False),
                    "submitted order", require_match=True)
        result = edge.wait_for_fill(conn, order["orderId"], prepared=prepared,
                                    on_event=event, on_alert=notify)
        check_write(orders.update_one(
            {"_id": order_key}, {"$set": dict(result)}), "broker result", require_match=True)
        if (result.get("quantity") != qty or result.get("transactionType") != side
                or str(result.get("securityId")) != str(secid)
                or result.get("productType") != "CNC"):
            raise ValueError("Broker order does not match requested ETF order")
        filled(result)
        if result.get("orderStatus") == "TRADED":
            price = float(result["averageTradedPrice"])
            if not math.isfinite(price) or price <= 0:
                raise ValueError("Invalid broker fill price")
            if side == "BUY" and price * qty + etf_order_charges(tsym.removesuffix("-EQ"), "BUY", price * qty)["total"] > alloc:
                raise ValueError("Broker fill exceeds allocation; reconcile before continuing")
        return result
    except Exception as error:
        notify("MOMENTUM live order failed/uncertain: " + util.exception_detail(error))
        raise
    finally:
        notify("MOMENTUM order timeline | " + tsym + "\n" + "\n".join(timeline))


def filled(order):
    if order.get("orderStatus") == "TRADED":
        quantity = order.get("quantity")
        if (not isinstance(quantity, int) or quantity <= 0
                or order.get("filledQty") != quantity):
            raise RuntimeError("Inconsistent traded quantity requires reconciliation: " + str(order))
    if order.get("orderStatus") not in ("TRADED", "DRYRUN"):
        if (order.get("orderStatus") not in ("REJECTED", "CANCELLED", "EXPIRED")
                or order.get("filledQty") != 0):
            raise RuntimeError("Unresolved/partial order requires reconciliation: " + str(order))
    return order.get("orderStatus") in ("TRADED", "DRYRUN")


def why_failed(order):
    return order.get("omsErrorDescription") or order.get("message") or order.get("orderStatus")


# --- trade actions (accounting invariants preserved exactly from the engine) --
def sell(conn, pos, reason):
    """Market-sell a full position, realize P&L and update the ledger."""
    sym, qty = pos["symbol"], pos["quantity"]
    if live_trading:
        current = positions.find_one({"symbol": sym, "status": "active"})
        if current is None or current["quantity"] != qty:
            raise ValueError("Live holding changed before SELL; reconcile " + sym)
    tsym = pos.get("tsym") or sym
    secid = pos.get("secid")
    if not secid:
        raise ValueError(f"MOMENTUM: cannot sell {sym} - no secid on position")
    order = place_market(conn, secid, tsym, "SELL", qty)
    if not live_trading:
        check_write(orders.insert_one(dict(order)), "sell order")
    if not filled(order):
        notify(f"MOMENTUM: SELL FAILED {sym} - {why_failed(order)}")
        return 0.0
    avg = float(order["averageTradedPrice"])
    cap = float(pos["capital_deployed"])
    pnl = round(avg * qty - cap, 2)
    roi = round(pnl / cap * 100, 2) if cap else 0.0
    entry_charges = pos.get("charges")
    if entry_charges is None:
        entry_charges = etf_order_charges(sym, "BUY", cap)
    charges = sum_charges(entry_charges, etf_order_charges(sym, "SELL", avg * qty))
    net_pnl = round(pnl - charges["total"], 2)

    check_write(positions.update_one(
        {"symbol": sym, "status": "active"},
        {"$set": {"status": "closed", "exit_price": avg, "exit_date": str(today()),
                  "pnl": pnl, "net_pnl": net_pnl, "charges": charges,
                  "roi": roi, "exit_reason": reason}}), "closed position", require_match=True)
    save_accounts()             # derived from the positions, so recompute AFTER the write
    notify(f"MOMENTUM: SOLD {sym} x{qty} @ {avg:.2f} ({reason}) gross=Rs{pnl:.2f} "
           f"charges=Rs{charges['total']:.2f} net=Rs{net_pnl:.2f} gross_roi={roi:.1f}%")
    return round(avg * qty, 2)


def buy(conn, sym, secid, tsym, bucket, alloc):
    """Market-buy `alloc` rupees of `sym`, floored to whole units, and record it."""
    if live_trading:
        if positions.find_one({"symbol": sym, "status": "active"}):
            raise ValueError("Live holding already exists; refusing duplicate BUY " + sym)
        if compute_accounts()["unused_balance"] < alloc:
            raise ValueError("Live cash changed before BUY; recalculate allocation")
    try:
        etf_tax_segment(sym)
    except ValueError as error:
        notify(f"MOMENTUM: BUY BLOCKED {sym} - {error}. Verify ETF charges before entry.")
        return None
    price = ltp(conn, secid)
    if not price or not math.isfinite(price) or price <= 0:
        notify(f"MOMENTUM: no price for {sym}; skipping buy")
        return None                                     # local skip (not a broker rejection)
    sizing_price = price * 1.01 if live_trading else price
    qty = int(alloc / sizing_price)
    while qty > 0:
        charges = etf_order_charges(sym, "BUY", sizing_price * qty)
        if sizing_price * qty + charges["total"] <= alloc:
            break
        qty -= 1
    if qty <= 0:                                        # skip if the slice buys 0 units
        notify(f"MOMENTUM: alloc Rs{alloc:.0f} buys 0 units of {sym}; skipping")
        return None                                     # local skip (not a broker rejection)
    if live_trading:
        order = place_market(conn, secid, tsym, "BUY", qty, alloc=alloc)
    else:
        order = place_market(conn, secid, tsym, "BUY", qty)
        check_write(orders.insert_one(dict(order)), "buy order")
    if not filled(order):
        notify(f"MOMENTUM: BUY FAILED {sym} - {why_failed(order)}")
        return False                                    # broker rejected (e.g. funds unsettled)
    avg = float(order["averageTradedPrice"])
    spend = round(avg * qty, 2)
    charges = etf_order_charges(sym, "BUY", spend)

    open_pos = positions.find_one({"symbol": sym, "status": "active"})
    if open_pos:
        # Defensive: every live caller buys only from `missing`, which by
        # definition excludes names we already hold, so this should not fire.
        # But buy() ending in an unconditional insert_one would be a silent
        # money bug if it ever did - sell() and signal's peak update both do
        # update_one({"symbol": sym, "status": "active"}), which touches
        # exactly one of two active docs for the same symbol and leaves the
        # ledger claiming units already sold. Weighted-average cost instead,
        # with entry_date/peak left at the ORIGINAL entry: signal recomputes
        # the peak from scratch over the closes since entry_date every
        # session, so the trailing stop keeps measuring from where the
        # position really began.
        total_qty = open_pos["quantity"] + qty
        total_cap = round(float(open_pos["capital_deployed"]) + spend, 2)
        previous_charges = open_pos.get("charges")
        if previous_charges is None:
            previous_charges = etf_order_charges(sym, "BUY", open_pos["capital_deployed"])
        charges = sum_charges(previous_charges, charges)
        check_write(positions.update_one(
            {"symbol": sym, "status": "active"},
            {"$set": {"quantity": total_qty, "capital_deployed": total_cap,
                      "entry_price": round(total_cap / total_qty, 4), "charges": charges}}),
                    "position top-up", require_match=True)
        save_accounts()         # derived from the positions, so recompute AFTER the write
        notify(f"MOMENTUM: ADDED {sym} x{qty} @ {avg:.2f} (Rs{spend:.0f}) - now "
               f"x{total_qty}, avg cost {total_cap / total_qty:.2f}")
        return True

    check_write(positions.insert_one({
        "symbol": sym, "secid": secid, "tsym": tsym, "bucket": bucket,
        "entry_price": avg, "quantity": qty, "capital_deployed": spend,
        "entry_date": str(today()), "peak": avg, "status": "active",
        "marked_for_exit": False, "exit_reason": "",
        "ltp": avg, "last_close": avg, "shadow_pnl": 0.0,
        "exit_price": "", "exit_date": "", "pnl": "", "roi": 0.0,
        "charges": charges, "net_pnl": None,
    }), "new position")
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
        result = guarded_execution(
            mongo_client, EXECUTION_SCOPE, notify, "BUY " + sym, buy,
            conn, sym, secid, entry.get("tsym") or sym, entry.get("bucket", ""), alloc_each)
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
           "realized_balance=Rs{e:.0f} | realized_gross=Rs{p:.0f} | "
           "realized_net=Rs{n:.0f} | incurred_charges=Rs{f:.0f} | net W/L={w}/{l}".format(
               h=holdings, c=acc["unused_balance"], i=acc["invested"], e=equity,
               p=acc["total_pnl"], n=acc["net_pnl"], f=acc["charges"]["total"],
               w=acc.get("winning_trades", 0),
               l=acc.get("losing_trades", 0)))


def live_ready():
    if not live_trading:
        return True
    if str(today()) < LIVE_START_DATE:
        notify(f"MOMENTUM LIVE waiting until {LIVE_START_DATE}; no orders.")
        return False
    if "on_event" not in inspect.signature(edge.prepare_order).parameters:
        raise RuntimeError("Live execution requires tamingnifty 2.4.7 event hooks")
    for hook in ("prepared", "on_event", "on_alert"):
        if hook not in inspect.signature(edge.wait_for_fill).parameters:
            raise RuntimeError("Live execution missing wait_for_fill hook: " + hook)
    meta = get_meta()
    if meta.get("last_signal_date") != str(today()):
        raise ValueError("Live signal has not completed today; refusing all orders")
    if not meta.get("last_candle_date") or meta["last_candle_date"] >= str(today()):
        raise ValueError("Live signal needs a completed previous-session candle")
    moment = util.ist_now()
    if not "09:15" <= moment.strftime("%H:%M") < "15:30":
        raise ValueError("Live orders are restricted to the regular NSE session")
    return True


def main():
    if not check_trading_day(mongo_client, notify):
        return
    assert_execution_clear(mongo_client, EXECUTION_SCOPE, notify)
    if not live_ready():
        return
    mode = "LIVE" if live_trading else "DRY-RUN"
    conn = edge.login_to_dhan(slack_channel=slack_channel)
    notify(f"MOMENTUM started [{mode}] (NSE)")
    notify(f"MOMENTUM public IP: {util.get_public_ip()}")   # must be whitelisted

    # 1. Sell everything the signal job flagged - stops and rotations alike.
    sold_today = []
    for pos in active_positions():
        if pos.get("marked_for_exit"):
            time.sleep(1)
            guarded_execution(mongo_client, EXECUTION_SCOPE, notify, "SELL " + pos["symbol"],
                              sell, conn, pos, pos.get("exit_reason") or "trailing_stop")
            sold_today.append(pos["symbol"])

    # 2. Sold something? Stop - Dhan settles T+1. Nothing is written down for
    #    tomorrow: the missing names will still be missing tomorrow.
    if sold_today:
        notify(f"MOMENTUM: sold {sold_today} today - T+1 means that cash is not usable "
               f"until the next session, so no entries today.")
        summary()
        return
    if live_trading and positions.find_one({"status": "closed", "exit_date": str(today())}):
        notify("MOMENTUM: a live sale is already recorded today; no T+1 entries on rerun.")
        summary()
        return

    # 3. Nothing sold, so our cash is settled - buy what the target is missing.
    plan = get_plan()
    if not plan:
        notify("MOMENTUM: no plan in Mongo yet - run the signal job first.")
        summary()
        return

    this_month = month_key()
    if live_trading and plan.get("for_month") != this_month:
        raise ValueError("Live rebalance plan is not for the current month")
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
