"""Offline live handoff boundaries; never connect to broker, Mongo or Slack."""
import ast
import inspect
import math
import os
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_net_pnl import buy_namespace, load_functions


def configuration(folder, filename, mode):
    source = Path(__file__).parent / folder / filename
    tree = ast.parse(source.read_text(encoding="utf-8-sig"))
    wanted = {"live_trading", "ledger_user", "START_CAPITAL", "LIVE_START_DATE"}
    nodes = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "live_mode":
            nodes.append(node)
        if isinstance(node, ast.Assign) and any(
                isinstance(target, ast.Name) and target.id in wanted for target in node.targets):
            nodes.append(node)
    namespace = dict(os=os, user_name="test")
    with patch.dict(os.environ, {"live_trading": mode}):
        exec(compile(ast.Module(body=nodes, type_ignores=[]), str(source), "exec"), namespace)
    return namespace


def live_order_namespace(side="BUY", allocation=50000):
    events = []
    saved = []
    def insert(doc):
        saved.append(dict(doc))
        events.append("intent")
        return SimpleNamespace(acknowledged=True)
    def update(query, changes, **kwargs):
        saved[0].update(changes["$set"])
        events.append("persist")
        return SimpleNamespace(acknowledged=True, matched_count=1)
    result = dict(orderId="1", orderStatus="TRADED", quantity=495, filledQty=495,
                  transactionType=side, securityId="1", productType="CNC",
                  averageTradedPrice=100)
    def prepare(*args, **kwargs):
        kwargs["on_event"]("quote")
        return dict(request=dict(price=100, quantity=495),
                    policy=dict(tick=0.01, max_price=101))
    def submit(conn, prepared):
        assert saved[0]["orderStatus"] == "INTENT"
        events.append("submit")
        return dict(orderId="1", orderStatus="PENDING")
    edge = SimpleNamespace(
        prepare_order=Mock(side_effect=prepare), submit_prepared_order=Mock(side_effect=submit),
        wait_for_fill=Mock(return_value=result),
        round_execution_price=lambda price, tick, side: math.floor(price / tick + 1e-9) * tick)
    namespace = dict(live_trading=True, edge=edge, math=math, time=time,
        uuid4=lambda: SimpleNamespace(hex="test"), notify=Mock(),
        util=SimpleNamespace(ist_now=lambda: datetime(2026, 12, 1, 9, 29), exception_detail=str),
        orders=SimpleNamespace(insert_one=insert, update_one=update))
    load_functions({"place_market", "filled", "check_write"}, namespace)
    return namespace, edge, saved, events


class LivePreparationTests(unittest.TestCase):
    def test_mode_isolates_both_jobs_and_preserves_paper_capital(self):
        for mode, suffix, capital in (("false", "test", 400000), ("true", "test_live", 200000)):
            executor = configuration("momentum", "momentum.py", mode)
            signal = configuration("signal", "momentum_signal.py", mode)
            self.assertEqual(executor["ledger_user"], suffix)
            self.assertEqual(signal["ledger_user"], suffix)
            self.assertEqual(executor["START_CAPITAL"], capital)
            self.assertEqual(executor["LIVE_START_DATE"], signal["LIVE_START_DATE"])
        for folder, filename in (("momentum", "momentum.py"), ("signal", "momentum_signal.py")):
            with self.assertRaises(ValueError):
                configuration(folder, filename, "treu")

    def test_start_date_and_signal_gate_before_login(self):
        namespace = dict(live_trading=True, LIVE_START_DATE="2026-12-01",
            today=lambda: datetime(2026, 11, 30).date(), notify=Mock(), inspect=inspect)
        load_functions({"live_ready"}, namespace)
        self.assertFalse(namespace["live_ready"]())
        namespace.update(today=lambda: datetime(2026, 12, 1).date(),
            edge=SimpleNamespace(
                prepare_order=lambda on_event=None: None,
                wait_for_fill=lambda prepared=None, on_event=None, on_alert=None: None),
            get_meta=lambda: dict(last_signal_date="2026-11-30", last_candle_date="2026-11-27"))
        with self.assertRaisesRegex(ValueError, "completed today"):
            namespace["live_ready"]()
        namespace["get_meta"] = lambda: dict(last_signal_date="2026-12-01", last_candle_date="2026-11-30")
        namespace["util"] = SimpleNamespace(ist_now=lambda: datetime(2026, 12, 1, 9, 29))
        self.assertTrue(namespace["live_ready"]())
        namespace["util"].ist_now = lambda: datetime(2026, 12, 1, 15, 30)
        with self.assertRaisesRegex(ValueError, "regular NSE"):
            namespace["live_ready"]()

    def test_buy_intent_precedes_submission_and_caps_all_in_cost(self):
        namespace, edge, saved, events = live_order_namespace()
        namespace["place_market"](None, "1", "PHARMABEES-EQ", "BUY", 495, alloc=50000)
        prepared = saved[0]["prepared"]
        cap = prepared["policy"]["max_price"]
        charges = namespace["etf_order_charges"]("PHARMABEES", "BUY", cap * 495)
        self.assertLessEqual(cap * 495 + charges["total"], 50000)
        self.assertGreater(cap, 100)
        self.assertLess(events.index("intent"), events.index("submit"))
        self.assertEqual(saved[0]["orderStatus"], "TRADED")
        self.assertIs(edge.wait_for_fill.call_args.kwargs["prepared"], prepared)
        self.assertEqual(namespace["notify"].call_count, 1)

    def test_over_budget_depth_and_intent_write_failure_submit_nothing(self):
        namespace, edge, _, _ = live_order_namespace()
        with self.assertRaisesRegex(ValueError, "allocated cash"):
            namespace["place_market"](None, "1", "PHARMABEES-EQ", "BUY", 495, alloc=49000)
        edge.submit_prepared_order.assert_not_called()
        namespace, edge, _, _ = live_order_namespace()
        namespace["orders"].insert_one = Mock(return_value=SimpleNamespace(acknowledged=False))
        with self.assertRaisesRegex(RuntimeError, "persistence"):
            namespace["place_market"](None, "1", "PHARMABEES-EQ", "BUY", 495, alloc=50000)
        edge.submit_prepared_order.assert_not_called()

    def test_partial_fill_and_invalid_fill_price_stop_after_persistence(self):
        for changes in (dict(orderStatus="PART_TRADED", filledQty=1),
                        dict(averageTradedPrice=0), dict(quantity=496)):
            namespace, edge, saved, _ = live_order_namespace()
            edge.wait_for_fill.return_value.update(changes)
            with self.assertRaises((RuntimeError, ValueError)):
                namespace["place_market"](None, "1", "PHARMABEES-EQ", "BUY", 495, alloc=50000)
            for key, value in changes.items():
                self.assertEqual(saved[0][key], value)

    def test_sell_has_no_long_entry_budget_or_deadline(self):
        namespace, edge, _, _ = live_order_namespace("SELL")
        namespace["place_market"](None, "1", "PHARMABEES-EQ", "SELL", 495)
        self.assertIsNone(edge.prepare_order.call_args.kwargs["entry_deadline"])

    def test_live_buy_reserves_one_percent_move_and_entry_fees(self):
        namespace, inserts, _, _ = buy_namespace()
        namespace["live_trading"] = True
        namespace["math"] = math
        namespace["compute_accounts"] = lambda: dict(unused_balance=50000)
        namespace["place_market"] = Mock(return_value=dict(
            orderStatus="TRADED", averageTradedPrice=101))
        namespace["buy"](None, "PHARMABEES", "1", "PHARMABEES-EQ", "Healthcare", 50000)
        pos = inserts[0]
        self.assertLessEqual(pos["capital_deployed"] + pos["charges"]["total"], 50000)
        self.assertEqual(namespace["place_market"].call_args.kwargs["alloc"], 50000)

    def test_live_ltp_never_falls_back_to_daily_close(self):
        namespace = dict(live_trading=True,
            edge=SimpleNamespace(get_equity_ltp=Mock(side_effect=TimeoutError("quote unavailable"))),
            last_daily_close=Mock())
        load_functions({"ltp"}, namespace)
        with self.assertRaises(TimeoutError):
            namespace["ltp"](None, "1")
        namespace["last_daily_close"].assert_not_called()

    def test_stale_worker_cannot_duplicate_live_holding_or_overspend_cash(self):
        namespace, _, _, _ = buy_namespace(dict(quantity=100))
        namespace["live_trading"] = True
        with self.assertRaisesRegex(ValueError, "already exists"):
            namespace["buy"](None, "PHARMABEES", "1", "PHARMABEES", "Healthcare", 50000)
        namespace, _, _, _ = buy_namespace()
        namespace.update(live_trading=True, compute_accounts=lambda: dict(unused_balance=1000))
        with self.assertRaisesRegex(ValueError, "cash changed"):
            namespace["buy"](None, "PHARMABEES", "1", "PHARMABEES", "Healthcare", 50000)

    def test_previous_session_skips_holiday_and_weekend(self):
        path = Path(__file__).parent / "signal" / "momentum_signal.py"
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "previous_session")
        namespace = dict(today=lambda: datetime(2026, 12, 28).date(), timedelta=timedelta,
            mongo_client={"Bots": {"exchange_calendars": SimpleNamespace(find_one=lambda query: {})}},
            validate_calendar=lambda document, day: {datetime(2026, 12, 25).date(): "Christmas"})
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
        self.assertEqual(namespace["previous_session"](), datetime(2026, 12, 24).date())

    def test_signal_waits_before_december_without_login_or_state_write(self):
        path = Path(__file__).parent / "signal" / "momentum_signal.py"
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
        namespace = dict(check_trading_day=lambda *args: True, mongo_client=None,
            notify=Mock(), live_trading=True, LIVE_START_DATE="2026-12-01",
            today=lambda: datetime(2026, 11, 30).date(), set_meta=Mock(),
            edge=SimpleNamespace(login_to_dhan=Mock()))
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), "exec"), namespace)
        namespace["main"]()
        namespace["set_meta"].assert_not_called()
        namespace["edge"].login_to_dhan.assert_not_called()

    def test_live_rerun_after_sale_cannot_buy_unsettled_cash(self):
        namespace = dict(check_trading_day=lambda *args: True, mongo_client=None,
            EXECUTION_SCOPE="test", assert_execution_clear=Mock(), live_ready=lambda: True,
            live_trading=True, edge=SimpleNamespace(login_to_dhan=Mock(return_value=None)),
            notify=Mock(), util=SimpleNamespace(get_public_ip=lambda: "test"),
            slack_channel="test", active_positions=lambda: [],
            positions=SimpleNamespace(find_one=lambda query: dict(status="closed")),
            today=lambda: datetime(2026, 12, 1).date(), summary=Mock(), get_plan=Mock())
        load_functions({"main"}, namespace)
        namespace["main"]()
        namespace["get_plan"].assert_not_called()
        namespace["summary"].assert_called_once()


if __name__ == "__main__":
    unittest.main()
