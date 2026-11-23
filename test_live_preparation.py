"""Offline live handoff boundaries; never connect to broker, Mongo or Slack."""
import ast
import inspect
import importlib.util
import math
import os
import time
import unittest
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from test_net_pnl import buy_namespace, load_functions
from test_execution_safety import guard_client


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
        uuid4=lambda: SimpleNamespace(hex="a" * 32), notify=Mock(),
        util=SimpleNamespace(ist_now=lambda: datetime(2026, 12, 1, 9, 29), exception_detail=str),
        orders=SimpleNamespace(insert_one=insert, update_one=update))
    load_functions({"place_market", "filled", "check_write"}, namespace)
    return namespace, edge, saved, events


def real_executor():
    documents = {}
    client, guards = guard_client()
    for name in ("etf_positions_test_live", "etf_state_test_live", "etf_orders_test_live"):
        records = []
        documents[name] = records
        def collection(records):
            def find(query):
                return [deepcopy(doc) for doc in records if all(doc.get(k) == v for k, v in query.items())]
            def insert(doc):
                records.append(deepcopy(doc))
                return SimpleNamespace(acknowledged=True)
            def update(query, changes, upsert=False):
                for doc in records:
                    if all(doc.get(k) == v for k, v in query.items()):
                        doc.update(deepcopy(changes["$set"]))
                        return SimpleNamespace(acknowledged=True, matched_count=1)
                if upsert:
                    records.append(dict(query, **deepcopy(changes["$set"])))
                return SimpleNamespace(acknowledged=True, matched_count=0)
            return SimpleNamespace(find=find, find_one=lambda query: next(iter(find(query)), None),
                                   insert_one=insert, update_one=update)
        client["Bots"][name] = collection(records)
    path = Path(__file__).parent / "momentum" / "momentum.py"
    spec = importlib.util.spec_from_file_location("momentum_review_test", path)
    bot = importlib.util.module_from_spec(spec)
    with patch.dict(os.environ, {"live_trading": "true", "user_name": "test", "MONGO_DB": "Bots"}), \
            patch("dotenv.load_dotenv"), patch("dotenv.find_dotenv", return_value=""), \
            patch("pymongo.MongoClient", return_value=client), patch("slack_sdk.WebClient"):
        spec.loader.exec_module(bot)
    bot.today = lambda: datetime(2026, 12, 1).date()
    return bot, documents, guards


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
        self.assertLessEqual(len(edge.prepare_order.call_args.kwargs["correlation_id"]), 30)

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

    def test_real_job_http_boundaries_persist_actual_fill_and_batch_slack(self):
        bot, documents, guards = real_executor()
        requests = []
        quote = dict(depth=dict(buy=[dict(price=99.9, quantity=10000)],
                                sell=[dict(price=100, quantity=10000)]),
                     lower_circuit_limit=50, upper_circuit_limit=150)
        def post(url, headers, json, timeout):
            if url.endswith("/marketfeed/quote"):
                data = dict(status="success", data={"NSE_EQ": {"1": quote}})
            else:
                self.assertEqual(json["exchangeSegment"], "NSE_EQ")
                self.assertEqual(json["productType"], "CNC")
                self.assertEqual(json["orderType"], "LIMIT")
                self.assertLessEqual(len(json["correlationId"]), 30)
                self.assertTrue(documents["etf_orders_test_live"])
                requests.append(deepcopy(json))
                data = dict(orderId="123", orderStatus="PENDING")
            return SimpleNamespace(status_code=200, json=lambda: data)
        def get(url, headers, timeout):
            request = requests[-1]
            data = dict(request, orderId="123", orderStatus="TRADED",
                        filledQty=request["quantity"],
                        averageTradedPrice=100.05 if request["transactionType"] == "BUY" else 99.85)
            return SimpleNamespace(status_code=200, json=lambda: data)
        documents["etf_state_test_live"].extend([
            dict(_id="meta", last_signal_date="2026-12-01", last_candle_date="2026-11-30"),
            dict(_id="plan", for_month="2026-12", target=[
                dict(symbol="PHARMABEES", secid="1", tsym="PHARMABEES-EQ", bucket="Healthcare")])])
        slack = bot.slack_client.chat_postMessage
        def posted(**kwargs):
            self.assertEqual(kwargs["channel"], "#etf-momentum-investing")
            if "order timeline" in kwargs["text"]:
                self.assertEqual(documents["etf_orders_test_live"][-1]["orderStatus"], "TRADED")
            self.assertNotEqual(kwargs["text"], "")
        slack.side_effect = posted
        with patch.object(bot, "check_trading_day", return_value=True), \
                patch.object(bot.edge, "login_to_dhan", return_value=dict(client_id="test", access_token="test")), \
                patch.object(bot.edge, "get_equity_ltp", return_value=100), \
                patch.object(bot.edge, "execution_tick", return_value=0.01), \
                patch.object(bot.edge.requests, "post", side_effect=post), \
                patch.object(bot.edge.requests, "get", side_effect=get), \
                patch.object(bot.util, "ist_now", return_value=datetime(2026, 12, 1, 9, 29)) as clock, \
                patch.object(bot.util, "get_public_ip", return_value="test"), \
                patch.object(bot.time, "sleep"):
            bot.main()
            count = len(requests)
            bot.main()
            self.assertEqual(len(requests), 1, "Restart must not duplicate the BUY")
            documents["etf_positions_test_live"][0]["marked_for_exit"] = True
            documents["etf_state_test_live"][0]["last_signal_date"] = "2026-12-02"
            documents["etf_state_test_live"][0]["last_candle_date"] = "2026-12-01"
            bot.today = lambda: datetime(2026, 12, 2).date()
            clock.return_value = datetime(2026, 12, 2, 9, 29)
            bot.main()
            bot.main()
        self.assertEqual(count, 1)
        self.assertEqual(len(requests), 2, "Same-day sale rerun must not submit another order")
        self.assertGreater(requests[0]["price"], 100, "BUY must price from the ask, with cushion")
        self.assertLess(requests[1]["price"], 99.9, "SELL must price from the bid, with cushion")
        position = documents["etf_positions_test_live"][0]
        self.assertEqual(position["entry_price"], 100.05, "Use broker fill, not quote LIMIT or LTP")
        self.assertEqual(position["exit_price"], 99.85)
        self.assertEqual(position["status"], "closed")
        self.assertEqual(position["pnl"], round((99.85 - 100.05) * position["quantity"], 2))
        self.assertEqual(position["net_pnl"], round(position["pnl"] - position["charges"]["total"], 2))
        self.assertEqual(guards[bot.EXECUTION_SCOPE]["status"], "complete")
        timelines = [call.kwargs["text"] for call in slack.call_args_list
                     if "order timeline" in call.kwargs["text"]]
        self.assertEqual(len(timelines), 2)
        self.assertIn("Broker status TRADED", timelines[0])

    def test_executor_rebalance_stamp_requires_acknowledged_write(self):
        namespace = dict(state=SimpleNamespace(update_one=lambda *args, **kwargs:
                                               SimpleNamespace(acknowledged=False)))
        load_functions({"set_meta", "check_write"}, namespace)
        with self.assertRaisesRegex(RuntimeError, "executor metadata"):
            namespace["set_meta"](last_rebalanced_month="2026-12")


if __name__ == "__main__":
    unittest.main()
