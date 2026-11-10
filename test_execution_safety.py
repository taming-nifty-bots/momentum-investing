"""Offline execution-boundary checks using the real shared durable guard."""
import ast
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from test_net_pnl import buy_namespace, load_functions

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tamingnifty"))
from tamingnifty.safety import assert_execution_clear, guarded_execution
from pymongo.errors import DuplicateKeyError


def guard_client():
    documents = {}

    def update(query, changes):
        document = documents.get(query["_id"])
        matched = document is not None and all(document.get(k) == v for k, v in query.items())
        if matched:
            document.update(changes["$set"])
        return SimpleNamespace(acknowledged=True, matched_count=int(matched))

    def insert(document):
        if document["_id"] in documents:
            raise DuplicateKeyError("existing scope")
        documents[document["_id"]] = dict(document)
        return SimpleNamespace(acknowledged=True)

    collection = SimpleNamespace(update_one=update, insert_one=insert,
                                 find_one=lambda query: documents.get(query["_id"]))
    return {"Bots": {"execution_guards": collection}}, documents


def assert_stopped(function, *args):
    try:
        function(*args)
    except SystemExit as error:
        assert error.code == 1
        return
    raise AssertionError("Expected fail-stop")


def test_buy_persistence_failure_latches_and_blocks_restart():
    namespace, _, _, _ = buy_namespace()
    client, documents = guard_client()
    broker = Mock(return_value=dict(orderStatus="DRYRUN", averageTradedPrice=100))
    namespace["place_market"] = broker
    namespace["positions"].insert_one = Mock(side_effect=RuntimeError("position write failed"))
    namespace.update(mongo_client=client, EXECUTION_SCOPE="Bots.etf_positions_test",
                     guarded_execution=guarded_execution,
                     time=SimpleNamespace(sleep=lambda _: None))
    load_functions({"place_new_entries"}, namespace)
    entries = [dict(symbol="PHARMABEES", secid="1")]
    assert_stopped(namespace["place_new_entries"], None, entries, 1000)
    assert broker.call_count == 1
    assert documents[namespace["EXECUTION_SCOPE"]]["status"] == "failed"
    try:
        assert_execution_clear(client, namespace["EXECUTION_SCOPE"], namespace["notify"])
    except RuntimeError:
        pass
    else:
        raise AssertionError("Restart must remain blocked")
    assert_stopped(namespace["place_new_entries"], None, entries, 1000)
    assert broker.call_count == 1


def test_ambiguous_submission_is_not_converted_to_rejection():
    client, documents = guard_client()
    broker = Mock(side_effect=TimeoutError("response lost after submission"))
    namespace = dict(live_trading=True, edge=SimpleNamespace(place_equity_order=broker))
    load_functions({"place_market"}, namespace)
    assert_stopped(guarded_execution, client, "Bots.etf_positions_test", Mock(), "BUY",
                   namespace["place_market"], None, "1", "PHARMABEES", "BUY", 1)
    assert broker.call_count == 1
    assert documents["Bots.etf_positions_test"]["status"] == "failed"


def test_pending_and_partial_orders_cannot_complete_guard():
    namespace = {}
    load_functions({"filled"}, namespace)
    for order in (dict(orderStatus="PENDING", filledQty=0),
                  dict(orderStatus="CANCELLED", filledQty=1),
                  dict(orderStatus="REJECTED"),
                  dict(orderStatus="TRADED", quantity=2, filledQty=1)):
        try:
            namespace["filled"](order)
        except RuntimeError:
            pass
        else:
            raise AssertionError("Ambiguous/partial order must stop")
    assert namespace["filled"](dict(orderStatus="REJECTED", filledQty=0)) is False
    assert namespace["filled"](dict(orderStatus="TRADED", quantity=2, filledQty=2)) is True


def test_successful_buy_completes_only_after_position_and_accounts():
    namespace, inserts, _, _ = buy_namespace()
    client, documents = guard_client()
    namespace.update(mongo_client=client, EXECUTION_SCOPE="Bots.etf_positions_test",
                     guarded_execution=guarded_execution,
                     time=SimpleNamespace(sleep=lambda _: None))
    def save_accounts():
        assert inserts
        assert documents[namespace["EXECUTION_SCOPE"]]["status"] == "pending"
    namespace["save_accounts"] = save_accounts
    load_functions({"place_new_entries"}, namespace)
    assert namespace["place_new_entries"](None, [dict(symbol="PHARMABEES", secid="1")], 1000)
    assert documents[namespace["EXECUTION_SCOPE"]]["status"] == "complete"
    assert_execution_clear(client, namespace["EXECUTION_SCOPE"], namespace["notify"])


def test_sell_failure_after_submission_retains_guard():
    client, documents = guard_client()
    broker = Mock(return_value=dict(orderStatus="DRYRUN", averageTradedPrice=110))
    namespace = dict(place_market=broker,
                     orders=SimpleNamespace(insert_one=Mock(side_effect=RuntimeError("order log lost"))))
    load_functions({"sell", "check_write"}, namespace)
    position = dict(symbol="PHARMABEES", secid="1", quantity=10)
    assert_stopped(guarded_execution, client, "Bots.etf_positions_test", Mock(), "SELL",
                   namespace["sell"], None, position, "stop")
    assert broker.call_count == 1
    assert documents["Bots.etf_positions_test"]["status"] == "failed"


def test_critical_writes_require_acknowledgement_and_matching_position():
    namespace = {}
    load_functions({"check_write"}, namespace)
    for result, require_match in ((SimpleNamespace(acknowledged=False), False),
                                  (SimpleNamespace(acknowledged=True, matched_count=0), True)):
        try:
            namespace["check_write"](result, "test", require_match)
        except RuntimeError:
            pass
        else:
            raise AssertionError("Unconfirmed persistence must stop")


def test_pending_guard_from_interrupted_process_blocks_submission():
    client, documents = guard_client()
    scope = "Bots.etf_positions_test"
    documents[scope] = dict(_id=scope, status="pending", action="previous BUY")
    broker = Mock()
    assert_stopped(guarded_execution, client, scope, Mock(), "BUY", broker)
    broker.assert_not_called()
    assert documents[scope]["status"] == "pending"


def test_startup_and_sell_boundary_are_guarded():
    source = Path(__file__).parent / "momentum" / "momentum.py"
    tree = ast.parse(source.read_text(encoding="utf-8-sig"))
    main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "main")
    startup = main.body[1]
    assert ast.unparse(startup.value.func) == "assert_execution_clear"
    assert not any(isinstance(n, ast.Try) for n in main.body)
    sells = [n for n in ast.walk(main) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Name) and n.func.id == "guarded_execution"]
    assert len(sells) == 1 and ast.unparse(sells[0].args[4]) == "sell"


if __name__ == "__main__":
    tests = [value for key, value in sorted(globals().items()) if key.startswith("test_")]
    for test in tests:
        test()
        print("PASS", test.__name__)
    print(len(tests), "passed")
