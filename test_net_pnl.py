"""Cost accounting tests; no Mongo, broker or Slack access."""
import ast
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tamingnifty"))
from tamingnifty.charges import etf_order_charges, etf_tax_segment, sum_charges


def load_functions(names, namespace):
    path = Path(__file__).parent / "momentum" / "momentum.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    assert len(nodes) == len(names)
    namespace.update(etf_order_charges=etf_order_charges, etf_tax_segment=etf_tax_segment,
                     sum_charges=sum_charges)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)


def test_accounts_debit_all_incurred_charges_once():
    closed = dict(symbol="PHARMABEES", status="closed", quantity=1000,
                  capital_deployed=100000, exit_price=110, pnl=10000)
    active = dict(symbol="LIQUIDCASE", status="active", quantity=500, capital_deployed=50000)
    namespace = dict(START_CAPITAL=400000, positions=SimpleNamespace(
        find=lambda query: [active] if query["status"] == "active" else [closed]))
    load_functions({"compute_accounts"}, namespace)
    accounts = namespace["compute_accounts"]()
    assert accounts["total_pnl"] == 10000
    assert accounts["net_pnl"] == 9961.39, accounts
    assert accounts["charges"]["total"] == 48.47, accounts
    assert accounts["total_balance"] == 409951.53
    assert accounts["unused_balance"] == 359951.53
    closed["charges"] = sum_charges(
        etf_order_charges("PHARMABEES", "BUY", 100000),
        etf_order_charges("PHARMABEES", "SELL", 110000))
    active["charges"] = etf_order_charges("LIQUIDCASE", "BUY", 50000)
    assert namespace["compute_accounts"]() == accounts


def buy_namespace(open_position=None):
    inserts = []
    updates = []
    messages = []
    namespace = dict(
        positions=SimpleNamespace(find_one=lambda query: open_position,
                                  insert_one=lambda doc: inserts.append(doc),
                                  update_one=lambda query, update: updates.append(update["$set"])),
        orders=SimpleNamespace(insert_one=lambda order: None),
        notify=messages.append, ltp=lambda *args: 100,
        place_market=lambda conn, secid, tsym, side, qty: dict(
            orderStatus="DRYRUN", averageTradedPrice=100),
        filled=lambda order: True, save_accounts=lambda: None, today=lambda: "2026-10-26")
    load_functions({"buy"}, namespace)
    return namespace, inserts, updates, messages


def test_buy_reserves_fees_and_adds_only_requested_fields():
    namespace, inserts, _, _ = buy_namespace()
    assert namespace["buy"](None, "PHARMABEES", "1", "PHARMABEES", "Healthcare", 1000)
    position = inserts[0]
    assert position["quantity"] == 9
    assert position["capital_deployed"] == 900
    assert position["capital_deployed"] + position["charges"]["total"] <= 1000
    assert position["net_pnl"] is None
    assert "charges_schedule" not in position and "charges_estimated" not in position


def test_topup_adds_entry_fees_without_duplicate_position():
    pos = dict(quantity=5, capital_deployed=500)
    namespace, inserts, updates, _ = buy_namespace(pos)
    namespace["buy"](None, "PHARMABEES", "1", "PHARMABEES", "Healthcare", 1000)
    assert not inserts
    assert updates[0]["quantity"] == 14
    assert updates[0]["capital_deployed"] == 1400
    assert updates[0]["charges"]["total"] == 0.06


def test_unverified_symbols_block_before_any_order():
    namespace, inserts, updates, messages = buy_namespace()
    for symbol in ("MON100", "HNGSNGBEES", "SILVERBEES", "UNREGISTERED"):
        result = namespace["buy"](None, symbol, "1", symbol, "Unknown", 1000)
        assert result is None
    assert not inserts and not updates
    assert len(messages) == 4 and all("BUY BLOCKED" in message for message in messages)


def test_sell_records_gross_and_net_and_keeps_gross_roi():
    writes = []
    position = dict(symbol="PHARMABEES", quantity=1000, secid="1",
                    entry_price=100, capital_deployed=100000)
    namespace = dict(
        positions=SimpleNamespace(update_one=lambda query, update: writes.append(update["$set"])),
        orders=SimpleNamespace(insert_one=lambda order: None), notify=lambda message: None,
        place_market=lambda *args: dict(orderStatus="DRYRUN", averageTradedPrice=110),
        filled=lambda order: True, save_accounts=lambda: None, today=lambda: "2026-10-26")
    load_functions({"sell"}, namespace)
    proceeds = namespace["sell"](None, position, "test")
    assert proceeds == 110000
    assert writes[0]["pnl"] == 10000
    assert writes[0]["net_pnl"] == 9961.39
    assert writes[0]["charges"]["total"] == 38.61
    assert writes[0]["roi"] == 10


def test_net_win_count_not_gross_win_count():
    closed = dict(symbol="PHARMABEES", status="closed", quantity=1000,
                  capital_deployed=100000, exit_price=100.01, pnl=10)
    namespace = dict(START_CAPITAL=400000, positions=SimpleNamespace(
        find=lambda query: [] if query["status"] == "active" else [closed]))
    load_functions({"compute_accounts"}, namespace)
    accounts = namespace["compute_accounts"]()
    assert accounts["total_pnl"] == 10 and accounts["net_pnl"] < 0
    assert accounts["winning_trades"] == 0 and accounts["losing_trades"] == 1


if __name__ == "__main__":
    tests = [value for key, value in sorted(globals().items()) if key.startswith("test_")]
    for test in tests:
        test()
        print("PASS", test.__name__)
    print(len(tests), "passed")
