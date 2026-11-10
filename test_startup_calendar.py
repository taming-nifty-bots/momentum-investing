"""Offline tests of the real startup guards, with no broker/Mongo imports."""
import ast
import unittest
from pathlib import Path
from unittest.mock import Mock

JOBS = (("signal", "momentum_signal.py"), ("momentum", "momentum.py"))


class StartupCalendarTests(unittest.TestCase):
    def test_calendar_is_first_and_failures_escape(self):
        for folder, filename in JOBS:
            path = Path(__file__).parent / folder / filename
            tree = ast.parse(path.read_text(encoding="utf-8-sig"))
            self.assertTrue(any(isinstance(n, ast.ImportFrom)
                                and n.module == "tamingnifty.safety"
                                and any(a.name == "check_trading_day" for a in n.names)
                                for n in tree.body))
            main = next(n for n in tree.body
                        if isinstance(n, ast.FunctionDef) and n.name == "main")
            gate = main.body[0]
            self.assertIsInstance(gate, ast.If)
            self.assertIn("check_trading_day", ast.unparse(gate.test))
            # Keep the actual guard; never execute the trading remainder.
            main.body = [gate] + ast.parse("after_gate()").body
            for live in (False, True):
                for outcome in (True, False, ValueError("missing/invalid calendar"),
                                RuntimeError("calendar database unavailable")):
                    with self.subTest(job=filename, live=live, outcome=outcome):
                        mongo, notify, after = Mock(), Mock(), Mock()
                        def check(client, callback):
                            self.assertIs(client, mongo)
                            callback("calendar status")
                            if isinstance(outcome, Exception):
                                raise outcome
                            return outcome
                        check_mock = Mock(side_effect=check)
                        namespace = dict(check_trading_day=check_mock, mongo_client=mongo,
                                         notify=notify, after_gate=after, live_trading=live)
                        exec(compile(ast.fix_missing_locations(
                            ast.Module(body=[main], type_ignores=[])), str(path), "exec"), namespace)
                        if isinstance(outcome, Exception):
                            with self.assertRaises(type(outcome)):
                                namespace["main"]()
                        else:
                            self.assertIsNone(namespace["main"]())
                        check_mock.assert_called_once()
                        notify.assert_called_once_with("calendar status")
                        self.assertEqual(after.call_count, int(outcome is True))


if __name__ == "__main__":
    unittest.main()
