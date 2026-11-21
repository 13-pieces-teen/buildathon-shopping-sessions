"""Run with: python -m unittest discover -s tests -p test_oracle.py -v"""
from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from collections import defaultdict
from pathlib import Path

import adversarial
import oracle


class ParserTests(unittest.TestCase):
    def test_strict_csv(self):
        examples = [
            (b'"u1",u2\n', 2, ["u1", "u2"]),
            (b'"", \n', 2, ["", " "]),
            (b' u1,"u1 "\n', 2, [" u1", "u1 "]),
            (b'a,b\r\n', 2, ["a", "b\r"]),
            (b'"a","b"\r\n', 2, None),
            (b'a,b"\n', 2, None),
            (b'"a"x,b\n', 2, None),
            (b'"a""",b\n', 2, None),
            (b'a,b,c\n', 2, None),
            (b'\n', 2, None),
        ]
        for row, columns, expected in examples:
            with self.subTest(row=row):
                self.assertEqual(oracle.parse_line(row, columns), expected)

    def test_timestamps(self):
        reference = oracle.parse_ts("2028-02-29T00:00:00Z")
        valid = [
            "2028-02-29T00:00:00-00:00", "2028-02-29T23:59:00+23:59",
            "2028-02-28T00:01:00-23:59", str(reference), "0" * 5000 + str(reference),
        ]
        for stamp in valid:
            with self.subTest(stamp=stamp[:60]):
                self.assertEqual(oracle.parse_ts(stamp), reference)
        invalid = [
            "2027-02-29T00:00:00Z", "2028-02-30T00:00:00Z", "2028-02-29T24:00:00Z",
            "2028-02-29T00:00:60Z", "2028-02-29T00:00:00+24:00", "2028-02-29T00:00:00+05:60",
            "2028-02-29T00:00:00+0200", "2028-02-29T00:00:00.000Z", "2028-02-29T00:00:00z",
            "2028-02-29t00:00:00Z", "2028-02-29 00:00:00Z", "2028-02-29T00:00:00Z\r",
            "", "+1", "-1", "٢٥٠", "２５０", "1.0", "1e3", "9" * 5000,
        ]
        for stamp in invalid:
            with self.subTest(stamp=stamp[:60]):
                self.assertIsNone(oracle.parse_ts(stamp))
        self.assertEqual(oracle.parse_ts("86400000"), oracle.DAY)

    def test_prices(self):
        for raw, expected in [("000250", 250), ("0" * 5000 + "250", 250), ("10000000", 10000000), ("1", 1)]:
            self.assertEqual(oracle.purchase_price(raw), expected)
        for raw in ["0", "000", "-1", "+1", "1.0", "1e3", "10000001", "٢٥٠", "２５０", "1\r", " 1", ""]:
            self.assertIsNone(oracle.purchase_price(raw))

    def test_long_identity_chain_and_unused_minimum(self):
        groups = oracle.IdGroups()
        for index in range(2000):
            groups.join(f"z{index + 1}", f"z{index}")
        groups.join("z2000", "\tunused")
        self.assertEqual(groups.name("z1500"), "\tunused")
        self.assertEqual(groups.name("isolated"), "isolated")
        groups.join("é", "😀")
        self.assertEqual(groups.name("😀"), min(["é", "😀"], key=lambda x: x.encode()))


class FocusedPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.records = {
            "products": [["p1", "shoes"], ["p2", "books"]],
            "campaigns": [["c_1", "search"], ["c_2", "social"], ["c_3", "email"]],
            "events": [], "clicks": [], "links": [],
        }

    def tearDown(self):
        self.temp.cleanup()

    def event(self, ident, timestamp, user="u1", kind="view", price="", product="p1", ingest=None):
        if ingest is None:
            ts = oracle.parse_ts(timestamp)
            ingest = (oracle.EPOCH + oracle.timedelta(milliseconds=ts)).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.records["events"].append([ident, user, timestamp, ingest, kind, product, price])

    def solve(self):
        adversarial.write_dataset(self.root / "input", self.records, 123)
        return oracle.solve(self.root / "input")

    def test_empty_tables(self):
        self.assertEqual(self.solve(), {"user_daily": [], "campaign_daily": []})

    def test_global_dedup_invalid_winner_changes_user(self):
        self.event("same", "invalid", ingest="2028-02-23T00:00:00Z")
        self.event("same", "2028-02-23T00:00:00Z", user="u2", ingest="2028-02-23T00:00:01Z")
        self.assertEqual(self.solve()["user_daily"], [])

    def test_removed_bridge_does_not_join_sessions(self):
        self.event("a", "2028-02-23T10:00:00Z")
        self.event("b", "2028-02-23T10:30:00Z", kind="purchase", price="0")
        self.event("c", "2028-02-23T11:00:00Z")
        row = self.solve()["user_daily"][0]
        self.assertEqual((row["sessions"], row["events"]), (2, 2))

    def test_linked_bridge_and_unused_minimum(self):
        self.records["links"] = [["u1", "u2"], ["u2", "!unused"]]
        self.event("a", "2028-02-23T10:00:00Z")
        self.event("b", "2028-02-23T10:30:00Z", user="u2")
        self.event("c", "2028-02-23T11:00:00Z")
        row = self.solve()["user_daily"][0]
        self.assertEqual((row["user_id"], row["sessions"], row["events"]), ("!unused", 1, 3))

    def test_cross_midnight_assists_and_mid_session_click(self):
        self.event("a", "2028-02-23T23:50:00Z")
        self.event("b", "2028-02-24T00:10:00Z", kind="purchase", price="500")
        self.records["clicks"] = [
            ["k1", "u1", "c_1", "2028-02-23T22:00:00Z"],
            ["k2", "u1", "c_2", "2028-02-23T23:00:00Z"],
            ["k3", "u1", "c_3", "2028-02-24T00:00:00Z"],
        ]
        tables = self.solve()
        row = tables["user_daily"][0]
        self.assertEqual((row["day"], row["sessions"], row["revenue_cents"]), ("2028-02-23", 1, 500))
        campaigns = {row["campaign_id"]: row for row in tables["campaign_daily"]}
        self.assertNotIn("c_3", campaigns)
        self.assertEqual(campaigns["c_1"]["sessions"], 0)
        self.assertEqual(campaigns["c_1"]["assisted_sessions"], 1)
        self.assertEqual(campaigns["c_2"]["revenue_cents"], 500)

    def test_day_top_category_not_session_winner(self):
        for suffix, hour in [("a", 1), ("b", 3)]:
            self.event(suffix + "1", f"2028-02-23T{hour:02}:00:00Z", kind="purchase", price="100", product="p1")
            self.event(suffix + "2", f"2028-02-23T{hour:02}:01:00Z", kind="purchase", price="90", product="p2")
        self.event("c", "2028-02-23T06:00:00Z", kind="purchase", price="30", product="p2")
        row = self.solve()["user_daily"][0]
        self.assertEqual((row["sessions"], row["revenue_cents"], row["top_category"]), (3, 410, "books"))

    def test_tie_click_byte_order_and_distinct_campaign(self):
        self.event("a", "2028-02-23T10:00:00Z", kind="purchase", price="1")
        self.records["clicks"] = [
            ["z", "u1", "c_1", "2028-02-23T10:00:00Z"],
            ["A", "u1", "c_2", "2028-02-23T10:00:00Z"],
            ["b", "u1", "c_2", "2028-02-23T10:00:00Z"],
        ]
        campaigns = {row["campaign_id"]: row for row in self.solve()["campaign_daily"]}
        self.assertEqual(campaigns["c_1"]["sessions"], 0)
        self.assertEqual(campaigns["c_2"]["sessions"], 1)
        self.assertEqual(campaigns["c_2"]["assisted_sessions"], 1)

    def test_one_click_assists_closed_week_and_no_later(self):
        start = oracle.parse_ts("2028-02-23T00:00:00Z")
        self.event("a", str(start), user="early")
        self.event("b", str(start + oracle.WEEK), user="exact")
        self.event("c", str(start + oracle.WEEK + 1), user="outside")
        for user in ["early", "exact", "outside"]:
            self.records["clicks"].append([user, user, "c_1", str(start)])
        rows = self.solve()["campaign_daily"]
        nonorganic = [row for row in rows if row["campaign_id"] == "c_1"]
        organic = [row for row in rows if row["campaign_id"] == "organic"]
        self.assertEqual(sum(row["assisted_sessions"] for row in nonorganic), 2)
        self.assertEqual(sum(row["sessions"] for row in organic), 1)

    def test_cr_changes_purchase_but_not_view(self):
        self.event("a", "2028-02-23T00:00:00Z", price="100\r")
        self.event("b", "2028-02-23T00:00:00Z", kind="purchase", price="100\r")
        row = self.solve()["user_daily"][0]
        self.assertEqual((row["events"], row["revenue_cents"], row["top_category"]), (1, 0, None))

    def test_late_exact_and_negative_ingest(self):
        self.event("a", "2028-02-23T00:00:00Z", ingest="2028-02-24T00:00:00Z")
        self.event("b", "2028-02-23T00:00:00Z", ingest="2028-02-24T00:00:01Z")
        self.event("c", "2028-02-23T00:00:00Z", ingest="2028-02-22T00:00:00Z")
        self.assertEqual(self.solve()["user_daily"][0]["events"], 2)

    def test_csv_round_trip(self):
        self.event("a", "2028-02-23T00:00:00Z")
        result = self.solve()
        oracle.write_tables(result, self.root / "output")
        self.assertEqual(oracle.load_tables(self.root / "output"), result)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow optional")
    def test_parquet_round_trip_and_empty_schema(self):
        for empty in [True, False]:
            if not empty:
                self.event("a", "2028-02-23T00:00:00Z")
            result = self.solve()
            folder = self.root / ("empty" if empty else "nonempty")
            oracle.write_tables(result, folder, "parquet")
            self.assertEqual(oracle.load_tables(folder), result)

    @unittest.skipUnless(importlib.util.find_spec("pyarrow"), "pyarrow optional")
    def test_parquet_rejects_noninteger_metrics(self):
        import pyarrow as pa
        import pyarrow.parquet as pq
        self.event("a", "2028-02-23T00:00:00Z")
        result = self.solve()
        folder = self.root / "invalid_schema"
        oracle.write_tables(result, folder, "parquet")
        path = folder / "user_daily/part-00000.parquet"
        data = pq.ParquetFile(path).read()
        index = data.column_names.index("sessions")
        data = data.set_column(index, "sessions", pa.array([1.25], type=pa.float64()))
        pq.write_table(data, path)
        with self.assertRaisesRegex(ValueError, "not an integer column"):
            oracle.load_tables(folder)


class FixtureTests(unittest.TestCase):
    def test_all_official_fixtures(self):
        folder = (Path(__file__).resolve().parents[2] / "databricks-shopping-sessions" /
                  "extracted/starter-duckdb/starter-duckdb/fixtures")
        if not folder.exists():
            self.skipTest("Official fixtures are not present")
        cases = sorted(path for path in folder.iterdir() if (path / "input").is_dir())
        self.assertEqual(len(cases), 36)
        for case in cases:
            with self.subTest(fixture=case.name):
                self.assertEqual(oracle.solve(case / "input"), oracle.load_tables(case / "expected"))


class RandomCombinationTests(unittest.TestCase):
    def check_invariants(self, tables):
        user_days, campaign_days = defaultdict(lambda: [0, 0]), defaultdict(lambda: [0, 0])
        for row in tables["user_daily"]:
            self.assertGreaterEqual(row["events"], row["sessions"])
            self.assertGreater(row["sessions"], 0)
            self.assertEqual(row["top_category"] is None, row["revenue_cents"] == 0)
            user_days[row["day"]][0] += row["sessions"]
            user_days[row["day"]][1] += row["revenue_cents"]
        for row in tables["campaign_daily"]:
            self.assertLessEqual(row["purchasing_sessions"], row["sessions"])
            self.assertEqual(row["revenue_cents"] > 0, row["purchasing_sessions"] > 0)
            self.assertGreater(row["sessions"] + row["assisted_sessions"], 0)
            if row["campaign_id"] == "organic":
                self.assertEqual(row["assisted_sessions"], 0)
            else:
                self.assertGreaterEqual(row["assisted_sessions"], row["sessions"])
            campaign_days[row["day"]][0] += row["sessions"]
            campaign_days[row["day"]][1] += row["revenue_cents"]
        self.assertEqual(dict(user_days), dict(campaign_days))


def randomized_test(seed):
    def test(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            expected = adversarial.make_case(root, seed, write_expected=False)
            transformed = oracle.solve(root / "equivalent")
            self.assertEqual(expected, transformed)
            self.check_invariants(expected)
    return test


for _seed in range(120):
    setattr(RandomCombinationTests, f"test_randomized_{_seed:03}", randomized_test(_seed))


if __name__ == "__main__":
    unittest.main(verbosity=2)
