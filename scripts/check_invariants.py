#!/usr/bin/env python3
"""Check necessary output consistency conditions with columnar PyArrow operations.

These invariants are NOT a proof of exact row correctness. They do not compare
against an independently computed answer or verify attribution from raw inputs.

  python scripts/check_invariants.py OUTPUT --expected-events 19564682
  python scripts/check_invariants.py OUTPUT --json REPORT.json

Print one JSON report and exit nonzero on failed checks or unreadable output.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq


SCHEMAS = {
    "user_daily": pa.schema([
        ("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
        ("events", pa.int64()), ("revenue_cents", pa.int64()), ("top_category", pa.string()),
    ]),
    "campaign_daily": pa.schema([
        ("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
        ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
        ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64()),
    ]),
}
SCOPE = (
    "Necessary consistency checks only; PASS does not establish exact row correctness, "
    "correct identity resolution, session boundaries, or attribution."
)


def supported_string(data_type):
    if pa.types.is_dictionary(data_type):
        data_type = data_type.value_type
    return pa.types.is_string(data_type) or pa.types.is_large_string(data_type)


def count_true(condition) -> int:
    return int(pc.sum(pc.cast(pc.fill_null(condition, False), pa.int64())).as_py() or 0)


def load_table(output: Path, name: str):
    schema = SCHEMAS[name]
    paths = sorted(
        path for path in (output / name).glob("*.parquet")
        if path.is_file() and not path.is_symlink() and not path.name.startswith((".", "_"))
    )
    if not paths:
        raise ValueError(f"{name}: no visible Parquet files directly in output directory")
    parts = []
    for path in paths:
        part = pq.ParquetFile(path).read()
        if len(part.column_names) != len(schema.names) or set(part.column_names) != set(schema.names):
            raise ValueError(f"{path}: incorrect column names {part.column_names!r}")
        for actual in part.schema:
            expected = schema.field(actual.name).type
            if pa.types.is_integer(expected):
                valid = pa.types.is_integer(actual.type)
            elif pa.types.is_string(expected):
                valid = supported_string(actual.type)
            else:
                valid = actual.type == pa.date32()
            if not valid:
                raise ValueError(f"{path}: wrong type for {actual.name}: {actual.type}")
        # Safe casts preserve values and let differently sized integer/string
        # columns from separate files be checked together.
        parts.append(part.select(schema.names).cast(schema, safe=True))
    return pa.concat_tables(parts), len(paths)


def check_output(output: str | Path, expected_events: int | None = None) -> dict:
    started = time.perf_counter()
    output = Path(output).resolve()
    report = {
        "status": "PASS", "scope": SCOPE, "output": str(output),
        "expected_events": expected_events, "tables": {}, "checks": [], "errors": [],
    }

    def check(name: str, violations: int, **details):
        report["checks"].append(dict(name=name, passed=violations == 0,
                                      violations=violations, **details))
        if violations:
            report["status"] = "FAIL"

    tables = {}
    for name, schema in SCHEMAS.items():
        try:
            table, file_count = load_table(output, name)
            tables[name] = table
            totals = {
                field.name: int(pc.sum(table[field.name]).as_py() or 0)
                for field in schema if pa.types.is_integer(field.type)
            }
            report["tables"][name] = dict(rows=table.num_rows, files=file_count, totals=totals)
            check(f"{name}.schema", 0)
            required = [column for column in schema.names if column != "top_category"]
            nulls = {column: table[column].null_count for column in required}
            check(f"{name}.required_nonnull", sum(nulls.values()), null_counts=nulls)
            key = "user_id" if name == "user_daily" else "campaign_id"
            check(f"{name}.nonempty_id", count_true(pc.equal(table[key], "")))
            groups = table.group_by([key, "day"]).aggregate([([], "count_all")])
            duplicate_keys = count_true(pc.greater(groups["count_all"], 1))
            duplicate_extra_rows = table.num_rows - groups.num_rows
            check(f"{name}.unique_primary_key", duplicate_extra_rows,
                  duplicate_keys=duplicate_keys)
            for field in schema:
                if pa.types.is_integer(field.type):
                    check(f"{name}.{field.name}_nonnegative", count_true(pc.less(table[field.name], 0)))
        except Exception as error:
            report["status"] = "FAIL"
            report["errors"].append(f"{name}: {type(error).__name__}: {error}")

    try:
        if "user_daily" in tables:
            users = tables["user_daily"]
            check("user_daily.sessions_at_least_one", count_true(pc.less(users["sessions"], 1)))
            check("user_daily.events_at_least_sessions", count_true(pc.less(users["events"], users["sessions"])))
            null_category = pc.is_null(users["top_category"])
            zero_revenue = pc.equal(users["revenue_cents"], 0)
            check("user_daily.category_null_iff_zero_revenue", count_true(pc.not_equal(null_category, zero_revenue)))
            if expected_events is not None:
                actual_events = int(pc.sum(users["events"]).as_py() or 0)
                check("user_daily.expected_event_total", int(actual_events != expected_events),
                      actual=actual_events, expected=expected_events)
        if "campaign_daily" in tables:
            campaigns = tables["campaign_daily"]
            check("campaign_daily.purchasing_at_most_sessions", count_true(
                pc.greater(campaigns["purchasing_sessions"], campaigns["sessions"])))
            check("campaign_daily.positive_revenue_iff_purchasing", count_true(pc.not_equal(
                pc.greater(campaigns["revenue_cents"], 0),
                pc.greater(campaigns["purchasing_sessions"], 0))))
            organic = pc.equal(campaigns["campaign_id"], "organic")
            check("campaign_daily.nonorganic_assists_at_least_sessions", count_true(pc.and_(
                pc.invert(organic), pc.less(campaigns["assisted_sessions"], campaigns["sessions"])) ))
            check("campaign_daily.organic_assists_zero", count_true(pc.and_(
                organic, pc.not_equal(campaigns["assisted_sessions"], 0))))
            check("campaign_daily.organic_channel", count_true(pc.and_(
                organic, pc.not_equal(campaigns["channel"], "organic"))))
            # Avoid adding two arbitrary int64 values merely to test positivity.
            # Nonnegativity is checked separately above.
            check("campaign_daily.has_credit_or_assist", count_true(pc.and_(
                pc.less_equal(campaigns["sessions"], 0),
                pc.less_equal(campaigns["assisted_sessions"], 0))))
        if set(tables) == set(SCHEMAS):
            daily = {}
            for name, table in tables.items():
                aggregated = table.group_by(["day"]).aggregate([
                    ("sessions", "sum"), ("revenue_cents", "sum"),
                ])
                # Only the small list of daily totals is materialized in Python.
                daily[name] = {
                    str(row["day"]): (row["sessions_sum"], row["revenue_cents_sum"])
                    for row in aggregated.to_pylist()
                }
            all_days = sorted(set(daily["user_daily"]) | set(daily["campaign_daily"]))
            report["daily_totals"] = []
            differences = 0
            for day in all_days:
                user = daily["user_daily"].get(day, (0, 0))
                campaign = daily["campaign_daily"].get(day, (0, 0))
                same = user == campaign
                differences += not same
                report["daily_totals"].append(dict(
                    day=day, user_sessions=user[0], campaign_sessions=campaign[0],
                    user_revenue_cents=user[1], campaign_revenue_cents=campaign[1], match=same,
                ))
            check("cross_table.daily_sessions_and_revenue", differences)
    except Exception as error:
        report["status"] = "FAIL"
        report["errors"].append(f"invariants: {type(error).__name__}: {error}")
    report["elapsed_seconds"] = round(time.perf_counter() - started, 6)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--expected-events", type=int)
    parser.add_argument("--json", type=Path, help="Also save the printed JSON report to this path")
    args = parser.parse_args()
    if args.expected_events is not None and args.expected_events < 0:
        parser.error("--expected-events must be nonnegative")
    report = check_output(args.output, args.expected_events)
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
