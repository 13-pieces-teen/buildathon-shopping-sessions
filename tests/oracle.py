#!/usr/bin/env python3
"""Independent, deliberately slow reference for the Shopping Sessions rules.

The calculation uses only Python's standard library. It scans every person's
clicks for every session instead of sharing the optimized program's algorithms.
Optional Parquet I/O imports pyarrow only at the I/O boundary.

  python tests/oracle.py DATA OUTPUT --format csv
  python tests/oracle.py DATA OUTPUT --format parquet
  python tests/oracle.py --fixtures PATH_TO_OFFICIAL_FIXTURES
  python tests/oracle.py --compare EXPECTED ACTUAL

Use small generated datasets: this oracle deliberately retains records and
sessions, and its naive attribution is not intended for competition-sized data.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

DAY = 86_400_000
WEEK = 7 * DAY
GAP = 1_800_000
EPOCH = datetime(1970, 1, 1)
ASCII_DIGITS = re.compile(r"[0-9]+\Z")
ISO = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T"
    r"([0-9]{2}):([0-9]{2}):([0-9]{2})"
    r"(Z|([+-])([0-9]{2}):([0-9]{2}))\Z"
)
SCHEMAS = {
    "user_daily": (
        "user_id", "day", "sessions", "events", "revenue_cents", "top_category"
    ),
    "campaign_daily": (
        "campaign_id", "channel", "day", "sessions", "purchasing_sessions",
        "revenue_cents", "assisted_sessions"
    ),
}
NUMERIC = {
    "sessions", "events", "revenue_cents", "purchasing_sessions", "assisted_sessions"
}


def byte_key(value: str) -> bytes:
    return value.encode("utf-8")


def parse_line(line: bytes, columns: int) -> list[str] | None:
    """Parse one physical LF-delimited record, retaining CR and whitespace."""
    if line.endswith(b"\n"):
        line = line[:-1]
    parts = line.split(b",")
    if len(parts) != columns:
        return None
    result = []
    for part in parts:
        if b'"' in part:
            if len(part) < 2 or part[:1] != b'"' or part[-1:] != b'"':
                return None
            part = part[1:-1]
            if b'"' in part:
                return None
        result.append(part.decode("utf-8"))
    return result


def read_records(paths: Iterable[Path], columns: int):
    for path in sorted(paths):
        with path.open("rb") as stream:
            next(stream, None)  # Only the first physical line is a header.
            for physical in stream:
                record = parse_line(physical, columns)
                if record is not None:
                    yield record


def parse_ts(value: str) -> int | None:
    if ASCII_DIGITS.fullmatch(value):
        normalized = value.lstrip("0") or "0"
        # Such values cannot be in a supported manifest window. Avoid Python's
        # decimal-string conversion limit, including adversarial 10,000 digits.
        if len(normalized) > 19:
            return None
        return int(normalized)
    match = ISO.fullmatch(value)
    if match is None:
        return None
    year, month, day, hour, minute, second = map(int, match.group(1, 2, 3, 4, 5, 6))
    try:
        local = datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None
    delta = local - EPOCH
    milliseconds = (delta.days * 86_400 + delta.seconds) * 1000
    if match.group(7) != "Z":
        offset_hour, offset_minute = int(match.group(9)), int(match.group(10))
        if offset_hour > 23 or offset_minute > 59:
            return None
        offset = (offset_hour * 60 + offset_minute) * 60_000
        milliseconds -= offset if match.group(8) == "+" else -offset
    return milliseconds


def purchase_price(value: str) -> int | None:
    if ASCII_DIGITS.fullmatch(value) is None:
        return None
    normalized = value.lstrip("0") or "0"
    if len(normalized) > 8:
        return None
    number = int(normalized)
    return number if 1 <= number <= 10_000_000 else None


class IdGroups:
    """A small standard-library union-find, with an explicit byte-minimum name."""

    def __init__(self):
        self.parent: dict[str, str] = {}
        self.size: dict[str, int] = {}
        self.minimum: dict[str, str] = {}

    def find(self, item: str) -> str:
        if item not in self.parent:
            self.parent[item] = item
            self.size[item] = 1
            self.minimum[item] = item
        root = item
        while self.parent[root] != root:
            root = self.parent[root]
        while item != root:
            parent = self.parent[item]
            self.parent[item] = root
            item = parent
        return root

    def join(self, left: str, right: str):
        a, b = self.find(left), self.find(right)
        if a == b:
            return
        if self.size[a] < self.size[b]:
            a, b = b, a
        self.parent[b] = a
        self.size[a] += self.size[b]
        self.minimum[a] = min(self.minimum[a], self.minimum[b], key=byte_key)

    def name(self, item: str) -> str:
        return self.minimum[self.find(item)]


def solve(data: str | Path) -> dict[str, list[dict]]:
    data = Path(data)
    manifest = json.loads((data / "manifest.json").read_text(encoding="utf-8"))
    start, end = parse_ts(manifest["window_start"]), parse_ts(manifest["window_end"])
    if start is None or end is None or start >= end:
        raise ValueError("Invalid manifest window")
    products = dict(read_records([data / "products.csv"], 2))
    campaigns = dict(read_records([data / "campaigns.csv"], 2))
    identities = IdGroups()
    for left, right in read_records((data / "links").glob("*.csv"), 2):
        if left and right:
            identities.join(left, right)

    winners: dict[str, tuple[int, list[str]]] = {}
    for event in read_records((data / "events").glob("*.csv"), 7):
        event_id = event[0]
        if not event_id:
            continue
        ingest = parse_ts(event[3])
        if ingest is None:
            raise ValueError("Input contract violated: ingest_ts must be valid F1")
        old = winners.get(event_id)
        if old is None or ingest < old[0]:
            winners[event_id] = (ingest, event)

    # Record tuple: (timestamp, purchase cents or zero, purchase category or None).
    per_person: dict[str, list[tuple[int, int, str | None]]] = defaultdict(list)
    for ingest, event in winners.values():
        _, user, stamp, _, kind, product, raw_price = event
        if not user or kind not in {"view", "cart", "purchase"} or not product:
            continue
        ts = parse_ts(stamp)
        if ts is None or not start <= ts < end or ingest - ts > DAY:
            continue
        price = 0
        category = None
        if kind == "purchase":
            price = purchase_price(raw_price)
            if price is None:
                continue
            category = products.get(product, "unknown")
        per_person[identities.name(user)].append((ts, price, category))

    unique_clicks: dict[str, list[str]] = {}
    for click in read_records((data / "clicks").glob("*.csv"), 4):
        if click[0]:
            unique_clicks.setdefault(click[0], click)
    person_clicks: dict[str, list[tuple[int, str, str]]] = defaultdict(list)
    for click_id, user, campaign, stamp in unique_clicks.values():
        ts = parse_ts(stamp)
        if user and campaign in campaigns and ts is not None and start - WEEK <= ts < end:
            person_clicks[identities.name(user)].append((ts, click_id, campaign))

    user_days: dict[tuple[str, str], dict] = {}
    categories: dict[tuple[str, str], Counter] = defaultdict(Counter)
    campaign_days: dict[tuple[str, str], dict] = {}

    def campaign_day(campaign: str, day: str) -> dict:
        key = campaign, day
        if key not in campaign_days:
            campaign_days[key] = dict(
                campaign_id=campaign,
                channel="organic" if campaign == "organic" else campaigns[campaign],
                day=day, sessions=0, purchasing_sessions=0, revenue_cents=0,
                assisted_sessions=0,
            )
        return campaign_days[key]

    def finish(person: str, session: list[tuple[int, int, str | None]]):
        beginning = session[0][0]
        day = (EPOCH + timedelta(milliseconds=beginning)).date().isoformat()
        key = person, day
        row = user_days.setdefault(key, dict(
            user_id=person, day=day, sessions=0, events=0, revenue_cents=0,
            top_category=None,
        ))
        revenue = sum(record[1] for record in session)
        row["sessions"] += 1
        row["events"] += len(session)
        row["revenue_cents"] += revenue
        for _, price, category in session:
            if category is not None:
                categories[key][category] += price

        # Deliberately naive reference: exact predicate over the full click set.
        candidates = [
            click for click in person_clicks[person]
            if beginning - WEEK <= click[0] <= beginning
        ]
        credit = "organic"
        if candidates:
            credit = min(candidates, key=lambda c: (-c[0], byte_key(c[1])))[2]
        credited = campaign_day(credit, day)
        credited["sessions"] += 1
        credited["purchasing_sessions"] += int(revenue > 0)
        credited["revenue_cents"] += revenue
        for campaign in {click[2] for click in candidates}:
            campaign_day(campaign, day)["assisted_sessions"] += 1

    for person, events in per_person.items():
        events.sort(key=lambda record: record[0])
        session = []
        for event in events:
            if session and event[0] - session[-1][0] > GAP:
                finish(person, session)
                session = []
            session.append(event)
        if session:
            finish(person, session)
    for key, amounts in categories.items():
        user_days[key]["top_category"] = min(
            amounts, key=lambda category: (-amounts[category], byte_key(category))
        )
    result = {"user_daily": list(user_days.values()), "campaign_daily": list(campaign_days.values())}
    return normalize_tables(result)


def normalize_tables(tables: dict[str, list[dict]]) -> dict[str, list[dict]]:
    result = {}
    for table, columns in SCHEMAS.items():
        key = "user_id" if table == "user_daily" else "campaign_id"
        rows = []
        for original in tables[table]:
            if set(original) != set(columns):
                raise ValueError(f"{table}: expected exactly columns {columns}, got {tuple(original)}")
            row = dict(original)
            if isinstance(row["day"], date):
                row["day"] = row["day"].isoformat()
            for column in NUMERIC & row.keys():
                row[column] = int(row[column])
            rows.append(row)
        rows.sort(key=lambda row: (byte_key(row[key]), row["day"]))
        if len({(row[key], row["day"]) for row in rows}) != len(rows):
            raise ValueError(f"{table}: duplicate primary key")
        result[table] = rows
    return result


def load_tables(folder: str | Path) -> dict[str, list[dict]]:
    folder = Path(folder)
    tables = {}
    for table, columns in SCHEMAS.items():
        csv_path = folder / f"{table}.csv"
        if csv_path.exists():
            with csv_path.open("r", encoding="utf-8", newline="") as stream:
                reader = csv.DictReader(stream)
                if (set(reader.fieldnames or ()) != set(columns) or
                        len(reader.fieldnames or ()) != len(columns)):
                    raise ValueError(f"{csv_path}: incorrect columns")
                tables[table] = list(reader)
                for row in tables[table]:
                    if table == "user_daily" and row["top_category"] == "":
                        row["top_category"] = None
        else:
            import pyarrow as pa
            import pyarrow.parquet as pq
            files = [
                path for path in sorted((folder / table).glob("*.parquet"))
                if not path.is_symlink() and not path.name.startswith((".", "_"))
            ]
            if not files:
                raise ValueError(f"{folder / table}: no visible Parquet files")
            tables[table] = []
            for path in files:
                data = pq.ParquetFile(path).read()
                if set(data.column_names) != set(columns) or len(data.column_names) != len(columns):
                    raise ValueError(f"{path}: incorrect columns")
                if not pa.types.is_date(data.schema.field("day").type):
                    raise ValueError(f"{path}: day is not Parquet DATE")
                for column in NUMERIC & set(columns):
                    if not pa.types.is_integer(data.schema.field(column).type):
                        raise ValueError(f"{path}: {column} is not an integer column")
                tables[table].extend(data.to_pylist())
    return normalize_tables(tables)


def write_tables(tables: dict[str, list[dict]], folder: str | Path, fmt="csv"):
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    for table, columns in SCHEMAS.items():
        if fmt == "csv":
            with (folder / f"{table}.csv").open("w", encoding="utf-8", newline="") as stream:
                writer = csv.DictWriter(stream, fieldnames=columns, lineterminator="\n")
                writer.writeheader()
                writer.writerows(tables[table])
        elif fmt == "parquet":
            import pyarrow as pa
            import pyarrow.parquet as pq
            target = folder / table
            target.mkdir(parents=True, exist_ok=True)
            schema = pa.schema([
                pa.field(column, pa.date32() if column == "day" else
                         pa.int64() if column in NUMERIC else pa.string(),
                         nullable=column == "top_category")
                for column in columns
            ])
            rows = [dict(row, day=date.fromisoformat(row["day"])) for row in tables[table]]
            pq.write_table(pa.Table.from_pylist(rows, schema=schema), target / "part-00000.parquet")
        else:
            raise ValueError(f"Unsupported output format {fmt!r}")


def differences(expected: dict, actual: dict) -> list[str]:
    errors = []
    for table, columns in SCHEMAS.items():
        a = Counter(tuple(row[c] for c in columns) for row in expected[table])
        b = Counter(tuple(row[c] for c in columns) for row in actual[table])
        missing, extra = a - b, b - a
        if missing or extra:
            errors.append(f"{table}: missing {sum(missing.values())}, extra {sum(extra.values())}; "
                          f"sample missing={list(missing)[:3]!r}; sample extra={list(extra)[:3]!r}")
    return errors


def check_fixtures(folder: str | Path) -> int:
    cases = sorted(path for path in Path(folder).iterdir() if (path / "input").is_dir())
    if not cases:
        raise ValueError(f"No fixtures found in {folder}")
    failed = 0
    for case in cases:
        errors = differences(load_tables(case / "expected"), solve(case / "input"))
        print(f"{case.name}: {'FAIL' if errors else 'PASS'}")
        for error in errors:
            print(error)
        failed += bool(errors)
    print(f"Fixtures: {len(cases) - failed}/{len(cases)} passed")
    return int(bool(failed))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("data", nargs="?")
    parser.add_argument("output", nargs="?")
    parser.add_argument("--format", choices=("csv", "parquet"), default="csv")
    parser.add_argument("--fixtures", type=Path)
    parser.add_argument("--compare", nargs=2, metavar=("EXPECTED", "ACTUAL"))
    args = parser.parse_args()
    if args.fixtures:
        return check_fixtures(args.fixtures)
    if args.compare:
        errors = differences(*(load_tables(folder) for folder in args.compare))
        print("\n".join(errors) if errors else "MATCH")
        return int(bool(errors))
    if not args.data or not args.output:
        parser.error("Specify DATA OUTPUT, --fixtures FOLDER, or --compare EXPECTED ACTUAL")
    tables = solve(args.data)
    write_tables(tables, args.output, args.format)
    print(json.dumps({table: len(rows) for table, rows in tables.items()}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
