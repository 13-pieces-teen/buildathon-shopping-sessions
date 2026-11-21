#!/usr/bin/env python3
"""Deterministic small, dirty datasets for oracle/optimized-code differential tests.

  python tests/adversarial.py --output DIR --cases 120

Each case contains input/, equivalent/ (semantically identical shuffled and
duplicated representation), and expected/ CSVs computed by the independent
oracle. This is a test-data generator, not an alternative competition engine.
"""
from __future__ import annotations

import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

import oracle

HEADERS = {
    "events": "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents",
    "clicks": "click_id,user_id,campaign_id,ts",
    "links": "user_id_a,user_id_b",
    "products": "product_id,category",
    "campaigns": "campaign_id,channel",
}
START = datetime(2028, 2, 23)
END = START + timedelta(days=14)
EPOCH = datetime(1970, 1, 1)
DAY = 86_400_000
WEEK = DAY * 7


def millis(value: datetime) -> int:
    delta = value - EPOCH
    return (delta.days * 86_400 + delta.seconds) * 1000 + delta.microseconds // 1000


def f1(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp(value: datetime, rng: random.Random) -> str:
    mode = rng.randrange(4)
    if value.microsecond or mode >= 2:
        return "0" * rng.choice([0, 1, 8, 40, 5000]) + str(millis(value))
    if mode == 0:
        return f1(value)
    offset_minutes = rng.choice([-1439, -600, -60, 0, 330, 720, 1439])
    local = value + timedelta(minutes=offset_minutes)
    hours, minutes = divmod(abs(offset_minutes), 60)
    sign = "+" if offset_minutes >= 0 else "-"
    return local.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{hours:02}:{minutes:02}"


def encode_row(values: list[str], rng: random.Random) -> bytes:
    # All semantic fields here satisfy the no comma/quote/LF contract.
    result = []
    for value in values:
        if any(character in value for character in ',"\n'):
            raise ValueError(f"Invalid generated semantic field {value!r}")
        result.append(f'"{value}"' if rng.random() < 0.45 else value)
    return ",".join(result).encode("utf-8") + b"\n"


def semantic_records(seed: int):
    rng = random.Random(seed)
    users = ["u1", "u2", "U1", " u1", "u1 ", "用户甲", "é", "😀", "u\r1"]
    products = [["p1", "shoes"], ["p2", "books"], ["p3", "é"], ["p4", "😀"]]
    campaigns = [["c_1", "search"], ["c_2", "social"], ["c_3", "email"]]
    links = [
        ["u1", "alias1"], ["alias1", "alias2"], ["alias2", "\tunused_min"],
        ["用户甲", "é"], ["", "u2"], ["u2", ""], ["u1", "u1"],
        ["user_id_a", "user_id_b"],  # Header text used as an ordinary data row.
    ]
    if seed % 3 == 0:
        links += [["u1", "shared"], ["shared", "u2"], ["shared", "😀"]]
    if seed % 5 == 0:
        for i in range(450):
            links.append([f"chain{i}", f"chain{i + 1}"])
        links += [["chain450", "u2"], ["!unused_chain_min", "chain0"]]
    all_users = users + ["alias1", "alias2", "user_id_a", "user_id_b"]
    anchors = [START, START + timedelta(days=2, hours=23, minutes=50),
               START + timedelta(days=6), END - timedelta(milliseconds=1)]
    bad_times = ["", "1791071890000.0", "1.79e12", "-5", "٢٥٠", "２５０",
                 "2028-02-30T00:00:00Z", "2027-02-29T00:00:00Z",
                 "2028-02-29t00:00:00Z", "2028-02-29T00:00:00z",
                 "2028-02-29T00:00:00.000Z", "2028-02-29T24:00:00Z",
                 "2028-02-29T00:00:60Z", "2028-02-29T00:00:00+24:00",
                 "2028-02-29T00:00:00+05:60", "9" * 5000]
    bad_prices = ["", "-1", "+1", "0", "1.0", "1e3", "10000001", "２５０", "٢٥٠", "100\r"]
    events = []
    for index in range(80):
        instant = rng.choice(anchors) + timedelta(milliseconds=rng.choice([
            -1, 0, 1, 600_000, 1_200_000, 1_800_000, 1_800_001, 3_600_000,
            DAY - 1, DAY, DAY + 1,
        ]))
        kind = rng.choice(["view", "view", "cart", "purchase", "purchase"])
        row = [
            f"event{index}", rng.choice(all_users), stamp(instant, rng),
            f1(instant + timedelta(milliseconds=rng.choice([-DAY, 0, DAY, DAY + 1000]))),
            kind, rng.choice(["p1", "p2", "p3", "p4", "missing", " p1"]),
            rng.choice(["1", "500", "10000000", "0" * 5000 + "250"]),
        ]
        if rng.random() < 0.15:
            row[2] = rng.choice(bad_times)
        if rng.random() < 0.20:
            row[6] = rng.choice(bad_prices)
        if rng.random() < 0.06:
            row[1] = ""
        if rng.random() < 0.06:
            row[4] = rng.choice(["Purchase", "cart ", " view", "unknown"])
        if rng.random() < 0.05:
            row[5] = ""
        events.append(row)
        if index % 7 == 0:
            duplicate = list(row)
            duplicate[1] = rng.choice(all_users)
            duplicate[3] = f1(datetime.strptime(row[3], "%Y-%m-%dT%H:%M:%SZ") + timedelta(seconds=120))
            duplicate[2] = stamp(instant, rng)
            events.append(duplicate)

    # Reliable boundaries and values survive regardless of random dirt.
    for index, instant in enumerate([
        START, START + timedelta(minutes=30), START + timedelta(minutes=60, milliseconds=1),
        START + timedelta(days=6), END - timedelta(milliseconds=1), END,
    ]):
        events.append([f"boundary{index}", "boundary_user", str(millis(instant)),
                       f1(instant), "purchase", "p1" if index % 2 else "p2", "500"])
    events.extend([
        ["bad_winner", "u1", "bad", f1(START), "view", "p1", ""],
        ["bad_winner", "u2", f1(START), f1(START + timedelta(seconds=1)), "view", "p1", ""],
        ["cr_view", "u1", f1(START), f1(START), "view", "p1", "1\r"],
        ["cr_purchase", "u1", f1(START), f1(START), "purchase", "p1", "1\r"],
        ["", "u1", f1(START), f1(START), "view", "p1", ""],
    ])

    clicks = []
    for index in range(60):
        instant = rng.choice(anchors) + timedelta(milliseconds=rng.choice([
            -WEEK - 1, -WEEK, -DAY, -1, 0, 1, 1_800_000, 1_800_001, DAY,
        ]))
        row = [f"click{index}", rng.choice(all_users), rng.choice(["c_1", "c_2", "c_3"]), stamp(instant, rng)]
        if index % 13 == 0:
            row[3] = rng.choice(bad_times)
        if index % 17 == 0:
            row[2] = rng.choice(["missing", " c_1", "", "C_1"])
        if index % 19 == 0:
            row[1] = ""
        clicks.append(row)
        if index % 6 == 0:
            clicks.append(list(row))
    for index, instant in enumerate([START - timedelta(days=7, milliseconds=1),
                                    START - timedelta(days=7), START,
                                    START + timedelta(milliseconds=1), END]):
        clicks.append([f"boundary_click{index}", "boundary_user", "c_1", str(millis(instant))])
    clicks += [["Z_tie", "boundary_user", "c_2", str(millis(START))],
               ["a_tie", "boundary_user", "c_3", str(millis(START))],
               ["cr_click", "u1", "c_1", f1(START) + "\r"],
               ["", "u1", "c_1", f1(START)]]
    return dict(events=events, clicks=clicks, links=links, products=products, campaigns=campaigns)


def write_dataset(target: Path, records: dict, seed: int, equivalent: bool = False):
    target.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    (target / "manifest.json").write_text(json.dumps({
        "window_start": f1(START), "window_end": f1(END),
    }), encoding="utf-8")
    for name in HEADERS:
        rows = [list(row) for row in records[name]]
        if equivalent and name == "links":
            rows += [[right, left] for left, right in rows]
            rows += [[left, left] for left, right in rows if left and right]
        if equivalent and name == "clicks":
            rows += [list(row) for row in rows[::3]]
        if equivalent and name == "events":
            # Same ID, later ingest, deliberately different user/invalid ts:
            # these cannot rescue or change an existing earlier winner.
            for original in rows[::4]:
                late = list(original)
                late[1] = "changed_late_user"
                late[2] = "bad_late_timestamp"
                late[3] = f1(datetime.strptime(original[3], "%Y-%m-%dT%H:%M:%SZ") + timedelta(days=3))
                rows.append(late)
        rng.shuffle(rows)
        if name in {"products", "campaigns"}:
            (target / f"{name}.csv").write_bytes(
                HEADERS[name].encode() + b"\n" + b"".join(encode_row(row, rng) for row in rows)
            )
            continue
        directory = target / name
        directory.mkdir(parents=True, exist_ok=True)
        count = rng.randint(2, 6)
        parts = [bytearray(HEADERS[name].encode() + b"\n") for _ in range(count)]
        for row in rows:
            # Last file intentionally holds only the header.
            parts[rng.randrange(count - 1)].extend(encode_row(row, rng))
        # Unambiguously malformed rows with wrong field counts/broken quotes.
        parts[0].extend(b'\nonly_one_field\n"broken\n')
        if name == "events":
            parts[0].extend(b'a,b,c,d,e,f,g,h\n')
        elif name == "clicks":
            parts[0].extend(b'a,b,c,d,e\n')
        else:
            parts[0].extend(b'a,b,c\na,b"\n')
        for index, part in enumerate(parts):
            (directory / f"part-{index:03}.csv").write_bytes(part)


def make_case(root: Path, seed: int, write_expected: bool = True) -> dict:
    records = semantic_records(seed)
    write_dataset(root / "input", records, seed * 2)
    write_dataset(root / "equivalent", records, seed * 2 + 1, equivalent=True)
    expected = oracle.solve(root / "input")
    if write_expected:
        oracle.write_tables(expected, root / "expected")
    return expected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cases", type=int, default=120)
    parser.add_argument("--first-seed", type=int, default=0)
    args = parser.parse_args()
    for seed in range(args.first_seed, args.first_seed + args.cases):
        root = args.output / f"case-{seed:04}"
        expected = make_case(root, seed)
        errors = oracle.differences(expected, oracle.solve(root / "equivalent"))
        if errors:
            raise AssertionError(f"Metamorphic mismatch for seed {seed}: {errors}")
    print(f"Generated {args.cases} cases and verified equivalent variants in {args.output}")


if __name__ == "__main__":
    main()
