#!/usr/bin/env python3
"""Small independent regression cases for one-pass event ingestion changes.

No solver code is imported. Each case has hand-written expected output; the
unchanged Python oracle must agree before fixtures or a solver run are accepted.

Quick oracle/expectation checks (no container, no generated large datasets):
    python tests/test_ingest_optimization.py
Generate fixtures for scripts/check_fixtures.py:
    python tests/test_ingest_optimization.py --generate /path/to/fixtures
Run a supplied complete pipeline with DATA_DIR/OUTPUT_DIR/SCRATCH_DIR:
    python tests/test_ingest_optimization.py --run-command /app/run.sh

Set NCPU/PARTITIONS outside this script to exercise different concurrency and
partition boundaries. --run-command uses argv directly, never a shell string.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

import oracle

HEADERS = {
    "events": b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n",
    "clicks": b"click_id,user_id,campaign_id,ts\n",
    "links": b"user_id_a,user_id_b\n",
}
TS = "2026-10-03T10:00:00Z"
EARLY = "2026-10-03T10:01:00Z"
LATE = "2026-10-03T10:05:00Z"


def csv_line(values, quoted=False):
    """Generate legal records without changing NUL/CR/tab/non-ASCII values."""
    parts = []
    for value in values:
        if any(character in value for character in ',"\n'):
            raise ValueError("Use explicit bytes to construct malformed records")
        parts.append(f'"{value}"' if quoted else value)
    return (",".join(parts) + "\n").encode("utf-8")


def event(ident="same", user="winner", ts=TS, ingest=EARLY,
          kind="purchase", product="p1", price="250", quoted=False):
    return csv_line([ident, user, ts, ingest, kind, product, price], quoted)


def user_row(user="winner", events=1, revenue=250, sessions=1,
             day="2026-10-03", category="shoes"):
    return dict(user_id=user, day=day, sessions=sessions, events=events,
                revenue_cents=revenue, top_category=category)


def campaign_row(campaign="organic", sessions=1, revenue=250, purchasing=1,
                 assists=0, day="2026-10-03"):
    channels = {"organic": "organic", "c_1": "search", "c_2": "social"}
    return dict(campaign_id=campaign, channel=channels[campaign], day=day,
                sessions=sessions, purchasing_sessions=purchasing,
                revenue_cents=revenue, assisted_sessions=assists)


def tables(users=(), campaigns=()):
    return oracle.normalize_tables({"user_daily": list(users), "campaign_daily": list(campaigns)})


@dataclass
class Case:
    name: str
    events: list[bytes]
    expected: dict
    clicks: list[bytes] = field(default_factory=list)
    links: list[bytes] = field(default_factory=list)

    def write(self, folder: Path):
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "manifest.json").write_text(json.dumps({
            "window_start": "2026-10-01T00:00:00Z", "window_end": "2026-10-15T00:00:00Z",
        }), encoding="utf-8")
        (folder / "products.csv").write_bytes(b"product_id,category\np1,shoes\np2,books\n")
        (folder / "campaigns.csv").write_bytes(b"campaign_id,channel\nc_1,search\nc_2,social\n")
        for name in HEADERS:
            target = folder / name
            target.mkdir(exist_ok=True)
            rows = getattr(self, name)
            # Interleave duplicate groups across files; include a header-only
            # part and let the caller reverse physical rows as a second variant.
            for index in range(3):
                selected = rows[index::2] if index < 2 else []
                (target / f"part-{index:03}.csv").write_bytes(HEADERS[name] + b"".join(selected))


def build_cases():
    result = []
    invalid_earliest = {
        "empty_user": dict(user=""),
        "empty_ts": dict(ts=""),
        "invalid_calendar": dict(ts="2026-02-30T10:00:00Z"),
        "out_of_window_before": dict(ts="2026-09-30T23:59:59Z"),
        "out_of_window_end": dict(ts="2026-10-15T00:00:00Z"),
        "type_case": dict(kind="Purchase"),
        "type_trailing_space": dict(kind="purchase "),
        "empty_product": dict(product=""),
        "empty_price": dict(price=""),
        "zero_price": dict(price="0"),
        "over_price_limit": dict(price="10000001"),
        "unicode_digits": dict(price="２５０"),
        "cr_price": dict(price="250\r"),
        "too_late": dict(ingest="2026-10-04T10:00:01Z"),
    }
    for name, bad in invalid_earliest.items():
        earliest = event(quoted=True, **bad)
        rescue = event(user="changed_later_user", ts="2026-10-05T10:00:00Z",
                       ingest="2026-10-05T10:01:00Z", price="999")
        result.append(Case("bad_first_" + name, [rescue, earliest], tables()))

    result.append(Case("valid_first_later_changes_user_and_is_bad", [
        event(user="different", ingest=LATE, kind="Purchase", price="999"), event(),
    ], tables([user_row()], [campaign_row()])))
    result.append(Case("valid_first_later_changes_user_and_value", [
        event(user="different", ingest=LATE, price="999"), event(quoted=True),
    ], tables([user_row()], [campaign_row()])))
    result.append(Case("equal_ingest_identical_different_quoting", [
        event(quoted=False), event(quoted=True), event(quoted=False),
    ], tables([user_row()], [campaign_row()])))
    for name, malformed in {
        "missing_field": b"same,winner,bad,2026-10-03T09:00:00Z,purchase,p1\n",
        "extra_field": b"same,winner,bad,2026-10-03T09:00:00Z,purchase,p1,250,extra\n",
        "broken_quote": b'same,winner,bad,2026-10-03T09:00:00Z,purchase,p1,250"\n',
    }.items():
        result.append(Case("malformed_first_" + name, [malformed, event(ingest=LATE)],
                           tables([user_row()], [campaign_row()])))

    ids = [
        "a", "e", "E", " e", "e ", "e\r", "e\t", "e\x00", "e\x00suffix",
        "é", "用戶", "😀", "é"*8, "é"*8+"x", "😀"*4, "😀"*4+"x",
        "x"*15, "x"*16, "x"*16+"A", "x"*16+"B",
        "x"*31, "x"*32, "x"*33, "x"*127, "x"*128, "x"*129,
        "x"*255, "x"*256, "x"*257, "p"*4096+"A", "p"*4096+"B",
    ]
    rows = []
    for index, ident in enumerate(ids):
        rows.extend([event(ident, user="wrong_later", ingest=LATE, price="999"),
                     event(ident, user="arbitrary_ids", price=str(index+1), quoted=True)])
    revenue = len(ids)*(len(ids)+1)//2
    result.append(Case("full_utf8_id_identity_and_inline_boundaries", rows,
                       tables([user_row("arbitrary_ids", len(ids), revenue)],
                              [campaign_row(revenue=revenue)])))
    prefix = "a"*4096
    result.append(Case("long_ids_invalid_winner_not_prefix_collision", [
        event(prefix+"A", user="rescued", ingest=LATE),
        event(prefix+"A", ts="bad", quoted=True),
        event(prefix+"B", user="long_survivor", quoted=True),
    ], tables([user_row("long_survivor")], [campaign_row()])))
    result.append(Case("empty_quoted_id_not_real_id", [
        event(ident="", quoted=True), event(ident="", quoted=False),
        event(ident="0", user="zero"), event(ident=" ", user="space", quoted=True),
    ], tables([user_row("zero"), user_row("space")],
              [campaign_row(sessions=2, revenue=500, purchasing=2)])))
    result.append(Case("case_and_control_byte_users_stay_distinct", [
        event(f"id{i}", user=user, price="1", quoted=bool(i%2))
        for i, user in enumerate(["u", "U", " u", "u ", "u\r", "u\x00", "用户", "😀"])
    ], tables([user_row(user, revenue=1) for user in ["u", "U", " u", "u ", "u\r", "u\x00", "用户", "😀"]],
              [campaign_row(sessions=8, revenue=8, purchasing=8)])))

    # This is one connected person, including an unused canonical ID. Events
    # from several IDs bridge each other, and the duplicate's later alias must
    # not survive a second time. Both campaigns assist through linked identities.
    result.append(Case("shared_identity_and_changed_alias_duplicate", [
        event("first", "alias_b", "2026-10-03T10:00:00Z", LATE, price="999"),
        event("first", "alias_a", "2026-10-03T10:00:00Z", EARLY, price="100"),
        event("bridge", "alias_b", "2026-10-03T10:30:00Z", "2026-10-03T10:30:00Z", price="200", product="p2"),
        event("end", "alias_c", "2026-10-03T11:00:00Z", "2026-10-03T11:00:00Z", kind="view", price="not_a_price"),
    ], tables([user_row("\tunused", events=3, revenue=300, category="books")], [
        campaign_row("c_1", sessions=0, revenue=0, purchasing=0, assists=1),
        campaign_row("c_2", revenue=300, assists=1),
    ]), clicks=[
        csv_line(["k1", "alias_a", "c_1", "2026-10-03T08:00:00Z"]),
        csv_line(["k2", "alias_c", "c_2", "2026-10-03T09:00:00Z"], True),
    ], links=[csv_line(pair, True) for pair in [
        ["alias_a", "shared"], ["shared", "alias_b"], ["alias_c", "shared"],
        ["\tunused", "alias_c"], ["shared", "shared"],
    ]]))
    result.append(Case("invalid_linked_winner_not_rescued", [
        event(user="alias_b", ingest=LATE), event(user="alias_a", ts="bad"),
    ], tables(), links=[csv_line(["alias_a", "alias_b"])]))
    result.append(Case("empty_user_cannot_be_rescued_through_links", [
        event(user="", quoted=True), event(user="alias_b", ingest=LATE),
    ], tables(), links=[csv_line(["", "alias_b"]), csv_line(["alias_b", "alias_a"])]))
    result.append(Case("price_ignored_for_view_but_csv_syntax_still_matters", [
        event("view", kind="view", price="not numeric\r"),
        event("cart", kind="cart", price="٢٥٠"),
        b'malformed,winner,2026-10-03T10:00:00Z,2026-10-03T10:01:00Z,view,p1,x"\n',
    ], tables([user_row(events=2, revenue=0, category=None)],
              [campaign_row(revenue=0, purchasing=0)])))
    result.append(Case("valid_first_then_late_bad_price", [
        event(price="000250"), event(ingest=LATE, price="0"),
    ], tables([user_row()], [campaign_row()])))
    result.append(Case("same_person_cannot_be_partitioned_by_raw_alias", [
        event("a", "alias_a", "2026-10-03T23:50:00Z", "2026-10-03T23:50:00Z", price="100"),
        event("b", "alias_b", "2026-10-04T00:10:00Z", "2026-10-04T00:10:00Z", price="200", product="p2"),
    ], tables([user_row("alias_a", events=2, revenue=300, category="books")],
              [campaign_row(revenue=300)]), links=[csv_line(["alias_a", "alias_b"])]))
    result.append(Case("linked_canonical_name_retains_nul_and_utf8_bytes", [
        event("a", "用户", "2026-10-03T10:00:00Z", "2026-10-03T10:00:00Z", price="125"),
        event("b", "😀", "2026-10-03T10:10:00Z", "2026-10-03T10:10:00Z", price="125"),
        event("c", "alias\rtail", "2026-10-03T10:20:00Z", "2026-10-03T10:20:00Z", price="125"),
    ], tables([user_row("\x00minimum", events=3, revenue=375)],
              [campaign_row("c_2", revenue=375, assists=1)]),
        links=[csv_line(pair, True) for pair in [
            ["\x00minimum", "用户"], ["用户", "😀"], ["😀", "alias\rtail"],
        ]], clicks=[csv_line(["nul_click", "\x00minimum", "c_2", "2026-10-03T09:59:00Z"], True)]))

    # Both variants have the same hand-derived answer, but winner encounter order
    # and cross-file placement differ. Reversal also exercises shared graph order.
    expanded = []
    for case in result:
        expanded.append(case)
        expanded.append(Case(case.name+"_reversed", list(reversed(case.events)), case.expected,
                             list(reversed(case.clicks)), list(reversed(case.links))))
    return expanded


def verify_case(case: Case, folder: Path):
    case.write(folder)
    computed = oracle.solve(folder)
    differences = oracle.differences(case.expected, computed)
    if differences:
        raise AssertionError(f"Hand expectation/oracle mismatch in {case.name}: {differences}")


def run_cases(command=None, generate=None):
    cases = build_cases()
    started = time.perf_counter()
    records = []
    scratch_root = os.environ.get("SCRATCH_DIR")
    if scratch_root and not Path(scratch_root).is_dir():
        scratch_root = None
    with tempfile.TemporaryDirectory(prefix="ingest-regression-", dir=scratch_root) as temporary:
        temporary = Path(temporary)
        for case in cases:
            root = (generate / case.name) if generate else (temporary / case.name)
            if generate and root.exists():
                raise ValueError(f"Refusing to mix fixture data with an existing case directory: {root}")
            data = root / "input"
            verify_case(case, data)
            if generate:
                oracle.write_tables(case.expected, root / "expected")
            record = dict(case=case.name, oracle_matches_manual=True)
            if command:
                output, scratch = root / "out", root / "scratch"
                output.mkdir(); scratch.mkdir()
                env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=str(output), SCRATCH_DIR=str(scratch))
                run = subprocess.run(command, env=env, capture_output=True, timeout=30)
                record["exit_code"] = run.returncode
                if run.returncode:
                    record["differences"] = ["pipeline returned nonzero"]
                else:
                    try:
                        record["differences"] = oracle.differences(case.expected, oracle.load_tables(output))
                    except Exception as error:
                        record["differences"] = [f"output unreadable: {type(error).__name__}: {error}"]
                record["passed"] = not record["differences"]
                if not record["passed"]:
                    record["stderr"] = run.stderr.decode("utf-8", errors="replace")[-4000:]
            else:
                record["passed"] = True
            records.append(record)
            if not record["passed"]:
                print(json.dumps(record, ensure_ascii=True), flush=True)
    summary = dict(status="PASS" if all(r["passed"] for r in records) else "FAIL",
                   cases=len(records), passed=sum(r["passed"] for r in records),
                   pipeline_executed=bool(command), seconds=round(time.perf_counter()-started, 3),
                   threads=os.environ.get("NCPU"), partitions=os.environ.get("PARTITIONS"),
                   failed=[record for record in records if not record["passed"]])
    print(json.dumps(summary, ensure_ascii=True), flush=True)
    return 0 if summary["status"] == "PASS" else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generate", type=Path)
    parser.add_argument("--run-command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.run_command == []:
        parser.error("--run-command requires an executable")
    if args.generate:
        args.generate = args.generate.resolve()
    return run_cases(args.run_command, args.generate)


if __name__ == "__main__":
    raise SystemExit(main())
