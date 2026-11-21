#!/usr/bin/env python3
"""Disk-sharded independent reference verification, not an official answer.

Example (native Python with PyArrow):
  python full_oracle_check.py --data DATA --actual OUTPUT --work CHECKPOINT_DIR

Container: mount this script, /tests/oracle.py, DATA and OUTPUT read-only, and
CHECKPOINT_DIR writable; pass --oracle /tests/oracle.py explicitly. The solver
and its intermediate files are never imported or read. Resume by repeating the
same command. Checkpoints are durable completed work, NOT evidence of a live job.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date, datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
import zlib

VERSION = 1
START = time.monotonic()
EVENT_HEADER = b"event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents\n"
CLICK_HEADER = b"click_id,user_id,campaign_id,ts\n"
LINK_HEADER = b"user_id_a,user_id_b\n"
METADATA = ("manifest.json", "products.csv", "campaigns.csv")


def atomic_json(path: Path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        json.dump(value, stream, ensure_ascii=True, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)


def read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def progress(work: Path, event: str, **fields):
    record = dict(time_utc=datetime.now(timezone.utc).isoformat(), event=event,
                  process_elapsed_s=round(time.monotonic()-START, 3), **fields)
    # This append-only log is informational. Only atomic *.done.json files are
    # resume authorities; parallel workers can interleave console/log messages.
    line = json.dumps(record, ensure_ascii=True, separators=(",", ":"))
    with (work / "progress.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(line + "\n")
    print(line, flush=True)


def oracle_module(path: Path):
    spec = importlib.util.spec_from_file_location("independent_shopping_oracle", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def sha256(path: Path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024*1024), b""):
            digest.update(block)
    return digest.hexdigest()


def inventory(root: Path, paths):
    result = []
    for path in paths:
        stat = path.stat()
        result.append(dict(path=str(path.relative_to(root)), size=stat.st_size,
                           mtime_ns=stat.st_mtime_ns))
    return result


def parquet_files(folder: Path):
    if folder.is_symlink() or not folder.is_dir():
        raise ValueError(f"Expected real output directory: {folder}")
    files = [p for p in sorted(folder.glob("*.parquet"))
             if p.is_file() and not p.is_symlink() and not p.name.startswith((".", "_"))]
    if not files:
        raise ValueError(f"No visible Parquet files: {folder}")
    return files


@contextmanager
def exclusive_lock(work: Path):
    # OS locks are released on process death. The file can remain after a crash;
    # its presence and recorded PID never count as proof that a job is alive.
    path = work / "coordinator.lock"
    with path.open("a+b") as stream:
        if path.stat().st_size == 0:
            stream.write(b"0"); stream.flush()
        stream.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as error:
                raise RuntimeError("Another coordinator holds this work directory's OS lock") from error
        else:
            import fcntl
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise RuntimeError("Another coordinator holds this work directory's OS lock") from error
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def partition(person: str, count: int):
    return zlib.crc32(person.encode("utf-8")) % count if person else 0


def encode_record(record):
    # The rules exclude delimiters/quotes/LF from field values. CR, tabs, spaces,
    # NUL and arbitrary UTF-8 bytes must remain untouched.
    if any(any(char in value for char in ',"\n') for value in record):
        raise ValueError("Cannot losslessly re-encode a parsed field")
    return (",".join(record) + "\n").encode("utf-8")


class FragmentWriter:
    """Write one restartable unit into per-person shards; commit files last."""
    def __init__(self, work, count, folder, name, header=b""):
        self.work, self.count, self.folder, self.name = work, count, folder, name
        self.header, self.handles, self.counts = header, {}, [0]*count
        # Only these explicitly generated paths belong to this unit.
        for index in range(count):
            target = self.target(index)
            target.unlink(missing_ok=True)
            target.with_name(target.name + ".tmp").unlink(missing_ok=True)

    def target(self, index):
        return self.work / "persons" / f"shard-{index:04d}" / self.folder / self.name

    def write(self, index, payload):
        if index not in self.handles:
            target = self.target(index)
            target.parent.mkdir(parents=True, exist_ok=True)
            stream = target.with_name(target.name + ".tmp").open("wb", buffering=1024*1024)
            stream.write(self.header)
            self.handles[index] = stream
        self.handles[index].write(payload)
        self.counts[index] += 1

    def finish(self):
        artifacts = []
        for index, stream in self.handles.items():
            stream.flush(); os.fsync(stream.fileno()); stream.close()
            target = self.target(index)
            os.replace(target.with_name(target.name + ".tmp"), target)
            artifacts.append(dict(path=str(target.relative_to(self.work)), size=target.stat().st_size,
                                  rows=self.counts[index]))
        self.handles.clear()
        return artifacts

    def close(self):
        for stream in self.handles.values():
            stream.close()
        self.handles.clear()


def done(work, relative):
    path = work / "checkpoints" / relative
    if not path.exists():
        return None
    value = read_json(path)
    for artifact in value.get("artifacts", []):
        output = work / artifact["path"]
        if not output.is_file() or output.stat().st_size != artifact["size"]:
            raise ValueError(f"Completed checkpoint artifact missing/changed: {output}")
    return value


def save_done(work, relative, **fields):
    value = dict(completed_utc=datetime.now(timezone.utc).isoformat(), **fields)
    atomic_json(work / "checkpoints" / relative, value)
    return value


def prepare(args, oracle):
    work, source = args.work, args.data
    for index in range(args.person_shards):
        target = work / "persons" / f"shard-{index:04d}" / "input"
        target.mkdir(parents=True, exist_ok=True)
        for name in METADATA:
            shutil.copyfile(source / name, target / name)
        for folder, header in (("events", EVENT_HEADER), ("clicks", CLICK_HEADER), ("links", LINK_HEADER)):
            (target / folder).mkdir(exist_ok=True)
            (target / folder / "empty.csv").write_bytes(header)
    completed = done(work, "event-buckets.done.json")
    if completed:
        for index in range(args.event_buckets):
            path = work / "event-buckets" / f"bucket-{index:04d}.csv"
            routed = done(work, f"route-event-{index:04d}.done.json")
            if not routed and (not path.is_file() or path.stat().st_size != completed["sizes"][index]):
                raise ValueError(f"Unconsumed event bucket missing/changed: {path}")
        return completed
    folder = work / "event-buckets"; folder.mkdir(exist_ok=True)
    paths = [folder / f"bucket-{index:04d}.csv" for index in range(args.event_buckets)]
    streams = [path.open("wb", buffering=1024*1024) for path in paths]
    counts = [0]*len(streams); raw = malformed = empty_id = 0
    try:
        for stream in streams:
            stream.write(EVENT_HEADER)
        for path in sorted((source / "events").glob("*.csv")):
            with path.open("rb") as stream:
                next(stream, None)
                for physical in stream:
                    raw += 1
                    row = oracle.parse_line(physical, 7)
                    if row is None:
                        malformed += 1; continue
                    if not row[0]:
                        empty_id += 1; continue
                    index = partition(row[0], args.event_buckets)
                    # Parsing already proved this record structurally valid.
                    # Preserve original quoting rather than decoding/re-encoding
                    # every field a second time during the large first pass.
                    streams[index].write(physical if physical.endswith(b"\n") else physical+b"\n")
                    counts[index] += 1
            progress(work, "event_source_bucketed", file=path.name, physical_rows=raw,
                     routed_rows=sum(counts), malformed=malformed, empty_id=empty_id)
        for stream in streams:
            stream.flush(); os.fsync(stream.fileno())
    finally:
        for stream in streams:
            stream.close()
    assert raw == malformed + empty_id + sum(counts)
    return save_done(work, "event-buckets.done.json", raw=raw, malformed=malformed,
                     empty_id=empty_id, rows=sum(counts), counts=counts,
                     sizes=[path.stat().st_size for path in paths])


def route(args, oracle):
    work = args.work
    buckets = done(work, "event-buckets.done.json")
    if not buckets:
        raise ValueError("Run prepare before route")
    click_files = sorted((args.data / "clicks").glob("*.csv"))
    pending_events = [i for i in range(args.event_buckets) if not done(work, f"route-event-{i:04d}.done.json")]
    pending_clicks = [i for i in range(len(click_files)) if not done(work, f"route-click-{i:04d}.done.json")]
    if not pending_events and not pending_clicks:
        progress(work, "routing_already_complete"); return
    identities = oracle.IdGroups()
    link_raw = link_bad = link_empty = link_good = 0
    for path in sorted((args.data / "links").glob("*.csv")):
        with path.open("rb") as stream:
            next(stream, None)
            for physical in stream:
                link_raw += 1; row = oracle.parse_line(physical, 2)
                if row is None:
                    link_bad += 1
                elif not row[0] or not row[1]:
                    link_empty += 1
                else:
                    identities.join(*row); link_good += 1
    assert link_raw == link_bad + link_empty + link_good
    progress(work, "independent_links_rebuilt", rows=link_raw, valid=link_good,
             malformed=link_bad, empty_side=link_empty, linked_ids=len(identities.parent))

    def canonical(user):
        # Empty user is INVALID, not an identity to rescue. A missing nonempty
        # ID is already its singleton name; avoid inflating the DSU with it.
        return identities.name(user) if user and user in identities.parent else user

    for index in pending_events:
        started = time.monotonic()
        path = work / "event-buckets" / f"bucket-{index:04d}.csv"
        winners = {}; rows = duplicates = 0
        for event in oracle.read_records([path], 7):
            rows += 1; ingest = oracle.parse_ts(event[3])
            if ingest is None:
                raise ValueError("Input contract violated: invalid ingest_ts")
            old = winners.get(event[0])
            if old is not None:
                duplicates += 1
                if ingest == old[0] and event != old[1]:
                    raise ValueError("Input contract violated: equal-ingest copies differ")
            if old is None or ingest < old[0]:
                winners[event[0]] = ingest, event
        assert rows == buckets["counts"][index]
        assert rows == len(winners) + duplicates
        writer = FragmentWriter(work, args.person_shards, "input/events", f"event-{index:04d}.csv", EVENT_HEADER)
        empty_users = 0
        try:
            for _, event in winners.values():
                event[1] = canonical(event[1])
                empty_users += not bool(event[1])
                writer.write(partition(event[1], args.person_shards), encode_record(event))
            artifacts = writer.finish()
        finally:
            writer.close()
        assert sum(writer.counts) == len(winners)
        save_done(work, f"route-event-{index:04d}.done.json", raw_rows=rows,
                  winners=len(winners), duplicates=duplicates, empty_users=empty_users,
                  counts=writer.counts, artifacts=artifacts)
        # Delete only this explicitly generated, durably routed bucket.
        path.unlink()
        progress(work, "event_bucket_routed", bucket=index, rows=rows, winners=len(winners),
                 duplicates=duplicates, empty_users=empty_users, elapsed_s=round(time.monotonic()-started, 3))
        del winners
    for index in pending_clicks:
        path = click_files[index]; raw = malformed = 0
        writer = FragmentWriter(work, args.person_shards, "input/clicks", f"click-{index:04d}.csv", CLICK_HEADER)
        try:
            with path.open("rb") as stream:
                next(stream, None)
                for physical in stream:
                    raw += 1; click = oracle.parse_line(physical, 4)
                    if click is None:
                        malformed += 1; continue
                    click[1] = canonical(click[1])
                    writer.write(partition(click[1], args.person_shards), encode_record(click))
            artifacts = writer.finish()
        finally:
            writer.close()
        assert raw == malformed + sum(writer.counts)
        save_done(work, f"route-click-{index:04d}.done.json", raw_rows=raw, malformed=malformed,
                  rows=sum(writer.counts), counts=writer.counts, artifacts=artifacts)
        progress(work, "click_source_routed", file=path.name, rows=raw, malformed=malformed)
    # This entire routing subprocess exits before any oracle.solve worker starts.


def validate_schema(schema, table, oracle, path):
    import pyarrow as pa
    columns = oracle.SCHEMAS[table]
    if set(schema.names) != set(columns) or len(schema.names) != len(columns):
        raise ValueError(f"{path}: wrong columns")
    if not pa.types.is_date32(schema.field("day").type):
        raise ValueError(f"{path}: day must be Parquet DATE/date32")
    for column in columns:
        kind = schema.field(column).type
        if column in oracle.NUMERIC:
            if not pa.types.is_integer(kind):
                raise ValueError(f"{path}: {column} must be integer")
        elif column != "day":
            if pa.types.is_dictionary(kind):
                kind = kind.value_type
            if not (pa.types.is_string(kind) or pa.types.is_large_string(kind)):
                raise ValueError(f"{path}: {column} must be a string type")


def normalize_actual(row, table, oracle):
    for column in oracle.SCHEMAS[table]:
        value = row[column]
        if value is None:
            if table != "user_daily" or column != "top_category":
                raise ValueError(f"{table}: unexpected null in {column}")
        elif column == "day":
            if not isinstance(value, date) or isinstance(value, datetime):
                raise ValueError("Non-date value in day column")
            row[column] = value.isoformat()
        elif column in oracle.NUMERIC:
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"{column}: non-integer value")
        elif not isinstance(value, str):
            raise ValueError(f"{column}: non-string value")
    return row


def actual(args, oracle):
    import pyarrow.parquet as pq
    work = args.work
    files = parquet_files(args.actual / "user_daily")
    for index, path in enumerate(files):
        if done(work, f"actual-user-{index:04d}.done.json"):
            continue
        parquet = pq.ParquetFile(path)
        validate_schema(parquet.schema_arrow, "user_daily", oracle, path)
        writer = FragmentWriter(work, args.person_shards, "actual", f"user-{index:04d}.jsonl")
        read_rows = 0
        try:
            for batch in parquet.iter_batches(batch_size=16384):
                for original in batch.to_pylist():
                    row = normalize_actual(original, "user_daily", oracle)
                    writer.write(partition(row["user_id"], args.person_shards),
                                 (json.dumps(row, ensure_ascii=True, separators=(",", ":"))+"\n").encode())
                    read_rows += 1
            artifacts = writer.finish()
        finally:
            writer.close()
        assert read_rows == parquet.metadata.num_rows == sum(writer.counts)
        save_done(work, f"actual-user-{index:04d}.done.json", rows=read_rows,
                  metadata_rows=parquet.metadata.num_rows, counts=writer.counts, artifacts=artifacts)
        progress(work, "actual_user_file_partitioned", file=path.name, rows=read_rows)
    # Validate campaign schema now too, before any JSON transformations.
    count = 0
    for path in parquet_files(args.actual / "campaign_daily"):
        parquet = pq.ParquetFile(path)
        validate_schema(parquet.schema_arrow, "campaign_daily", oracle, path)
        count += parquet.metadata.num_rows
    save_done(work, "actual-campaign-schema.done.json", metadata_rows=count)


def summarize(rows):
    return {name: sum(row.get(name, 0) for row in rows)
            for name in ("sessions", "events", "revenue_cents", "purchasing_sessions", "assisted_sessions")}


def conservation_plan(args):
    counts = {name:[0]*args.person_shards for name in ("events", "clicks", "actual")}
    files = [{"events":["empty.csv"], "clicks":["empty.csv"], "actual":[]}
             for _ in range(args.person_shards)]
    groups = (("events", "route-event", args.event_buckets),
              ("clicks", "route-click", len(list((args.data/"clicks").glob("*.csv")))),
              ("actual", "actual-user", len(parquet_files(args.actual/"user_daily"))))
    for kind, prefix, units in groups:
        for index in range(units):
            value = done(args.work, f"{prefix}-{index:04d}.done.json")
            if not value:
                raise ValueError(f"Incomplete {prefix} unit {index}")
            for shard, rows in enumerate(value["counts"]):
                counts[kind][shard] += rows
            for artifact in value["artifacts"]:
                path = Path(artifact["path"])
                shard = int(path.parts[1].removeprefix("shard-"))
                files[shard][kind].append(path.name)
    save_done(args.work, "conservation-plan.done.json", **counts, files=files)


def validate_person_layout(args, index, plan):
    root = args.work/"persons"/f"shard-{index:04d}"
    for kind, relative in (("events", "input/events"), ("clicks", "input/clicks"), ("actual", "actual")):
        folder = root/relative
        entries = list(folder.iterdir()) if folder.exists() else []
        observed = sorted(path.name for path in entries)
        expected = sorted(plan["files"][index][kind])
        if observed != expected or any(not p.is_file() or p.is_symlink() for p in entries):
            raise ValueError(f"Unexpected/missing generated shard files: {folder}")
    links = root/"input/links"
    if sorted(p.name for p in links.iterdir()) != ["empty.csv"] or (links/"empty.csv").read_bytes() != LINK_HEADER:
        raise ValueError(f"Generated shard links must contain only an empty header: {links}")
    config = read_json(args.work/"configuration.json")
    for name, digest in config["metadata_sha256"].items():
        if sha256(root/"input"/name) != digest:
            raise ValueError(f"Generated metadata changed: {root/'input'/name}")


def generated_record_count(folder):
    # Generated records cannot contain LF inside values, including canonical
    # IDs; every file has one LF-terminated header and LF-terminated records.
    records = 0
    for path in folder.glob("*.csv"):
        newlines = 0
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(8*1024*1024), b""):
                newlines += block.count(b"\n")
        if newlines < 1:
            raise ValueError(f"Generated input has no header: {path}")
        records += newlines-1
    return records


def check_person(args, oracle, index):
    work = args.work; marker = f"check-person-{index:04d}.done.json"
    plan = done(work, "conservation-plan.done.json")
    validate_person_layout(args, index, plan)
    if done(work, marker):
        return
    started = time.monotonic()
    root = work / "persons" / f"shard-{index:04d}"
    input_events = generated_record_count(root/"input/events")
    input_clicks = generated_record_count(root/"input/clicks")
    assert input_events == plan["events"][index]
    assert input_clicks == plan["clicks"][index]
    # Deliberately invoke the original independent implementation unchanged.
    reference = oracle.solve(root / "input")
    actual_rows = []
    for path in sorted((root / "actual").glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                assert partition(row["user_id"], args.person_shards) == index
                actual_rows.append(row)
    assert len(actual_rows) == plan["actual"][index]
    columns = oracle.SCHEMAS["user_daily"]
    expected_counter = Counter(tuple(row[c] for c in columns) for row in reference["user_daily"])
    actual_counter = Counter(tuple(row[c] for c in columns) for row in actual_rows)
    missing, extra = expected_counter-actual_counter, actual_counter-expected_counter
    duplicate_keys = len(actual_rows)-len({(row["user_id"], row["day"]) for row in actual_rows})
    for row in reference["user_daily"]:
        assert partition(row["user_id"], args.person_shards) == index
    user_totals, campaign_totals = summarize(reference["user_daily"]), summarize(reference["campaign_daily"])
    assert user_totals["sessions"] == campaign_totals["sessions"]
    assert user_totals["revenue_cents"] == campaign_totals["revenue_cents"]
    nonorganic = sum(row["sessions"] for row in reference["campaign_daily"] if row["campaign_id"] != "organic")
    assert campaign_totals["assisted_sessions"] >= nonorganic
    value = save_done(work, marker, expected_rows=len(reference["user_daily"]),
                      routed_input_events=input_events, routed_input_clicks=input_clicks,
                      actual_rows=len(actual_rows), missing=sum(missing.values()), extra=sum(extra.values()),
                      duplicate_keys=duplicate_keys, missing_sample=list(missing)[:3], extra_sample=list(extra)[:3],
                      expected_totals=user_totals, actual_totals=summarize(actual_rows),
                      campaign_rows=reference["campaign_daily"], elapsed_s=round(time.monotonic()-started, 3))
    progress(work, "person_shard_checked", shard=index, expected_rows=value["expected_rows"],
             actual_rows=value["actual_rows"], missing=value["missing"], extra=value["extra"],
             duplicate_keys=duplicate_keys, elapsed_s=value["elapsed_s"])


def finish(args, oracle):
    import pyarrow.parquet as pq
    configure(args)  # reject source/actual/oracle/tool changes during the run
    work = args.work; merged = {}; checks = []
    numeric = ("sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions")
    for index in range(args.person_shards):
        value = done(work, f"check-person-{index:04d}.done.json")
        if not value:
            raise ValueError(f"Person shard {index} not checked")
        checks.append({k: v for k, v in value.items() if k != "campaign_rows"})
        for row in value["campaign_rows"]:
            key = row["campaign_id"], row["day"]
            if key not in merged:
                merged[key] = row.copy()
            else:
                target = merged[key]
                if target["channel"] != row["channel"]:
                    raise ValueError(f"Inconsistent campaign channel for {key!r}")
                for column in numeric:
                    target[column] += row[column]
    observed = []; metadata_rows = 0
    for path in parquet_files(args.actual / "campaign_daily"):
        parquet = pq.ParquetFile(path)
        validate_schema(parquet.schema_arrow, "campaign_daily", oracle, path)
        metadata_rows += parquet.metadata.num_rows
        for batch in parquet.iter_batches(batch_size=16384):
            observed.extend(normalize_actual(row, "campaign_daily", oracle) for row in batch.to_pylist())
    assert len(observed) == metadata_rows
    columns = oracle.SCHEMAS["campaign_daily"]
    expected = Counter(tuple(row[c] for c in columns) for row in merged.values())
    actual_rows = Counter(tuple(row[c] for c in columns) for row in observed)
    missing, extra = expected-actual_rows, actual_rows-expected
    duplicates = len(observed)-len({(row["campaign_id"], row["day"]) for row in observed})
    route_events = [done(work, f"route-event-{i:04d}.done.json") for i in range(args.event_buckets)]
    route_clicks = [done(work, f"route-click-{i:04d}.done.json") for i, _ in enumerate(sorted((args.data/"clicks").glob("*.csv")))]
    actual_parts = [done(work, f"actual-user-{i:04d}.done.json") for i, _ in enumerate(parquet_files(args.actual/"user_daily"))]
    if any(value is None for value in route_events+route_clicks+actual_parts):
        raise ValueError("Incomplete routing or actual-output checkpoints")
    bucketed = done(work, "event-buckets.done.json")
    assert sum(value["raw_rows"] for value in route_events) == bucketed["rows"]
    assert sum(value["winners"]+value["duplicates"] for value in route_events) == bucketed["rows"]
    assert sum(value["winners"] for value in route_events) == sum(sum(value["counts"]) for value in route_events)
    assert sum(value["rows"] for value in actual_parts) == sum(value["actual_rows"] for value in checks)
    assert sum(value["rows"] for value in actual_parts) == sum(value["metadata_rows"] for value in actual_parts)
    expected_sessions = sum(value["expected_totals"]["sessions"] for value in checks)
    expected_revenue = sum(value["expected_totals"]["revenue_cents"] for value in checks)
    assert expected_sessions == sum(row["sessions"] for row in merged.values())
    assert expected_revenue == sum(row["revenue_cents"] for row in merged.values())
    expected_events = sum(value["expected_totals"]["events"] for value in checks)
    assert expected_events <= sum(value["winners"] for value in route_events)
    failed_users = sum(bool(value["missing"] or value["extra"] or value["duplicate_keys"]) for value in checks)
    report = dict(status="MATCH" if not (failed_users or missing or extra or duplicates) else "DIFFER",
                  authority="independent Python reference verification; not official ground truth",
                  coverage="Full output-row values and staging record-count conservation; no source-row ordinal trace",
                  resume_assumption="Sources/checkpoints remain unchanged. Large source files use path/size/mtime; staged files use exact names/sizes, not content hashes.",
                  source_snapshot_rechecked_at_finish=True,
                  checked_utc=datetime.now(timezone.utc).isoformat(),
                  event_physical_rows=bucketed["raw"], event_bucketed_rows=bucketed["rows"],
                  global_event_winners=sum(value["winners"] for value in route_events),
                  expected_surviving_events=expected_events,
                  expected_user_rows=sum(value["expected_rows"] for value in checks),
                  actual_user_rows=sum(value["actual_rows"] for value in checks),
                  user_missing=sum(value["missing"] for value in checks), user_extra=sum(value["extra"] for value in checks),
                  user_duplicate_keys=sum(value["duplicate_keys"] for value in checks),
                  expected_campaign_rows=len(merged), actual_campaign_rows=len(observed),
                  campaign_missing=sum(missing.values()), campaign_extra=sum(extra.values()),
                  campaign_duplicate_keys=duplicates, campaign_missing_sample=list(missing)[:3],
                  campaign_extra_sample=list(extra)[:3], user_shard_checks=checks)
    configure(args)
    atomic_json(work/"final-report.json", report)
    progress(work, "independent_reference_finished", **{k:v for k,v in report.items() if k != "user_shard_checks"})
    return 0 if report["status"] == "MATCH" else 1


def command(args, phase, shard=None):
    values = [sys.executable, str(Path(__file__).resolve()), "--data", str(args.data),
              "--actual", str(args.actual), "--work", str(args.work), "--oracle", str(args.oracle),
              "--event-buckets", str(args.event_buckets), "--person-shards", str(args.person_shards),
              "--internal-phase", phase]
    if shard is not None:
        values += ["--shard", str(shard)]
    return values


def validate_work_location(args):
    for source in (args.data, args.actual):
        if args.work == source or args.work in source.parents or source in args.work.parents:
            raise ValueError("Work and source directories must be disjoint, with neither containing the other")


def configure(args):
    validate_work_location(args)
    args.work.mkdir(parents=True, exist_ok=True)
    source_files = [args.data/name for name in METADATA]
    for folder in ("events", "clicks", "links"):
        source_files.extend(sorted((args.data/folder).glob("*.csv")))
    actual_files = parquet_files(args.actual/"user_daily") + parquet_files(args.actual/"campaign_daily")
    config = dict(version=VERSION, data=str(args.data), actual=str(args.actual), oracle=str(args.oracle),
                  oracle_sha256=sha256(args.oracle), tool_sha256=sha256(Path(__file__)),
                  event_buckets=args.event_buckets, person_shards=args.person_shards,
                  input_inventory=inventory(args.data, source_files), actual_inventory=inventory(args.actual, actual_files),
                  metadata_sha256={name:sha256(args.data/name) for name in METADATA},
                  fingerprint_note="Large files are bound by relative path, size and mtime_ns; keep sources immutable.")
    path = args.work/"configuration.json"
    if path.exists() and read_json(path) != config:
        raise ValueError("Checkpoint configuration/source inventory changed; use a new work directory")
    if not path.exists():
        atomic_json(path, config)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--actual", type=Path, required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--oracle", type=Path, default=Path(__file__).resolve().parent.parent/"shopping-sessions-solution/tests/oracle.py")
    parser.add_argument("--event-buckets", type=int, default=128)
    parser.add_argument("--person-shards", type=int, default=128)
    parser.add_argument("--workers", type=int, default=1, choices=(1,2), help="One recommended under a 6 GiB limit")
    parser.add_argument("--stage", choices=("all","prepare","route","actual","check"), default="all")
    parser.add_argument("--internal-phase", choices=("route","person"), help=argparse.SUPPRESS)
    parser.add_argument("--shard", type=int, help=argparse.SUPPRESS)
    args = parser.parse_args()
    for name in ("data", "actual", "work", "oracle"):
        setattr(args, name, getattr(args, name).resolve())
    if args.event_buckets<1 or args.person_shards<1:
        parser.error("Bucket and shard counts must be positive")
    if not __debug__:
        parser.error("Do not run with python -O: conservation assertions must remain enabled")
    oracle = oracle_module(args.oracle)
    if args.internal_phase:
        if not os.environ.get("FULL_ORACLE_PARENT_TOKEN"):
            parser.error("Internal phases must be launched by the coordinator")
        if args.internal_phase == "route":
            route(args, oracle)
        else:
            check_person(args, oracle, args.shard)
        return 0
    validate_work_location(args)
    args.work.mkdir(parents=True, exist_ok=True)
    with exclusive_lock(args.work):
        configure(args)
        progress(args.work, "coordinator_started", pid=os.getpid(), stage=args.stage,
                 note="Checkpoint files do not indicate live process state; OS lock is authoritative.")
        env = dict(os.environ, FULL_ORACLE_PARENT_TOKEN=uuid.uuid4().hex)
        if args.stage in ("all","prepare"):
            prepare(args, oracle)
        if args.stage in ("all","route"):
            subprocess.run(command(args,"route"), env=env, check=True)
        if args.stage in ("all","actual"):
            actual(args, oracle)
        if args.stage in ("all","check"):
            for index in range(args.event_buckets):
                if not done(args.work, f"route-event-{index:04d}.done.json"):
                    raise ValueError("Event routing incomplete")
            for index, _ in enumerate(sorted((args.data/"clicks").glob("*.csv"))):
                if not done(args.work, f"route-click-{index:04d}.done.json"):
                    raise ValueError("Click routing incomplete")
            for index, _ in enumerate(parquet_files(args.actual/"user_daily")):
                if not done(args.work, f"actual-user-{index:04d}.done.json"):
                    raise ValueError("Actual Parquet partitioning incomplete")
            conservation_plan(args)
            plan = done(args.work, "conservation-plan.done.json")
            pending = []
            for index in range(args.person_shards):
                validate_person_layout(args, index, plan)
                if not done(args.work, f"check-person-{index:04d}.done.json"):
                    pending.append(index)
            with ThreadPoolExecutor(max_workers=args.workers) as pool:
                list(pool.map(lambda i: subprocess.run(command(args,"person",i), env=env, check=True), pending))
            return finish(args,oracle)
        progress(args.work, "requested_stage_completed", stage=args.stage)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (Exception, KeyboardInterrupt) as error:
        print(f"REFERENCE CHECK STOPPED: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
        raise
