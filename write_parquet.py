"""Convert the native engine's column buffers into Parquet without Python row loops."""
import concurrent.futures
import os
from pathlib import Path
import struct
import sys
import time

import pyarrow as pa
import pyarrow.parquet as pq


def convert(source: Path, output: Path):
    with pa.memory_map(str(source), "r") as mapped:
        if mapped.read(8) != b"SSCOL01\0":
            raise ValueError(f"bad column file: {source}")
        n = struct.unpack("<Q", mapped.read(8))[0]

        def number(dtype):
            width = 4 if pa.types.is_date32(dtype) else dtype.bit_width // 8
            values = mapped.read_buffer(n * width) if n else pa.py_buffer(b"")
            return pa.Array.from_buffers(dtype, n, [None, values])

        def string(nullable=False):
            size = struct.unpack("<Q", mapped.read(8))[0]
            offsets = mapped.read_buffer((n + 1) * 4)
            # A zero-byte memory-map read can expose a null pointer; Arrow string
            # arrays still require a non-null data buffer when every value is null.
            values = mapped.read_buffer(size) if size else pa.py_buffer(b"")
            valid = mapped.read_buffer((n + 7) // 8) if nullable else None
            return pa.Array.from_buffers(pa.string(), n, [valid, offsets, values])

        if source.name.startswith("user-"):
            table = pa.Table.from_arrays(
                [string(), number(pa.date32()), number(pa.int64()), number(pa.int64()), number(pa.int64()), string(True)],
                names=["user_id", "day", "sessions", "events", "revenue_cents", "top_category"],
            )
            folder = output / "user_daily"
        else:
            table = pa.Table.from_arrays(
                [string(), string(), number(pa.date32()), number(pa.int64()), number(pa.int64()), number(pa.int64()), number(pa.int64())],
                names=["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"],
            )
            folder = output / "campaign_daily"
        if mapped.tell() != mapped.size():
            raise ValueError(f"trailing bytes: {source}")
        folder.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, folder / (source.stem + ".parquet"), compression="snappy", write_statistics=False, row_group_size=262144)
        return n


def main():
    start = time.monotonic()
    columns, output = map(Path, sys.argv[1:3])
    files = sorted(columns.glob("*.bin"))
    if not files:
        raise ValueError("native engine produced no column files")
    workers = max(1, int(os.environ.get("NCPU", "8")))
    pa.set_cpu_count(workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        rows = sum(pool.map(lambda p: convert(p, output), files))
    print(f"parquet seconds={time.monotonic()-start:.6f} rows={rows} files={len(files)}", file=sys.stderr)


if __name__ == "__main__":
    main()
