# Shopping Sessions

An exact single-machine implementation of the Databricks Shopping Sessions
Buildathon task. The C++20 engine handles strict CSV parsing, linked identities,
global event deduplication, sessions, last-click attribution and campaign assists.
PyArrow converts native column buffers to Parquet; there are no Python row loops
in the production pipeline.

## Build and run

```sh
docker build --platform linux/amd64 -t shopping-sessions .
mkdir -p out scratch
chmod 777 out scratch
docker run --rm --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 4096 \
  --cpus 8 --memory 24g --memory-swap 24g --user 1000:1000 \
  -v "$PWD/dev:/data:ro" -v "$PWD/out:/output" -v "$PWD/scratch:/scratch" \
  -e NCPU=8 -e MEM_GB=24 -e DATA_DIR=/data -e OUTPUT_DIR=/output \
  -e SCRATCH_DIR=/scratch -e HOME=/scratch shopping-sessions
```

The container has no network dependency at runtime. Its command takes no
arguments, reads the mounted data and writes the two output directories.
`NCPU` controls worker count; `PARTITIONS` defaults to 128. Input and output
locations use the standard environment variables. Diagnostics go to stderr.

## Algorithms

- Input files are mmap-backed. CSV records are split only at LF; values remain
  exact bytes without trimming or case normalization.
- A union-find graph computes connected components and their minimum-byte ID,
  including aliases that never occur in event/click logs.
- Events are hash-partitioned by full event ID, deduplicated to the smallest
  ingest timestamp, and only then validated. Exact key comparisons resolve hash
  collisions. Invalid earliest copies cannot be rescued by later copies.
- Event IDs of at most 16 bytes are stored inline in raw descriptors; longer
  IDs retain exact full-byte comparison. This avoids random source-file paging
  during duplicate checks without using hashes as identity proofs.
- Unlinked person names are interned in sharded, address-stable storage. Linked
  names refer to the much smaller links files. Event CSV mappings can then be
  released after validation instead of remaining live throughout sorting.
- Surviving events and clicks are partitioned by canonical person and sorted.
  A scan constructs sessions and user-day purchase-category revenue totals.
- Last-click attribution merges ordered sessions and clicks, resolving timestamp
  ties by minimum click ID.
- Every click at `t` covers session starts in `[t,t+7 days]`. Per-person,
  per-campaign interval unions plus session-index bounds count assists directly
  by day without constructing a session-by-click join.
- Identical click duplicates are handled idempotently by both last-click choice
  and interval union; a separate full click-ID hash table is unnecessary.
- Native memory is released before PyArrow starts. Column buffers are mapped
  into Arrow arrays and written as Parquet with DATE columns and true nulls.

## Validation

Official rules and test assets:
https://www.joinbuilders.org/buildathon/tracks/shopping-sessions

Download the official fixtures and generator separately; no competition dataset
or expected full-size answer is embedded in the runtime or repository.

```sh
# Native module tests (g++ 12+)
g++ -std=c++20 -O2 -pthread -Isrc tests/aggregate_test.cpp src/aggregate.cpp -o aggregate-test
./aggregate-test

# Independent Python reference, using only stdlib for computation
python tests/oracle.py --fixtures /path/to/official/fixtures
python tests/oracle.py /path/to/input /path/to/expected --format csv
python scripts/compare.py /path/to/actual /path/to/expected

# Random adversarial combinations and equivalent transformed inputs
python tests/adversarial.py --output /path/to/generated-cases --cases 120
```

`scripts/compare.py` requires PyArrow and enforces exact values, nulls, DATE and
integer logical types. `tests/oracle.py` also checks key uniqueness. The oracle
uses independent datetime/DSU/session logic and a deliberately naive click scan.

The final v4 container was validated on 2026-11-21:

- All **36 official fixtures** and the public seed-1 **1M-event/250K-click answer**
  match exactly, including schemas, nulls and both complete output tables.
- All **240 adversarial fixtures** pass against the independent Python oracle:
  120 original inputs and 120 separately executed equivalent transformations.
- All **five generated seeds** (100K events/25K clicks each) match the independent
  Python oracle exactly.
- The optimized aggregation module passes 300 randomized comparisons against
  an independent C++ brute-force oracle. The final native executable ran all
  36 official fixtures under ASan/UBSan with leak checking and no report.
- The complete 20M output matches the original implementation exactly. Two 60M
  repeat outputs match the first successful v4 output exactly; 26 consistency
  checks also pass on that first output.

Local container start-to-exit timings (8 CPU quota, 6 GiB container limit,
Windows bind mounts in a shared Docker Desktop VM; **not official scores**):

| Input | Final v4 runtime | Correctness evidence |
| --- | ---: | --- |
| Public 1M events / 250K clicks | 1.054 s | Exact official answers |
| Generated 20M events / 5M clicks, seed 42 | 10.438 s | Exact match to original implementation; 26 invariants pass on original output |
| Generated 60M events / 15M clicks, seed 20261121 | 54.367 s, 56.189 s | Two exact repeats; 26 consistency checks; no official 60M answer |

Both timed 60M repeats completed within a 180-second timeout. The final image is
Linux/amd64, runs as UID 1000, and is 410,005,173 bytes. Its immutable image ID,
source hashes, timings, comparison summaries and failed-run history are in
[`validation/local-results.json`](validation/local-results.json).

The first v4 60M run completed successfully, but its wall-time measurement was
invalid because PowerShell converted timestamps inconsistently. It is excluded
from timing statistics. The two reported repeats use raw UTC timestamps and
agree with an independent client-side monotonic timer. They are two measurements,
not a three-run median. Shared load and file caching affect these timings.

Earlier attempts are retained as evidence: v1 was OOM-killed in the shared
approximately 7.6 GiB Linux VM while below its own 6 GiB limit; v3 timed out
during deduplication. The inline-ID and interning changes allowed v4 to finish.
The competition's 24 GiB environment has not been tested locally. Consistency
checks and agreement between versions or repeats do not prove independently
correct results for every large output row. Hidden-data correctness, official
runtime and leaderboard placement remain unverified.

## Provenance and licensing

Original implementation written during the Buildathon with AI assistance,
based on the published task rules and public fixtures. No precomputed competition
answers or generator-specific identities/session labels are used by the solver.
The official Rust generator is used only for local test data and is not part of
the submitted runtime. Runtime dependencies are the C++ standard library and
Apache Arrow/PyArrow 22.0.0 (Apache License 2.0); the Docker base is Python 3.12
on Debian Bookworm, with Debian's GCC 12 toolchain installed only in the build
stage. No Rust toolchain is required to build or run the submitted solver.

This implementation is released under the MIT License.
