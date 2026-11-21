# Shopping Sessions

An exact single-machine implementation of the Databricks Shopping Sessions
Buildathon task. The C++20 engine handles strict CSV parsing, linked identities,
global event deduplication, sessions, last-click attribution and campaign assists.
The current runtime also writes Parquet directly in C++ using Apache Arrow and
Parquet libraries distributed by the pinned PyArrow 22.0.0 wheel. It launches no
Python interpreter and produces no intermediate column files.

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
- Each event is parsed once, including precomputed business validity. Every
  structurally valid candidate remains in global event-ID deduplication; only
  after selecting the smallest ingest timestamp is validity applied. Exact key
  comparisons resolve collisions. Invalid earliest copies cannot be rescued.
- SSE2 accelerates strictly validated common CSV and timestamp shapes, with
  exact scalar fallback for long rows and arbitrary identifiers. Calendar caches
  contain computed dates, not assumptions about a dataset's window.
- Event IDs of at most 16 bytes are stored inline in raw descriptors; longer
  IDs retain exact full-byte comparison. This avoids random source-file paging
  during duplicate checks without using hashes as identity proofs.
- Unlinked person names are interned in sharded, address-stable storage. Linked
  names refer to the much smaller links files. Event CSV mappings can then be
  released after validation instead of remaining live throughout sorting.
- Canonical persons have internal integer IDs, with exact original names retained
  for output. Flat identity slots compare complete short keys inline and use full
  equality for longer strings. Event records are 24 bytes and clicks 32 bytes.
- Surviving events and clicks are partitioned by canonical person and sorted.
  A scan constructs sessions and user-day purchase-category revenue totals.
- Last-click attribution merges ordered sessions and clicks, resolving timestamp
  ties by minimum click ID.
- Every click at `t` covers session starts in `[t,t+7 days]`. Per-person,
  per-campaign interval unions plus session-index bounds count assists directly
  by day without constructing a session-by-click join.
- Identical click duplicates are handled idempotently by both last-click choice
  and interval union; a separate full click-ID hash table is unnecessary.
- Independent Parquet files are written in parallel, using bounded 262144-row
  groups, DATE columns and true nulls. ByteArray views refer to live source
  strings without constructing an intermediate copy of every output string.

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

### Current v7 validation and performance limits

The current native runtime passes all 36 official fixtures, 62 additional
manual/oracle boundary cases, 240 adversarial cases, five independent generated
100K-event seeds, and the public 1M official answer. Its full 20M and 60M outputs
match the previously verified v4 outputs exactly. At 60M this covers 6,643,504
user rows and 28,014 campaign rows; the v4 result was independently verified as
described below. See [`validation/optimization-v7/summary.json`](validation/optimization-v7/summary.json).

Fixed-CPU native-volume 20M samples were 8.832 and 4.675 seconds for v7, versus
8.672 and 7.260 seconds for interleaved v4 controls. The large variation prevents
claiming a stable speedup from the best sample. A local Windows-bind 60M run
finished correctly in 100.197 seconds, slower than the historical v4 repeats;
it recorded 25,338 major page faults in the shared 6 GiB environment. These are
local measurements, not official scores or guarantees about 24 GiB execution.

The previously submitted v4 commit `11c8679` received an official practice score
of 5.90 seconds (receipt BB-888F13C7). v7's official evaluation is pending at the
time this source checkpoint is recorded. No final 60M competition result exists.

### Historical v4 evidence

The v4 container was validated on 2026-11-21:

- All **36 official fixtures** and the public seed-1 **1M-event/250K-click answer**
  match exactly, including schemas, nulls and both complete output tables.
- All **240 adversarial fixtures** pass against the independent Python oracle:
  120 original inputs and 120 separately executed equivalent transformations.
- All **five generated seeds** (100K events/25K clicks each) match the independent
  Python oracle exactly.
- The optimized aggregation module passes 300 randomized comparisons against
  an independent C++ brute-force oracle. The final native executable ran all
  36 official fixtures under ASan/UBSan with leak checking and no report.
- The complete 20M output matches the original implementation exactly.
- The complete local **60M-event/15M-click output matches the independent Python
  reference**: 6,643,504 user rows and 28,014 campaign rows, with zero missing,
  extra or duplicate keys. All 128 complete-person shards were checked. The
  reference independently parsed the raw inputs, rebuilt identities and event
  winners, and used the original oracle's naive per-session click scan.
- Two 60M repeat outputs also match the first successful v4 output exactly;
  26 consistency checks pass on that first output.

Local container start-to-exit timings (8 CPU quota, 6 GiB container limit,
Windows bind mounts in a shared Docker Desktop VM; **not official scores**):

| Input | Final v4 runtime | Correctness evidence |
| --- | ---: | --- |
| Public 1M events / 250K clicks | 1.054 s | Exact official answers |
| Generated 20M events / 5M clicks, seed 42 | 10.438 s | Exact match to original implementation; 26 invariants pass on original output |
| Generated 60M events / 15M clicks, seed 20261121 | 54.367 s, 56.189 s | Full independent Python reference match; two exact repeats; 26 consistency checks |

Both timed 60M repeats completed within a 180-second timeout. The final image is
Linux/amd64, runs as UID 1000, and is 410,005,173 bytes. Its immutable image ID,
source hashes, timings, comparison summaries and failed-run history are in
[`validation/local-results.json`](validation/local-results.json).

The full independent comparison, original run records and frozen reference tools
are in [`validation/independent-reference/summary.json`](validation/independent-reference/summary.json).
The 60M reference check took about 34 minutes; this is verification cost, not
the solver's runtime. It checked full output values and staged record-count
conservation, without tracing a source ordinal for every raw row. Input and
checkpoint immutability is required; its detailed resume assumptions are retained
in the reports. All 89 input/output files copied to native Docker storage were
individually SHA256-verified before the check.

The first v4 60M run completed successfully, but its wall-time measurement was
invalid because PowerShell converted timestamps inconsistently. It is excluded
from timing statistics. The two reported repeats use raw UTC timestamps and
agree with an independent client-side monotonic timer. They are two measurements,
not a three-run median. Shared load and file caching affect these timings.

Earlier attempts are retained as evidence: v1 was OOM-killed in the shared
approximately 7.6 GiB Linux VM while below its own 6 GiB limit; v3 timed out
during deduplication. The inline-ID and interning changes allowed v4 to finish.
The competition's 24 GiB environment has not been tested locally. The full-scale
independent comparison covers the specified local seed; it is not an official
60M answer or proof for every possible input. The official v4 practice result is
recorded above; the final hidden evaluation remains pending.

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
