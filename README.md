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

Validation on 2026-11-21: both initial and optimized engines match all 36 official
fixtures and the public seed-1 1M-event/250K-click development answer exactly.
The initial complete container additionally passes 240 adversarial fixtures
(120 original inputs and 120 separately executed equivalent transformations),
and five official generated seeds with 100K events/25K clicks each, against the
independent Python oracle. The optimized version's corresponding extended
regression is recorded in the development validation report when finished.
The aggregation module passes 300 randomized comparisons against an independent
C++ brute-force oracle. The initial native executable also ran all official
fixtures under ASan/UBSan without a report.

Local single-run measurements (8 CPU quota, 6 GiB container limit, Windows bind
mounts in a shared Docker Desktop VM; NOT official scores):

| Input | Initial container | Optimized container | Correctness evidence |
| --- | ---: | ---: | --- |
| Public 1M events / 250K clicks | 2.034 s | 1.033 s | Exact official answers |
| Generated 20M events / 5M clicks, seed 42 | 21.749 s | 16.774 s | Both implementations identical; 26 output invariants pass on initial output |
| Generated 60M events / 15M clicks | OOM, exit 137 | Not yet validated | No final-scale performance claim |

The 60M attempt was killed while the entire shared Linux VM had about 7.6 GiB
and other workloads were active; the container's reported peak was about 3 GiB,
below its own 6 GiB limit. This is not a measurement under the competition's
24 GiB environment. Single-run timings are affected by shared load and caching.
Invariants and agreement between versions do not establish independently proven
correctness for every large output row. Hidden-data correctness and final-scale
performance remain unverified until further evaluation.

## Provenance and licensing

Original implementation written during the Buildathon with AI assistance,
based on the published task rules and public fixtures. No precomputed competition
answers or generator-specific identities/session labels are used by the solver.
The official Rust generator is used only for local test data and is not part of
the submitted runtime. Runtime dependencies are the C++ standard library and
Apache Arrow/PyArrow 22.0.0 (Apache License 2.0); the Docker base is Python 3.12
on Debian Bookworm, with the Rust Bookworm image supplying GCC at build time.

This implementation is released under the MIT License.
