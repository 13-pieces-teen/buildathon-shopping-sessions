# Frozen independent reference tools

These two Python files are copied byte-for-byte from the files whose SHA256
hashes were recorded by BOTH completed checker configurations. Their original
code and default paths have deliberately NOT been edited.

The full checker is independent Python reference verification, not official
ground truth. It imports this oracle, not the optimized solver. Running it can
require substantial time and scratch space; packaging these files does not run it.

From this tools directory, explicitly select the frozen oracle:

```sh
python full_oracle_check.py --data /path/to/input --actual /path/to/solver-output --work /path/to/new-reference-checkpoints --oracle oracle.py --event-buckets 128 --person-shards 128 --workers 1
```

Always pass `--oracle oracle.py` (or its absolute path). The frozen checker's
unchanged default was written for the original workspace layout and is not a
portable default inside this evidence package. Use each dataset's recorded
configuration for its bucket/shard counts; the dev check used 16/16. Install
PyArrow in the execution environment. Do not use Python `-O`: conservation
assertions must remain enabled. Data, solver output and checkpoint directories
must be disjoint; do not modify source files while a check or resume is in use.

The checker compares full output-row values and duplicate keys, and checks
staging record-count conservation. It does NOT trace a stable ordinal for every
original raw input row. Counts alone do not prove one-to-one source-row ordinal
coverage. Large files are bound to the run by path/size/mtime, with small metadata
hashes; checkpoint artifacts use expected names/sizes rather than content hashes.
The saved 60M copy manifest adds original-to-copy SHA256 evidence, but packaging
does not rehash the large data or add a final end-to-end hash verification.

Reference-run wall times are verification costs, not the solver's performance.
The two valid local 60M solver timings are reported separately in ../summary.json.
Neither reference agreement nor local solver timings constitute official scoring.
