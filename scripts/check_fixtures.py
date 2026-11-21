import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

from compare import compare


def main():
    root = Path(sys.argv[1])
    records = []
    start = time.monotonic()
    for case in sorted(p for p in root.iterdir() if p.is_dir()):
        with tempfile.TemporaryDirectory(prefix="fixture-", dir=os.environ.get("SCRATCH_DIR", "/scratch")) as temporary:
            out = Path(temporary) / "out"
            scratch = Path(temporary) / "scratch"
            out.mkdir(); scratch.mkdir()
            env = dict(os.environ, DATA_DIR=str(case / "input"), OUTPUT_DIR=str(out), SCRATCH_DIR=str(scratch))
            run = subprocess.run(["/app/run.sh"], env=env, capture_output=True, text=True, timeout=120)
            result = {"case": case.name, "exit": run.returncode}
            if run.returncode == 0:
                result["comparison"] = compare(out, case / "expected")
                result["passed"] = all(v["match"] for v in result["comparison"].values())
            else:
                result["passed"] = False
            if not result["passed"]:
                result["stderr"] = run.stderr
            records.append(result)
            print(f"{'PASS' if result['passed'] else 'FAIL'} {case.name}", flush=True)
    summary = {"passed": sum(r["passed"] for r in records), "total": len(records), "seconds": time.monotonic()-start, "cases": records}
    if len(sys.argv) > 2:
        Path(sys.argv[2]).write_text(json.dumps(summary, indent=2))
    print(json.dumps({k: v for k, v in summary.items() if k != "cases"}))
    return 0 if records and all(r["passed"] for r in records) else 1


if __name__ == "__main__":
    sys.exit(main())
