"""Exact Arrow comparison, preserving nulls, integer values and DATE semantics."""
import json
from pathlib import Path
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.parquet as pq

SCHEMAS = {
    "user_daily": pa.schema([(c, t) for c, t in [
        ("user_id", pa.string()), ("day", pa.date32()), ("sessions", pa.int64()),
        ("events", pa.int64()), ("revenue_cents", pa.int64()), ("top_category", pa.string())]]),
    "campaign_daily": pa.schema([(c, t) for c, t in [
        ("campaign_id", pa.string()), ("channel", pa.string()), ("day", pa.date32()),
        ("sessions", pa.int64()), ("purchasing_sessions", pa.int64()),
        ("revenue_cents", pa.int64()), ("assisted_sessions", pa.int64())]]),
}


def load(root, name):
    root = Path(root)
    schema = SCHEMAS[name]
    if (root / (name + ".csv")).is_file():
        table = csv.read_csv(root / (name + ".csv"), convert_options=csv.ConvertOptions(
            column_types={f.name: f.type for f in schema}, strings_can_be_null=True, null_values=[""], quoted_strings_can_be_null=False))
    else:
        paths = sorted(p for p in (root / name).glob("*.parquet") if not p.name.startswith((".", "_")) and not p.is_symlink())
        if not paths:
            raise ValueError(f"no parquet: {root / name}")
        parts = []
        for path in paths:
            part = pq.read_table(path)
            if set(part.column_names) != set(schema.names):
                raise ValueError(f"wrong columns: {path}")
            for field in part.schema:
                expected = schema.field(field.name).type
                if pa.types.is_integer(expected):
                    valid = pa.types.is_integer(field.type)
                elif pa.types.is_string(expected):
                    valid = pa.types.is_string(field.type) or pa.types.is_large_string(field.type) or pa.types.is_dictionary(field.type)
                else:
                    valid = field.type == expected
                if not valid:
                    raise ValueError(f"wrong type {path}:{field}")
            parts.append(part.select(schema.names).cast(schema))
        table = pa.concat_tables(parts)
    key = "user_id" if name == "user_daily" else "campaign_id"
    return table.select(schema.names).cast(schema).sort_by([(key, "ascending"), ("day", "ascending")])


def compare(left, right):
    report = {}
    for name in SCHEMAS:
        a, b = load(left, name), load(right, name)
        matched = a.equals(b)
        report[name] = {"actual_rows": a.num_rows, "expected_rows": b.num_rows, "match": matched}
        if not matched and a.num_rows == b.num_rows:
            for column in a.column_names:
                if not a[column].equals(b[column]):
                    report[name].setdefault("different_columns", []).append(column)
        for column in ("sessions", "events", "revenue_cents", "assisted_sessions"):
            if column in a.column_names:
                report[name].setdefault("actual_totals", {})[column] = pc.sum(a[column]).as_py() or 0
    return report


if __name__ == "__main__":
    report = compare(*sys.argv[1:3])
    print(json.dumps(report, indent=2))
    sys.exit(0 if all(item["match"] for item in report.values()) else 1)
