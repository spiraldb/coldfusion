#!/usr/bin/env python3
"""Compare two complete, validated SF10 TPC-H benchmark result files."""

import argparse
import json
import math
from pathlib import Path


def load(path: Path) -> dict[int, dict]:
    records = {}
    for line in path.read_text().splitlines():
        record = json.loads(line)
        query = record["query_idx"]
        if query in records:
            raise ValueError(f"{path}: duplicate query {query}")
        records[query] = record
    if set(records) != set(range(1, 23)):
        raise ValueError(f"{path}: expected all 22 TPC-H queries")
    for query, record in records.items():
        if record["status"] != "passed" or record.get("value_ns", 0) <= 0:
            raise ValueError(f"{path}: query {query} has no passing timing: {record}")
        if float(record["scale_factor"]) != 10:
            raise ValueError(f"{path}: expected scale factor 10")
        if record["dataset"] != "tpch" or record.get("answer_match") != "exact":
            raise ValueError(f"{path}: expected exact TPC-H answer validation")
    return records


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("arbor", type=Path)
    parser.add_argument("iceberg", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    arbor, iceberg = load(args.arbor), load(args.iceberg)
    for query in range(1, 23):
        for key in ("runner", "storage", "query_sql", "result_digest"):
            if arbor[query][key] != iceberg[query][key]:
                raise ValueError(f"Query {query}: mismatched {key}")
    rows = [
        {
            "query": query,
            "arbor_seconds": arbor[query]["value_ns"] / 1e9,
            "iceberg_seconds": iceberg[query]["value_ns"] / 1e9,
            "iceberg_speedup": arbor[query]["value_ns"] / iceberg[query]["value_ns"],
        }
        for query in range(1, 23)
    ]
    arbor_seconds = sum(row["arbor_seconds"] for row in rows)
    iceberg_seconds = sum(row["iceberg_seconds"] for row in rows)
    report = {
        "arbor_results": str(args.arbor),
        "iceberg_results": str(args.iceberg),
        "validated_queries": 22,
        "runner": arbor[1]["runner"],
        "arbor_total_seconds": arbor_seconds,
        "iceberg_total_seconds": iceberg_seconds,
        "arbor_queries_per_second": 22 / arbor_seconds,
        "iceberg_queries_per_second": 22 / iceberg_seconds,
        "aggregate_throughput_speedup": arbor_seconds / iceberg_seconds,
        "geometric_mean_query_speedup": math.exp(
            sum(math.log(row["iceberg_speedup"]) for row in rows) / 22
        ),
        "queries": rows,
    }
    serialized = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized)
    print(serialized, end="")


if __name__ == "__main__":
    main()
