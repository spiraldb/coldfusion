#!/usr/bin/env python3
"""Summarize CPU-weighted Rust stacks from a presymbolicated samply profile."""

import argparse
import bisect
from collections import Counter
import gzip
import json
from pathlib import Path


def summarize(path: Path) -> dict:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as source:
        profile = json.load(source)
    if profile["meta"].get("sampleUnits", {}).get("threadCPUDelta") != "µs":
        raise ValueError("Expected microsecond thread CPU deltas")
    symbols_path = Path(str(path).removesuffix(".gz") + ".syms.json")
    symbols = json.loads(symbols_path.read_text())
    strings = symbols["string_table"]
    libraries = {
        library["debug_id"].replace("-", "").upper(): library
        for library in symbols["data"]
    }
    lookups = []
    for library in profile["libs"]:
        debug_id = library["breakpadId"][:-1].upper()
        data = libraries.get(debug_id, {})
        table = data.get("symbol_table", [])
        lookups.append(
            (table, dict(data.get("known_addresses", [])), [s["rva"] for s in table])
        )
    own, inclusive = Counter(), Counter()
    total_us = 0
    thread_cpu = []
    for thread in profile["threads"]:
        frames = thread["frameTable"]
        functions = thread["funcTable"]
        resources = thread["resourceTable"]
        stack = thread["stackTable"]
        samples = thread["samples"]
        thread_cpu.append(
            {
                "name": thread["name"],
                "tid": thread["tid"],
                "cpu_seconds": sum(value or 0 for value in samples["threadCPUDelta"]) / 1e6,
            }
        )
        frame_names = []
        for frame, address in enumerate(frames["address"]):
            function = frames["func"][frame]
            resource = functions["resource"][function]
            names = [thread["stringArray"][functions["name"][function]]]
            if resource is not None and resource >= 0:
                library = resources["lib"][resource]
                if library is not None and library >= 0:
                    table, known, addresses = lookups[library]
                    index = known.get(address, bisect.bisect_right(addresses, address) - 1)
                    if index is not None and 0 <= index < len(table):
                        symbol = table[index]
                        if symbol["rva"] <= address < symbol["rva"] + symbol["size"]:
                            names = [
                                strings[f["function"]] for f in symbol.get("frames", [])
                            ] or [strings[symbol["symbol"]]]
            frame_names.append(names)
        cache = {}
        for sample, cursor in enumerate(samples["stack"]):
            if cursor is None:
                continue
            weight = samples["threadCPUDelta"][sample] or 0
            if weight <= 0:
                continue
            total_us += weight
            own[frame_names[stack["frame"][cursor]][0]] += weight
            if cursor not in cache:
                names = set()
                current = cursor
                while current is not None:
                    names.update(frame_names[stack["frame"][current]])
                    current = stack["prefix"][current]
                cache[cursor] = names
            for name in cache[cursor]:
                inclusive[name] += weight

    def rows(counter: Counter) -> list[dict]:
        return [
            {"function": function, "cpu_percent": 100 * weight / total_us}
            for function, weight in counter.most_common(30)
        ]

    return {
        "profile": str(path),
        "sampled_cpu_seconds": total_us / 1e6,
        "threads": sorted(thread_cpu, key=lambda row: row["cpu_seconds"], reverse=True),
        "self": rows(own),
        "inclusive": rows(inclusive),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = summarize(args.profile)
    if args.output:
        args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"Sampled CPU: {report['sampled_cpu_seconds']:.2f}s")
    for category in ("self", "inclusive"):
        print(category)
        for row in report[category][:15]:
            print(f"{row['cpu_percent']:6.2f}% {row['function'][:220]}")


if __name__ == "__main__":
    main()
