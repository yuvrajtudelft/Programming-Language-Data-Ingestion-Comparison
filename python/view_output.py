#!/usr/bin/env python3
"""
Simple helper to inspect benchmark parquet output.
"""

from __future__ import annotations

import argparse

import pyarrow.parquet as pq


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect parquet output from ingestion benchmark.")
    parser.add_argument(
        "--parquet",
        default="data/out/python_cpu_1h.parquet",
        help="Path to parquet file to inspect.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Number of rows to preview.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    table = pq.read_table(args.parquet)
    preview = table.slice(0, max(args.limit, 0))

    print(f"Parquet file: {args.parquet}")
    print(f"Rows: {table.num_rows}")
    print(f"Columns: {table.num_columns}")
    print("\nSchema:")
    print(table.schema)
    print(f"\nPreview (top {args.limit} rows):")
    print(preview.to_pydict())


if __name__ == "__main__":
    main()
