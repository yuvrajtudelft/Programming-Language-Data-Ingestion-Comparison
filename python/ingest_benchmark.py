#!/usr/bin/env python3
"""
Reference ingestion benchmark pipeline for cross-language comparison.

Pipeline:
JSONL(.gz) -> validate -> transform -> aggregate -> Parquet + checksum
"""

from __future__ import annotations

import glob
import gzip
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

# Edit these values directly instead of passing CLI arguments.
CONFIG = {
    "MODE": "cpu",  # "cpu" (strict validation) or "io" (relaxed validation)
    "INPUT_GLOBS": [],  # Example: ["data/raw/gharchive/*.json.gz"]
    "DOWNLOAD_GHARCHIVE_DATE": "2026-01-01",  # Set to None to disable downloads
    "DOWNLOAD_HOURS": 10,
    "DOWNLOAD_DIR": "data/raw/gharchive",
    "OUTPUT_PARQUET": "data/out/python_cpu_1h.parquet",
    "OUTPUT_CHECKSUM": "data/out/python_cpu_1h.sha256",
}


AggregateKey = Tuple[str, str]
AggregateValue = Tuple[int, int]


@dataclass
class Stats:
    total_lines: int = 0
    valid_lines: int = 0
    invalid_lines: int = 0


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def download_gharchive(date_str: str, hours: int, download_dir: str) -> List[str]:
    if hours <= 0:
        return []
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    os.makedirs(download_dir, exist_ok=True)
    downloaded = []
    for hour in range(hours):
        ts = date_obj + timedelta(hours=hour)
        url = f"https://data.gharchive.org/{ts:%Y-%m-%d}-{ts.hour}.json.gz"
        out_path = os.path.join(download_dir, f"{ts:%Y-%m-%d}-{ts.hour}.json.gz")
        if not os.path.exists(out_path):
            print(f"[download] {url} -> {out_path}")
            req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req) as resp, open(out_path, "wb") as out_f:
                out_f.write(resp.read())
        else:
            print(f"[download] skip existing {out_path}")
        downloaded.append(out_path)
    return downloaded


def resolve_input_files(patterns: Iterable[str]) -> List[str]:
    files: List[str] = []
    for pattern in patterns:
        matches = sorted(glob.glob(pattern))
        files.extend(matches)
    return sorted(set(files))


def validate_record(record: dict, strict: bool) -> bool:
    if not isinstance(record, dict):
        return False

    record_id = record.get("id")
    event_type = record.get("type")
    created_at = record.get("created_at")
    repo = record.get("repo")

    if not isinstance(record_id, str) or not record_id:
        return False
    if not isinstance(event_type, str) or not event_type:
        return False
    if not isinstance(created_at, str) or not created_at:
        return False
    if not isinstance(repo, dict):
        return False
    if "id" not in repo:
        return False

    repo_id = repo.get("id")
    try:
        int(repo_id)
    except (TypeError, ValueError):
        return False

    if strict:
        # GH Archive timestamps are expected as ISO-like UTC strings.
        if "T" not in created_at or not created_at.endswith("Z"):
            return False
        if len(event_type) > 100:
            return False
    return True


def transform_record(record: dict) -> Tuple[str, str, int]:
    created_at = record["created_at"]
    event_day = created_at[:10]
    event_type = str(record["type"]).strip().lower()
    repo_id = int(record["repo"]["id"])
    return event_day, event_type, repo_id


def process_files(input_files: List[str], strict: bool) -> Tuple[Dict[AggregateKey, AggregateValue], Stats]:
    aggregates: Dict[AggregateKey, AggregateValue] = {}
    stats = Stats()

    for file_path in input_files:
        print(f"[process] {file_path}")
        with gzip.open(file_path, mode="rt", encoding="utf-8") as f:
            for line in f:
                stats.total_lines += 1
                line = line.strip()
                if not line:
                    stats.invalid_lines += 1
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    stats.invalid_lines += 1
                    continue

                if not validate_record(record, strict=strict):
                    stats.invalid_lines += 1
                    continue

                stats.valid_lines += 1
                event_day, event_type, repo_id = transform_record(record)
                key = (event_day, event_type)
                count, repo_sum = aggregates.get(key, (0, 0))
                aggregates[key] = (count + 1, repo_sum + repo_id)

    return aggregates, stats


def write_parquet(output_parquet: str, rows: List[Tuple[str, str, int, int]]) -> None:
    ensure_parent(output_parquet)
    table = pa.table(
        {
            "event_day": [r[0] for r in rows],
            "event_type": [r[1] for r in rows],
            "record_count": [r[2] for r in rows],
            "sum_repo_id": [r[3] for r in rows],
        }
    )
    pq.write_table(table, output_parquet, compression="zstd")


def write_checksum(output_checksum: str, rows: List[Tuple[str, str, int, int]]) -> str:
    ensure_parent(output_checksum)
    digest = hashlib.sha256()
    for row in rows:
        digest.update(f"{row[0]}|{row[1]}|{row[2]}|{row[3]}\n".encode("utf-8"))
    checksum = digest.hexdigest()
    with open(output_checksum, "w", encoding="utf-8") as f:
        f.write(checksum + "\n")
    return checksum


def main() -> None:
    mode = str(CONFIG["MODE"]).lower()
    if mode not in {"cpu", "io"}:
        raise SystemExit("CONFIG['MODE'] must be either 'cpu' or 'io'.")
    strict = mode == "cpu"

    downloaded: List[str] = []
    download_date = CONFIG["DOWNLOAD_GHARCHIVE_DATE"]
    download_hours = int(CONFIG["DOWNLOAD_HOURS"])
    download_dir = str(CONFIG["DOWNLOAD_DIR"])
    if download_date and download_hours > 0:
        downloaded = download_gharchive(str(download_date), download_hours, download_dir)

    input_patterns = [str(p) for p in CONFIG["INPUT_GLOBS"]]
    input_files = resolve_input_files(input_patterns) + downloaded
    input_files = sorted(set(input_files))
    if not input_files:
        raise SystemExit("No input files found. Set CONFIG['INPUT_GLOBS'] and/or download settings.")

    aggregates, stats = process_files(input_files=input_files, strict=strict)
    sorted_rows = sorted((k[0], k[1], v[0], v[1]) for k, v in aggregates.items())

    output_parquet = str(CONFIG["OUTPUT_PARQUET"])
    output_checksum = str(CONFIG["OUTPUT_CHECKSUM"])
    write_parquet(output_parquet, sorted_rows)
    checksum = write_checksum(output_checksum, sorted_rows)

    print("[done]")
    print(f"mode={mode}")
    print(f"input_files={len(input_files)}")
    print(f"total_lines={stats.total_lines}")
    print(f"valid_lines={stats.valid_lines}")
    print(f"invalid_lines={stats.invalid_lines}")
    print(f"aggregate_rows={len(sorted_rows)}")
    print(f"checksum={checksum}")
    print(f"parquet={output_parquet}")
    print(f"checksum_file={output_checksum}")


if __name__ == "__main__":
    main()
