#!/usr/bin/env python3
"""
Run the Rust benchmark multiple times with EnergiBridge and generate a report.
"""

from __future__ import annotations

import csv
import math
import os
import shutil
import statistics
import subprocess
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RUST_DIR = REPO_ROOT / "rust"
RUST_BINARY = RUST_DIR / "target" / "release" / "ingestion-rust"

ENERGIBRIDGE_CANDIDATES = [
    REPO_ROOT.parent / "energiBridge" / "target" / "release" / "energibridge",
    REPO_ROOT.parent / "EnergiBridge" / "target" / "release" / "energibridge",
]

WARMUP_RUNS = int(os.getenv("WARMUP_RUNS", "2"))
MEASURED_RUNS = int(os.getenv("MEASURED_RUNS", "10"))
EB_INTERVAL_US = int(os.getenv("EB_INTERVAL_US", "200"))

RUNS_DIR = REPO_ROOT / "data" / "out" / "energibridge_runs_rust"
REPORT_CSV = REPO_ROOT / "data" / "out" / "energibridge_rust_report.csv"
REPORT_MD = REPO_ROOT / "data" / "out" / "energibridge_rust_report.md"


def find_energibridge_binary() -> Path:
    env_path = os.getenv("ENERGIBRIDGE_BIN")
    if env_path:
        p = Path(env_path).expanduser().resolve()
        if p.exists():
            return p

    in_path = shutil.which("energibridge")
    if in_path:
        return Path(in_path).resolve()

    for candidate in ENERGIBRIDGE_CANDIDATES:
        if candidate.exists():
            return candidate
    raise SystemExit("EnergiBridge binary not found.")


def build_rust_binary() -> None:
    cmd = ["cargo", "build", "--release"]
    print(f"[build] {' '.join(cmd)}")
    proc = subprocess.run(cmd, cwd=RUST_DIR, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(f"Rust build failed: {proc.returncode}")


def parse_energy(csv_path: Path) -> tuple[float, float, int, int]:
    total_energy_j = 0.0
    total_time_s = 0.0
    samples = 0
    skipped = 0
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                delta_ms = float(row.get("Delta", "0") or "0")
                power_w = float(row.get("SYSTEM_POWER (Watts)", "nan"))
            except ValueError:
                continue
            if math.isnan(power_w) or delta_ms <= 0 or delta_ms > 5000:
                skipped += 1
                continue
            dt_s = delta_ms / 1000.0
            total_energy_j += power_w * dt_s
            total_time_s += dt_s
            samples += 1
    avg_power_w = (total_energy_j / total_time_s) if total_time_s > 0 else 0.0
    return total_energy_j, avg_power_w, samples, skipped


def run_once(energibridge_bin: Path, run_idx: int, measured: bool) -> dict[str, float | int | str]:
    kind = "measured" if measured else "warmup"
    run_label = f"{kind}_{run_idx:02d}"
    run_csv = RUNS_DIR / f"{run_label}.csv"
    cmd = [
        str(energibridge_bin),
        "--output",
        str(run_csv),
        "--interval",
        str(EB_INTERVAL_US),
        str(RUST_BINARY),
    ]
    print(f"[run] {run_label}: {' '.join(cmd)}")
    started = time.perf_counter()
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    elapsed_s = time.perf_counter() - started
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(f"Run failed: {run_label}")
    energy_j, avg_power_w, samples, skipped = parse_energy(run_csv)
    return {
        "run_label": run_label,
        "kind": kind,
        "wall_time_s": elapsed_s,
        "energy_j": energy_j,
        "avg_power_w": avg_power_w,
        "samples": samples,
        "skipped_samples": skipped,
        "csv_path": str(run_csv.relative_to(REPO_ROOT)),
    }


def write_report(results: list[dict[str, float | int | str]]) -> None:
    measured = [r for r in results if r["kind"] == "measured"]
    with REPORT_CSV.open("w", encoding="utf-8", newline="") as f:
        fields = ["run_label", "kind", "wall_time_s", "energy_j", "avg_power_w", "samples", "skipped_samples", "csv_path"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)

    energies = [float(r["energy_j"]) for r in measured]
    times = [float(r["wall_time_s"]) for r in measured]
    powers = [float(r["avg_power_w"]) for r in measured]

    def mean_std(v: list[float]) -> tuple[float, float]:
        if not v:
            return 0.0, 0.0
        if len(v) == 1:
            return v[0], 0.0
        return statistics.mean(v), statistics.stdev(v)

    me, se = mean_std(energies)
    mt, st = mean_std(times)
    mp, sp = mean_std(powers)

    lines = [
        "# EnergiBridge Rust Measurement Report",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Warmup runs: {WARMUP_RUNS}",
        f"- Measured runs: {MEASURED_RUNS}",
        f"- Interval (us): {EB_INTERVAL_US}",
        "",
        "## Aggregate (measured runs)",
        "",
        f"- Mean energy (J): {me:.3f}",
        f"- Std energy (J): {se:.3f}",
        f"- Mean wall time (s): {mt:.3f}",
        f"- Std wall time (s): {st:.3f}",
        f"- Mean avg power (W): {mp:.3f}",
        f"- Std avg power (W): {sp:.3f}",
        "",
        "## Per-run files",
        "",
        f"- Detailed run CSVs: `{RUNS_DIR.relative_to(REPO_ROOT)}`",
        f"- Tabular report CSV: `{REPORT_CSV.relative_to(REPO_ROOT)}`",
    ]
    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_CSV.parent.mkdir(parents=True, exist_ok=True)
    energibridge_bin = find_energibridge_binary()
    print(f"[info] Using EnergiBridge: {energibridge_bin}")
    build_rust_binary()

    results: list[dict[str, float | int | str]] = []
    for i in range(1, WARMUP_RUNS + 1):
        results.append(run_once(energibridge_bin, i, measured=False))
    for i in range(1, MEASURED_RUNS + 1):
        results.append(run_once(energibridge_bin, i, measured=True))

    write_report(results)
    print(f"[done] Wrote report CSV: {REPORT_CSV.relative_to(REPO_ROOT)}")
    print(f"[done] Wrote report MD:  {REPORT_MD.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
