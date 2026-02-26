#!/usr/bin/env python3
"""
Run cross-language Docker experiments under EnergiBridge and generate one report.

Languages/services:
- python-benchmark
- go-benchmark
- rust-benchmark
- java-benchmark

Environment overrides:
- WARMUP_RUNS (default: 1)
- MEASURED_RUNS (default: 5)
- EB_INTERVAL_US (default: 200)
- ENERGIBRIDGE_BIN (optional explicit energibridge path)
"""

from __future__ import annotations

import csv
import math
import os
import shutil
import random
import statistics
import subprocess
import time
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

LANGUAGE_SERVICES = [
    ("python", "python-benchmark"),
    ("go", "go-benchmark"),
    ("rust", "rust-benchmark"),
    ("java", "java-benchmark"),
]

ENERGIBRIDGE_CANDIDATES = [
    REPO_ROOT.parent / "energiBridge" / "target" / "release" / "energibridge",
    REPO_ROOT.parent / "EnergiBridge" / "target" / "release" / "energibridge",
]

WARMUP_RUNS = int(os.getenv("WARMUP_RUNS", "1"))
MEASURED_RUNS = int(os.getenv("MEASURED_RUNS", "5"))
EB_INTERVAL_US = int(os.getenv("EB_INTERVAL_US", "200"))

RUNS_DIR = REPO_ROOT / "data" / "out" / "energibridge_runs_all_languages"
REPORT_CSV = REPO_ROOT / "data" / "out" / "energibridge_all_languages_report.csv"
REPORT_MD = REPO_ROOT / "data" / "out" / "energibridge_all_languages_report.md"


def run_cmd(cmd: list[str], cwd: Path, fail_message: str) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(fail_message)
    return proc


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

    raise SystemExit(
        "EnergiBridge binary not found. Set ENERGIBRIDGE_BIN or add energibridge to PATH."
    )


def ensure_preconditions() -> None:
    if shutil.which("docker") is None:
        raise SystemExit("Docker CLI not found in PATH.")
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_CSV.parent.mkdir(parents=True, exist_ok=True)


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


def build_images() -> None:
    services = [service for _, service in LANGUAGE_SERVICES]
    cmd = ["docker", "compose", "build", *services]
    print(f"[build] {' '.join(cmd)}")
    run_cmd(cmd, REPO_ROOT, "Docker compose build failed.")


def run_once(energibridge_bin: Path, language: str, service: str, run_idx: int, measured: bool) -> dict[str, float | int | str]:
    kind = "measured" if measured else "warmup"
    run_label = f"{kind}_{run_idx:02d}"
    run_csv = RUNS_DIR / f"{language}_{run_label}.csv"

    cmd = [
        str(energibridge_bin),
        "--output",
        str(run_csv),
        "--interval",
        str(EB_INTERVAL_US),
        "docker",
        "compose",
        "run",
        "--rm",
        service,
    ]

    print(f"[run] {language} {run_label}")
    started = time.perf_counter()
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True)
    elapsed_s = time.perf_counter() - started

    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr)
        raise SystemExit(f"Run failed for {language} {run_label}")

    energy_j, avg_power_w, samples, skipped_samples = parse_energy(run_csv)
    return {
        "language": language,
        "service": service,
        "run_label": run_label,
        "kind": kind,
        "wall_time_s": elapsed_s,
        "energy_j": energy_j,
        "avg_power_w": avg_power_w,
        "samples": samples,
        "skipped_samples": skipped_samples,
        "csv_path": str(run_csv.relative_to(REPO_ROOT)),
    }


def write_report(results: list[dict[str, float | int | str]]) -> None:
    with REPORT_CSV.open("w", encoding="utf-8", newline="") as f:
        fields = [
            "language",
            "service",
            "run_label",
            "kind",
            "wall_time_s",
            "energy_j",
            "avg_power_w",
            "samples",
            "skipped_samples",
            "csv_path",
        ]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)

    measured_by_lang: dict[str, list[dict[str, float | int | str]]] = {}
    for row in results:
        if row["kind"] != "measured":
            continue
        measured_by_lang.setdefault(str(row["language"]), []).append(row)

    summary_rows: list[tuple[str, float, float, float, float, float, float]] = []
    for language, rows in measured_by_lang.items():
        energies = [float(r["energy_j"]) for r in rows]
        times = [float(r["wall_time_s"]) for r in rows]
        powers = [float(r["avg_power_w"]) for r in rows]

        def mean_std(values: list[float]) -> tuple[float, float]:
            if len(values) == 1:
                return values[0], 0.0
            return statistics.mean(values), statistics.stdev(values)

        mean_e, std_e = mean_std(energies)
        mean_t, std_t = mean_std(times)
        mean_p, std_p = mean_std(powers)
        summary_rows.append((language, mean_e, std_e, mean_t, std_t, mean_p, std_p))

    summary_rows.sort(key=lambda x: x[1])  # rank by mean energy asc

    lines = [
        "# Cross-Language EnergiBridge Report (Docker)",
        "",
        f"- Generated at: {datetime.now().isoformat(timespec='seconds')}",
        f"- Warmup runs per language: {WARMUP_RUNS}",
        f"- Measured runs per language: {MEASURED_RUNS}",
        f"- Interval (us): {EB_INTERVAL_US}",
        "",
        "## Ranked by Mean Energy (lower is better)",
        "",
        "| Rank | Language | Mean Energy (J) | Std Energy (J) | Mean Time (s) | Std Time (s) | Mean Power (W) | Std Power (W) |",
        "|---:|---|---:|---:|---:|---:|---:|---:|",
    ]

    for idx, (lang, me, se, mt, st, mp, sp) in enumerate(summary_rows, start=1):
        lines.append(
            f"| {idx} | {lang} | {me:.3f} | {se:.3f} | {mt:.3f} | {st:.3f} | {mp:.3f} | {sp:.3f} |"
        )

    lines.extend(
        [
            "",
            "## Output Files",
            "",
            f"- Per-run EnergiBridge CSVs: `{RUNS_DIR.relative_to(REPO_ROOT)}`",
            f"- Full run table: `{REPORT_CSV.relative_to(REPO_ROOT)}`",
        ]
    )

    REPORT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ensure_preconditions()
    energibridge_bin = find_energibridge_binary()
    print(f"[info] Using EnergiBridge: {energibridge_bin}")

    # Quick check that compose is valid before running long experiment.
    run_cmd(["docker", "compose", "config"], REPO_ROOT, "docker compose config failed.")
    build_images()

    results: list[dict[str, float | int | str]] = []

    for i in range(1, WARMUP_RUNS + 1):
        for language, service in LANGUAGE_SERVICES:
            run_once(energibridge_bin, language, service, i, measured=False)
    
    shuffle_seed = 44
    if shuffle_seed is not None:
        try:
            random.seed(int(shuffle_seed))
        except ValueError:
            print("shuffle failed with invalid SHUFFLE_SEED, proceeding with non-deterministic randomness")
            pass

    for i in range(1, MEASURED_RUNS + 1):
        services = list(LANGUAGE_SERVICES)
        random.shuffle(services)
        for language, service in services:
            results.append(run_once(energibridge_bin, language, service, i, measured=True))

    write_report(results)
    print(f"[done] Wrote report CSV: {REPORT_CSV.relative_to(REPO_ROOT)}")
    print(f"[done] Wrote report MD:  {REPORT_MD.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
