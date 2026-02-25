# Programming-Language-Data-Ingestion-Comparison

## Python benchmark (Docker)

This repo includes a portable Docker setup for the Python ingestion benchmark so it runs consistently across machines.
The script is intentionally simple: configuration is hardcoded in `python/ingest_benchmark.py` under `CONFIG`.

### 1) Build image

```bash
docker build -t pl-ingestion-python:latest ./python
```

### 2) Run benchmark

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  pl-ingestion-python:latest
```

### 3) Run with compose (same example)

```bash
docker compose up --build python-benchmark
```

### Notes

- Input/output data lives on the host in `./data` and is mounted into the container at `/app/data`.
- To change mode/date/hours/output names, edit the `CONFIG` block in `python/ingest_benchmark.py`.
- Dockerfile location is now language-scoped at `python/Dockerfile` (same pattern for `go/`, `java/`, `rust/` when added).

## Go benchmark (Docker)

The Go benchmark mirrors the same ingestion pipeline with a hardcoded config in `go/main.go`.

### 1) Build image

```bash
docker build -t pl-ingestion-go:latest ./go
```

### 2) Run benchmark

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  pl-ingestion-go:latest
```

### 3) Run with compose

```bash
docker compose up --build go-benchmark
```

### Notes

- To change mode/date/hours/output names, edit the `config` block in `go/main.go`.
- Outputs default to `data/out/go_cpu_1h.parquet` and `data/out/go_cpu_1h.sha256`.
- Go auto-detects the shared data directory (`data/` or `../data`) so local runs and Docker both reuse the same dataset/output location.

## Rust benchmark (Docker)

The Rust benchmark mirrors the same ingestion pipeline with a hardcoded config in `rust/src/main.rs`.

### 1) Build image

```bash
docker build -t pl-ingestion-rust:latest ./rust
```

### 2) Run benchmark

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  pl-ingestion-rust:latest
```

### 3) Run with compose

```bash
docker compose up --build rust-benchmark
```

## Java benchmark (Docker)

The Java benchmark mirrors the same ingestion pipeline with a hardcoded config in `java/src/main/java/org/tud/IngestBenchmark.java`.

### 1) Build image

```bash
docker build -t pl-ingestion-java:latest ./java
```

### 2) Run benchmark

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  pl-ingestion-java:latest
```

### 3) Run with compose

```bash
docker compose up --build java-benchmark
```

## View parquet output

```bash
source .venv/bin/activate
python python/view_output.py --parquet data/out/python_cpu_1h.parquet --limit 20
```

## Measure with EnergiBridge (multiple runs)

```bash
source .venv/bin/activate
python scripts/measure_python_energibridge.py
```

Optional overrides (defaults: warmup=2, measured=10, interval=200us):

```bash
WARMUP_RUNS=1 MEASURED_RUNS=3 EB_INTERVAL_US=200 python scripts/measure_python_energibridge.py
```

If your venv needs activation first, keep env vars on the Python command:

```bash
source .venv/bin/activate
WARMUP_RUNS=1 MEASURED_RUNS=3 EB_INTERVAL_US=200 python scripts/measure_python_energibridge.py
```

Outputs:
- Per-run EnergiBridge CSVs in `data/out/energibridge_runs/`
- Aggregated run table in `data/out/energibridge_report.csv`
- Human-readable summary in `data/out/energibridge_report.md`

## Measure Go with EnergiBridge (multiple runs)

```bash
python scripts/measure_go_energibridge.py
```

Optional overrides:

```bash
WARMUP_RUNS=1 MEASURED_RUNS=3 EB_INTERVAL_US=200 python scripts/measure_go_energibridge.py
```

Outputs:
- Per-run EnergiBridge CSVs in `data/out/energibridge_runs_go/`
- Aggregated run table in `data/out/energibridge_go_report.csv`
- Human-readable summary in `data/out/energibridge_go_report.md`

## Measure Rust with EnergiBridge (multiple runs)

```bash
python scripts/measure_rust_energibridge.py
```

Optional overrides:

```bash
WARMUP_RUNS=1 MEASURED_RUNS=3 EB_INTERVAL_US=200 python scripts/measure_rust_energibridge.py
```

Outputs:
- Per-run EnergiBridge CSVs in `data/out/energibridge_runs_rust/`
- Aggregated run table in `data/out/energibridge_rust_report.csv`
- Human-readable summary in `data/out/energibridge_rust_report.md`

## Measure Java with EnergiBridge (multiple runs)

```bash
python scripts/measure_java_energibridge.py
```

Optional overrides:

```bash
WARMUP_RUNS=1 MEASURED_RUNS=3 EB_INTERVAL_US=200 python scripts/measure_java_energibridge.py
```

Outputs:
- Per-run EnergiBridge CSVs in `data/out/energibridge_runs_java/`
- Aggregated run table in `data/out/energibridge_java_report.csv`
- Human-readable summary in `data/out/energibridge_java_report.md`

## Measure all languages via Docker (single report)

```bash
python scripts/measure_all_languages_docker_energibridge.py
```

Optional overrides:

```bash
WARMUP_RUNS=1 MEASURED_RUNS=3 EB_INTERVAL_US=200 python scripts/measure_all_languages_docker_energibridge.py
```

Outputs:
- Per-run CSVs: `data/out/energibridge_runs_all_languages/`
- Full cross-language table: `data/out/energibridge_all_languages_report.csv`
- Ranked summary report: `data/out/energibridge_all_languages_report.md`