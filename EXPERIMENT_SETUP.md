# Experiment Setup: Language Energy Comparison (Data Ingestion)

## Goal
Measure and explain energy differences across `Python`, `Go`, `Java`, and `Rust` for the **same ingestion pipeline**:

`JSONL(.gz) -> validate -> transform -> aggregate -> Parquet + checksum`

We will compare:
- Total energy (J)
- Execution time (s)
- Average power (W)
- Energy per record (J/record)
- Energy-delay product (EDP)

## Primary Dataset Choice (Proposed)
Use **GH Archive** public event data (`https://data.gharchive.org/...json.gz`).

Why this dataset:
- Public and large (hourly files across many years; easy to scale up/down)
- Native compressed JSON Lines format
- Nested schema, good for realistic ingestion costs
- Easy to script reproducibly (fixed hour/day list)

### Suggested dataset slices
- **Medium**: 24 hourly files (1 day)
- **Large**: 7 x 24 hourly files (1 week)

Example file format:
- `https://data.gharchive.org/2025-01-01-0.json.gz`
- `https://data.gharchive.org/2025-01-01-1.json.gz`
- ...

## Fairness Contract (Same Task in Every Language)
Each language implementation must perform exactly these steps, in this order:

1. Read UTF-8 JSON Lines records (from local `.json.gz` files)
2. Validate required fields:
   - `id` (string)
   - `type` (string)
   - `repo.id` (numeric)
   - `created_at` (string timestamp)
3. Transform:
   - normalize event type to lowercase
   - derive `event_day` (`YYYY-MM-DD` from `created_at`)
4. Aggregate by `(event_day, event_type)`:
   - `record_count`
   - `sum_repo_id`
5. Write aggregate table to Parquet with fixed columns/order
6. Produce deterministic checksum from sorted aggregate rows

## Mode Definitions
Run both modes to separate compute-heavy and IO-heavy behavior.

- **CPU-bound mode**
  - Strict validation enabled
  - Minimal output (only aggregated table)
  - Local warm cache runs

- **IO-bound mode**
  - Relaxed validation
  - Same aggregation logic
  - Focus on larger input volume + full Parquet output path

## Language-Specific Task Definitions
- **Python**
  - Script: `python/ingest_benchmark.py`
  - Runtime: CPython
  - Libraries: stdlib JSON + `pyarrow` for Parquet

- **Go**
  - Planned script: `go/ingest_benchmark.go`
  - Runtime: Go toolchain
  - Libraries: stdlib JSON + mainstream Parquet writer

- **Java**
  - Planned class: `java/.../IngestBenchmark.java`
  - Runtime: OpenJDK (fixed `-Xms`, `-Xmx`, GC)
  - Libraries: Jackson + Parquet ecosystem libs

- **Rust**
  - Planned binary: `rust/src/main.rs`
  - Runtime: `cargo` release binary
  - Libraries: `serde_json` + `parquet` crate

## Measurement Procedure (EnergiBridge)
For each `(language, mode, size)`:
- Warm-up runs: 2
- Measured runs: 10
- Randomized run order
- Fixed machine, fixed core count, stable power state

Record:
- energy_joules, time_seconds, avg_power_watts
- records_processed, throughput_records_per_s
- output checksum

## Deliverables
- Implementations in 4 languages with identical behavior
- Run scripts for automated benchmark matrix
- Parsed results CSV + plots
- Reproducibility notes (OS, CPU, runtime versions, flags)

## Immediate Next Step
Implement Python baseline first (reference behavior), then mirror this behavior in Go/Java/Rust.
