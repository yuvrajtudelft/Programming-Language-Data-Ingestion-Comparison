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