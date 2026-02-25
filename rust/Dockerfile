FROM rust:1.83-slim AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock* ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /src/target/release/ingestion-rust /app/ingest-benchmark

ENTRYPOINT ["/app/ingest-benchmark"]
