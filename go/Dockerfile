FROM golang:1.22-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/ingest-benchmark .

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /out/ingest-benchmark /app/ingest-benchmark

ENTRYPOINT ["/app/ingest-benchmark"]
