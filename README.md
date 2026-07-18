# QUX DB

QuxDB (_kwuhks-db_) is a fast key-value database optimized for write-heavy workloads. It is powered by on a Log Structured Merge Tree (LSM-Tree) engine.

This project serves an experimental bench and not recommended for deploying in production, yet.

## Getting Started

Build a binary using

```
make build
```

Run the binary using
```
make runb
```

## Development

Run with hot-reload using
```
make dev
```

For observability metrics, run the prometheus+grafana stack using
```
make start-prom
```
