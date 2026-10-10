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


## Benchmarking

On i7-14700k, pin the quxdb and stress executables on separate cores

Run the server
```
$ taskset -c 2,3 .out/quxdb-server
```

Run 15min open loop stress testing sending ULID keys at 10k RPS (256 workers) 
```
$ taskset -c 16-27 .out/quxdb-stress -w 256 -r 10000 -d 15m --warmup 15s --kulid
```
Run 15min open loop stress testing sending mixed set of keys (ULID, UUIDv7, UUIDv4, counters, timestamps) at 10k RPS (256 workers) 
```
$ taskset -c 16-27 .out/quxdb-stress -w 256 -r 10000 -d 15m --warmup 15s
```
