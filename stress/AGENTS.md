# AGENTS.md

This is an open loop stress benchmark tool for QuxDB. NEVER add tests.

## Features

- Uses open-loop arrival scheduling at a configured `--rate`
- Sends a mixture of `PUT /k/:key` and `GET /k/:key` requests to the provided server
- Default payload is a random UUID key and a random JSON payload value.
- Tool can schedule arrivals for `--duration/-d` or until a fixed `--requests/-n` PUT count
- Supports an unmeasured `--warmup` phase
- Provides `--kseq`, `--kts`, `--kuuid4`, `--kulid`, `--kall`, and `--vts` to change the key/value payload type
