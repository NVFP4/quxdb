# AGENTS.md

This is QuxDB, a high-performance LSM database written in Go 1.26.

## Guidelines

- This is a research effort, so focus on broader LSM technology. Do not replicate implementation of specific products like RocksDB/LevelDB.
- Avoid defensive checks. Add a guard only for a realistic failure mode.

## Comment Guidelines

Add a comment only when it gives future coding agents useful context they cannot infer from nearby code. Keep each comment to one or two concise lines in clear technical English. Technical detail is welcome when it explains a non-obvious contract, invariant, constraint, side effect, security or performance reason, or dependency between files. Remove comments that restate code, describe obvious behavior, or record editing history. Preserve required tool directives, license notices, and generated-file comments.
