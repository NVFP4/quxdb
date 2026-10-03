# AGENTS.md

This is QuxDB, a high-performance LSM database written in Go 1.26.

## Guidelines

- This is a research effort, so focus on broader LSM technology. Do not replicate implementation of specific products like RocksDB/LevelDB.
- Avoid defensive checks. Add a guard only for a realistic failure mode.

## Comment Guidelines

Add commments for providing additional context to future coding agents or human readers. Do not write any comments that's obvious or can be inferred from nearby code. Keep comments succinct, lower case and limited to 1 or max 2 lines, something that a staff or principal engineer would write. Add simple one line docstring on public structs, interfaces and methods that explains what it does.
