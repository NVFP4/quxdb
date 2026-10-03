# AGENTS.md

This is QuxDB, a high-performance LSM database written in Go 1.26.

## Guidelines

- This is a research effort, so focus on broader LSM technology. Do not replicate implementation of specific products like RocksDB/LevelDB.
- Avoid defensive checks. Add a guard only for a realistic failure mode.

## Comment Guidelines

Comment only to add context a reader can't get from nearby code. Keep it lower case, one line (two at most), in the fewest words that carry the point: state the fact, not the reasoning behind it, e.g. `// set only by catalog snapshots`. No semicolons. Give public structs, interfaces and methods a simple one line docstring saying what they do.
