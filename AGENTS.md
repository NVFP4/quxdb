# AGENTS.md

This is QuxDB, a high-performance LSM database written in Go 1.26.

## Guidelines

### Focus on broader technology, not individual products

Do not over-index on implementation details of specific products like RocksDB/LevelDB.

When explaining or comparing designs of individual systems as examples, keep it focused on:
- Broader technology and design space
- Current areas of research interest
- Clever techniques used in production-grade systems engineering
- Tradeoffs and constraints typical to high-performance systems

### Avoid redundant defensive checks

Do not add defensive branches for states that are impossible or excluded by the program's contract.

Example 1: If an API input/output is guaranteed to be a non-negative, monotonically increasing integer, do not repeatedly check whether it is negative. That condition is outside the valid state space, and should not influence the implementation.

Example 2: If an API requires `minKey <= maxKey`, callers must provide the values in that order. Do not re-order them or add defensive handling throughout the call stack. An unordered pair indicates a bug in the caller.

Defensive branches have costs:
- They make invalid states appear legitimate
- They can hide bugs by recovering from contract violations
- They increase the number of execution paths
- They add code and runtime work for conditions that should never occur

Add a guard only when the condition is a realistic input or failure mode. When a contract violation must be detected, check it at the earliest relevant boundary rather than at every layer.

Examples of valid guards:
- Validating user-controlled input at public API boundaries
- Validating persisted or externally sourced data when it is read
- Handling realistic I/O, resource, and system failures

### Explanations should be clear, complete, and accessible

Explain concepts in enough detail for the reader to build an accurate mental model. Do not sacrifice clarity for brevity.

Use natural prose for the main explanation. Add bullet points when they make steps, distinctions, examples, or key takeaways easier to scan.

Prefer plain, familiar language over jargon or unnecessarily complex terminology. When a specialized term is useful or unavoidable, define it in simple language when it first appears.

### Use code formatting only for code

Write explanations, notes, instructions, and other ordinary text as normal prose or bullet points.

Use code or code-blocks only for code-related snippets like variables/expressions/code snippet/modules.

Do not place ordinary prose inside inline code or fenced code blocks merely for emphasis or visual separation.

### Code Comments

Write concise, staff-engineer-level comments. Explain intent, constraints, tradeoffs, or non-obvious behavior—not what the code already states. Prefer a few precise words over lengthy explanations
