# AI Use Disclosure

## Tools used

This project was developed with an AI assistant using Copilot SDK in VS Code. The
assistant was used as a coding and review aid; all implementation decisions,
generated outputs, and validation results were reviewed by the author.

## Prompt history

The main prompts supplied during the project were:

1. "Map source identifiers to canonical IDs using exact and rule-based matching.
   Detect one-to-many conflicts, retain mapping history, and expose unresolved
   mappings."
2. "Deliver a working Data Engineering solution within 72 hours" with requirements
   covering schemas, ingestion, validation, quarantine, idempotency, tests,
   sample data, reconciliation, performance, and documentation.
3. "Check if it is working properly and follows everything" against the project
   statement and acceptance criteria.
4. "What are further instructions for me?" for submission preparation.

The assistant's repository review identified one Windows-specific reproducibility
issue: `expected_summary.json` was logically identical but had different newline
bytes when generated on Windows. The author reviewed the diagnosis and accepted
the narrowly scoped fix to write the generated JSON with LF line endings.

## Where AI accelerated the work

- Mapped the business requirements to repository modules and acceptance criteria.
- Suggested a modular DuckDB pipeline structure and review checklist.
- Helped inspect the matching, validation, publishing, and reporting paths.
- Helped identify missing edge-case coverage and verify the evidence artifacts.
- Helped diagnose the cross-platform fixture reproducibility failure.
- Assisted with concise documentation of setup, trade-offs, and limitations.

## Author review and ownership

The author reviewed and owns:

- source and target schemas, keys, nullability, and lineage fields;
- normalization and matching rules and their priority order;
- conflict, unresolved, duplicate, and late-arrival behavior;
- SCD Type 2 history and idempotent rerun design;
- transaction boundaries, failure handling, and operational metrics;
- test scenarios, expected labels, reconciliation checks, and performance claims;
- the final README, data dictionary, design note, and submission artifacts.

AI-generated suggestions were not accepted automatically. The implementation was
checked against the business rules and existing repository patterns, and unrelated
changes were not included.

## Validation performed

The final validation included:

```text
python -m pytest -q
169 passed
```

Additional evidence is committed under `output/`:

- `output/test_results.txt` — test execution evidence;
- `output/coverage.txt` — coverage summary;
- `output/sample_run/report.md` — sample reconciliation report;
- `output/report.md` — larger generated dataset report;
- `output/perf.md` — performance measurements and optimization discussion.

The reports include hard reconciliation checks, deterministic rebuild validation,
quarantine traceability, conflict and unresolved counts, and quality metrics.
