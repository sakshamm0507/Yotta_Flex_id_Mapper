# AI Disclosure

- **Author and architect:** Saksham Mehrotra (mehrotrasaksham0507@gmail.com)
- **AI tool used:** Claude Code (Anthropic's coding agent), running Claude Opus 5.5
- **Period:** October 2026

## Summary

I took all of the design and architecture decisions in this project. I used AI in two ways:

1. **Brainstorming.** I explored options and trade-offs with the AI tool, then decided which
   approach to take.
2. **Implementation.** I wrote the project plan, which defines the architecture, data model,
   matching rules, failure semantics, test plan and acceptance checks. Claude Code wrote the code,
   tests and documentation from my specification and ran them.

AI made the build faster. Its output was then held to deterministic checks that do not depend on
the AI: nothing it produced is trusted without an independent test, and the pipeline itself
contains no AI. I set the direction and the constraints, I accepted the result, and I am
responsible for every line in this repository.

## What I did

- I took every design and architecture decision, including which of the AI tool's suggestions to adopt.
- I framed the problem and chose the stack, with my reasons: Python, DuckDB, pytest, click, YAML.
- I defined the domain (three source systems mapped to a canonical customer master) and the seven
  working assumptions.
- I designed the architecture. Each batch file is processed in one transaction through a fixed stage
  sequence. Rejects go to quarantine, conflicts and unresolved keys go to their own tables, and
  mapping history is SCD Type 2.
- I designed the data model: every core table, its columns and keys, and the reject reason codes.
- I specified the normalization rules, the five matching rules and their priority order, and the
  resolution semantics: conflicts are surfaced and never auto-resolved, and existing mappings are kept.
- I specified the idempotency, retry, restart and backfill behavior, and the failure-injection mechanism.
- I specified the synthetic data scenarios and injection rates, the 22 test scenarios, the
  reconciliation checks, the quality metrics, the observability requirements and the performance
  comparison.
- I set the constraints: runs locally, no credentials, synthetic data only, and every line explainable.
- I directed the implementation and accepted it.

## How AI was used

Six practices made the AI's speed safe to use. Each one is visible in the repository.

**1. Specification first, code second.** The AI worked from a written plan that fixed the
architecture, every table and column, the rules, the 22 test scenarios and the acceptance checks.
It implemented a specification rather than inventing a design. This removed guesswork and gave
every component a definition of done.

**2. AI at build time only; the pipeline is deterministic.** The pipeline makes no model calls.
Matching is exact rules on normalized keys, mapping ids are content hashes, history uses event time,
test data comes from a fixed seed, and every dependency version is pinned. The AI's own variability
never reaches the output: the same inputs always produce the same result. Report check 7 proves this
on every run by rebuilding from scratch and comparing fingerprints of 8 tables.

**3. Nothing the AI wrote is trusted without an independent check.**
- *Independent ground truth.* The data generator never imports the pipeline. Its labels come from
  how the data was designed, so the pipeline cannot agree with them by sharing a bug. Result: 100%
  agreement, 97,243 of 97,243 keys.
- *Two implementations of the core logic.* Normalization and matching exist once as set-based SQL
  and once as row-by-row Python. Tests and the benchmark require byte-identical decisions at 10k,
  100k and 1M rows. A 33-input parity test covers accents, NFD, Greek, Cyrillic, CJK, Devanagari,
  Turkish İ and Unicode spaces.
- *Checks that are themselves tested.* Each of the 12 reconciliation checks has a test that corrupts
  the database in one specific way and asserts that the check fails. A check that cannot fail
  proves nothing.
- *Failure at every stage.* A failure is injected at each of the 10 pipeline stages. The tests assert
  that nothing is left half-written and that the retry ends in exactly the state of a clean run.

**4. Verify assumptions before building on them.** Before relying on DuckDB features, the AI ran
small probes against the pinned version. They found three things that would otherwise have been
silent bugs:
- DuckDB accepts `'epoch'` as a valid timestamp, so the pipeline now requires ISO-8601 format.
- Python and DuckDB lowercase `İ` and `Σ` differently, so the Python implementation maps both
  characters explicitly.
- Fetching time-zone-aware timestamps into Python needs an extra library, so the pipeline converts
  to UTC inside SQL.

**5. Measure before optimizing.** Profiling showed that the Python CSV pass was the largest cost, and
that most of it came from a per-character check for invalid bytes. Replacing that loop with a single
regex halved the pass. Building the JSON payload only for rejected rows saved more. Together these
cut the 1M-row run from 29.9 s to 25.2 s and peak memory from 2.5 GB to 2.0 GB, with no change in
output.

**6. Try to break it, then guard the release.**
- Adversarial probes, all handled correctly: Windows line endings, multi-line quoted fields,
  trailing-comma headers, quotes and spaces in file paths, equal-timestamp updates applied
  alternately, forced reruns after newer files, and forced reruns that fail.
- The full chain (`make setup generate run test report`) was run on a clean copy of the repository.
- Every file was scanned for local machine paths, credentials and personal data before packaging.
- Python 3.11 compatibility was checked statically with a tokenizer scan, which was first validated
  against code known to be incompatible. Tests ran on Python 3.14.

### Errors the guardrails caught

These are the AI's mistakes or oversights during the build, each caught by the process before it
reached the result:

| What happened | Caught by |
|---|---|
| A test fixture expected a late arrival, but its two records had equal timestamps. The code was right and the test was wrong | The failing test, investigated before changing anything |
| A refactor made a SQL column reference ambiguous | The test suite, immediately |
| The determinism check skipped silently when reference files came from command-line flags | Reading the report output. Fixed to replay with the reference paths recorded in the database |
| Report and test output contained absolute local paths | The pre-packaging scan. Fixed to write repository-relative paths |

### Efficiency

The AI carried the implementation: 15 modules, the data generator, the benchmark harness, 169 tests
and three reference documents, built from my plan. That let me spend my time on design decisions and
review. Long jobs, such as the 1M-row benchmark, ran in the background while documentation was
written. The build was complete and verified ahead of my planned code freeze.

## What the AI tool did

- Brainstormed with me: discussed possible approaches and their trade-offs. It made no design or
  architecture decisions.
- Wrote the implementation of my plan: the Python package in `src/idmap/`, `scripts/generate.py`,
  `scripts/perf.py`, the `Makefile` and `config/pipeline.yaml`.
- Wrote the test suite: the 22 scenarios from my plan in `tests/test_pipeline.py`, plus additional
  tests (169 in total).
- Drafted `README.md`, `DESIGN.md` and `DATA_DICTIONARY.md` from my plan and the code.
- Ran the pipeline, tests, report and performance harness, and produced the outputs in `output/`.
- Proposed the implementation-level refinements below.

## Refinements proposed during implementation

The AI tool suggested these details while implementing my design. I decided to adopt each one, and
each is documented where it applies.

| Refinement | Documented in |
|---|---|
| Late arrivals are detected against the newest evidence already applied to a key, not against the current mapping's `valid_from`. My plan's original rule would let stale data override a newer confirmation | DESIGN.md §4 |
| Ground-truth labels carry both the expected outcome and the true entity, so spec conformance and precision/recall are measured separately | DESIGN.md §10 |
| Five reconciliation checks beyond the seven in my plan (1b, 1c, 8, 9, 10) | README, *Data quality* section |
| A reject-rate circuit breaker, and detection of runs left RUNNING by a killed process | DESIGN.md §8 |
| Extra traceability columns (`status_reason`, `resolved_run_id`, `existing_canonical_id` and others) and the `reference_load` table | DATA_DICTIONARY.md |
| Using my planned Python row-by-row baseline as a parity check against the SQL engine, not only as a timing comparison | DESIGN.md §10, `output/perf.md` |

## What I did not use AI for

- **No AI at runtime.** Matching is deterministic rules. The pipeline makes no model calls and no
  network calls.
- **No real or personal data.** I used only synthetic data, produced by the seeded generator in this
  repository.
- **No hand-written labels.** Ground-truth labels are computed by `scripts/generate.py` from a fixed
  seed. They are reproducible and were not edited by hand.
- **No credentials, services or external accounts.**

## Evidence anyone can reproduce

- `make test`: 169 tests pass. Output is in `output/test_results.txt`, line coverage is 96%.
- `make report`: all 12 hard reconciliation checks pass, including the rebuild-from-scratch
  determinism check.
- The pipeline agrees with the generator's independent labels on every key: 97,243 of 97,243 in the
  100k dataset and 154 of 154 in the committed sample.
- `make perf`: the Python baseline and the DuckDB engine make byte-identical decisions at 10k, 100k
  and 1M rows; the set-based engine is 3.9x faster at 1M.

## Ownership

I have reviewed the implementation and the design decisions, and I can explain them. `DESIGN.md`
records the reasoning behind each decision.
