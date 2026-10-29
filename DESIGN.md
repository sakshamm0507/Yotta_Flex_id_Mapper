# Design Decisions and Trade-offs

This document explains why the pipeline is built the way it is. The README covers how to run it,
and DATA_DICTIONARY.md covers what each table holds.

| Question | Section |
|---|---|
| Why DuckDB over pandas, SQLite or Spark? | §2 |
| Why does `valid_from` come from event time, and how does that make reruns deterministic? | §3 |
| How is late-arriving data handled, and what is the bitemporal limitation? | §4 |
| How does rule priority stop weak rules overriding strong identifiers? | §5 |
| Why are conflicts not auto-resolved, and what is the resolution workflow? | §6 |
| Why both a checksum skip and an idempotent merge? | §7 |
| How does the transaction boundary guarantee no partial writes? | §8 |
| How are precision and recall computed, and why are labels kept separate? | §10 |
| What breaks at 100M rows, and what changes? | §12 |

## 1. Shape of the system

```
CSV batch -> [ingest] -> BEGIN [schema -> validate -> dedup -> normalize -> match -> resolve -> publish -> status -> metrics] COMMIT
```

- **One file = one run = one transaction.** A file is the unit of arrival, retry and audit, so it is
  also the unit of atomicity. Each stage is a small module with one job (`validate.py`, `match.py`,
  `resolve.py`, `publish.py`, ...). Stages exchange data through temp tables (`t_valid`, `t_stage`,
  `t_hits`, `t_decision`, `t_eval`), so any intermediate result can be inspected with SQL while debugging.
- **The database is the system of record.** The state tables (`id_mapping_history`, `mapping_conflict`,
  `unresolved_mapping`, `source_key_status`) are the product. The batch tables (`stg_source_record`,
  `quarantine_record`, `match_candidate`, `late_arrival_log`) explain how each state was reached.
  `pipeline_run` explains when and with what counts.

## 2. Why DuckDB (and not pandas, SQLite or Spark)

| Option | Why not |
|---|---|
| pandas | No transactions, so a failed batch would leave half-written CSVs or parquet. Everything sits in memory. Joins and window functions are less readable than SQL. |
| SQLite | Transactions, but row-oriented and single-threaded. Set-based normalization (regex over 1M rows) and hash joins are far slower. No list types for candidate sets. |
| Spark | The right tool past one machine, but it needs a cluster or JVM setup, which this challenge rules out (local, zero infra). |
| **DuckDB** | Embedded (one file, `pip install`), ACID transactions with transactional DDL, vectorized multi-threaded SQL, `INSERT ... ON CONFLICT`, `QUALIFY`, list types, RE2 Unicode regexes, NFC normalization. 1M rows fit comfortably. |

The SQL is deliberately plain (CTEs, equi-joins, window functions, upserts), so it ports to Spark SQL
or Databricks almost unchanged (§12).

## 3. Event time, not run time, drives history

`valid_from` is the record's `source_updated_at`, and `mapping_id = sha256(system|id|canonical|valid_from)`.
Two consequences:

1. **Reruns are deterministic.** Replaying the same inputs reproduces the same history rows, ids
   included, whatever the clock says. Report check 7 replays every succeeded run into a scratch
   database and compares fingerprints of 8 tables. Wall-clock columns (`recorded_at`, `ingested_at`,
   `quarantined_at`) are excluded. Time-based run ids are replaced by the checksum of the file each
   run processed, so lineage is still checked.
2. **History reflects the business timeline.** An interval answers "which canonical did the source
   say this record was, during which period".

## 4. Late arrivals: protect current state with a high-water mark

A record is **late** when its `source_updated_at` is older than `source_key_status.last_source_updated_at`,
the newest evidence already applied to that key, whatever its outcome. A late record changes nothing.
It is logged to `late_arrival_log` with the outcome it would have produced (`proposed_status`,
`proposed_canonical_id`).

This is stricter than comparing with the current mapping's `valid_from`, and the difference matters.
Suppose t1 maps the key to A, t3 confirms A (a no-op, so `valid_from` stays t1), and then a record
from t2 says B. Comparing with `valid_from` would apply B even though newer evidence (t3) says A.
The high-water mark rejects it (`test_late_arrival_does_not_override`, second half).

**Limitation, stated plainly:** this is not bitemporal history. A late record does not rewrite past
intervals, so history records when the pipeline learned things, ordered by event time, with stale
evidence kept on the side. Full bitemporality would add `recorded_from/recorded_to` and re-derive
intervals on backfill.

Equal timestamps are not late. Two inputs with the same event time and different payloads are applied
in processing order, which is deterministic (filename order). In the edge case where both are applied
alternately, the ON CONFLICT clause reopens the existing `mapping_id` instead of duplicating it.

## 5. Matching: deterministic rules, priority, blocking keys

- **Rules P0 to P4** (override, tax id, email, name+domain, phone+first name token) are exact equality
  on normalized keys. There is no fuzzy scoring, so every mapping can be explained by one row in
  `match_candidate` (`matched_value` shows the key).
- **Blocking keys instead of comparisons.** Each rule turns a record into `(rule_id, key_value)`.
  Canonicals get the same pairs in `canonical_key`, and candidate generation is a single hash join.
  Cost grows linearly with records. Nothing is pairwise.
- **Priority prevents weak rules from overriding strong identifiers.** For each key only the
  highest-priority rule with an active hit is consulted. If the tax id says A, a coincidental
  phone+name hit on B is irrelevant (`test_priority_precedence`). If the deciding rule is ambiguous,
  the key is a conflict even when a weaker rule would be unambiguous. Weaker evidence must never
  break a tie that stronger evidence could not.
- **Only active canonicals are candidates.** Inactive hits are kept in `t_hits` only to give the
  precise reason `INACTIVE_CANONICAL_ONLY`.
- **Rules sharing a priority pool their hits.** This is configurable, but both engines implement it
  identically and the tests cover it.
- `canonical_ref` (the legacy pointer) is validated but deliberately **not** a rule. It is a
  pre-existing claim of unknown quality, so the report measures how often it agrees with the mapping
  (about 90% on generated data) and leaves the decision to the rules.

## 6. Conflicts are surfaced, never auto-resolved

Auto-picking a candidate (first id, newest, most hits) would turn ambiguity into silent, confident
errors, which are the most expensive kind in master data. Instead:

- `ONE_TO_MANY` creates an OPEN `mapping_conflict` row with the sorted candidate list and the
  deciding rule. The key gets no new mapping.
- If the key already had a mapping, **it is kept** (`has_existing_mapping = true`). A degraded record,
  for example one whose tax id was blanked, should not unmap a key that previously had strong evidence.
- **Resolution workflow:** a data steward reviews the OPEN conflicts (one query, in README) and
  records the decision in `mapping_override` (rule P0, highest priority). The next run maps the key,
  and the conflict flips to RESOLVED with `resolved_run_id`. The generated data includes overridden
  conflicts that demonstrate this loop end to end. In production this would sit behind a review UI
  with an approval trail. Here the override file is the trail.
- `MANY_TO_ONE_SAME_SYSTEM` (two CRM ids pointing to one company) is a duplicate-account signal for
  the source system's owners. It is a warning: both mappings stay valid.

## 7. Idempotency: checksum skip and idempotent merge (defense in depth)

- **Checksum skip:** a file whose SHA-256 already SUCCEEDED is recorded as a SKIPPED run and nothing
  else happens. This is cheap and catches the common case, including the same content under a new name.
- **Idempotent merge:** `--force` bypasses the skip. The run still produces no duplicates because
  - `mapping_id` is deterministic and an unchanged mapping is a no-op;
  - conflicts and unresolved rows are keyed per business key and upserted;
  - batch tables are replaced per file checksum;
  - `attempt_count` does not count reprocessing of the same file.
  `test_forced_rerun_no_duplicates` asserts identical fingerprints before and after.

Why both? The checksum protects against accidental reprocessing. The merge protects against the
cases the checksum cannot see: forced reruns, backfills that overlap earlier files, and a bug in the
registry. Either one alone would be a single point of failure.

## 8. Failure, retry, restart

- `pipeline_run` gets its RUNNING row **before** BEGIN, so a crash is visible. Every stage and the
  final `SUCCEEDED` update run **inside** one transaction, so SUCCEEDED is set exactly when the data
  committed.
- On any exception: ROLLBACK, then the run row becomes FAILED with `failed_stage` and
  `<stage>: <ExceptionClass>: <message>`, and `processed_file` records FAILED so the next run retries.
  `test_partial_failure_then_retry` injects a failure at **each of the 10 stages**
  (`IDMAP_FAIL_AT_STAGE`). Each time it asserts zero partial writes, then that the retry reaches
  exactly the state of a clean run.
- A process killed mid-run leaves a RUNNING row. At the next start it is marked FAILED (abandoned).
  DuckDB's file lock guarantees no other writer could own it. The transaction was never committed,
  so there is nothing to clean up.
- **In-run reconciliation.** Before committing, the metrics stage asserts
  `rows_read = staged + quarantined + duplicates` and `staged = mapped + conflicts + unresolved + late`.
  A violation rolls the batch back rather than committing unreconciled data.
- **Circuit breaker.** If more than `max_reject_rate` (50%) of a file's rows fail validation, the
  file is treated as broken (wrong export, wrong delimiter), not as data, and the run fails.

## 9. Data quality and traceability

- One reason code per rejected row, chosen by a documented precedence (validate.py docstring).
- Every quarantined row has `run_id`, `source_file`, the **physical line number** and the original
  untrimmed values. `quarantine_id` is deterministic. Line numbers come from a Python csv pass,
  because DuckDB's native reader does not expose line numbers for accepted rows, and quoted fields
  can span lines.
- Exact duplicates (identical after trimming) are dropped and counted, not quarantined, because they
  carry no new information. Conflicting versions of a key are quarantined as `DUPLICATE_KEY_SUPERSEDED`
  with a pointer to the winning row.
- Foreign keys are enforced by checks, not DDL. DuckDB forbids updating a parent row that has
  children, which would block refreshing the master. Report checks 4 and 8 assert referential
  integrity on every report.

## 10. Measuring quality honestly: two kinds of labels

The generator writes labels **outside the input files** (`expected/`). The pipeline never reads them.
Each key has two expectations:

1. **`expected_status` / `expected_canonical_id`:** what a correct implementation of the rules must
   produce, given how the inputs were designed. Agreement here is **spec conformance**. It is 100%
   on the sample and on generated datasets, and tests assert it for several seeds.
2. **`true_canonical_id`:** the entity the record really belongs to. This gives **precision and
   recall of the rules themselves**, and they are deliberately below 100%:
   - designed one-to-many conflicts and dirty names lower **recall** (conservative by design);
   - designed phone+name collisions lower **precision** of `rule_phone_name` (94% vs 100% for the
     exact rules). That is why P4 has the lowest priority.

Precision = current mappings pointing at the true entity / all current mappings. Recall = keys
mapped to their true entity / keys whose true entity is an active canonical and that were not
quarantined. Per rule, precision is grouped by the rule that produced the mapping and recall by the
rule that should have decided.

The generator does not import the pipeline. Noise (case, punctuation, suffix variants, phone and URL
formats, NFD Unicode, padding) is constructed to disappear under the documented normalization. The
expected outcome comes from applying the rule order to the designed key values, so the pipeline has to
recover the same answer from noisy text. A second Python implementation of normalization and
matching (`baseline.py`) is checked byte for byte against the SQL engine.

## 11. Observability

- `pipeline_run` is the single source of truth: status, timing, every count, failure stage and reason.
- One JSON log line per stage (`run_id`, `stage`, `rows_in`, `rows_out`, `duration_ms`, `level`, plus
  stage-specific counts) in `output/logs/<run_id>.jsonl`, with human-readable lines on stderr.
- `make status` shows recent runs. `make report` turns the database into a reviewable Markdown report
  and exits non-zero on a broken invariant, so it can gate a scheduler.

## 12. What breaks at 100M rows, and what to change

| Breaks first | Change |
|---|---|
| Python csv pass (single-threaded; about 30% of runtime at 1M) | DuckDB/Spark native CSV reader with a rejects table. Carry line numbers via a pre-split or a byte-offset index |
| Global MANY_TO_ONE recompute each run | Recompute only for canonicals touched by the batch |
| Unchanged re-sent records still go through every stage | Hash each record's matching attributes and skip keys whose hash and timestamp are unchanged |
| One machine's memory and disk | Spark/Databricks: blocking-key joins become shuffle joins on `(rule_id, key_value)`; SCD2 becomes a Delta `MERGE`; the per-file transaction becomes a Delta commit; state tables are partitioned by `source_system` |
| Hot blocking keys (a shared mailbox matching thousands of canonicals) | Cap candidate fan-out per key and route the key straight to conflict review; salt hot keys in the shuffle |

## 13. Things deliberately not built

- Fuzzy, phonetic or ML matching: recall on dirty names is capped, and the report shows the cost.
- Survivorship or golden-record creation: the master is an input.
- A conflict-resolution UI: overrides are a reviewed CSV.
- Bitemporal history (§4).
