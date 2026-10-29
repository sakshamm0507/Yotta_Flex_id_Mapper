# Data Dictionary

Every table in the DuckDB database (`data/idmap.duckdb`), plus the input and label file formats.
Conventions: `snake_case` throughout. Timestamps are `TIMESTAMP` holding **UTC** wall time; inputs with an
offset are converted on load. Operational rows carry `run_id` and `source_file` for lineage. **PK** means a
declared primary key. **Logical key** means unique by construction and asserted by report check 10.
Foreign keys are not declared (see DESIGN.md §9); report checks 4 and 8 enforce them.

The business key everywhere is **(`source_system`, `source_id`)**.

---

## 1. Input: source batch CSV (contract)

One file per batch, UTF-8 (a BOM is accepted), comma-separated, RFC 4180 quoting, header row required.
Columns are matched by header name (case-insensitive, trimmed), so order does not matter.

| Column | Type | Required | Rule |
|---|---|---|---|
| `source_system` | VARCHAR | yes | Trimmed and lowercased, then one of `validation.allowed_source_systems` (`crm`, `billing`, `support`) |
| `source_id` | VARCHAR | yes | Unique within `source_system` |
| `name` | VARCHAR | no | Organization name |
| `email` | VARCHAR | no | Must match the email pattern and be at most 254 characters, if present |
| `phone` | VARCHAR | no | Only digits, spaces, `( ) . -` and a leading `+`, with 8 to 15 digits after normalization, if present |
| `domain` | VARCHAR | no | Website or domain, in any URL form |
| `tax_id` | VARCHAR | no | Strongest identifier |
| `canonical_ref` | VARCHAR | no | Legacy pointer. Must exist in `canonical_entity` if present. Not used for matching; the report measures its agreement |
| `source_updated_at` | TIMESTAMP | yes | ISO-8601 (`YYYY-MM-DD[THH:MM[:SS[.f]]][Z\|±HH[:MM]]`). Naive values are UTC |

Whitespace-only values are NULL (Unicode-aware trim). A missing required column fails the run
(`SCHEMA_MISSING_COLUMN`), and so does a duplicated column (`SCHEMA_DUPLICATE_COLUMN`). An extra
column is a schema warning and is dropped. A missing optional column is a schema warning and is read as NULL.

### Reference inputs

`canonical_entity.csv` has the columns of the `canonical_entity` table below. `is_active` is `true` or `false`.
`mapping_override.csv` has the columns `source_system, source_id, canonical_id, reason, created_at`. A reference load fails
loudly (`ReferenceDataError`, nothing changes) on duplicate `canonical_id`s, missing ids or names, an invalid `is_active`,
or overrides pointing at unknown canonicals.

---

## 2. Reference tables

### `canonical_entity`: canonical customer master (provided, not built here)
| Column | Type | Null | Key | Meaning |
|---|---|---|---|---|
| `canonical_id` | VARCHAR | no | **PK** | e.g. `can_000001` |
| `legal_name` | VARCHAR | no | | Registered name |
| `tax_id` | VARCHAR | yes | | Tax identifier, as given |
| `primary_email` | VARCHAR | yes | | Primary mailbox, the target of rule P2 |
| `domain` | VARCHAR | yes | | Website domain |
| `phone` | VARCHAR | yes | | Main phone |
| `is_active` | BOOLEAN | no | | Only active canonicals can be match candidates |
| `created_at` | TIMESTAMP | yes | | Master record creation time |

### `canonical_key`: blocking index derived from the master
| Column | Type | Null | Key | Meaning |
|---|---|---|---|---|
| `rule_id` | VARCHAR | no | logical (`rule_id`, `key_value`, `canonical_id`) | `exact_tax_id`, `exact_email`, `rule_name_domain` or `rule_phone_name` |
| `key_value` | VARCHAR | no | | Normalized key, the same expression the source side uses (see match.py) |
| `canonical_id` | VARCHAR | no | | Canonical the key belongs to |
| `is_active` | BOOLEAN | no | | Copied from the master, so inactive hits can be reported |

Rebuilt on every reference reload. Two canonicals with the same `key_value` under one rule produce a one-to-many conflict for any source key that decides on that rule.

### `mapping_override`: manual crosswalk (rule P0)
| Column | Type | Null | Key | Meaning |
|---|---|---|---|---|
| `source_system` | VARCHAR | no | **PK** (`source_system`, `source_id`) | |
| `source_id` | VARCHAR | no | | |
| `canonical_id` | VARCHAR | no | | Steward's decision. Must exist in the master |
| `reason` | VARCHAR | yes | | Why, e.g. `steward resolved conflict` |
| `created_at` | TIMESTAMP | yes | | |

### `reference_load`: which reference files are loaded
| Column | Type | Null | Key | Meaning |
|---|---|---|---|---|
| `reference_name` | VARCHAR | no | **PK** | `canonical_entity` or `mapping_override` |
| `source_file` | VARCHAR | no | | Path loaded. The determinism replay reuses it |
| `file_sha256` | VARCHAR | no | | Checksum, so an unchanged file is not reloaded (`none` = no override file) |
| `row_count` | BIGINT | no | | Rows loaded |
| `loaded_at` | TIMESTAMP | no | | |

---

## 3. Operational tables

### `pipeline_run`: one row per attempt to process a file (single source of truth for run status)
| Column | Type | Null | Meaning |
|---|---|---|---|
| `run_id` | VARCHAR | no | **PK**, `run_<UTC yyyymmddThhmmssmmm>Z_<8 hex>` |
| `run_seq` | BIGINT | no | Monotonic order of runs (replay order) |
| `source_file` | VARCHAR | no | Repo-relative path of the batch |
| `file_sha256` | VARCHAR | yes | Content checksum. NULL only if the file could not be read |
| `reference_sha256` | VARCHAR | yes | Combined checksum of master + overrides used by the run |
| `forced` | BOOLEAN | no | Run with `--force` |
| `status` | VARCHAR | no | `RUNNING`, `SUCCEEDED`, `FAILED` or `SKIPPED` |
| `started_at`, `ended_at` | TIMESTAMP | no / yes | |
| `duration_ms` | BIGINT | yes | |
| `rows_read` | BIGINT | yes | Data records in the file, malformed lines included (blank lines are not records) |
| `rows_staged` | BIGINT | yes | Rows written to `stg_source_record`, one per key |
| `rows_quarantined` | BIGINT | yes | Rows written to `quarantine_record` |
| `duplicates_dropped` | BIGINT | yes | Exact duplicate rows collapsed |
| `mapped`, `conflicts`, `unresolved` | BIGINT | yes | Outcomes of non-late keys in this run |
| `late_arrivals` | BIGINT | yes | Keys whose record was older than already-applied evidence |
| `mappings_inserted`, `mappings_closed` | BIGINT | yes | SCD2 rows opened and closed |
| `schema_warnings` | BIGINT | yes | Extra or missing-optional columns |
| `many_to_one_open` | BIGINT | yes | Open MANY_TO_ONE_SAME_SYSTEM flags after the run |
| `failed_stage` | VARCHAR | yes | Stage that raised (FAILED only) |
| `failure_reason` | VARCHAR | yes | `<stage>: <ExceptionClass>: <message>` |
| `status_detail` | VARCHAR | yes | For SKIPPED: which run already processed the checksum |

Invariants (report checks 1 and 1b): `rows_read = rows_staged + rows_quarantined + duplicates_dropped` and
`rows_staged = mapped + conflicts + unresolved + late_arrivals`. Both are also enforced inside the run, and the
transaction rolls back if either fails.

### `processed_file`: idempotency registry
| Column | Type | Null | Meaning |
|---|---|---|---|
| `file_sha256` | VARCHAR | no | **PK** |
| `source_file` | VARCHAR | no | Latest path seen for this content |
| `run_id` | VARCHAR | no | Latest run that processed it |
| `status` | VARCHAR | no | `SUCCEEDED` (skip next time) or `FAILED` (retry next time) |
| `processed_at` | TIMESTAMP | no | |

### `stg_source_record`: validated, deduplicated, normalized rows (logical key `run_id, source_system, source_id`)
| Column | Type | Null | Meaning |
|---|---|---|---|
| `run_id`, `source_file` | VARCHAR | no | Lineage |
| `source_row_num` | BIGINT | no | Physical line in the file where the record starts (header = line 1) |
| `source_system`, `source_id` | VARCHAR | no | Business key (system lowercased) |
| `name`, `email`, `phone`, `domain`, `tax_id`, `canonical_ref` | VARCHAR | yes | Input values, trimmed |
| `source_updated_at` | TIMESTAMP | no | Event time (UTC) |
| `tax_id_norm` | VARCHAR | yes | Uppercase, without whitespace or hyphens |
| `email_norm` | VARCHAR | yes | Lowercase |
| `phone_norm` | VARCHAR | yes | E.164-style `+<cc><digits>` |
| `name_norm` | VARCHAR | yes | NFC, lowercase, non-alphanumerics replaced by spaces, legal suffixes dropped |
| `domain_norm` | VARCHAR | yes | Host only: no scheme, `www.`, path, port or edge dots |
| `ingested_at` | TIMESTAMP | no | Wall clock (excluded from determinism fingerprints) |

### `quarantine_record`: rejected rows, fully traceable (logical key `quarantine_id`)
| Column | Type | Null | Meaning |
|---|---|---|---|
| `quarantine_id` | VARCHAR | no | `sha256(file_sha256 ':' source_row_num)`, deterministic |
| `run_id`, `source_file` | VARCHAR | no | Lineage |
| `source_row_num` | BIGINT | no | Physical start line |
| `raw_payload` | JSON | yes | Original untrimmed values by column, or `{"fields": [...]}` for malformed lines (undecodable bytes shown as `\xNN`) |
| `reject_reason_code` | VARCHAR | no | See §6 |
| `reject_detail` | VARCHAR | yes | e.g. `email=bad@`, `expected 9 columns, found 10`, `superseded by row 12 (...)` |
| `quarantined_at` | TIMESTAMP | no | Wall clock |

### `match_candidate`: every active rule hit, for explainability (logical key `run_id, source_system, source_id, rule_id, canonical_id`)
| Column | Type | Null | Meaning |
|---|---|---|---|
| `run_id` | VARCHAR | no | |
| `source_system`, `source_id` | VARCHAR | no | |
| `canonical_id` | VARCHAR | no | Candidate (active canonicals only) |
| `rule_id` | VARCHAR | no | Rule that hit |
| `rule_priority` | INTEGER | no | Its configured priority (lower wins) |
| `matched_value` | VARCHAR | yes | The normalized key that matched, e.g. `blue harbor logistics\|blueharbor.example` |

### `late_arrival_log`: records older than already-applied evidence (logical key `run_id, source_system, source_id`)
| Column | Type | Null | Meaning |
|---|---|---|---|
| `run_id`, `source_system`, `source_id` | VARCHAR | no | |
| `record_updated_at` | TIMESTAMP | no | The late record's event time |
| `latest_applied_updated_at` | TIMESTAMP | no | Newest evidence already applied for the key |
| `current_valid_from` | TIMESTAMP | yes | `valid_from` of the current mapping (NULL if unmapped) |
| `current_canonical_id` | VARCHAR | yes | Current mapping kept |
| `proposed_status` | VARCHAR | no | What the late record would have produced: `MAPPED`, `CONFLICT` or `UNRESOLVED` |
| `proposed_canonical_id` | VARCHAR | yes | Its canonical, if MAPPED |
| `action` | VARCHAR | no | `IGNORED_FOR_CURRENT` |

Batch tables (`stg_source_record`, `quarantine_record`, `match_candidate`, `late_arrival_log`) belong to one file's content.
A `--force` rerun replaces the rows of earlier runs of the same checksum, so they never duplicate.

---

## 4. State tables

### `id_mapping_history`: SCD Type 2 mapping history
| Column | Type | Null | Meaning |
|---|---|---|---|
| `mapping_id` | VARCHAR | no | **PK**, `sha256(source_system\|source_id\|canonical_id\|valid_from)`, deterministic |
| `source_system`, `source_id` | VARCHAR | no | Business key |
| `canonical_id` | VARCHAR | no | Mapped canonical |
| `match_rule` | VARCHAR | no | Rule that created this version (rules sharing a priority are joined with `+`) |
| `valid_from` | TIMESTAMP | no | Event time of the evidence (not the run time) |
| `valid_to` | TIMESTAMP | yes | NULL while current, else the next version's `valid_from` (half-open interval) |
| `is_current` | BOOLEAN | no | At most one TRUE per key (check 3) |
| `created_run_id` | VARCHAR | no | Run that opened the version |
| `closed_run_id` | VARCHAR | yes | Run that closed it |
| `recorded_at` | TIMESTAMP | no | Wall clock, audit only, excluded from determinism |

**View `id_mapping_current`** = `SELECT * FROM id_mapping_history WHERE is_current`: the table consumers read.

### `mapping_conflict`: surfaced, never auto-resolved
| Column | Type | Null | Meaning |
|---|---|---|---|
| `conflict_id` | VARCHAR | no | **PK**, `sha256(conflict_type\|source_system\|source_id)`: one row per key and type |
| `conflict_type` | VARCHAR | no | `ONE_TO_MANY` (the deciding rule hit more than one canonical) or `MANY_TO_ONE_SAME_SYSTEM` (several ids in one system map to one canonical; a warning) |
| `source_system`, `source_id` | VARCHAR | no | |
| `candidate_canonical_ids` | VARCHAR[] | no | Sorted candidates (ONE_TO_MANY) or the shared canonical (MANY_TO_ONE) |
| `rule_id` | VARCHAR | no | Deciding rule (ONE_TO_MANY) or the mapping's rule (MANY_TO_ONE) |
| `has_existing_mapping` | BOOLEAN | no | The key keeps an earlier mapping |
| `existing_canonical_id` | VARCHAR | yes | That mapping |
| `status` | VARCHAR | no | `OPEN` or `RESOLVED` (reopened if it recurs) |
| `first_detected_run_id`, `last_detected_run_id` | VARCHAR | no | |
| `resolved_run_id` | VARCHAR | yes | Run in which it stopped occurring |

### `unresolved_mapping`: unresolved backlog
| Column | Type | Null | Meaning |
|---|---|---|---|
| `source_system`, `source_id` | VARCHAR | no | **PK** |
| `reason` | VARCHAR | no | `NO_CANDIDATE`, `INSUFFICIENT_ATTRIBUTES` or `INACTIVE_CANONICAL_ONLY` |
| `has_existing_mapping` | BOOLEAN | no | The key keeps an earlier mapping |
| `status` | VARCHAR | no | `OPEN` or `RESOLVED` |
| `first_seen_run_id`, `last_seen_run_id` | VARCHAR | no | |
| `resolved_run_id` | VARCHAR | yes | |
| `attempt_count` | INTEGER | no | Evaluations from different batches that left the key unresolved. Reprocessing the same file does not count |

### `source_key_status`: one row per business key, the reconciliation anchor
| Column | Type | Null | Meaning |
|---|---|---|---|
| `source_system`, `source_id` | VARCHAR | no | **PK** |
| `status` | VARCHAR | no | Outcome of the latest applied evidence: `MAPPED`, `CONFLICT` or `UNRESOLVED` |
| `status_reason` | VARCHAR | no | Rule (MAPPED), `ONE_TO_MANY` (CONFLICT) or unresolved reason |
| `canonical_id` | VARCHAR | yes | Current mapping. It can be set for CONFLICT or UNRESOLVED when an earlier mapping is kept |
| `last_run_id` | VARCHAR | no | Run that applied the latest evidence |
| `last_source_updated_at` | TIMESTAMP | no | That evidence's event time. Older records are late arrivals |

Check 2: distinct keys in `stg_source_record` = rows here = MAPPED + CONFLICT + UNRESOLVED.

### `schema_info`
| Column | Type | Null | Meaning |
|---|---|---|---|
| `schema_version` | INTEGER | no | DDL version (currently 1) |

---

## 5. Ground-truth files (generator output, kept separate from the inputs)

`expected/expected_labels.csv`: one row per business key the generator created.
| Column | Meaning |
|---|---|
| `source_system`, `source_id` | Business key |
| `expected_status` | What a correct implementation of the rules produces after all batches: `MAPPED`, `CONFLICT`, `UNRESOLVED` or `QUARANTINED` (every row of the key rejected) |
| `expected_canonical_id` | Expected current mapping (may be set for CONFLICT or UNRESOLVED when an earlier mapping is kept) |
| `expected_rule` | Rule that creates the expected mapping, or the conflicting rule |
| `true_canonical_id` | The entity the record really belongs to (empty = not in the master). Used for precision and recall |
| `scenario` | Scenario that produced the key, e.g. `one_to_many_email`, `dirty_name`, `late_arrival` |

`expected/expected_quarantine.csv` lists `source_file, source_row_num, expected_reason_code` for every row that must be rejected.
`expected/expected_summary.json` holds designed volumes (exact duplicates, late arrivals, many-to-one keys, counts per scenario).

---

## 6. Enumerations

**Reject reason codes** (one per row; the first failing rule in this order wins):

| Code | Level | Trigger |
|---|---|---|
| `SCHEMA_MISSING_COLUMN` | batch, fails the run | Required column absent, or no header |
| `SCHEMA_DUPLICATE_COLUMN` | batch, fails the run | Column named twice |
| `MALFORMED_ROW` | row | Wrong column count, unparseable line, invalid UTF-8 or NUL byte |
| `MISSING_REQUIRED_FIELD` | row | Null `source_system`, `source_id` or `source_updated_at` |
| `UNKNOWN_SOURCE_SYSTEM` | row | Not in the allowed set |
| `TYPE_CAST_FAILED` | row | `source_updated_at` not an ISO-8601 timestamp |
| `INVALID_EMAIL` / `INVALID_PHONE` | row | Fails the format rule |
| `INVALID_CANONICAL_REF` | row | `canonical_ref` not in the master |
| `DUPLICATE_KEY_SUPERSEDED` | row | Same key in the batch with an older `source_updated_at`, or the same time on an earlier line |

The batch circuit breaker (`RejectRateExceeded`) fails the run when row-validation rejects exceed
`validation.max_reject_rate` for files of at least `reject_rate_min_rows` rows.

**Run statuses:** `RUNNING`, `SUCCEEDED`, `FAILED`, `SKIPPED`. **Key statuses:** `MAPPED`, `CONFLICT`, `UNRESOLVED`.
**Work-item statuses** (conflicts, unresolved): `OPEN`, `RESOLVED`.
