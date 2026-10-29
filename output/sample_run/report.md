# ID Mapping: Reconciliation and Quality Report

- Generated: 2026-10-29 12:24:29 UTC
- Database: `output/sample_run/idmap.duckdb`
- Ground truth: `data/expected`

## Verdict: **PASS** (12/12 hard checks passed)

## Hard checks

| Check | Result | Detail |
|---|---|---|
| 1. rows_read = staged + quarantined + duplicates (every run) | PASS | holds for 5 succeeded run(s) |
| 1b. staged = mapped + conflicts + unresolved + late arrivals (every run) | PASS | holds for 5 succeeded run(s) |
| 1c. run counters match stored staging/quarantine rows | PASS | counters match table rows |
| 2. distinct staged keys = MAPPED + CONFLICT + UNRESOLVED | PASS | 149 distinct staged keys = 149 MAPPED+CONFLICT+UNRESOLVED |
| 3. at most one current mapping per business key | PASS | 131 current mappings, all unique |
| 4. every current canonical_id exists and is active | PASS | all current targets valid |
| 5. no overlapping [valid_from, valid_to) intervals per key | PASS | 132 history rows, no overlaps |
| 6. every quarantined row carries run_id, source_file, source_row_num, reason | PASS | 11 quarantined rows traceable |
| 7. determinism: fresh rebuild reproduces mapping tables | PASS | replayed 5 run(s); 8 tables identical (history fingerprint 35dc4d5bb2a9) |
| 8. source_key_status agrees with id_mapping_current | PASS | status and history consistent |
| 9. open conflicts/unresolved match per-key status | PASS | open work queues consistent |
| 10. logical keys of batch tables are unique | PASS | no duplicate logical keys |

## Pipeline runs

| # | Run | File | Status | Read | Staged | Quarantined | Dupes | Mapped | Conflict | Unresolved | Late | Ins | Closed | Warn | ms | Failure / detail |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | run_20261029T122428823Z_fcc4558c | data/sample/billing_batch_001.csv | SUCCEEDED | 55 | 52 | 3 | 0 | 45 | 2 | 5 | 0 | 45 | 0 | 0 | 38 |  |
| 2 | run_20261029T122428866Z_8d56054d | data/sample/broken_schema_batch.csv | FAILED |  |  |  |  |  |  |  |  |  |  |  | 2 | schema: SchemaContractError: SCHEMA_MISSING_COLUMN: required column(s) ['source_updated_at'] absent |
| 3 | run_20261029T122428869Z_50fb8df6 | data/sample/crm_batch_001.csv | SUCCEEDED | 55 | 51 | 2 | 2 | 45 | 0 | 6 | 0 | 45 | 0 | 0 | 31 |  |
| 4 | run_20261029T122428904Z_72fd3218 | data/sample/crm_batch_002.csv | SUCCEEDED | 5 | 5 | 0 | 0 | 2 | 1 | 0 | 2 | 2 | 1 | 0 | 27 |  |
| 5 | run_20261029T122428935Z_dddc4125 | data/sample/schema_drift_batch.csv | SUCCEEDED | 5 | 5 | 0 | 0 | 5 | 0 | 0 | 0 | 5 | 0 | 2 | 26 |  |
| 6 | run_20261029T122428965Z_c01f61f4 | data/sample/support_batch_001.csv | SUCCEEDED | 48 | 41 | 6 | 1 | 35 | 0 | 6 | 0 | 35 | 0 | 0 | 31 |  |
| 7 | run_20261029T122429124Z_5f52f964 | data/sample/billing_batch_001.csv | SKIPPED |  |  |  |  |  |  |  |  |  |  |  | 3 | checksum already processed by run_20261029T122428823Z_fcc4558c (data/sample/billing_batch_001.csv) |
| 8 | run_20261029T122429129Z_c10823fb | data/sample/broken_schema_batch.csv | FAILED |  |  |  |  |  |  |  |  |  |  |  | 2 | schema: SchemaContractError: SCHEMA_MISSING_COLUMN: required column(s) ['source_updated_at'] absent |
| 9 | run_20261029T122429133Z_727911eb | data/sample/crm_batch_001.csv | SKIPPED |  |  |  |  |  |  |  |  |  |  |  | 2 | checksum already processed by run_20261029T122428869Z_50fb8df6 (data/sample/crm_batch_001.csv) |
| 10 | run_20261029T122429135Z_58a420c2 | data/sample/crm_batch_002.csv | SKIPPED |  |  |  |  |  |  |  |  |  |  |  | 1 | checksum already processed by run_20261029T122428904Z_72fd3218 (data/sample/crm_batch_002.csv) |
| 11 | run_20261029T122429137Z_5035e46d | data/sample/schema_drift_batch.csv | SKIPPED |  |  |  |  |  |  |  |  |  |  |  | 1 | checksum already processed by run_20261029T122428935Z_dddc4125 (data/sample/schema_drift_batch.csv) |
| 12 | run_20261029T122429139Z_5035b3a8 | data/sample/support_batch_001.csv | SKIPPED |  |  |  |  |  |  |  |  |  |  |  | 1 | checksum already processed by run_20261029T122428965Z_c01f61f4 (data/sample/support_batch_001.csv) |

## Outcome summary

| Metric | Value |
|---|---|
| Business keys | 149 |
| MAPPED | 130 (87.25%) |
| CONFLICT | 3 (2.01%) |
| UNRESOLVED | 16 (10.74%) |
| Rows read (latest run per file) | 168 |
| Rows quarantined | 11 (6.55%) |
| Exact duplicates dropped | 3 |
| History rows / closed versions | 132 / 1 |
| Keys with more than one version | 1 |
| Late arrivals ignored (with different outcome) | 2 (2) |
| Legacy canonical_ref agreeing with mapping | 5/6 (83.33%) |

### By source system

| System | Keys | Mapped | Conflict | Unresolved |
|---|---|---|---|---|
| billing | 52 | 45 | 2 | 5 |
| crm | 51 | 45 | 1 | 5 |
| support | 46 | 40 | 0 | 6 |

### Current mappings by rule

| Rule | Mappings |
|---|---|
| exact_tax_id | 56 |
| exact_email | 50 |
| rule_phone_name | 12 |
| rule_name_domain | 11 |
| manual_override | 2 |

### Quarantine by reason code

| Reason | Rows | Share | Expected | Expected rows caught (same file+line+code) |
|---|---|---|---|---|
| MISSING_REQUIRED_FIELD | 3 | 27.27% | 3 | 3/3 |
| MALFORMED_ROW | 2 | 18.18% | 2 | 2/2 |
| DUPLICATE_KEY_SUPERSEDED | 1 | 9.09% | 1 | 1/1 |
| INVALID_CANONICAL_REF | 1 | 9.09% | 1 | 1/1 |
| INVALID_EMAIL | 1 | 9.09% | 1 | 1/1 |
| INVALID_PHONE | 1 | 9.09% | 1 | 1/1 |
| TYPE_CAST_FAILED | 1 | 9.09% | 1 | 1/1 |
| UNKNOWN_SOURCE_SYSTEM | 1 | 9.09% | 1 | 1/1 |

### Open unresolved backlog

| Reason | Keys | Of which keep an earlier mapping |
|---|---|---|
| NO_CANDIDATE | 13 | 0 |
| INACTIVE_CANONICAL_ONLY | 2 | 0 |
| INSUFFICIENT_ATTRIBUTES | 1 | 0 |

### Conflicts

| Type | Status | Keys | Of which keep an earlier mapping |
|---|---|---|---|
| MANY_TO_ONE_SAME_SYSTEM | OPEN | 6 | 6 |
| ONE_TO_MANY | OPEN | 3 | 1 |

## Quality against ground truth

Two views. **Spec conformance** compares each key's final status and canonical with what the rules
should produce given the designed inputs (expected_status / expected_canonical_id). **Identity quality**
compares mappings with the true entity (true_canonical_id). It shows the cost of deterministic,
conflict-averse rules: designed conflicts and dirty names lower recall, and the weak phone+name
rule lowers precision.

### Spec conformance: 154/154 labeled keys agree (100.00%)

| expected \ actual | MAPPED | CONFLICT | UNRESOLVED | QUARANTINED | ABSENT |
|---|---|---|---|---|---|
| CONFLICT | 0 | 3 | 0 | 0 | 0 |
| MAPPED | 130 | 0 | 0 | 0 | 0 |
| QUARANTINED | 0 | 0 | 0 | 5 | 0 |
| UNRESOLVED | 0 | 0 | 16 | 0 | 0 |

### Identity quality (vs true entity)

| Metric | Value | Counts |
|---|---|---|
| Precision | 99.24% | 130/131 current mappings correct |
| Recall | 93.53% | 130/139 in-scope keys mapped to their entity |
| F1 | 96.30% |  |

Per rule. Precision is grouped by the rule that produced the current mapping. Recall is grouped by the rule
that should decide the key, and counts how many of those keys ended up mapped to their true entity.

| Rule | Mapped | Correct | Precision | Expected to decide | Mapped to truth | Recall |
|---|---|---|---|---|---|---|
| exact_email | 50 | 50 | 100.00% | 52 | 50 | 96.15% |
| exact_tax_id | 56 | 56 | 100.00% | 56 | 56 | 100.00% |
| manual_override | 2 | 2 | 100.00% | 2 | 2 | 100.00% |
| rule_name_domain | 11 | 11 | 100.00% | 11 | 11 | 100.00% |
| rule_phone_name | 12 | 11 | 91.67% | 11 | 11 | 100.00% |

### Agreement by scenario

| Scenario | Keys | Agree | Rate |
|---|---|---|---|
| boundary_embedded_newline | 1 | 1 | 100.00% |
| boundary_long_id | 1 | 1 | 100.00% |
| boundary_max_length | 1 | 1 | 100.00% |
| boundary_unicode_name | 4 | 4 | 100.00% |
| boundary_whitespace_only | 1 | 1 | 100.00% |
| clean | 106 | 106 | 100.00% |
| conflict_keeps_mapping | 1 | 1 | 100.00% |
| dirty_name+override | 1 | 1 | 100.00% |
| duplicate_account | 2 | 2 | 100.00% |
| inactive_only | 2 | 2 | 100.00% |
| invalid_canonical_ref | 1 | 1 | 100.00% |
| late_arrival | 2 | 2 | 100.00% |
| malformed_value | 3 | 3 | 100.00% |
| missing_required | 1 | 1 | 100.00% |
| no_match | 7 | 7 | 100.00% |
| one_to_many_email | 2 | 2 | 100.00% |
| one_to_many_name_domain+override | 1 | 1 | 100.00% |
| remap | 1 | 1 | 100.00% |
| schema_drift | 5 | 5 | 100.00% |
| sparse_attributes | 6 | 6 | 100.00% |
| twin_entity | 3 | 3 | 100.00% |
| unresolved_then_mapped | 1 | 1 | 100.00% |
| weak_rule_false_positive | 1 | 1 | 100.00% |

### Designed volumes vs observed

| Measure | Designed | Observed |
|---|---|---|
| Exact duplicate rows dropped | 3 | 3 |
| Late-arrival records | 2 | 2 |
| Many-to-one keys (same system) | 6 | 6 |

## Soft thresholds (config `quality.thresholds`)

| Metric | Observed | Threshold | Result |
|---|---|---|---|
| Precision | 99.24% | ≥ 95.00% | OK |
| Recall | 93.53% | ≥ 80.00% | OK |
| Conflict rate | 2.01% | ≤ 5.00% | OK |
| Unresolved rate | 10.74% | ≤ 20.00% | OK |
| Quarantine rate | 6.55% | ≤ 10.00% | OK |
