# ID Mapping: Reconciliation and Quality Report

- Generated: 2026-10-29 12:24:28 UTC
- Database: `data/idmap.duckdb`
- Ground truth: `data/generated/expected`

## Verdict: **PASS** (12/12 hard checks passed)

## Hard checks

| Check | Result | Detail |
|---|---|---|
| 1. rows_read = staged + quarantined + duplicates (every run) | PASS | holds for 5 succeeded run(s) |
| 1b. staged = mapped + conflicts + unresolved + late arrivals (every run) | PASS | holds for 5 succeeded run(s) |
| 1c. run counters match stored staging/quarantine rows | PASS | counters match table rows |
| 2. distinct staged keys = MAPPED + CONFLICT + UNRESOLVED | PASS | 95014 distinct staged keys = 95014 MAPPED+CONFLICT+UNRESOLVED |
| 3. at most one current mapping per business key | PASS | 82099 current mappings, all unique |
| 4. every current canonical_id exists and is active | PASS | all current targets valid |
| 5. no overlapping [valid_from, valid_to) intervals per key | PASS | 82448 history rows, no overlaps |
| 6. every quarantined row carries run_id, source_file, source_row_num, reason | PASS | 4490 quarantined rows traceable |
| 7. determinism: fresh rebuild reproduces mapping tables | PASS | replayed 5 run(s); 8 tables identical (history fingerprint e6f19464c0a4) |
| 8. source_key_status agrees with id_mapping_current | PASS | status and history consistent |
| 9. open conflicts/unresolved match per-key status | PASS | open work queues consistent |
| 10. logical keys of batch tables are unique | PASS | no duplicate logical keys |

## Pipeline runs

| # | Run | File | Status | Read | Staged | Quarantined | Dupes | Mapped | Conflict | Unresolved | Late | Ins | Closed | Warn | ms | Failure / detail |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | run_20261029T122402428Z_eb093702 | data/incoming/billing_batch_001.csv | SUCCEEDED | 32949 | 30911 | 1403 | 635 | 27670 | 463 | 2778 | 0 | 27670 | 0 | 0 | 533 |  |
| 2 | run_20261029T122403116Z_006d979e | data/incoming/broken_schema_batch.csv | FAILED |  |  |  |  |  |  |  |  |  |  |  | 2 | schema: SchemaContractError: SCHEMA_MISSING_COLUMN: required column(s) ['source_updated_at'] absent |
| 3 | run_20261029T122403121Z_f4064339 | data/incoming/crm_batch_001.csv | SUCCEEDED | 37274 | 34869 | 1679 | 726 | 31755 | 0 | 3114 | 0 | 31755 | 0 | 0 | 635 |  |
| 4 | run_20261029T122404061Z_a3354de3 | data/incoming/crm_batch_002.csv | SUCCEEDED | 1743 | 1743 | 0 | 0 | 523 | 174 | 0 | 1046 | 523 | 349 | 0 | 85 |  |
| 5 | run_20261029T122404154Z_fd6586eb | data/incoming/schema_drift_batch.csv | SUCCEEDED | 955 | 955 | 0 | 0 | 797 | 0 | 158 | 0 | 797 | 0 | 2 | 70 |  |
| 6 | run_20261029T122404233Z_d311a90b | data/incoming/support_batch_001.csv | SUCCEEDED | 30236 | 28279 | 1408 | 549 | 21703 | 506 | 6070 | 0 | 21703 | 0 | 0 | 502 |  |

## Outcome summary

| Metric | Value |
|---|---|
| Business keys | 95014 |
| MAPPED | 81925 (86.22%) |
| CONFLICT | 1143 (1.20%) |
| UNRESOLVED | 11946 (12.57%) |
| Rows read (latest run per file) | 103157 |
| Rows quarantined | 4490 (4.35%) |
| Exact duplicates dropped | 1910 |
| History rows / closed versions | 82448 / 349 |
| Keys with more than one version | 349 |
| Late arrivals ignored (with different outcome) | 1046 (1046) |
| Legacy canonical_ref agreeing with mapping | 3989/4448 (89.68%) |

### By source system

| System | Keys | Mapped | Conflict | Unresolved |
|---|---|---|---|---|
| billing | 30911 | 27670 | 463 | 2778 |
| crm | 34869 | 31755 | 174 | 2940 |
| support | 29234 | 22500 | 506 | 6228 |

### Current mappings by rule

| Rule | Mappings |
|---|---|
| exact_tax_id | 38589 |
| exact_email | 29487 |
| rule_name_domain | 8901 |
| rule_phone_name | 4931 |
| manual_override | 191 |

### Quarantine by reason code

| Reason | Rows | Share | Expected | Expected rows caught (same file+line+code) |
|---|---|---|---|---|
| DUPLICATE_KEY_SUPERSEDED | 955 | 21.27% | 955 | 955/955 |
| MISSING_REQUIRED_FIELD | 955 | 21.27% | 955 | 955/955 |
| INVALID_CANONICAL_REF | 478 | 10.65% | 478 | 478/478 |
| INVALID_EMAIL | 478 | 10.65% | 478 | 478/478 |
| INVALID_PHONE | 478 | 10.65% | 478 | 478/478 |
| UNKNOWN_SOURCE_SYSTEM | 478 | 10.65% | 478 | 478/478 |
| TYPE_CAST_FAILED | 477 | 10.62% | 477 | 477/477 |
| MALFORMED_ROW | 191 | 4.25% | 191 | 191/191 |

### Open unresolved backlog

| Reason | Keys | Of which keep an earlier mapping |
|---|---|---|
| NO_CANDIDATE | 11104 | 0 |
| INACTIVE_CANONICAL_ONLY | 444 | 0 |
| INSUFFICIENT_ATTRIBUTES | 398 | 0 |

### Conflicts

| Type | Status | Keys | Of which keep an earlier mapping |
|---|---|---|---|
| MANY_TO_ONE_SAME_SYSTEM | OPEN | 1846 | 1846 |
| ONE_TO_MANY | OPEN | 1143 | 174 |

## Quality against ground truth

Two views. **Spec conformance** compares each key's final status and canonical with what the rules
should produce given the designed inputs (expected_status / expected_canonical_id). **Identity quality**
compares mappings with the true entity (true_canonical_id). It shows the cost of deterministic,
conflict-averse rules: designed conflicts and dirty names lower recall, and the weak phone+name
rule lowers precision.

### Spec conformance: 97243/97243 labeled keys agree (100.00%)

| expected \ actual | MAPPED | CONFLICT | UNRESOLVED | QUARANTINED | ABSENT |
|---|---|---|---|---|---|
| CONFLICT | 0 | 1143 | 0 | 0 | 0 |
| MAPPED | 81925 | 0 | 0 | 0 | 0 |
| QUARANTINED | 0 | 0 | 0 | 2229 | 0 |
| UNRESOLVED | 0 | 0 | 11946 | 0 | 0 |

### Identity quality (vs true entity)

| Metric | Value | Counts |
|---|---|---|
| Precision | 99.65% | 81812/82099 current mappings correct |
| Recall | 91.44% | 81812/89473 in-scope keys mapped to their entity |
| F1 | 95.37% |  |

Per rule. Precision is grouped by the rule that produced the current mapping. Recall is grouped by the rule
that should decide the key, and counts how many of those keys ended up mapped to their true entity.

| Rule | Mapped | Correct | Precision | Expected to decide | Mapped to truth | Recall |
|---|---|---|---|---|---|---|
| exact_email | 29487 | 29487 | 100.00% | 30009 | 29487 | 98.26% |
| exact_tax_id | 38589 | 38589 | 100.00% | 38589 | 38589 | 100.00% |
| manual_override | 191 | 191 | 100.00% | 191 | 191 | 100.00% |
| rule_name_domain | 8901 | 8901 | 100.00% | 9348 | 8901 | 95.22% |
| rule_phone_name | 4931 | 4644 | 94.18% | 4644 | 4644 | 100.00% |

### Agreement by scenario

| Scenario | Keys | Agree | Rate |
|---|---|---|---|
| boundary_embedded_newline | 179 | 179 | 100.00% |
| boundary_long_id | 179 | 179 | 100.00% |
| boundary_max_length | 179 | 179 | 100.00% |
| boundary_unicode_name | 490 | 490 | 100.00% |
| boundary_whitespace_only | 171 | 171 | 100.00% |
| clean | 76695 | 76695 | 100.00% |
| conflict_keeps_mapping | 174 | 174 | 100.00% |
| dirty_name | 817 | 817 | 100.00% |
| dirty_name+override | 96 | 96 | 100.00% |
| duplicate_account | 956 | 956 | 100.00% |
| inactive_only | 478 | 478 | 100.00% |
| invalid_canonical_ref | 478 | 478 | 100.00% |
| late_arrival | 1046 | 1046 | 100.00% |
| malformed_value | 1433 | 1433 | 100.00% |
| missing_required | 318 | 318 | 100.00% |
| no_match | 4776 | 4776 | 100.00% |
| one_to_many_email | 432 | 432 | 100.00% |
| one_to_many_email+override | 46 | 46 | 100.00% |
| one_to_many_name_domain | 428 | 428 | 100.00% |
| one_to_many_name_domain+override | 49 | 49 | 100.00% |
| remap | 349 | 349 | 100.00% |
| schema_drift | 955 | 955 | 100.00% |
| shared_identifier | 109 | 109 | 100.00% |
| sparse_attributes | 5490 | 5490 | 100.00% |
| twin_entity | 459 | 459 | 100.00% |
| unresolved_then_mapped | 174 | 174 | 100.00% |
| weak_rule_false_positive | 287 | 287 | 100.00% |

### Designed volumes vs observed

| Measure | Designed | Observed |
|---|---|---|
| Exact duplicate rows dropped | 1910 | 1910 |
| Late-arrival records | 1046 | 1046 |
| Many-to-one keys (same system) | 1846 | 1846 |

## Soft thresholds (config `quality.thresholds`)

| Metric | Observed | Threshold | Result |
|---|---|---|---|
| Precision | 99.65% | ≥ 95.00% | OK |
| Recall | 91.44% | ≥ 80.00% | OK |
| Conflict rate | 1.20% | ≤ 5.00% | OK |
| Unresolved rate | 12.57% | ≤ 20.00% | OK |
| Quarantine rate | 4.35% | ≤ 10.00% | OK |
