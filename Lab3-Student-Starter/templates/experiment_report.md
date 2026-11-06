# Lab 3 experiment report
Group / run label / author / reviewer:

## 1. Upstream handoff
State whether you used your approved Lab 2 Parquet or the course fallback.
Record the independently supplied manifest hash, source release ID, schema,
AS_OF, and logical baseline hash. Do not claim the course fixture was signed
or published by your group. Explain where the original Lab 2 raw data remain.

## 2. Table semantics and lineage
Record Bronze, Silver and Gold URIs and table versions. Explain a Bronze
receipt, a Silver business-key winner and a Gold site/day aggregation.
Trace one updated row to source object, physical row and SHA-256.
Draw only actual read/write edges; governance metadata are not data carriers.

## 3. Incremental evidence
- **Incoming physical records**: 140 physical rows from `lab3/source_batch02.csv`.
- **Per-key winners**: 130 unique business keys (`record_id`). Exactly one physical winner is selected per business key using a deterministic Window partitioned by `record_id` and ordered by `ingest_time_utc DESC, source_object ASC, source_row ASC`.
  - 10 physical records were superseded (source rows 131 to 140 of `lab3/source_batch02.csv`), where duplicate submissions with identical ingest timestamps and source objects were broken deterministically by smaller `source_row` (e.g. for `R010001`, row 1 won over row 131).
- **Inserts (New business keys)**: 100 records (e.g., `R010001` through `R010100`) did not exist in the baseline Silver table and were inserted via `whenNotMatchedInsertAll()`.
- **Updates (Newer corrections)**: 20 records (e.g., `R002001` through `R002020`) existed in Silver and had strictly newer ingest timestamps in Batch 02 (e.g., `R002001` had `2026-02-02T16:50:30Z` in incoming vs `2026-02-02T16:41:30Z` in baseline). The strict update predicate evaluated to true, updating Silver in place.
- **Unchanged/stale records**: 10 records (e.g., `R003001` through `R003010`) arrived with older ingest timestamps than currently stored in Silver (e.g., `R003001` incoming `2026-02-03T01:01:00Z` vs stored `2026-02-03T01:01:30Z`). The update predicate evaluated to false, leaving stored Silver values and provenance untouched. The remaining 9,590 baseline Silver records were completely untouched.
- **Bronze count**:
  - Baseline initial Bronze receipts: 9,610 rows.
  - After Batch 02 append: 9,610 + 140 = 9,750 physical receipts.
  - After replay insertion: 9,750 physical receipts (`bc1 == bc2 == 9750`). Idempotence is preserved because the merge key (`source_object + source_row + source_sha256`) prevents re-inserting identical receipts.
- **Silver count**:
  - Baseline initial Silver rows: 9,610 rows.
  - After Batch 02 merge: 9,610 + 100 = 9,710 rows.
  - After replay merge: 9,710 rows.
- **Replay and time-travel results**:
  - **Silver replay**: A second execution of `merge_silver` with the same winners batch is completely idempotent: `logical_sha256(after) == logical_sha256(replay)`. All existing keys match with identical timestamps and provenance, resulting in a safe no-op.
  - **Historical read (Time Travel)**: Reading Silver at `versionAsOf(silver_initial_version)` yields exactly 9,610 rows whose logical SHA-256 matches the upstream handoff digest (`25ef90f913d8d648b26dc31a1a7428f73111b2fc86fae82ec52e464c8f5f3e9d`), proving ACID snapshot isolation.
- **Distinction between Logical Equivalence, Delta Version, and Parquet File-Byte Identity**:
  - *Logical equivalence*: Canonical business state (sorted by business key, standardized timestamps/decimals/booleans) has an identical cryptographic hash.
  - *Delta version identity*: A no-op merge in Delta Lake may append a commit to the transaction log (e.g. advancing from version $v$ to $v+1$), recording execution metrics even when zero rows were updated/inserted. Lakehouse idempotence requires logical equivalence of data, not a frozen log version.
  - *Parquet file-byte identity*: Spark writes new Parquet data files containing distinct compression chunks, file metadata timestamps, and UUIDs. The physical bytes of Parquet files will differ across writes even when row contents are identical. Lakehouse reproducibility relies on logical equivalence, not byte-identical Parquet files.
- **Invalid-batch policy**:
  - Batch-acceptance policy enforces whole-batch rejection before any Delta write occurs.
  - Mutation probes (`VALUE` out of range $[-30, 60]$, `REQUIRED_VALUE` missing temperature, `SCHEMA_ALLOWLIST` unexpected operator email) immediately trigger a `ContractError` during pre-validation, aborting the transaction with `writes_performed: 0`. No partial insertion, quarantine of only bad rows, or silent substitution of older rows is permitted.

## 4. Analytics
Use the three saved SQL outputs. Explain why observation-weighted mean
is generally different from AVG(site means). Report numerators/denominators
for late fraction and >25 C fraction. Interpret >25 C as a teaching threshold,
not a calibrated physical safety threshold. State residual QA limitations.

## 5. Controlled layout experiment
Record exact dataset version, copy factor, expanded rows, resource settings,
setup times, total table files/bytes, execution plans, all six elapsed times,
medians, and A-median/B-median. Identify PartitionFilters in B's plan.
Do NOT call total table files 'files scanned'. Do NOT call repeated runs cold.
Explain the outcome even if layout B is slower. Synthetic copies are not new
independent observations and must never enter operational Gold.

## 6. Release and accountability
Record the pinned Silver/Gold pair and generating-code hash; validator code
may have its own hash. Attach five release-gate outcomes and the owner's
independent result. List who authored/reviewed each technical contribution.
Do not include credentials, kubeconfig, or restricted raw payloads.
