# Task 3: Annotated Row Trace (M02 & Provenance Verification)

This document provides row-level provenance tracing for the three canonical revision cases evaluated in Task 3:
1. **Inserted Key**: A business key arriving in Batch 02 that did not exist in the baseline Silver table.
2. **Newer Correction**: A business key existing in baseline Silver with an update that has a strictly newer ingest timestamp.
3. **Stale Revision**: A business key existing in baseline Silver with an incoming revision that has an older ingest timestamp.

---

## 1. Case 1: Inserted Key (`R010001`)

### Input Batch Physical Rows
In `data/changes/changes_batch02.csv`, key `R010001` arrives with two physical records:
- **Candidate 1 (source_row 1)**:
  - `record_id`: `R010001`
  - `sensor_id`: `S01`
  - `site`: `zone-1`
  - `event_time_utc`: `2026-02-06T00:01:00Z`
  - `ingest_time_utc`: `2026-02-06T00:31:00Z`
  - `temperature_c`: `19.10`
  - `is_late`: `true`
  - `source_object`: `lab3/source_batch02.csv`
  - `source_row`: `1`
  - `source_sha256`: `9c61bf8a4183e3ba040334059a451c7c72583ba45bd46def7bcb81bdde52a4bb`
- **Candidate 2 (source_row 131)**:
  - `record_id`: `R010001`
  - `sensor_id`: `S01`
  - `site`: `zone-1`
  - `event_time_utc`: `2026-02-06T00:01:00Z`
  - `ingest_time_utc`: `2026-02-06T00:31:00Z`
  - `temperature_c`: `20.10`
  - `is_late`: `true`
  - `source_object`: `lab3/source_batch02.csv`
  - `source_row`: `131`
  - `source_sha256`: `9c61bf8a4183e3ba040334059a451c7c72583ba45bd46def7bcb81bdde52a4bb`

### Processing and Provenance Trace
1. **Receipt Ingestion (Bronze)**: Both Candidate 1 and Candidate 2 are appended to Bronze as distinct physical receipts (`receipt_batch_id = 'batch-02'`).
2. **Winner Selection (`choose_revisions`)**:
   - Window partition: `record_id = 'R010001'`
   - Sort order: `ingest_time_utc DESC, source_object ASC, source_row ASC`
   - Comparison: `ingest_time_utc` is tied (`2026-02-06T00:31:00Z`), `source_object` is tied (`lab3/source_batch02.csv`). Tie-breaker evaluates `source_row ASC`: Candidate 1 (`source_row = 1`) < Candidate 2 (`source_row = 131`).
   - Winner selected: Candidate 1. Candidate 2 is marked as superseded.
3. **Silver Merge (`merge_silver`)**:
   - `record_id = 'R010001'` does not exist in baseline Silver.
   - Evaluates `whenNotMatchedInsertAll()`.
   - Result: Inserted into Silver as a new record with `temperature_c = 19.10`, `source_row = 1`.

---

## 2. Case 2: Newer Correction (`R002001`)

### Baseline Silver State
Before Batch 02:
- `record_id`: `R002001`
- `sensor_id`: `S01`
- `site`: `zone-1`
- `event_time_utc`: `2026-02-02T16:26:00Z`
- `ingest_time_utc`: `2026-02-02T16:41:30Z`
- `temperature_c`: `18.10`
- `is_late`: `true`
- `source_object`: `observations_a.csv`
- `source_row`: `2001`
- `source_sha256`: `4ff9ce43dbf1246c4f8021c33beea5ca833c8742d131f6df5d688cf87a2d480e`

### Incoming Batch 02 Candidates
Key `R002001` arrives with two physical records:
- **Candidate 1 (source_row 101)**: `ingest_time_utc = 2026-02-02T16:50:30Z`, `temperature_c = 20.10`, `source_object = lab3/source_batch02.csv`, `source_row = 101`.
- **Candidate 2 (source_row 136)**: `ingest_time_utc = 2026-02-02T16:50:30Z`, `temperature_c = 21.10`, `source_object = lab3/source_batch02.csv`, `source_row = 136`.

### Processing and Provenance Trace
1. **Winner Selection (`choose_revisions`)**:
   - Timestamps and source objects tied. `source_row = 101` < `source_row = 136`.
   - Winner selected: Candidate 1 (`temp = 20.10`, `source_row = 101`). Candidate 2 superseded.
2. **Silver Merge (`merge_silver`)**:
   - Match condition: `t.record_id = s.record_id` matches existing baseline row.
   - Update predicate:
     $$\text{s.ingest\_time\_utc} > \text{t.ingest\_time\_utc}$$
     $$\text{2026-02-02T16:50:30Z} > \text{2026-02-02T16:41:30Z} \implies \mathbf{TRUE}$$
   - Result: Existing Silver record is UPDATED in place.
   - Post-merge Silver: `temperature_c` updated from `18.10` to `20.10`; `source_object` updated to `lab3/source_batch02.csv`; `source_row` updated to `101`; `source_sha256` updated to `9c61bf...`.

---

## 3. Case 3: Stale Revision (`R003001`)

### Baseline Silver State
Before Batch 02:
- `record_id`: `R003001`
- `sensor_id`: `S01`
- `site`: `zone-1`
- `event_time_utc`: `2026-02-03T00:46:00Z`
- `ingest_time_utc`: `2026-02-03T01:01:30Z`
- `temperature_c`: `18.10`
- `is_late`: `true`
- `source_object`: `observations_a.csv`
- `source_row`: `3001`
- `source_sha256`: `4ff9ce43dbf1246c4f8021c33beea5ca833c8742d131f6df5d688cf87a2d480e`

### Incoming Batch 02 Candidate
Key `R003001` arrives with one physical record:
- **Candidate (source_row 121)**:
  - `ingest_time_utc`: `2026-02-03T01:01:00Z` (30 seconds older than baseline)
  - `temperature_c`: `17.10`
  - `source_object`: `lab3/source_batch02.csv`
  - `source_row`: `121`

### Processing and Provenance Trace
1. **Winner Selection (`choose_revisions`)**: Candidate is the sole physical record for `R003001`, selected as winner.
2. **Silver Merge (`merge_silver`)**:
   - Match condition: `t.record_id = s.record_id` matches existing baseline row.
   - Update predicate:
     - `s.ingest > t.ingest` $\implies \text{2026-02-03T01:01:00Z} > \text{2026-02-03T01:01:30Z} \implies \mathbf{FALSE}$
     - `s.ingest == t.ingest ...` $\implies \mathbf{FALSE}$
     - Overall update predicate evaluates to $\mathbf{FALSE}$.
   - Result: Stale revision is REJECTED. Silver retains original `temperature_c = 18.10` and original provenance (`observations_a.csv`, `row 3001`).

---

## 4. Summary Table of M02 Trace

| Case | Business Key | Incoming Winner (Ingest / Temp / Row) | Stored State (Ingest / Temp / Row) | Merge Decision | Post-Merge Silver State |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Inserted** | `R010001` | `00:31:00Z` / `19.10°C` / `row 1` | *Not present* | `INSERT` | `19.10°C`, `row 1`, `source_batch02.csv` |
| **Newer** | `R002001` | `16:50:30Z` / `20.10°C` / `row 101` | `16:41:30Z` / `18.10°C` / `row 2001` | `UPDATE` | `20.10°C`, `row 101`, `source_batch02.csv` |
| **Stale** | `R003001` | `01:01:00Z` / `17.10°C` / `row 121` | `01:01:30Z` / `18.10°C` / `row 3001` | `NO-OP` | `18.10°C`, `row 3001`, `observations_a.csv` |

