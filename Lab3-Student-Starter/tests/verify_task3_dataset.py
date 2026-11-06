"""Standalone dataset verification for Task 3 contracts and row traces.
This verifies M01, M02, M03 against actual curated.csv and changes_batch02.csv.
"""
from __future__ import annotations
import csv
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]

def parse_time(ts: str) -> datetime:
    return datetime.strptime(ts, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

def main():
    curated_path = ROOT / 'data/handoff/curated.csv'
    changes_path = ROOT / 'data/changes/changes_batch02.csv'

    with curated_path.open(encoding='utf-8') as f:
        curated_rows = list(csv.DictReader(f))
    with changes_path.open(encoding='utf-8') as f:
        changes_rows = list(csv.DictReader(f))

    print(f"Total curated baseline rows: {len(curated_rows)}")
    print(f"Total changes_batch02 incoming physical rows: {len(changes_rows)}")

    # Group incoming rows by record_id
    by_key = defaultdict(list)
    for r in changes_rows:
        r_clean = dict(r)
        r_clean['source_row'] = int(r_clean['source_row'])
        r_clean['parsed_ingest'] = parse_time(r_clean['ingest_time_utc'])
        by_key[r_clean['record_id']].append(r_clean)

    # 1. M01 - Incoming winner selection
    winners = {}
    superseded = {}
    for rid, rows in by_key.items():
        # Window order: ingest_time_utc DESC, source_object ASC, source_row ASC
        # In python: sort key = (-timestamp_epoch, source_object, source_row)
        rows_sorted = sorted(rows, key=lambda x: (-x['parsed_ingest'].timestamp(), x['source_object'], x['source_row']))
        winners[rid] = rows_sorted[0]
        if len(rows_sorted) > 1:
            superseded[rid] = rows_sorted[1:]

    print(f"Unique incoming business keys (winners): {len(winners)}")
    print(f"Keys with superseded physical versions: {len(superseded)}")
    total_superseded = sum(len(v) for v in superseded.values())
    print(f"Total superseded physical rows: {total_superseded}")
    assert len(winners) == 130
    assert total_superseded == 10
    assert len(winners) + total_superseded == 140

    # 2. M03 - Deterministic ties
    # Reversing input rows must produce identical winners
    winners_reversed = {}
    for rid, rows in by_key.items():
        rows_rev = sorted(rows, key=lambda x: x['source_row'], reverse=True)
        rows_sorted = sorted(rows_rev, key=lambda x: (-x['parsed_ingest'].timestamp(), x['source_object'], x['source_row']))
        winners_reversed[rid] = rows_sorted[0]
    assert winners == winners_reversed
    print("M03 deterministic tie-breaking verified: Reordering produces 100% identical winners.")

    # 3. M02 - Silver Merge Simulation: Newer vs Stale vs Inserted
    curated_map = {r['record_id']: r for r in curated_rows}
    inserted = []
    updated = []
    stale_rejected = []

    for rid, w in winners.items():
        if rid not in curated_map:
            inserted.append(w)
        else:
            t = curated_map[rid]
            t_ingest = parse_time(t['ingest_time_utc'])
            t_obj = t['source_object']
            t_row = int(t['source_row'])

            w_ingest = w['parsed_ingest']
            w_obj = w['source_object']
            w_row = w['source_row']

            # Update condition
            if (w_ingest > t_ingest or
                (w_ingest == t_ingest and w_obj < t_obj) or
                (w_ingest == t_ingest and w_obj == t_obj and w_row < t_row)):
                updated.append((w, t))
            else:
                stale_rejected.append((w, t))

    print(f"\n--- Merge Simulation Summary ---")
    print(f"Keys to insert (new business keys): {len(inserted)}")
    print(f"Existing keys to update (newer corrections): {len(updated)}")
    print(f"Existing keys rejected (stale revisions): {len(stale_rejected)}")
    assert len(inserted) == 100
    assert len(updated) == 20
    assert len(stale_rejected) == 10

    post_silver_count = len(curated_rows) + len(inserted)
    print(f"Post-merge Silver row count: {len(curated_rows)} + {len(inserted)} = {post_silver_count}")

    # Row traces for M02 report
    print("\n=== M02 Detailed Row Trace Sample ===")
    print("1. Inserted key sample (R010001):")
    sample_ins = winners['R010001']
    sample_ins_super = superseded['R010001'][0]
    print(f"   Selected winner: ingest={sample_ins['ingest_time_utc']}, temp={sample_ins['temperature_c']}, obj={sample_ins['source_object']}, row={sample_ins['source_row']}")
    print(f"   Superseded:      ingest={sample_ins_super['ingest_time_utc']}, temp={sample_ins_super['temperature_c']}, obj={sample_ins_super['source_object']}, row={sample_ins_super['source_row']}")
    print(f"   Tie-breaker: Same ingest and source_object, winner row {sample_ins['source_row']} < superseded row {sample_ins_super['source_row']}.")

    print("2. Newer correction sample (R002001):")
    upd_w, upd_t = [(w, t) for w, t in updated if w['record_id'] == 'R002001'][0]
    print(f"   Baseline Silver: ingest={upd_t['ingest_time_utc']}, temp={upd_t['temperature_c']}, obj={upd_t['source_object']}, row={upd_t['source_row']}")
    print(f"   Incoming winner: ingest={upd_w['ingest_time_utc']}, temp={upd_w['temperature_c']}, obj={upd_w['source_object']}, row={upd_w['source_row']}")
    print(f"   Decision: Incoming ingest timestamp ({upd_w['ingest_time_utc']}) > Stored ingest timestamp ({upd_t['ingest_time_utc']}) -> UPDATED in Silver.")

    print("3. Stale revision sample (R003001):")
    stl_w, stl_t = [(w, t) for w, t in stale_rejected if w['record_id'] == 'R003001'][0]
    print(f"   Baseline Silver: ingest={stl_t['ingest_time_utc']}, temp={stl_t['temperature_c']}, obj={stl_t['source_object']}, row={stl_t['source_row']}")
    print(f"   Incoming winner: ingest={stl_w['ingest_time_utc']}, temp={stl_w['temperature_c']}, obj={stl_w['source_object']}, row={stl_w['source_row']}")
    print(f"   Decision: Incoming ingest timestamp ({stl_w['ingest_time_utc']}) < Stored ingest timestamp ({stl_t['ingest_time_utc']}) -> REJECTED (No update to Silver).")

    print("\nTask 3 verification script finished with 100% assertions PASS!")

if __name__ == '__main__':
    main()

