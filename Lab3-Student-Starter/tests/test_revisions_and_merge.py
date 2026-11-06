"""Focused unit tests for Task 3 (T3) - choose_revisions and merge_silver contracts."""
from datetime import datetime, timezone, date
from decimal import Decimal
import unittest
import tempfile
import shutil

from pyspark.sql import SparkSession, types as T, functions as F
from delta.tables import DeltaTable

from templates.student_impl import choose_revisions, merge_silver


class RevisionsAndMergeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master('local[1]')
            .appName('lab3-t3-test')
            .config('spark.ui.enabled', 'false')
            .config('spark.sql.session.timeZone', 'UTC')
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel('ERROR')

        cls.schema = T.StructType([
            T.StructField('record_id', T.StringType(), False),
            T.StructField('sensor_id', T.StringType(), False),
            T.StructField('site', T.StringType(), False),
            T.StructField('event_time_utc', T.TimestampType(), False),
            T.StructField('ingest_time_utc', T.TimestampType(), False),
            T.StructField('temperature_c', T.DecimalType(8, 2), False),
            T.StructField('is_late', T.BooleanType(), False),
            T.StructField('source_object', T.StringType(), False),
            T.StructField('source_row', T.LongType(), False),
            T.StructField('source_sha256', T.StringType(), False),
            T.StructField('event_date', T.DateType(), False),
        ])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_choose_revisions_tie_breaking_and_schema(self):
        """M01 & M03: Deterministic Window, exactly one winner per record_id, no helper column leaked."""
        t1 = datetime(2026, 2, 6, 0, 30, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 2, 6, 0, 35, 0, tzinfo=timezone.utc)
        d = date(2026, 2, 6)

        rows = [
            # R1: newer timestamp wins (t2 > t1)
            ('R000001', 'S01', 'site1', t1, t1, Decimal('20.00'), False, 'obj_a', 10, 'h1', d),
            ('R000001', 'S01', 'site1', t1, t2, Decimal('22.00'), False, 'obj_b', 5, 'h2', d),
            # R2: same timestamp, smaller source_object wins ('obj_a' < 'obj_b')
            ('R000002', 'S02', 'site1', t1, t1, Decimal('19.00'), False, 'obj_b', 1, 'h3', d),
            ('R000002', 'S02', 'site1', t1, t1, Decimal('21.00'), False, 'obj_a', 99, 'h4', d),
            # R3: same timestamp and source_object, smaller source_row wins (3 < 15)
            ('R000003', 'S03', 'site1', t1, t1, Decimal('25.00'), False, 'obj_a', 15, 'h5', d),
            ('R000003', 'S03', 'site1', t1, t1, Decimal('23.00'), False, 'obj_a', 3, 'h6', d),
        ]

        df = self.spark.createDataFrame(rows, self.schema)
        winners = choose_revisions(df)

        # Schema must strictly match input columns without leaked _rn
        self.assertEqual(set(winners.columns), set(df.columns))
        self.assertEqual(len(winners.columns), len(df.columns))
        self.assertEqual(winners.count(), 3)

        results = {r['record_id']: r for r in winners.collect()}
        # R1: winner is the one with t2
        self.assertEqual(results['R000001']['ingest_time_utc'], t2)
        self.assertEqual(results['R000001']['temperature_c'], Decimal('22.00'))

        # R2: winner is the one with 'obj_a'
        self.assertEqual(results['R000002']['source_object'], 'obj_a')
        self.assertEqual(results['R000002']['temperature_c'], Decimal('21.00'))

        # R3: winner is the one with source_row == 3
        self.assertEqual(results['R000003']['source_row'], 3)
        self.assertEqual(results['R000003']['temperature_c'], Decimal('23.00'))

        # Reordering rows (reversed) must produce identical results (M03)
        winners_reversed = choose_revisions(df.orderBy(F.col('source_row').desc()))
        results_rev = {r['record_id']: r for r in winners_reversed.collect()}
        self.assertEqual(results, results_rev)

    def test_merge_silver_inserts_updates_and_stale_rejection(self):
        """M02 & M05: Insert missing keys, update newer, reject stale, idempotent replay."""
        temp_dir = tempfile.mkdtemp()
        silver_path = temp_dir + '/silver'

        try:
            t0 = datetime(2026, 2, 6, 0, 10, 0, tzinfo=timezone.utc)
            t1 = datetime(2026, 2, 6, 0, 20, 0, tzinfo=timezone.utc)
            t_older = datetime(2026, 2, 6, 0, 5, 0, tzinfo=timezone.utc)
            t_newer = datetime(2026, 2, 6, 0, 30, 0, tzinfo=timezone.utc)
            d = date(2026, 2, 6)

            # Initial Silver table with 2 records: R1 and R2
            initial_rows = [
                ('R000001', 'S01', 'site1', t0, t1, Decimal('20.00'), False, 'obj_a', 10, 'h1', d),
                ('R000002', 'S02', 'site1', t0, t1, Decimal('25.00'), False, 'obj_b', 20, 'h2', d),
            ]
            initial_df = self.spark.createDataFrame(initial_rows, self.schema)
            initial_df.write.format('delta').mode('errorifexists').save(silver_path)

            # Incoming batch:
            # - R1: newer correction (t_newer > t1) -> MUST update
            # - R2: stale revision (t_older < t1) -> MUST NOT update
            # - R3: brand new key -> MUST insert
            incoming_rows = [
                ('R000001', 'S01', 'site1', t0, t_newer, Decimal('21.50'), False, 'obj_a', 15, 'h_new', d),
                ('R000002', 'S02', 'site1', t0, t_older, Decimal('99.99'), False, 'obj_b', 5, 'h_old', d),
                ('R000003', 'S03', 'site1', t0, t1, Decimal('30.00'), False, 'obj_c', 1, 'h_r3', d),
            ]
            incoming_df = self.spark.createDataFrame(incoming_rows, self.schema)

            # Perform merge
            merge_silver(self.spark, silver_path, incoming_df)

            after_df = self.spark.read.format('delta').load(silver_path)
            self.assertEqual(after_df.count(), 3)
            after_rows = {r['record_id']: r for r in after_df.collect()}

            # R1 updated
            self.assertEqual(after_rows['R000001']['temperature_c'], Decimal('21.50'))
            self.assertEqual(after_rows['R000001']['ingest_time_utc'], t_newer)

            # R2 NOT updated (stale)
            self.assertEqual(after_rows['R000002']['temperature_c'], Decimal('25.00'))
            self.assertEqual(after_rows['R000002']['ingest_time_utc'], t1)

            # R3 inserted
            self.assertEqual(after_rows['R000003']['temperature_c'], Decimal('30.00'))

            # Replay merge: second application of the exact same incoming batch
            merge_silver(self.spark, silver_path, incoming_df)
            replay_df = self.spark.read.format('delta').load(silver_path)
            replay_rows = {r['record_id']: r for r in replay_df.collect()}
            self.assertEqual(after_rows, replay_rows)

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()

