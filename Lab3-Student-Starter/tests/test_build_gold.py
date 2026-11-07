"""Focused Spark checks for the T2 Gold aggregation contract."""
from datetime import date
from decimal import Decimal
import unittest

from pyspark.sql import SparkSession, types as T

from templates.student_impl import build_gold


class BuildGoldTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master('local[1]')
            .appName('lab3-t2-build-gold-test')
            .config('spark.ui.enabled', 'false')
            .config('spark.sql.session.timeZone', 'UTC')
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_site_day_metrics_and_schema(self):
        schema = T.StructType([
            T.StructField('site', T.StringType(), False),
            T.StructField('event_date', T.DateType(), False),
            T.StructField('temperature_c', T.DecimalType(8, 2), False),
            T.StructField('is_late', T.BooleanType(), False),
            T.StructField('sensor_id', T.StringType(), False),
        ])
        rows = [
            ('A', date(2026, 2, 6), Decimal('25.00'), False, 's1'),
            ('A', date(2026, 2, 6), Decimal('27.00'), True, 's1'),
            ('A', date(2026, 2, 6), Decimal('23.00'), True, 's2'),
            ('B', date(2026, 2, 6), Decimal('28.00'), False, 's3'),
        ]

        result = build_gold(self.spark.createDataFrame(rows, schema))
        self.assertEqual(
            result.columns,
            [
                'site', 'event_date', 'n_observations', 'sum_temperature_c',
                'mean_temperature_c', 'min_temperature_c', 'max_temperature_c',
                'n_late', 'late_rate', 'n_above_25', 'n_sensors',
            ],
        )
        self.assertEqual(
            [(field.name, field.dataType.simpleString()) for field in result.schema.fields],
            [
                ('site', 'string'),
                ('event_date', 'date'),
                ('n_observations', 'bigint'),
                ('sum_temperature_c', 'decimal(18,2)'),
                ('mean_temperature_c', 'decimal(18,6)'),
                ('min_temperature_c', 'decimal(18,2)'),
                ('max_temperature_c', 'decimal(18,2)'),
                ('n_late', 'bigint'),
                ('late_rate', 'decimal(18,6)'),
                ('n_above_25', 'bigint'),
                ('n_sensors', 'bigint'),
            ],
        )

        actual = {row['site']: row.asDict() for row in result.collect()}
        self.assertEqual(
            {key: value for key, value in actual['A'].items() if key != 'site'},
            {
                'event_date': date(2026, 2, 6),
                'n_observations': 3,
                'sum_temperature_c': Decimal('75.00'),
                'mean_temperature_c': Decimal('25.000000'),
                'min_temperature_c': Decimal('23.00'),
                'max_temperature_c': Decimal('27.00'),
                'n_late': 2,
                'late_rate': Decimal('0.666667'),
                'n_above_25': 1,
                'n_sensors': 2,
            },
        )
        self.assertEqual(actual['B']['n_above_25'], 1)
        self.assertEqual(actual['B']['n_late'], 0)
        self.assertEqual(actual['B']['late_rate'], Decimal('0.000000'))


if __name__ == '__main__':
    unittest.main()
