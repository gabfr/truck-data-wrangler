import unittest
import sys
import os
import pyspark
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.config import parse_config
from common import sql_queries
from py_spark_test import PySparkTest
from batch.etl import unify_separate_event_files, calculate_jerk_from_truck_events


class TestBatch(PySparkTest):

    def test_unify_separate_event_files(self):
        configs = parse_config()
        configs['data']['raw_path'] = 'tests/raw_data_test/'

        unified_df = unify_separate_event_files(self.spark, configs['data']['raw_path'])
        self.assertTrue(unified_df, 'The dataframe should assert to True')

    def test_calculate_jerk_from_truck_events(self):
        configs = parse_config()
        configs['data']['raw_path'] = 'tests/raw_data_test/'

        unified_df = unify_separate_event_files(self.spark, configs['data']['raw_path'])
        self.assertTrue(unified_df, 'The unified dataframe should assert to True')
        jerked_truck_events_df = calculate_jerk_from_truck_events(unified_df)
        self.assertTrue(jerked_truck_events_df, 'The jerked truck events dataframe should assert to True')


if __name__ == '__main__':
    unittest.main()