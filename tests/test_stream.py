import unittest
import sys
import os
import pyspark
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.config import parse_config
from common.connections import create_timescaledb_connection
from common import sql_queries
from stream.modifiers import classify_accelerometer_data
from stream.etl import start_watch_for_csvs, create_streaming_window
from py_spark_test import PySparkTest
import types


class TestStream(PySparkTest):

    def test_classify_accelerometer_data_return_function(self):
        self.assertIsInstance(classify_accelerometer_data, types.FunctionType)

    def test_watch_and_stream_test_files(self):
        configs = parse_config()
        new_db_name = configs['timescaledb']['db'] + '_test'
        configs['timescaledb']['db'] = new_db_name
        configs['data']['raw_path'] = 'tests/raw_data_test/'

        conn = create_timescaledb_connection()
        cur = conn.cursor()

        cur.execute("select count(*) AS c from pg_catalog.pg_database where datname = '" + new_db_name + "'")
        results = cur.fetchone()

        if results[0] < 1:
            cur.execute('CREATE DATABASE ' + new_db_name)

        cur.close()
        conn.close()

        conn = create_timescaledb_connection(configs)
        cur = conn.cursor()

        cur.execute(sql_queries.drop_jerked_truck_events_table)
        cur.execute(sql_queries.create_jerked_truck_events_table)

        cur.close()
        conn.close()

        read_stream = start_watch_for_csvs(self.spark, configs)

        self.assertTrue(read_stream, 'The read stream should assert True')

        write_stream = create_streaming_window(read_stream, configs)

        write_stream.processAllAvailable()

        self.assertTrue(read_stream, 'The write stream also should assert True')


if __name__ == '__main__':
    unittest.main()