import unittest
import sys
import os
import pyspark
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.udfs import _get_underscore_prefix
from common.config import parse_config
from common.schemas import csvSchema
from pgsanity.pgsanity import check_string
from common import sql_queries


class TestCommon(unittest.TestCase):

    def teste_udf_get_underscore_prefix(self):
        self.assertEqual(
            _get_underscore_prefix("aggressive_longitudinal_acceleration_1549653321089461"),
            "aggressive_longitudinal_acceleration",
            "Should return only aggressive_longitudinal_acceleration"
        )

    def test_udf_get_underscore_prefix_with_full_path(self):
        self.assertEqual(
            _get_underscore_prefix("s3n:///truck-data-wrangler/aggressive_longitudinal_acceleration_1549653321089461"),
            "aggressive_longitudinal_acceleration",
            "Should return only aggressive_longitudinal_acceleration"
        )

    def test_parse_config(self):
        self.assertTrue(parse_config())

    def test_csv_schema_is_struct(self):
        self.assertIsInstance(csvSchema, pyspark.sql.types.StructType)

    def test_sql_syntax_drop_jerked_truck_events_table(self):
        success, msg = check_string(sql_queries.drop_jerked_truck_events_table, True)
        self.assertTrue(success, msg)

    def test_sql_create_jerked_truck_events_table(self):
        success, msg = check_string(sql_queries.average_acceleration_query, True)
        self.assertTrue(success, msg)

    def test_sql_max_jerk_x_query(self):
        success, msg = check_string(sql_queries.max_jerk_x_query, True)
        self.assertTrue(success, msg)

    def test_sql_max_jerk_y_query(self):
        success, msg = check_string(sql_queries.max_jerk_y_query, True)
        self.assertTrue(success, msg)

    def test_sql_max_jerk_z_query(self):
        success, msg = check_string(sql_queries.max_jerk_z_query, True)
        self.assertTrue(success, msg)


if __name__ == '__main__':
    unittest.main()