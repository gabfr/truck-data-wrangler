import unittest
import logging
from pyspark.sql import SparkSession
from common.connections import create_spark_session


class PySparkTest(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()

        cls.spark = create_spark_session()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()