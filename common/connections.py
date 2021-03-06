import psycopg2
from common.config import parse_config
from pyspark.sql import SparkSession


def create_timescaledb_connection(configs=None, autocommit=True):
    if configs is None:
        configs = parse_config()

    # connect to recreate the database
    conn = psycopg2.connect("host={} dbname={} port={} user={} password={}".format(
        configs['timescaledb']['host'],
        configs['timescaledb']['db'],
        configs['timescaledb']['port'],
        configs['timescaledb']['user'],
        configs['timescaledb']['password']
    ))
    conn.set_session(autocommit=autocommit)

    return conn


def create_spark_session():
    configs = parse_config()

    return SparkSession.builder.appName(configs['spark']['app_name']).getOrCreate()

