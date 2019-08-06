from common.connections import create_spark_session
from pyspark.sql.functions import lit, col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
import os


def unify_separate_event_files(spark, raw_data_path = 'raw_data/'):
    data_files = {}

    for r, d, f in os.walk(raw_data_path):
        for file in f:
            if '.csv' in file:
                event_type = "_".join(file.split("_")[:-1])
                file_path = os.path.join(raw_data_path, file)
                data_files[file_path] = event_type

    spark_dfs = []

    for data_file, event_type in data_files.items():
        spark_dfs.append(
            spark.read.csv(data_file, header=True)
                 .withColumn("event_type", lit(event_type))
                 .select(
                    "event_type",
                    "label",
                    "timestamp",
                    "accel_x",
                    "accel_y",
                    "accel_z",
                    "gyro_roll",
                    "gyro_pitch",
                    "gyro_yaw"
                 )
        )

    spark_dfs_iterator = iter(spark_dfs)
    unified_spark_df = next(spark_dfs_iterator)
    for df in spark_dfs_iterator:
        unified_spark_df = unified_spark_df.unionAll(df)

    return unified_spark_df


def calculate_jerk_from_truck_events(df):
    column_list = ["event_type", "label"]
    win_spec = Window.partitionBy([col(x) for x in column_list]).orderBy("timestamp")

    columns_that_needs_latest_values = ['accel_x', 'accel_y', 'accel_z', 'timestamp']

    for column_name in columns_that_needs_latest_values:
        df = df.withColumn(
            "last_" + column_name,
            F.lag(col(column_name)).over(win_spec)
        )

    # x axis
    df = df.withColumn(
        "jerk_x",
        F.when(F.isnull(col("last_accel_x")), 0)
         .when(F.isnull(col("last_timestamp")), 0)
         .otherwise((col("accel_x") - col("last_accel_x")) / (col("timestamp") - col("last_timestamp")))
    )

    # y axis
    df = df.withColumn(
        "jerk_y",
        F.when(F.isnull(col("last_accel_y")), 0)
         .when(F.isnull(col("last_timestamp")), 0)
         .otherwise((col("accel_y") - col("last_accel_y")) / (col("timestamp") - col("last_timestamp")))
    )

    # z axis
    df = df.withColumn(
        "jerk_z",
        F.when(F.isnull(col("last_accel_z")), 0)
         .when(F.isnull(col("last_timestamp")), 0)
         .otherwise((col("accel_z") - col("last_accel_z")) / (col("timestamp") - col("last_timestamp")))
    )

    return df
