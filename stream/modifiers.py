from pyspark.sql.functions import stddev as _stddev, col
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def classify_accelerometer_data(configs):
    def _(df, epochId):
        df.describe().show()
        df_stats = df.select(
            _stddev(col('gyro_yaw')).alias('std')
        ).collect()

        std_val = 0.0 if df_stats[0]['std'] is None else df_stats[0]['std']

        turn_threshold_min = std_val * -1
        turn_threshold_max = std_val

        df = df.withColumn("timestamp", F.unix_timestamp(col("date_timestamp")))

        column_list = ["event_type", "label"]

        win_spec = Window.partitionBy([col(x) for x in column_list]).orderBy("timestamp")

        columns_that_needs_latest_values = ['accel_x', 'accel_y', 'accel_z']

        for column_name in columns_that_needs_latest_values:
            df = df.withColumn("last_" + column_name, F.lag(col(column_name)).over(win_spec))
            
        # appending the last timestamp before this row
        df = df.withColumn(
            "last_timestamp",
            F.when(F.isnull(F.lag(col("date_timestamp")).over(win_spec)), 0)
             .otherwise(F.unix_timestamp(F.lag(col("date_timestamp")).over(win_spec)))
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

        # adding the is_accelerating flag
        df = df.withColumn(
            "is_accelerating",
            F.when(F.isnull(col("jerk_x")), 0)
             .when(col("jerk_x") > 0, 1)
             .otherwise(0)
        )

        # adding the is_breaking flag
        df = df.withColumn(
            "is_breaking",
            F.when(F.isnull(col("jerk_x")), 0)
             .when(col("jerk_x") < 0, 1)
             .otherwise(0)
        )

        # adding the is_turning_left flag
        df = df.withColumn(
            "is_turning_left",
            F.when(col("gyro_yaw") <= turn_threshold_min, 1)
             .otherwise(0)
        )

        # adding the is_turning_right flag
        df = df.withColumn(
            "is_turning_right",
            F.when(col("gyro_yaw") >= turn_threshold_max, 1)
             .otherwise(0)
        )

        url = "jdbc:postgresql://" + configs['timescaledb']['host'] + ":" + configs['timescaledb']['port'] + "/" + configs['timescaledb']['db']
        properties = {
            "driver": "org.postgresql.Driver",
            "user": configs['timescaledb']['user'],
            "password": configs['timescaledb']['password']
        }

        df = df[[
            'date_timestamp',
            'event_type',
            'label',
            'accel_x',
            'accel_y',
            'accel_z',
            'gyro_roll',
            'gyro_pitch',
            'gyro_yaw',
            'last_timestamp',
            'last_accel_x',
            'last_accel_y',
            'last_accel_z',
            'jerk_x',
            'jerk_y',
            'jerk_z',
            'is_accelerating',
            'is_breaking',
            'is_turning_left',
            'is_turning_right'
        ]]

        df.write.jdbc(url=url, table="jerked_truck_events", mode="append", properties=properties)
    return _
