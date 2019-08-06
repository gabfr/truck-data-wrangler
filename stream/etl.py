from common.schemas import csvSchema
from common.udfs import get_underscore_prefix
from pyspark.sql.window import Window
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from stream.modifiers import classify_accelerometer_data


def start_watch_for_csvs(spark, input_path):
    return (
        spark.readStream
             .schema(csvSchema)
             .option("maxFilesPerTrigger", 1)
             .csv(input_path, header=True)
    )


def create_streaming_window(raw_df, configs):
    raw_df = raw_df.withColumn(
        "date_timestamp",
        F.to_timestamp(F.from_unixtime(((col("timestamp") / 1000) / 1000), 'yyyy-MM-dd HH:mm:ss.SSS'))
    )

    raw_df = raw_df.withColumn("original_file_name", F.input_file_name())

    raw_df = raw_df.withColumn("event_type", get_underscore_prefix("original_file_name"))
    
    windowedStreaming = (
        raw_df.groupBy(col("date_timestamp"), col("event_type"), col("label"), F.window(col("date_timestamp"), "1 second"))
            .agg(
                F.mean('accel_x'),
                F.mean('accel_y'),
                F.mean('accel_z'),
                F.mean('gyro_roll'),
                F.mean('gyro_pitch'),
                F.mean('gyro_yaw')
            )
            .withColumnRenamed('avg(accel_x)', 'accel_x')
            .withColumnRenamed('avg(accel_y)', 'accel_y')
            .withColumnRenamed('avg(accel_z)', 'accel_z')
            .withColumnRenamed('avg(gyro_roll)', 'gyro_roll')
            .withColumnRenamed('avg(gyro_pitch)', 'gyro_pitch')
            .withColumnRenamed('avg(gyro_yaw)', 'gyro_yaw')
            .withColumn('accel_x', col("accel_x").cast("double"))
            .withColumn('accel_y', col("accel_y").cast("double"))
            .withColumn('accel_z', col("accel_z").cast("double"))
            .withColumn('gyro_roll', col("gyro_roll").cast("double"))
            .withColumn('gyro_pitch', col("gyro_pitch").cast("double"))
            .withColumn('gyro_yaw', col("gyro_yaw").cast("double"))
    )

    return windowedStreaming \
        .writeStream \
        .trigger(processingTime=configs['streaming']['processing_time']) \
        .outputMode("complete") \
        .option("checkpointLocation", configs['streaming']['checkpoint_location']) \
        .foreachBatch(classify_accelerometer_data(configs)) \
        .start(path=configs['data']['raw_path'])
