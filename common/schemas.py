from pyspark.sql.types import *

csvSchema = StructType([
    StructField("timestamp", LongType(), False),
    StructField("accel_x", DoubleType(), False),
    StructField("accel_y", DoubleType(), False),
    StructField("accel_z", DoubleType(), False),
    StructField("gyro_roll", DoubleType(), False),
    StructField("gyro_pitch", DoubleType(), False),
    StructField("gyro_yaw", DoubleType(), False),
    StructField("label", StringType(), False)
])
