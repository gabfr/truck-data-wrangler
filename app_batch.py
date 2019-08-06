from common.config import parse_config
from common.connections import create_spark_session
from common.sql_queries import average_acceleration_query, max_jerk_x_query, max_jerk_y_query, max_jerk_z_query
from batch.etl import unify_separate_event_files, calculate_jerk_from_truck_events


def main():
    configs = parse_config()
    spark = create_spark_session()
    
    truck_events_df = unify_separate_event_files(spark, configs['data']['raw_path'])

    spark.sql(average_acceleration_query).show(truncate=False)
    
    jerked_truck_events_df = calculate_jerk_from_truck_events(spark)
    
    spark.sql(max_jerk_x_query).show(truncate=False)
    spark.sql(max_jerk_y_query).show(truncate=False)
    spark.sql(max_jerk_z_query).show(truncate=False)


if __name__ == "__main__":
    main()
