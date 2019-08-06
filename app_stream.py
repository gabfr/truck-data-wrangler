from common.config import parse_config
from common.connections import create_spark_session
from stream.etl import start_watch_for_csvs, create_streaming_window


def main():
    configs = parse_config()
    spark = create_spark_session()

    read_stream = start_watch_for_csvs(spark, configs['data']['raw_path'])

    write_stream = create_streaming_window(read_stream, configs)

    write_stream.awaitTermination()


if __name__ == "__main__":
    main()
