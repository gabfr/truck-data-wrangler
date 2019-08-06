from common.connections import create_timescaledb_connection
from common.sql_queries import create_jerked_truck_events_table, drop_jerked_truck_events_table


def main():
    print("Creating jerked_truck_events table...")

    conn = create_timescaledb_connection()
    cur = conn.cursor()

    cur.execute(drop_jerked_truck_events_table)
    cur.execute(create_jerked_truck_events_table)

    cur.close()
    conn.close()

    print("Done!")


if __name__ == "__main__":
    main()
