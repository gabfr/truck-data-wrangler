
drop_jerked_truck_events_table = "DROP TABLE IF EXISTS jerked_truck_events"

create_jerked_truck_events_table = """
    CREATE TABLE IF NOT EXISTS jerked_truck_events (
        date_timestamp TIMESTAMP NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        label VARCHAR(50) NOT NULL,
        accel_x DECIMAL(40, 20),
        accel_y DECIMAL(40, 20),
        accel_z DECIMAL(40, 20),
        gyro_roll DECIMAL(40, 20),
        gyro_pitch DECIMAL(40, 20),
        gyro_yaw DECIMAL(40, 20),
        -- derived columns
        last_timestamp BIGINT,
        last_accel_x DECIMAL(40, 20),
        last_accel_y DECIMAL(40, 20),
        last_accel_z DECIMAL(40, 20),
        jerk_x DECIMAL(40, 20),
        jerk_y DECIMAL(40, 20),
        jerk_z DECIMAL(40, 20),
        is_accelerating INTEGER,
        is_breaking INTEGER,
        is_turning_left INTEGER,
        is_turning_right INTEGER,
        PRIMARY KEY (date_timestamp, event_type, label)
    )
"""


average_acceleration_query = """
    SELECT 
        event_type, 
        label, 
        AVG(accel_x) AS avg_accel_x,
        AVG(accel_y) AS avg_accel_y,
        AVG(accel_z) AS avg_accel_z
    FROM 
        truck_events 
    GROUP BY
        event_type, label
    ORDER BY
        event_type, label
"""


max_jerk_x_query = """
    SELECT 
        event_type, 
        label, 
        from_unixtime(cast(((timestamp / 1000) / 1000) as bigint),'yyyy-MM-dd HH:mm:ss.SSS') AS timestamp,
        jerk_x
    FROM 
        jerked_truck_events 
    ORDER BY
        jerk_x DESC
    LIMIT 1
"""


max_jerk_y_query = """
    SELECT 
        event_type, 
        label, 
        from_unixtime(cast(((timestamp / 1000) / 1000) as bigint),'yyyy-MM-dd HH:mm:ss.SSS') AS timestamp,
        jerk_y
    FROM 
        jerked_truck_events 
    ORDER BY
        jerk_y DESC, event_type, label
    LIMIT 1
"""


max_jerk_z_query = """
    SELECT 
        event_type, 
        label, 
        from_unixtime(cast(((timestamp / 1000) / 1000) as bigint),'yyyy-MM-dd HH:mm:ss.SSS') AS timestamp,
        jerk_z
    FROM 
        jerked_truck_events 
    ORDER BY
        jerk_z DESC, event_type, label
    LIMIT 1
"""

