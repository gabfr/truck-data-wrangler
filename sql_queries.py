
drop_jerked_truck_events_table = "DROP TABLE IF EXISTS jerked_truck_events"
create_jerked_truck_events_table = """
    CREATE TABLE IF NOT EXISTS jerked_truck_events (
        date_timestamp TIMESTAMP NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        label VARCHAR(50) NOT NULL,
        accel_x DECIMAL(20, 17),
        accel_y DECIMAL(20, 17),
        accel_z DECIMAL(20, 17),
        gyro_roll DECIMAL(20, 17),
        gyro_pitch DECIMAL(20, 17),
        gyro_yaw DECIMAL(20, 17),
        -- derived columns
        last_timestamp BIGINT,
        last_accel_x DECIMAL(20, 17),
        last_accel_y DECIMAL(20, 17),
        last_accel_z DECIMAL(20, 17),
        jerk_x DECIMAL(20, 17),
        jerk_y DECIMAL(20, 17),
        jerk_z DECIMAL(20, 17),
        is_accelerating INTEGER,
        is_breaking INTEGER,
        PRIMARY KEY (date_timestamp, event_type, label)
    )
"""

