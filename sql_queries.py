
drop_jerked_truck_events_table = "DROP DATABASE IF EXISTS truckdata"
create_jerked_truck_events_table = """
    CREATE TABLE IF NOT EXISTS jerked_truck_events (
        timestamp TIMESTAMP PRIMARY KEY,
        date_timestamp DATETIME,
        event_type VARCHAR(255) NOT NULL,
        label VARCHAR(50) NOT NULL,
        accel_x DECIMAL(15, 6),
        accel_y DECIMAL(15, 6),
        accel_z DECIMAL(15, 6),
        gyro_roll DECIMAL(15, 6),
        gyro_pitch DECIMAL(15, 6),
        gyro_yaw DECIMAL(15, 6),
        -- derived columns
        last_timestamp TIMESTAMP,
        last_accel_x DECIMAL(15, 6),
        last_accel_y DECIMAL(15, 6),
        last_accel_z DECIMAL(15, 6),
        jerk_x DECIMAL(15, 6),
        jerk_y DECIMAL(15, 6),
        jerk_z DECIMAL(15, 6),
        is_accelerating INTEGER,
        is_breaking INTEGER
    )
"""