CREATE TABLE routes (
    route_id INTEGER PRIMARY KEY
);


CREATE TABLE buses (
    device_id INTEGER PRIMARY KEY
);


CREATE TABLE directions (
    direction_id SMALLINT NOT NULL,
    route_id INTEGER NOT NULL,
    direction_name VARCHAR(100) NOT NULL,
    start_terminal_id VARCHAR(20),
    end_terminal_id VARCHAR(20),

    PRIMARY KEY (direction_id, route_id),

    FOREIGN KEY (route_id)
        REFERENCES routes(route_id)
);


CREATE TABLE stops (
    stop_id VARCHAR(20) PRIMARY KEY,

    route_id INTEGER NOT NULL,

    address VARCHAR(255),

    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    FOREIGN KEY (route_id)
        REFERENCES routes(route_id)
);


CREATE TABLE trips (
    trip_id BIGINT PRIMARY KEY,

    device_id INTEGER NOT NULL,
    route_id INTEGER NOT NULL,
    direction_id SMALLINT NOT NULL,

    trip_date DATE NOT NULL,

    start_terminal_id VARCHAR(20),
    end_terminal_id VARCHAR(20),

    start_time TIME NOT NULL,
    end_time TIME NOT NULL,

    FOREIGN KEY (device_id)
        REFERENCES buses(device_id),

    FOREIGN KEY (route_id)
        REFERENCES routes(route_id),

    FOREIGN KEY (direction_id, route_id)
        REFERENCES directions(direction_id, route_id),

    FOREIGN KEY (start_terminal_id)
        REFERENCES stops(stop_id),

    FOREIGN KEY (end_terminal_id)
        REFERENCES stops(stop_id)
);


CREATE TABLE trip_segments (
    trip_id BIGINT NOT NULL,

    segment_no INTEGER NOT NULL,

    start_stop_id VARCHAR(20),
    end_stop_id VARCHAR(20),

    start_time TIME NOT NULL,
    end_time TIME NOT NULL,

    run_time_seconds INTEGER NOT NULL,

    distance_km DOUBLE PRECISION,

    PRIMARY KEY (trip_id, segment_no),

    FOREIGN KEY (trip_id)
        REFERENCES trips(trip_id),

    FOREIGN KEY (start_stop_id)
        REFERENCES stops(stop_id),

    FOREIGN KEY (end_stop_id)
        REFERENCES stops(stop_id)
);


CREATE TABLE trip_stops (
    trip_id BIGINT NOT NULL,

    stop_id VARCHAR(20) NOT NULL,

    arrival_time TIME NOT NULL,
    departure_time TIME NOT NULL,

    dwell_time_seconds INTEGER NOT NULL,

    PRIMARY KEY (trip_id, stop_id),

    FOREIGN KEY (trip_id)
        REFERENCES trips(trip_id),

    FOREIGN KEY (stop_id)
        REFERENCES stops(stop_id)
);
