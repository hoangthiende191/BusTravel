"""
load_csv.py - Load layer for the BusTravel ETL pipeline.

"""

import sys
from pathlib import Path

from psycopg2.extras import execute_values

# Allow running directly from source/etl/ without installing the package
sys.path.insert(0, str(Path(__file__).resolve().parent))

from connect import connectDB
from clean import (
    build_routes,
    build_buses,
    build_directions,
    clean_stops,
    clean_trips,
    clean_trip_stops,
    clean_trip_segments,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rows(frame, columns):
    """Convert a DataFrame subset to a list of plain Python tuples for psycopg2."""
    return [tuple(row) for row in frame.loc[:, columns].itertuples(index=False, name=None)]


def _upsert(cur, table, columns, values, conflict_columns):
    """
    Bulk-insert rows with an ON CONFLICT upsert.
    - If there are non-PK columns, perform DO UPDATE SET.
    - If all columns are part of the PK, use DO NOTHING (no update needed).
    Safe to run multiple times (idempotent).
    """
    if not values:
        return

    column_sql   = ", ".join(columns)
    conflict_sql = ", ".join(conflict_columns)
    updates      = ", ".join(
        f"{col} = EXCLUDED.{col}"
        for col in columns if col not in conflict_columns
    )
    conflict_action = f"DO UPDATE SET {updates}" if updates else "DO NOTHING"

    sql = f"""
        INSERT INTO {table} ({column_sql}) VALUES %s
        ON CONFLICT ({conflict_sql}) {conflict_action}
    """
    execute_values(cur, sql, values, page_size=1_000)


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def main():
    # 1. Clean all raw data first (raises on unrecoverable errors)
    stops    = clean_stops()
    trips    = clean_trips()
    valid_ids = set(trips["trip_id"])
    t_stops  = clean_trip_stops(valid_ids)
    t_segs   = clean_trip_segments(valid_ids)

    directions = build_directions()

    # 2. Connect and upsert in FK dependency order inside ONE transaction
    conn = connectDB()
    try:
        with conn.cursor() as cur:
            # --- Tier 1: no FK dependencies ---
            _upsert(cur, "routes",
                    ["route_id"],
                    build_routes(),
                    ["route_id"])

            _upsert(cur, "buses",
                    ["device_id"],
                    build_buses(trips),
                    ["device_id"])

            _upsert(cur, "stops",
                    ["stop_id", "route_id", "address", "latitude", "longitude"],
                    _rows(stops, ["stop_id", "route_id", "address", "latitude", "longitude"]),
                    ["stop_id"])

            # --- Tier 2: depends on routes ---
            _upsert(cur, "directions",
                    list(directions.columns),
                    _rows(directions, list(directions.columns)),
                    ["direction_id", "route_id"])

            # --- Tier 3: depends on buses, routes, directions, stops ---
            _upsert(cur, "trips",
                    ["trip_id", "device_id", "route_id", "direction_id",
                     "trip_date", "start_terminal_id", "end_terminal_id",
                     "start_time", "end_time"],
                    _rows(trips, ["trip_id", "device_id", "route_id", "direction_id",
                                  "trip_date", "start_terminal_id", "end_terminal_id",
                                  "start_time", "end_time"]),
                    ["trip_id"])

            # --- Tier 4: depends on trips and stops ---
            _upsert(cur, "trip_stops",
                    ["trip_id", "stop_id", "arrival_time", "departure_time", "dwell_time_seconds"],
                    _rows(t_stops, ["trip_id", "stop_id", "arrival_time", "departure_time", "dwell_time_seconds"]),
                    ["trip_id", "stop_id"])

            _upsert(cur, "trip_segments",
                    ["trip_id", "segment_no", "start_stop_id", "end_stop_id",
                     "start_time", "end_time", "run_time_seconds", "distance_km"],
                    _rows(t_segs, ["trip_id", "segment_no", "start_stop_id", "end_stop_id",
                                   "start_time", "end_time", "run_time_seconds", "distance_km"]),
                    ["trip_id", "segment_no"])

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()

    print("\n--- Load complete ---")
    print(f"  trips      : {len(trips):>7,}")
    print(f"  trip_stops : {len(t_stops):>7,}")
    print(f"  trip_segs  : {len(t_segs):>7,}")


if __name__ == "__main__":
    main()
