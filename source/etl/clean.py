"""
clean.py - Data cleaning and validation layer for the BusTravel ETL pipeline.

"""

import logging
from pathlib import Path

import pandas as pd


ROUTE_ID = 654
RAW_DIR = Path(__file__).resolve().parents[2] / "Dataset" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("clean")

# Stop sequences per direction (matches load_csv segment mapping)
# Outbound (direction=1): BT01 -> 101..114 -> BT02  (segments 1-15)
# Inbound  (direction=2): BT02 -> 201..213 -> BT01  (segments 21-34)
_OUTBOUND = ["BT01"] + [str(n) for n in range(101, 115)] + ["BT02"]
_INBOUND  = ["BT02"] + [str(n) for n in range(201, 214)] + ["BT01"]

SEGMENT_STOP_PAIRS = {
    **{n: (_OUTBOUND[n - 1], _OUTBOUND[n]) for n in range(1, 16)},
    **{n: (_INBOUND[n - 21], _INBOUND[n - 20]) for n in range(21, 35)},
}

VALID_SEGMENT_NOS = set(SEGMENT_STOP_PAIRS.keys())


def _drop_report(df, mask, label):
    """Drop rows where mask is True and log the count."""
    bad = mask.sum()
    if bad:
        log.warning("Dropping %d invalid rows from %s", bad, label)
    return df[~mask].copy()


def _parse_time(series, label):
    """Parse a time column (HH:MM:SS strings) -> datetime.time objects."""
    parsed = pd.to_datetime(series, format="%H:%M:%S", errors="coerce").dt.time
    null_count = parsed.isna().sum()
    if null_count:
        log.warning("%s: %d rows have unparseable time values", label, null_count)
    return parsed


def _parse_date(series, label):
    """Parse a date column flexibly (handles YYYY-MM-DD and M/D/YYYY)."""
    parsed = pd.to_datetime(series, errors="coerce").dt.date
    null_count = parsed.isna().sum()
    if null_count:
        log.warning("%s: %d rows have unparseable date values", label, null_count)
    return parsed




def clean_stops():
    """
    Source: bus_stops_and_terminals_654.csv \n
    Target: stops table (stop_id PK, route_id, address, latitude, longitude) \n
    Note: CSV 'direction' column is not in schema -> dropped.
    """
    df = pd.read_csv(RAW_DIR / "bus_stops_and_terminals_654.csv")
    log.info("stops raw shape: %s", df.shape)

    df["stop_id"]  = df["stop_id"].astype(str).str.strip()
    df["route_id"] = df["route_id"].astype(int)

    # Drop rows missing required fields
    required = ["stop_id", "route_id", "latitude", "longitude"]
    df = _drop_report(df, df[required].isnull().any(axis=1), "stops[required fields]")

    # Validate coordinate ranges
    invalid_lat = (df["latitude"] < -90)  | (df["latitude"] > 90)
    invalid_lon = (df["longitude"] < -180) | (df["longitude"] > 180)
    df = _drop_report(df, invalid_lat | invalid_lon, "stops[coordinate range]")

    # Deduplicate on PK
    before = len(df)
    df = df.drop_duplicates("stop_id", keep="last")
    if len(df) < before:
        log.info("stops: dropped %d duplicate stop_id rows", before - len(df))

    df = df[["stop_id", "route_id", "address", "latitude", "longitude"]].reset_index(drop=True)
    log.info("stops clean shape: %s", df.shape)
    return df


def clean_trips():
    """
    Source: bus_trips_654.csv \n
    Target: trips table \n
    Dropped columns: duration, duration_in_mins (not in schema) \n
    """
    df = pd.read_csv(RAW_DIR / "bus_trips_654.csv")
    log.info("trips raw shape: %s", df.shape)

    df = df.rename(columns={
        "deviceid":       "device_id",
        "date":           "trip_date",
        "direction":      "direction_id",
        "start_terminal": "start_terminal_id",
        "end_terminal":   "end_terminal_id",
    })

    df["route_id"] = ROUTE_ID

    # Numeric coercions (errors -> NaN for later drop)
    df["trip_id"]      = pd.to_numeric(df["trip_id"],      errors="coerce")
    df["device_id"]    = pd.to_numeric(df["device_id"],    errors="coerce")
    df["direction_id"] = pd.to_numeric(df["direction_id"], errors="coerce")

    # Date: M/D/YYYY format in this CSV (e.g. 10/1/2021)
    df["trip_date"]  = _parse_date(df["trip_date"], "trips[trip_date]")
    df["start_time"] = _parse_time(df["start_time"], "trips[start_time]")
    df["end_time"]   = _parse_time(df["end_time"],   "trips[end_time]")

    required = ["trip_id", "device_id", "route_id", "direction_id",
                "trip_date", "start_time", "end_time"]
    df = _drop_report(df, df[required].isnull().any(axis=1), "trips[required fields]")

    # Safe int cast after NULLs are removed
    df["trip_id"]      = df["trip_id"].astype(int)
    df["device_id"]    = df["device_id"].astype(int)
    df["direction_id"] = df["direction_id"].astype(int)

    df = _drop_report(df, ~df["direction_id"].isin([1, 2]), "trips[invalid direction_id]")

    before = len(df)
    df = df.drop_duplicates("trip_id", keep="last")
    if len(df) < before:
        log.info("trips: dropped %d duplicate trip_id rows", before - len(df))

    schema_cols = [
        "trip_id", "device_id", "route_id", "direction_id",
        "trip_date", "start_terminal_id", "end_terminal_id",
        "start_time", "end_time",
    ]
    df = df[schema_cols].reset_index(drop=True)
    log.info("trips clean shape: %s", df.shape)
    return df


def clean_trip_stops(valid_trip_ids):
    """
    Source: bus_dwell_times_654.csv \n
    Target: trip_stops table (PK: trip_id, stop_id) \n
    """
    df = pd.read_csv(RAW_DIR / "bus_dwell_times_654.csv")
    log.info("dwell raw shape: %s", df.shape)

    # stop_id must be VARCHAR
    df["bus_stop"] = df["bus_stop"].astype(str).str.strip()
    df = df.rename(columns={"bus_stop": "stop_id"})

    # Drop orphaned records (no parent trip)
    before = len(df)
    df = df[df["trip_id"].isin(valid_trip_ids)].copy()
    if len(df) < before:
        log.warning("trip_stops: dropped %d rows with no matching trip_id", before - len(df))

    df["arrival_time"]   = _parse_time(df["arrival_time"],   "trip_stops[arrival_time]")
    df["departure_time"] = _parse_time(df["departure_time"], "trip_stops[departure_time]")

    # float -> int for dwell seconds
    df["dwell_time_seconds"] = df["dwell_time_in_seconds"].round().astype("Int64")

    required = ["trip_id", "stop_id", "arrival_time", "departure_time", "dwell_time_seconds"]
    df = _drop_report(df, df[required].isnull().any(axis=1), "trip_stops[required fields]")

    df = _drop_report(df, df["dwell_time_seconds"] < 0, "trip_stops[negative dwell_time]")

    df["dwell_time_seconds"] = df["dwell_time_seconds"].astype(int)
    df["trip_id"] = df["trip_id"].astype(int)
    # PK (trip_id, stop_id): keep last departure when duplicates exist
    before = len(df)
    df = df.sort_values("departure_time").drop_duplicates(["trip_id", "stop_id"], keep="last")
    if len(df) < before:
        log.info("trip_stops: dropped %d duplicate (trip_id, stop_id) rows", before - len(df))

    df = df[["trip_id", "stop_id", "arrival_time", "departure_time", "dwell_time_seconds"]].reset_index(drop=True)
    log.info("trip_stops clean shape: %s", df.shape)
    return df


def clean_trip_segments(valid_trip_ids):
    """
    Source: bus_running_times_654.csv
    Target: trip_segments table (PK: trip_id, segment_no)
    """
    df = pd.read_csv(RAW_DIR / "bus_running_times_654.csv")
    log.info("running raw shape: %s", df.shape)

    # Drop the 64 rows where trip_id is NULL
    df = _drop_report(df, df["trip_id"].isnull(), "trip_segments[NULL trip_id]")

    df["trip_id"]    = df["trip_id"].astype(int)
    df["segment_no"] = df["segment"].astype(int)

    # Drop orphaned records
    before = len(df)
    df = df[df["trip_id"].isin(valid_trip_ids)].copy()
    if len(df) < before:
        log.warning("trip_segments: dropped %d rows with no matching trip_id", before - len(df))

    # Drop the rows where time/date columns are NULL
    df = _drop_report(df, df[["start_time", "end_time"]].isnull().any(axis=1), "trip_segments[NULL time columns]")

    df["start_time"] = _parse_time(df["start_time"], "trip_segments[start_time]")
    df["end_time"]   = _parse_time(df["end_time"],   "trip_segments[end_time]")

    df["run_time_seconds"] = df["run_time_in_seconds"].round().astype("Int64")
    df["distance_km"]      = df["length"]

    # Validate segment_no is in the known stop-pair mapping
    df = _drop_report(df, ~df["segment_no"].isin(VALID_SEGMENT_NOS), "trip_segments[unknown segment_no]")

    # Map segment_no -> (start_stop_id, end_stop_id)
    df[["start_stop_id", "end_stop_id"]] = (
        df["segment_no"].map(SEGMENT_STOP_PAIRS).apply(pd.Series)
    )
    df = _drop_report(df, df["run_time_seconds"] <= 0, "trip_segments[non-positive run_time]")

    before = len(df)
    df = df.drop_duplicates(["trip_id", "segment_no"], keep="last")
    if len(df) < before:
        log.info("trip_segments: dropped %d duplicate (trip_id, segment_no) rows", before - len(df))

    df["run_time_seconds"] = df["run_time_in_seconds"].astype("int")

    schema_cols = [
        "trip_id", "segment_no", "start_stop_id", "end_stop_id",
        "start_time", "end_time", "run_time_seconds", "distance_km",
    ]
    df = df[schema_cols].reset_index(drop=True)
    log.info("trip_segments clean shape: %s", df.shape)
    return df


# ---------------------------------------------------------------------------
# Derived reference tables (constants, no raw CSV source)
# ---------------------------------------------------------------------------

def build_routes():
    """Returns [(route_id,)] for the routes table."""
    return [(ROUTE_ID,)]


def build_buses(trips_df):
    """Returns unique [(device_id,)] from the cleaned trips DataFrame."""
    return [(int(v),) for v in trips_df["device_id"].unique()]


def build_directions():
    """Authoritative direction rows for route 654."""
    return pd.DataFrame([
        (1, ROUTE_ID, "Kandy-Digana", "BT01", "BT02"),
        (2, ROUTE_ID, "Digana-Kandy", "BT02", "BT01"),
    ], columns=["direction_id", "route_id", "direction_name", "start_terminal_id", "end_terminal_id"])


def build_schedule_patterns(trips_df, trip_stops_df):
    """
    Computes scheduled offsets based on median values from historical data.
    Takes cleaned trips_df and trip_stops_df, avoiding re-parsing raw CSVs.
    """
    log.info("Computing schedule patterns from clean data...")
    merged = pd.merge(
        trip_stops_df[["trip_id", "stop_id", "arrival_time", "departure_time"]],
        trips_df[["trip_id", "direction_id", "start_time"]],
        on="trip_id",
        how="inner"
    )

    def time_to_seconds(time_series):
        return pd.to_timedelta(time_series.astype(str)).dt.total_seconds()

    start_sec = time_to_seconds(merged["start_time"])
    arr_sec   = time_to_seconds(merged["arrival_time"])
    dep_sec   = time_to_seconds(merged["departure_time"])

    
    arr_offset = arr_sec - start_sec
    arr_offset = arr_offset.apply(lambda x: x + 86400 if x < -43200 else x)
    
    dep_offset = dep_sec - start_sec
    dep_offset = dep_offset.apply(lambda x: x + 86400 if x < -43200 else x)

    merged["arrival_offset_s"]   = arr_offset
    merged["departure_offset_s"] = dep_offset

    bad = (merged["arrival_offset_s"] < 0) | (merged["departure_offset_s"] < 0)
    if bad.sum():
        log.warning("build_schedule_patterns: dropping %d rows with negative offsets", bad.sum())
    merged = merged[~bad]

    pattern = (
        merged
        .groupby(["direction_id", "stop_id"], as_index=False)
        .agg(
            scheduled_arrival_offset_s   = ("arrival_offset_s",   "median"),
            scheduled_departure_offset_s = ("departure_offset_s", "median"),
        )
    )

    pattern["scheduled_arrival_offset_s"]   = pattern["scheduled_arrival_offset_s"].round().astype(int)
    pattern["scheduled_departure_offset_s"] = pattern["scheduled_departure_offset_s"].round().astype(int)

    pattern["stop_sequence"] = (
        pattern
        .groupby("direction_id")["scheduled_arrival_offset_s"]
        .rank(method="first")
        .astype(int)
    )

    pattern["route_id"] = ROUTE_ID

    return pattern[[
        "route_id", "direction_id", "stop_id", "stop_sequence",
        "scheduled_arrival_offset_s", "scheduled_departure_offset_s"
    ]]


