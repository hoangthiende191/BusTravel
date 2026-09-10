"""
simulator.py - Bus Event Simulator for the BusTravel pipeline.

Reads scheduled trip_stops from PostgreSQL, then replays each
arrival/departure event in chronological order into Kafka topic bus-events.

Each event represents a bus physically arriving at or departing from a stop,
with a small random jitter applied to the scheduled time to simulate real-world
variance (early/late buses).

Event schema published to Kafka:
{
    "trip_id":        int,
    "device_id":      int,
    "route_id":       int,
    "direction_id":   int,
    "stop_id":        str,
    "event_type":     "arrival" | "departure",
    "scheduled_time": "HH:MM:SS",
    "actual_time":    "HH:MM:SS",
    "trip_date":      "YYYY-MM-DD",
    "emitted_at":     "ISO8601 UTC timestamp"
}

Usage:
    python simulator.py                        # replay all trips, speed x1
    python simulator.py --speed 60             # 1 simulated minute = 1 real second
    python simulator.py --date 2021-10-01      # only trips on this date
    python simulator.py --trip-id 1            # single trip for debugging
    python simulator.py --dry-run              # print events, skip Kafka
"""

import argparse
import logging
import random
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras

# Allow running from source/simulator/ directly
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "etl"))
from connect import connectDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulator")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ROUTE_ID = 654

# Jitter range in seconds applied to scheduled times to simulate real variance.
# Positive = late, negative = early.
JITTER_MIN_SECONDS = -60   # up to 1 minute early
JITTER_MAX_SECONDS = 300   # up to 5 minutes late


# ---------------------------------------------------------------------------
# Database queries
# ---------------------------------------------------------------------------

_SQL_TRIPS = """
    SELECT
        t.trip_id,
        t.device_id,
        t.route_id,
        t.direction_id,
        t.trip_date,
        t.start_time,
        t.end_time
    FROM trips t
    WHERE t.route_id = %(route_id)s
      AND (%(trip_date)s IS NULL OR t.trip_date = %(trip_date)s)
      AND (%(trip_id)s   IS NULL OR t.trip_id   = %(trip_id)s)
    ORDER BY t.trip_date, t.start_time
"""

_SQL_TRIP_STOPS = """
    SELECT
        ts.trip_id,
        ts.stop_id,
        ts.arrival_time,
        ts.departure_time,
        ts.dwell_time_seconds
    FROM trip_stops ts
    WHERE ts.trip_id = ANY(%(trip_ids)s)
    ORDER BY ts.trip_id, ts.arrival_time
"""


def _fetch_trips(conn, trip_date=None, trip_id=None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_SQL_TRIPS, {
            "route_id":  ROUTE_ID,
            "trip_date": trip_date,
            "trip_id":   trip_id,
        })
        return cur.fetchall()


def _fetch_trip_stops(conn, trip_ids: list[int]) -> dict[int, list[dict]]:
    """Returns {trip_id: [stop_events sorted by arrival_time]}"""
    if not trip_ids:
        return {}
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_SQL_TRIP_STOPS, {"trip_ids": trip_ids})
        rows = cur.fetchall()

    result: dict[int, list[dict]] = {}
    for row in rows:
        result.setdefault(row["trip_id"], []).append(dict(row))
    return result


# ---------------------------------------------------------------------------
# Event building
# ---------------------------------------------------------------------------

def _apply_jitter(scheduled_time, jitter_seconds: int):
    """Add jitter (seconds) to a datetime.time object. Returns datetime.time."""
    dummy_date = date(2000, 1, 1)
    dt = datetime.combine(dummy_date, scheduled_time) + timedelta(seconds=jitter_seconds)
    return dt.time()


def _build_events(trip: dict, stops: list[dict], jitter_seconds: int) -> list[dict]:
    """
    For a single trip, build one arrival + one departure event per stop.
    Both events share the same jitter offset so actual_time is consistent.
    """
    events = []
    base = {
        "trip_id":      trip["trip_id"],
        "device_id":    trip["device_id"],
        "route_id":     trip["route_id"],
        "direction_id": trip["direction_id"],
        "trip_date":    str(trip["trip_date"]),
    }

    for stop in stops:
        actual_arrival   = _apply_jitter(stop["arrival_time"],   jitter_seconds)
        actual_departure = _apply_jitter(stop["departure_time"], jitter_seconds)

        events.append({
            **base,
            "stop_id":        stop["stop_id"],
            "event_type":     "arrival",
            "scheduled_time": str(stop["arrival_time"]),
            "actual_time":    str(actual_arrival),
        })
        events.append({
            **base,
            "stop_id":        stop["stop_id"],
            "event_type":     "departure",
            "scheduled_time": str(stop["departure_time"]),
            "actual_time":    str(actual_departure),
        })

    return events


# ---------------------------------------------------------------------------
# Replay engine
# ---------------------------------------------------------------------------

def _time_to_seconds(t) -> int:
    """Convert datetime.time to total seconds since midnight."""
    return t.hour * 3600 + t.minute * 60 + t.second


def replay(trips: list[dict], stops_by_trip: dict, speed: float, dry_run: bool, producer=None):
    """
    Replay all events in chronological order.

    - speed: simulated seconds per real second (1 = realtime, 60 = 1 min/sec)
    - dry_run: if True, print events instead of sending to Kafka
    - producer: BusEventProducer instance (None if dry_run)
    """
    # Build flat event list across all trips, sorted by (trip_date, actual_time)
    all_events = []
    for trip in trips:
        stops = stops_by_trip.get(trip["trip_id"], [])
        if not stops:
            log.warning("trip_id=%s has no stops — skipping", trip["trip_id"])
            continue

        # Assign a consistent random jitter per trip (same bus, same trip = same delay)
        jitter = random.randint(JITTER_MIN_SECONDS, JITTER_MAX_SECONDS)
        events = _build_events(trip, stops, jitter)
        all_events.extend(events)

    if not all_events:
        log.warning("No events to replay.")
        return

    # Sort by trip_date then actual_time
    all_events.sort(key=lambda e: (e["trip_date"], e["actual_time"]))

    log.info("Replaying %d events across %d trips (speed=x%s, dry_run=%s)",
             len(all_events), len(trips), speed, dry_run)

    # Replay loop — emit each event at the correct simulated time
    sim_start_str = all_events[0]["actual_time"]   # HH:MM:SS string
    sim_start_sec = _time_to_seconds(
        datetime.strptime(sim_start_str, "%H:%M:%S").time()
    )
    wall_start = time.monotonic()

    for event in all_events:
        # How many simulated seconds from the start of replay to this event?
        event_sec = _time_to_seconds(
            datetime.strptime(event["actual_time"], "%H:%M:%S").time()
        )
        sim_delta = event_sec - sim_start_sec
        if sim_delta < 0:
            sim_delta += 86400  # handle midnight rollover

        # How many real seconds should have elapsed?
        target_wall = wall_start + (sim_delta / speed)
        sleep_for = target_wall - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

        # Stamp with real UTC emit time
        event["emitted_at"] = datetime.utcnow().isoformat() + "Z"

        if dry_run:
            log.info("[DRY-RUN] %s", event)
        else:
            producer.send(event)
            log.debug("Sent: trip=%s stop=%s type=%s actual=%s",
                      event["trip_id"], event["stop_id"],
                      event["event_type"], event["actual_time"])

    log.info("Replay finished.")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(description="BusTravel Event Simulator")
    parser.add_argument("--speed",    type=float, default=1.0,
                        help="Simulation speed multiplier (default: 1 = realtime, 60 = 1 min/sec)")
    parser.add_argument("--date",     type=str,   default=None,
                        help="Only replay trips on this date (YYYY-MM-DD)")
    parser.add_argument("--trip-id",  type=int,   default=None,
                        help="Only replay a single trip by trip_id")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Print events to stdout instead of sending to Kafka")
    parser.add_argument("--kafka",    type=str,   default="localhost:9092",
                        help="Kafka bootstrap servers (default: localhost:9092)")
    parser.add_argument("--topic",    type=str,   default="bus-events",
                        help="Kafka topic name (default: bus-events)")
    return parser.parse_args()


def main():
    args = _parse_args()

    # Parse optional date filter
    trip_date = None
    if args.date:
        try:
            trip_date = date.fromisoformat(args.date)
        except ValueError:
            log.error("Invalid --date format. Use YYYY-MM-DD.")
            sys.exit(1)

    # Fetch scheduled data from PostgreSQL
    log.info("Connecting to PostgreSQL...")
    conn = connectDB()
    try:
        trips = _fetch_trips(conn, trip_date=trip_date, trip_id=args.trip_id)
        if not trips:
            log.warning("No trips found matching the filters. Exiting.")
            sys.exit(0)

        trip_ids = [t["trip_id"] for t in trips]
        log.info("Fetched %d trip(s) from PostgreSQL.", len(trips))

        stops_by_trip = _fetch_trip_stops(conn, trip_ids)
        log.info("Fetched stop schedules for %d trip(s).", len(stops_by_trip))
    finally:
        conn.close()

    # Setup Kafka producer (skip if dry-run)
    producer = None
    if not args.dry_run:
        # Import here so dry-run works even without confluent_kafka installed
        from producer import BusEventProducer
        producer = BusEventProducer(
            bootstrap_servers=args.kafka,
            topic=args.topic,
        )

    try:
        replay(
            trips=trips,
            stops_by_trip=stops_by_trip,
            speed=args.speed,
            dry_run=args.dry_run,
            producer=producer,
        )
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
