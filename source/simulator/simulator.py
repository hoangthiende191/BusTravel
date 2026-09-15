"""
simulator.py - Bus Event Simulator for the BusTravel pipeline.

Simulates real-time bus arrivals and departures by replaying historical trips.
Uses 'schedule_patterns' to calculate expected arrival times, and applies a
Cumulative Jitter algorithm to simulate realistic traffic delays over segments.

Event schema published to Kafka:
{
    "trip_id":            int,
    "device_id":          int,
    "route_id":           int,
    "direction_id":       int,
    "stop_id":            str,
    "event_type":         "arrival" | "departure",
    "expected_time":      "HH:MM:SS",
    "actual_time":        "HH:MM:SS",
    "delay_hint_seconds": int,
    "trip_date":          "YYYY-MM-DD",
    "emitted_at":         "ISO8601 UTC timestamp"
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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import psycopg2
import psycopg2.extras


from source.etl.connect import connectDB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulator")

# Constants

ROUTE_ID = 654

# Dispatch delay (at terminal start)
DISPATCH_JITTER_MIN = -30
DISPATCH_JITTER_MAX = 120

# Delay accumulated between consecutive stops (segment traffic)
SEGMENT_JITTER_MIN = -10
SEGMENT_JITTER_MAX = 45

# DTB Queries
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

_SQL_PATTERNS = """
    SELECT
        direction_id,
        stop_id,
        stop_sequence,
        scheduled_arrival_offset_s,
        scheduled_departure_offset_s
    FROM schedule_patterns
    WHERE route_id = %(route_id)s
    ORDER BY direction_id, stop_sequence
"""


def _fetch_trips(conn, trip_date=None, trip_id=None) -> list[dict]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_SQL_TRIPS, {
            "route_id":  ROUTE_ID,
            "trip_date": trip_date,
            "trip_id":   trip_id,
        })
        return cur.fetchall()


def _fetch_patterns(conn) -> dict[int, list[dict]]:
    """Returns {direction_id: [stops ordered by stop_sequence]}"""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(_SQL_PATTERNS, {"route_id": ROUTE_ID})
        rows = cur.fetchall()

    result = {}
    for row in rows:
        dir_id = row["direction_id"]     
        if dir_id not in result:
            result[dir_id] = []
        result[dir_id].append(dict(row))
        
    return result


# Event building

def _build_events(trip: dict, pattern: list[dict]) -> list[dict]:
    """ For a single trip, build arrival + departure events per stop using cumulative segment jitter."""
    events = []
    
    # Base datetime 
    base_dt = datetime.combine(trip["trip_date"], trip["start_time"])
    
    # Random delay 
    current_delay = random.randint(DISPATCH_JITTER_MIN, DISPATCH_JITTER_MAX)

    base_event = {
        "trip_id":      trip["trip_id"],
        "device_id":    trip["device_id"],
        "route_id":     trip["route_id"],
        "direction_id": trip["direction_id"],
        "trip_date":    str(trip["trip_date"]),
    }

    for stop in pattern:
        # Accumulate traffic delay 
        segment_jitter = random.randint(SEGMENT_JITTER_MIN, SEGMENT_JITTER_MAX)
        current_delay += segment_jitter

        # Expected times
        expected_arr_dt = base_dt + timedelta(seconds=stop["scheduled_arrival_offset_s"])
        expected_dep_dt = base_dt + timedelta(seconds=stop["scheduled_departure_offset_s"])
        
        # Actual times (Expected + Delay)
        actual_arr_dt = expected_arr_dt + timedelta(seconds=current_delay)
        actual_dep_dt = expected_dep_dt + timedelta(seconds=current_delay)

        events.append({
            **base_event,
            "stop_id":            stop["stop_id"],
            "event_type":         "arrival",
            "expected_time":      str(expected_arr_dt.time()),
            "actual_time":        str(actual_arr_dt.time()),
            "delay_hint_seconds": current_delay,
        })
        
        events.append({
            **base_event,
            "stop_id":            stop["stop_id"],
            "event_type":         "departure",
            "expected_time":      str(expected_dep_dt.time()),
            "actual_time":        str(actual_dep_dt.time()),
            "delay_hint_seconds": current_delay,
        })

    return events



# Replay engine

def _time_to_seconds(t) -> int:
    """Convert datetime.time to total seconds since midnight."""
    return t.hour * 3600 + t.minute * 60 + t.second


def replay(trips: list[dict], patterns_by_dir: dict, speed: float, dry_run: bool, producer=None):
    """Replay all events in chronological order."""
    all_events = []
    for trip in trips:
        pattern = patterns_by_dir.get(trip["direction_id"], [])
        if not pattern:
            log.warning("direction_id=%s has no schedule pattern — skipping trip %s", 
                        trip["direction_id"], trip["trip_id"])
            continue

        events = _build_events(trip, pattern)
        all_events.extend(events)

    if not all_events:
        log.warning("No events to replay.")
        return

    # Sort by trip_date then actual_time to replay chronologically
    all_events.sort(key=lambda e: (e["trip_date"], e["actual_time"]))

    log.info("Replaying %d events across %d trips (speed=x%s, dry_run=%s)",
             len(all_events), len(trips), speed, dry_run)

    # Replay loop — emit each event at the correct simulated time
    sim_start_str = all_events[0]["actual_time"]
    sim_start_sec = _time_to_seconds(
        datetime.strptime(sim_start_str, "%H:%M:%S").time()
    )
    # Wall = wall-clock time 
    wall_start = time.monotonic()

    for event in all_events:
        event_sec = _time_to_seconds(
            datetime.strptime(event["actual_time"], "%H:%M:%S").time()
        )
        sim_delta = event_sec - sim_start_sec
        if sim_delta < 0:
            sim_delta += 86400  # handle midnight rollover

        target_wall = wall_start + (sim_delta / speed)
        sleep_for = target_wall - time.monotonic()
        if sleep_for > 0:
            time.sleep(sleep_for)

        event["emitted_at"] = datetime.now(timezone.utc).isoformat()

        if dry_run:
            log.info("[DRY-RUN] T=%s | S=%-4s | %-9s | Exp: %s | Act: %s | Dly: %+ds",
                     event["trip_id"], event["stop_id"], event["event_type"],
                     event["expected_time"], event["actual_time"], event["delay_hint_seconds"])
        else:
            producer.send(event)
            log.debug("Sent: trip=%s stop=%s type=%s actual=%s",
                      event["trip_id"], event["stop_id"],
                      event["event_type"], event["actual_time"])

    log.info("Replay finished.")



# CLI entry point

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

    trip_date = None
    if args.date:
        try:
            trip_date = date.fromisoformat(args.date)
        except ValueError:
            log.error("Invalid --date format. Use YYYY-MM-DD.")
            sys.exit(1)

    log.info("Connecting to PostgreSQL...")
    conn = connectDB()
    try:
        trips = _fetch_trips(conn, trip_date=trip_date, trip_id=args.trip_id)
        if not trips:
            log.warning("No trips found matching the filters. Exiting.")
            sys.exit(0)
        
        log.info("Fetched %d trip(s) from PostgreSQL.", len(trips))

        patterns_by_dir = _fetch_patterns(conn)
        log.info("Fetched schedule patterns for %d direction(s).", len(patterns_by_dir))
    finally:
        conn.close()

    producer = None
    if not args.dry_run:
        from producer import BusEventProducer
        producer = BusEventProducer(
            bootstrap_servers=args.kafka,
            topic=args.topic,
        )

    try:
        replay(
            trips=trips,
            patterns_by_dir=patterns_by_dir,
            speed=args.speed,
            dry_run=args.dry_run,
            producer=producer,
        )
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
