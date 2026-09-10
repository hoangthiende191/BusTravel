"""
producer.py - Kafka producer wrapper for the BusTravel Event Simulator.

Wraps confluent_kafka.Producer to provide:
  - Simple send() interface with JSON serialization
  - Delivery confirmation logging
  - Graceful flush on close
"""

import json
import logging

from confluent_kafka import Producer

log = logging.getLogger("producer")


class BusEventProducer:
    """
    Thin wrapper around confluent_kafka.Producer.

    Usage:
        producer = BusEventProducer(bootstrap_servers="localhost:9092")
        producer.send("bus-events", event_dict)
        producer.close()
    """

    def __init__(self, bootstrap_servers: str = "localhost:9092", topic: str = "bus-events"):
        self._topic = topic
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            # Retry up to 3 times on transient network errors
            "retries": 3,
            "retry.backoff.ms": 500,
        })
        log.info("Kafka producer connected to %s | topic: %s", bootstrap_servers, topic)

    def send(self, event: dict) -> None:
        """
        Serialize event to JSON and produce to the configured topic.
        Delivery errors are logged but do not raise — the simulator continues.
        """
        payload = json.dumps(event, default=str).encode("utf-8")

        self._producer.produce(
            topic=self._topic,
            value=payload,
            key=str(event.get("trip_id", "")).encode("utf-8"),
            on_delivery=self._on_delivery,
        )

        # Trigger delivery callbacks without blocking
        self._producer.poll(0)

    def close(self) -> None:
        """Flush all pending messages before shutdown (up to 10 seconds)."""
        log.info("Flushing Kafka producer...")
        remaining = self._producer.flush(timeout=10)
        if remaining > 0:
            log.warning("%d message(s) were NOT delivered before timeout", remaining)
        else:
            log.info("All messages delivered successfully.")

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err:
            log.error("Delivery failed | topic=%s | error=%s", msg.topic(), err)
        else:
            log.debug("Delivered | topic=%s | partition=%d | offset=%d",
                      msg.topic(), msg.partition(), msg.offset())
