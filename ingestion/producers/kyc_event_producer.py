from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Iterable
from typing import Any

from confluent_kafka import Producer

try:
    from ingestion.producers.contract_validator import validate_event
    from ingestion.producers.event_factory import build_customer_journey, build_kyc_event
except ModuleNotFoundError:
    from contract_validator import validate_event
    from event_factory import build_customer_journey, build_kyc_event


def delivery_report(error: Exception | None, message: Any) -> None:
    if error is not None:
        print(f"delivery_failed topic={message.topic()} error={error}")
        return
    print(f"delivered topic={message.topic()} partition={message.partition()} offset={message.offset()}")


def iter_events(total_events: int, journey_mode: bool) -> Iterable[dict[str, Any]]:
    if not journey_mode:
        for _ in range(total_events):
            yield build_kyc_event()
        return
    emitted = 0
    while emitted < total_events:
        for event in build_customer_journey():
            if emitted >= total_events:
                break
            emitted += 1
            yield event


def produce(events: int, sleep_ms: int, journey_mode: bool) -> None:
    producer = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"), "client.id": "kyc-event-producer", "enable.idempotence": True, "acks": "all", "compression.type": "zstd", "linger.ms": 10})
    topic = os.getenv("KYC_EVENTS_TOPIC", "kyc.events.v1")
    for event in iter_events(events, journey_mode):
        validate_event(event)
        key = f"{event['tenant_id']}::{event['customer_id']}"
        producer.produce(topic=topic, key=key.encode(), value=json.dumps(event, separators=(",", ":")).encode(), on_delivery=delivery_report)
        producer.poll(0)
        if sleep_ms:
            time.sleep(sleep_ms / 1000)
    producer.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce synthetic financial KYC lifecycle events.")
    parser.add_argument("--events", type=int, default=100)
    parser.add_argument("--sleep-ms", type=int, default=0)
    parser.add_argument("--journey-mode", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    produce(events=args.events, sleep_ms=args.sleep_ms, journey_mode=args.journey_mode)
