"""Order-event producer for Shop-A-Zone.

Emits JSON order events to Kafka. Every N-th event is re-sent unchanged to
simulate an at-least-once producer, so the pipeline has real duplicates to
collapse.
"""

from __future__ import annotations

import json
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "shopazone.orders.events")
RATE = float(os.getenv("EVENTS_PER_SECOND", "1"))

running = True


def _stop(*_args) -> None:
    global running
    running = False


def main() -> None:
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    producer = Producer({"bootstrap.servers": BOOTSTRAP, "client.id": "shopazone-orders"})
    random.seed(42)
    state: dict[int, str] = {}
    last_message: dict | None = None
    sent = 0

    print(f"[producer] {TOPIC} @ {BOOTSTRAP} ({RATE} events/s)", flush=True)

    while running:
        order_id = random.randint(2001, 2060)
        status = state.get(order_id, "CREATED")
        transitions = {
            "CREATED": ["PAID", "CANCELLED"],
            "PAID": ["COMPLETED", "CANCELLED", "REFUNDED"],
            "COMPLETED": ["REFUNDED"],
            "CANCELLED": ["CANCELLED"],
            "REFUNDED": ["REFUNDED"],
        }
        status = random.choice(transitions[status])
        state[order_id] = status

        event = {
            "event_id": uuid.uuid4().hex,
            "order_id": order_id,
            "customer_id": 501 + (order_id % 8),
            "order_amount": round(random.uniform(40, 1500), 2),
            "order_status": status,
            "event_time": datetime.now(timezone.utc).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # At-least-once delivery: replay the previous event verbatim now and then.
        payload = last_message if sent and sent % 17 == 0 and last_message else event
        producer.produce(TOPIC, key=str(payload["order_id"]), value=json.dumps(payload))
        producer.poll(0)
        last_message = event
        sent += 1

        if sent % 10 == 0:
            print(f"[producer] sent={sent} last_order={payload['order_id']} "
                  f"status={payload['order_status']}", flush=True)

        time.sleep(1.0 / RATE if RATE > 0 else 1.0)

    producer.flush(10)
    print(f"[producer] stopped after {sent} events", flush=True)


if __name__ == "__main__":
    sys.exit(main())
