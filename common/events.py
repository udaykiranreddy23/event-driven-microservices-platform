import json
import time
import uuid
from kafka import KafkaProducer
from common.config import KAFKA_BOOTSTRAP_SERVERS

def producer():
    for _ in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode(),
                key_serializer=lambda v: v.encode() if isinstance(v, str) else v,
                retries=5,
            )
        except Exception:
            time.sleep(1)
    raise RuntimeError("Kafka unavailable")

def make_event(event_type, payload):
    return {"event_id": str(uuid.uuid4()), "event_type": event_type, "payload": payload}
