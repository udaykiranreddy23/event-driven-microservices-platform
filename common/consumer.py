import json
import time
from kafka import KafkaConsumer, KafkaProducer
from common.config import KAFKA_BOOTSTRAP_SERVERS, SERVICE_NAME
from common.db import connection

def consumer(topic, group_id):
    for _ in range(30):
        try:
            return KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode()),
            )
        except Exception:
            time.sleep(1)
    raise RuntimeError("Kafka unavailable")

def mark_processed(event_id):
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO processed_events(event_id, service_name) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (str(event_id), SERVICE_NAME),
            )
            return cur.rowcount == 1

def publish_dlq(event, error):
    p = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    event["error"] = str(error)
    p.send("events.dlq", event)
    p.flush()
    p.close()
