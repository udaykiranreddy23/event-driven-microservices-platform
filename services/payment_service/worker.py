import time
from common.db import init_db
from common.consumer import consumer, mark_processed, publish_dlq

def handle(payload):
    print("[payment_service] Payment processing: order=" + payload["order_id"])

def main():
    init_db()
    c = consumer("orders.created", "payment")
    for message in c:
        payload = message.value
        event_id = payload.get("event_id", f"offset-{message.offset}")
        try:
            if mark_processed(event_id):
                handle(payload)
            c.commit()
        except Exception as exc:
            publish_dlq(payload, exc)
            c.commit()

if __name__ == "__main__":
    main()
