from common.db import init_db
from common.consumer import consumer, mark_processed, publish_dlq


def handle(event):
    payload = event["payload"]
    print(
        "[analytics_service] Analytics recording: order="
        + payload["order_id"]
    )


def main():
    init_db()
    c = consumer("orders.created", "analytics")

    for message in c:
        event = message.value
        event_id = event.get("event_id", f"offset-{message.offset}")

        try:
            if mark_processed(event_id):
                handle(event)

            c.commit()

        except Exception as exc:
            publish_dlq(event, exc)
            c.commit()


if __name__ == "__main__":
    main()