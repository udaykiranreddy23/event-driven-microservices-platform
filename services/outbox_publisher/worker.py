import json
import time

from common.config import DATABASE_URL, KAFKA_BOOTSTRAP_SERVERS
from common.db import init_db
from common.events import producer
import psycopg


POLL_INTERVAL_SECONDS = 2
BATCH_SIZE = 50


def publish_batch():
    published = 0

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, topic, event_key, payload
                FROM outbox
                WHERE published = FALSE
                ORDER BY created_at
                FOR UPDATE SKIP LOCKED
                LIMIT %s
                """,
                (BATCH_SIZE,),
            )

            rows = cur.fetchall()

            if not rows:
                return 0

            kafka = producer()

            try:
                for outbox_id, topic, event_key, payload in rows:
                    kafka.send(
                        topic,
                        payload,
                        key=str(event_key),
                    )

                kafka.flush()

                for outbox_id, _, _, _ in rows:
                    cur.execute(
                        """
                        UPDATE outbox
                        SET published = TRUE
                        WHERE id = %s
                        """,
                        (outbox_id,),
                    )

                published = len(rows)
                conn.commit()

            except Exception:
                conn.rollback()
                raise

            finally:
                kafka.close()

    return published


def main():
    init_db()

    while True:
        try:
            count = publish_batch()

            if count == 0:
                time.sleep(POLL_INTERVAL_SECONDS)

        except Exception as exc:
            print(f"outbox publisher error: {exc}", flush=True)
            time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
