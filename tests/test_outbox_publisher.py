from unittest.mock import MagicMock, patch

from services.outbox_publisher import worker


def test_publish_batch_publishes_events_and_marks_them_published():
    fake_rows = [
        (
            "outbox-1",
            "orders.created",
            "order-1",
            {
                "event_id": "event-1",
                "event_type": "order.created",
                "payload": {
                    "order_id": "order-1",
                },
            },
        )
    ]

    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = fake_rows

    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

    fake_kafka = MagicMock()

    with patch(
        "services.outbox_publisher.worker.psycopg.connect"
    ) as mock_connect, patch(
        "services.outbox_publisher.worker.producer",
        return_value=fake_kafka,
    ):
        mock_connect.return_value.__enter__.return_value = fake_conn

        result = worker.publish_batch()

    assert result == 1

    fake_kafka.send.assert_called_once_with(
        "orders.created",
        fake_rows[0][3],
        key="order-1",
    )

    fake_kafka.flush.assert_called_once()
    fake_conn.commit.assert_called_once()

    fake_cursor.execute.assert_any_call(
        """
                        UPDATE outbox
                        SET published = TRUE
                        WHERE id = %s
                        """,
        ("outbox-1",),
    )


def test_publish_batch_rolls_back_when_kafka_fails():
    fake_rows = [
        (
            "outbox-1",
            "orders.created",
            "order-1",
            {
                "event_id": "event-1",
                "event_type": "order.created",
                "payload": {
                    "order_id": "order-1",
                },
            },
        )
    ]

    fake_cursor = MagicMock()
    fake_cursor.fetchall.return_value = fake_rows

    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cursor

    fake_kafka = MagicMock()
    fake_kafka.flush.side_effect = RuntimeError("Kafka unavailable")

    with patch(
        "services.outbox_publisher.worker.psycopg.connect"
    ) as mock_connect, patch(
        "services.outbox_publisher.worker.producer",
        return_value=fake_kafka,
    ):
        mock_connect.return_value.__enter__.return_value = fake_conn

        try:
            worker.publish_batch()
            assert False, "Expected Kafka failure"
        except RuntimeError as exc:
            assert str(exc) == "Kafka unavailable"

    fake_conn.rollback.assert_called_once()
    fake_conn.commit.assert_not_called()