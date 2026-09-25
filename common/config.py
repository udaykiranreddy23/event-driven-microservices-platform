import os
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://app:app@localhost:5432/orders")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SERVICE_NAME = os.getenv("SERVICE_NAME", "service")
