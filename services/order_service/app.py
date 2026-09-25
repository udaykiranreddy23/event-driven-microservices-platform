import json
import uuid
import redis
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
from common.config import REDIS_URL
from common.db import init_db, connection
from common.events import make_event, producer

app = FastAPI(title="Order Service", version="1.0.0")
r = redis.from_url(REDIS_URL, decode_responses=True)

class Item(BaseModel):
    sku: str = Field(min_length=1)
    quantity: int = Field(gt=0, le=100)

class OrderCreate(BaseModel):
    customer_id: str = Field(min_length=1)
    items: list[Item] = Field(min_length=1)

@app.on_event("startup")
def startup():
    init_db()

@app.get("/health")
def health():
    return {"status": "ok", "service": "order-service"}

@app.post("/orders")
def create_order(body: OrderCreate, idempotency_key: str | None = Header(None, alias="Idempotency-Key")):
    if not idempotency_key:
        raise HTTPException(400, "Idempotency-Key header is required")

    cache_key = f"idempotency:order:{idempotency_key}"
    existing = r.get(cache_key)
    if existing:
        return json.loads(existing)

    order_id = str(uuid.uuid4())
    payload = {
        "order_id": order_id,
        "customer_id": body.customer_id,
        "items": [x.model_dump() for x in body.items],
    }

    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders(id,customer_id,items) VALUES (%s,%s,%s::jsonb)",
                (order_id, body.customer_id, json.dumps(payload["items"])),
            )
            cur.execute(
                "INSERT INTO outbox(id,topic,event_key,payload) VALUES (%s,%s,%s,%s::jsonb)",
                (str(uuid.uuid4()), "orders.created", order_id, json.dumps(make_event("order.created", payload))),
            )

    result = {"order_id": order_id, "status": "CREATED"}
    r.setex(cache_key, 3600, json.dumps(result))

    # For a production deployment, publish through a dedicated outbox publisher.
    p = producer()
    p.send("orders.created", payload, key=order_id)
    p.flush()
    p.close()
    return result

@app.get("/orders")
def list_orders():
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id,customer_id,status,created_at FROM orders ORDER BY created_at DESC")
            rows = cur.fetchall()
    return [
        {"id": str(x[0]), "customer_id": x[1], "status": x[2], "created_at": x[3].isoformat()}
        for x in rows
    ]
