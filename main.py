from fastapi import FastAPI, Header, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from collections import deque
from threading import Lock
import time

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TOTAL_ORDERS = 60
RATE_LIMIT = 18
WINDOW_SECONDS = 10

orders = {}
idempotency_keys = {}
next_order_id = 61

rate_buckets = {}
lock = Lock()


class OrderRequest(BaseModel):
    item: str = "sample item"


@app.get("/")
def home():
    return {"status": "ok", "total_orders": TOTAL_ORDERS}


@app.post("/orders", status_code=201)
def create_order(
    body: OrderRequest,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
):
    global next_order_id

    with lock:
        if idempotency_key in idempotency_keys:
            response.status_code = 200
            return idempotency_keys[idempotency_key]

        order = {
            "id": next_order_id,
            "item": body.item,
        }
        next_order_id += 1

        orders[order["id"]] = order
        idempotency_keys[idempotency_key] = order
        return order


@app.get("/orders")
def list_orders(
    limit: int = 10,
    cursor: Optional[str] = None,
):
    limit = max(1, min(limit, 60))

    try:
        start_id = int(cursor) + 1 if cursor is not None else 1
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid cursor")

    if start_id < 1:
        start_id = 1

    end_id = min(start_id + limit - 1, TOTAL_ORDERS)

    items = [
        {"id": order_id, "item": f"catalog item {order_id}"}
        for order_id in range(start_id, end_id + 1)
    ]

    next_cursor = str(end_id) if end_id < TOTAL_ORDERS else None

    return {
        "items": items,
        "next_cursor": next_cursor,
    }


@app.middleware("http")
async def client_rate_limit(request, call_next):
    # Rate limit only the orders API routes.
    if request.url.path not in ("/orders",):
        return await call_next(request)

    client_id = request.headers.get("X-Client-Id", "default")
    now = time.monotonic()

    with lock:
        bucket = rate_buckets.setdefault(client_id, deque())

        while bucket and now - bucket[0] >= WINDOW_SECONDS:
            bucket.popleft()

        if len(bucket) >= RATE_LIMIT:
            retry_after = max(
                1, int(WINDOW_SECONDS - (now - bucket[0]) + 0.999)
            )
            return Response(
                content='{"detail":"Rate limit exceeded"}',
                status_code=429,
                headers={
                    "Retry-After": str(retry_after),
                    "Content-Type": "application/json",
                },
            )

        bucket.append(now)

    return await call_next(request)
