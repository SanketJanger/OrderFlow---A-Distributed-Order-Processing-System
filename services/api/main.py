import uuid
import json
import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import asyncpg
import aio_pika
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL  = os.getenv("DATABASE_URL")
RABBITMQ_URL  = os.getenv("RABBITMQ_URL")
REDIS_URL     = os.getenv("REDIS_URL")

# Global connections
db_pool       = None
rabbitmq_conn = None
redis_client  = None

# ─── Lifespan ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, rabbitmq_conn, redis_client

    # Startup
    db_pool       = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    rabbitmq_conn = await aio_pika.connect_robust(RABBITMQ_URL)
    redis_client  = await aioredis.from_url(REDIS_URL, decode_responses=True)

    # Setup RabbitMQ exchanges and queues
    async with rabbitmq_conn.channel() as channel:
        # Main exchange for routing jobs
        exchange = await channel.declare_exchange(
            "orderflow",
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        # Dead letter exchange
        dlx = await channel.declare_exchange(
            "orderflow.dlx",
            aio_pika.ExchangeType.DIRECT,
            durable=True
        )
        # Sync queue — cart validation, inventory, payment
        sync_queue = await channel.declare_queue(
            "sync.jobs",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "orderflow.dlx",
                "x-dead-letter-routing-key": "dead.jobs"
            }
        )
        # Async queue — email, invoice, warehouse, analytics
        async_queue = await channel.declare_queue(
            "async.jobs",
            durable=True,
            arguments={
                "x-dead-letter-exchange": "orderflow.dlx",
                "x-dead-letter-routing-key": "dead.jobs"
            }
        )
        # Dead letter queue
        dead_queue = await channel.declare_queue(
            "dead.jobs",
            durable=True
        )
        await sync_queue.bind(exchange, "sync.jobs")
        await async_queue.bind(exchange, "async.jobs")
        await dead_queue.bind(dlx, "dead.jobs")

    print("OrderFlow API started")
    yield

    # Shutdown
    await db_pool.close()
    await rabbitmq_conn.close()
    await redis_client.close()
    print("OrderFlow API stopped")

# ─── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(title="OrderFlow API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Models ──────────────────────────────────────────────────────────────────
class OrderItem(BaseModel):
    product_id: str
    quantity: int

class CreateOrderRequest(BaseModel):
    user_id: str
    items: List[OrderItem]
    priority: Optional[str] = "medium"

# ─── Helper ──────────────────────────────────────────────────────────────────
async def publish_job(job_type: str, payload: dict, queue: str, priority: str):
    """Publish a job to RabbitMQ."""
    async with rabbitmq_conn.channel() as channel:
        exchange = await channel.get_exchange("orderflow")
        message = aio_pika.Message(
            body=json.dumps(payload).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority={"high": 10, "medium": 5, "low": 1}.get(priority, 5),
            content_type="application/json"
        )
        await exchange.publish(message, routing_key=queue)

async def update_order_status(order_id: str, status: str):
    """Update order status in PostgreSQL and notify via Redis pub/sub."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2",
            status, order_id
        )
    # Publish status update to Redis for WebSocket
    await redis_client.publish(
        f"order:{order_id}",
        json.dumps({"order_id": order_id, "status": status})
    )

# ─── Routes ──────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    """Pipeline health check."""
    try:
        # Check PostgreSQL
        async with db_pool.acquire() as conn:
            total_orders = await conn.fetchval("SELECT COUNT(*) FROM orders")
            total_jobs   = await conn.fetchval("SELECT COUNT(*) FROM jobs")
            dlq_count    = await conn.fetchval("SELECT COUNT(*) FROM dead_letter_queue")
            last_order   = await conn.fetchval("SELECT MAX(created_at) FROM orders")

        # Check Redis
        await redis_client.ping()

        return {
            "status":          "healthy",
            "total_orders":    total_orders,
            "total_jobs":      total_jobs,
            "dlq_count":       dlq_count,
            "last_order_at":   str(last_order) if last_order else None,
            "postgres":        "reachable",
            "redis":           "reachable",
            "rabbitmq":        "reachable"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Health check failed: {e}")

@app.get("/products")
async def get_products():
    """Get all products with current stock."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM products ORDER BY name")
        return [dict(r) for r in rows]

@app.post("/orders")
async def create_order(request: CreateOrderRequest):
    """Create a new order and push to sync queue."""
    async with db_pool.acquire() as conn:
        # Validate products exist
        for item in request.items:
            product = await conn.fetchrow(
                "SELECT * FROM products WHERE id=$1", item.product_id
            )
            if not product:
                raise HTTPException(
                    status_code=404,
                    detail=f"Product {item.product_id} not found"
                )

        # Calculate total
        total = 0
        for item in request.items:
            product = await conn.fetchrow(
                "SELECT price FROM products WHERE id=$1", item.product_id
            )
            total += float(product["price"]) * item.quantity

        # Create order
        order_id = str(uuid.uuid4())
        await conn.execute(
            """INSERT INTO orders (id, user_id, status, total_amount)
               VALUES ($1, $2, 'pending', $3)""",
            order_id, request.user_id, total
        )

        # Create order items
        for item in request.items:
            product = await conn.fetchrow(
                "SELECT price FROM products WHERE id=$1", item.product_id
            )
            await conn.execute(
                """INSERT INTO order_items (order_id, product_id, quantity, price)
                   VALUES ($1, $2, $3, $4)""",
                order_id, item.product_id, item.quantity, product["price"]
            )

        # Create sync job
        job_id = str(uuid.uuid4())
        await conn.execute(
            """INSERT INTO jobs (id, order_id, job_type, status, priority)
               VALUES ($1, $2, 'sync_process', 'pending', $3)""",
            job_id, order_id, request.priority
        )

    # Push to sync queue
    await publish_job(
        job_type="sync_process",
        payload={
            "job_id":   job_id,
            "order_id": order_id,
            "priority": request.priority,
            "items":    [i.dict() for i in request.items]
        },
        queue="sync.jobs",
        priority=request.priority
    )

    await update_order_status(order_id, "queued")

    return {
        "order_id": order_id,
        "job_id":   job_id,
        "status":   "queued",
        "total":    total
    }

@app.get("/orders/{order_id}")
async def get_order(order_id: str):
    """Get order details with all jobs."""
    async with db_pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT * FROM orders WHERE id=$1", order_id
        )
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")

        jobs = await conn.fetch(
            "SELECT * FROM jobs WHERE order_id=$1 ORDER BY created_at",
            order_id
        )
        items = await conn.fetch(
            """SELECT oi.*, p.name as product_name
               FROM order_items oi
               JOIN products p ON p.id = oi.product_id
               WHERE oi.order_id=$1""",
            order_id
        )

        return {
            "order": dict(order),
            "jobs":  [dict(j) for j in jobs],
            "items": [dict(i) for i in items]
        }

@app.get("/orders")
async def list_orders():
    """List all orders with latest status."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT 50"
        )
        return [dict(r) for r in rows]

@app.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: str):
    """Cancel a pending or queued order."""
    async with db_pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT * FROM orders WHERE id=$1", order_id
        )
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        if order["status"] not in ("pending", "queued"):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot cancel order in status: {order['status']}"
            )

    await update_order_status(order_id, "cancelled")
    return {"order_id": order_id, "status": "cancelled"}

# ─── WebSocket ───────────────────────────────────────────────────────────────
@app.websocket("/ws/{order_id}")
async def websocket_endpoint(websocket: WebSocket, order_id: str):
    """Stream real-time order status updates to the browser."""
    await websocket.accept()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(f"order:{order_id}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                await websocket.send_text(message["data"])
    except WebSocketDisconnect:
        await pubsub.unsubscribe(f"order:{order_id}")
