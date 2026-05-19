import asyncio
import json
import os
import random
import uuid
import asyncpg
import aio_pika
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL    = os.getenv("REDIS_URL")

async def update_order_status(conn, redis_client, order_id, status):
    await conn.execute(
        "UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2",
        status, order_id
    )
    await redis_client.publish(
        f"order:{order_id}",
        json.dumps({"order_id": order_id, "status": status})
    )
    print(f"[ORDER] {order_id[:8]} -> {status}", flush=True)

async def update_job_status(conn, job_id, status, error=None):
    await conn.execute(
        "UPDATE jobs SET status=$1, error_message=$2, updated_at=NOW() WHERE id=$3",
        status, error, job_id
    )

async def increment_job_attempts(conn, job_id):
    await conn.execute(
        "UPDATE jobs SET attempts=attempts+1, updated_at=NOW() WHERE id=$1",
        job_id
    )

async def send_to_dlq(conn, job_id, order_id, job_type, error, payload):
    await conn.execute(
        """INSERT INTO dead_letter_queue
           (job_id, order_id, job_type, error_message, payload)
           VALUES ($1, $2, $3, $4, $5)""",
        job_id, order_id, job_type, error, json.dumps(payload)
    )
    print(f"[DLQ] Job {job_id[:8]} -> dead letter queue | reason={error}", flush=True)

async def validate_cart(conn, order_id, items):
    print(f"[VALIDATE] Checking cart for order {order_id[:8]}", flush=True)
    await asyncio.sleep(0.5)
    for item in items:
        product = await conn.fetchrow(
            "SELECT * FROM products WHERE id=$1", item["product_id"]
        )
        if not product:
            return False, f"Product {item['product_id']} not found"
        if product["stock"] < item["quantity"]:
            return False, f"Insufficient stock for {product['name']}"
    return True, "Cart valid"

async def reserve_inventory(conn, order_id, items):
    print(f"[INVENTORY] Reserving stock for order {order_id[:8]}", flush=True)
    await asyncio.sleep(0.7)
    for item in items:
        product = await conn.fetchrow(
            "SELECT * FROM products WHERE id=$1 FOR UPDATE",
            item["product_id"]
        )
        if product["stock"] < item["quantity"]:
            return False, f"Race condition - stock depleted for {product['name']}"
        await conn.execute(
            "UPDATE products SET stock=stock-$1 WHERE id=$2",
            item["quantity"], item["product_id"]
        )
    return True, "Inventory reserved"

async def process_payment(order_id):
    print(f"[PAYMENT] Processing payment for order {order_id[:8]}", flush=True)
    await asyncio.sleep(1.0)
    if random.random() < 0.15:
        return False, "Payment gateway declined"
    return True, "Payment successful"

async def process_sync_job(payload, db_pool, redis_client, exchange):
    job_id   = payload["job_id"]
    order_id = payload["order_id"]
    items    = payload["items"]

    async with db_pool.acquire() as conn:
        order = await conn.fetchrow(
            "SELECT status FROM orders WHERE id=$1", order_id
        )
        if order["status"] == "cancelled":
            print(f"[SKIP] Order {order_id[:8]} was cancelled", flush=True)
            await update_job_status(conn, job_id, "cancelled")
            return True

        job = await conn.fetchrow("SELECT * FROM jobs WHERE id=$1", job_id)
        if job["attempts"] >= job["max_attempts"]:
            await send_to_dlq(conn, job_id, order_id, "sync_process",
                              "Max attempts reached", payload)
            await update_job_status(conn, job_id, "dead")
            await update_order_status(conn, redis_client, order_id, "failed")
            return True

        await increment_job_attempts(conn, job_id)
        await update_job_status(conn, job_id, "running")
        await update_order_status(conn, redis_client, order_id, "validating")

        success, message = await validate_cart(conn, order_id, items)
        if not success:
            await update_job_status(conn, job_id, "failed", message)
            await update_order_status(conn, redis_client, order_id, "validation_failed")
            await send_to_dlq(conn, job_id, order_id, "sync_process", message, payload)
            return True

        await update_order_status(conn, redis_client, order_id, "inventory_reserving")

        async with conn.transaction():
            success, message = await reserve_inventory(conn, order_id, items)
            if not success:
                await update_job_status(conn, job_id, "failed", message)
                await update_order_status(conn, redis_client, order_id, "inventory_failed")
                await send_to_dlq(conn, job_id, order_id, "sync_process", message, payload)
                return True

        await update_order_status(conn, redis_client, order_id, "payment_processing")

        success, message = await process_payment(order_id)
        if not success:
            print(f"[PAYMENT] Failed for order {order_id[:8]}", flush=True)
            await update_job_status(conn, job_id, "failed", message)
            await update_order_status(conn, redis_client, order_id, "payment_failed")
            return False

        await update_job_status(conn, job_id, "completed")
        await update_order_status(conn, redis_client, order_id, "confirmed")

        async_jobs = ["send_email", "generate_invoice", "notify_warehouse", "update_analytics"]
        for job_type in async_jobs:
            async_job_id = str(uuid.uuid4())
            await conn.execute(
                """INSERT INTO jobs (id, order_id, job_type, status, priority)
                   VALUES ($1, $2, $3, 'pending', 'medium')""",
                async_job_id, order_id, job_type
            )
            msg = aio_pika.Message(
                body=json.dumps({
                    "job_id": async_job_id,
                    "order_id": order_id,
                    "job_type": job_type
                }).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            await exchange.publish(msg, routing_key="async.jobs")
            print(f"[ASYNC] Queued {job_type} for order {order_id[:8]}", flush=True)

        print(f"[CONFIRMED] Order {order_id[:8]} confirmed", flush=True)
        return True

async def main():
    print("Sync worker starting...", flush=True)
    db_pool      = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    rabbitmq     = await aio_pika.connect_robust(RABBITMQ_URL)
    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)

    channel = await rabbitmq.channel()
    await channel.set_qos(prefetch_count=1)

    exchange = await channel.declare_exchange(
        "orderflow", aio_pika.ExchangeType.DIRECT, durable=True
    )
    dlx = await channel.declare_exchange(
        "orderflow.dlx", aio_pika.ExchangeType.DIRECT, durable=True
    )
    sync_queue = await channel.declare_queue(
        "sync.jobs", durable=True,
        arguments={
            "x-dead-letter-exchange": "orderflow.dlx",
            "x-dead-letter-routing-key": "dead.jobs"
        }
    )
    async_queue = await channel.declare_queue(
        "async.jobs", durable=True,
        arguments={
            "x-dead-letter-exchange": "orderflow.dlx",
            "x-dead-letter-routing-key": "dead.jobs"
        }
    )
    dead_queue = await channel.declare_queue("dead.jobs", durable=True)
    await sync_queue.bind(exchange, "sync.jobs")
    await async_queue.bind(exchange, "async.jobs")
    await dead_queue.bind(dlx, "dead.jobs")

    print("Sync worker ready - waiting for jobs...", flush=True)

    async with sync_queue.iterator() as queue_iter:
        async for message in queue_iter:
            payload = json.loads(message.body.decode())
            print(f"\n[JOB] Received sync job for order {payload['order_id'][:8]}", flush=True)
            try:
                success = await process_sync_job(payload, db_pool, redis_client, exchange)
                if success:
                    await message.ack()
                else:
                    job_id = payload["job_id"]
                    async with db_pool.acquire() as conn:
                        job = await conn.fetchrow(
                            "SELECT attempts FROM jobs WHERE id=$1", job_id
                        )
                    delay = 2 ** job["attempts"]
                    print(f"[RETRY] Retrying in {delay}s", flush=True)
                    await asyncio.sleep(delay)
                    await message.nack(requeue=True)
            except Exception as e:
                print(f"[ERROR] Unexpected error: {e}", flush=True)
                await message.nack(requeue=False)

if __name__ == "__main__":
    asyncio.run(main())
