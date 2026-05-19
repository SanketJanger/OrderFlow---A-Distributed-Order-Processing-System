import asyncio
import json
import os
import asyncpg
import aio_pika
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")
REDIS_URL    = os.getenv("REDIS_URL")

async def update_order_status(conn, redis_client, order_id: str, status: str):
    await conn.execute(
        "UPDATE orders SET status=$1, updated_at=NOW() WHERE id=$2",
        status, order_id
    )
    await redis_client.publish(
        f"order:{order_id}",
        json.dumps({"order_id": order_id, "status": status})
    )
    print(f"[ORDER] {order_id[:8]} → {status}")

async def update_job_status(conn, job_id: str, status: str, result: dict = None):
    await conn.execute(
        "UPDATE jobs SET status=$1, result=$2, updated_at=NOW() WHERE id=$3",
        status, json.dumps(result) if result else None, job_id
    )

async def send_email(order_id: str) -> dict:
    print(f"[EMAIL] Sending confirmation for order {order_id[:8]}")
    await asyncio.sleep(0.8)
    return {"email_sent": True, "template": "order_confirmation"}

async def generate_invoice(order_id: str) -> dict:
    print(f"[INVOICE] Generating PDF for order {order_id[:8]}")
    await asyncio.sleep(1.2)
    return {"invoice_url": f"https://orderflow.io/invoices/{order_id}.pdf"}

async def notify_warehouse(order_id: str) -> dict:
    print(f"[WAREHOUSE] Notifying fulfillment for order {order_id[:8]}")
    await asyncio.sleep(0.5)
    return {"warehouse_notified": True, "estimated_ship_date": "2026-05-21"}

async def update_analytics(order_id: str, conn) -> dict:
    print(f"[ANALYTICS] Updating analytics for order {order_id[:8]}")
    await asyncio.sleep(0.3)
    total = await conn.fetchval(
        "SELECT total_amount FROM orders WHERE id=$1", order_id
    )
    return {"analytics_updated": True, "order_value": str(total)}

async def process_async_job(job_id: str, order_id: str,
                            job_type: str, db_pool, redis_client):
    async with db_pool.acquire() as conn:
        await update_job_status(conn, job_id, "running")
        try:
            if job_type == "send_email":
                result = await send_email(order_id)
            elif job_type == "generate_invoice":
                result = await generate_invoice(order_id)
            elif job_type == "notify_warehouse":
                result = await notify_warehouse(order_id)
            elif job_type == "update_analytics":
                result = await update_analytics(order_id, conn)
            else:
                result = {"skipped": True}

            await update_job_status(conn, job_id, "completed", result)
            print(f"[DONE] {job_type} completed for order {order_id[:8]}")

            pending = await conn.fetchval(
                """SELECT COUNT(*) FROM jobs
                   WHERE order_id=$1
                   AND job_type != 'sync_process'
                   AND status NOT IN ('completed', 'failed', 'dead')""",
                order_id
            )
            if pending == 0:
                await update_order_status(conn, redis_client, order_id, "fulfilling")

        except Exception as e:
            await update_job_status(conn, job_id, "failed", {"error": str(e)})
            print(f"[ERROR] {job_type} failed for order {order_id[:8]}: {e}")

async def main():
    print("Async worker starting...")
    db_pool      = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=5)
    rabbitmq     = await aio_pika.connect_robust(RABBITMQ_URL)
    redis_client = await aioredis.from_url(REDIS_URL, decode_responses=True)

    channel = await rabbitmq.channel()
    await channel.set_qos(prefetch_count=5)

    # Declare everything here so worker is self-contained
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

    print("Async worker ready — waiting for jobs...")

    async with async_queue.iterator() as queue_iter:
        async for message in queue_iter:
            payload  = json.loads(message.body.decode())
            job_id   = payload["job_id"]
            order_id = payload["order_id"]
            job_type = payload["job_type"]
            print(f"\n[JOB] Received {job_type} for order {order_id[:8]}")
            asyncio.create_task(
                process_async_job(job_id, order_id, job_type, db_pool, redis_client)
            )
            await message.ack()

if __name__ == "__main__":
    asyncio.run(main())
