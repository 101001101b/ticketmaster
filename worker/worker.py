import os
import json
import logging
import pika
import redis
from datetime import datetime
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uvicorn

# --------------------------
# Configuration
# --------------------------
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
QUEUE_NAME    = "ticket_requests"
RESULT_QUEUE  = "ticket_results"

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

MODE = os.environ.get("MODE", "direct")
PORT = int(os.environ.get("PORT", 8000))

TOTAL_UNNUMBERED = 20000
TOTAL_NUMBERED   = 20000

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --------------------------
# Redis Connection
# --------------------------
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# --------------------------
# RabbitMQ helpers
# --------------------------
def _make_pika_params():
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS),
        heartbeat=60,
    )

def create_rabbitmq_channel():
    conn = pika.BlockingConnection(_make_pika_params())
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    ch.queue_declare(queue=RESULT_QUEUE, durable=True)
    return conn, ch

# Persistent channel for background publish tasks
_global_conn = None
_global_ch   = None

def get_global_channel():
    global _global_conn, _global_ch
    if _global_conn is None or _global_conn.is_closed or \
       _global_ch is None or _global_ch.is_closed:
        logger.info("Opening persistent RabbitMQ connection...")
        _global_conn = pika.BlockingConnection(_make_pika_params())
        _global_ch   = _global_conn.channel()
        _global_ch.queue_declare(queue=QUEUE_NAME, durable=True)
        _global_ch.queue_declare(queue=RESULT_QUEUE, durable=True)
    return _global_ch

# --------------------------
# Ticket Processing Logic
# --------------------------
def process_unnumbered(producer_id: str, request_id: str) -> bool:
    sold = r.incr("unnumbered_sold")
    if sold <= TOTAL_UNNUMBERED:
        logger.info(f"[UNNUMBERED] SUCCESS: {producer_id} req:{request_id} (sold={sold})")
        return True
    logger.info(f"[UNNUMBERED] FAILED: {producer_id} req:{request_id} (limit reached)")
    return False

def process_numbered(seat_id: int, producer_id: str, request_id: str) -> bool:
    key = f"seat:{seat_id}"
    success = r.setnx(key, producer_id)
    if success:
        r.incr("numbered_sold_count")
        logger.info(f"[NUMBERED] SUCCESS: {producer_id} req:{request_id} seat:{seat_id}")
    else:
        logger.info(f"[NUMBERED] FAILED: {producer_id} req:{request_id} seat:{seat_id} already sold")
    return success

def publish_result(request_id: str, producer_id: str, seat_id, success: bool):
    result = {
        "request_id": request_id,
        "producer_id": producer_id,
        "seat_id": seat_id,
        "success": success,
        "timestamp": datetime.utcnow().isoformat(),
    }
    global _global_conn, _global_ch
    for attempt in range(3):
        try:
            ch = get_global_channel()
            ch.basic_publish(
                exchange="",
                routing_key=RESULT_QUEUE,
                body=json.dumps(result),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            return  # success
        except Exception as e:
            logger.warning(f"publish_result attempt {attempt + 1} failed: {e}")
            _global_conn = None
            _global_ch   = None
    logger.error(f"publish_result definitively failed for request {request_id}")

# --------------------------
# FastAPI endpoints (Direct Mode)
# --------------------------
app = FastAPI(title="Ticket Worker", version="1.0")

class BuyRequest(BaseModel):
    producer_id: str
    request_id: str

@app.get("/health")
def health():
    return {"status": "healthy", "mode": MODE}

@app.post("/buy/unnumbered")
def buy_unnumbered(req: BuyRequest, background_tasks: BackgroundTasks):
    success = process_unnumbered(req.producer_id, req.request_id)
    background_tasks.add_task(publish_result, req.request_id, req.producer_id, None, success)
    return {"success": success, "producer_id": req.producer_id, "request_id": req.request_id}

@app.post("/buy/numbered/{seat_id}")
def buy_numbered(seat_id: int, req: BuyRequest, background_tasks: BackgroundTasks):
    if seat_id < 1 or seat_id > TOTAL_NUMBERED:
        raise HTTPException(status_code=400, detail=f"Seat ID must be between 1 and {TOTAL_NUMBERED}")
    success = process_numbered(seat_id, req.producer_id, req.request_id)
    background_tasks.add_task(publish_result, req.request_id, req.producer_id, seat_id, success)
    return {"success": success, "producer_id": req.producer_id, "request_id": req.request_id, "seat_id": seat_id}

@app.post("/reset")
def reset():
    r.set("unnumbered_sold", 0)
    r.set("unnumbered_success_count", 0)
    r.set("numbered_sold_count", 0)

    cursor, deleted = 0, 0
    while True:
        cursor, keys = r.scan(cursor, match="seat:*", count=200)
        if keys:
            r.delete(*keys)
            deleted += len(keys)
        if cursor == 0:
            break

    logger.info(f"System reset: all counters cleared, {deleted} seat keys removed")
    return {"status": "reset", "unnumbered_sold": 0, "numbered_sold": 0}

# --------------------------
# RabbitMQ Consumer (Indirect Mode)
# --------------------------
indirect_conn = None

def rabbitmq_callback(ch, method, properties, body):
    try:
        msg         = json.loads(body)
        type_       = msg.get("type")
        producer_id = msg.get("producer_id")
        request_id  = msg.get("request_id")
        seat_id     = msg.get("seat_id")

        if type_ == "unnumbered":
            success = process_unnumbered(producer_id, request_id)
        elif type_ == "numbered":
            success = process_numbered(seat_id, producer_id, request_id)
        else:
            success = False
            logger.warning(f"Unknown message type: {type_}")

        publish_result(request_id, producer_id, seat_id, success)
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
    else:
        ch.basic_ack(delivery_tag=method.delivery_tag)

def run_indirect_mode():
    global indirect_conn
    logger.info("Worker started in INDIRECT mode (RabbitMQ consumer)")
    indirect_conn, ch = create_rabbitmq_channel()
    ch.basic_qos(prefetch_count=1)
    ch.basic_consume(queue=QUEUE_NAME, on_message_callback=rabbitmq_callback)
    try:
        ch.start_consuming()
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
        ch.stop_consuming()
        if indirect_conn and indirect_conn.is_open:
            indirect_conn.close()

def run_direct_mode():
    logger.info(f"Worker started in DIRECT mode (HTTP server on port {PORT})")
    uvicorn.run(app, host="0.0.0.0", port=PORT)

# --------------------------
# Main Entry Point
# --------------------------
if __name__ == "__main__":
    if not r.exists("unnumbered_sold"):
        r.set("unnumbered_sold", 0)
        r.set("unnumbered_success_count", 0)
    if not r.exists("numbered_sold_count"):
        r.set("numbered_sold_count", 0)

    if MODE == "indirect":
        run_indirect_mode()
    else:
        run_direct_mode()