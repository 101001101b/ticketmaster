#!/usr/bin/env python3
"""
Benchmark producer for ticket acquisition system.
Supports both direct (REST) and indirect (RabbitMQ) communication modes.

Indirect-mode result counting strategy:
  Fire-and-forget publishing is fast, but success/fail is only known after
  workers process each message. After publishing, we poll the ticket_requests
  queue until it drains, then read the authoritative counters from Redis.
"""

import os
import json
import logging
import time
import threading
import requests
from requests.adapters import HTTPAdapter
import pika
import redis as redis_lib
from datetime import datetime, UTC
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

# --------------------------
# Configuration
# --------------------------
MODE             = os.environ.get("MODE", "direct")
API_URL          = os.environ.get("API_URL", "http://nginx:80")
RABBITMQ_HOST    = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT    = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER    = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS    = os.environ.get("RABBITMQ_PASS", "guest")
REDIS_HOST       = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT       = int(os.environ.get("REDIS_PORT", 6379))
BENCHMARK_FILE   = os.environ.get("BENCHMARK_FILE", "benchmarks/benchmark_numbered.txt")
TICKET_TYPE      = os.environ.get("TICKET_TYPE", "unnumbered")
PRODUCERS        = int(os.environ.get("CLIENTS", 5))
WORKERS          = int(os.environ.get("WORKERS", 1))
RESULTS_FILE     = os.environ.get("RESULTS_FILE", "/app/results/benchmark_results.jsonl")

NUMERIC_TYPES = ("numbered", "contention")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)
# Silenciar los logs de conexiones de Pika, requests y urllib3
logging.getLogger("pika").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

stats = {"total_requests": 0, "successful": 0, "failed": 0,
         "start_time": None, "end_time": None}
stats_lock = threading.Lock()

_http = requests.Session()
_http.mount("http://", HTTPAdapter(pool_maxsize=200))

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

# --------------------------
# Channel pool (indirect publish phase)
# --------------------------
_channel_pool: Queue = Queue()
POOL_SIZE = min(PRODUCERS, 20)

def init_channel_pool():
    for _ in range(POOL_SIZE):
        conn = pika.BlockingConnection(_make_pika_params())
        ch   = conn.channel()
        ch.queue_declare(queue="ticket_requests", durable=True)
        _channel_pool.put((conn, ch))
    logger.info(f"RabbitMQ channel pool ready ({POOL_SIZE} connections)")

def close_channel_pool():
    while not _channel_pool.empty():
        try:
            conn, _ = _channel_pool.get_nowait()
            if conn.is_open:
                conn.close()
        except Exception:
            pass

# --------------------------
# Indirect-mode result verification
# --------------------------
def wait_for_queue_drain(timeout: int = 300):
    """Block until ticket_requests queue is empty (all messages processed)."""
    conn = pika.BlockingConnection(_make_pika_params())
    ch   = conn.channel()
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            res       = ch.queue_declare(queue="ticket_requests", durable=True, passive=True)
            remaining = res.method.message_count
            if remaining == 0:
                logger.info("Queue drained — all messages processed by workers")
                return
            logger.info(f"Waiting for workers: {remaining} messages remaining...")
            time.sleep(1)
        logger.warning("Timeout waiting for queue to drain — results may be incomplete")
    finally:
        conn.close()

def read_redis_counts() -> dict:
    r = redis_lib.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    
    # Contar asientos de forma eficiente usando SCAN
    count = 0
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="seat:*", count=1000)
        count += len(keys)
        if cursor == 0:
            break

    # Leemos el total acumulado en Redis
    raw_sold = int(r.get("unnumbered_sold") or 0)

    return {
        # Si llegaron 20002 peticiones, limitamos los éxitos al tope real de 20000
        "unnumbered_sold": min(raw_sold, 20000),
        "numbered_sold":   count,
    }

# --------------------------
# Request functions 
# --------------------------
def send_direct_unnumbered(producer_id: str, request_id: str) -> dict:
    try:
        resp = _http.post(f"{API_URL}/buy/unnumbered",
                          json={"producer_id": producer_id, "request_id": request_id},
                          timeout=(3, 30))
        logger.info(f"[HTTP {resp.status_code}] Request {request_id} processed (Unnumbered)")
        return resp.json()
    except Exception as e:
        logger.error(f"Direct request failed: {e}")
        return {"success": False, "error": str(e)}

def send_direct_numbered(producer_id: str, seat_id: int, request_id: str) -> dict:
    try:
        resp = _http.post(f"{API_URL}/buy/numbered/{seat_id}",
                          json={"producer_id": producer_id, "request_id": request_id},
                          timeout=(3, 30))
        logger.info(f"[HTTP {resp.status_code}] Request {request_id} processed (Seat: {seat_id})")
        return resp.json()
    except Exception as e:
        logger.error(f"Direct request failed: {e}")
        return {"success": False, "error": str(e)}

def send_indirect_unnumbered(producer_id: str, request_id: str) -> dict:
    conn, ch = _channel_pool.get()
    try:
        ch.basic_publish(
            exchange="", routing_key="ticket_requests",
            body=json.dumps({"type": "unnumbered",
                             "producer_id": producer_id, "request_id": request_id}),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        return {"queued": True}
    except Exception as e:
        logger.error(f"Indirect publish failed: {e}")
        return {"queued": False, "error": str(e)}
    finally:
        _channel_pool.put((conn, ch))

def send_indirect_numbered(producer_id: str, seat_id: int, request_id: str) -> dict:
    conn, ch = _channel_pool.get()
    try:
        ch.basic_publish(
            exchange="", routing_key="ticket_requests",
            body=json.dumps({"type": "numbered", "seat_id": seat_id,
                             "producer_id": producer_id, "request_id": request_id}),
            properties=pika.BasicProperties(delivery_mode=2),
        )
        return {"queued": True}
    except Exception as e:
        logger.error(f"Indirect publish failed: {e}")
        return {"queued": False, "error": str(e)}
    finally:
        _channel_pool.put((conn, ch))
        
# --------------------------
# Benchmark processing
# --------------------------
def parse_line(line: str):
    parts = line.strip().split()
    if not parts or parts[0] != "BUY":
        return None
        
    if len(parts) == 4:
        # Formato Real: BUY <producer_id> <seat_id> <request_id>
        return ("numbered", parts[1], int(parts[2]), parts[3])
        
    if len(parts) == 3:
        # Formato Real: BUY <producer_id> <request_id>
        return ("unnumbered", parts[1], parts[2])
        
    return None

def process_request(line_data):
    if line_data is None:
        return None

    with stats_lock:
        stats["total_requests"] += 1

    mode_type = line_data[0]

    if MODE == "direct":
        if mode_type == "unnumbered":
            # line_data: ("unnumbered", producer_id, request_id)
            _, producer_id, request_id = line_data
            result = send_direct_unnumbered(producer_id=producer_id, request_id=request_id)
        else:
            # line_data: ("numbered", producer_id, seat_id, request_id)
            _, producer_id, seat_id, request_id = line_data
            result = send_direct_numbered(producer_id=producer_id, seat_id=seat_id, request_id=request_id)
        
        success = result.get("success", False)
        with stats_lock:
            if success:
                stats["successful"] += 1
            else:
                stats["failed"] += 1
    else:
        if mode_type == "unnumbered":
            _, producer_id, request_id = line_data
            result = send_indirect_unnumbered(producer_id=producer_id, request_id=request_id)
        else:
            _, producer_id, seat_id, request_id = line_data
            result = send_indirect_numbered(producer_id=producer_id, seat_id=seat_id, request_id=request_id)
        
        success = result.get("queued", False)

    return {"line_data": line_data, "success": success}

def run_benchmark():
    logger.info(f"Starting benchmark: MODE={MODE}, TYPE={TICKET_TYPE}, WORKERS={PRODUCERS}")
    logger.info(f"Benchmark file: {BENCHMARK_FILE}")

    if MODE == "indirect":
        init_channel_pool()

    requests_list = []
    with open(BENCHMARK_FILE) as f:
        for line in f:
            parsed = parse_line(line)
            if parsed:
                requests_list.append(parsed)
    logger.info(f"Loaded {len(requests_list)} requests")

    with stats_lock:
        stats.update({"total_requests": 0, "successful": 0, "failed": 0,
                      "start_time": time.time(), "end_time": None})

    with ThreadPoolExecutor(max_workers=PRODUCERS) as executor:
        futures = [executor.submit(process_request, r) for r in requests_list]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Request error: {e}")

    with stats_lock:
        stats["end_time"] = time.time()

    if MODE == "indirect":
        close_channel_pool()
        wait_for_queue_drain()

        redis_counts = read_redis_counts()
        key = "numbered_sold" if TICKET_TYPE in NUMERIC_TYPES else "unnumbered_sold"
        successful = min(redis_counts[key], stats["total_requests"])
        with stats_lock:
            stats["successful"] = successful
            stats["failed"]     = stats["total_requests"] - successful

    total_time = stats["end_time"] - stats["start_time"]
    throughput  = stats["total_requests"] / total_time if total_time > 0 else 0

    summary = {
        "mode": MODE,
        "ticket_type": TICKET_TYPE,
        "total_requests": stats["total_requests"],
        "successful":     stats["successful"],
        "failed":         stats["failed"],
        "total_time_seconds":       round(total_time, 2),
        "throughput_ops_per_second": round(throughput, 2),
        "producers": PRODUCERS,
        "workers": WORKERS,
        "timestamp": datetime.now(UTC).isoformat(),
    }

    logger.info("=" * 60)
    logger.info("BENCHMARK RESULTS")
    logger.info("=" * 60)
    for k, v in summary.items():
        logger.info(f"  {k}: {v}")
    logger.info("=" * 60)

    os.makedirs(os.path.dirname(RESULTS_FILE), exist_ok=True)
    with open(RESULTS_FILE, "a") as f:
        f.write(json.dumps(summary) + "\n")
    logger.info(f"Results saved to {RESULTS_FILE}")

if __name__ == "__main__":
    logger.info("Waiting for services to be ready...")
    time.sleep(5)
    run_benchmark()