#!/usr/bin/env python3
"""
Results consumer for ticket acquisition system.
Consumes results from RabbitMQ and writes to JSONL file.
"""

import os
import json
import logging
import pika
from datetime import datetime

# --------------------------
# Configuration
# --------------------------
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.environ.get("RABBITMQ_PASS", "guest")
RESULT_QUEUE = "ticket_results"
RESULTS_FILE = os.environ.get("RESULTS_FILE", "/app/results/results.jsonl")

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# --------------------------
# RabbitMQ Connection
# --------------------------
credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credentials)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=RESULT_QUEUE, durable=True)

# --------------------------
# Results File Setup
# --------------------------
def ensure_results_dir():
    """Ensure the results directory exists."""
    results_dir = os.path.dirname(RESULTS_FILE)
    if results_dir:
        os.makedirs(results_dir, exist_ok=True)

ensure_results_dir()

# --------------------------
# Callback
# --------------------------
def callback(ch, method, properties, body):
    """Process result message and write to JSONL file."""
    try:
        msg = json.loads(body)

        # Add received timestamp
        msg["received_at"] = datetime.utcnow().isoformat()

        # Write to JSONL file (one JSON object per line)
        with open(RESULTS_FILE, 'a') as f:
            f.write(json.dumps(msg) + '\n')

        logger.info(f"Saved result: producer={msg.get('producer_id')}, "
                   f"request={msg.get('request_id')}, "
                   f"success={msg.get('success')}")

    except Exception as e:
        logger.error(f"Error processing result: {e}")
    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)

# --------------------------
# Main Entry Point
# --------------------------
if __name__ == "__main__":
    logger.info(f"Results consumer started. Writing results to {RESULTS_FILE}")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RESULT_QUEUE, on_message_callback=callback)

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("Results consumer stopped by user")
    finally:
        channel.stop_consuming()
        connection.close()
        logger.info("Results consumer shutdown complete.")
