import asyncio
import json
import logging
import random
import os
import aio_pika
from dotenv import load_dotenv
from src.messaging.rabbitmq.publisher_rabbitmq import publish_message
from src.persistence.database.db import notification_status

load_dotenv()

NAME = os.getenv("NAME")
RETRY_QUEUE = f"queue.notification.retry.{NAME}"
VALIDATION_QUEUE = f"queue.notification.validation.{NAME}"
DLQ_QUEUE = f"queue.notification.dlq.{NAME}"

async def entry_consumer(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        trace_id = data["traceId"]
        logging.info(f"[ENTRY] Processing: {trace_id}")

        if random.random() < 0.15:
            logging.warning(f"[ENTRY] Simulated failure for: {trace_id}")
            notification_status[trace_id]["status"] = "INITIAL_PROCESSING_FAILED"
            await publish_message(RETRY_QUEUE, data)
        else:
            await asyncio.sleep(random.uniform(1, 1.5))
            logging.info(f"[ENTRY] Successfully processed: {trace_id}")
            notification_status[trace_id]["status"] = "INTERMEDIATE_PROCESSED"
            await publish_message(VALIDATION_QUEUE, data)

async def retry_consumer(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        trace_id = data["traceId"]
        logging.info(f"[RETRY] Reprocessing: {trace_id}")
        
        await asyncio.sleep(3)

        if random.random() < 0.20:
            logging.error(f"[RETRY] Final failure: {trace_id}. Sending to DLQ.")
            notification_status[trace_id]["status"] = "FINAL_REPROCESSING_FAILED"
            await publish_message(DLQ_QUEUE, data)
        else:
            logging.info(f"[RETRY] Reprocessing succeeded: {trace_id}")
            notification_status[trace_id]["status"] = "REPROCESSED_SUCCESSFULLY"
            await publish_message(VALIDATION_QUEUE, data)

async def validation_consumer(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        trace_id = data["traceId"]
        notification_type = data["notificationType"]
        logging.info(f"[VALIDATION] Sending ({notification_type}) for: {trace_id}")

        await asyncio.sleep(random.uniform(0.5, 1))

        if random.random() < 0.05:
            logging.error(f"[VALIDATION] Final sending failure: {trace_id}. Sending to DLQ.")
            notification_status[trace_id]["status"] = "FINAL_SENDING_FAILED"
            await publish_message(DLQ_QUEUE, data)
        else:
            logging.info(f"[VALIDATION] Successfully sent for: {trace_id}")
            notification_status[trace_id]["status"] = "SENT_SUCCESSFULLY"

async def dlq_consumer(message: aio_pika.abc.AbstractIncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        trace_id = data.get("traceId", "N/A")
        logging.critical(f"[DLQ] Dead letter message received. traceId: {trace_id}. Content: {data}")
