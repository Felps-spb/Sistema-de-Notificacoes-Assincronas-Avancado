import asyncio
import logging
import aio_pika
from fastapi import FastAPI
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv
from src.messaging.rabbitmq import consumer_rabbitmq
from src.http.rest.controller import notify_controller

load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL","amqp://bjnuffmq:gj-YQIiEXyfxQxjsZtiYDKeXIT8ppUq7@jaragua-01.lmq.cloudamqp.com/bjnuffmq")
NAME = os.getenv("NAME")

ENTRY_QUEUE = f"queue.notification.entry.{NAME}"
RETRY_QUEUE = f"queue.notification.retry.{NAME}"
VALIDATION_QUEUE = f"queue.notification.validation.{NAME}"
DLQ_QUEUE = f"queue.notification.dlq.{NAME}"

rabbitmq_connection = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global rabbitmq_connection
    loop = asyncio.get_running_loop()

    logging.info("Starting application and connecting to RabbitMQ...")
    try:
        rabbitmq_connection = await aio_pika.connect_robust(RABBITMQ_URL, loop=loop)
        channel = await rabbitmq_connection.channel()

        await channel.declare_queue(ENTRY_QUEUE, durable=True)
        await channel.declare_queue(RETRY_QUEUE, durable=True)
        await channel.declare_queue(VALIDATION_QUEUE, durable=True)
        await channel.declare_queue(DLQ_QUEUE, durable=True)

        entry_queue = await channel.get_queue(ENTRY_QUEUE)
        retry_queue = await channel.get_queue(RETRY_QUEUE)
        validation_queue = await channel.get_queue(VALIDATION_QUEUE)
        dlq_queue = await channel.get_queue(DLQ_QUEUE)

        await entry_queue.consume(consumer_rabbitmq.entry_consumer)
        await retry_queue.consume(consumer_rabbitmq.retry_consumer)
        await validation_queue.consume(consumer_rabbitmq.validation_consumer)
        await dlq_queue.consume(consumer_rabbitmq.dlq_consumer)

        logging.info("RabbitMQ consumers started successfully.")
        yield

    finally:
        logging.info("Shutting down application and closing RabbitMQ connection...")
        if rabbitmq_connection and not rabbitmq_connection.is_closed:
            await rabbitmq_connection.close()
            logging.info("RabbitMQ connection closed.")


app = FastAPI(lifespan=lifespan)
app.include_router(notify_controller.router)
