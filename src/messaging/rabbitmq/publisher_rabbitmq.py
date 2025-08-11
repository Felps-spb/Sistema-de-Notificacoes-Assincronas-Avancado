import json
import logging
import aio_pika
import os
from dotenv import load_dotenv

load_dotenv()

RABBITMQ_URL = os.getenv(
    "RABBITMQ_URL",
    "amqp://bjnuffmq:gj-YQIiEXyfxQxjsZtiYDKeXIT8ppUq7@jaragua-01.lmq.cloudamqp.com/bjnuffmq"
)

async def get_rabbitmq_connection() -> aio_pika.abc.AbstractConnection:
    return await aio_pika.connect_robust(RABBITMQ_URL)

async def publish_message(queue_name: str, message_body: dict):
    try:
        connection = await get_rabbitmq_connection()
        async with connection:
            channel = await connection.channel()
            await channel.declare_queue(queue_name, durable=True)
            
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=json.dumps(message_body).encode(),
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key=queue_name,
            )
        logging.info(f"Message {message_body.get('traceId')} published to {queue_name}")
    except Exception as e:
        logging.error(f"Failed to publish message to {queue_name}: {e}")
        raise
