import json
from unittest.mock import patch, AsyncMock
import pytest

from src.messaging.rabbitmq.publisher_rabbitmq import publish_message

@pytest.mark.asyncio
async def test_publish_message_successfully():
    queue_name = "test.queue"
    message_data = {"traceId": "uuid-123-test", "message": "hello world"}

    mock_connection = AsyncMock()
    mock_channel = AsyncMock()
    mock_exchange = AsyncMock()

    mock_connection.channel.return_value = mock_channel
    mock_channel.default_exchange = mock_exchange

    with patch(
        'src.messaging.rabbitmq.publisher_rabbitmq.aio_pika.connect_robust',
        new=AsyncMock(return_value=mock_connection)
    ):
        await publish_message(queue_name, message_data)

        from src.messaging.rabbitmq.publisher_rabbitmq import aio_pika
        aio_pika.connect_robust.assert_awaited_once()

        mock_connection.channel.assert_awaited_once()
        mock_channel.declare_queue.assert_awaited_with(queue_name, durable=True)
        mock_exchange.publish.assert_awaited_once()

        args, kwargs = mock_exchange.publish.call_args
        sent_message_obj = args[0]
        routing_key_arg = kwargs['routing_key']

        assert json.loads(sent_message_obj.body.decode()) == message_data
        assert routing_key_arg == queue_name
