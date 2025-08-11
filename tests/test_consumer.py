import pytest
import json
import logging
from unittest.mock import AsyncMock, patch
import src.messaging.rabbitmq.consumer_rabbitmq as consumer_rabbitmq

class MockProcessCM:
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc, tb):
        return False

class MockMessage:
    def __init__(self, body):
        self.body = body.encode()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def process(self):
        return MockProcessCM()

@pytest.mark.asyncio
@patch("src.messaging.rabbitmq.consumer_rabbitmq.publish_message", new_callable=AsyncMock)
async def test_entry_consumer_success(mock_publish):
    data = {
        "traceId": "trace-entry-success",
        "messageId": "msg-001",
        "messageContent": "Hello entry",
        "notificationType": "EMAIL",
        "status": "RECEIVED"
    }
    message = MockMessage(json.dumps(data))
    consumer_rabbitmq.notification_status.clear()
    consumer_rabbitmq.notification_status[data["traceId"]] = {"status": "RECEIVED"}

    with patch("random.random", return_value=0.5):
        await consumer_rabbitmq.entry_consumer(message)

    assert consumer_rabbitmq.notification_status[data["traceId"]]["status"] == "INTERMEDIATE_PROCESSED"
    mock_publish.assert_called_once()

@pytest.mark.asyncio
@patch("src.messaging.rabbitmq.consumer_rabbitmq.publish_message", new_callable=AsyncMock)
async def test_entry_consumer_failure(mock_publish):
    data = {
        "traceId": "trace-entry-fail",
        "messageId": "msg-002",
        "messageContent": "Hello fail",
        "notificationType": "SMS",
        "status": "RECEIVED"
    }
    message = MockMessage(json.dumps(data))
    consumer_rabbitmq.notification_status.clear()
    consumer_rabbitmq.notification_status[data["traceId"]] = {"status": "RECEIVED"}

    with patch("random.random", return_value=0.1):
        await consumer_rabbitmq.entry_consumer(message)

    assert consumer_rabbitmq.notification_status[data["traceId"]]["status"] == "INITIAL_PROCESSING_FAILED"
    mock_publish.assert_called_once()

@pytest.mark.asyncio
@patch("src.messaging.rabbitmq.consumer_rabbitmq.publish_message", new_callable=AsyncMock)
async def test_retry_consumer_success(mock_publish):
    data = {
        "traceId": "trace-retry-success",
        "messageId": "msg-003",
        "messageContent": "Retry success",
        "notificationType": "PUSH",
        "status": "INITIAL_PROCESSING_FAILED"
    }
    message = MockMessage(json.dumps(data))
    consumer_rabbitmq.notification_status.clear()
    consumer_rabbitmq.notification_status[data["traceId"]] = {"status": "INITIAL_PROCESSING_FAILED"}

    with patch("random.random", return_value=0.5):
        await consumer_rabbitmq.retry_consumer(message)

    assert consumer_rabbitmq.notification_status[data["traceId"]]["status"] == "REPROCESSED_SUCCESSFULLY"
    mock_publish.assert_called_once()

@pytest.mark.asyncio
@patch("src.messaging.rabbitmq.consumer_rabbitmq.publish_message", new_callable=AsyncMock)
async def test_retry_consumer_failure(mock_publish):
    data = {
        "traceId": "trace-retry-fail",
        "messageId": "msg-004",
        "messageContent": "Retry fail",
        "notificationType": "EMAIL",
        "status": "INITIAL_PROCESSING_FAILED"
    }
    message = MockMessage(json.dumps(data))
    consumer_rabbitmq.notification_status.clear()
    consumer_rabbitmq.notification_status[data["traceId"]] = {"status": "INITIAL_PROCESSING_FAILED"}

    with patch("random.random", return_value=0.1):
        await consumer_rabbitmq.retry_consumer(message)

    assert consumer_rabbitmq.notification_status[data["traceId"]]["status"] == "FINAL_REPROCESSING_FAILED"
    mock_publish.assert_called_once()

@pytest.mark.asyncio
@patch("src.messaging.rabbitmq.consumer_rabbitmq.publish_message", new_callable=AsyncMock)
async def test_validation_consumer_success(mock_publish):
    data = {
        "traceId": "trace-validation-success",
        "messageId": "msg-005",
        "messageContent": "Validation success",
        "notificationType": "EMAIL",
        "status": "INTERMEDIATE_PROCESSED"
    }
    message = MockMessage(json.dumps(data))
    consumer_rabbitmq.notification_status.clear()
    consumer_rabbitmq.notification_status[data["traceId"]] = {"status": "INTERMEDIATE_PROCESSED"}

    with patch("random.random", return_value=0.5):
        await consumer_rabbitmq.validation_consumer(message)

    assert consumer_rabbitmq.notification_status[data["traceId"]]["status"] == "SENT_SUCCESSFULLY"
    mock_publish.assert_not_called()

@pytest.mark.asyncio
@patch("src.messaging.rabbitmq.consumer_rabbitmq.publish_message", new_callable=AsyncMock)
async def test_validation_consumer_failure(mock_publish):
    data = {
        "traceId": "trace-validation-fail",
        "messageId": "msg-006",
        "messageContent": "Validation fail",
        "notificationType": "SMS",
        "status": "INTERMEDIATE_PROCESSED"
    }
    message = MockMessage(json.dumps(data))
    consumer_rabbitmq.notification_status.clear()
    consumer_rabbitmq.notification_status[data["traceId"]] = {"status": "INTERMEDIATE_PROCESSED"}

    with patch("random.random", return_value=0.01):
        await consumer_rabbitmq.validation_consumer(message)

    assert consumer_rabbitmq.notification_status[data["traceId"]]["status"] == "FINAL_SENDING_FAILED"
    mock_publish.assert_called_once()

@pytest.mark.asyncio
async def test_dlq_consumer_logs(caplog):
    data = {
        "traceId": "trace-dlq",
        "messageId": "msg-007",
        "messageContent": "DLQ message",
        "status": "FINAL_REPROCESSING_FAILED"
    }
    message = MockMessage(json.dumps(data))

    with caplog.at_level(logging.CRITICAL):
        await consumer_rabbitmq.dlq_consumer(message)
        assert any("DLQ" in record.message for record in caplog.records)
