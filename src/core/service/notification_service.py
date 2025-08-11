import uuid
import logging
from fastapi import HTTPException, status
from src.core.repository.notification_repository import NotificationRepository
from src.messaging.rabbitmq.publisher_rabbitmq import publish_message
from src.http.rest.dto.notification_dto import (
    NotificationRequestDTO,
    NotificationResponseDTO,
    NotificationStatusDTO
)
import os
from dotenv import load_dotenv

load_dotenv()

NAME = os.getenv("NAME")
ENTRY_QUEUE = f"queue.notification.entry.{NAME}"

class NotificationService:
    @staticmethod
    async def send_notification(payload: NotificationRequestDTO) -> NotificationResponseDTO:
        trace_id = uuid.uuid4()
        message_id = payload.messageId or uuid.uuid4()

        NotificationRepository.save_status(
            str(trace_id), str(message_id),
            payload.messageContent, payload.notificationType, "RECEIVED"
        )

        logging.info(f"Request received. traceId: {trace_id}")

        message_body = {
            "traceId": str(trace_id),
            "messageId": str(message_id),
            "messageContent": payload.messageContent,
            "notificationType": payload.notificationType,
            "status": "RECEIVED"
        }

        await publish_message(ENTRY_QUEUE, message_body)

        return NotificationResponseDTO(
            traceId=trace_id,
            messageId=message_id,
            status="Request received for processing."
        )

    @staticmethod
    def get_status(trace_id: str) -> NotificationStatusDTO:
        status_data = NotificationRepository.get_status(trace_id)
        if not status_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="traceId not found"
            )

        return NotificationStatusDTO(
            messageId=status_data["messageId"],
            messageContent=status_data["messageContent"],
            notificationType=status_data["notificationType"],
            status=status_data["status"]
        )
