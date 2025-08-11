from fastapi import APIRouter, status
from src.http.rest.dto.notification_dto import (
    NotificationRequestDTO,
    NotificationResponseDTO,
    NotificationStatusDTO
)
from src.core.service.notification_service import NotificationService

router = APIRouter(prefix="/api/notification")

@router.post("/",response_model=NotificationResponseDTO)
async def send_notification(payload: NotificationRequestDTO):
    return await NotificationService.send_notification(payload)

@router.get("/status/{traceId}",response_model=NotificationStatusDTO)
async def get_status(traceId: str):
    return NotificationService.get_status(traceId)
