import uuid
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

class NotificationType(str, Enum):
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"

class NotificationRequestDTO(BaseModel):
    messageId: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4)
    messageContent: str
    notificationType: NotificationType

class NotificationResponseDTO(BaseModel):
    traceId: uuid.UUID
    messageId: uuid.UUID
    status: str

class NotificationStatusDTO(BaseModel):
    messageId: uuid.UUID
    messageContent: str
    notificationType: NotificationType
    status: str
