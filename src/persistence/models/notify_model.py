import uuid
from typing import Optional

from pydantic import BaseModel, Field

class NotificationType(str, Enum):
    EMAIL = "EMAIL"
    SMS = "SMS"
    PUSH = "PUSH"

class Notification(BaseModel):
    messageId: Optional[uuid.UUID] = Field(default_factory=uuid.uuid4)
    messageContent: str
    notificationType: NotificationType