from src.persistence.database.db import notification_status

class NotificationRepository:
    @staticmethod
    def save_status(trace_id: str, message_id: str, message_content: str, notification_type: str, status: str):
        notification_status[trace_id] = {
            "messageId": message_id,
            "messageContent": message_content,
            "notificationType": notification_type,
            "status": status
        }

    @staticmethod
    def get_status(trace_id: str):
        return notification_status.get(trace_id)
