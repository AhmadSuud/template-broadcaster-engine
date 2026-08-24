from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # SMTP
    SMTP_HOST: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_PASS: str

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_GROUP_ID: str
    
    KAFKA_EMAIL_TOPIC: str
    KAFKA_WA_TOPIC: str
    KAFKA_SMS_TOPIC: str  

    KAFKA_STATUS_EMAIL_TOPIC: str = 'notification.status.email'
    KAFKA_STATUS_WA_TOPIC: str = 'notification.status.wa'
    KAFKA_STATUS_SMS_TOPIC: str = 'notification.status.sms'

    # Infobip SMS
    INFOBIP_BASE_URL: str
    INFOBIP_API_KEY: str
    INFOBIP_SENDER: str
    
    # WHAPI WhatsApp
    WHAPI_BASE_URL: str
    WHAPI_TOKEN: str

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()