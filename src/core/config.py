from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    SMTP_HOST: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_PASS: str

    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_GROUP_ID: str
    
    KAFKA_EMAIL_TOPIC: str
    KAFKA_WA_TOPIC: str
    KAFKA_SMS_TOPIC: str  

    KAFKA_STATUS_EMAIL_TOPIC: str = 'notification.status.email'
    KAFKA_STATUS_WA_TOPIC: str = 'notification.status.wa'
    KAFKA_STATUS_SMS_TOPIC: str = 'notification.status.sms'

    model_config = SettingsConfigDict(env_file=".env", extra="ignore") 

settings = Settings()