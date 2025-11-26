from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    # SMTP
    SMTP_HOST: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_PASS: str

    # KAFKA
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_GROUP_ID: str
    KAFKA_EMAIL_TOPIC: str
    KAFKA_WA_TOPIC: str

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
