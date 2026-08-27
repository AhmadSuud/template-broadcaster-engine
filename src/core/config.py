from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import StringDeserializer, StringSerializer

BROADCAST_SCHEMA_STR = """{"type": "record", "name": "BroadcastMessage", "namespace": "bni.notification", "fields": [{"name": "event_id", "type": "string"}, {"name": "channel", "type": "string"}, {"name": "template_id", "type": ["null", "string"], "default": null}, {"name": "sender", "type": "string"}, {"name": "receiver", "type": "string"}, {"name": "subject", "type": ["null", "string"], "default": null}, {"name": "body", "type": "string"}]}"""

STATUS_SCHEMA_STR = """{"type": "record", "name": "StatusMessage", "namespace": "bni.notification.status", "fields": [{"name": "event_id", "type": "string"}, {"name": "status", "type": "string"}, {"name": "error_message", "type": ["null", "string"], "default": null}]}"""

class Settings(BaseSettings):
    KAFKA_BOOTSTRAP_SERVERS: str
    KAFKA_GROUP_ID: str
    KAFKA_EMAIL_TOPIC: str
    KAFKA_WA_TOPIC: str
    KAFKA_SMS_TOPIC: str  
    KAFKA_STATUS_EMAIL_TOPIC: str = 'notification.status.email'
    KAFKA_STATUS_WA_TOPIC: str = 'notification.status.wa'
    KAFKA_STATUS_SMS_TOPIC: str = 'notification.status.sms'
    
    KAFKA_SECURITY_PROTOCOL: Optional[str] = None
    KAFKA_SSL_CA_LOCATION: Optional[str] = None
    SCHEMA_REGISTRY_URL: str
    
    DB_HOST: str
    DB_PORT: int
    DB_NAME: str
    DB_USER: str
    DB_PASSWORD: str

    def get_sr_client(self):
        sr_conf = {'url': self.SCHEMA_REGISTRY_URL}
        if self.KAFKA_SECURITY_PROTOCOL == 'SSL':
            sr_conf['ssl.ca.location'] = self.KAFKA_SSL_CA_LOCATION.replace('\\', '/')
        return SchemaRegistryClient(sr_conf)

    def get_producer_conf(self) -> dict:
        conf = {
            'bootstrap.servers': self.KAFKA_BOOTSTRAP_SERVERS,
            'compression.type': 'zstd',
            'linger.ms': 5,
            'batch.size': 32768,
            'key.serializer': StringSerializer('utf_8'),
            'value.serializer': AvroSerializer(self.get_sr_client(), STATUS_SCHEMA_STR, lambda obj, ctx: obj)
        }
        if self.KAFKA_SECURITY_PROTOCOL:
            conf['security.protocol'] = self.KAFKA_SECURITY_PROTOCOL
            conf['ssl.ca.location'] = self.KAFKA_SSL_CA_LOCATION.replace('\\', '/')
        return conf

    def get_consumer_conf(self) -> dict:
        conf = {
            'bootstrap.servers': self.KAFKA_BOOTSTRAP_SERVERS,
            'group.id': self.KAFKA_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'key.deserializer': StringDeserializer('utf_8'),
            'max.poll.interval.ms': 500000,
            'value.deserializer': AvroDeserializer(self.get_sr_client(), BROADCAST_SCHEMA_STR, lambda obj, ctx: obj)
        }
        if self.KAFKA_SECURITY_PROTOCOL:
            conf['security.protocol'] = self.KAFKA_SECURITY_PROTOCOL
            conf['ssl.ca.location'] = self.KAFKA_SSL_CA_LOCATION.replace('\\', '/')
        return conf

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()