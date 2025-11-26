import gzip
import json
import smtplib
from email.message import EmailMessage

from confluent_kafka import Consumer, KafkaError

from src.core.config import settings


class ConsumerEmail:
    def __init__(self):
        self.kafka_broker = settings.KAFKA_BOOTSTRAP_SERVERS
        self.kafka_group_id = settings.KAFKA_GROUP_ID
        self.kafka_topic = settings.KAFKA_EMAIL_TOPIC
        self._auto_offset_reset = 'earliest'
        self.consumer_conf = self.create_consumer_conf()
        self.smtp_host = settings.SMTP_HOST
        self.smtp_port = settings.SMTP_PORT
        self.smtp_user = settings.SMTP_USER
        self.smtp_pass = settings.SMTP_PASS

    def create_consumer_conf(self):
        conf = {
            'bootstrap.servers': self.kafka_broker,
            'group.id': self.kafka_group_id,
            'auto.offset.reset': self._auto_offset_reset
        }
        return conf

    def send_email(self, sender, receiver, subject, body):
        msg = EmailMessage()
        msg["From"] = sender
        msg["To"] = ", ".join(receiver) if isinstance(receiver, (list, tuple)) else receiver
        msg["Subject"] = subject
        msg.set_content(body)  # plain text

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.ehlo()
            server.starttls()  # upgrade to secure TLS
            server.ehlo()
            server.login(self.smtp_user, self.smtp_pass)
            server.send_message(msg)
            print("Email sent to", msg["To"])

    def consume(self):
        consumer = Consumer(self.consumer_conf)
        consumer.subscribe([self.kafka_topic])
        try:
            while True:
                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"❌ Error: {msg.error()}")
                        continue
                try:
                    # decompress gzip
                    value = msg.value()
                    data = json.loads(value.decode("utf-8"))
                    self.send_email(data['sender'], data['receiver'], data['subject'], data['body'])
                except Exception as e:
                    print("⚠️ Failed to process message:", e)

        except KeyboardInterrupt:
            print("\n🛑 Stopped by user.")

        finally:
            consumer.close()
            print("✅ Consumer closed.")


