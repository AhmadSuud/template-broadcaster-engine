from confluent_kafka import DeserializingConsumer, SerializingProducer
from src.core.config import settings
from src.core.rate_limiter import global_rate_limiter
from src.core.channel_manager import global_channel_manager
import smtplib
from email.message import EmailMessage
class ConsumerEmail:
    def __init__(self):
        self.kafka_topic = settings.KAFKA_EMAIL_TOPIC
        self.kafka_status_topic = settings.KAFKA_STATUS_EMAIL_TOPIC
        self.consumer_conf = settings.get_consumer_conf()
        self.producer = SerializingProducer(settings.get_producer_conf())

    def send_status(self, event_id, status, error_message=None):
        if not event_id: return
        payload = {"event_id": event_id, "status": status, "error_message": str(error_message) if error_message else None}
        try:
            self.producer.produce(topic=self.kafka_status_topic, value=payload, key=event_id)
            self.producer.poll(0)
        except Exception: pass

    def send_email(self, receiver, subject, body):
        account_config = global_channel_manager.get_account("email")
        if not account_config: raise Exception("Kredensial SMTP tidak ditemukan atau nonaktif di DB.")
            
        smtp_host = account_config.get("host")
        smtp_port = int(account_config.get("port", 587))
        smtp_user = account_config.get("username")
        smtp_pass = account_config.get("password")
        from_name = account_config.get("fromName", "BNI Notif")
        from_addr = account_config.get("fromAddress", smtp_user)
        
        msg = EmailMessage()
        msg["From"] = f"{from_name} <{from_addr}>"
        msg["To"] = ", ".join(receiver) if isinstance(receiver, (list, tuple)) else receiver
        msg["Subject"] = subject
        msg.set_content(body, subtype='html') 

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            if account_config.get("encryption", "").lower() in ["tls", "starttls"]:
                server.starttls()  
                server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.send_message(msg)

    def consume(self):
        consumer = DeserializingConsumer(self.consumer_conf)
        consumer.subscribe([self.kafka_topic])
        print(f"[*] Broadcaster Email Listening on topic: {self.kafka_topic}...", flush=True)
        
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None or msg.error(): continue
                
                try:
                    data = msg.value() 
                    event_id = data.get('event_id', 'UNKNOWN_EVENT')
                    

                    print(f"\n=======================================================", flush=True)
                    print(f"[{event_id}] Mencoba mengirim via SMTP dinamis...", flush=True)
                    self.send_email(data.get('receiver'), data.get('subject'), data.get('body'))
                    print(f"[SMTP] Email sukses terkirim ke: {data.get('receiver')}", flush=True)
                    
                    self.send_status(event_id, "SUCCESS")
                    global_rate_limiter.acquire("email")
                except Exception as e:
                    print(f"[{event_id}] [GAGAL] Pengiriman Email: {e}", flush=True)
                    if event_id != 'UNKNOWN_EVENT': self.send_status(event_id, "FAILED", str(e))
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
            self.producer.flush()