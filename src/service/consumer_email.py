import json
import smtplib
from email.message import EmailMessage
from confluent_kafka import Consumer, Producer, KafkaError
from src.core.config import settings

class ConsumerEmail:
    def __init__(self):
        self.kafka_broker = settings.KAFKA_BOOTSTRAP_SERVERS
        self.kafka_group_id = settings.KAFKA_GROUP_ID
        self.kafka_topic = settings.KAFKA_EMAIL_TOPIC
        
        # Topik khusus untuk mengirim laporan status
        self.kafka_status_topic = getattr(settings, 'KAFKA_STATUS_EMAIL_TOPIC', 'notification.status.email')
        
        self._auto_offset_reset = 'earliest'
        self.consumer_conf = self.create_consumer_conf()
        
        # TAMBAHAN: Inisialisasi Producer
        self.producer = Producer({'bootstrap.servers': self.kafka_broker})
        
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

    # TAMBAHAN: Fungsi untuk menembak status kembali ke Kafka
    def send_status(self, event_id, status, error_message=None):
        if not event_id:
            return
        payload = {
            "event_id": event_id,
            "status": status,
            "error_message": str(error_message) if error_message else None
        }
        try:
            self.producer.produce(
                topic=self.kafka_status_topic,
                value=json.dumps(payload).encode('utf-8'),
                key=event_id.encode('utf-8')
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"Gagal mengirim status ke Kafka: {e}")

    def send_email(self, sender, receiver, subject, body):
        msg = EmailMessage()
        msg["From"] = sender
        msg["To"] = ", ".join(receiver) if isinstance(receiver, (list, tuple)) else receiver
        msg["Subject"] = subject
        
        msg.set_content(body, subtype='html') 

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            server.ehlo()
            server.starttls()  
            server.ehlo()
            server.login(self.smtp_user, self.smtp_pass)
            server.send_message(msg)
            print("Email sent to", msg["To"])

    def consume(self):
        consumer = Consumer(self.consumer_conf)
        consumer.subscribe([self.kafka_topic])
        print(f"Broadcaster Email Listening on topic: {self.kafka_topic}...")
        
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"  Error: {msg.error()}")
                        continue
                
                # Inisialisasi event_id agar aman jika terjadi error saat parsing JSON
                event_id = None 
                try:
                    # Ambil data dari Kafka yang sudah di-render oleh ETL Engine
                    value = msg.value()
                    data = json.loads(value.decode("utf-8"))
                    event_id = data.get('event_id')
                    
                    print(f"Menerima email untuk: {data.get('receiver')} dengan subjek '{data.get('subject')}'")
                    
                    # 1. Eksekusi pengiriman email
                    self.send_email(data['sender'], data['receiver'], data['subject'], data['body'])
                    
                    # 2. Jika sukses, kirim laporan SUCCESS ke ETL Engine
                    self.send_status(event_id, "SUCCESS")
                    print(f"Status SUCCESS terkirim untuk event: {event_id}")
                    
                except Exception as e:
                    print(f"  Failed to process message/send email: {e}")
                    # 3. Jika gagal (misal email salah/SMTP putus), kirim laporan FAILED
                    if event_id:
                        self.send_status(event_id, "FAILED", str(e))
                        
        except KeyboardInterrupt:
            print("\n  Stopped by user.")
        finally:
            consumer.close()
            # Pastikan semua antrean pesan status terkirim sebelum aplikasi mati
            self.producer.flush() 
            print("  Consumer closed.")