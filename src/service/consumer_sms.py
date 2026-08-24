import json
import http.client
from confluent_kafka import Consumer, Producer, KafkaError
from src.core.config import settings

class ConsumerSMS:
    def __init__(self):
        self.kafka_broker = settings.KAFKA_BOOTSTRAP_SERVERS
        self.kafka_group_id = settings.KAFKA_GROUP_ID
        self.kafka_topic = settings.KAFKA_SMS_TOPIC
        
        self.kafka_status_topic = getattr(settings, 'KAFKA_STATUS_SMS_TOPIC', 'notification.status.sms')
        self._auto_offset_reset = 'earliest'
        
        self.consumer_conf = self.create_consumer_conf()
        self.producer = Producer({'bootstrap.servers': self.kafka_broker})
        
        self.infobip_base_url = settings.INFOBIP_BASE_URL
        self.infobip_api_key = settings.INFOBIP_API_KEY
        self.infobip_sender = settings.INFOBIP_SENDER

    def create_consumer_conf(self):
        return {
            'bootstrap.servers': self.kafka_broker,
            'group.id': self.kafka_group_id,
            'auto.offset.reset': self._auto_offset_reset
        }

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
            print(f"Gagal mengirim status SMS ke Kafka: {e}")

    def send_infobip_sms(self, receiver, text_content):
        clean_receiver = receiver.replace('+', '')
        
        conn = http.client.HTTPSConnection(self.infobip_base_url)
        payload = json.dumps({
            "messages": [
                {
                    "destinations": [{"to": clean_receiver}],
                    "sender": self.infobip_sender,
                    "content": {"text": text_content}
                }
            ]
        })
        
        headers = {
            'Authorization': f'App {self.infobip_api_key}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        conn.request("POST", "/sms/3/messages", payload, headers)
        res = conn.getresponse()
        data = res.read()
        response_str = data.decode("utf-8")
        
        if res.status not in (200, 201, 202):
            raise Exception(f"Infobip API Error {res.status}: {response_str}")
            
        return response_str

    def consume(self):
        consumer = Consumer(self.consumer_conf)
        consumer.subscribe([self.kafka_topic])
        print(f"Broadcaster SMS (Infobip) Listening on topic: {self.kafka_topic}...")
        
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
                
                event_id = None 
                try:
                    value = msg.value()
                    data = json.loads(value.decode("utf-8"))
                    event_id = data.get('event_id')
                    
                    receiver = data.get('receiver')
                    if isinstance(receiver, list):
                        receiver = receiver[0]
                        
                    body_text = data.get('body')
                    print(f"Menerima SMS untuk: {receiver}")
                    
                    response = self.send_infobip_sms(receiver, body_text)
                    print(f"SMS berhasil dikirim ke {receiver} via Infobip.")
                    self.send_status(event_id, "SUCCESS")
                    
                except Exception as e:
                    print(f"  Gagal memproses/mengirim SMS: {e}")
                    if event_id:
                        self.send_status(event_id, "FAILED", str(e))
                        
        except KeyboardInterrupt:
            print("\n  Stopped by user.")
        finally:
            consumer.close()
            self.producer.flush() 
            print("  Consumer closed.")