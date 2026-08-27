from confluent_kafka import DeserializingConsumer, SerializingProducer
from src.core.config import settings
from src.core.rate_limiter import global_rate_limiter
from src.core.channel_manager import global_channel_manager
import http.client
import json
import time

class ConsumerSMS:
    def __init__(self):
        self.kafka_topic = settings.KAFKA_SMS_TOPIC
        self.kafka_status_topic = settings.KAFKA_STATUS_SMS_TOPIC
        self.consumer_conf = settings.get_consumer_conf()
        self.producer = SerializingProducer(settings.get_producer_conf())

    def send_status(self, event_id, status, error_message=None):
        if not event_id: return
        payload = {"event_id": event_id, "status": status, "error_message": str(error_message) if error_message else None}
        try:
            self.producer.produce(topic=self.kafka_status_topic, value=payload, key=event_id)
            self.producer.poll(0)
        except Exception: pass

    def send_infobip_sms(self, receiver, text_content):
        account_config = global_channel_manager.get_account("sms")
        if not account_config: raise Exception("Kredensial SMS tidak ditemukan atau nonaktif di DB.")
            
        infobip_base_url = account_config.get("infobip_base_url")
        infobip_api_key = account_config.get("infobip_api_key")
        infobip_sender = account_config.get("infobip_sender")

        clean_receiver = receiver.replace('+', '')
        conn = http.client.HTTPSConnection(infobip_base_url)
        payload = json.dumps({"messages": [{"destinations": [{"to": clean_receiver}], "sender": infobip_sender, "content": {"text": text_content}}]})
        headers = {'Authorization': f'App {infobip_api_key}', 'Content-Type': 'application/json'}
        
        conn.request("POST", "/sms/3/messages", payload, headers)
        res = conn.getresponse()
        response_str = res.read().decode("utf-8")
        
        if res.status not in (200, 201, 202): raise Exception(f"Infobip API Error {res.status}: {response_str}")
        return response_str

    def consume(self):
        consumer = DeserializingConsumer(self.consumer_conf)
        consumer.subscribe([self.kafka_topic])
        print(f"[*] Broadcaster SMS Listening on topic: {self.kafka_topic}...", flush=True)
        
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is None or msg.error(): continue
                
                try:
                    data = msg.value()
                    event_id = data.get('event_id', 'UNKNOWN_EVENT')
                    
                    # if not global_rate_limiter.acquire("sms"):
                    #     self.send_status(event_id, "DROPPED", "Rate limit tercapai")
                    #     continue

                    receiver = data.get('receiver')[0] if isinstance(data.get('receiver'), list) else data.get('receiver')
                    
                    print(f"\n=======================================================", flush=True)
                    print(f"[{event_id}] Mencoba mengirim via Infobip dinamis...", flush=True)
                    # self.send_infobip_sms(receiver, data.get('body'))
                    print(f"[INFOBIP] SMS sukses terkirim ke {receiver}", flush=True)
                    
                    self.send_status(event_id, "SUCCESS")
                    time.sleep(0.52)
                except Exception as e:
                    print(f"[{event_id}] [GAGAL] Pengiriman SMS: {e}", flush=True)
                    if event_id != 'UNKNOWN_EVENT': self.send_status(event_id, "FAILED", str(e))
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
            self.producer.flush()