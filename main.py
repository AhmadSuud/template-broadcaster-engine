import threading
import sys
import time

from src.service.consumer_email import ConsumerEmail
from src.service.consumer_sms import ConsumerSMS
from src.service.consumer_wa import ConsumerWA

def run_email():
    ConsumerEmail().consume()

def run_wa():
    ConsumerWA().consume()

def run_sms():
    ConsumerSMS().consume()

if __name__ == '__main__':
    print("=" * 70, flush=True)
    print("Memulai All-in-One Broadcaster Engine (Avro, DB Config, Zero-Latency)...", flush=True)
    print("=" * 70, flush=True)

    try:
        t_email = threading.Thread(target=run_email, daemon=True)
        t_wa = threading.Thread(target=run_wa, daemon=True)
        t_sms = threading.Thread(target=run_sms, daemon=True)

        t_email.start()
        t_wa.start()
        t_sms.start()

        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        print("\n[INFO] Mematikan semua Broadcaster. Harap tunggu...", flush=True)
        sys.exit(0)