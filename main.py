import argparse

from src.service.consumer_email import ConsumerEmail
from src.service.consumer_sms import ConsumerSMS
from src.service.consumer_wa import ConsumerWA

parser = argparse.ArgumentParser()

# TAMBAHAN: Update help text agar mencakup sms
parser.add_argument("--mode", required=True, help="mode email / wa / sms", default='email')

args = parser.parse_args()

if args.mode == 'email':
    consumer_email = ConsumerEmail()
    consumer_email.consume()
    
elif args.mode == 'wa':
    consumer_wa = ConsumerWA()
    consumer_wa.consume()

# TAMBAHAN: Blok eksekusi untuk mode sms
elif args.mode == 'sms':
    consumer_sms = ConsumerSMS()
    consumer_sms.consume()
    
else:
    print("Mode tidak dikenali! Silakan gunakan --mode email, --mode wa, atau --mode sms")