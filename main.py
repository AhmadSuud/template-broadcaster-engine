import argparse

from src.service.consumer_email import ConsumerEmail

parser = argparse.ArgumentParser()

parser.add_argument("--mode", required=True, help="mode email / wa", default='email')

args = parser.parse_args()

if args.mode == 'email':
    consumer_email = ConsumerEmail()
    consumer_email.consume()
elif args.mode == 'wa':
    pass
