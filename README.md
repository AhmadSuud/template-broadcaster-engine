# Broadcaster Notification Engine

A Python-based notification engine that consumes messages from Kafka topics and sends notifications via email and WhatsApp.

## Features

- **Email Notifications**: Consumes messages from Kafka and sends emails via SMTP
- **WhatsApp Notifications**: Planned support for WhatsApp messaging (coming soon)
- **Kafka Integration**: Uses Confluent Kafka for message consumption
- **Configurable**: Environment-based configuration using Pydantic settings

## Prerequisites

- Python 3.7+
- Kafka broker
- SMTP server access
- Virtual environment (recommended)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd broadcaster-notif-engine
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1  # Windows PowerShell
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the project root with the following variables:
```env
# SMTP Configuration
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USER=your-email@example.com
SMTP_PASS=your-password

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_GROUP_ID=broadcaster-consumer-group
KAFKA_EMAIL_TOPIC=email-notifications
KAFKA_WA_TOPIC=whatsapp-notifications
```

## Usage

### Email Mode

Run the consumer in email mode to process email notifications:

```bash
python main.py --mode email
```

### WhatsApp Mode (Coming Soon)

```bash
python main.py --mode wa
```

## Message Format

### Email Messages

Messages consumed from the Kafka email topic should be in the following JSON format:

```json
{
  "sender": "sender@example.com",
  "receiver": ["recipient@example.com"],
  "subject": "Notification Subject",
  "body": "Email body content"
}
```

## Project Structure

```
broadcaster-notif-engine/
├── main.py                 # Application entry point
├── src/
│   ├── core/
│   │   └── config.py      # Configuration settings
│   └── service/
│       ├── consumer_email.py  # Email consumer service
│       └── consumer_wa.py     # WhatsApp consumer service (WIP)
├── .env                   # Environment variables (not in repo)
├── .gitignore
└── README.md
```

## Configuration

The application uses Pydantic settings for configuration management. All settings are loaded from environment variables or a `.env` file:

- **SMTP_HOST**: SMTP server hostname
- **SMTP_PORT**: SMTP server port (typically 587 for TLS)
- **SMTP_USER**: SMTP authentication username
- **SMTP_PASS**: SMTP authentication password
- **KAFKA_BOOTSTRAP_SERVERS**: Kafka broker address(es)
- **KAFKA_GROUP_ID**: Consumer group ID for Kafka
- **KAFKA_EMAIL_TOPIC**: Kafka topic for email notifications
- **KAFKA_WA_TOPIC**: Kafka topic for WhatsApp notifications

## Dependencies

- `confluent-kafka`: Kafka client
- `pydantic-settings`: Configuration management
- `smtplib`: Email sending (built-in)

## Development

To contribute or modify the project:

1. Ensure you're in the virtual environment
2. Make your changes
3. Test thoroughly with your Kafka setup
4. Submit a pull request

## License

[Add your license here]

## Support

For issues or questions, please open an issue in the repository.
