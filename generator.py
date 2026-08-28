import json
import time
import random
import uuid
from datetime import datetime
from confluent_kafka import Producer
from faker import Faker

fake = Faker()

conf = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'telecom-generator'
}

producer = Producer(conf)
TOPIC_NAME = 'telecom.cdr.events'
EVENT_TYPES = ['VOICE', 'SMS', 'DATA']

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Ошибка отправки: {err}")
    else:
        print(f"✅ Доставлено в {msg.topic()} [{msg.partition()}]")

print(f"🚀 Запуск генератора CDR событий в топик {TOPIC_NAME}...")

try:
    while True:
        event_type = random.choice(EVENT_TYPES)
        duration = random.randint(5, 3600) if event_type == 'VOICE' else 0
        bytes_tx = random.randint(1024, 104857600) if event_type == 'DATA' else 0

        payload = {
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "msisdn": f"+99450{random.randint(1000000, 9999999)}",
            "imsi": f"40001{random.randint(1000000000, 9999999999)}",
            "event_type": event_type,
            "duration_sec": duration,
            "bytes_transferred": bytes_tx,
            "cell_id": f"CELL-{random.randint(100, 999)}",
            "ip_address": fake.ipv4()
        }

        producer.produce(
            TOPIC_NAME,
            value=json.dumps(payload).encode('utf-8'),
            callback=delivery_report
        )
        # Обязательный сброс буфера для доставки сетевого пакета!
        producer.poll(0)
        producer.flush()

        print(f"Sent: {payload['event_type']} | MSISDN: {payload['msisdn']} | Cell: {payload['cell_id']}")
        time.sleep(0.3)

except KeyboardInterrupt:
    print("\n🛑 Остановка...")
    producer.flush()
