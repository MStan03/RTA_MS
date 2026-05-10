from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime, timedelta
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

user_history = defaultdict(list)

print("Nasłuchuję przeciążeń (>3 transakcje / 60 sek)...")

for message in consumer:
    tx = message.value

    user_id = tx['user_id']
    timestamp = datetime.fromisoformat(tx['timestamp'])

    user_history[user_id].append(timestamp)

    cutoff = timestamp - timedelta(seconds=60)

    user_history[user_id] = [
        t for t in user_history[user_id]
        if t >= cutoff
    ]

    if len(user_history[user_id]) > 3:
        print(
            f"ALERT: {user_id} | {len(user_history[user_id])} transakcje w 60 sek."
        )
