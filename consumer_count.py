from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    alert = message.value
    store = alert['store']
    amount = alert['amount']

    store_counts['store'] += 1
    total_amount['store'] += amount
    msg_count += 1

    if msg_count % 10 == 0:
        print('TABELA:')
        for s in store_counts:
            print(f"{s}: ilość={store_counts[s]}, suma={total_amount[s]:.2f} PLN")
