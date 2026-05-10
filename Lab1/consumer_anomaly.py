from kafka import KafkaConsumer
from collections import defaultdict
import json
import time

user_timestamps = defaultdict(list)

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly_detector_v1',
    auto_offset_reset='latest' 
)

print("Nasłuchiwanie...")

for message in consumer:
    tx = message.value
    user_id = tx.get('user_id')
    
    if not user_id:
        continue
        
    current_time = time.time()
    user_timestamps[user_id].append(current_time)
    
    time_threshold = current_time - 60.0
    user_timestamps[user_id] = [ts for ts in user_timestamps[user_id] if ts >= time_threshold]
    
    transaction_count = len(user_timestamps[user_id])
    
    if transaction_count > 3:
        print(f"[ALERT FRAUD] Użytkownik {user_id} wygenerował {transaction_count} transakcji w mniej niż 60 sekund!")
