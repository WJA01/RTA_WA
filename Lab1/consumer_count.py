from kafka import KafkaConsumer
from collections import Counter
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='analytics_consumer_v1'
)

store_counts = Counter()
total_amount = {}
msg_count = 0

print("Analiza strumienia rozpoczęta...")

for message in consumer:
    transaction = message.value
    store = transaction.get('store', 'Nieznany')
    amount = transaction.get('amount', 0)
    
    # 1. Zwiększ liczbę transakcji dla danego sklepu
    store_counts[store] += 1
    
    # 2. Dodaj kwotę do sumy dla danego sklepu
    total_amount[store] = total_amount.get(store, 0) + amount
    
    # Zwiększ całkowity licznik wiadomości
    msg_count += 1
    
    # 3. Co 10 wiadomości wypisz tabelę
    if msg_count % 10 == 0:
        print(f"\n--- Raport po {msg_count} wiadomościach ---")
        print(f"{'Sklep':<15} | {'Liczba':<8} | {'Suma':<10} | {'Średnia':<10}")
        print("-" * 55)
        
        for s in sorted(store_counts.keys()):
            cnt = store_counts[s]
            total = total_amount[s]
            avg = total / cnt if cnt > 0 else 0
            print(f"{s:<15} | {cnt:<8} | {total:<10.2f} | {avg:<10.2f}")
