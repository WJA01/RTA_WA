from kafka import KafkaConsumer
from collections import defaultdict
import json

# Inicjalizacja struktury danych: każda nowa kategoria dostaje zestaw startowy
category_stats = defaultdict(lambda: {
    'count': 0, 
    'total': 0.0, 
    'min': float('inf'), 
    'max': float('-inf')
})

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='stats_analyzer_v1',
    auto_offset_reset='earliest'
)

msg_count = 0

print("Rozpoczęto zaawansowaną analizę kategorii...")

for message in consumer:
    tx = message.value
    category = tx.get('category', 'N/A')
    amount = tx.get('amount', 0.0)

    # Aktualizacja statystyk dla kategorii
    stats = category_stats[category]
    stats['count'] += 1
    stats['total'] += amount
    stats['min'] = min(stats['min'], amount)
    stats['max'] = max(stats['max'], amount)

    msg_count += 1

    # Co 10 wiadomości generujemy czytelny raport
    if msg_count % 10 == 0:
        print(f"\n--- STATYSTYKI PER KATEGORIA (Po {msg_count} msg) ---")
        header = f"{'Kategoria':<15} | {'Liczba':<7} | {'Suma':<10} | {'Min':<8} | {'Max':<8}"
        print(header)
        print("-" * len(header))
        
        for cat, data in sorted(category_stats.items()):
            print(f"{cat:<15} | {data['count']:<7} | {data['total']:<10.2f} | {data['min']:<8.2f} | {data['max']:<8.2f}")
