from kafka import KafkaConsumer
import json

# Konfiguracja konsumenta
consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='enrichment_worker_v1',  # Nowy group_id pozwala na niezależne czytanie
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    auto_offset_reset='earliest'     # Opcjonalne: czytaj od początku, jeśli grupa jest nowa
)

print("Rozpoczęto wzbogacanie transakcji...")

for message in consumer:
    transaction = message.value
    amount = transaction.get('amount', 0)
    
    # Logika klasyfikacji ryzyka
    if amount > 3000:
        risk_level = "HIGH"
    elif amount > 1000:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"
    
    # Wzbogacenie słownika o nowe pole
    transaction['risk_level'] = risk_level
    
    # Wypisanie wzbogaconego eventu
    print(f"Przetworzono: {transaction}")
