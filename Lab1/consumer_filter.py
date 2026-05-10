from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Rozpoczęto nasłuchiwanie transakcji...")

for message in consumer:
    transaction = message.value
    
    # Sprawdzenie warunku kwoty
    if transaction.get('amount', 0) > 3000:
        print(f"ALERT: Wykryto transakcję powyżej 1000! Szczegóły: {transaction}")
    else:
        # Opcjonalnie: logowanie pominiętych transakcji
        print(f"Przetworzono transakcję: {transaction['amount']}")
