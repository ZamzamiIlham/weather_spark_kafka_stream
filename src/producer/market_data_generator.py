import json
import time
import random
import os
import uuid
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = 'btc-trades'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_trade_data():
    """Menghasilkan mock data pergerakan harga Bitcoin yang realistis."""
    current_price = 60000.0
    
    while True:
        # Pergerakan harga acak (Random Walk) untuk BTC
        price_change = random.uniform(-10.0, 10.0)
        current_price += price_change
        
        volume = random.uniform(0.001, 2.5) # Volume dalam BTC
        
        # 5% kemungkinan menghasilkan data kotor (Dirty Data) untuk simulasi Validasi ETL
        is_dirty_data = random.random() < 0.05
        
        if is_dirty_data:
            price = -1.0 # Harga tidak valid
            vol = 0.0 # Volume tidak valid
        else:
            price = round(current_price, 2)
            vol = round(volume, 4)

        trade = {
            "trade_id": str(uuid.uuid4()),
            "symbol": "BTC-USD",
            "price": price,
            "volume": vol,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }

        try:
            producer.send(TOPIC_NAME, trade)
            print(f"Mengirim: {trade}")
        except Exception as e:
            print(f"Gagal mengirim ke Kafka: {e}")
            
        time.sleep(random.uniform(0.1, 1.0))

def main():
    print(f"Menghubungkan ke Kafka broker di {KAFKA_BROKER}...")
    print(f"Mulai menyimulasikan data transaksi BTC ke topik '{TOPIC_NAME}'...")
    generate_trade_data()

if __name__ == "__main__":
    main()
