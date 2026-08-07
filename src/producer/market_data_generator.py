import json
import time
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

import os

# Konfigurasi Kafka
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
TOPIC_NAME = 'spy-trades'

def generate_trade_data(base_price=500.0):
    """
    Menghasilkan data transaksi (tick) acak namun realistis.
    Terkadang akan sengaja menghasilkan data yang tidak valid (harga negatif atau volume 0) 
    untuk mensimulasikan data kotor agar bisa dibersihkan di Spark.
    """
    # 5% kemungkinan menghasilkan data kotor untuk simulasi Data Validation
    is_dirty_data = random.random() < 0.05
    
    if is_dirty_data:
        price = -1.0 # Harga tidak valid
        volume = 0   # Volume tidak valid
    else:
        # Volatilitas harga sekitar +/- 0.5 dolar dari base_price
        price_change = random.uniform(-0.5, 0.5)
        price = round(base_price + price_change, 2)
        volume = random.randint(1, 100) * 10 # Volume dari 10 sampai 1000

    return {
        "trade_id": str(uuid.uuid4()),
        "symbol": "SPY",
        "price": price,
        "volume": volume,
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }

def main():
    print(f"Menghubungkan ke Kafka broker di {KAFKA_BROKER}...")
    producer = None
    for i in range(10):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            break
        except Exception as e:
            print(f"Gagal terhubung, mencoba lagi dalam 3 detik... ({e})")
            time.sleep(3)
            
    if not producer:
        print("Gagal terhubung ke Kafka setelah 10 percobaan.")
        return

    print("Berhasil terhubung ke Kafka!")
    print(f"Mulai mengirim data ke topik '{TOPIC_NAME}'...")

    base_price = 505.25

    try:
        while True:
            trade = generate_trade_data(base_price)
            
            # Simulasi pergerakan harga dasar yang perlahan berubah
            if trade['price'] > 0:
                base_price = trade['price']

            # Kirim pesan ke Kafka
            producer.send(TOPIC_NAME, trade)
            
            print(f"Mengirim: {trade}")
            
            # Tunggu waktu acak antara 0.1 sampai 1 detik sebelum kirim trade selanjutnya
            time.sleep(random.uniform(0.1, 1.0))
            
    except KeyboardInterrupt:
        print("Menghentikan producer...")
    finally:
        producer.flush()
        producer.close()
        print("Producer ditutup.")

if __name__ == "__main__":
    main()
