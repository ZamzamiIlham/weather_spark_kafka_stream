import json, time, random, os
import websocket
from datetime import datetime
from kafka import KafkaProducer

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = 'btc-trades'
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_message(ws, message):
    data = json.loads(message)
    
    # 5% kemungkinan menghasilkan data kotor (Dirty Data) untuk simulasi Validasi ETL
    is_dirty_data = random.random() < 0.05
    
    if is_dirty_data:
        price = -1.0 # Harga tidak valid
        volume = 0.0 # Volume tidak valid
    else:
        price = float(data['p'])
        volume = float(data['q'])

    trade = {
        "trade_id": str(data['t']),
        "symbol": "BTC-USDT",
        "price": price,
        "volume": volume,
        "timestamp": datetime.utcfromtimestamp(data['T']/1000).isoformat() + "Z"
    }

    producer.send(TOPIC_NAME, trade)
    print(f"Mengirim: {trade}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Koneksi WebSocket Tertutup")

def on_open(ws):
    print(f"Menghubungkan ke Kafka broker di {KAFKA_BROKER}...")
    print(f"Mulai menyedot data REAL-TIME dari Binance untuk topik '{TOPIC_NAME}'...")

def main():
    ws = websocket.WebSocketApp(BINANCE_WS_URL,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.run_forever()

if __name__ == "__main__":
    main()
