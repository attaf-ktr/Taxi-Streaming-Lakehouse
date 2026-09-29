import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

def generate_taxi_trip():
    return {
        "trip_id": random.randint(10000, 99999),
        "vendor_id": random.choice([1, 2]),
        "pickup_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count": random.randint(1, 6),
        "trip_distance": round(random.uniform(0.5, 20.0), 2),
        "total_amount": round(random.uniform(5.0, 100.0), 2)
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("🚕 Starting Taxi Trip Simulator... Press Ctrl+C to stop.")
try:
    while True:
        trip = generate_taxi_trip()
        producer.send('taxi-trips-stream', value=trip)
        print(f"Sent: {trip}")
        time.sleep(2)  # Generates a new trip every 2 seconds
except KeyboardInterrupt:
    print("\n🛑 Simulator stopped.")
finally:
    producer.close()
