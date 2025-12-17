import requests
import json
import time
import random
from datetime import datetime
# import sseclient  # Not using this anymore
import sys

# Constants
API_URL = "http://localhost:8080/api"
SYMBOL = "TEST_UPLOAD"
FILENAME = "test_data.ndjson"

def generate_data():
    """Generate sample NDJSON data."""
    print(f"Generating sample data in {FILENAME}...")
    base_price = 50000.0
    start_ts = int(time.time() * 1000) - 3600000 # 1 hour ago
    
    with open(FILENAME, 'w') as f:
        for i in range(100):
            price = base_price + random.uniform(-100, 100)
            ts = start_ts + (i * 1000)
            record = {
                "symbol": "BTCUSDT",
                "ts": ts,
                "price": round(price, 2),
                "size": round(random.uniform(0.1, 2.0), 4)
            }
            f.write(json.dumps(record) + '\n')
            base_price = price

def upload_file():
    """Upload the file."""
    print("Uploading file...")
    with open(FILENAME, 'rb') as f:
        files = {'file': (FILENAME, f)}
        data = {'symbol_name': SYMBOL}
        try:
            res = requests.post(f"{API_URL}/upload", files=files, data=data)
            print(f"Status: {res.status_code}")
            if res.status_code == 200:
                print(f"Response: {res.json()}")
                return True
            else:
                print(f"Error: {res.text}")
                return False
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

def stream_results():
    """Stream SSE results."""
    print("Streaming analytics...")
    url = f"{API_URL}/upload/UPLOAD:{SYMBOL}/stream"
    
    try:
        with requests.get(url, stream=True) as response:
            if response.status_code != 200:
                print(f"Stream failed: {response.text}")
                return

            for line in response.iter_lines():
                if line:
                    decoded = line.decode('utf-8')
                    if decoded.startswith('data: '):
                        data = json.loads(decoded[6:])
                        print(f"Received event: {data.get('type')}")
                        if data.get('type') == 'stats':
                            print(f"Stats: {data.get('data')}")
                        if data.get('type') == 'complete':
                            print("Streaming complete!")
                            break
    except Exception as e:
        print(f"Streaming error: {e}")

if __name__ == "__main__":
    generate_data()
    if upload_file():
        stream_results()
    
    # Cleanup
    # import os
    # os.remove(FILENAME)
