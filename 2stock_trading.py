import time
import threading
import requests
import csv
from collections import deque
from datetime import datetime

# ================= CONFIG =================
API_KEY = "782e5e5babb44a8c8ccca62caed741e7"
ACCESS_TOKEN = "eyJhbGciOiJIUzM4NCJ9.eyJzdWIiOiJBQkxPREg4MiIsImV4cCI6MTc2NzQ0Njk3OCwibWVyY2hhbnQiOiJIREZDIEFsZ28gVHJhZGUiLCJtZXJjaGFudF9hcGlfa2V5IjoiNzgyZTVlNWJhYmI0NGE4YzhjY2NhNjJjYWVkNzQxZTciLCJpYXQiOjE3Njc0MTgxNzh9.SeZ86n7lNLPyO8OmkpUbAao9mfX_SBO5G29RikzAYCa0Mtg6GOBiuyQyJLt_t8li"

URL = f"https://developer.hdfcsec.com/oapi/v1/fetch-ltp?api_key={API_KEY}"
ORDER_URL = f"https://developer.hdfcsec.com/oapi/v1/orders/regular/?api_key={API_KEY}"

HEADERS = {
    "Authorization": ACCESS_TOKEN,
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json"
}

ORDER_HEADERS = HEADERS.copy()

TOKENS = [
    {"exchange": "NFO", "token": "52997"},
    {"exchange": "NFO", "token": "52998"},
]

ORDER_PAYLOADS = {
    "52997": {
        "exchange": "NSE",
        "security_id": "52997",
        "underlying_symbol": "BANKNIFTY",
        "instrument_segment": "OPTIDX",
        "transaction_type": "Buy",
        "product": "OVERNIGHT",
        "order_type": "MARKET",
        "price": 0,
        "trigger_price": 0,
        "quantity": 1,
        "disclosed_quantity": 0,
        "option_type": "CE",
        "strike_price": 22400,
        "expiry_date": "20240425",
        "validity": "DAY",
        "amo": False
    },
    "52998": {
        "exchange": "NSE",
        "security_id": "68181",
        "underlying_symbol": "NIFTYEQEQNR",
        "instrument_segment": "OPTIDX",
        "transaction_type": "Buy",
        "product": "OVERNIGHT",
        "order_type": "MARKET",
        "price": 0,
        "trigger_price": 0,
        "quantity": 50,
        "disclosed_quantity": 0,
        "option_type": "CE",
        "strike_price": 22500,
        "expiry_date": "20240425",
        "validity": "DAY",
        "amo": False
    }
}

PAYLOAD = {"data": TOKENS}

POLL_INTERVAL = 1
MAX_WINDOW = 180
SPIKE_RECORDS = 45
SPIKE_THRESHOLD = 0.003
SPIKE_COOLDOWN = 20
TRADE_COOLDOWN_SECONDS = 45
TRADING_ENABLED = False   # 🔴 KEEP FALSE UNTIL TESTED

CSV_FILE = "ltp_data.csv"
SPIKE_CSV_FILE = "spikes.csv"

# ================= STORAGE =================
price_buffer = {}
spike_buffer = {}
LAST_SPIKE_TS = {}

for t in TOKENS:
    token = t["token"]
    price_buffer[token] = deque(maxlen=MAX_WINDOW)
    spike_buffer[token] = deque(maxlen=SPIKE_RECORDS)
    LAST_SPIKE_TS[token] = 0

lock = threading.Lock()
stop_event = threading.Event()

active_trade = {
    "in_position": False,
    "token": None,
    "entry_time": None
}

# ================= CSV SETUP =================
with open(CSV_FILE, "w", newline="") as f:
    csv.writer(f).writerow(["timestamp", "token", "ltp"])

with open(SPIKE_CSV_FILE, "w", newline="") as f:
    csv.writer(f).writerow([
        "timestamp", "token", "direction",
        "old_price", "new_price", "change_percent"
    ])

# ================= API =================
def fetch_ltp():
    r = requests.put(URL, headers=HEADERS, json=PAYLOAD, timeout=5)
    r.raise_for_status()
    data = r.json()

    return {str(i["token"]): float(i["ltp"]) for i in data["data"]}

def place_order(token):
    payload = ORDER_PAYLOADS[token].copy()
    payload["external_reference_number"] = int(time.time())

    try:
        r = requests.post(ORDER_URL, headers=ORDER_HEADERS, json=payload, timeout=5)
        r.raise_for_status()
        print(f"✅ ORDER PLACED for {token}")
        return True
    except Exception as e:
        print("❌ ORDER FAILED:", e)
        return False

def square_off():
    global active_trade
    print(f"⏹️ SQUARE OFF for {active_trade['token']}")
    active_trade = {"in_position": False, "token": None, "entry_time": None}

def detect_spike(token, price):
    with lock:
        spike_buffer[token].append(price)

        if len(spike_buffer[token]) < SPIKE_RECORDS:
            return None

        idx = len(price_buffer[token])

        if idx - LAST_SPIKE_TS[token] < SPIKE_COOLDOWN:
            return None

        low = min(spike_buffer[token])
        high = max(spike_buffer[token])

        up_change = (price - low) / low

        if up_change >= SPIKE_THRESHOLD:
            LAST_SPIKE_TS[token] = idx
            return ("UP", low, price, up_change)

    return None

# ================= WORKER =================
def poller():
    while not stop_event.is_set():
        try:
            prices = fetch_ltp()
            ts = datetime.now()

            for token, price in prices.items():
                with lock:
                    price_buffer[token].append((ts, price))

                with open(CSV_FILE, "a", newline="") as f:
                    csv.writer(f).writerow([ts.isoformat(), token, price])

                spike = detect_spike(token, price)

                if spike and not active_trade["in_position"] and TRADING_ENABLED:
                    print(f"🚨 SPIKE UP → TRADE {token}")
                    if place_order(token):
                        active_trade.update({
                            "in_position": True,
                            "token": token,
                            "entry_time": time.time()
                        })
                        threading.Timer(TRADE_COOLDOWN_SECONDS, square_off).start()

                print(f"{ts.strftime('%H:%M:%S')} | {token} | {price}")

            time.sleep(POLL_INTERVAL)

        except Exception as e:
            print("Error:", e)
            time.sleep(2)

# ================= START =================
print("Starting LTP polling...")
thread = threading.Thread(target=poller, daemon=True)
thread.start()

try:
    while thread.is_alive():
        time.sleep(1)
except KeyboardInterrupt:
    stop_event.set()
    thread.join()