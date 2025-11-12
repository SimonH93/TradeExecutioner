from fastapi import FastAPI, Request
import re
import os
from dotenv import load_dotenv
import requests
import time
import hmac
import hashlib
import base64
import json


app = FastAPI()

# Lädt die .env Datei
load_dotenv()

# API Keys auslesen
BITGET_API_KEY = os.getenv("BITGET_API_KEY")
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET")
BITGET_PASSWORD = os.getenv("BITGET_PASSWORD")

USDT_BUDGET = float(os.getenv("BITGET_USDT_SIZE") or 10)
DEFAULT_LEVERAGE = float(os.getenv("BITGET_LEVERAGE", 1))
TEST_MODE = os.getenv("BITGET_TEST_MODE", "True").lower() == "true"


# Base URL für Bitget Futures
BASE_URL = "https://api.bitget.com"

if not all([BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSWORD]):
    raise ValueError("Bitget API credentials are missing in .env")


# Beispiel: Telegram Webhook Endpoint
@app.post("/telegram_webhook")
async def telegram_webhook(req: Request):
    data = await req.json()  # Telegram Update
    message_text = data.get("message", {}).get("text", "")
    
    if not message_text:
        return {"ok": True}  # leere Nachricht ignorieren
    
    # Parsing der Nachricht
    signal = parse_signal(message_text)
    
    if signal:
        print("Signal erkannt:", signal)
        place_bitget_trade(signal, test_mode=TEST_MODE)
    else:
        print("Keine valide Nachricht")
    
    return {"ok": True}

def get_current_price(symbol: str, product_type: str = "USDT-FUTURES") -> float:
    url = f"{BASE_URL}/api/v2/mix/market/ticker"
    params = {"symbol": symbol, "productType": product_type}
    resp = requests.get(url, params=params, timeout=5)

    if resp.status_code != 200:
        raise Exception(f"Bitget API returned {resp.status_code}: {resp.text}")

    data = resp.json()
    if data.get("code") != "00000":
        raise Exception(f"API error: {data}")

    try:
        ticker_info = data["data"][0]
        price = float(ticker_info["lastPr"])
        print(f"[DEBUG] get_current_price: {symbol} = {price}")
        return price
    except (KeyError, TypeError, ValueError, IndexError):
        raise Exception(f"Unexpected response: {data}")


def get_position_size(symbol: str, usdt_budget: float = None, leverage: float = None, product_type: str = "USDT-FUTURES"):
    usdt_budget = usdt_budget or USDT_BUDGET
    leverage = leverage or DEFAULT_LEVERAGE

    # Aktueller Preis
    price = get_current_price(symbol, product_type)
    total_size = (usdt_budget * leverage) / price
    total_size = round(total_size, 8)

    # Teilpositionen für Take-Profit
    tp_sizes = [
        round(total_size * 0.5, 8),  # TP1 = 50%
        round(total_size * 0.3, 8),  # TP2 = 30%
    ]
    tp_sizes.append(round(total_size - sum(tp_sizes), 8))  # Rest in TP3

    print(f"[DEBUG] get_position_size: symbol={symbol}, price={price}, usdt_budget={usdt_budget}, "
          f"leverage={leverage}, total_size={total_size}, tp_sizes={tp_sizes}")
    
    return total_size, tp_sizes



def validate_trade(position_type, entry_price, stop_loss, take_profits, current_price):
    if not take_profits:  # TP-Liste leer → Trade verwerfen
        return False
    if position_type.upper() == "LONG":
        if current_price < stop_loss or current_price > max(take_profits):
            return False
    elif position_type.upper() == "SHORT":
        if current_price > stop_loss or current_price < min(take_profits):
            return False
    return True


# Parser für dein Signalformat
def parse_signal(text: str):
    
    if "POSITION SIZE" not in text.upper():  # case-insensitive check
        print("no valid signal")
        return None
    
    try:
        clean_text = re.sub(r"\s+", " ", text.replace("\u00A0", " "))
        # PAIR
        pair_match = re.search(r"PAIR:\s*(\w+/\w+)", clean_text)
        pair = pair_match.group(1) if pair_match else None
        symbol = pair.replace("/", "").upper() if pair else None

        # TYPE
        type_match = re.search(r"TYPE:\s*(LONG|SHORT)", clean_text)
        position_type = type_match.group(1) if type_match else None

        # ENTRY
        entry_match = re.search(r"ENTRY:\s*([\d.]+)", clean_text)
        entry_price = float(entry_match.group(1)) if entry_match else None

        # SL
        sl_match = re.search(r"SL:\s*([\d.]+)", clean_text)
        stop_loss = float(sl_match.group(1)) if sl_match else None

        # TAKE PROFIT TARGETS
        tp_matches = re.findall(r"TP\d+:\s*([\d.]+)", clean_text)
        take_profits = [float(tp) for tp in tp_matches if tp]
        # Wenn TP-Liste leer ist → Trade ungültig
        if not take_profits:
            print("No valid TP found")
            return None

        # LEVERAGE
        lev_match = re.search(r"LEVERAGE:\s*x(\d+)", clean_text)
        leverage = int(lev_match.group(1)) if lev_match else None

        print("DEBUG:", symbol, position_type, entry_price, stop_loss, take_profits, leverage)


        if not all([symbol, position_type, entry_price, stop_loss, leverage]):
            return None  # Pflichtfelder fehlen
        
        current_price = get_current_price(symbol)
        # Position berechnen + TP-Aufteilung
        position_size, tp_sizes = get_position_size(symbol, leverage=leverage)
        print(f"[DEBUG] Total position size: {position_size}, TP split sizes: {tp_sizes}")


        if not validate_trade(position_type, entry_price, stop_loss, take_profits, current_price):
            print(f"[DEBUG] Current={current_price}, SL={stop_loss}, TP_min={min(take_profits)}, TP_max={max(take_profits)}")
            return None        
        
        return {
            "symbol": symbol,
            "position_size": position_size,
            "tp_sizes": tp_sizes,
            "type": position_type,
            "entry": entry_price,
            "sl": stop_loss,
            "tps": take_profits,
            "leverage": leverage
        }
    except Exception as e:
        print("Parsing Error:", e)
    
    return None

def sign_request(method, request_path, timestamp, body=""):
    message = f"{timestamp}{method.upper()}{request_path}{body}"
    h = hmac.new(BITGET_API_SECRET.encode(), message.encode(), hashlib.sha256)
    return base64.b64encode(h.digest()).decode()

def place_market_order(symbol, size, side="buy", leverage=10):
    url_path = "/api/mix/v1/order/placeOrder"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    payload = {
        "symbol": symbol,
        "size": size,
        "side": side,             # "buy" oder "sell"
        "orderType": "market",
        "tradeMode": "isolated",  # isolierter Margin-Modus
        "leverage": leverage,
        "marginCoin": "USDT"
    }
    body = json.dumps(payload)
    signature = sign_request("POST", url_path, timestamp, body)

    headers = {
        "ACCESS-KEY": BITGET_API_KEY,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": BITGET_PASSWORD,
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(url, headers=headers, data=body, timeout=5)
        data = resp.json()  # Bitget gibt immer JSON zurück
        if data.get("code") != "00000":
            print("[ERROR] Bitget API response:", data)
            return None
        return data
    except requests.RequestException as e:
        print("[ERROR] Request failed:", e)
        return None



def place_trigger_order(symbol, size, trigger_price, side, order_type="limit"):
    url_path = "/api/mix/v1/order/placeOrder"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    payload = {
        "symbol": symbol,
        "size": size,
        "side": side,          # z.B. "sell" für TP/SL
        "orderType": order_type,
        "tradeSide": "close",  # schließt die Position
        "triggerPrice": trigger_price,
        "orderPrice": trigger_price if order_type=="limit" else None,
        "marginCoin": "USDT"
    }
    if payload["orderPrice"] is None:
        payload.pop("orderPrice")

    body = json.dumps(payload)
    signature = sign_request("POST", url_path, timestamp, body)

    headers = {
        "ACCESS-KEY": BITGET_API_KEY,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": BITGET_PASSWORD,
        "Content-Type": "application/json"
    }

    try:
        resp = requests.post(url, headers=headers, data=body, timeout=5)
        data = resp.json()  # Bitget gibt immer JSON zurück
        if data.get("code") != "00000":
            print("[ERROR] Bitget Trigger Order API response:", data)
            return None
        return data
    except requests.RequestException as e:
        print("[ERROR] Request failed:", e)
        return None
    
    
def place_bitget_trade(signal, test_mode=True):
    symbol = signal["symbol"]
    position_size = signal["position_size"]
    tp_sizes = signal["tp_sizes"]
    side = "buy" if signal["type"].upper() == "LONG" else "sell"
    leverage = signal["leverage"]
    sl_price = signal["sl"]
    tp_prices = signal["tps"]

    print(f"[INFO] Platzieren Market Order: {side} {position_size} {symbol} (Leverage={leverage})")

    if test_mode:
        print("[TEST MODE] Market Order, SL und TP Orders werden nicht gesendet")
        return

    market_side = "buy" if signal["type"].upper() == "LONG" else "sell"
    trigger_side = "sell" if market_side == "buy" else "buy"

    # --- 1. Market Order platzieren ---
    market_order_resp = place_market_order(symbol, position_size, side=market_side, leverage=leverage)
    print("Market Order Response:", market_order_resp)

    # Optional: warten bis Order ausgeführt
    time.sleep(0.5)

    # --- 2. Stop-Loss Order ---
    sl_resp = place_trigger_order(symbol, position_size, sl_price, side=trigger_side, order_type="market")
    print("Stop-Loss Order Response:", sl_resp)

    # --- 3. Take-Profit Orders ---
    for tp_price, tp_size in zip(tp_prices, tp_sizes):
        tp_resp = place_trigger_order(symbol, tp_size, tp_price, side=trigger_side, order_type="limit")
        print(f"TP Order {tp_price} Response:", tp_resp)
