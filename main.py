from fastapi import FastAPI, Request
import re
import os
from dotenv import load_dotenv
import requests
import hmac, hashlib, time


app = FastAPI()

# Lädt die .env Datei
load_dotenv()

# API Keys auslesen
BITGET_API_KEY = os.getenv("BITGET_API_KEY")
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET")
BITGET_PASSWORD = os.getenv("BITGET_PASSWORD")

# Base URL für Bitget Futures
BASE_URL = "https://api.bitget.com"

# Optional: einfache Prüfung, dass alles geladen wurde
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
        # Hier könntest du es an Worker / Queue weitergeben
    else:
        print("Keine valide Nachricht")
    
    return {"ok": True}

def get_futures_balance(usdt=True):
    url = BASE_URL + "/api/mix/v1/account/accounts"
    # Auth Header je nach Bitget Docs nötig (hier nur symbolisch)
    headers = {
        "ACCESS-KEY": BITGET_API_KEY,
        "ACCESS-SIGN": "...",
        "ACCESS-TIMESTAMP": str(time.time()),
        "ACCESS-PASSPHRASE": BITGET_PASSWORD
    }
    resp = requests.get(url, headers=headers)
    data = resp.json()
    # Beispiel: USDT Futures Konto
    balance = 0
    for account in data["data"]:
        if account["currency"] == "USDT":
            balance = float(account["available"])
    return balance

def get_current_price(symbol: str):
    url = f"{BASE_URL}/api/mix/v1/market/ticker?symbol={symbol}"
    resp = requests.get(url)
    data = resp.json()
    # z.B. letzter Preis
    return float(data["data"]["last"])

def calculate_position_size(balance: float, risk_percent: float, entry_price: float):
    # X% vom Kontostand
    risk_amount = balance * risk_percent / 100
    # Positionsgröße = Risiko / Entry Preis
    position_size = risk_amount / entry_price
    return position_size

def validate_trade(position_type, entry_price, stop_loss, take_profits, current_price):
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
        # PAIR
        pair_match = re.search(r"PAIR:\s*(\w+/\w+)", text)
        pair = pair_match.group(1) if pair_match else None
        symbol = pair.replace("/", "") if pair else None  # Bitget Format

        # TYPE
        type_match = re.search(r"TYPE:\s*(LONG|SHORT)", text)
        position_type = type_match.group(1) if type_match else None

        # ENTRY
        entry_match = re.search(r"ENTRY:\s*([\d.]+)", text)
        entry_price = float(entry_match.group(1)) if entry_match else None

        # SL
        sl_match = re.search(r"SL:\s*([\d.]+)", text)
        stop_loss = float(sl_match.group(1)) if sl_match else None

        # TAKE PROFIT TARGETS
        tp_matches = re.findall(r"TP\d+:\s*([\d.]+)", text)
        take_profits = [float(tp) for tp in tp_matches] if tp_matches else []

        # LEVERAGE
        lev_match = re.search(r"LEVERAGE:\s*x(\d+)", text)
        leverage = int(lev_match.group(1)) if lev_match else None


        if not all([symbol, position_type, entry_price, stop_loss, leverage]):
            return None  # Pflichtfelder fehlen
        
        balance = get_futures_balance()
        position_size = calculate_position_size(balance, 2, entry_price)
        current_price = get_current_price(symbol)
        
        if not validate_trade(position_type, entry_price, stop_loss, take_profits, current_price):
            print("Trade invalid due to current price vs SL/TP")
            return None        
        
        return {
            "symbol": symbol,
            "position_size": position_size,
            "type": position_type,
            "entry": entry_price,
            "sl": stop_loss,
            "tps": take_profits,
            "leverage": leverage
        }
    except Exception as e:
        print("Parsing Error:", e)
    
    return None
