from fastapi import FastAPI, Request
import re
import os
from dotenv import load_dotenv
import requests


app = FastAPI()

# Lädt die .env Datei
load_dotenv()

# API Keys auslesen
BITGET_API_KEY = os.getenv("BITGET_API_KEY")
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET")
BITGET_PASSWORD = os.getenv("BITGET_PASSWORD")

USDT_BUDGET = float(os.getenv("BITGET_USDT_SIZE") or 10)
DEFAULT_LEVERAGE = float(os.getenv("BITGET_LEVERAGE", 1))

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
        # Hier könntest du es an Worker / Queue weitergeben
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
        # data["data"] ist eine Liste → erstes Element auswählen
        ticker_info = data["data"][0]
        price = float(ticker_info["lastPr"])
        print(f"[DEBUG] get_current_price: {symbol} = {price}")
        return price
    except (KeyError, TypeError, ValueError, IndexError):
        raise Exception(f"Unexpected response: {data}")


def get_position_size(symbol: str, usdt_budget: float = None, leverage: float = None, product_type: str = "USDT-FUTURES") -> float:
    usdt_budget = usdt_budget or USDT_BUDGET
    leverage = leverage or DEFAULT_LEVERAGE

    price = get_current_price(symbol, product_type)
    base_amount = (usdt_budget * leverage) / price
    base_amount = round(base_amount, 6)

    print(f"[DEBUG] get_position_size: symbol={symbol}, price={price}, usdt_budget={usdt_budget}, "
          f"leverage={leverage}, calculated_size={base_amount}")
    return base_amount


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
        position_size = get_position_size(symbol, leverage=leverage)


        if not validate_trade(position_type, entry_price, stop_loss, take_profits, current_price):
            print(f"[DEBUG] Current={current_price}, SL={stop_loss}, TP_min={min(take_profits)}, TP_max={max(take_profits)}")
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
