from fastapi import FastAPI, Request
import re
import os
from dotenv import load_dotenv
import httpx 
import asyncio 
import time
import hmac
import hashlib
import base64
import json
import logging 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
    try: 
        data = await req.json()  # Telegram Update
        message_text = data.get("message", {}).get("text", "")
        
        if not message_text:
            return {"ok": True}  # leere Nachricht ignorieren
        
        # Parsing der Nachricht
        signal = await parse_signal(message_text)
        
        if signal:
            print("Signal erkannt:", signal)
            await place_bitget_trade(signal, test_mode=TEST_MODE)
        else:
            print("Keine valide Nachricht")
        
    except Exception as e:
        # Protokolliert den Fehler ausführlich und verhindert das Looping
        logging.error("FATAL UNHANDLED EXCEPTION DURING WEBHOOK PROCESSING! Loop averted.", exc_info=True)

    return {"ok": True}

async def get_current_price(symbol: str, product_type: str = "USDT-FUTURES") -> float:
    url = f"{BASE_URL}/api/v2/mix/market/ticker"
    params = {"symbol": symbol, "productType": product_type}
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status() 
            data = resp.json()

        if data.get("code") != "00000":
            raise Exception(f"API error (get_current_price): {data}")

        ticker_info = data["data"][0]
        price = float(ticker_info["lastPr"])
        logging.info("[DEBUG] get_current_price: %s = %f", symbol, price)
        return price
    except httpx.RequestError as e:
        logging.error("Request failed: %s", e)
        raise
    except (KeyError, TypeError, ValueError, IndexError) as e:
        logging.error("Unexpected response structure: %s - Error: %s", data, e)
        raise Exception(f"Unexpected response: {data}")


async def get_position_size(base_symbol: str, usdt_budget: float = None, leverage: float = None, product_type: str = "USDT-FUTURES"):
    usdt_budget = usdt_budget or USDT_BUDGET
    leverage = leverage or DEFAULT_LEVERAGE

    # Aktueller Preis
    price = await get_current_price(base_symbol, product_type)
    total_size = (usdt_budget * leverage) / price
    total_size = round(total_size, 4)

    # Teilpositionen für Take-Profit
    tp_sizes = [
        round(total_size * 0.5, 4),  # TP1 = 50%
        round(total_size * 0.3, 4),  # TP2 = 30%
    ]
    tp_sizes.append(round(total_size - sum(tp_sizes), 4))  # Rest in TP3

    print(f"[DEBUG] get_position_size: symbol={base_symbol}, price={price}, usdt_budget={usdt_budget}, "
          f"leverage={leverage}, total_size={total_size}, tp_sizes={tp_sizes}")
    
    return total_size, tp_sizes



def validate_trade(position_type, stop_loss, take_profits, current_price):
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
async def parse_signal(text: str):
    
    if "POSITION SIZE" not in text.upper():  # case-insensitive check
        print("no valid signal")
        return None
    
    try:
        clean_text = re.sub(r"\s+", " ", text.replace("\u00A0", " "))
        # PAIR
        pair_match = re.search(r"PAIR:\s*(\w+/\w+)", clean_text)
        pair = pair_match.group(1) if pair_match else None
        base_symbol = pair.replace("/", "").upper() if pair else None 
        trading_symbol = base_symbol + "_UMCBL" if base_symbol else None

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

        print("DEBUG:", base_symbol, position_type, entry_price, stop_loss, take_profits, leverage)


        if not all([base_symbol, position_type, entry_price, stop_loss, leverage]):
            return None  # Pflichtfelder fehlen
        
        current_price = await get_current_price(base_symbol)
        # Position berechnen + TP-Aufteilung
        position_size, tp_sizes = await get_position_size(base_symbol, leverage=leverage)
        print(f"[DEBUG] Total position size: {position_size}, TP split sizes: {tp_sizes}")


        if not validate_trade(position_type, stop_loss, take_profits, current_price):
            print(f"[DEBUG] Current={current_price}, SL={stop_loss}, TP_min={min(take_profits)}, TP_max={max(take_profits)}")
            return None        
        
        return {
            "symbol": trading_symbol,
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

async def place_market_order(symbol, size, side, leverage=10):
    url_path = "/api/mix/v1/order/placeOrder"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    payload = {
        "symbol": symbol,
        "size": size,
        "side": side,             # "buy" oder "sell"
        "orderType": "market",
        "marginMode": "isolated",  # isolierter Margin-Modus
        "productType": "UMCBL",
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
         # Verwende httpx.AsyncClient für nicht-blockierende Anfragen
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(url, headers=headers, data=body)
        
            if resp.status_code >= 400:
                try:
                    error_data = resp.json()
                    logging.error("[ERROR 4xx/5xx MARKET ORDER] Bitget API Status Code %d. Response Body: %s (Payload: %s)", 
                                  resp.status_code, error_data, payload)
                except json.JSONDecodeError:
                    logging.error("[ERROR 4xx/5xx MARKET ORDER] Bitget API Status Code %d. Response Text: %s (Payload: %s)", 
                                  resp.status_code, resp.text, payload)
                return None

        data = resp.json() 
        if data.get("code") != "00000":
            logging.error("[ERROR] Bitget API response (Status 200, but Code != 00000): %s", data)
            return None
        return data
    except httpx.RequestException as e:
        # Fängt Netzwerkfehler oder Timeouts
        logging.error("[ERROR] Market Order Request failed (network/timeout): %s", e)
        return None

async def place_conditional_order(symbol, size, trigger_price, side: str, is_sl: bool):
    url_path = "/api/mix/v1/plan/placePlan" 
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    if side not in ["close_long", "close_short"]:
        logging.error("Ungültiger Side für Conditional Order: %s. Erwarte 'close_long' oder 'close_short'.", side)
        return None

    # SL soll Market sein, TP soll Limit sein
    order_type = "market" if is_sl else "limit"
    
    payload = {
        "symbol": symbol,
        "size": str(size),
        "side": side,
        "orderType": order_type,
        "productType": "UMCBL",
        "marginCoin": "USDT",
        "triggerPrice": str(trigger_price), # Trigger-Preis
        "triggerType": "mark_price"
    }
    if is_sl:
        payload["entrustPrice"] = "0"
    else:
        payload["executePrice"] = str(trigger_price)
    
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
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(url, headers=headers, data=body)
            
            # <<< GEÄNDERT: Manuelles Handling, um Crash zu verhindern. Protokolliert den Bitget Fehler-Body.
            if resp.status_code >= 400:
                try:
                    error_data = resp.json()
                    logging.error("[ERROR 4xx/5xx CONDITIONAL ORDER] Bitget API Status Code %d. Response Body: %s (Payload: %s)", 
                                  resp.status_code, error_data, payload)
                except json.JSONDecodeError:
                    logging.error("[ERROR 4xx/5xx CONDITIONAL ORDER] Bitget API Status Code %d. Response Text: %s (Payload: %s)", 
                                  resp.status_code, resp.text, payload)
                return None  # Hält den Aufruf an place_bitget_trade() an der Absturzstelle
            # <<< ENDE ÄNDERUNG

            # Jetzt Status 200/300
            data = resp.json()
            if data.get("code") != "00000":
                logging.error("[ERROR] Bitget Conditional Plan Order API response (Status 200, but Code != 00000): %s (Payload: %s)", data, payload)
                return None
            return data
    except httpx.RequestError as e:
        logging.error("[ERROR] Conditional Order Request failed (network/timeout): %s (Payload: %s)", e, payload)
        return None

    
async def place_bitget_trade(signal, test_mode=True):
    symbol = signal["symbol"]
    position_size = signal["position_size"]
    tp_sizes = signal["tp_sizes"]
    leverage = signal["leverage"]
    sl_price = signal["sl"]
    tp_prices = signal["tps"]

    if signal["type"].upper() == "LONG":
        market_side_open = "open_long"
        closing_side = "close_short"  # Schließt LONG-Position
    else: # SHORT
        market_side_open = "open_short"
        closing_side = "close_long"   # Schließt SHORT-Position
    

    logging.info(f"[INFO] Platzieren Market Order: {market_side_open} {position_size} {symbol} (Leverage={leverage})")

    if test_mode:
        logging.info("[TEST MODE] Market Order, SL und TP Orders werden nicht gesendet")
        return

    # --- 1. Market Order platzieren ---
    market_order_resp = await place_market_order(symbol, position_size, side=market_side_open, leverage=leverage)
    logging.info("Market Order Response: %s", market_order_resp)

    if not market_order_resp:
        logging.error("Konnte Market Order nicht platzieren. SL/TP Orders werden abgebrochen.")
        return
    
    # Warten bis Order ausgeführt
    logging.info("[INFO] Warte 3.0 Sekunden (nicht-blockierend) zur Sicherheit.")
    await asyncio.sleep(3.0)

    # --- 2. Stop-Loss Order ---
    sl_resp = await place_conditional_order(
        symbol=symbol,
        size=position_size, 
        trigger_price=sl_price,
        side=closing_side,
        is_sl=True # Kennzeichnet SL -> orderType="market"
    )
    logging.info("Stop-Loss Plan Order Response: %s", sl_resp)

    # --- 3. Take-Profit Orders ---
    for i, (tp_price, tp_size) in enumerate(zip(tp_prices, tp_sizes)):
        if tp_size <= 0: continue
        
        tp_resp = await place_conditional_order(
            symbol=symbol,
            size=tp_size,
            trigger_price=tp_price,
            side=closing_side,
            is_sl=False # Kennzeichnet TP -> orderType="limit"
        )
        logging.info(f"TP{i+1} Plan Order (Price: {tp_price}, Size: {tp_size}) Response: %s", tp_resp)
        
    logging.info("[INFO] Alle Orders (Market, SL, 3xTP) wurden gesendet.")
