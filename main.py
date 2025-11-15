import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import re
import time

import httpx
import uvicorn
from telethon import TelegramClient, events
from dotenv import load_dotenv
from fastapi import FastAPI, Request

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()

@app.get("/")
def read_root():
    """Zeigt den Status an. Wird von Railway für den Health Check benötigt."""
    return {"status": "Service is running", "message": "OK - User Client is listening in background."}


load_dotenv()
# --- Telethon Config ---
API_ID = os.getenv("TELEGRAM_API_ID")
API_HASH = os.getenv("TELEGRAM_API_HASH")
SESSION_NAME = os.getenv("TELEGRAM_SESSION_FILE", "bot_session") # Benötigt, um sich nicht ständig neu anzumelden
SESSION_PARTS = []
# Durchsuche alle Umgebungsvariablen
for key, value in os.environ.items():
    # Prüfe auf das Präfix und stelle sicher, dass der Wert nicht leer ist
    if key.startswith("TELEGRAM_SESSION_PART") and value:
        try:
            # Extrahiere die Part-Nummer (z.B. '1' aus 'TELEGRAM_SESSION_PART1')
            part_number_str = key.replace("TELEGRAM_SESSION_PART", "")
            part_number = int(part_number_str)
            SESSION_PARTS.append((part_number, value))
        except ValueError:
            # Ignoriere Schlüssel, die nicht mit einer Zahl enden
            continue 

# Sortiere nach der Part-Nummer (1, 2, 3...)
SESSION_PARTS.sort(key=lambda x: x[0])
SESSION_BASE64 = "".join([part[1] for part in SESSION_PARTS])

if SESSION_BASE64:
    logging.info("Telethon Session Content gefunden und aus %d Teilen zusammengesetzt.", len(SESSION_PARTS))
else:
    logging.warning("Kein TELEGRAM_SESSION_PART... Umgebungsvariable gefunden.")

SOURCE_CHANNELS_RAW = os.getenv("TELEGRAM_SOURCE_CHANNELS", "")
if not SOURCE_CHANNELS_RAW:
    logging.error("TELEGRAM_SOURCE_CHANNELS Umgebungsvariable fehlt!")

SOURCE_FILTERS = set()
SOURCE_CHANNELS = set()

for item in SOURCE_CHANNELS_RAW.split(','):
    item = item.strip()
    if not item: continue
    
    # Der item kann entweder nur die Chat ID sein oder Chat ID:Thread ID
    parts = item.split(':')
    
    try:
        chat_id_raw = parts[0]
        # Füge die Haupt-ID zur Filter-Set hinzu (für Nachrichten ohne Thread)
        SOURCE_FILTERS.add(chat_id_raw)
        
        # Füge die Haupt-ID zur Telethon-Event-Liste hinzu (muss ein Integer sein, wenn negativ)
        chat_id = int(chat_id_raw) if chat_id_raw.startswith('-') else chat_id_raw
        SOURCE_CHANNELS.add(chat_id)
        
        # Wenn eine Thread-ID vorhanden ist, füge die Kombination ebenfalls zur Filter-Set hinzu
        if len(parts) > 1:
            thread_id = parts[1]
            if thread_id:
                SOURCE_FILTERS.add(f"{chat_id_raw}:{thread_id}")

    except ValueError:
        logging.warning("Ungültiges Format in TELEGRAM_SOURCE_CHANNELS gefunden: %s. Ignoriere.", item)

if not SOURCE_CHANNELS and SOURCE_CHANNELS_RAW:
    logging.warning("Keine gültigen Kanal-IDs in TELEGRAM_SOURCE_CHANNELS gefunden.")


# Cache für Symbol-Informationen (Precision, Min Size, etc.)
SYMBOL_INFO_CACHE = {} 

# API Keys auslesen
BITGET_API_KEY = os.getenv("BITGET_API_KEY")
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET")
BITGET_PASSWORD = os.getenv("BITGET_PASSWORD")

USDT_BUDGET = float(os.getenv("BITGET_USDT_SIZE") or 10)
DEFAULT_LEVERAGE = float(os.getenv("BITGET_LEVERAGE", 1))
TEST_MODE = os.getenv("BITGET_TEST_MODE", "True").lower() == "true"


# Base URL für Bitget Futures
BASE_URL = "https://api.bitget.com"

if not all([BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSWORD, API_ID, API_HASH]):
    raise ValueError("Mindestens ein benötigtes API Credential (Bitget oder Telegram) fehlt in .env")

async def run_client():
    if SESSION_BASE64:
        session_filepath = f"{SESSION_NAME}.session"
    try:
        # Den Base64-String in binäre Daten dekodieren
        session_data = base64.b64decode(SESSION_BASE64)
        # Die .session-Datei im Container speichern
        with open(session_filepath, 'wb') as f:
            f.write(session_data)
        logging.info("Telethon Session erfolgreich aus Base64 dekodiert und als Datei gespeichert.")
    except Exception as e:
        logging.error("Fehler beim Dekodieren oder Speichern der Session-Datei: %s", e)
        # Bei Fehler den Start abbrechen oder versuchen, neu zu autorisieren (was hier nicht implementiert ist)
        return

    # client wird als 'user' initialisiert (phone=None, da die Session-Datei später geladen wird)
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    try:
        await client.start()
        
        # Wichtig: Die erste Anmeldung erfordert die Eingabe des Codes im Terminal.
        # Da Railway kein interaktives Terminal hat, MÜSSEN Sie sich lokal anmelden,
        # damit die Session-Datei (bot_session.session) erstellt wird, 
        # und diese Datei dann zu Ihrem Deployment hinzufügen.
        # Ohne Session-Datei wird der Start auf Railway fehlschlagen!
        logging.info("Telethon Client gestartet.")

        # Nachricht an den Admin senden, dass der Bot erfolgreich gestartet wurde
        # (Optional, aber hilfreich für die Diagnose)
        
    except Exception as e:
        logging.error("Fehler beim Starten des Telethon-Clients: %s", e)
        return

    # Registriert den Handler für neue Nachrichten
    # Hier filtern wir nur Nachrichten aus den definierten Source Channels
    @client.on(events.NewMessage(chats=list(SOURCE_CHANNELS)))
    async def handler(event):
        """Verarbeitet jede neue Nachricht aus den überwachten Channels."""
        message_text = event.message.message
        
        logging.debug(f"Event received for Chat ID: {event.chat_id} (Type: {type(event.chat_id)}), Message: {message_text[:30]}...")
        
        # Ignoriere leere Nachrichten oder solche, die nur Bilder sind
        if not message_text:
             return
        
        chat_id_str = str(event.chat_id)
        thread_id = getattr(event.message, 'reply_to_top_id', None)
        filter_key = chat_id_str
        if thread_id:
            # Wenn es einen Thread gibt, verwenden wir die Kombination
            filter_key = f"{chat_id_str}:{thread_id}"
            
        if filter_key not in SOURCE_FILTERS:
            logging.debug("Nachricht von nicht autorisiertem Thread/Chat %s empfangen. Ignoriere.", filter_key)
            return
            
        logging.info(f"Nachricht von autorisiertem Chat/Thread {filter_key} empfangen: {message_text[:50]}...")

        # Die gesamte Logik des Webhooks wird hierher verschoben
        signal = await parse_signal(message_text)
        
        if signal:
            logging.info("Signal erkannt: %s", signal)
            await place_bitget_trade(signal, test_mode=TEST_MODE)
        else:
            logging.debug("Kein valides Handelssignal in der Nachricht.")


    # Halte den Client am Laufen, bis er manuell beendet wird
    await client.run_until_disconnected()


# --- START THE APPLICATION ---

@app.on_event("startup")
async def startup_event():
    # Startet den Client-Task, blockiert aber nicht den FastAPI-Server
    asyncio.create_task(run_client())


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

async def get_symbol_metadata(base_symbol: str) -> int:
    global SYMBOL_INFO_CACHE
    
    if base_symbol in SYMBOL_INFO_CACHE:
        return SYMBOL_INFO_CACHE[base_symbol]

    url = f"{BASE_URL}/api/v3/market/instruments" 
    params = {"category": "USDT-FUTURES"} # USDT-M Contracts

    fallback = {"sizeScale": 4, "priceScale": 4} 

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status() 
            data = resp.json()

        if data.get("code") != "00000":
            logging.error("API error (get_symbol_precision): %s", data)
            return 4 # Fallback
            
        for symbol_info in data["data"]:
            symbol = symbol_info["symbol"]
            if not symbol: continue
            if symbol == base_symbol: 
                size_scale_raw = symbol_info.get("quantityPrecision")
                price_scale_raw = symbol_info.get("pricePrecision")
                
                if size_scale_raw is None or price_scale_raw is None:
                    logging.warning("Precision (quantityPrecision or pricePrecision) not found for %s. Defaulting to 4/4.", base_symbol)
                    return fallback
                size_scale = int(size_scale_raw)
                price_scale = int(price_scale_raw)
                
                metadata = {
                    "sizeScale": size_scale,
                    "priceScale": price_scale
                }

                SYMBOL_INFO_CACHE[base_symbol] = metadata
                logging.info("[DEBUG] Fetched metadata for %s: quantityPrecision=%d, pricePrecision=%d", 
                             base_symbol, size_scale, price_scale)
                return metadata
        logging.warning("Metadata not found for symbol %s in market instruments list. Defaulting to 4/4.", base_symbol)
        return fallback
        
    except Exception as e:
        logging.error("Error fetching symbol precision: %s", e)
        return 4 # Fallback bei Fehler

async def get_quantity_scale(base_symbol: str) -> int:
    """Gibt die Präzision für die Positionsgröße zurück."""
    metadata = await get_symbol_metadata(base_symbol)
    return metadata.get("sizeScale", 4)

async def get_price_scale(base_symbol: str) -> int:
    """Gibt die Präzision für den Preis zurück."""
    metadata = await get_symbol_metadata(base_symbol)
    return metadata.get("priceScale", 4)


async def get_position_size(base_symbol: str, usdt_budget: float = None, leverage: float = None, product_type: str = "USDT-FUTURES"):
    usdt_budget = usdt_budget or USDT_BUDGET
    leverage = leverage or DEFAULT_LEVERAGE

    size_scale = await get_quantity_scale(base_symbol)
    
    # Aktueller Preis
    price = await get_current_price(base_symbol, product_type)
    total_size_raw = (usdt_budget * leverage) / price
    total_size = round(total_size_raw, size_scale)

    tp_sizes_raw = [
        total_size_raw * 0.5,  # TP1 = 50%
        total_size_raw * 0.3,  # TP2 = 30%
    ]
    
    tp_sizes = [round(s, size_scale) for s in tp_sizes_raw]

    tp3_size_raw = total_size - sum(tp_sizes)
    tp3_size = round(tp3_size_raw, size_scale) # Stelle sicher, dass TP3 ebenfalls korrekt gerundet ist

    # Füge TP3 hinzu, aber nur wenn es größer als 0 ist
    if tp3_size > 0:
        tp_sizes.append(tp3_size)
    else:
        # Falls TP3 zu klein ist, verteilen wir den Rest auf TP2 (oder TP1)
        if len(tp_sizes) >= 2:
            tp_sizes[-1] = round(tp_sizes[-1] + tp3_size_raw, size_scale)
        elif len(tp_sizes) == 1:
            tp_sizes[0] = round(tp_sizes[0] + tp3_size_raw, size_scale)
    

    # Final prüfen, ob die Gesamtgröße durch die Rundung auf 0 gefallen ist (Minimale Größe)
    if total_size <= 0 and total_size_raw > 0:
         logging.warning("Calculated position size %f was rounded down to 0 or below due to precision or minimum size.", total_size_raw)

    print(f"[DEBUG] get_position_size: symbol={base_symbol}, price={price}, sizeScale={size_scale}, total_size={total_size}, tp_sizes={tp_sizes}")
    
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

# Stellt den Hebel für das Handelspaar explizit ein
async def set_leverage(symbol: str, leverage: int, margin_mode: str = "isolated"):
    url_path = "/api/v2/mix/account/set-leverage"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))
  
    payload = {
        "symbol": symbol,
        "marginCoin": "USDT",
        "leverage": str(leverage),
        "marginMode": margin_mode,
        "productType": "UMCBL",
        "side": "whole" # Setzt Leverage für LONG und SHORT gleichzeitig
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
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(url, headers=headers, data=body)
            
            if resp.status_code >= 400:
                error_data = resp.json()
                logging.error("[ERROR 4xx/5xx SET LEVERAGE] Status %d. Response Body: %s (Payload: %s)", 
                              resp.status_code, error_data, payload)
                return False

            data = resp.json()
            if data.get("code") != "00000":
                logging.error("[ERROR] Bitget API response (Set Leverage failed): %s (Payload: %s)", data, payload)
                return False
            
            logging.info(f"[SUCCESS] Leverage erfolgreich auf x{leverage} für {symbol} gesetzt.")
            return True
            
    except httpx.RequestException as e:
        logging.error("[ERROR] Set Leverage Request failed (network/timeout): %s", e)
        return False

async def place_market_order(symbol, size, side, leverage=10):
    url_path = "/api/mix/v1/order/placeOrder"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    payload = {
        "symbol": symbol,
        "size": str(size),
        "side": side,             # "buy" oder "sell"
        "orderType": "market",
        "marginMode": "isolated",  # isolierter Margin-Modus
        "productType": "UMCBL",
        "leverage": str(leverage),
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

    base_symbol = symbol.replace("_UMCBL", "")
    price_scale = await get_price_scale(base_symbol)
    rounded_price = round(trigger_price, price_scale)
    
    logging.info(f"[DEBUG] Applying price rounding: Original={trigger_price}, Scale={price_scale}, Rounded={rounded_price}")


    # SL soll Market sein, TP soll Limit sein
    order_type = "market" if is_sl else "limit"
    if is_sl:
        # SL muss Market sein, damit er garantiert ausgeführt wird.
        order_type = "market" 
        entrust_price = "0" # Oder weglassen, aber 0 ist sicherer
        logging.info("[DEBUG] Conditional Order: Setting SL as Market Order.")
    else:
        # TP ist eine Limit Order mit dem TP-Preis als Limit-Preis
        order_type = "limit"
        entrust_price = str(rounded_price) # Der Preis, zu dem ausgeführt werden soll (Limit-Preis)
        logging.info("[DEBUG] Conditional Order: Setting TP as Limit Order.")


    payload = {
        "symbol": symbol,
        "size": str(size),
        "side": side,
        "orderType": order_type,
        "productType": "UMCBL",
        "marginCoin": "USDT",
        "triggerPrice": str(rounded_price),
        "triggerType": "mark_price",
        "entrustPrice": entrust_price
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
    
    if test_mode:
        logging.info("[TEST MODE] Market Order, SL und TP Orders werden nicht gesendet")
        return

    # Set explicit Leverage for pair
    if leverage:
        base_symbol_for_leverage = symbol.replace("_UMCBL", "")
        leverage_set = await set_leverage(symbol=base_symbol_for_leverage, leverage=leverage, margin_mode="isolated")
        if not leverage_set:
            logging.error("Konnte Leverage nicht setzen. Trade wird abgebrochen.")
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


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)