import asyncio
import hashlib
import hmac
import time
import json
import os
import httpx
import logging
import sys 
import base64 
from dotenv import load_dotenv

# --- Konfiguration und Konstanten ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)

# Bitget API Endpunkte
BASE_URL = "https://api.bitget.com"
PLAN_URL = "/api/mix/v1/plan/currentPlan"
POSITION_URL = "/api/mix/v1/position/allPosition" 
# Generischer Plan Cancel Endpunkt (wird verwendet, da dedizierte Endpunkte nicht funktionieren)
CANCEL_PLAN_URL = "/api/mix/v1/plan/cancelPlan" 
PLACE_PLAN_URL = "/api/mix/v1/plan/placePlan"
TIME_URL = "/api/v2/public/time" 

# --- Globale Variablen ---
# Zeitversatz zwischen lokalem System und Bitget-Server (in Millisekunden)
TIME_OFFSET_MS = 0 

# --- Umgebungsvariablen laden ---
load_dotenv() 

API_KEY = os.getenv("BITGET_API_KEY")
SECRET_KEY = os.getenv("BITGET_API_SECRET")
PASSPHRASE = os.getenv("BITGET_PASSWORD")

if not all([API_KEY, SECRET_KEY, PASSPHRASE]):
    print("FATAL ERROR: Umgebungsvariablen (BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSWORD) fehlen. Programm wird beendet.")
    logging.error("Umgebungsvariablen (BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSWORD) fehlen. Beende Programm.")
    sys.exit(1)

logging.info("Umgebungsvariablen erfolgreich geladen und geprüft.") 

# --- Hilfsfunktionen für die API-Kommunikation ---

def generate_bitget_signature(timestamp: str, method: str, request_path: str, body: str = "") -> str:
    """Generiert die Bitget API-Signatur (Base64-kodiert)."""
    message = timestamp + method.upper() + request_path + body
    hmac_key = SECRET_KEY.encode('utf-8')
    
    # Verwendung der Base64-Kodierung
    h = hmac.new(hmac_key, message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(h.digest()).decode()
    return signature

def get_bitget_timestamp() -> str:
    """Gibt den korrigierten, synchronisierten Bitget-Timestamp zurück."""
    global TIME_OFFSET_MS
    # Aktuelle lokale Zeit in MS + Korrektur-Offset
    return str(int(time.time() * 1000) + TIME_OFFSET_MS)

def get_headers(method: str, path: str, body: dict = None) -> dict:
    """
    Erstellt die notwendigen HTTP-Header für Bitget.
    Für die Signatur wird der Body in ein kompaktes JSON-Format konvertiert.
    """
    timestamp = get_bitget_timestamp() # Verwende synchronisierten Timestamp
    
    # Kompaktes JSON für die Signatur verwenden.
    body_str = json.dumps(body, separators=(',', ':')) if body else "" 
    
    signature = generate_bitget_signature(timestamp, method, path, body_str)

    return {
        "Content-Type": "application/json",
        "ACCESS-KEY": API_KEY,
        "ACCESS-SIGN": signature,
        "ACCESS-TIMESTAMP": timestamp,
        "ACCESS-PASSPHRASE": PASSPHRASE,
        "locale": "en-US"
    }

async def make_api_request(method: str, url: str, params: dict = None, json_data: dict = None):
    """Führt einen API-Request aus und verarbeitet Fehler."""
    method = method.upper()
    path = url.replace(BASE_URL, '')
    
    # Pfad für die Signatur korrigieren (mit Query-Parametern bei GET)
    signed_path = path
    if method == "GET" and params:
        query_string = str(httpx.QueryParams(params)) 
        if query_string:
            signed_path += "?" + query_string
            
    headers = get_headers(method, signed_path, json_data)

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=10.0) as client:
        try:
            if method == "GET":
                response = await client.get(path, headers=headers, params=params)
            else: # POST
                response = await client.post(path, headers=headers, json=json_data)
            
            # Prüft auf 4xx/5xx HTTP-Statuscodes
            response.raise_for_status()
            
            data = response.json()
            
            # Prüft auf Bitget-spezifische Fehlercodes (Code != 00000)
            if data.get("code") != "00000":
                # Tolerierte Fehler, die unwahrscheinlicherweise 200 OK zurückgeben, aber für den Fall
                if data.get("code") in ["40034", "43020"]:
                    logging.warning(f"Bitget API Fehler (Code: {data.get('code')}): Order existiert nicht. IGNORIERE und fahre fort.")
                    return {} 
                    
                logging.error(f"Bitget API Fehler (Code: {data.get('code')}): {data.get('msg')} für {path}")
                logging.error(f"-> Vollständige API-Antwort: {data}")
                return None
            
            return data.get("data")
        
        except httpx.HTTPStatusError as e:
            # HIER liegt das Problem: Der HTTP-Fehler 400 wird ausgelöst, bevor wir den Body prüfen.
            logging.error(f"HTTP-Statusfehler beim Aufruf von {path}: {e.response.status_code}")
            
            # NEUE LOGIK: Versuche, den Body zu parsen, um den bekannten Bitget-Fehler abzufangen.
            try:
                error_data = e.response.json()
                error_code = error_data.get("code")
                
                # Wenn es HTTP 400 und der bekannte 'Order existiert nicht' Fehler ist
                if e.response.status_code == 400 and error_code in ["43020", "40034"]:
                    logging.warning(f"  -> Bitget API Fehler (Code: {error_code}): Order existiert nicht (im HTTP-Fehlerblock abgefangen). IGNORIERE und setze Korrektur fort.")
                    return {} # Signalisiere Erfolg, um die Platzierung zu starten

            except Exception:
                pass # Ignoriere JSON-Parsing-Fehler

            # Standard-Fehlerbehandlung, wenn es ein anderer oder unparsbarer Fehler ist.
            try:
                response_text = e.response.text
            except Exception:
                response_text = "Kein Response-Text verfügbar."
            
            logging.error(f"-> Response Body (Text): {response_text}")
            logging.error(f"-> Response Header: {dict(e.response.headers)}")
            return None
        
        except httpx.RequestError as e:
            logging.error(f"Netzwerk- oder Request-Fehler beim Aufruf von {path}: {e}")
            return None


async def sync_server_time():
    """Ruft die Bitget Serverzeit ab und berechnet den Zeitversatz."""
    global TIME_OFFSET_MS
    
    logging.info("Synchronisiere Zeit mit Bitget Server...")
    
    local_timestamp_start = int(time.time() * 1000)
    
    # Öffentlicher Endpunkt, benötigt keine Signatur oder speziellen Header
    async with httpx.AsyncClient(base_url=BASE_URL, timeout=5.0) as client:
        try:
            response = await client.get(TIME_URL)
            response.raise_for_status()
            data = response.json()
            
            server_timestamp = 0
            
            # Robuster Zugriff auf das verschachtelte Feld {'data': {'serverTime': '...'}}
            server_timestamp_raw = data.get("data", {}).get("serverTime")
            
            if server_timestamp_raw is not None:
                try:
                    # Konvertiere den String-Timestamp in einen Integer
                    server_timestamp = int(server_timestamp_raw)
                except (ValueError, TypeError) as e:
                    logging.error(f"Konvertierungsfehler: Konnte Server-Timestamp ({server_timestamp_raw}) nicht in Integer umwandeln.")
                    logging.error(f"-> Rohe Antwort (Data-Feld): {data.get('data')}") 
                    server_timestamp = 0
            
            local_timestamp_end = int(time.time() * 1000)
            
            if server_timestamp:
                TIME_OFFSET_MS = server_timestamp - local_timestamp_end
                logging.info(f"Zeit-Synchronisation erfolgreich. Lokale Abweichung: {TIME_OFFSET_MS} ms.")
            else:
                logging.warning(f"Konnte Server-Timestamp nicht korrekt abrufen. Verwende lokale Zeit.")
        
        except Exception as e:
            logging.error(f"Fehler bei der Zeit-Synchronisation: {e}. Verwende lokale Zeit.")


# --- Kernlogik des Handlers ---

async def get_all_open_positions():
    """Ruft alle offenen Positionen ab und gibt ein Dictionary zurück: {symbol: size}."""
    logging.info("Schritt 1: Rufe alle offenen Positionen ab.")
    
    params = {"productType": "UMCBL"}
    positions_data = await make_api_request("GET", BASE_URL + POSITION_URL, params=params)
    
    open_positions = {}
    position_list_from_api = positions_data if isinstance(positions_data, list) else [] 

    if position_list_from_api:
        for pos in position_list_from_api:
            # Wir verwenden 'total', welches die gesamte Positionsgröße darstellt.
            total_size_raw = pos.get("total", "0")
            
            try:
                hold_size = float(total_size_raw) # Konvertiere den String "total" zu float
            except (ValueError, TypeError):
                logging.error(f"Fehler beim Konvertieren der Positionsgröße 'total' ({total_size_raw}) für {pos.get('symbol')}. Überspringe diese Position.")
                continue

            if hold_size > 0:
                symbol = pos["symbol"]
                # Speichere auch den Entry Price für zukünftige SL-Platzierung
                entry_price_raw = pos.get("averageOpenPrice", pos.get("averageOpenPriceUsd"))
                try:
                    entry_price = float(entry_price_raw)
                except (ValueError, TypeError):
                    entry_price = None # Nicht kritisch für diesen Check, aber gut zu haben
                    
                open_positions[symbol] = {
                    "size": hold_size, 
                    "side": pos["holdSide"],
                    "entry_price": entry_price
                }
                logging.info(f"-> Offene Position gefunden: {symbol}, Größe (Total): {total_size_raw}, Seite: {pos['holdSide']}")
            
            # Logge Positionen mit size=0 (z.B. Hedge-Mode Null-Positionen) zur Diagnose
            elif hold_size == 0 and pos.get("symbol"):
                logging.debug(f"-> Ignoriere geschlossene Position/Hedge-Side mit Größe 0: {pos['symbol']}")

        if not open_positions:
             logging.warning("API-Aufruf lieferte Daten, aber keine aktive Position (> 0 Größe) gefunden.")
             logging.warning("Bitte prüfen Sie den API-Schlüssel, die Berechtigungen (Futures Read) und den Bitget Kontotyp (Unified/Classic Futures).")
             logging.debug(f"Rohe API-Antwort für Positionen (Debug-Daten): {json.dumps(position_list_from_api, indent=2)}")

    elif positions_data is None:
        logging.error("API-Anfrage für Positionen ist fehlgeschlagen (siehe vorherige Fehlermeldungen).")
    else: # positions_data is []
        logging.info("API-Aufruf für Positionen lieferte eine leere Liste.")

    return open_positions

async def get_sl_and_tp_orders():
    """Ruft alle ausstehenden Plan-Orders ab und identifiziert SL- und TP-Orders (gefiltert auf Market-Orders)."""
    logging.info("Schritt 2: Rufe alle ausstehenden Conditional Orders ab.")
    
    params = {"productType": "UMCBL"}
    plan_orders_raw = await make_api_request("GET", BASE_URL + PLAN_URL, params=params)
    
    sl_orders = {}
    plan_orders_list = [] # Hier speichern wir die tatsächliche Liste der Orders

    if isinstance(plan_orders_raw, list):
        plan_orders_list = plan_orders_raw
    elif isinstance(plan_orders_raw, dict):
        logging.debug(f"Rohe API-Antwort für Conditional Orders (Debug-Daten): {json.dumps(plan_orders_raw, indent=2)}")
        
        if plan_orders_raw.get("planList") and isinstance(plan_orders_raw["planList"], list):
            plan_orders_list = plan_orders_raw["planList"]
            logging.info(f"Erfolgreich 'planList' mit {len(plan_orders_list)} Elementen extrahiert.")
        elif plan_orders_raw.get("orderList") and isinstance(plan_orders_raw["orderList"], list):
            plan_orders_list = plan_orders_raw["orderList"]
            logging.info(f"Erfolgreich 'orderList' mit {len(plan_orders_list)} Elementen extrahiert.")
        else:
            logging.error("Konnte Order-Liste nicht aus Dictionary-Antwort extrahieren.")
    elif plan_orders_raw is None:
        logging.warning("Keine Conditional Order Daten von der API erhalten (siehe vorherige Fehler).")
        return sl_orders
    
    # Weiterverarbeitung, wenn eine Liste extrahiert wurde
    if plan_orders_list:
        for order in plan_orders_list:
            
            if not all(k in order for k in ["symbol", "orderId", "size", "triggerPrice"]):
                logging.warning(f"Order-Objekt unvollständig (fehlt orderId/size/etc.). Überspringe: {order}")
                continue

            symbol = order["symbol"]
            
            # WICHTIG: Filtern nach Stop-Loss-Orders (orderType == "market")
            if order.get("orderType") == "market": 
                sl_orders[symbol] = {
                    "planId": order["orderId"],
                    "size": float(order["size"]),
                    "triggerPrice": float(order["triggerPrice"])
                }
                logging.info(f"-> SL-Order gefunden: {symbol}, Plan-ID (orderId): {order['orderId']}, Größe: {order['size']}")
            else:
                 logging.debug(f"-> Ignoriere Nicht-Market Plan Order (wahrscheinlich TP oder Limit SL): {symbol}, Type: {order.get('orderType')}")

    return sl_orders

async def cancel_and_replace_sl(symbol: str, old_sl: dict, new_size: float, position_side: str, entry_price: float | None):
    """Storniert die alte SL-Order und platziert eine neue mit korrigierter Größe."""
    
    old_plan_id = old_sl["planId"]
    old_size = old_sl["size"]
    trigger_price = old_sl["triggerPrice"]
    rounded_trigger_price = round(trigger_price, 4)

    logging.warning(f"--- KORREKTUR ERFORDERLICH für {symbol} ---")
    logging.warning(f"  Position: {new_size:.4f}, SL-Order: {old_size:.4f} (Plan-ID: {old_plan_id})")
    
    # 1. Storniere die alte, überdimensionierte SL-Order
    # Wir verwenden planType: "normal_plan"
    cancel_payload = {
        "symbol": symbol,
        "productType": "UMCBL",
        "marginCoin": "USDT", 
        "orderId": old_plan_id, 
        "planType": "normal_plan" 
    }
    logging.info(f"  -> Storniere alte SL-Order {old_plan_id} mit generischem Plan-Endpunkt ({CANCEL_PLAN_URL}, planType: normal_plan)...")
    
    # make_api_request fängt 40034/43020 ab und gibt {} zurück, um Platzierung zu erlauben.
    cancel_result = await make_api_request("POST", BASE_URL + CANCEL_PLAN_URL, json_data=cancel_payload)

    if cancel_result is None:
        # Hier landen wir nur bei nicht-tolerierten Fehlern (z.B. 404, 500)
        logging.error(f"  !! Fehler beim Stornieren der SL-Order {old_plan_id} über {CANCEL_PLAN_URL}. Abbruch der Korrektur.")
        return
    elif cancel_result == {}:
        # Hier landen wir bei den ignorierten Fehlern 40034/43020
        logging.warning(f"  -> Stornierungsversuch fehlgeschlagen (Fehler 43020/40034). Platziere dennoch neue SL-Order.")
    else:
         # Stornierung war erfolgreich (Code 00000)
        logging.info(f"  -> Stornierung erfolgreich. Platziere neue SL-Order.")


    # 2. Platziere die neue SL-Order mit der korrekten Größe (new_size)
    
    if position_side == "long":
        new_side = "close_long"
    elif position_side == "short":
        new_side = "close_short"
    else:
        logging.error(f"  !! Fehler: Unbekannte Position Side ({position_side}). Abbruch der Platzierung.")
        return
    
    # executePrice für Market SL auf "0" setzen 
    execute_price = "0"
    
    place_payload = {
        "symbol": symbol,
        "size": str(round(new_size, 4)), 
        "side": new_side, 
        "orderType": "market", 
        "productType": "UMCBL",
        "marginCoin": "USDT",
        "triggerPrice": str(rounded_trigger_price),
        "triggerType": "mark_price",
        "executePrice": execute_price 
    }
    
    logging.info(f"  -> Platziere neue SL-Order: Side={new_side}, Größe={new_size:.4f}, Trigger={rounded_trigger_price}")
    new_order_result = await make_api_request("POST", BASE_URL + PLACE_PLAN_URL, json_data=place_payload)
    
    if new_order_result:
        # Die Platzierung gibt orderId zurück, wir loggen dies zur Info
        logging.info(f"  -> Korrektur erfolgreich! Neue Plan-ID (orderId): {new_order_result.get('orderId')}")
    else:
        logging.error("  !! Kritischer Fehler: Neue SL-Order konnte nicht platziert werden.")


async def run_sl_correction_check():
    """Die Hauptfunktion, die alle Schritte des Polling-Prozesses durchläuft."""
    
    open_positions = await get_all_open_positions()
    
    if not open_positions:
        logging.info("Keine offenen Positionen gefunden. Beende Check.")
        return

    sl_orders = await get_sl_and_tp_orders()

    corrected_count = 0
    missing_sl_symbols = [] 
    
    for symbol, pos_data in open_positions.items():
        current_size = pos_data["size"]
        position_side = pos_data["side"]
        entry_price = pos_data["entry_price"] 
        
        if symbol in sl_orders:
            sl_data = sl_orders[symbol]
            registered_sl_size = sl_data["size"]
            
            # Toleranz für Fließkommavergleiche
            if abs(current_size - registered_sl_size) > 0.0001:
                logging.info(f"*** ABWEICHUNG ERKANNT für {symbol} ***")
                logging.info(f"  - Offene Größe: {current_size:.4f}")
                logging.info(f"  - Registrierte SL-Größe: {registered_sl_size:.4f}")
                
                await cancel_and_replace_sl(
                    symbol=symbol, 
                    old_sl=sl_data, 
                    new_size=current_size, 
                    position_side=position_side,
                    entry_price=entry_price
                )
                corrected_count += 1
            else:
                logging.info(f"Position für {symbol} ist synchronisiert. Größe: {current_size:.4f}")
        else:
            missing_sl_symbols.append(symbol)
            logging.warning(f"Offene Position für {symbol} hat KEINE aktive SL-Order.")

    if missing_sl_symbols:
        logging.warning(f"ZUSAMMENFASSUNG: Für die folgenden Symbole fehlen aktive Stop-Loss (Market Plan) Orders: {', '.join(missing_sl_symbols)}. Manuelle Prüfung/Platzierung erforderlich!")

    logging.info(f"Polling-Durchlauf abgeschlossen. Korrigierte SL-Orders: {corrected_count}.")


async def main_loop():
    """Startet die Endlosschleife für den Polling-Dienst."""
    await sync_server_time() 
    logging.info("--- Bitget Position Handler gestartet ---")
    while True:
        try:
            await run_sl_correction_check()
        except Exception as e:
            logging.critical(f"Kritischer Fehler im Haupt-Loop: {e}")
            
        logging.info("Warte 5 Minuten bis zum nächsten Polling-Durchlauf...")
        await asyncio.sleep(300)

if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logging.info("Programm durch Benutzer beendet.")
    except Exception as e:
        logging.critical(f"Unerwarteter Fehler im Main: {e}")