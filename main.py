import asyncio
import base64
import hashlib
import hmac
import json
import logging
import websockets
import os
import re
import time
import contextlib

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()
load_dotenv()

# Cache for symbol information (Precision, Min Size, etc.)
SYMBOL_INFO_CACHE = {} 

# Get User-Key for Postgres DB
BOT_USER_KEY = os.getenv("BOT_USER_KEY")
if not BOT_USER_KEY:
    logging.warning("BOT_USER_KEY environment variable is missing. Database entries will have a default key.")
    BOT_USER_KEY = "DEFAULT_USER"

# Read API Keys
BITGET_API_KEY = os.getenv("BITGET_API_KEY")
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET")
BITGET_PASSWORD = os.getenv("BITGET_PASSWORD")

USDT_BUDGET = float(os.getenv("BITGET_USDT_SIZE") or 10)
DEFAULT_LEVERAGE = float(os.getenv("BITGET_LEVERAGE", 1))
TEST_MODE = os.getenv("BITGET_TEST_MODE", "True").lower() == "true"

# Base URL for Bitget Futures
BASE_URL = "https://api.bitget.com"

if not all([BITGET_API_KEY, BITGET_API_SECRET, BITGET_PASSWORD]):
    raise ValueError("At least one required API credential is missing in .env")

# Reads TP percentages from .env (e.g., TP1_PERCENT=50, TP2_PERCENT=30, etc.)
def get_tp_config():
    """Dynamically reads TP percentages from environment variables."""
    tp_percentages = []
    max_tp_count = 3
    i = 1
    for i in range(1, max_tp_count + 1):
        key = f"TP{i}_PERCENT"
        percent_str = os.getenv(key)
        if percent_str is None:
            logging.error(f"Required environment variable {key} is missing. Assuming 0%% for this TP.")
            tp_percentages.append(0.0)
            continue
        try:
            percent = float(percent_str)
            if percent < 0:
                logging.error(f"Invalid value for {key}: {percent_str}. Must be non-negative.")
                tp_percentages.append(0.0)
            else:
                tp_percentages.append(percent / 100.0)

        except ValueError:
            logging.error(f"Invalid value for {key}: {percent_str}. Must be a number.")
            tp_percentages.append(0.0)
    
    # Fallback to a default 100% split if no TPs are defined
    if not tp_percentages:
        logging.warning("No TP percentages defined in .env (e.g., TP1_PERCENT). Defaulting to 100%% split at TP1.")
        return [1.0] # 100% at TP1
        
    return tp_percentages

TP_SPLIT_PERCENTAGES = get_tp_config()

DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    # Die DB-URL von Railway ist synchron, wir benötigen aber eine asynchrone Session. 
    # Für einfache Schreibvorgänge verwenden wir den synchrone Engine/async Session-Mix von asyncpg/SQLAlchemy
    # Beachten Sie, dass Sie für erweiterte async-Features ggf. SQLAlchemy 2.0+ und das asyncio-Backend benötigen.
    # Hier verwenden wir den synchronen connect, der in der async-Funktion läuft.
    Engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    Base = declarative_base()
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=Engine)

    class TradingSignal(Base):
        __tablename__ = "trading_signals"

        id = Column(Integer, primary_key=True, index=True)
        user_key = Column(String, index=True, nullable=False) 
        
        symbol = Column(String, index=True, nullable=False)
        position_type = Column(String, nullable=False)
        entry_price = Column(Float)
        stop_loss = Column(Float)
        leverage = Column(Integer)
        total_size = Column(Float)

        tp1_price = Column(Float, nullable=True)
        tp2_price = Column(Float, nullable=True)
        tp3_price = Column(Float, nullable=True)
        tp1_reached = Column(Boolean, default=False)
        tp2_reached = Column(Boolean, default=False)
        tp3_reached = Column(Boolean, default=False)
        sl_reached = Column(Boolean, default=False)
        sl_moved_to_be = Column(Boolean, default=False)
        tp1_order_placed = Column(Boolean, default=False)
        tp2_order_placed = Column(Boolean, default=False)
        tp3_order_placed = Column(Boolean, default=False)

        tp1_order_id = Column(String, nullable=True)
        tp2_order_id = Column(String, nullable=True)
        tp3_order_id = Column(String, nullable=True)
        sl_order_id = Column(String, nullable=True)
        
        
        # Order-Status und Metadaten
        market_order_placed = Column(Boolean, default=False)
        sl_order_placed = Column(Boolean, default=False)
        
        # Zeitstempel der Aktion
        timestamp = Column(DateTime, default=datetime.utcnow)
        
        # Optional: Die gesamte Webhook-Nachricht als String speichern, falls der Position Handler sie braucht
        raw_signal_text = Column(String) 

        is_active = Column(Boolean, default=False)
        
        
    def init_db():
        """Erstellt die Tabelle, falls sie noch nicht existiert."""
        Base.metadata.create_all(bind=Engine)

    # Funktion, die am besten beim Start der Anwendung aufgerufen wird (z.B. in der main-Sektion)
    init_db()

    @contextlib.contextmanager
    def get_db():
        """Ein Kontextmanager für die Datenbank-Session."""
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()

    logging.info("PostgreSQL Database connection and model setup successful.")
else:
    logging.warning("DATABASE_URL is missing. Database persistence is disabled.")
    get_db = None

@app.get("/")
def read_root():
    """Health Check."""
    return {"status": "Service is running", "mode": "Webhook Bot"}

async def handle_tp_trigger(triggered_order_id, symbol):
    logging.info(f"[TP TRIGGER] Verarbeite Trigger: {triggered_order_id}")
    
    # 1. Find Trade in DB
    trade = await asyncio.to_thread(find_trade_by_tp_id, triggered_order_id)

    if not trade:
        logging.warning("Kein Trade zu diesem TP gefunden.")
        return

    # --- Special Case: SL was hit ---
    if triggered_order_id == trade.sl_order_id:
        logging.info(f"🛑 STOP LOSS hit for Trade {trade.id} ({symbol}).")
        
        # DB Update: SL reached, trade inactive
        updates = {
            "sl_reached": True,
            "is_active": False
        }
        await asyncio.to_thread(update_trade_db_fields, trade.id, updates)
        
        # clean up all TPs
        ids_to_cancel = []
        if trade.tp1_order_id and not trade.tp1_reached: ids_to_cancel.append(trade.tp1_order_id)
        if trade.tp2_order_id and not trade.tp2_reached: ids_to_cancel.append(trade.tp2_order_id)
        if trade.tp3_order_id and not trade.tp3_reached: ids_to_cancel.append(trade.tp3_order_id)
        
        if ids_to_cancel:
            logging.info(f"SL Hit Cleanup: Deleting open TPs {ids_to_cancel}...")
            for oid in ids_to_cancel:
                await cancel_plan_order(symbol, oid)
                await asyncio.sleep(0.1)
        
        return

    # 2. Check which TP was hit
    hit_tp_level = 0
    if triggered_order_id == trade.tp1_order_id:
        hit_tp_level = 1
        logging.info(f"TP1 getroffen (Trade {trade.id}).")
    elif triggered_order_id == trade.tp2_order_id:
        hit_tp_level = 2
        logging.info(f"TP2 getroffen (Trade {trade.id}).")
    elif triggered_order_id == trade.tp3_order_id:
        hit_tp_level = 3
        logging.info(f"TP3 getroffen (Trade {trade.id}).")
    else:
        logging.warning("Getriggerte Order ID passt zu keinem bekannten TP.")
        return

    # 3. Clean up: delete all open orders
    ids_to_cancel = []
    
    if trade.sl_order_id:
        ids_to_cancel.append(trade.sl_order_id)
    
    # If TP1 hit -> delete TP2 and TP3
    if hit_tp_level < 2 and trade.tp2_order_id:
        ids_to_cancel.append(trade.tp2_order_id)
    # If TP1 or TP2 hit -> delete TP3
    if hit_tp_level < 3 and trade.tp3_order_id:
        ids_to_cancel.append(trade.tp3_order_id)
    
    if ids_to_cancel:
        logging.info(f"Räume auf: Lösche alte Orders {ids_to_cancel}...")
        for oid in ids_to_cancel:
            await cancel_plan_order(symbol, oid)
            await asyncio.sleep(0.1)

    await asyncio.sleep(0.5)

    # 4. Aktuellen Status ermitteln
    remaining_size = await get_current_position_size(symbol, trade.position_type)
    logging.info(f"Echte verbleibende Position nach TP{hit_tp_level}: {remaining_size}")

    # Prepare Update dictionary for DB 
    updates = {}
    if hit_tp_level == 1: 
        updates["tp1_reached"] = True
        updates["sl_moved_to_be"] = True
    elif hit_tp_level == 2: 
        updates["tp2_reached"] = True
    elif hit_tp_level == 3: 
        updates["tp3_reached"] = True

    if remaining_size <= 0:
        logging.info("Position ist komplett geschlossen. Keine neuen Orders nötig.")
        await asyncio.to_thread(update_trade_db_fields, trade.id, updates)
        return

    # 5. Get scales for precision
    base_symbol = symbol.replace("_UMCBL", "")
    metadata = await get_symbol_metadata(base_symbol)
    size_scale = metadata.get("sizeScale", 2)

    # 6. Calculate new Stop Loss and set it
    new_sl_price = None
    if hit_tp_level == 1:
        new_sl_price = trade.entry_price # Break Even
    elif hit_tp_level == 2:
        new_sl_price = trade.tp1_price # Trail to TP1
    
    side = "close_long" if trade.position_type.upper() == "LONG" else "close_short"

    if new_sl_price:
        logging.info(f"Setze neuen SL auf {new_sl_price} für Menge {remaining_size}")
        sl_resp = await place_conditional_order(symbol, remaining_size, new_sl_price, side, is_sl=True)
        if sl_resp and "data" in sl_resp:
            updates["sl_order_id"] = sl_resp["data"]["orderId"]
    
    current_pool_size = remaining_size
    
    # -- TP2 ressurection --
    if hit_tp_level < 2 and trade.tp2_price:
        target_tp2_size = float(trade.total_size) * TP_SPLIT_PERCENTAGES[1]
        target_tp2_size = round(target_tp2_size, size_scale)
        
        # Safety Check
        tp2_size = min(target_tp2_size, current_pool_size)
        
        # If there is no TP3, TP2 takes the remaining size
        if not trade.tp3_price:
            tp2_size = current_pool_size

        if tp2_size > 0:
            logging.info(f"Setze TP2 neu: {trade.tp2_price} Menge: {tp2_size}")
            tp2_resp = await place_conditional_order(symbol, tp2_size, trade.tp2_price, side, is_sl=False)
            
            if tp2_resp and "data" in tp2_resp:
                updates["tp2_order_id"] = tp2_resp["data"]["orderId"]
                current_pool_size -= tp2_size

    # -- TP3 ressurection --
    # only if TP1 and TP2 hit and TP3 originally defined
    if hit_tp_level < 3 and trade.tp3_price:
        # TP3 always takes the rest
        tp3_size = round(current_pool_size, size_scale)
        
        if tp3_size > 0:
            logging.info(f"Setze TP3 neu: {trade.tp3_price} Menge: {tp3_size}")
            tp3_resp = await place_conditional_order(symbol, tp3_size, trade.tp3_price, side, is_sl=False)
            
            if tp3_resp and "data" in tp3_resp:
                updates["tp3_order_id"] = tp3_resp["data"]["orderId"]

    # 8. DB Update
    await asyncio.to_thread(update_trade_db_fields, trade.id, updates)
    
def update_trade_db(signal_id: int, update_field: str, new_sl_id: str = None):
    """Aktualisiert den Status eines Trades in der DB."""
    with get_db() as db:
        trade = db.query(TradingSignal).filter(TradingSignal.id == signal_id).first()
        if trade:
            if update_field == "tp1_reached":
                trade.tp1_reached = True
            elif update_field == "tp2_reached":
                trade.tp2_reached = True
            elif update_field == "tp3_reached":
                trade.tp3_reached = True
            elif update_field == "sl_reached":
                trade.sl_reached = True
            
            if new_sl_id:
                trade.sl_order_id = new_sl_id
            
            db.commit()
            logging.info(f"DB Update for Trade {signal_id}: {update_field} marked.")

def update_trade_db_fields(signal_id: int, updates: dict):
    """
    Aktualisiert beliebige Felder eines Trades basierend auf einem Dictionary.
    Beispiel updates: {"sl_order_id": "123", "tp1_reached": True}
    """
    with get_db() as db:
        trade = db.query(TradingSignal).filter(TradingSignal.id == signal_id).first()
        if trade:
            for key, value in updates.items():
                if hasattr(trade, key):
                    setattr(trade, key, value)
                else:
                    logging.warning(f"DB Update Warning: Feld '{key}' existiert nicht im Model.")
            
            db.commit()
            logging.info(f"DB Update for Trade {signal_id}: {list(updates.keys())} updated.")


async def cancel_plan_order(symbol: str, order_id: str):
    """Storniert eine bestehende Plan-Order (SL oder TP)."""
    url_path = "/api/v2/mix/order/cancel-plan-order"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))
    
    
    payload = {
        "symbol": symbol.replace("_UMCBL", ""),
        "productType": "USDT-FUTURES",
        "marginCoin": "USDT",
        "orderId": order_id
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
            data = resp.json()
            if data.get("code") == "00000":
                logging.info(f"Successfully cancelled Plan Order: {order_id}")
                return True
            else:
                logging.error(f"Failed to cancel Plan Order {order_id}: {data}")
                return False
    except Exception as e:
        logging.error(f"Error calling cancel-plan-order: {e}")
        return False

def calculate_remaining_size(trade_signal):
    """
    Berechnet die verbleibende Position für den neuen Stop Loss.
    Bitget V2 Plan-Orders benötigen die exakte Menge.
    """
    total = float(trade_signal.total_size)
    used = 0.0
    if trade_signal.tp1_reached:
        used += float(format(total * TP_SPLIT_PERCENTAGES[0], ".2f"))
    if trade_signal.tp2_reached:
        used += float(format(total * TP_SPLIT_PERCENTAGES[1], ".2f"))
        
    remaining = total - used
    return round(remaining, 4)

def find_trade_by_tp_id(order_id):
    with get_db() as db:
        return db.query(TradingSignal).filter(
            (TradingSignal.tp1_order_id == order_id) |
            (TradingSignal.tp2_order_id == order_id) |
            (TradingSignal.tp3_order_id == order_id) |
            (TradingSignal.sl_order_id == order_id)
        ).first()

# Globale Variable für die Verbindung
ws_client = None

class BitgetWSClient:
    def __init__(self):
        self.url = "wss://ws.bitget.com/v2/ws/private"
        self.api_key = os.getenv("BITGET_API_KEY")
        self.api_secret = os.getenv("BITGET_API_SECRET")
        self.passphrase = os.getenv("BITGET_PASSWORD")
        self.running = False

    def _generate_signature(self, timestamp):
        # Bitget V2 WS Auth Signatur (identisch zu REST, aber simpler Payload)
        message = f"{timestamp}GET/user/verify"
        h = hmac.new(self.api_secret.encode("utf-8"), message.encode("utf-8"), hashlib.sha256)
        return base64.b64encode(h.digest()).decode("utf-8")

    async def connect(self):
        self.running = True
        while self.running:
            try:
                async with websockets.connect(self.url, ping_interval=None) as websocket:
                    logging.info("WebSocket connected.")
                    await self._login(websocket)
                    
                    # Heartbeat Loop & Listen Loop parallel
                    listener = asyncio.create_task(self._listen(websocket))
                    pinger = asyncio.create_task(self._keep_alive(websocket))
                    
                    done, pending = await asyncio.wait(
                        [listener, pinger],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    
                    for task in pending:
                        task.cancel()
                        
            except Exception as e:
                logging.error(f"WebSocket connection failed: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)

    async def _login(self, ws):
        timestamp = str(int(time.time()))
        sign = self._generate_signature(timestamp)
        login_msg = {
            "op": "login",
            "args": [{
                "apiKey": self.api_key,
                "passphrase": self.passphrase,
                "timestamp": timestamp,
                "sign": sign
            }]
        }
        await ws.send(json.dumps(login_msg))
        logging.info("Login request sent.")

    async def _subscribe(self, ws):
        sub_msg = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "USDT-FUTURES",
                    "channel": "orders", 
                    "instId": "default"
                }
            ]
        }
        await ws.send(json.dumps(sub_msg))
        logging.info("Subscribe request sent to orders-algo.")

    async def _keep_alive(self, ws):
        """Sendet 'ping' alle 20 Sekunden"""
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send("ping")
                logging.debug("Ping sent")
            except Exception as e:
                    logging.error(f"Error sending ping: {e}")
                    break

    async def _listen(self, ws):
        async for message in ws:
            if str(message).strip() == "pong":
                logging.debug("Pong received")
                continue

            try:
                data = json.loads(message)
            except json.JSONDecodeError:
                logging.error(f"Could not decode JSON: {message}")
                continue
            
            # 1. Event handling
            event = data.get("event")

            # 2. Check login confirmation
            if event == "login":
                code = data.get("code")
                if code == 0 or code == "00000":
                    logging.info("Bitget Login Confirmed. Sending Subscription now...")
                    await self._subscribe(ws)
                else:
                    logging.error(f"Bitget Login Failed with code: {code}")

            elif event == "subscribe":
                logging.info(f"Subscription confirmed for: {data.get('arg')}")

            elif event == "error":
                logging.error(f"BITGET WS ERROR: {data}")

            # 4. Process push messages
            channel = data.get("arg", {}).get("channel")
            
            if channel == "orders":
                order_data_list = data.get("data", [])
                for order in order_data_list:
                    status = order.get("status")
                    if status == "filled":
                        trigger_id = order.get("clientOid")
                        symbol = order.get("instId")
                        
                        # Wichtig: Prüfen ob trigger_id existiert UND unser Prefix hat (optional, aber sicher)
                        if trigger_id:
                            logging.info(f"ORDER FILLED (Status: {status}). Prüfe TP-Trigger für ID: {trigger_id}")
                            await handle_tp_trigger(trigger_id, symbol)

@app.post("/process_signal")
async def process_router_signal(update: dict):
    msg = update.get("channel_post") or update.get("message") or update.get("edited_channel_post") or update.get("edited_message")
    
    if not msg:
        return {"status": "ignored", "reason": "no_message_content"}
        
    text = msg.get("text", "")

    logging.info(f"Signal received from router: {text[:50]}...")
    
    # Parse signal
    signal = await parse_signal(text)
    if signal:
        signal["raw_text"] = text
        logging.info("Signal recognized: %s", signal)
        asyncio.create_task(place_bitget_trade(signal, test_mode=TEST_MODE))

    return {"status": "ok"}

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

async def get_real_fill_price(symbol: str, order_id: str):
    url = f"{BASE_URL}/api/v2/mix/order/detail"
    params = {
        "symbol": symbol.replace("_UMCBL", ""),
        "productType": "USDT-FUTURES",
        "orderId": order_id
    }
    
    for i in range(3):
        try:
            timestamp = str(int(time.time() * 1000))
            # Signatur erstellen (GET request)
            # Query Params müssen im Request String sein für Signatur bei GET? 
            # Bitget V2 GET Params sind Teil der URL, aber requests lib handled das.
            # Für Signatur bei GET: timestamp + method + path + ?query
            query_string = f"symbol={params['symbol']}&productType={params['productType']}&orderId={params['orderId']}"
            sign_path = f"/api/v2/mix/order/detail?{query_string}"
            
            signature = sign_request("GET", sign_path, timestamp, "")
            
            headers = {
                "ACCESS-KEY": BITGET_API_KEY,
                "ACCESS-SIGN": signature,
                "ACCESS-TIMESTAMP": timestamp,
                "ACCESS-PASSPHRASE": BITGET_PASSWORD,
                "Content-Type": "application/json"
            }
            
            async with httpx.AsyncClient(timeout=5) as client:
                full_url = f"{BASE_URL}{sign_path}"
                resp = await client.get(full_url, headers=headers)
                data = resp.json()
                
                if data.get("code") == "00000" and "data" in data:
                    price_avg = data["data"].get("priceAvg")
                    if price_avg and float(price_avg) > 0:
                        return float(price_avg)
        except Exception as e:
            logging.warning(f"Versuch {i+1} Real Price zu holen fehlgeschlagen: {e}")
        
        await asyncio.sleep(0.4) 
        
    return None

async def get_current_position_size(symbol: str, position_type: str) -> float:
    url = f"{BASE_URL}/api/v2/mix/position/single-position"
    params = {
        "symbol": symbol.replace("_UMCBL", ""),
        "productType": "USDT-FUTURES",
        "marginCoin": "USDT"
    }

    try:
        timestamp = str(int(time.time() * 1000))
        query_string = f"symbol={params['symbol']}&productType={params['productType']}&marginCoin={params['marginCoin']}"
        sign_path = f"/api/v2/mix/position/single-position?{query_string}"
        signature = sign_request("GET", sign_path, timestamp, "")

        headers = {
            "ACCESS-KEY": BITGET_API_KEY,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": BITGET_PASSWORD,
            "Content-Type": "application/json"
        }

        async with httpx.AsyncClient(timeout=5) as client:
            full_url = f"{BASE_URL}{sign_path}"
            resp = await client.get(full_url, headers=headers)
            data = resp.json()
            
            if data.get("code") == "00000" and "data" in data:
                positions = data["data"]
                # Wir suchen die Position, die zu unserem Typ (LONG/SHORT) passt
                target_side = "long" if position_type.lower() == "long" else "short"
                
                for pos in positions:
                    # Bitget V2 gibt holdSide als 'long' oder 'short' zurück
                    if pos.get("holdSide") == target_side:
                        # 'total' ist die Gesamtgröße der Position
                        return float(pos.get("total", 0.0))
                        
    except Exception as e:
        logging.error(f"Fehler beim Abrufen der Positionsgröße: {e}")
    
    return 0.0

async def get_symbol_metadata(base_symbol: str) -> dict:
    global SYMBOL_INFO_CACHE
    
    search_symbol = base_symbol.upper()
    if not search_symbol.endswith("USDT"):
        search_symbol += "USDT"

    if search_symbol in SYMBOL_INFO_CACHE:
        return SYMBOL_INFO_CACHE[search_symbol]

    url = "https://api.bitget.com/api/v2/mix/market/contracts"
    params = {"productType": "USDT-FUTURES"}

    fallback = {
        "sizeScale": 4, 
        "priceScale": 4,
        "maxLimitQty": float('inf'),
        "maxMarketQty": float('inf')
        } 

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status() 
            data = resp.json()

        if data.get("code") != "00000":
            logging.error("API error (get_symbol_precision): %s", data)
            return fallback # Fallback
            
        for item in data["data"]:
            symbol = item["symbol"]
            if not symbol: continue
            if symbol == search_symbol:

                metadata = {
                    "sizeScale": int(item.get("volumePlace", 4)),
                    "priceScale": int(item.get("pricePlace", 4)),
                    "maxLimitQty": float(item.get("maxOrderQty", 1000000)),
                    "maxMarketQty": float(item.get("maxMarketOrderQty", 1000000))
                }
                SYMBOL_INFO_CACHE[base_symbol] = metadata
                SYMBOL_INFO_CACHE[search_symbol] = metadata
                logging.info("[DEBUG] Fetched V2 metadata for %s: sizeScale=%d, priceScale=%d", 
                             base_symbol, metadata["sizeScale"], metadata["priceScale"])
                return metadata
                
        logging.warning("Metadata not found for symbol %s in V2 list. Using fallback.", base_symbol)
        return fallback
        
    except Exception as e:
        logging.error("Error fetching symbol metadata: %s", e)
        return fallback

async def get_quantity_scale(base_symbol: str) -> int:
    """Returns the precision for the position size."""
    metadata = await get_symbol_metadata(base_symbol)
    return metadata.get("sizeScale", 4)

async def get_price_scale(base_symbol: str) -> int:
    """Returns the precision for the price."""
    metadata = await get_symbol_metadata(base_symbol)
    return metadata.get("priceScale", 4)


async def get_position_size(base_symbol: str, usdt_budget: float = None, leverage: float = None, product_type: str = "USDT-FUTURES"):
    usdt_budget = usdt_budget or USDT_BUDGET
    leverage = leverage or DEFAULT_LEVERAGE
    metadata = await get_symbol_metadata(base_symbol)
    size_scale = metadata.get("sizeScale", 4)
    max_market_qty = metadata.get("maxMarketQty")
    max_limit_qty = metadata.get("maxLimitQty")
    
    # Current Price
    price = await get_current_price(base_symbol, product_type)
    total_size_raw_desired = (usdt_budget * leverage) / price
    max_allowed_qty = min(max_market_qty, max_limit_qty)
    total_size_raw_final = min(total_size_raw_desired, max_allowed_qty)

    if total_size_raw_final < total_size_raw_desired:
        logging.warning("Desired size (%.2f) reduced to max allowed size (%.2f) due to exchange limits (Market or Limit Qty).", 
                        total_size_raw_desired, total_size_raw_final, max_allowed_qty)
    
    total_size = round(total_size_raw_final, size_scale)

    if total_size <= 0 and total_size_raw_desired > 0:
        logging.warning("Calculated position size %f was rounded down to 0 or below due to precision or minimum size.", total_size_raw_desired)
    
    tp_split_percentages_raw = TP_SPLIT_PERCENTAGES.copy()

    # Calculate the total percentage covered by the defined TPs
    total_defined_percent = sum([p for p in tp_split_percentages_raw if p > 0])
    remaining_percent = 1.0 - total_defined_percent
    last_valid_tp_index = -1
    for i in range(len(tp_split_percentages_raw) - 1, -1, -1):
        if tp_split_percentages_raw[i] > 0:
            last_valid_tp_index = i
            break

    if last_valid_tp_index != -1 and remaining_percent > 0:
        tp_split_percentages_raw[last_valid_tp_index] += remaining_percent
        logging.info(
            "[TP SPLIT] Defined percentages sum to %.2f%%. Adjusted last defined TP (Index %d) to cover the remaining %.2f%%.",
            total_defined_percent * 100, 
            last_valid_tp_index + 1,
            remaining_percent * 100
        )
    
    tp_sizes_raw = []
    current_remainder = total_size # 'total_size' wurde bereits früher berechnet
        
    for i, percent in enumerate(tp_split_percentages_raw):
        tp_size = 0.0
        
        # Nur für Anteile > 0 eine Größe berechnen
        if percent > 0:
            if i == len(tp_split_percentages_raw) - 1:
                # Für das letzte Element (letzter TP) den Rest der Gesamtgröße verwenden (wegen Rundungsfehlern)
                tp_size = round(current_remainder, size_scale)
            else:
                tp_size_raw = total_size * percent
                tp_size = round(tp_size_raw, size_scale)
                current_remainder -= tp_size
        
        # Wichtig: Wir speichern 0-Größen im Roh-Array, um die Länge von tp_split_percentages_raw beizubehalten
        tp_sizes_raw.append(tp_size)

    tp_sizes = [size for size in tp_sizes_raw if size > 0]

    return total_size, tp_sizes

async def recalculate_tp_sizes(total_size: float, base_symbol: str) -> list[float]:
    """Recalculates the proportional TP split sizes based on a new total position size."""
    metadata = await get_symbol_metadata(base_symbol)
    size_scale = metadata.get("sizeScale", 4)
    tp_split_percentages = TP_SPLIT_PERCENTAGES.copy()

    # The list TP_SPLIT_PERCENTAGES already contains the normalized percentages
    # (e.g., [0.5, 0.3, 0.2]) including the adjustment for the remainder (see get_tp_config).

    tp_sizes_unfiltered = []
    current_remainder = total_size
    
    for i, percent in enumerate(tp_split_percentages):
        tp_size = 0.0
        if percent > 0: 
            if i == len(tp_split_percentages) - 1:
                # Für das letzte Element den Rest der Gesamtgröße verwenden (wg. Rundung)
                tp_size = round(current_remainder, size_scale)
            else:
                tp_size_raw = total_size * percent
                tp_size = round(tp_size_raw, size_scale)
            
            # Subtrahiere die Größe nur, wenn sie berechnet wurde und nicht das letzte Element ist
            if i < len(tp_split_percentages) - 1:
                 current_remainder -= tp_size
                 
        tp_sizes_unfiltered.append(tp_size)
        
    
    # Log the result for debugging/tracking
    logging.info("[RECALC] Recalculated TP sizes based on new total size (%.4f): %s", total_size, tp_sizes_unfiltered)
    
    return tp_sizes_unfiltered


def validate_trade(position_type, stop_loss, take_profits, current_price):
    if not take_profits:
        return False
    if position_type.upper() == "LONG":
        if current_price < stop_loss or current_price > max(take_profits):
            return False
    elif position_type.upper() == "SHORT":
        if current_price > stop_loss or current_price < min(take_profits):
            return False
    return True


# Parser for the signal format
async def parse_signal(text: str):
    
    if "POSITION SIZE" not in text.upper():  # case-insensitive check
        logging.warning("no valid signal")
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
        # Seperates the TP block
        tp_block_match = re.search(r"TAKE PROFIT TARGETS:\s*(.+?)LEVERAGE:", clean_text)
        
        signal_tp_prices = []
        if tp_block_match:
            tp_block = tp_block_match.group(1)
            # Searches in the isolated TP-Block for the TP values
            tp_matches = re.findall(r"TP\d+:\s*([\d.]+)", tp_block)
            signal_tp_prices = [float(tp) for tp in tp_matches if tp]

        # If TP list is empty, reject trade
        if not signal_tp_prices:
            logging.warning("No valid TP found")
            return None

        raw_tp_prices_for_db = signal_tp_prices

        filtered_tp_prices = []
        max_tps_to_check = min(len(signal_tp_prices), len(TP_SPLIT_PERCENTAGES))
        for i in range(max_tps_to_check):
            # Nur Preise übernehmen, wenn der konfigurierte Prozentsatz > 0 ist
            if TP_SPLIT_PERCENTAGES[i] > 0:
                filtered_tp_prices.append(signal_tp_prices[i])

        take_profits = filtered_tp_prices

        # LEVERAGE
        lev_match = re.search(r"LEVERAGE:\s*[xX]?\s*(\d+)", clean_text)
        leverage = int(lev_match.group(1)) if lev_match else None

        logging.info(f"DEBUG: Symbol={base_symbol}, Type={position_type}, Entry={entry_price}, SL={stop_loss}, TPs={take_profits}, Lev={leverage}")

        if not all([base_symbol, position_type, entry_price, stop_loss, leverage]):
            return None  # Required fields missing
              
        
        try:
            tp1 = signal_tp_prices[0]
            # Sicherstellen, dass alle Werte vorhanden sind
            if not entry_price or not tp1 or not leverage:
                logging.warning("Mindestdaten für ROI-Check fehlen.")
                return None

            # ROI Check - min 10% between TP1 and Entry
            if position_type.upper() == "LONG":
                roi_tp1 = ((tp1 - entry_price) / entry_price) * leverage
            else: # SHORT
                roi_tp1 = ((entry_price - tp1) / entry_price) * leverage

            logging.info(f"Check ROI für TP1: {roi_tp1:.2%}")

            # ROI needs to be at least 10%
            if roi_tp1 < 0.10:
                logging.warning(f"Trade rejected: TP1 ROI ({roi_tp1:.2%}) is less than 10%.")
                return None

        except Exception as e:
            logging.error(f"Error in ROI-Check: {e}")
            return None

        current_price = await get_current_price(base_symbol)
        # Calculate position + TP split
        position_size, tp_sizes = await get_position_size(base_symbol, leverage=leverage)
        logging.info(f"[DEBUG] Total position size: {position_size}, TP split sizes: {tp_sizes}")


        if not validate_trade(position_type, stop_loss, take_profits, current_price):
            logging.info(f"[DEBUG] Current={current_price}, SL={stop_loss}, TP_min={min(take_profits)}, TP_max={max(take_profits)}")
            return None  

        return {
            "symbol": trading_symbol,
            "position_size": position_size,
            "tp_sizes": tp_sizes,
            "type": position_type,
            "entry": entry_price,
            "sl": stop_loss,
            "raw_tps": raw_tp_prices_for_db,
            "tps": take_profits,
            "leverage": leverage
        }
    except Exception as e:
        logging.error("Parsing Error:", e)
    
    return None

def sign_request(method, request_path, timestamp, body=""):
    message = f"{timestamp}{method.upper()}{request_path}{body}"
    h = hmac.new(BITGET_API_SECRET.encode(), message.encode(), hashlib.sha256)
    return base64.b64encode(h.digest()).decode()

# Explicitly sets the leverage for the trading pair
async def set_leverage(symbol: str, leverage: int, margin_mode: str = "crossed"):
    url_path = "/api/v2/mix/account/set-leverage"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))
  
    payload = {
        "symbol": symbol,
        "marginCoin": "USDT",
        "leverage": str(leverage),
        "marginMode": margin_mode,
        "productType": "UMCBL",
        "side": "whole" # Sets leverage for LONG and SHORT simultaneously
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
            
            logging.info(f"[SUCCESS] Leverage successfully set to x{leverage} for {symbol}.")
            return True
            
    except httpx.RequestException as e:
        logging.error("[ERROR] Set Leverage Request failed (network/timeout): %s", e)
        return False

async def save_trade_to_db(signal: dict, market_success: bool, sl_success: bool, tp_success_list: list[bool], tp_ids: list, sl_id: str, raw_text: str, is_active: bool):
    """Persistiert die Handelsdaten in der Datenbank."""
    if not get_db:
        return

    try:
        await asyncio.to_thread(_save_trade_sync, signal, market_success, sl_success, tp_success_list, tp_ids, sl_id, raw_text, is_active)
    except Exception as e:
        logging.error("Database save failed: %s", e)

def _save_trade_sync(signal: dict, market_success: bool, sl_success: bool, tp_success_list: list[bool], tp_ids: list, sl_id: str, raw_text: str, is_active: bool):
    """Synchrone Funktion zum Speichern (für asyncio.to_thread)."""
    tp_prices_for_db = signal.get("raw_tps", [])
    with get_db() as db:
        db_signal = TradingSignal(
            user_key=BOT_USER_KEY,
            symbol=signal["symbol"],
            position_type=signal["type"].upper(),
            entry_price=signal["entry"],
            stop_loss=signal["sl"],
            leverage=signal["leverage"],
            total_size=signal["position_size"],
            
            tp1_price=tp_prices_for_db[0] if len(tp_prices_for_db) > 0 else None,
            tp2_price=tp_prices_for_db[1] if len(tp_prices_for_db) > 1 else None,
            tp3_price=tp_prices_for_db[2] if len(tp_prices_for_db) > 2 else None,

            tp1_order_placed=tp_success_list[0] if len(tp_success_list) > 0 else False,
            tp2_order_placed=tp_success_list[1] if len(tp_success_list) > 1 else False,
            tp3_order_placed=tp_success_list[2] if len(tp_success_list) > 2 else False,

            tp1_order_id=tp_ids[0] if len(tp_ids) > 0 else None,
            tp2_order_id=tp_ids[1] if len(tp_ids) > 1 else None,
            tp3_order_id=tp_ids[2] if len(tp_ids) > 2 else None,
            sl_order_id=sl_id,

            market_order_placed=market_success,
            sl_order_placed=sl_success,
            raw_signal_text=raw_text,
            is_active=is_active
        )
        db.add(db_signal)
        db.commit()
        db.refresh(db_signal)
        logging.info("Trade signal successfully saved to DB with ID: %d", db_signal.id)

async def place_market_order(symbol, size, side, leverage=10, retry_count=0):
    if retry_count >= 6:
        logging.error("Market Order after %d tries failed. Trade cancelled.", retry_count)
        return None, None
    
    url_path = "/api/v2/mix/order/place-order"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    if side == "open_long":
        v2_side = "buy"  # Kaufen, um Long zu eröffnen
    elif side == "open_short":
        v2_side = "sell" # Verkaufen, um Short zu eröffnen
    else:
        logging.error("Invalid side passed to place_market_order: %s", side)
        return None, None

    payload = {
        "symbol": symbol.replace("_UMCBL", ""),
        "size": str(size),
        "side": v2_side,
        "tradeSide": "open",
        "orderType": "market",
        "marginMode": "crossed",
        "marginCoin": "USDT",
        "productType": "UMCBL"
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
        # Use httpx.AsyncClient for non-blocking requests
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(url, headers=headers, data=body)
        
        if resp.status_code >= 400:
            error_data = resp.json()
            error_code = error_data.get("code")
            error_msg = error_data.get("msg", "")

            # --- DIRECT LIMIT EXTRACTION FOR SIZE ERRORS 45133---
            new_size = None
            
            # Error code 45133: Exceeded the maximum quantity of contract orders: XXXXX ZK
            if error_code == "45133": 
                # RegEx: Extract a number group (with or without comma/dot) from the string
                limit_match = re.search(r"contract orders: ([\d,.]+)", error_msg)
                
                if limit_match:
                    # Clean the string (remove commas) and try to parse the float
                    limit_str = limit_match.group(1).replace(",", "")
                    try:
                        # Set the new size to the limit reported by the server
                        max_allowed_size = float(limit_str) 
                        
                        # Use 99.9% of the reported limit to avoid rounding issues
                        new_size_raw = max_allowed_size * 0.999 
                        
                        base_symbol = symbol.replace("_UMCBL", "")
                        metadata = await get_symbol_metadata(base_symbol)
                        size_scale = metadata.get("sizeScale", 0) 
                        new_size = round(new_size_raw, size_scale)
                        
                        logging.warning(
                            "[LIMIT FIXED 45133] Desired size (%.2f) exceeded limit. Setting order size to max allowed (%.2f).", 
                            size, new_size
                        )

                    except ValueError:
                        logging.error("Failed to parse numeric limit from error message: %s. Falling back to 20%% reduction.", limit_str)
                        # Fallback if parsing fails: Reduce by 10%
                        new_size = round(size * 0.80, size_scale)

            # Error code 40921: Position-level limit (use the 10% reduction fallback)
            elif error_code == "40921":
                # Get the precision (Scale) for correct rounding
                base_symbol = symbol.replace("_UMCBL", "")
                metadata = await get_symbol_metadata(base_symbol)
                size_scale = metadata.get("sizeScale", 0) 
                
                new_size_raw = size * 0.80
                new_size = round(new_size_raw, size_scale)
                
                logging.warning(
                    "[RETRY 40921] Size exceeds positions-level-limit. Trying with reduced order size (5%%): %s", 
                    new_size
                )
            
            # Check if we calculated a new size (i.e., error was 45133 or 40921)
            if new_size is not None and new_size > 0:
                 return await place_market_order(symbol, new_size, side, leverage, retry_count + 1)
            
            # Otherwise: Log the error and cancel
            logging.error("[ERROR 4xx/5xx MARKET ORDER] Bitget API Status Code %d (Code: %s). Response Body: %s (Payload: %s)", 
                          resp.status_code, error_code, error_data, payload)
            return None, None


        data = resp.json() 
        if data.get("code") != "00000":
            logging.error("[ERROR] Bitget API response (Status 200, but Code != 00000): %s", data)
            return None, None
        return data, size
    
    except httpx.RequestException as e:
        # Catches network errors or timeouts
        logging.error("[ERROR] Market Order Request failed (network/timeout): %s", e)
        return None
    
async def place_conditional_order(symbol, size, trigger_price, side: str, is_sl: bool):
    url_path = "/api/v2/mix/order/place-plan-order"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))
    prefix = "sl" if is_sl else "tp"
    client_oid = f"{prefix}_{int(time.time() * 1000)}_{os.urandom(2).hex()}"
    if side == "close_long":
        v2_side = "buy"
    elif side == "close_short":
        v2_side = "sell"
    else:
        logging.error("Invalid side for Conditional Order: %s. Expected 'close_long' or 'close_short'.", side)
        return None

    base_symbol = symbol.replace("_UMCBL", "")
    metadata = await get_symbol_metadata(base_symbol)
    size_scale = metadata.get("sizeScale", 4)
    price_scale = metadata.get("priceScale", 4)
    formatted_size = format(float(size), f".{size_scale}f")
    formatted_trigger_price = format(float(trigger_price), f".{price_scale}f")

    if is_sl:
        order_type = "market"
        execution_price = None 
    else:
        # Take-Profit: OrderType ist LIMIT
        order_type = "limit"
        # Bei Limit Orders ist der Ausführungspreis gleich dem Trigger-Preis
        execution_price = formatted_trigger_price

    payload = {
        "symbol": base_symbol,
        "productType": "UMCBL",
        "marginMode": "isolated",
        "marginCoin": "USDT",
        "size": formatted_size,
        "side": v2_side, 
        "orderType": order_type,
        "planType": "normal_plan",
        "tradeSide": "close",
        "reduceOnly": "yes",
        "triggerPrice": formatted_trigger_price,
        "triggerType": "mark_price",
        "clientOid": client_oid
    }

    if execution_price is not None:
         payload["price"] = str(execution_price)
    
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
            data = resp.json()
            
            if data.get("code") == "00000":
                return data
            else:
                logging.error(f"[ERROR] API Response: {data}")
                return None

    except Exception as e:
        logging.error(f"[ERROR] Request failed: {e}")
        return None

    
async def place_bitget_trade(signal, test_mode=True):
    symbol = signal["symbol"]
    initial_position_size = signal["position_size"]
    tp_sizes = signal["tp_sizes"]
    leverage = signal["leverage"]
    sl_price = signal["sl"]
    tp_prices = signal["tps"]

    market_success = False
    sl_success = False
    tp_success_list = []

    if signal["type"].upper() == "LONG":
        market_side_open = "open_long"
        closing_side = "close_long"  # Closes LONG position
    else: # SHORT
        market_side_open = "open_short"
        closing_side = "close_short"   # Closes SHORT position
    
    if test_mode:
        logging.info("[TEST MODE] Market Order, SL and TP Orders will not be sent")
        return

    # Set explicit Leverage for pair
    if leverage:
        base_symbol_for_leverage = symbol.replace("_UMCBL", "")
        leverage_set = await set_leverage(symbol=base_symbol_for_leverage, leverage=leverage, margin_mode="isolated")
        if not leverage_set:
            logging.error("Could not set leverage. Trade will be aborted.")
            return

    num_splits = len(tp_sizes)
    if len(tp_prices) > num_splits:
        logging.warning("More TP prices found in signal (%d) than defined splits in .env (%d). Ignoring the extra TP prices.", 
                        len(tp_prices), num_splits)
        tp_prices = tp_prices[:num_splits]
    elif len(tp_prices) < num_splits:
        logging.error("Too few TP prices found in signal (%d) for the defined splits in .env (%d). The remaining position size will not be protected by a TP. Aborting TP orders.",
                      len(tp_prices), num_splits)
        return

    # --- 1. Place Market Order ---
    market_order_resp, final_position_size = await place_market_order(symbol, initial_position_size, side=market_side_open, leverage=leverage)
    logging.info("Market Order Response: %s, Final Position Size: %s", market_order_resp, final_position_size)

    if not market_order_resp or final_position_size is None or final_position_size <= 0:
        logging.error("Could not place Market Order or final size is zero/invalid. SL/TP Orders will be aborted.")
        await save_trade_to_db(signal, market_success=False, sl_success=False, tp_success_list=[], raw_text=signal.get("raw_text", "N/A"), is_active=False)
        return
    
    market_success = True
    total_size = float(final_position_size)
    signal["position_size"] = final_position_size
    base_symbol = symbol.replace("_UMCBL", "")
    final_tp_sizes = await recalculate_tp_sizes(final_position_size, base_symbol)
    signal["tp_sizes"] = final_tp_sizes 

    entry_price = signal["entry"]
    real_entry_price = None
    if market_order_resp and "data" in market_order_resp:
        market_order_id = market_order_resp["data"].get("orderId")
        logging.info(f"Market Order platziert: {market_order_id}")

        if market_order_id:
            real_entry_price = await get_real_fill_price(symbol, market_order_id)

    if real_entry_price:
        logging.info(f"Überschreibe Signal-Entry {entry_price} mit echtem Fill: {real_entry_price}")
        signal["entry"] = real_entry_price

    full_sizes = final_tp_sizes
    all_tp_data = [] 
    price_list_index = 0
    
    for i in range(len(TP_SPLIT_PERCENTAGES)):
        size = full_sizes[i]
        price = None

        if size > 0 and price_list_index < len(tp_prices):
            price = tp_prices[price_list_index] 
            price_list_index += 1
            
        all_tp_data.append((price, size))

    num_splits = len(final_tp_sizes)
    if not final_tp_sizes:
         logging.warning("Recalculated TP sizes list is empty. No TP orders will be placed.")
    
    if len(tp_prices) > num_splits:
        # Hier wird final_tp_sizes verwendet, das bereits durch die Neuberechnung angepasst ist
        logging.warning("More TP prices found in signal (%d) than defined splits in .env (%d). Ignoring the extra TP prices.", 
                         len(tp_prices), num_splits)
        tp_prices = tp_prices[:num_splits]
    elif len(tp_prices) < num_splits:
        # Dieser Fall sollte nach der Neuberechnung nicht auftreten, da die TP-Größenliste
        # immer die Länge der TP-Definitionen aus der .env hat. Wir loggen es trotzdem.
        logging.error("Too few TP prices found in signal (%d) for the defined splits (%d). This is unexpected.",
                       len(tp_prices), num_splits)
        # Wir fahren fort, um die verfügbaren TP-Orders zu platzieren

    # Wait for order execution
    logging.info("[INFO] Waiting 2 seconds (non-blocking) for safety.")
    await asyncio.sleep(2.0)

    # --- 2. Stop-Loss Order ---
    sl_resp = await place_conditional_order(
        symbol=symbol,
        size=final_position_size, 
        trigger_price=sl_price,
        side=closing_side,
        is_sl=True # Marks as SL -> orderType="market"
    )
    logging.info("Stop-Loss Plan Order Response: %s", sl_resp)
    sl_order_id = None
    if sl_resp and "data" in sl_resp:
        sl_success = True
        sl_order_id = sl_resp["data"].get("orderId")

    # --- 3. Take-Profit Orders ---
    metadata = await get_symbol_metadata(symbol.replace("_UMCBL", ""))
    size_scale = metadata.get("sizeScale", 2)

    tp_ids = [None, None, None]
    tp_success_list = []
    accumulated_tp_size = 0.0  # Hier tracken wir, was schon verplant wurde
    
    num_tps_to_set = len(tp_prices)
    
    for i in range(num_tps_to_set):
        tp_price = tp_prices[i]
        
        if i == num_tps_to_set - 1:
            # Der letzte TP nimmt den exakten Rest der Gesamtposition
            tp_size = float(total_size) - accumulated_tp_size
        else:
            # Zwischen-TPs nach Prozenten berechnen
            raw_size = float(total_size) * TP_SPLIT_PERCENTAGES[i]
            # Sofort auf die richtige Scale formatieren
            tp_size = float(format(raw_size, f".{size_scale}f"))
        
        # Sicherstellen, dass wir keine negativen Werte durch Rundung bekommen
        tp_size = max(0, float(format(tp_size, f".{size_scale}f")))
        accumulated_tp_size += tp_size

        if tp_size <= 0 or tp_price is None:
            tp_success_list.append(False)
            logging.info(f"TP{i+1} übersprungen (Größe 0 oder kein Preis)")
            continue
        
        tp_resp = await place_conditional_order(
            symbol=symbol,
            size=tp_size,
            trigger_price=tp_price,
            side=closing_side,
            is_sl=False # Marks as TP -> orderType="limit"
        )
        if tp_resp and "data" in tp_resp:
            tp_ids[i] = tp_resp["data"].get("orderId")

        success = bool(tp_resp)
        tp_success_list.append(success)
        logging.info(f"TP{i+1} Plan order (Price: {tp_price}, Size: {tp_size}) Response: %s", tp_resp)
    
    
    await save_trade_to_db(
        signal, 
        market_success=market_success, 
        sl_success=sl_success, 
        tp_success_list=tp_success_list,
        tp_ids=tp_ids,      
        sl_id=sl_order_id,
        raw_text=signal.get("raw_text", "N/A"),
        is_active=True
    )

    logging.info("[INFO] All orders (Market, SL, 3xTP) have been sent.")

@app.on_event("startup")
async def startup_event():
    # DB Init
    init_db()
    
    # Start WebSocket Monitor
    global ws_client
    ws_client = BitgetWSClient()
    asyncio.create_task(ws_client.connect())

@app.on_event("shutdown")
async def shutdown_event():
    if ws_client:
        ws_client.running = False