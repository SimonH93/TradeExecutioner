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
    # 1. Trade in DB finden
    # Wir müssen prüfen, ob diese order_id eine TP1, TP2 oder TP3 ID in unserer DB ist
    
    # Hier nutzen wir sync DB access in einem Thread, um async nicht zu blockieren
    trade_signal = await asyncio.to_thread(find_trade_by_tp_id, triggered_order_id)
    
    if not trade_signal:
        return # Nicht unsere Order oder schon erledigt

    current_sl_id = trade_signal.sl_order_id
    new_sl_price = None
    update_field = None

    # Logik: Welcher TP war es?
    if trade_signal.tp1_order_id == triggered_order_id:
        logging.info(f"TP1 reached for Trade {trade_signal.id}")
        new_sl_price = trade_signal.entry_price # Breakeven
        update_field = "tp1_reached"
        
    elif trade_signal.tp2_order_id == triggered_order_id:
        logging.info(f"TP2 reached for Trade {trade_signal.id}")
        new_sl_price = trade_signal.tp1_price # SL auf TP1
        update_field = "tp2_reached"
        
    # SL Verschieben Logik
    if new_sl_price and current_sl_id:
        # 1. Alten SL stornieren
        cancel_success = await cancel_plan_order(symbol, current_sl_id)
        
        if cancel_success:
            # 2. Neuen SL setzen
            # Achtung: side muss "close_long" (sell) oder "close_short" (buy) sein
            side = "close_long" if trade_signal.position_type == "LONG" else "close_short"
            
            # Wir müssen die REST-Size wissen. Entweder aus DB tracken oder Position abfragen.
            # Vereinfacht: Wir nehmen die aktuelle Size aus der DB (total_size minus bereits ausgeführte TPs)
            # Eine sicherere Methode ist, die offene Position via API abzufragen (get_position_details).
            
            # Für dieses Beispiel nehmen wir an, wir setzen SL für die Restmenge
            # (Das erfordert etwas mehr Logik zur Berechnung der Restmenge)
            remaining_size = calculate_remaining_size(trade_signal) 
            
            sl_resp = await place_conditional_order(
                symbol=trade_signal.symbol,
                size=remaining_size,
                trigger_price=new_sl_price,
                side=side,
                is_sl=True
            )
            
            if sl_resp and sl_resp.get("data", {}).get("orderId"):
                new_sl_id = sl_resp["data"]["orderId"]
                # DB Update: Alter SL weg, neuer SL da, TP Status update
                await asyncio.to_thread(update_trade_db, trade_signal.id, update_field, new_sl_id)

def update_trade_db(signal_id: int, update_field: str, new_sl_id: str = None):
    """Aktualisiert den Status eines Trades in der DB."""
    with get_db() as db:
        trade = db.query(TradingSignal).filter(TradingSignal.id == signal_id).first()
        if trade:
            if update_field == "tp1_reached":
                trade.tp1_reached = True
            elif update_field == "tp2_reached":
                trade.tp2_reached = True
            elif update_field == "sl_reached":
                trade.sl_reached = True
            
            if new_sl_id:
                trade.sl_order_id = new_sl_id
            
            db.commit()
            logging.info(f"DB Update for Trade {signal_id}: {update_field} marked.")

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
    # Summiere alle TPs, die noch nicht erreicht wurden
    remaining = trade_signal.total_size
    if trade_signal.tp1_reached:
        remaining -= (trade_signal.total_size * TP_SPLIT_PERCENTAGES[0])
    if trade_signal.tp2_reached:
        remaining -= (trade_signal.total_size * TP_SPLIT_PERCENTAGES[1])
    return round(remaining, 4)

def find_trade_by_tp_id(order_id):
    with get_db() as db:
        return db.query(TradingSignal).filter(
            (TradingSignal.tp1_order_id == order_id) |
            (TradingSignal.tp2_order_id == order_id) |
            (TradingSignal.tp3_order_id == order_id)
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
                async with websockets.connect(self.url) as websocket:
                    logging.info("WebSocket connected.")
                    await self._login(websocket)
                    await self._subscribe(websocket)
                    
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

    async def _subscribe(self, ws):
        # Wir abonnieren 'orders-algo' für Plan Orders (TP/SL)
        sub_msg = {
            "op": "subscribe",
            "args": [
                {
                    "instType": "USDT-FUTURES",
                    "channel": "orders-algo",
                    "instId": "default" # 'default' = alle Paare
                }
            ]
        }
        await ws.send(json.dumps(sub_msg))
        logging.info("Subscribed to orders-algo.")

    async def _keep_alive(self, ws):
        """Sendet 'ping' alle 30 Sekunden"""
        while True:
            await asyncio.sleep(30)
            await ws.send("ping")

    async def _listen(self, ws):
        async for message in ws:
            if message == "pong":
                continue
            
            data = json.loads(message)
            
            # Prüfen auf Push-Daten
            if data.get("action") == "push" and data.get("arg", {}).get("channel") == "orders-algo":
                await self._handle_order_update(data["data"])

    async def _handle_order_update(self, update_list):
        # Hier passiert die Magie
        for order in update_list:
            # Wichtige Felder laut Bitget V2 API Docs für Plan Orders:
            # orderId: Die ID der Plan Order
            # status: "live", "executed" (getriggert), "fail", "cancel"
            # planType: "normal_plan" (nutzt du), "profit_plan", "loss_plan"
            
            order_id = order.get("orderId")
            status = order.get("status")
            symbol = order.get("instId") # z.B. BTCUSDT
            
            if status == "executed":
                with get_db() as db:
                    is_sl = db.query(TradingSignal).filter(TradingSignal.sl_order_id == order_id).first()
                    if is_sl:
                        update_trade_db(is_sl.id, "sl_reached")
                        logging.info(f"STOP LOSS reached for Trade {is_sl.id}. Monitoring stopped.")
                        continue 
                logging.info(f"Plan Order {order_id} triggered/executed on {symbol}!")
                await handle_tp_trigger(order_id, symbol) # Implementierung siehe unten


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

async def get_symbol_metadata(base_symbol: str) -> dict:
    global SYMBOL_INFO_CACHE
    
    if base_symbol in SYMBOL_INFO_CACHE:
        return SYMBOL_INFO_CACHE[base_symbol]

    url = f"{BASE_URL}/api/v3/market/instruments" 
    params = {"category": "USDT-FUTURES"}

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
            return 4 # Fallback
            
        for symbol_info in data["data"]:
            symbol = symbol_info["symbol"]
            if not symbol: continue
            if symbol == base_symbol: 
                size_scale_raw = symbol_info.get("quantityPrecision")
                price_scale_raw = symbol_info.get("pricePrecision")
                max_order_qty_raw = symbol_info.get("maxOrderQty")
                max_market_qty_raw = symbol_info.get("maxMarketOrderQty")

                if size_scale_raw is None or price_scale_raw is None:
                    logging.warning("Precision (quantityPrecision or pricePrecision) not found for %s. Defaulting to 4/4.", base_symbol)
                    return fallback
                else:
                    metadata = {
                        "sizeScale": int(size_scale_raw),
                        "priceScale": int(price_scale_raw)
                    }

                if max_order_qty_raw is not None:
                    metadata["maxLimitQty"] = float(max_order_qty_raw)
                if max_market_qty_raw is not None:
                    metadata["maxMarketQty"] = float(max_market_qty_raw)
                metadata.setdefault("maxLimitQty", fallback["maxLimitQty"])
                metadata.setdefault("maxMarketQty", fallback["maxMarketQty"])

                SYMBOL_INFO_CACHE[base_symbol] = metadata
                logging.info("[DEBUG] Fetched metadata for %s: quantityPrecision=%d, pricePrecision=%d, maxLimitQty=%f, maxMarketQty=%f", 
                             base_symbol, 
                             metadata["sizeScale"], 
                             metadata["priceScale"], 
                             metadata["maxLimitQty"], 
                             metadata["maxMarketQty"])
                return metadata
                
        logging.warning("Metadata not found for symbol %s in market instruments list. Defaulting to fallback.", base_symbol)
        return fallback
        
    except Exception as e:
        logging.error("Error fetching symbol precision: %s", e)
        return 4 # Fallback for errors

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
        await asyncio.to_thread(_save_trade_sync, signal, market_success, sl_success, tp_success_list, raw_text, is_active)
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
    if side == "close_long":
        v2_side = "buy"
    elif side == "close_short":
        v2_side = "sell"
    else:
        logging.error("Invalid side for Conditional Order: %s. Expected 'close_long' or 'close_short'.", side)
        return None

    base_symbol = symbol.replace("_UMCBL", "")
    original_price_scale = await get_price_scale(base_symbol)
    max_allowed_scale = 4
    trigger_price_scale = min(original_price_scale, max_allowed_scale)
    rounded_trigger_price = round(trigger_price, min(original_price_scale, 4))
    
    if is_sl:
        # Stop-Loss: OrderType ist MARKET
        order_type = "market"
        # Bei Market Orders wird kein price (Ausführungspreis) im Payload gesendet.
        execution_price = None 
        logging.info(f"[SL-MARKET] Trigger Price: {rounded_trigger_price}. Execution: MARKET.")
    else:
        # Take-Profit: OrderType ist LIMIT
        order_type = "limit"
        # Bei Limit Orders ist der Ausführungspreis gleich dem Trigger-Preis
        execution_price = rounded_trigger_price 
        logging.info(f"[TP-LIMIT] Trigger Price: {rounded_trigger_price}. Execution Price: {execution_price}.")
    

    logging.info(f"[DEBUG] Applying price rounding: Original={trigger_price}, Instrument Scale={original_price_scale}, Trigger Scale ={trigger_price_scale}, Rounded={rounded_trigger_price}")


    payload = {
        "symbol": base_symbol,
        "productType": "UMCBL",
        "marginMode": "isolated",
        "marginCoin": "USDT",
        "size": str(size),
        "side": v2_side, 
        "orderType": order_type,
        "planType": "normal_plan",
        "tradeSide": "close",
        "reduceOnly": "yes",
        "triggerPrice": str(rounded_trigger_price),
        "triggerType": "mark_price",
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
            
            if resp.status_code >= 400:
                try:
                    error_data = resp.json()
                    logging.error("[ERROR 4xx/5xx CONDITIONAL ORDER] Bitget API Status Code %d. Response Body: %s (Payload: %s)", 
                                  resp.status_code, error_data, payload)
                except json.JSONDecodeError:
                    logging.error("[ERROR 4xx/5xx CONDITIONAL ORDER] Bitget API Status Code %d. Response Text: %s (Payload: %s)", 
                                  resp.status_code, resp.text, payload)
                return None

            # Status 200/300
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
    signal["position_size"] = final_position_size
    base_symbol = symbol.replace("_UMCBL", "")
    final_tp_sizes = await recalculate_tp_sizes(final_position_size, base_symbol)
    signal["tp_sizes"] = final_tp_sizes 

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
        sl_order_id = sl_resp["data"]["orderId"]

    # --- 3. Take-Profit Orders ---
    tp_ids = [None, None, None]
    for i, (tp_price, tp_size) in enumerate(all_tp_data):
        if tp_size <= 0 or tp_price is None:
            tp_success_list.append(False)
            logging.info(f"TP{i+1} skipped (Size: {tp_size}, Price: {tp_price}). Success set to False.")
            continue
        
        tp_resp = await place_conditional_order(
            symbol=symbol,
            size=tp_size,
            trigger_price=tp_price,
            side=closing_side,
            is_sl=False # Marks as TP -> orderType="limit"
        )
        if tp_resp and "data" in tp_resp:
            tp_ids[i] = tp_resp["data"]["orderId"]

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