import asyncio
import base64
import hashlib
import hmac
import json
import logging
import uvicorn
import os
import re
import time

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Request, HTTPException

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

app = FastAPI()
load_dotenv()

# Cache for symbol information (Precision, Min Size, etc.)
SYMBOL_INFO_CACHE = {} 

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
    raise ValueError("Mindestens ein benötigtes API Credential fehlt in .env")

@app.get("/")
def read_root():
    """Health Check."""
    return {"status": "Service is running", "mode": "Webhook Bot"}

# NEU: Empfängt den HTTP POST vom Router-Service
@app.post("/process_signal")
async def process_router_signal(update: dict):
    # Wichtig: Die Autorisierung ist jetzt implizit, da nur der Router senden soll
    # Ihre Autorisierungs- und Filterlogik muss hier eventuell entfernt/vereinfacht werden.
    
    # 1. Die Nachricht aus dem Update holen (genau wie im Router)
    msg = update.get("channel_post") or update.get("message") or update.get("edited_channel_post") or update.get("edited_message")
    
    if not msg:
        return {"status": "ignored", "reason": "no_message_content"}
        
    text = msg.get("text", "")

    logging.info(f"Signal vom Router empfangen: {text[:50]}...")
    
    # 2. Den Signal-Prozess starten
    signal = await parse_signal(text)
    if signal:
        logging.info("Signal erkannt: %s", signal)
        asyncio.create_task(place_bitget_trade(signal, test_mode=TEST_MODE))

    return {"status": "ok"}

# --- START THE APPLICATION ---

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
    
    tp_sizes_raw_splits = [
        total_size * 0.5,  # TP1 = 50%
        total_size * 0.3,  # TP2 = 30%
    ]
    tp_sizes = [round(s, size_scale) for s in tp_sizes_raw_splits]

    tp3_size_raw = total_size - sum(tp_sizes)
    tp3_size = round(tp3_size_raw, size_scale)

    if tp3_size > 0:
        tp_sizes.append(tp3_size)
    else:
        if len(tp_sizes) > 0:
            tp_sizes[-1] = round(tp_sizes[-1] + tp3_size_raw, size_scale)
    
    return total_size, tp_sizes



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
        # If TP list is empty, reject trade
        if not take_profits:
            print("No valid TP found")
            return None

        # LEVERAGE
        lev_match = re.search(r"LEVERAGE:\s*x(\d+)", clean_text)
        leverage = int(lev_match.group(1)) if lev_match else None

        print("DEBUG:", base_symbol, position_type, entry_price, stop_loss, take_profits, leverage)


        if not all([base_symbol, position_type, entry_price, stop_loss, leverage]):
            return None  # Required fields missing
        
        current_price = await get_current_price(base_symbol)
        # Calculate position + TP split
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

# Explicitly sets the leverage for the trading pair
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
            
            logging.info(f"[SUCCESS] Leverage erfolgreich auf x{leverage} für {symbol} gesetzt.")
            return True
            
    except httpx.RequestException as e:
        logging.error("[ERROR] Set Leverage Request failed (network/timeout): %s", e)
        return False

async def place_market_order(symbol, size, side, leverage=10, retry_count=0):
    if retry_count >= 3:
        logging.error("Markt Order after %d tries failed. Trade cancelled.", retry_count)
        return None
    
    url_path = "/api/mix/v1/order/placeOrder"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    payload = {
        "symbol": symbol,
        "size": str(size),
        "side": side,             # "buy" oder "sell"
        "orderType": "market",
        "marginMode": "isolated",  # Isolated margin mode
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
         # Use httpx.AsyncClient for non-blocking requests
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.post(url, headers=headers, data=body)
        
        if resp.status_code >= 400:
            error_data = resp.json()
            if error_data.get("code") == "40921":
                
                # Strategy: Reduce size in 5% steps and try again
                new_size_raw = size * 0.95 
                # Wichtig: Holen Sie die Präzision (Scale) für die korrekte Rundung
                base_symbol = symbol.replace("_UMCBL", "")
                metadata = await get_symbol_metadata(base_symbol)
                size_scale = metadata.get("sizeScale", 0) 
                
                new_size = round(new_size_raw, size_scale)
                
                logging.warning(
                    "[RETRY 40921] Size %s exceeds positions-level-limit. Try with reduced order size (%.2f%%): %s", 
                    size, 
                    (1 - 0.95) * 100,
                    new_size
                )
                # Recursive call with smaller size
                return await place_market_order(symbol, new_size, side, leverage, retry_count + 1)
                
            else:
                # Check other 4xx/5xx errors
                logging.error("[ERROR 4xx/5xx MARKET ORDER] Bitget API Status Code %d. Response Body: %s (Payload: %s)", 
                              resp.status_code, error_data, payload)
                return None

        data = resp.json() 
        if data.get("code") != "00000":
            logging.error("[ERROR] Bitget API response (Status 200, but Code != 00000): %s", data)
            return None
        return data
    except httpx.RequestException as e:
        # Catches network errors or timeouts
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


    # SL should be Market, TP should be Limit
    order_type = "market" if is_sl else "limit"
    if is_sl:
        # SL must be a Market order to ensure execution.
        order_type = "market" 
        entrust_price = "0" # Or omit, but 0 is safer
        logging.info("[DEBUG] Conditional Order: Setting SL as Market Order.")
    else:
        # TP is a Limit Order with the TP price as the limit price
        order_type = "limit"
        entrust_price = str(rounded_price) # DThe price at which the order should be executed (limit price)
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
        "executePrice": entrust_price
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
    position_size = signal["position_size"]
    tp_sizes = signal["tp_sizes"]
    leverage = signal["leverage"]
    sl_price = signal["sl"]
    tp_prices = signal["tps"]

    if signal["type"].upper() == "LONG":
        market_side_open = "open_long"
        closing_side = "close_long"  # Closes LONG position
    else: # SHORT
        market_side_open = "open_short"
        closing_side = "close_short"   # Closes SHORT position
    
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


    # --- 1. Place Market Order ---
    market_order_resp = await place_market_order(symbol, position_size, side=market_side_open, leverage=leverage)
    logging.info("Market Order Response: %s", market_order_resp)

    if not market_order_resp:
        logging.error("Konnte Market Order nicht platzieren. SL/TP Orders werden abgebrochen.")
        return
    
    # Wait for order execution
    logging.info("[INFO] Warte 3.0 Sekunden (nicht-blockierend) zur Sicherheit.")
    await asyncio.sleep(3.0)

    # --- 2. Stop-Loss Order ---
    sl_resp = await place_conditional_order(
        symbol=symbol,
        size=position_size, 
        trigger_price=sl_price,
        side=closing_side,
        is_sl=True # Marks as SL -> orderType="market"
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
            is_sl=False # Marks as TP -> orderType="limit"
        )
        logging.info(f"TP{i+1} Plan order (Price: {tp_price}, Size: {tp_size}) Response: %s", tp_resp)
        
    logging.info("[INFO] Alle Orders (Market, SL, 3xTP) wurden gesendet.")