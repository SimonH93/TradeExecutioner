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
    raise ValueError("At least one required API credential is missing in .env")

# Reads TP percentages from .env (e.g., TP1_PERCENT=50, TP2_PERCENT=30, etc.)
def get_tp_config():
    """Dynamically reads TP percentages from environment variables."""
    tp_percentages = []
    i = 1
    while True:
        key = f"TP{i}_PERCENT"
        percent_str = os.getenv(key)
        if percent_str is None:
            break
        try:
            percent = float(percent_str)
            if percent <= 0:
                logging.warning(f"TP{i}_PERCENT is set to zero or less. Ignoring.")
            else:
                tp_percentages.append(percent / 100.0)
        except ValueError:
            logging.error(f"Invalid value for {key}: {percent_str}. Must be a number.")
        i += 1
    
    # Fallback to a default 100% split if no TPs are defined
    if not tp_percentages:
        logging.warning("No TP percentages defined in .env (e.g., TP1_PERCENT). Defaulting to 100%% split at TP1.")
        return [1.0] # 100% at TP1
        
    return tp_percentages

TP_SPLIT_PERCENTAGES = get_tp_config()

@app.get("/")
def read_root():
    """Health Check."""
    return {"status": "Service is running", "mode": "Webhook Bot"}


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
        logging.info("Signal recognized: %s", signal)
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
    
    tp_split_percentages = TP_SPLIT_PERCENTAGES.copy()

    # Calculate the total percentage covered by the defined TPs
    total_defined_percent = sum(tp_split_percentages)
    
    # Calculate the difference from 100%
    remaining_percent = 1.0 - total_defined_percent
    
    # Adjust the last TP percentage to cover the total remaining size
    if tp_split_percentages:
        tp_split_percentages[-1] += remaining_percent
        logging.info(
            "[TP SPLIT] Defined percentages sum to %.2f%%. Adjusted last TP (%.2f%%) to cover the remaining %.2f%%.",
            total_defined_percent * 100, 
            tp_split_percentages[-1] * 100,
            remaining_percent * 100
        )
    
    # Calculate sizes based on the adjusted percentages
    tp_sizes = []
    current_remainder = total_size
    
    for i, percent in enumerate(tp_split_percentages):
        if i == len(tp_split_percentages) - 1:
            # For the last element, use the remainder of the total size 
            # to prevent cumulative rounding errors from fractional parts
            tp_size = round(current_remainder, size_scale)
        else:
            tp_size_raw = total_size * percent
            tp_size = round(tp_size_raw, size_scale)
            current_remainder -= tp_size
        
        # Ensure size is not negative due to over-rounding
        if tp_size > 0:
            tp_sizes.append(tp_size)
    
    # The sum of all rounded sizes may not exactly equal the rounded total_size,
    # but the split logic ensures the full amount is covered across the TPs.

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
        tp_matches = re.findall(r"TP\d+:\s*([\d.]+)", clean_text)
        take_profits = [float(tp) for tp in tp_matches if tp]
        # If TP list is empty, reject trade
        if not take_profits:
            logging.warning("No valid TP found")
            return None

        # LEVERAGE
        lev_match = re.search(r"LEVERAGE:\s*x(\d+)", clean_text)
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
            
            logging.info(f"[SUCCESS] Leverage successfully set to x{leverage} for {symbol}.")
            return True
            
    except httpx.RequestException as e:
        logging.error("[ERROR] Set Leverage Request failed (network/timeout): %s", e)
        return False

async def place_market_order(symbol, size, side, leverage=10, retry_count=0):
    if retry_count >= 6:
        logging.error("Market Order after %d tries failed. Trade cancelled.", retry_count)
        return None
    
    url_path = "/api/v2/mix/order/place-order"
    url = f"{BASE_URL}{url_path}"
    timestamp = str(int(time.time() * 1000))

    if side == "open_long":
        v2_side = "buy"  # Kaufen, um Long zu eröffnen
    elif side == "open_short":
        v2_side = "sell" # Verkaufen, um Short zu eröffnen
    else:
        logging.error("Invalid side passed to place_market_order: %s", side)
        return None

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
    rounded_price = round(trigger_price, min(await get_price_scale(base_symbol), 4))
    order_type = "limit"

    logging.info(f"[DEBUG] Applying price rounding: Original={trigger_price}, Instrument Scale={original_price_scale}, Trigger Scale ={trigger_price_scale}, Rounded={rounded_price}")


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
        "price": str(rounded_price),
        "triggerPrice": str(rounded_price),
        "triggerType": "mark_price",
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
    market_order_resp = await place_market_order(symbol, position_size, side=market_side_open, leverage=leverage)
    logging.info("Market Order Response: %s", market_order_resp)

    if not market_order_resp:
        logging.error("Could not place Market Order. SL/TP Orders will be aborted.")
        return
    
    # Wait for order execution
    logging.info("[INFO] Waiting 2 seconds (non-blocking) for safety.")
    await asyncio.sleep(2.0)

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
        
    logging.info("[INFO] All orders (Market, SL, 3xTP) have been sent.")