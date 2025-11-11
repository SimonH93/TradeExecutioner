from fastapi import FastAPI, Request
import re

app = FastAPI()

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

# Parser für dein Signalformat
def parse_signal(text: str):
    try:
        # PAIR
        pair_match = re.search(r"PAIR:\s*(\w+/\w+)", text)
        pair = pair_match.group(1) if pair_match else None
        symbol = pair.replace("/", "") if pair else None  # Bitget Format

        # POSITION SIZE
        size_match = re.search(r"POSITION SIZE:\s*(\d+\s*-\s*\d+%)", text)
        position_size = size_match.group(1) if size_match else None

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

        # Prüfen, ob mindestens die wichtigsten Felder vorhanden sind
        if all([pair, position_size, position_type, entry_price, stop_loss, leverage]):
            return {
                "pair": pair,
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
