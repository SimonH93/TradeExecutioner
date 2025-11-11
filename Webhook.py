from fastapi import FastAPI, Request
app = FastAPI()

@app.post("/tg_webhook")
async def tg_webhook(req: Request):
    data = await req.json()               # Telegram update
    if not is_valid_sender(data):         # whitelist
        return {"ok": True}
    signal = parse_signal(data)
    if not quick_validate(signal):
        notify_admin("invalid signal")
        return {"ok": True}
    enqueue_signal(signal)                # push to Redis queue (Huey/RQ)
    return {"ok": True}
