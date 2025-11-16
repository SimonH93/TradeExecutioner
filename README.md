# 🤖 Telegram-Bitget Futures Bot

A lightweight Python-based trading bot that listens for real-time trading signals from specified Telegram channels/threads and automatically executes market orders (including leverage, stop-loss, and take-profit) on the **Bitget Futures (USDT-M)** platform.

## ✨ Key Features

* **Telegram Integration:** Securely monitors private/public Telegram channels and specific message threads using the Telethon library (as a user client).
* **Signal Parsing:** Parses standardized signal messages (containing `PAIR`, `TYPE`, `ENTRY`, `SL`, `TPx`, `LEVERAGE`) to extract trade parameters.
* **Automated Trade Execution:** Places instant **Market Orders** on Bitget upon receiving a valid signal.
* **Risk Management:** Automatically calculates and submits **conditional Stop-Loss (SL)** and **multiple Take-Profit (TP)** orders (up to 3 splits) immediately after the market entry.
* **Position Sizing:** Dynamically calculates the position size based on a predefined **USDT budget** and the signal's requested **leverage**.
* **Exchange Limit Handling:** Fetches and applies minimum/maximum quantity and price precision rules from the Bitget API to ensure compliant order submission.
* **Robust Deployment:** Built using FastAPI for a health check endpoint (ideal for platforms like Railway) and uses non-blocking `asyncio` for concurrent operation.

---

## ⚙️ Setup and Configuration

The application requires specific credentials and settings to run. All configurations are managed via environment variables (e.g., in a `.env` file).

### 1. Environment Variables

| Variable | Description | Required | Example |
| :--- | :--- | :--- | :--- |
| `TELEGRAM_API_ID` | Your Telegram API ID. | Yes | `1234567` |
| `TELEGRAM_API_HASH` | Your Telegram API Hash. | Yes | `a1b2c3d4e5f6g7h8i9j0...` |
| `TELEGRAM_SESSION_PART1` | Base64-encoded part of your Telethon session file. **Required for non-interactive server deployment.** | Yes | `AQIDBAUGBwkTEx...` |
| `TELEGRAM_SOURCE_CHANNELS` | Comma-separated list of Chat IDs (e.g., `-1001234567890`) or specific Chat ID:Thread ID combinations. | Yes | `-100123,-100456:55` |
| `BITGET_API_KEY` | Your Bitget Futures API Key. | Yes | `bg_a1b2c3d4e5f6g7h8` |
| `BITGET_API_SECRET` | Your Bitget Futures API Secret. | Yes | `ZGlzIGlzIG15IHNlY3JldA==` |
| `BITGET_PASSWORD` | Your Bitget API Passphrase (required for V1/V2/V3 endpoints). | Yes | `my-secure-pass` |
| `BITGET_USDT_SIZE` | The capital (in USDT) to allocate per trade. | No | `25.5` |
| `BITGET_LEVERAGE` | Default leverage if not specified in the signal. | No | `5` |
| `BITGET_TEST_MODE` | Set to `True` to disable actual API calls to Bitget. | No | `True` |

### 2. Telegram Session Note

Since this is a non-interactive environment, you **must** obtain the Telegram session file (`bot_session.session`) by running the client locally first. This binary file must then be converted to a **Base64 string** (and split into parts if necessary, e.g., `TELEGRAM_SESSION_PART1`, `TELEGRAM_SESSION_PART2`, etc.) and provided via environment variables.

---

### 3. Running the Application

This application runs as an asynchronous Python server using FastAPI and Uvicorn.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run the server (Uvicorn)
uvicorn main:app --host 0.0.0.0 --port 8000