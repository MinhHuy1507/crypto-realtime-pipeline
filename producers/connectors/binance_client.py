import websocket
import logging
import time
from typing import Callable

logger = logging.getLogger(__name__)

# Binance Settings
SYMBOLS = [
    'btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'adausdt', 
    'xrpusdt', 'dogeusdt', 'dotusdt', 'avaxusdt', 'polusdt'
]
streams = '/'.join([f"{s}@trade" for s in SYMBOLS])
BINANCE_WS_URL=f"wss://stream.binance.com:9443/ws/{streams}"

class BinanceClient:
    def __init__(self, on_message_callback: Callable[[str], None]):
        self.url = BINANCE_WS_URL
        self.on_message_callback = on_message_callback
        self.ws = None

    def on_error(self, ws, error):
        logger.error(f"WebSocket Error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        logger.warning("### WebSocket Closed ###")

    def on_open(self, ws):
        logger.info(f"Connected to Binance WebSocket! Listening for: {SYMBOLS}")

    def _on_message(self, ws, message):
        self.on_message_callback(message)

    def start(self):
        while True:
            try:
                self.ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self.on_open,
                    on_message=self._on_message,
                    on_error=self.on_error,
                    on_close=self.on_close
                )
                self.ws.run_forever()
            except KeyboardInterrupt:
                logger.info("Stopping Binance Client...")
                break
            except Exception as e:
                logger.error(f"Connection failed: {e}. Retrying in 5s...")
                time.sleep(5)
