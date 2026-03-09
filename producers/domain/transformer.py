import json
import time
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class TradeTransformer:
    @staticmethod
    def transform(message: str) -> Optional[Dict[str, Any]]:
        """
        Parses the raw WebSocket message from Binance and transforms it 
        into a clean dictionary for Kafka.
        """
        try:
            data = json.loads(message)
            
            if 'e' in data and data['e'] == 'trade':
                return {
                    'symbol': data['s'],              # Symbol
                    'price': float(data['p']),        # Price
                    'quantity': float(data['q']),     # Quantity
                    'event_time': data['T'],          # Trade time (ms)
                    'processing_time': int(time.time() * 1000) # Ingest time (ms)
                }
            return None
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON message")
            return None
        except KeyError as e:
            logger.error(f"Missing key in message: {e}")
            return None
        except Exception as e:
            logger.error(f"Error transforming message: {e}")
            return None
