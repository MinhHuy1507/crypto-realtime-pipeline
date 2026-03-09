"""Unit tests for TradeTransformer — validates parsing of Binance WebSocket messages."""
import json
import pytest
from unittest.mock import patch
from producers.domain.transformer import TradeTransformer


# Sample valid Binance trade message
VALID_TRADE_MSG = json.dumps({
    "e": "trade",
    "E": 1672531200000,
    "s": "BTCUSDT",
    "t": 123456,
    "p": "16800.50",
    "q": "0.001",
    "T": 1672531200000,
    "m": True,
    "M": True
})


class TestTradeTransformer:
    def test_valid_trade_message(self):
        result = TradeTransformer.transform(VALID_TRADE_MSG)

        assert result is not None
        assert result["symbol"] == "BTCUSDT"
        assert result["price"] == 16800.50
        assert result["quantity"] == 0.001
        assert result["event_time"] == 1672531200000
        assert "processing_time" in result
        assert isinstance(result["processing_time"], int)

    def test_non_trade_event_returns_none(self):
        msg = json.dumps({"e": "aggTrade", "s": "BTCUSDT"})
        assert TradeTransformer.transform(msg) is None

    def test_missing_event_type_returns_none(self):
        msg = json.dumps({"s": "BTCUSDT", "p": "100", "q": "1", "T": 123})
        assert TradeTransformer.transform(msg) is None

    def test_invalid_json_returns_none(self):
        assert TradeTransformer.transform("not json") is None

    def test_missing_required_field_returns_none(self):
        msg = json.dumps({"e": "trade", "s": "BTCUSDT", "p": "100"})  # missing 'q' and 'T'
        assert TradeTransformer.transform(msg) is None

    def test_empty_message_returns_none(self):
        assert TradeTransformer.transform("") is None

    def test_price_converted_to_float(self):
        result = TradeTransformer.transform(VALID_TRADE_MSG)
        assert isinstance(result["price"], float)

    def test_quantity_converted_to_float(self):
        result = TradeTransformer.transform(VALID_TRADE_MSG)
        assert isinstance(result["quantity"], float)

    def test_processing_time_is_recent(self):
        """processing_time should be within the last few seconds."""
        import time
        result = TradeTransformer.transform(VALID_TRADE_MSG)
        now_ms = int(time.time() * 1000)
        assert abs(now_ms - result["processing_time"]) < 5000
