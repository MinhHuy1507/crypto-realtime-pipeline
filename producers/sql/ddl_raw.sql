-- candles_log table to store raw candle data
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.candles_log (
    batch_id INT,
    symbol VARCHAR(20),
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT,
    num_trades INT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index to optimize queries on symbol and window_start
CREATE INDEX IF NOT EXISTS idx_candles_symbol_time ON raw.candles_log (batch_id, symbol, window_start, window_end);

-- candles_latest view to see the candle movement of the coin in each window. 
-- Each window will update 6 times (1 window is 1 minute and each batch is 10s, so there are 6 candle movements in 1 window).
CREATE OR REPLACE VIEW raw.candles_latest AS
SELECT DISTINCT ON (batch_id, symbol, window_start)
    batch_id,
    symbol,
    window_start,
    window_end,
    open,
    high,
    low,
    close,
    volume,
    num_trades,
    updated_at
FROM raw.candles_log;