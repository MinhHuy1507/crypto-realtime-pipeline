{{
    config(
        materialized='incremental',
        unique_key='daily_candle_id'
    )
}}

WITH daily_candles AS (
    SELECT * FROM {{ ref('int_crypto__daily_candles') }}
    {% if is_incremental() %}
    WHERE trade_date >= (SELECT MAX(trade_date) FROM {{ this }})
    {% endif %}
),
crypto_symbols AS (
    SELECT * FROM {{ ref('dim_crypto__symbols') }}
),
-- Calculate rolling metrics needed for performance calculations
-- These include past prices (1d, 7d, 30d), average volume (7d), 
-- historical volatilities (30d) and average intraday volatility (30d)
calc_rolling_metrics AS (
    SELECT
        *,
        LAG(daily_close, 1) OVER (PARTITION BY symbol ORDER BY trade_date) AS close_price_1d_ago,
        LAG(daily_close, 7) OVER (PARTITION BY symbol ORDER BY trade_date) AS close_price_7d_ago,
        LAG(daily_close, 30) OVER (PARTITION BY symbol ORDER BY trade_date) AS close_price_30d_ago,
        
        AVG(daily_volume) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_volume_7d,
        STDDEV(daily_close) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS volatility_30d,
        AVG(intraday_volatility) OVER (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_intraday_volatility_30d
    FROM daily_candles
),
final_metrics AS (
    SELECT
        *,
        CASE 
            WHEN close_price_1d_ago > 0 THEN (daily_close - close_price_1d_ago) / close_price_1d_ago 
            ELSE NULL 
        END AS return_1d,
        CASE 
            WHEN close_price_7d_ago > 0 THEN (daily_close - close_price_7d_ago) / close_price_7d_ago 
            ELSE NULL 
        END AS return_7d,
        CASE 
            WHEN close_price_30d_ago > 0 THEN (daily_close - close_price_30d_ago) / close_price_30d_ago 
            ELSE NULL 
        END AS return_30d
    FROM calc_rolling_metrics
),

joined AS (
    SELECT
        m.daily_candle_id,
        s.symbol_code as symbol,
        s.base_asset_name,
        s.sector,
        
        m.trade_date,
        m.daily_open,
        m.daily_close,
        m.daily_high,
        m.daily_low,
        m.daily_volume,
        m.daily_trades,
        
        -- Performance Metrics
        m.return_1d,
        m.return_7d,
        m.return_30d,
        
        -- Risk Metrics (Interday Volatility Risk, Intraday Volatility Risk)
        m.volatility_30d AS risk_interday_volatility,
        m.avg_intraday_volatility_30d AS risk_intraday_volatility,
        
        -- Liquidity Metrics
        m.avg_volume_7d AS liquidity_avg_volume_7d
        
    FROM final_metrics m
    JOIN crypto_symbols s ON m.symbol = s.symbol_code
)

SELECT * FROM joined