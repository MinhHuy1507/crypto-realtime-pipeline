{{
    config(
        materialized='incremental',
        unique_key='candle_id'
    )
}}

WITH crypto_symbols AS (
    SELECT * FROM {{ ref('dim_crypto__symbols') }}
),

technical_indicators AS (
    SELECT * FROM {{ ref('int_crypto__technical_indicators') }}
    {% if is_incremental() %}
    WHERE window_start > (SELECT MAX(window_start) - INTERVAL '200 minutes' FROM {{ this }})
    {% endif %}
),

joined AS (
    SELECT
        ti.*,
        cs.base_asset_name,
        cs.quote_asset,
        cs.exchange,
        cs.sector
    FROM technical_indicators ti
    LEFT JOIN crypto_symbols cs
        ON ti.symbol = cs.symbol_code
),

business_metrics AS (
    SELECT
        *,
        -- RSI Signal (Momentum)
        CASE 
            WHEN rsi_14 < 30 THEN 'oversold'
            WHEN rsi_14 > 70 THEN 'overbought'
            ELSE 'neutral'
        END AS signal_rsi,
        
        -- SMA (Trend Direction)
        CASE 
            WHEN sma_5 > sma_20 THEN 'uptrend'
            WHEN sma_5 < sma_20 THEN 'downtrend'
            ELSE 'neutral'
        END AS trend_direction,
        
        -- Strategy Signal
        CASE 
            -- Buy when uptrend + oversold
            WHEN sma_5 > sma_20 AND rsi_14 < 30 THEN 'strong_buy'
            -- Sell when downtrend + overbought
            WHEN sma_5 < sma_20 AND rsi_14 > 70 THEN 'strong_sell'
            ELSE 'hold'
        END AS trade_recommendation
    FROM joined
)

SELECT 
    -- IDs & Metadata
    candle_id,
    symbol,
    base_asset_name,
    quote_asset,
    exchange,
    sector,
    
    -- Time
    trade_date,
    window_start,
    window_end,
    
    -- Price Data (OHLCV)
    open_price,
    highest_price,
    lowest_price,
    close_price,
    trade_volume,
    total_trades,
    is_gap_filled,
    
    -- Indicators
    sma_5,
    sma_20,
    rsi_14,
    
    -- Signals (Business Logic)
    signal_rsi,
    trend_direction,
    trade_recommendation

FROM business_metrics