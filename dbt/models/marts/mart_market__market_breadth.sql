{{
    config(
        materialized='incremental',
        unique_key='window_start'
    )
}}

WITH technical_indicators AS (
    SELECT * FROM {{ ref('int_crypto__technical_indicators') }}
    {% if is_incremental() %}
    WHERE window_start > (SELECT MAX(window_start) FROM {{ this }})
    {% endif %}
),
-- Aggregate market-level metrics
-- This includes liquidity, BTC dominance, market breadth, sentiment.
market_aggregation AS (
    SELECT
        trade_date,
        window_start,
        window_end,
        COUNT(DISTINCT symbol) AS active_coins,
        
        -- Total Market Liquidity
        SUM(trade_volume * close_price) AS total_market_volume_usdt,
        SUM(total_trades) AS total_market_trades,
        
        -- BTC Volume Dominance
        SUM(CASE WHEN symbol = 'BTCUSDT' THEN (trade_volume * close_price) ELSE 0 END) AS btc_volume_usdt,
        
        -- Market Sentiment (Market Psychology): Average RSI of all coins
        AVG(rsi_14) AS avg_market_rsi,
        
        -- Market Breadth (Market Width)
        SUM(CASE WHEN close_price > sma_20 THEN 1 ELSE 0 END) AS coins_uptrend_count,
        SUM(CASE WHEN rsi_14 > 70 THEN 1 ELSE 0 END) AS coins_overbought_count,
        SUM(CASE WHEN rsi_14 < 30 THEN 1 ELSE 0 END) AS coins_oversold_count

    FROM technical_indicators
    GROUP BY trade_date, window_start, window_end
),
-- Final selection with calculated metrics
final_metrics AS (
    SELECT
        trade_date,
        window_start,
        window_end,
        active_coins,
        total_market_volume_usdt AS total_market_volume,
        total_market_trades,
        avg_market_rsi,
        
        -- BTC Dominance
        CASE 
            WHEN total_market_volume_usdt > 0 THEN (btc_volume_usdt / total_market_volume_usdt) 
            ELSE 0 
        END AS btc_volume_dominance_pct,
        
        -- Market Breadth
        CASE 
            WHEN active_coins > 0 THEN (coins_uptrend_count::FLOAT / active_coins) 
            ELSE 0 
        END AS market_breadth_pct,
        
        -- Market Sentiment State
        CASE
            WHEN avg_market_rsi > 70 THEN 'Extreme Greed'
            WHEN avg_market_rsi > 60 THEN 'Greed'
            WHEN avg_market_rsi < 30 THEN 'Extreme Fear'
            WHEN avg_market_rsi < 40 THEN 'Fear'
            ELSE 'Neutral'
        END AS market_sentiment_state
        
    FROM market_aggregation
)

SELECT * FROM final_metrics