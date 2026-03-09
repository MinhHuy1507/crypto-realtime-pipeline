{{
    config(
        materialized='incremental',
        unique_key='daily_candle_id'
    )
}}

-- Retrieves all candles since the start of the day for the latest date available in the database.
WITH continuous_candles AS (
    SELECT * FROM {{ ref('int_crypto__continuous_candles') }}
    {% if is_incremental() %}
    WHERE window_start >= (SELECT DATE_TRUNC('day', MAX(trade_date)) FROM {{ this }})
    {% endif %}
),

-- Compute daily aggregates: High, Low, Volume, Trades
agg_stats AS (
    SELECT
        symbol,
        trade_date,
        MAX(highest_price) as daily_high,
        MIN(lowest_price) as daily_low,
        SUM(trade_volume) as daily_volume,
        SUM(total_trades) as daily_trades
    FROM continuous_candles
    GROUP BY 1, 2
),
-- Compute daily open and close prices
window_prices AS (
    SELECT DISTINCT ON (symbol, trade_date)
        symbol,
        trade_date,
        
        FIRST_VALUE(open_price) OVER (
            PARTITION BY symbol, trade_date 
            ORDER BY window_start ASC
        ) as daily_open,
        
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol, trade_date 
            ORDER BY window_start DESC
        ) as daily_close
    FROM continuous_candles
),
-- Join and calculate additional metrics (intraday volatility)
joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['p.symbol', 'p.trade_date']) }} AS daily_candle_id,
        p.symbol,
        p.trade_date,
        p.daily_open,
        s.daily_high,
        s.daily_low,
        p.daily_close,
        s.daily_volume,
        s.daily_trades,
        
        -- Intraday Volatility formula: (High - Low) / Low
        CASE 
            WHEN s.daily_low > 0 THEN (s.daily_high - s.daily_low) / s.daily_low
            ELSE 0 
        END AS intraday_volatility
        
    FROM window_prices p
    JOIN agg_stats s 
        ON p.symbol = s.symbol 
        AND p.trade_date = s.trade_date
)

SELECT * FROM joined