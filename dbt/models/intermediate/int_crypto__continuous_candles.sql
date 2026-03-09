{{
    config(
        materialized='incremental',
        unique_key='candle_id' 
    )
}}

-- Retrieves incremental data from the staging (bronze) layer.
-- Includes a 10-minute buffer to mitigate data loss caused by ingestion latency.
WITH source AS (
    SELECT * FROM {{ ref('stg_binance__candles') }}
    {% if is_incremental() %}
    WHERE start_time > (SELECT MAX(window_start) - INTERVAL '10 minutes' FROM {{ this }})
    {% endif %}
),

-- Find the global time range across all symbols
get_global_time_range AS (
    SELECT 
        MIN(start_time) as min_timestamp,
        MAX(end_time) as max_timestamp
    FROM source
),

-- Create a time spine (1-minute intervals)
generate_minutes_spine AS (
    SELECT
        generate_series(min_timestamp, max_timestamp, interval '1 minute') as minute
    FROM get_global_time_range
),

-- Get list of coins and their first seen timestamp
get_unique_symbols AS (
    SELECT
        symbol,
        MIN(start_time) as first_seen_at
    FROM source
    GROUP BY symbol
),

-- Create grid (Symbol x Time)
-- Only create timeline starting from when the coin first appeared (Optimization)
build_complete_grid AS (
    SELECT
        s.symbol,
        m.minute as window_start,
        m.minute + interval '1 minute' as window_end
    FROM generate_minutes_spine m
    CROSS JOIN get_unique_symbols s
    WHERE m.minute >= s.first_seen_at
),

-- Merge raw data into the grid
join_raw_data AS (
    SELECT
        -- Check if this row is gap-filled or real
        raw.candle_id IS NULL as is_gap_filled, 
        grid.symbol,
        grid.window_start,
        grid.window_end,
        raw.open_price,
        raw.highest_price,
        raw.lowest_price,
        raw.close_price,
        
        COALESCE(raw.trade_volume, 0) as trade_volume,
        COALESCE(raw.total_trades, 0) as total_trades
    FROM build_complete_grid grid
    LEFT JOIN source raw
        ON grid.symbol = raw.symbol
        AND grid.window_start = raw.start_time
),

-- FORWARD FILL LOGIC
-- Goal: If price is NULL, use the price from the most recent valid row.
-- Method: Create a partition group (value_partition) based on the count of observed close_prices.
prepare_forward_fill AS (
    SELECT
        *,
        COUNT(close_price) OVER (PARTITION BY symbol ORDER BY window_start) as value_partition
    FROM join_raw_data
),
apply_forward_fill AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['symbol', 'window_start']) }} AS candle_id,
        symbol,
        DATE_TRUNC('day', window_start) AS trade_date,
        window_start,
        window_end,
        is_gap_filled,
        
        COALESCE(open_price, FIRST_VALUE(close_price) OVER (PARTITION BY symbol, value_partition ORDER BY window_start)) as open_price,
        COALESCE(highest_price, FIRST_VALUE(close_price) OVER (PARTITION BY symbol, value_partition ORDER BY window_start)) as highest_price,
        COALESCE(lowest_price, FIRST_VALUE(close_price) OVER (PARTITION BY symbol, value_partition ORDER BY window_start)) as lowest_price,
        COALESCE(close_price, FIRST_VALUE(close_price) OVER (PARTITION BY symbol, value_partition ORDER BY window_start)) as close_price,
        
        trade_volume,
        total_trades
    FROM prepare_forward_fill
)

SELECT
    *
FROM apply_forward_fill