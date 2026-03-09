{{
    config(
        materialized='incremental',
        unique_key='candle_id'
    )
}}

with continuous_candles AS (
    SELECT * FROM {{ ref('int_crypto__continuous_candles') }}
    {% if is_incremental() %}
    WHERE window_start > (SELECT MAX(window_start) - INTERVAL '300 minutes' FROM {{ this }})
    {% endif %}
),
-- calculate difference between current close price and previous close price
calc_price_diff AS (
    SELECT
        *,
        LAG(close_price) OVER(PARTITION BY symbol ORDER BY window_start) AS prev_price,
        close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY window_start) AS diff
    FROM continuous_candles
),
-- calculate gains and losses
calc_price_changes AS (
    SELECT
        *,
        CASE WHEN diff > 0 THEN diff ELSE 0 END AS gain,
        CASE WHEN diff < 0 THEN ABS(diff) ELSE 0 END AS loss
    FROM calc_price_diff
),
-- calculate rolling averages for SMA and RSI
calc_rolling_averages AS (
    SELECT
        *,
        AVG(close_price) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sma_5,
        AVG(close_price) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY window_start ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
    FROM calc_price_changes
),
-- calculate RSI
final_calc AS (
    SELECT
        *,
        CASE WHEN avg_loss_14 = 0 THEN 100 ELSE 100 - (100 / (1 + avg_gain_14 / avg_loss_14)) END AS rsi_14
    FROM calc_rolling_averages
)

SELECT *
FROM final_calc