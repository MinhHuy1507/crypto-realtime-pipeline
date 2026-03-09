/*
Problem: 
- With each late event arrival, the entire window is recalculated, leading to duplicate data. 
- We need a way to keep only the latest record for each (symbol, window_start) pair.

Solution:
- Use ROW_NUMBER partitioned by the key columns (symbol, window_start) and ordered by the (updated_at) 
in descending order to identify the most recent record. Then filter to keep only the first row.
*/

WITH source AS (
    SELECT * FROM {{ source('raw_layer', 'candles_log') }}
),
renamed AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['symbol', 'window_start']) }} AS candle_id,
        
        batch_id AS batch_number,
        symbol,
        window_start AS start_time,
        window_end AS end_time,
        open AS open_price,
        high AS highest_price,
        low AS lowest_price,
        close AS close_price,
        volume AS trade_volume,
        num_trades AS total_trades,
        updated_at AS last_updated,

        -- Deduplication technique: Assign row numbers.
        -- The newest row (with the largest updated_at) will have row_num = 1
        ROW_NUMBER() OVER (
            PARTITION BY symbol, window_start 
            ORDER BY updated_at DESC, batch_id DESC
        ) as row_num

    FROM source
)

-- Only take the newest row
SELECT *
FROM renamed
WHERE row_num = 1