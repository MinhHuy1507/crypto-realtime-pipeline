-- depends_on: {{ ref('crypto_symbols') }}
WITH seed_data AS (
    SELECT * FROM {{ ref('crypto_symbols') }}
),
final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['symbol_code']) }} AS symbol_id,
        
        symbol_code,
        base_asset_name,
        quote_asset,
        exchange,
        sector
    FROM seed_data
)

SELECT * FROM final