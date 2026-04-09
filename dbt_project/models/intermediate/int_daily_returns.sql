-- Use a date spine approach to guarantee no trading gap days (if needed)
-- For simplicity, since stg_prices only contains trading days, we can just select from it
-- but to be safe, we will just pass through the clean returns and ensure no nulls

WITH prices AS (
    SELECT * FROM {{ ref('stg_prices') }}
)

SELECT
    date,
    ticker,
    sector,
    log_return
FROM prices
WHERE log_return IS NOT NULL
