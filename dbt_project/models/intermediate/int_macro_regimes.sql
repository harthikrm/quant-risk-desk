WITH macro AS (
    SELECT * FROM {{ ref('stg_macro') }}
),

lagged_macro AS (
    SELECT
        *,
        -- ~252 trading days is approx 1 year
        LAG(cpi_level, 252) OVER (ORDER BY date) AS cpi_1yr_ago,
        LAG(fed_funds_rate, 21) OVER (ORDER BY date) AS ff_1mo_ago
    FROM macro
),

computed_macro AS (
    SELECT
        *,
        ((cpi_level - cpi_1yr_ago) / NULLIF(cpi_1yr_ago, 0)) * 100 AS cpi_yoy,
        (fed_funds_rate - ff_1mo_ago) AS ff_change
    FROM lagged_macro
)

SELECT
    date,
    fed_funds_rate,
    cpi_level,
    cpi_yoy,
    vix_level,
    yield_curve_10y_2y,
    high_yield_spread,
    recession_indicator,
    CASE
        WHEN recession_indicator = 1 THEN 'Recession'
        WHEN cpi_yoy > 6.0 AND ff_change > 0 THEN 'Inflation Shock'
        WHEN vix_level > 30 OR yield_curve_10y_2y <= 0 THEN 'Risk-Off'
        WHEN vix_level < 20 AND yield_curve_10y_2y > 0 THEN 'Risk-On'
        ELSE 'Normal'
    END AS regime_label
FROM computed_macro
