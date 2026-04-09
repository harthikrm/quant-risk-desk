WITH raw_macro AS (
    SELECT *
    FROM {{ source('raw', 'macro_indicators') }}
),

pivoted AS (
    SELECT
        date::date AS date,
        MAX(CASE WHEN series_id = 'DFF' THEN value END) AS fed_funds_rate,
        MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi_level,
        MAX(CASE WHEN series_id = 'VIXCLS' THEN value END) AS vix_level,
        MAX(CASE WHEN series_id = 'T10Y2Y' THEN value END) AS yield_curve_10y_2y,
        MAX(CASE WHEN series_id = 'BAMLH0A0HYM2' THEN value END) AS high_yield_spread,
        MAX(CASE WHEN series_id = 'USREC' THEN value END) AS recession_indicator
    FROM raw_macro
    GROUP BY date
),

macro_ffill AS (
    -- Forward fill missing macro values because CPI is monthly, but we want daily joins
    SELECT
        date,
        -- using last_value with ignore nulls is currently complex in raw pg,
        -- alternative approach: simple subquery or just leave nulls and handle in intermediate.
        -- We will keep them null and forward handle it in the intermediate logic where we have date spines.
        fed_funds_rate,
        cpi_level,
        vix_level,
        yield_curve_10y_2y,
        high_yield_spread,
        recession_indicator
    FROM pivoted
)

SELECT * FROM macro_ffill
ORDER BY date
