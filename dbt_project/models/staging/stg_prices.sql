WITH raw_prices AS (
    SELECT *
    FROM {{ source('raw', 'prices') }}
)

SELECT
    date::date AS date,
    ticker,
    -- Add sector logic mapping from original brief
    CASE 
        WHEN ticker = 'XLB' THEN 'Materials'
        WHEN ticker = 'XLC' THEN 'Communication Services'
        WHEN ticker = 'XLE' THEN 'Energy'
        WHEN ticker = 'XLF' THEN 'Financials'
        WHEN ticker = 'XLI' THEN 'Industrials'
        WHEN ticker = 'XLK' THEN 'Technology'
        WHEN ticker = 'XLP' THEN 'Consumer Staples'
        WHEN ticker = 'XLRE' THEN 'Real Estate'
        WHEN ticker = 'XLU' THEN 'Utilities'
        WHEN ticker = 'XLV' THEN 'Health Care'
        WHEN ticker = 'XLY' THEN 'Consumer Discretionary'
        WHEN ticker = 'SPY' THEN 'Benchmark S&P 500'
        WHEN ticker = 'IEF' THEN 'Treasury Default'
        WHEN ticker = 'IRX' THEN 'Risk Free Rate'
        ELSE 'Unknown'
    END AS sector,
    close,
    volume,
    -- Compute daily return safely avoiding ln() on negative or zero prices
    CASE 
        WHEN close > 0 AND lag(close) OVER (PARTITION BY ticker ORDER BY date) > 0 
        THEN ln(close / lag(close) OVER (PARTITION BY ticker ORDER BY date))
        ELSE NULL
    END AS log_return
FROM raw_prices
WHERE date IS NOT NULL
