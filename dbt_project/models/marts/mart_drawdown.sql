WITH prices AS (
    SELECT date, ticker, close FROM {{ ref('stg_prices') }}
),
rolling_high AS (
    SELECT 
        date, 
        ticker, 
        close,
        -- Trailing 1-year (252 trading day) peak
        MAX(close) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS rolling_peak
    FROM prices
),
drawdown_calc AS (
    SELECT 
        date,
        ticker,
        -- Underwater percentage
        (close - rolling_peak) / NULLIF(rolling_peak, 0) AS drawdown_pct
    FROM rolling_high
)
SELECT 
    *,
    -- Count of consecutive days where price is below the peak
    -- (A simple way to proxy recovery period or days underwater)
    SUM(CASE WHEN drawdown_pct < 0 THEN 1 ELSE 0 END) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS days_underwater_1yr
FROM drawdown_calc
