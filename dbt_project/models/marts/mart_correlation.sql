WITH returns AS (
    SELECT date, ticker, log_return 
    FROM {{ ref('int_daily_returns') }}
    -- Filter to only the 11 base sectors to keep the matrix clean
    WHERE ticker NOT IN ('SPY', 'IEF', 'IRX')
),
paired AS (
    SELECT 
        r1.date,
        r1.ticker AS etf_1,
        r2.ticker AS etf_2,
        r1.log_return AS ret_1,
        r2.log_return AS ret_2
    FROM returns r1
    JOIN returns r2 ON r1.date = r2.date
    -- Include full cross-join so Tableau heatmap paints the full square
),
rolling_corr AS (
    SELECT 
        a.date AS target_date,
        a.etf_1,
        a.etf_2,
        -- Correlation over approx 63 trading days (using 90 calendar days approximation for robust joining)
        CORR(b.ret_1, b.ret_2) AS correlation_63d
    FROM paired a
    JOIN paired b
      ON a.etf_1 = b.etf_1 
      AND a.etf_2 = b.etf_2
      AND b.date <= a.date
      AND b.date >= a.date - INTERVAL '90 days'
    GROUP BY a.date, a.etf_1, a.etf_2
)
SELECT 
    target_date AS date,
    etf_1,
    etf_2,
    correlation_63d
FROM rolling_corr
WHERE correlation_63d IS NOT NULL
