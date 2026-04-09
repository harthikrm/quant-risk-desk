WITH returns AS (
    SELECT * FROM {{ ref('int_daily_returns') }}
),

rolling_calcs AS (
    SELECT
        date,
        ticker,
        sector,
        log_return,
        
        -- 21-Day (1 Month)
        AVG(log_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS mean_21d,
        STDDEV(log_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS std_21d,
        
        -- 63-Day (1 Quarter)
        AVG(log_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 62 PRECEDING AND CURRENT ROW) AS mean_63d,
        STDDEV(log_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 62 PRECEDING AND CURRENT ROW) AS std_63d,
        
        -- 252-Day (1 Year)
        AVG(log_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS mean_252d,
        STDDEV(log_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS std_252d,
        
        -- Downside returns setup for Sortino (only negative returns)
        CASE WHEN log_return < 0 THEN log_return ELSE NULL END AS downside_return

    FROM returns
)

SELECT 
    *,
    -- 252-day downside deviation (for Sortino Ratio downstream)
    -- We compute standard deviation of only negative returns over the rolling year
    STDDEV(downside_return) OVER (PARTITION BY ticker ORDER BY date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW) AS downside_std_252d
FROM rolling_calcs
