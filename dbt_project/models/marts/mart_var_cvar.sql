WITH returns AS (
    SELECT date, ticker, log_return 
    FROM {{ ref('int_daily_returns') }}
),

-- Create trailing windows via self-join (1-year calendar lookback ≈ 252 trading days)
-- This powers our Historical Simulation calculations.
trailing_returns AS (
    SELECT 
        a.date AS target_date,
        a.ticker,
        b.log_return AS past_return
    FROM returns a
    JOIN returns b
      ON a.ticker = b.ticker
      AND b.date <= a.date
      AND b.date >= a.date - INTERVAL '365 days'
),

var_calc AS (
    SELECT 
        target_date,
        ticker,
        -- Worst 5% and 1% of trailing array returns, mathematically capped to <= 0.0 
        LEAST(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY past_return), 0.0) AS var_95,
        LEAST(PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY past_return), 0.0) AS var_99
    FROM trailing_returns
    GROUP BY target_date, ticker
),

cvar_calc AS (
    SELECT 
        t.target_date,
        t.ticker,
        -- CVaR is the expectation (mean) of all returns worse than the VaR threshold
        AVG(CASE WHEN t.past_return <= v.var_95 THEN t.past_return END) AS cvar_95,
        AVG(CASE WHEN t.past_return <= v.var_99 THEN t.past_return END) AS cvar_99
    FROM trailing_returns t
    JOIN var_calc v ON t.target_date = v.target_date AND t.ticker = v.ticker
    GROUP BY t.target_date, t.ticker
)

SELECT 
    v.target_date AS date,
    v.ticker,
    v.var_95,
    v.var_99,
    c.cvar_95,
    c.cvar_99
FROM var_calc v
JOIN cvar_calc c ON v.target_date = c.target_date AND v.ticker = c.ticker
