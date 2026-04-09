WITH stats AS (
    SELECT * FROM {{ ref('int_rolling_stats') }}
),

-- Risk-Free Rate
-- IRX is annualized Treasury Yield percentage (e.g. 5.12 meant 5.12%). We divide by 100.
rf_rate AS (
    SELECT 
        date, 
        close / 100.0 AS rf_annual 
    FROM {{ ref('stg_prices') }}
    WHERE ticker = 'IRX'
),

-- Benchmark standard
spy_returns AS (
    SELECT date, log_return AS spy_return
    FROM {{ ref('int_daily_returns') }}
    WHERE ticker = 'SPY'
),

joined AS (
    SELECT 
        s.date,
        s.ticker,
        s.mean_252d,
        s.std_252d,
        s.downside_std_252d,
        s.log_return,
        spy.spy_return,
        COALESCE(rf.rf_annual, 0.0) AS rf_annual
    FROM stats s
    LEFT JOIN rf_rate rf ON s.date = rf.date
    LEFT JOIN spy_returns spy ON s.date = spy.date
),

-- Rolling 1-year Covariance for Beta 
beta_calc AS (
    SELECT 
        a.date AS target_date,
        a.ticker,
        -- Covariance(ETF, SPY) / Variance(SPY)
        COVAR_SAMP(b.log_return, b.spy_return) / NULLIF(VAR_SAMP(b.spy_return), 0) AS beta_1yr
    FROM joined a
    JOIN joined b
      ON a.ticker = b.ticker
      AND b.date <= a.date
      AND b.date >= a.date - INTERVAL '365 days'
    GROUP BY a.date, a.ticker
)

SELECT 
    j.date,
    j.ticker,
    -- Sharpe: (Annualized Return - Annualized RF) / Annualized Volatility
    -- Daily mean * 252 = Annual mean. Daily std * sqrt(252) = Annual std.
    ((j.mean_252d * 252) - j.rf_annual) / NULLIF(j.std_252d * SQRT(252), 0) AS sharpe_1yr,
    
    -- Sortino: Same numerator, downside volatility denominator
    ((j.mean_252d * 252) - j.rf_annual) / NULLIF(j.downside_std_252d * SQRT(252), 0) AS sortino_1yr,
    
    b.beta_1yr
FROM joined j
JOIN beta_calc b ON j.date = b.target_date AND j.ticker = b.ticker
WHERE j.std_252d IS NOT NULL 
  AND j.std_252d > 0
  AND j.downside_std_252d IS NOT NULL 
  AND j.downside_std_252d > 0
