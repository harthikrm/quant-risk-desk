WITH latest_date AS (
    SELECT MAX(date) AS max_date 
    FROM {{ ref('mart_macro_overlay') }}
)
SELECT 
    m.ticker,
    m.sector,
    m.var_95 AS current_var_95,
    m.sharpe_1yr AS current_sharpe,
    m.beta_1yr AS current_beta,
    m.drawdown_pct AS current_drawdown,
    m.regime_label AS current_regime
FROM {{ ref('mart_macro_overlay') }} m
JOIN latest_date l ON m.date = l.max_date
WHERE m.ticker NOT IN ('SPY', 'IEF', 'IRX')
