WITH daily AS (
    SELECT date, ticker, sector FROM {{ ref('int_daily_returns') }}
),
var_cvar AS (
    SELECT * FROM {{ ref('mart_var_cvar') }}
),
ratios AS (
    SELECT * FROM {{ ref('mart_risk_ratios') }}
),
drawdown AS (
    SELECT * FROM {{ ref('mart_drawdown') }}
),
macro AS (
    SELECT * FROM {{ ref('int_macro_regimes') }}
)

SELECT 
    d.date,
    d.ticker,
    d.sector,
    
    -- Risk Metrics
    v.var_95,
    v.var_99,
    v.cvar_95,
    r.sharpe_1yr,
    r.sortino_1yr,
    r.beta_1yr,
    dw.drawdown_pct,
    dw.days_underwater_1yr,
    
    -- Macro Indicators
    m.fed_funds_rate,
    m.cpi_level,
    m.cpi_yoy,
    m.vix_level,
    m.yield_curve_10y_2y,
    m.high_yield_spread,
    m.regime_label

FROM daily d
LEFT JOIN var_cvar v ON d.date = v.date AND d.ticker = v.ticker
LEFT JOIN ratios r ON d.date = r.date AND d.ticker = r.ticker
LEFT JOIN drawdown dw ON d.date = dw.date AND d.ticker = dw.ticker
LEFT JOIN macro m ON d.date = m.date
