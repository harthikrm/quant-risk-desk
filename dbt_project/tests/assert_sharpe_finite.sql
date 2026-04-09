-- assert_sharpe_finite.sql
-- Sharpe should not be infinite or NaN, which happens if Volatility (std_252d) is perfectly 0.
-- We flag rows where sharpe or sortino are essentially unbounded nulls disguised as zero division.
SELECT *
FROM {{ ref('mart_risk_ratios') }}
WHERE sharpe_1yr IS NULL OR sortino_1yr IS NULL
