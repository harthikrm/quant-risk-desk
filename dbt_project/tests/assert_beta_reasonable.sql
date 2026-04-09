-- assert_beta_reasonable.sql
-- Beta compared to SPY typically ranges from -3.0 to 5.0 for standard unleveraged ETFs.
-- Unreasonable betas indicate a covariance math error or data shifting issues.
SELECT *
FROM {{ ref('mart_risk_ratios') }}
WHERE beta_1yr < -3.0 OR beta_1yr > 5.0
