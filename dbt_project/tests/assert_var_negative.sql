-- assert_var_negative.sql
-- Value at Risk (VaR) should always be negative or zero (representing a loss threshold).
-- If it is positive, it means the 5% worst cases are profits, which defies expected boundaries and indicates data errors.
SELECT *
FROM {{ ref('mart_var_cvar') }}
WHERE var_95 > 0 OR var_99 > 0
