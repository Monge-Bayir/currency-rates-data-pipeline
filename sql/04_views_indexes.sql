CREATE INDEX IF NOT EXISTS idx_fact_rates_date_code
ON fact_rates (rate_date, code);

CREATE OR REPLACE VIEW v_top_rates AS
SELECT rate_date, code, name, value_per_1
FROM fact_rates
ORDER BY value_per_1 DESC;