CREATE OR REPLACE VIEW dm_currency_rates AS
SELECT
    rate_date,
    code,
    name,
    nominal,
    value,
    value_per_1
FROM fact_rates;