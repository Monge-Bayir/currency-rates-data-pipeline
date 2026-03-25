CREATE OR REPLACE VIEW dm_currency_rates_top AS
SELECT
    rate_date,
    code,
    name,
    value_per_1,
    RANK() OVER (
        PARTITION BY rate_date
        ORDER BY value_per_1 DESC
    ) AS price_rank
FROM fact_rates;