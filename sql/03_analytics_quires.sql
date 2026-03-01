-- top 5
SELECT code, name, value_per_1
FROM fact_rates
ORDER BY value_per_1 DESC
LIMIT 5;

-- average per day
SELECT rate_date, ROUND(AVG(value_per_1), 6) AS avg_rate
FROM fact_rates
GROUP BY rate_date;

-- rank window
SELECT
  code, name, value_per_1,
  RANK() OVER (ORDER BY value_per_1 DESC) AS price_rank
FROM fact_rates
ORDER BY price_rank
LIMIT 10;