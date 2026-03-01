-- duplicates
SELECT rate_date, code, COUNT(*) AS cnt
FROM fact_rates
GROUP BY rate_date, code
HAVING COUNT(*) > 1;

-- invalid values
SELECT *
FROM fact_rates
WHERE value_per_1 <= 0 OR nominal <= 0;