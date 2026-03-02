CREATE TABLE IF NOT EXISTS public.fact_rates (
  rate_date   DATE NOT NULL,
  code        TEXT NOT NULL,
  name        TEXT NOT NULL,
  nominal     INTEGER NOT NULL,
  value       NUMERIC(12,6) NOT NULL,
  value_per_1 NUMERIC(12,6) NOT NULL,
  CONSTRAINT fact_rates_pk PRIMARY KEY (rate_date, code)
);