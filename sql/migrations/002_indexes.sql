CREATE INDEX IF NOT EXISTS idx_fact_rates_rate_date ON public.fact_rates (rate_date);
CREATE INDEX IF NOT EXISTS idx_fact_rates_value_per_1 ON public.fact_rates (value_per_1);