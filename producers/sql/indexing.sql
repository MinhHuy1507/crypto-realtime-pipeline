CREATE INDEX IF NOT EXISTS idx_tech_symbol_time 
ON gold.mart_crypto__technical_analysis (symbol, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_breadth_time 
ON gold.mart_market__market_breadth (window_start DESC);

CREATE INDEX IF NOT EXISTS idx_perf_date 
ON gold.mart_crypto__periodic_performance (trade_date DESC);