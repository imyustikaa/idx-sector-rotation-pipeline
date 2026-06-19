-- ========================== IDX Sector Rotation Warehouse Init ==========================
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ===================================== Silver Schema =====================================
CREATE TABLE IF NOT EXISTS silver.ticker_metrics_daily (
  trade_date          DATE NOT NULL,
  week_label          VARCHAR(10) NOT NULL,
  sector              VARCHAR(50) NOT NULL,
  ticker              VARCHAR(20) NOT NULL,
  price_momentum      NUMERIC(10, 6),
  sharpe_ratio        NUMERIC(10, 6),
  rolling_beta        NUMERIC(10, 6),
  max_drawdown        NUMERIC(10, 6),
  computed_at         TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS silver.sector_daily_returns (
  trade_date         DATE NOT NULL,
  week_label         VARCHAR(10) NOT NULL,
  sector             VARCHAR(50) NOT NULL,
  ticker             VARCHAR(20) NOT NULL,
  daily_return       NUMERIC(10, 6),
  loaded_at          TIMESTAMP DEFAULT NOW(),
  PRIMARY KEY (ticker, trade_date)
);

CREATE TABLE IF NOT EXISTS silver.ihsg_benchmark (
  trade_date        DATE NOT NULL PRIMARY KEY,
  week_label        VARCHAR(10) NOT NULL,
  close             NUMERIC(12, 2),
  daily_return      NUMERIC(10,6),
  loaded_at         TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ticker_metrics_sector_date ON silver.ticker_metrics_daily(sector, trade_date);
CREATE INDEX IF NOT EXISTS idx_ticker_metrics_week ON silver.ticker_metrics_daily(week_label);
CREATE INDEX IF NOT EXISTS idx_returns_sector_date ON silver.sector_daily_returns(sector, trade_date);
CREATE INDEX IF NOT EXISTS idx_returns_week ON silver.sector_daily_returns(week_label);
CREATE INDEX IF NOT EXISTS idx_ihsg_benchmark_date ON silver.ihsg_benchmark(trade_date);
CREATE INDEX IF NOT EXISTS idx_ihsg_benchmark_week ON silver.ihsg_benchmark(week_label);