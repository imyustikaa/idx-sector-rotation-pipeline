
  create view "idx_warehouse"."gold_silver"."stg_ticker_metrics__dbt_tmp"
    
    
  as (
    with source as (
  select * from "idx_warehouse"."silver"."ticker_metrics_daily"
),

cleaned as (
  select
    trade_date,
    week_label,
    sector,
    ticker,
    nullif(price_momentum, 0) as price_momentum,
    nullif(sharpe_ratio, 0) as sharpe_ratio,
    nullif(rolling_beta, 0) as rolling_beta,
    max_drawdown,
    computed_at
  from source
  where trade_date is not null and ticker is not null
)

select * from cleaned
  );