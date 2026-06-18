
      
  
    

  create  table "idx_warehouse"."gold_gold"."sector_rotation"
  
  
    as
  
  (
    

with ticker_metrics as (
  select * from "idx_warehouse"."gold_silver"."stg_ticker_metrics"

  
),

sector_agg as (
  select
    week_label,
    sector,
    avg(price_momentum) as avg_price_momentum,
    avg(sharpe_ratio) as avg_sharpe_ratio,
    avg(rolling_beta) as avg_rolling_beta,
    avg(max_drawdown) as avg_max_drawdown
  from ticker_metrics
  group by week_label, sector
),

weekly_return as (
  select
    week_label,
    sector,
    avg(daily_return) * 5 as avg_weekly_return
  from "idx_warehouse"."bronze"."sector_metrics"

  

  group by week_label, sector
),

scored as (
  select
    s.week_label,
    s.sector,
    s.avg_price_momentum,
    s.avg_sharpe_ratio,
    s.avg_rolling_beta,
    s.avg_max_drawdown,
    w.avg_weekly_return,
    (
      percent_rank() over (partition by s.week_label order by s.avg_price_momentum asc nulls last)
      + percent_rank() over (partition by s.week_label order by s.avg_sharpe_ratio asc nulls last)
      + percent_rank() over (partition by s.week_label order by s.avg_rolling_beta desc nulls last)
      + percent_rank() over (partition by s.week_label order by s.avg_max_drawdown desc nulls last)
    ) / 4.0 as momentum_score
  from sector_agg s
  left join weekly_return w
    on s.week_label = w.week_label and s.sector = w.sector
),

ranked as (
  select
    week_label,
    sector,
    avg_price_momentum,
    avg_sharpe_ratio,
    avg_rolling_beta,
    avg_max_drawdown,
    avg_weekly_return,
    momentum_score,
    rank() over (partition by week_label order by momentum_score desc) as sector_rank
  from scored
)

select
  week_label,
  sector,
  avg_price_momentum,
  avg_sharpe_ratio,
  avg_rolling_beta,
  avg_max_drawdown,
  avg_weekly_return,
  momentum_score,
  sector_rank,
  case
    when sector_rank <= 3 then 'BUY'
    when sector_rank <= 7 then 'HOLD'
    else 'SELL'
  end as signal,
  now() as updated_at
from ranked
  );
  
  