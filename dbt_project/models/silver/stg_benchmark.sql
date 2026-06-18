with source as (
  select * from {{source('silver', 'ihsg_benchmark')}}
)

select
  trade_date,
  -- to_char(date, 'YYYY"-W"IW') as week_label,
  close as close_price,
  daily_return
  -- case
  --   when lag(close) over (order by date) is not null
  --   then ln(close / lag(close) over (order by date))
  --   else 0
  -- end as daily_return
from source
where trade_date is not null