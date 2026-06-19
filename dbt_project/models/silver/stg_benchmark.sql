with source as (
  select * from {{source('silver', 'ihsg_benchmark')}}
)

select
  trade_date,
  close as close_price,
  daily_return
from source
where trade_date is not null