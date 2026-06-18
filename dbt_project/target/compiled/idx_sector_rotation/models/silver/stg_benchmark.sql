with source as (
  select * from "idx_warehouse"."bronze"."ihsg_benchmark"
)

select
  trade_date,
  week_label,
  close_price,
  daily_return
from source
where trade_date is not null and daily_return is not null