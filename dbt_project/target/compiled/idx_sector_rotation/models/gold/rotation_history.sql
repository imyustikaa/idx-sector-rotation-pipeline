

select
  week_label,
  sector,
  avg_weekly_return,
  momentum_score,
  sector_rank
from "idx_warehouse"."gold_gold"."sector_rotation"
order by week_label desc, sector_rank asc