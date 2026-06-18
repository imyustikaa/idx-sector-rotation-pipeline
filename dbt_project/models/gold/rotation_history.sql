{{ config(materialized='view') }}

select
  week_label,
  sector,
  avg_weekly_return,
  momentum_score,
  sector_rank
from {{ ref('sector_rotation') }}
order by week_label desc, sector_rank asc