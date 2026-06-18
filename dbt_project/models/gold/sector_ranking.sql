{{ config(materialized='view') }}

select
  week_label,
  sector,
  momentum_score,
  sector_rank,
  signal
from {{ ref('sector_rotation') }}