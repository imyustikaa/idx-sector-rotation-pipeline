
  create view "idx_warehouse"."gold_gold"."sector_ranking__dbt_tmp"
    
    
  as (
    

select
  week_label,
  sector,
  momentum_score,
  sector_rank,
  signal
from "idx_warehouse"."gold_gold"."sector_rotation"
  );