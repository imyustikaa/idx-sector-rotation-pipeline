
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sector_rank
from "idx_warehouse"."gold_gold"."sector_rotation"
where sector_rank is null



  
  
      
    ) dbt_internal_test