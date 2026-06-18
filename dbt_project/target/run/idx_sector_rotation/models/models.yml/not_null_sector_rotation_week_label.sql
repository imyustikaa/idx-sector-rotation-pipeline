
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select week_label
from "idx_warehouse"."gold_gold"."sector_rotation"
where week_label is null



  
  
      
    ) dbt_internal_test