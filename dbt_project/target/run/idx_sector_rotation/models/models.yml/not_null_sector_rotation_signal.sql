
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select signal
from "idx_warehouse"."gold_gold"."sector_rotation"
where signal is null



  
  
      
    ) dbt_internal_test