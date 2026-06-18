
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sector
from "idx_warehouse"."gold_silver"."stg_ticker_metrics"
where sector is null



  
  
      
    ) dbt_internal_test