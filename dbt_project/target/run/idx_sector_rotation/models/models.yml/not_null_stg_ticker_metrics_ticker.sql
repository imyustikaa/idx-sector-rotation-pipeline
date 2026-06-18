
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select ticker
from "idx_warehouse"."gold_silver"."stg_ticker_metrics"
where ticker is null



  
  
      
    ) dbt_internal_test