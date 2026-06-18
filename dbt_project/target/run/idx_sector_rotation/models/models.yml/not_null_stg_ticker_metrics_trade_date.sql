
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_date
from "idx_warehouse"."gold_silver"."stg_ticker_metrics"
where trade_date is null



  
  
      
    ) dbt_internal_test