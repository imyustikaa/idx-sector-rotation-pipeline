
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        signal as value_field,
        count(*) as n_records

    from "idx_warehouse"."gold_gold"."sector_rotation"
    group by signal

)

select *
from all_values
where value_field not in (
    'BUY','HOLD','SELL'
)



  
  
      
    ) dbt_internal_test