
    
    

select
    trade_date as unique_field,
    count(*) as n_records

from "idx_warehouse"."gold_silver"."stg_benchmark"
where trade_date is not null
group by trade_date
having count(*) > 1


