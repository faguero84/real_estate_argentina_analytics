select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select precio
from "real_estate"."main_marts"."fact_publicaciones"
where precio is null



      
    ) dbt_internal_test