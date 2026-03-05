select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select fecha_alta
from "real_estate"."main_staging"."stg_properties"
where fecha_alta is null



      
    ) dbt_internal_test