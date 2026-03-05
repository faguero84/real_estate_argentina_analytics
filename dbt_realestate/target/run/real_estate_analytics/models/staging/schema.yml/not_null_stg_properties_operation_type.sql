select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select operation_type
from "real_estate"."main_staging"."stg_properties"
where operation_type is null



      
    ) dbt_internal_test