select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select id_publicacion
from "real_estate"."main_marts"."fact_publicaciones"
where id_publicacion is null



      
    ) dbt_internal_test