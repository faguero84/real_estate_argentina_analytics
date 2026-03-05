select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    id_publicacion as unique_field,
    count(*) as n_records

from "real_estate"."main_marts"."fact_publicaciones"
where id_publicacion is not null
group by id_publicacion
having count(*) > 1



      
    ) dbt_internal_test