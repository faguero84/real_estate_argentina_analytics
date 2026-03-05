
    
    

select
    id_publicacion as unique_field,
    count(*) as n_records

from "real_estate"."main_marts"."fact_publicaciones"
where id_publicacion is not null
group by id_publicacion
having count(*) > 1


