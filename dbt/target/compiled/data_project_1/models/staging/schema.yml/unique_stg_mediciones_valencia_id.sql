
    
    

select
    id as unique_field,
    count(*) as n_records

from "data_project_1"."public"."stg_mediciones_valencia"
where id is not null
group by id
having count(*) > 1


