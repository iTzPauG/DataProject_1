
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from "data_project_1"."public"."stg_mediciones_valencia"
where id is null



  
  
      
    ) dbt_internal_test