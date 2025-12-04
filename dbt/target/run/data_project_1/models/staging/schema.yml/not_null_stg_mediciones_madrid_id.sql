
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from "data_project_1"."analytics"."stg_mediciones_madrid"
where id is null



  
  
      
    ) dbt_internal_test