
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select nombre_estacion
from "data_project_1"."analytics"."stg_mediciones_valencia"
where nombre_estacion is null



  
  
      
    ) dbt_internal_test