
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select air_quality_pk
from "data_project_1"."public"."stg_air_quality_valencia"
where air_quality_pk is null



  
  
      
    ) dbt_internal_test