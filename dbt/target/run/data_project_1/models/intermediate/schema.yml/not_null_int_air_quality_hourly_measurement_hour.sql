
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select measurement_hour
from "data_project_1"."public"."int_air_quality_hourly"
where measurement_hour is null



  
  
      
    ) dbt_internal_test