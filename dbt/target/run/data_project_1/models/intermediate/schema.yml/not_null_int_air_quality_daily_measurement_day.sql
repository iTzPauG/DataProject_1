
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select measurement_day
from "data_project_1"."public"."int_air_quality_daily"
where measurement_day is null



  
  
      
    ) dbt_internal_test