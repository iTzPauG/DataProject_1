
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_id
from "data_project_1"."public"."int_air_quality_daily"
where location_id is null



  
  
      
    ) dbt_internal_test