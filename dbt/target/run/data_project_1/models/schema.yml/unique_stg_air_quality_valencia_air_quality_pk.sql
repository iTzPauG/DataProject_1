
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    air_quality_pk as unique_field,
    count(*) as n_records

from "data_project_1"."public"."stg_air_quality_valencia"
where air_quality_pk is not null
group by air_quality_pk
having count(*) > 1



  
  
      
    ) dbt_internal_test