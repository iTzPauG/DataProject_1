
  create view "data_project_1"."analytics"."int_monthly_avg__dbt_tmp"
    
    
  as (
    

with all_data as (
    select * from "data_project_1"."analytics"."stg_mediciones_madrid"
    union all
    select * from "data_project_1"."analytics"."stg_mediciones_valencia"
)

select
    city,
    nombre_estacion,
    fecha_month,
    avg(no2)  as no2_avg,
    avg(o3)   as o3_avg,
    avg(pm10) as pm10_avg,
    avg(pm25) as pm25_avg
from all_data
group by city, nombre_estacion, fecha_month
order by city, nombre_estacion, fecha_month
  );