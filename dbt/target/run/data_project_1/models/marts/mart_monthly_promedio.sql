
  
    

  create  table "data_project_1"."public"."mart_monthly_promedio__dbt_tmp"
  
  
    as
  
  (
    

select
    city,
    nombre_estacion,
    fecha_month,
    no2_avg,
    o3_avg,
    pm10_avg,
    pm25_avg,
    round((no2_avg + o3_avg + pm10_avg + pm25_avg)/4,2) as contaminacion_promedio
from "data_project_1"."public"."int_monthly_avg"
  );
  