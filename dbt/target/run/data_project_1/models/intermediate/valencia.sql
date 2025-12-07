
  create view "data_project_1"."analytics"."valencia__dbt_tmp"
    
    
  as (
    

select
    id,
    nombre_estacion,
    'valencia' as city,
    cast(lon as numeric) as lon,
    cast(lat as numeric) as lat,
    cast(no2 as numeric) as no2,
    cast(o3 as numeric) as o3,
    cast(pm10 as numeric) as pm10,
    cast(pm25 as numeric) as pm25,
    fecha_carg,
    created_at,
    date_trunc('hour', fecha_carg) as fecha_hour,
    date_trunc('day', fecha_carg) as fecha_day,
    date_trunc('month', fecha_carg) as fecha_month
from "data_project_1"."analytics"."stg_mediciones_valencia"
where city='valencia'
  );