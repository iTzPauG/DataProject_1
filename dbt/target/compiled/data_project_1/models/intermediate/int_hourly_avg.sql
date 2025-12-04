with all_data as (
    select * from "data_project_1"."analytics"."stg_mediciones_madrid"
    union all
    select * from "data_project_1"."analytics"."stg_mediciones_valencia"
)

select
    city,
    nombre_estacion,
    fecha_hour,
    avg(no2) as no2_avg,
    avg(o3) as o3_avg,
    avg(pm10) as pm10_avg,
    avg(pm25) as pm25_avg
from all_data
group by 1,2,3
order by fecha_hour, city, nombre_estacion