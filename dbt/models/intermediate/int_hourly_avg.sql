with all_data as (
    select * from {{ ref('stg_mediciones_madrid') }}
    union all
    select * from {{ ref('stg_mediciones_valencia') }}
)

select
    city,
    nombre_estacion,
    fecha_day,
    fecha_hour,
    avg(no2)  as no2_avg,
    avg(o3)   as o3_avg,
    avg(pm10) as pm10_avg,
    avg(pm25) as pm25_avg
from all_data
group by city, nombre_estacion, fecha_day, fecha_hour
order by city, nombre_estacion, fecha_day, fecha_hour
