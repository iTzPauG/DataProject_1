

with all_data as (
    select
        city,
        nombre_estacion,
        fecha_month,
        fecha_carg,
        no2,
        o3,
        pm10,
        pm25
    from "data_project_1"."public"."int_monthly_avg"
)

select
    city,
    nombre_estacion,
    fecha_month,
    max(fecha_carg) as fecha_carg,  -- la última fecha de carga del mes
    avg(no2)  as no2_avg,
    avg(o3)   as o3_avg,
    avg(pm10) as pm10_avg,
    avg(pm25) as pm25_avg,
    round(
        (avg(no2) + avg(o3) + avg(pm10) + avg(pm25)) / 4
    , 2) as indice_contaminacion
from all_data
group by city, nombre_estacion, fecha_month
order by city, nombre_estacion, fecha_month