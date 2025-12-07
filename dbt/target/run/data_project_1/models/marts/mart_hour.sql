
  
    

  create  table "data_project_1"."public"."mart_hour__dbt_tmp"
  
  
    as
  
  (
    

with hourly_data as (
    select
        city,
        nombre_estacion,
        fecha_day,
        fecha_hour,
        avg(no2)  as no2_avg,
        avg(o3)   as o3_avg,
        avg(pm10) as pm10_avg,
        avg(pm25) as pm25_avg
    from "data_project_1"."public"."int_hourly_avg"
    group by city, nombre_estacion, fecha_day, fecha_hour
)

select
    city,
    nombre_estacion,
    fecha_day,
    to_char(fecha_hour::time, 'HH24:MI') as fecha_hour,
    coalesce(no2_avg,0)  as no2_avg,
    coalesce(o3_avg,0)   as o3_avg,
    coalesce(pm10_avg,0) as pm10_avg,
    coalesce(pm25_avg,0) as pm25_avg,
    round(
        (coalesce(no2_avg,0) + coalesce(o3_avg,0) + coalesce(pm10_avg,0) + coalesce(pm25_avg,0)) / 4
    , 2) as indice_contaminacion,
    case
        when coalesce(no2_avg,0) > 200 then 'Muy Alto'
        when coalesce(no2_avg,0) > 100 then 'Alto'
        when coalesce(no2_avg,0) > 50  then 'Moderado'
        else 'Bajo'
    end as nivel_no2,
    rank() over (
        partition by city, fecha_hour
        order by coalesce(pm25_avg,0) desc
    ) as ranking_pm25
from hourly_data
order by fecha_day, fecha_hour, city, nombre_estacion
  );
  