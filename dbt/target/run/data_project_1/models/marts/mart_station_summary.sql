
  
    

  create  table "data_project_1"."analytics"."mart_station_summary__dbt_tmp"
  
  
    as
  
  (
    

with all_data as (
    select * from "data_project_1"."analytics"."stg_mediciones_madrid"
    union all
    select * from "data_project_1"."analytics"."stg_mediciones_valencia"
),

daily_avg as (
    select
        city,
        nombre_estacion,
        fecha_day,
        avg(no2)  as no2_avg,
        avg(o3)   as o3_avg,
        avg(pm10) as pm10_avg,
        avg(pm25) as pm25_avg
    from all_data
    group by city, nombre_estacion, fecha_day
),

station_summary as (
    select
        city,
        nombre_estacion,
        min(fecha_day) as first_measurement,
        max(fecha_day) as last_measurement,
        avg(no2_avg)  as no2_avg,
        avg(o3_avg)   as o3_avg,
        avg(pm10_avg) as pm10_avg,
        avg(pm25_avg) as pm25_avg,
        case
            when avg(pm25_avg) > 50 then 'Alto'
            when avg(pm25_avg) > 25 then 'Moderado'
            else 'Bajo'
        end as nivel_pm25
    from daily_avg
    group by city, nombre_estacion
)

select *
from station_summary
order by city, nombre_estacion
  );
  