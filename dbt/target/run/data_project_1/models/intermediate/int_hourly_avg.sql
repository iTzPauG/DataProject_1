
  create view "data_project_1"."public"."int_hourly_avg__dbt_tmp"
    
    
  as (
    with all_data as (
    select * from "data_project_1"."public"."stg_mediciones_madrid"
    union all
    select * from "data_project_1"."public"."stg_mediciones_valencia"
)

select
    city,
    nombre_estacion,
    fecha_hour,          -- fecha sin hora
    fecha_day,           -- hora sin fecha

    avg(no2) as no2_avg,
    avg(o3) as o3_avg,
    avg(pm10) as pm10_avg,
    avg(pm25) as pm25_avg

from all_data
group by 1,2,3,4
order by solo_hora, city, nombre_estacion;
  );