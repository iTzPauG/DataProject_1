
  create view "data_project_1"."public"."mart_monthly_promedio__dbt_tmp"
    
    
  as (
    with all_data as (
    select
        'madrid' as city,
        nombre_estacion,
        date_trunc('month', fecha_day)::date as fecha_month,
        no2,
        o3,
        pm10,
        pm25
    from "data_project_1"."public"."stg_mediciones_madrid"

    union all

    select
        'valencia' as city,
        nombre_estacion,
        date_trunc('month', fecha_carg)::date as fecha_month,
        no2,
        o3,
        pm10,
        pm25
    from "data_project_1"."public"."stg_mediciones_valencia"
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