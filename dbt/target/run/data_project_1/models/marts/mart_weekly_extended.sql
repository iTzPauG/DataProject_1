
  
    

  create  table "data_project_1"."analytics"."mart_weekly_extended__dbt_tmp"
  
  
    as
  
  (
    

with daily_data as (
    select *
    from "data_project_1"."analytics"."int_daily_avg"
),

weekly_data as (
    select
        city,
        nombre_estacion,
        date_trunc('week', fecha_day) as fecha_week,
        avg(no2_avg)  as no2_avg,
        avg(o3_avg)   as o3_avg,
        avg(pm10_avg) as pm10_avg,
        avg(pm25_avg) as pm25_avg
    from daily_data
    group by city, nombre_estacion, date_trunc('week', fecha_day)
)

select
    *,
    round((no2_avg + o3_avg + pm10_avg + pm25_avg)/4,2) as indice_contaminacion,
    case
        when no2_avg > 200 then 'Muy Alto'
        when no2_avg > 100 then 'Alto'
        when no2_avg > 50 then 'Moderado'
        else 'Bajo'
    end as nivel_no2
from weekly_data
order by fecha_week, city, nombre_estacion
  );
  