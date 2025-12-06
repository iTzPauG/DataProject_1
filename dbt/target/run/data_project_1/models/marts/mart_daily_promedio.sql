
  
    

  create  table "data_project_1"."public"."mart_daily_promedio__dbt_tmp"
  
  
    as
  
  (
    

with daily_data as (
    select * 
    from "data_project_1"."public"."int_daily_avg"
)

select
    city,
    nombre_estacion,
    fecha_day,
    no2_avg,
    o3_avg,
    pm10_avg,
    pm25_avg,
    -- Calculamos un índice de contaminación promedio
    round((no2_avg + o3_avg + pm10_avg + pm25_avg)/4,2) as indice_contaminacion,
    -- Clasificación del nivel de NO2
    case
        when no2_avg > 200 then 'Muy Alto'
        when no2_avg > 100 then 'Alto'
        when no2_avg > 50 then 'Moderado'
        else 'Bajo'
    end as nivel_no2,
    -- Ranking por día dentro de cada ciudad según PM2.5
    rank() over (partition by city, fecha_day order by pm25_avg desc) as ranking_pm25
from daily_data
order by fecha_day, city, nombre_estacion
  );
  