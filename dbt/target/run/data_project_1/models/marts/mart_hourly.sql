
  
    

  create  table "data_project_1"."public"."mart_hourly__dbt_tmp"
  
  
    as
  
  (
    

with hourly_data as (
    select * 
    from "data_project_1"."public"."int_hourly_avg"
)

select
    city,
    nombre_estacion,
    fecha_hour,
    no2_avg,
    o3_avg,
    pm10_avg,
    pm25_avg,
    -- Índice de contaminación por hora
    round((no2_avg + o3_avg + pm10_avg + pm25_avg)/4,2) as indice_contaminacion,
    -- Clasificación de NO2 por niveles
    case
        when no2_avg > 200 then 'Muy Alto'
        when no2_avg > 100 then 'Alto'
        when no2_avg > 50 then 'Moderado'
        else 'Bajo'
    end as nivel_no2,
    -- Ranking por hora dentro de cada ciudad según PM2.5
    rank() over (partition by city, fecha_hour order by pm25_avg desc) as ranking_pm25
from hourly_data
order by fecha_hour, city, nombre_estacion
  );
  