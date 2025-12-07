{{ config(materialized='table') }}

with hourly_data as (
    select * 
    from {{ ref('int_hourly_avg') }}
)

select
    city,
    nombre_estacion,

    fecha_day,       -- ← Día limpio (solo fecha)
    fecha_hour,      -- ← Hora limpia (solo HH:MM)

    no2_avg,
    o3_avg,
    pm10_avg,
    pm25_avg,

    round((no2_avg + o3_avg + pm10_avg + pm25_avg) / 4, 2) as indice_contaminacion,

    case
        when no2_avg > 200 then 'Muy Alto'
        when no2_avg > 100 then 'Alto'
        when no2_avg > 50 then 'Moderado'
        else 'Bajo'
    end as nivel_no2,

    rank() over (
        partition by city, fecha_hour
        order by pm25_avg desc
    ) as ranking_pm25

from hourly_data
order by fecha_day, fecha_hour, city, nombre_estacion;
