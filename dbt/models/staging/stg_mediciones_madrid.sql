{{ config(
    materialized='view'
) }}

select
    id,
    nombre_estacion,
    'madrid' as city,
    cast(lon as numeric) as lon,
    cast(lat as numeric) as lat,
    cast(no2 as numeric) as no2,
    cast(o3 as numeric) as o3,
    cast(pm10 as numeric) as pm10,
    cast(pm25 as numeric) as pm25,
    fecha_day,
    fecha_hour,
    created_at, 
    
    -- SOLO HORA
    strftime(fecha_carg, '%H:%M:%S') as fecha_hour,

    -- SOLO FECHA
    cast(fecha_carg as date) as fecha_day,

    -- MES (como primer día del mes)
    date_trunc('month', fecha_carg) as fecha_month

from {{ source('raw', 'mediciones') }}
where city='madrid';
