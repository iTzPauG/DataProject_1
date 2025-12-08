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
    fecha_carg,
    created_at, 

    -- SOLO HORA
    to_char(fecha_carg, 'HH24:MI:SS') as fecha_hour,

    -- SOLO FECHA
    cast(fecha_carg as date) as fecha_day,

    -- Mes
    date_trunc('month', fecha_carg) as fecha_month

from {{ source('raw', 'mediciones') }}
where city = 'madrid'
