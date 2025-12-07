

select
    id,
    nombre_estacion,
    'valencia' as city,
    cast(lon as numeric) as lon,
    cast(lat as numeric) as lat,
    cast(no2 as numeric) as no2,
    cast(o3 as numeric) as o3,
    cast(pm10 as numeric) as pm10,
    cast(pm25 as numeric) as pm25,
    fecha_carg,
    created_at, 
    
    -- SOLO HORA
    strftime(fecha_carg, '%H:%M:%S') as fecha_hour,

    -- SOLO FECHA (si la quieres)
    cast(fecha_carg as date) as fecha_day,

    -- Mes (si la necesitas)
    date_trunc('month', fecha_carg) as fecha_month

from "data_project_1"."public"."mediciones"
where city='valencia';