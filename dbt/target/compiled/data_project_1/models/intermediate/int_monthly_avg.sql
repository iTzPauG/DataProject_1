

with madrid as (
    select
        'madrid' as city,
        nombre_estacion,
        cast(fecha_carg as date) as fecha_day,
        date_trunc('month', fecha_carg)::date as fecha_month,
        to_char(fecha_carg, 'HH24:MI:SS') as fecha_hour,
        fecha_carg,  -- <-- añadimos la columna original
        cast(no2 as numeric) as no2,
        cast(o3 as numeric) as o3,
        cast(pm10 as numeric) as pm10,
        cast(pm25 as numeric) as pm25
    from "data_project_1"."public"."stg_mediciones_madrid"
),

valencia as (
    select
        'valencia' as city,
        nombre_estacion,
        cast(fecha_carg as date) as fecha_day,
        date_trunc('month', fecha_carg)::date as fecha_month,
        to_char(fecha_carg, 'HH24:MI:SS') as fecha_hour,
        fecha_carg,  -- <-- añadimos la columna original
        cast(no2 as numeric) as no2,
        cast(o3 as numeric) as o3,
        cast(pm10 as numeric) as pm10,
        cast(pm25 as numeric) as pm25
    from "data_project_1"."public"."stg_mediciones_valencia"
)

select *
from madrid
union all
select *
from valencia