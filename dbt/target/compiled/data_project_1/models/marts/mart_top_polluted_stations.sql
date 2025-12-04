

with daily_data as (
    select *
    from "data_project_1"."analytics"."int_daily_avg"
),

ranked as (
    select
        city,
        nombre_estacion,
        fecha_day,
        no2_avg,
        o3_avg,
        pm10_avg,
        pm25_avg,
        rank() over (partition by city, fecha_day order by pm25_avg desc) as rank_pm25,
        rank() over (partition by city, fecha_day order by pm10_avg desc) as rank_pm10
    from daily_data
)

select *
from ranked
where rank_pm25 <= 5 or rank_pm10 <= 5
order by fecha_day, city, rank_pm25, rank_pm10