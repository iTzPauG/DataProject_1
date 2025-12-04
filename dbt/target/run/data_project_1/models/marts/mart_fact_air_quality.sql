
  
    

  create  table "data_project_1"."analytics"."mart_fact_air_quality__dbt_tmp"
  
  
    as
  
  (
    

with daily_data as (
    select * from "data_project_1"."analytics"."int_daily_avg"
),
weekly_data as (
    select * from "data_project_1"."analytics"."mart_weekly_extended"
),
monthly_data as (
    select * from "data_project_1"."analytics"."int_monthly_avg"
)

select
    coalesce(d.city, w.city, m.city) as city,
    coalesce(d.nombre_estacion, w.nombre_estacion, m.nombre_estacion) as nombre_estacion,
    d.fecha_day,
    w.fecha_week,
    m.fecha_month,
    d.no2_avg as daily_no2,
    d.o3_avg as daily_o3,
    d.pm10_avg as daily_pm10,
    d.pm25_avg as daily_pm25,
    w.no2_avg as weekly_no2,
    w.o3_avg as weekly_o3,
    w.pm10_avg as weekly_pm10,
    w.pm25_avg as weekly_pm25,
    m.no2_avg as monthly_no2,
    m.o3_avg as monthly_o3,
    m.pm10_avg as monthly_pm10,
    m.pm25_avg as monthly_pm25
from daily_data d
full outer join weekly_data w
    on d.city = w.city
    and d.nombre_estacion = w.nombre_estacion
    and date_trunc('week', d.fecha_day) = w.fecha_week
full outer join monthly_data m
    on coalesce(d.city, w.city) = m.city
    and coalesce(d.nombre_estacion, w.nombre_estacion) = m.nombre_estacion
    and date_trunc('month', d.fecha_day) = m.fecha_month
order by city, nombre_estacion, d.fecha_day
  );
  