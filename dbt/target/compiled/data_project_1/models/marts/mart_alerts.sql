

with hourly_data as (
    select *
    from "data_project_1"."analytics"."int_hourly_avg"
)

select
    city,
    nombre_estacion,
    fecha_hour,
    no2_avg,
    o3_avg,
    pm10_avg,
    pm25_avg,
    case 
        when no2_avg > 200 then 'Alerta NO2'
        when o3_avg > 180 then 'Alerta O3'
        when pm10_avg > 150 then 'Alerta PM10'
        when pm25_avg > 75 then 'Alerta PM2.5'
        else null
    end as alerta
from hourly_data
where no2_avg > 200
   or o3_avg > 180
   or pm10_avg > 150
   or pm25_avg > 75
order by fecha_hour, city, nombre_estacion