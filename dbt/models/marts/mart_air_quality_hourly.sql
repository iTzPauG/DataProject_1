{{ config(materialized='table') }}

SELECT
    location_id,
    station_name,
    pollutant_type,
    measurement_hour,
    avg_hourly,
    max_hourly,
    min_hourly,
    num_measurements,
    variation_vs_prev_hour
FROM {{ ref('int_air_quality_hourly') }}
--Este mart proporciona un resumen horario de las mediciones de calidad del aire, incluyendo promedios, máximos, mínimos y variaciones respecto a la hora anterior. Es útil para análisis detallados y monitoreo en tiempo real.