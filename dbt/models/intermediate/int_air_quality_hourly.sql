{{ config(materialized='table') }}

WITH base AS (
    SELECT
        location_id,
        station_name,
        pollutant_type,
        DATE_TRUNC('hour', measurement_timestamp) AS measurement_hour,
        concentration_ug_m3
    FROM {{ ref('stg_air_quality_valencia') }}
),

hourly AS (
    SELECT
        location_id,
        station_name,
        pollutant_type,
        measurement_hour,
        AVG(concentration_ug_m3) AS avg_hourly,
        MAX(concentration_ug_m3) AS max_hourly,
        MIN(concentration_ug_m3) AS min_hourly,
        COUNT(*) AS num_measurements
    FROM base
    GROUP BY 
        location_id,
        station_name,
        pollutant_type,
        measurement_hour
),

variation AS (
    SELECT
        hourly.*,
        avg_hourly 
            - LAG(avg_hourly) OVER (
                PARTITION BY location_id, pollutant_type
                ORDER BY measurement_hour
            ) AS variation_vs_prev_hour
    FROM hourly
)

SELECT *
FROM variation 