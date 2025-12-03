

WITH base AS (
    SELECT
        location_id,
        station_name,
        pollutant_type,
        DATE_TRUNC('day', measurement_timestamp) AS measurement_day,
        concentration_ug_m3
    FROM "data_project_1"."public"."stg_air_quality_valencia"
),

daily AS (
    SELECT
        location_id,
        station_name,
        pollutant_type,
        measurement_day,
        AVG(concentration_ug_m3) AS avg_daily,
        MAX(concentration_ug_m3) AS max_daily,
        MIN(concentration_ug_m3) AS min_daily,
        COUNT(*) AS num_measurements
    FROM base
    GROUP BY 
        location_id, 
        station_name, 
        pollutant_type, 
        measurement_day
),

variation AS (
    SELECT
        *,
        avg_daily - LAG(avg_daily) OVER (
            PARTITION BY location_id, pollutant_type
            ORDER BY measurement_day
        ) AS variation_vs_prev_day
    FROM daily
)

SELECT *
FROM variation