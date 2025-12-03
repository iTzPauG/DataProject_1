{{ config(materialized='table') }}

SELECT
    location_id,
    station_name,
    pollutant_type,
    measurement_month,
    avg_monthly,
    max_monthly,
    min_monthly,
    num_measurements,
    variation_vs_prev_month
FROM {{ ref('int_air_quality_monthly') }}
