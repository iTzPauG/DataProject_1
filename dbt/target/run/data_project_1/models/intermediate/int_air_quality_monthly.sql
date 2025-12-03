
  
    

  create  table "data_project_1"."public"."int_air_quality_monthly__dbt_tmp"
  
  
    as
  
  (
    

WITH base AS (
    SELECT
        location_id,
        station_name,
        pollutant_type,
        DATE_TRUNC('month', measurement_timestamp) AS measurement_month,
        concentration_ug_m3
    FROM "data_project_1"."public"."stg_air_quality_valencia"
),

monthly AS (
    SELECT
        location_id,
        station_name,
        pollutant_type,
        measurement_month,
        AVG(concentration_ug_m3) AS avg_monthly,
        MAX(concentration_ug_m3) AS max_monthly,
        MIN(concentration_ug_m3) AS min_monthly,
        COUNT(*) AS num_measurements
    FROM base
    GROUP BY 
        location_id,
        station_name,
        pollutant_type,
        measurement_month
),

variation AS (
    SELECT
        monthly.*,
        avg_monthly 
            - LAG(avg_monthly) OVER (
                PARTITION BY location_id, pollutant_type
                ORDER BY measurement_month
            ) AS variation_vs_prev_month
    FROM monthly
)

SELECT *
FROM variation
  );
  