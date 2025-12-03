
  
    

  create  table "data_project_1"."public"."mart_air_quality_monthly__dbt_tmp"
  
  
    as
  
  (
    

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
FROM "data_project_1"."public"."int_air_quality_monthly"
  );
  