
  
    

  create  table "data_project_1"."public"."mart_air_quality_daily__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    location_id,
    station_name,
    pollutant_type,
    measurement_day,
    avg_daily,
    max_daily,
    min_daily,
    num_measurements,
    variation_vs_prev_day
FROM "data_project_1"."public"."int_air_quality_daily"
--Este mart proporciona un resumen diario de las mediciones de calidad del aire, incluyendo promedios, máximos, mínimos y variaciones respecto al día anterior. Es útil para análisis diarios y reportes de cumplimiento.
  );
  