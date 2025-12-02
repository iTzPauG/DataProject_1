-- Toma los datos directamente de la tabla fuente (estaciones) y los prepara para que se puedan consumir de forma sencilla en las capas intermedias. 
--1) DESPIVOTEAMOS:
--La tabla fuente(estaciones) tiene un formato ancho (wide), donde cada contaminante es una columna separada (so2, no2 ...)
--Necesitamos calcular promedios de forma genérica para cualquier contaminante, de modo que necesitamos un formato ancho (long) donde los contaminantes se apilan en filas. Esto lo conseguimos utilizando los UNION ALL.
-- Toma los datos directamente de la tabla fuente (estaciones) y los prepara para que se puedan consumir de forma sencilla en las capas intermedias.
WITH raw_data AS (
    SELECT
        objectid,
        nombre,
        direccion,
        tipozona,
        fecha_carg,
        so2,
        no2,
        o3,
        co,
        pm10,
        pm25,
        lon,
        lat
    FROM "data_project_1"."public"."estaciones"
),

unpivoted_data AS (

    SELECT
        objectid AS location_id,
        nombre AS station_name,
        direccion AS station_address,
        tipozona AS zona_type,
        lon,
        lat,
        fecha_carg AS measurement_timestamp,
        'SO2' AS pollutant_type,
        so2 AS concentration_ug_m3
    FROM raw_data
    WHERE so2 IS NOT NULL AND so2 >= 0 AND so2 < 1000

    UNION ALL

    SELECT
        objectid AS location_id,
        nombre AS station_name,
        direccion AS station_address,
        tipozona AS zona_type,
        lon,
        lat,
        fecha_carg AS measurement_timestamp,
        'NO2' AS pollutant_type,
        no2 AS concentration_ug_m3
    FROM raw_data
    WHERE no2 IS NOT NULL AND no2 >= 0 AND no2 < 1000

    UNION ALL

    SELECT
        objectid AS location_id,
        nombre AS station_name,
        direccion AS station_address,
        tipozona AS zona_type,
        lon,
        lat,
        fecha_carg AS measurement_timestamp,
        'O3' AS pollutant_type,
        o3 AS concentration_ug_m3
    FROM raw_data
    WHERE o3 IS NOT NULL AND o3 >= 0 AND o3 < 1000

    UNION ALL

    SELECT
        objectid AS location_id,
        nombre AS station_name,
        direccion AS station_address,
        tipozona AS zona_type,
        lon,
        lat,
        fecha_carg AS measurement_timestamp,
        'CO' AS pollutant_type,
        co AS concentration_ug_m3
    FROM raw_data
    WHERE co IS NOT NULL AND co >= 0 AND co < 50

    UNION ALL

    SELECT
        objectid AS location_id,
        nombre AS station_name,
        direccion AS station_address,
        tipozona AS zona_type,
        lon,
        lat,
        fecha_carg AS measurement_timestamp,
        'PM10' AS pollutant_type,
        pm10 AS concentration_ug_m3
    FROM raw_data
    WHERE pm10 IS NOT NULL AND pm10 >= 0 AND pm10 < 1000

    UNION ALL

    SELECT
        objectid AS location_id,
        nombre AS station_name,
        direccion AS station_address,
        tipozona AS zona_type,
        lon,
        lat,
        fecha_carg AS measurement_timestamp,
        'PM25' AS pollutant_type,
        pm25 AS concentration_ug_m3
    FROM raw_data
    WHERE pm25 IS NOT NULL AND pm25 >= 0 AND pm25 < 1000
)

SELECT
    md5(cast(coalesce(cast(measurement_timestamp as TEXT), '') || '-' || coalesce(cast(location_id as TEXT), '') || '-' || coalesce(cast(pollutant_type as TEXT), '') as TEXT)) AS air_quality_pk,
    location_id,
    station_name,
    station_address,
    zona_type,
    measurement_timestamp,
    lon AS longitude,
    lat AS latitude,
    pollutant_type,
    concentration_ug_m3,
    CASE
        WHEN pollutant_type IN ('PM10', 'PM25') THEN 'Particulates'
        WHEN pollutant_type IN ('NO2', 'SO2') THEN 'Gases_Inorgánicos'
        WHEN pollutant_type IN ('O3', 'CO') THEN 'Gases_Monóxido'
        ELSE 'Otro'
    END AS pollutant_category
FROM unpivoted_data
WHERE concentration_ug_m3 >= 0