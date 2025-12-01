-- Toma los datos directamente de la tabla fuente (estaciones) y los prepara para que se puedan consumir de forma sencilla en las capas intermedias. 
--1) DESPIVOTEAMOS:
--La tabla fuente(estaciones) tiene un formato ancho (wide), donde cada contaminante es una columna separada (so2, no2 ...)
--Necesitamos calcular promedios de forma genérica para cualquier contaminante, de modo que necesitamos un formato ancho (long) donde los contaminantes se apilan en filas. Esto lo conseguimos utilizando los UNION ALL.
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
    FROM {{source('raw_data', 'estaciones')}}
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
    'S02' AS pollution_type,
    s02 AS concentratios_ug_m3
    FROM raw_data
    WHERE so2 IS NOT NULL AND so2 >=0 AND so2<1000 --Filtramos valores atípicos.
    
    UNION ALL 
    
    SELECT
    objectid AS location_id,
    nombre AS station_name,
    direccion AS station_address,
    tipozona AS zona_type,
    lon, 
    lat,
    fecha_carg AS measurement_timestamp,
    'NO2' AS pollution_type,
    no2 AS concentratios_ug_m3
    
    FROM  raw_data
    WHERE no2 IS NOT NULL AND no2>=0 AAND no2<1000

    UNION ALL
    SELECT
    objectid AS location_id,
    nombre AS station_name,
    direccion AS station_address,
    tipozona AS zona_type,
    lon,
    lat,
    fecha_carg AS measurement_timestamp,
    'O3' AS pollution_type,
    o3 AS concentratios_ug_m3
    FROM raw_data
    WHERE o3 IS NOT NULL AND o3>=0 AND o3<1000

    UNION ALL
    SELECT
    objectid AS location_id,
    nombre AS station_name,
    direccion AS station_address,
    lon,
    lat,
    tipozona AS zona_type,
    fecha_carg AS measurement_timestamp,
    'CO' AS pollution_type,
    co AS concentratios_ug_m3
    FROM raw_data
    WHERE co IS NOT NULL AND co>=0 AND co<50 

    UNION ALL
    SELECT
    objectid AS location_id,
    nombre AS station_name,
    direccion AS station_address,
    tipozona AS zona_type,
    lon,
    lat,
    fecha_carg AS measurement_timestamp,
    'PM10' AS pollution_type,
    pm10 AS concentratios_ug_m3
    FROM raw_data
    WHERE pm10 IS NOT NULL AND pm10>=0 AND pm10<1000

    UNION ALL
    SELECT
    objectid AS location_id,
    nombre AS station_name,
    direccion AS station_address,
    tipozona AS zona_type,
    lon,
    lat,
    fecha_carg AS measurement_timestamp,
    'PM2.5' AS pollution_type,
    pm25 AS concentratios_ug_m3
    FROM raw_data
    WHERE pm25 IS NOT NULL AND pm25>=0 AND pm25<1000 --Filtrado básico.
)
SELECT 
    {{ dbt_utils.surrogate_key(['measurement_timestamp', 'location_id', 'pollutant_type']) }} AS air_quality_pk, --Clave primaria: para cada fila. Así garantizamos que cada registro transformado tenaga una identificación única.
    location_id,
    station_name,
    station_address,
    zona_type,
    measurement_timestamp,
    lon AS longitude,
    lat AS latitude,
    pollution_type,
    concentratios_ug_m3

    CASE
        WHEN pollutant_type IN ('PM10', 'PM2.5') THEN 'Particulates'
        WHEN pollutant_type IN ('NO2', 'SO2') THEN 'Gases_Inorgánicos'
        WHEN pollutant_type IN ('O3', 'CO') THEN 'Gases_Monóxido'
        ELSE 'Otro'
    END AS pollutant_category
FROM 
    unpivoted_data
WHERE
    concentratios_ug_m3 >= 0 -- Asegura que los valores de concentración sean positivos.
