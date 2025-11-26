import requests
import psycopg
import os
import time
from urllib.parse import urlparse

# -------------------------------
# Configuración de conexión
# -------------------------------
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@db:5432/pruebadb"
)

parsed_url = urlparse(DATABASE_URL)
PG_HOST = parsed_url.hostname
PG_PORT = parsed_url.port or 5432
PG_DB   = parsed_url.path.lstrip("/")
PG_USER = parsed_url.username
PG_PASS = parsed_url.password

# -------------------------------
# Función para descargar todos los datos de la API
# -------------------------------
def fetch_data():
    url = (
        "https://valencia.opendatasoft.com/api/records/1.0/search/"
        "?dataset=estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas"
        "&rows=1000"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("records", [])

# -------------------------------
# Función para guardar datos en Postgres
# -------------------------------
def save_to_postgres(records):
    conn = psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()
    
    # Borrar tabla antigua y crear tabla nueva
    cur.execute("""
        DROP TABLE estaciones CASCADE;
        CREATE TABLE estaciones (
            id SERIAL PRIMARY KEY,
            objectid INTEGER NOT NULL,
            nombre TEXT,
            direccion TEXT,
            tipozona TEXT,
            so2 NUMERIC,
            no2 NUMERIC,
            o3 NUMERIC,
            co NUMERIC,
            pm10 NUMERIC,
            pm25 NUMERIC,
            tipoemisio TEXT,
            fecha_carg TIMESTAMP,
            calidad_am TEXT,
            fiwareid TEXT,
            lon NUMERIC,
            lat NUMERIC,
            created_at TIMESTAMP DEFAULT NOW()
        );
    """)
    
    # Insertar registros
    for rec in records:
        fields = rec.get("fields", {})
        geo = fields.get("geo_point_2d", [])
        lon = geo[0] if len(geo) > 0 else None
        lat = geo[1] if len(geo) > 1 else None

        cur.execute(
            """
            INSERT INTO estaciones (
                objectid, nombre, direccion, tipozona,
                so2, no2, o3, co, pm10, pm25,
                tipoemisio, fecha_carg, calidad_am, fiwareid,
                lon, lat
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (objectid) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                direccion = EXCLUDED.direccion,
                tipozona = EXCLUDED.tipozona,
                so2 = EXCLUDED.so2,
                no2 = EXCLUDED.no2,
                o3 = EXCLUDED.o3,
                co = EXCLUDED.co,
                pm10 = EXCLUDED.pm10,
                pm25 = EXCLUDED.pm25,
                tipoemisio = EXCLUDED.tipoemisio,
                fecha_carg = EXCLUDED.fecha_carg,
                calidad_am = EXCLUDED.calidad_am,
                fiwareid = EXCLUDED.fiwareid,
                lon = EXCLUDED.lon,
                lat = EXCLUDED.lat;
            """,
            (
                fields.get("objectid"),
                fields.get("nombre"),
                fields.get("direccion"),
                fields.get("tipozona"),
                fields.get("so2"),
                fields.get("no2"),
                fields.get("o3"),
                fields.get("co"),
                fields.get("pm10"),
                fields.get("pm25"),
                fields.get("tipoemisio"),
                fields.get("fecha_carg"),
                fields.get("calidad_am"),
                fields.get("fiwareid"),
                lon,
                lat
            )
        )
    
    conn.commit()
    cur.close()
    conn.close()

# -------------------------------
# Ejecución principal con actualización cada hora
# -------------------------------
if __name__ == "__main__":
    while True:
        try:
            recs = fetch_data()
            print(f"Obtenidos {len(recs)} registros de la API")
            save_to_postgres(recs)
            print("Datos guardados en Postgres correctamente")
        except Exception as e:
            print(f"Error al actualizar datos: {e}")
        print("Esperando 1 hora para la siguiente actualización...")
        time.sleep(3600)  # 1 hora
