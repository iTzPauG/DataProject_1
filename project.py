import requests
import psycopg
import os
import time
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------------------
# Configuración de conexión
# -------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    logging.error("No se encontró la variable de entorno DATABASE_URL")
    exit(1)

logging.info("Intentando conectar a la base de datos...")

# Intentos de conexión
intentos_conexion = 0
conectado = False
while intentos_conexion < 5 and not conectado:
    try:
        conexion = psycopg.connect(DATABASE_URL)
        cursor = conexion.cursor()
        conectado = True
        logging.info("Conexión exitosa a la base de datos")
    except Exception as e:
        intentos_conexion += 1
        logging.warning(f"Error al conectar a la base de datos (intento {intentos_conexion}/5): {e}")
        time.sleep(10)

if not conectado:
    logging.error("No se pudo conectar a la base de datos después de 5 intentos")
    exit(1)

# -------------------------------
# Función para descargar todos los datos de la API
# -------------------------------
def fetch_data():
    url = (
        "https://valencia.opendatasoft.com/api/records/1.0/search/"
        "?dataset=estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas"
        "&rows=1000"
    )
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        data = resp.json()
        return data.get("records", [])
    except Exception as e:
        logging.error(f"Error al obtener datos de la API: {e}")
        return []

# -------------------------------
# Función para guardar datos en Postgres
# -------------------------------
def save_to_postgres(records):
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS estaciones CASCADE;")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS estaciones (
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
        logging.info(f"{len(records)} registros guardados en Postgres correctamente")
    except Exception as e:
        logging.error(f"Error guardando datos en Postgres: {e}")

# -------------------------------
# Ejecución principal
# -------------------------------
if __name__ == "__main__":
    logging.info("Inicio del proceso de actualización de datos")
    while True:
        records = fetch_data()
        if records:
            save_to_postgres(records)
        else:
            logging.warning("No se obtuvieron registros de la API")
        logging.info("Esperando 1 hora para la siguiente actualización...")
        time.sleep(3600)  # Espera 1 hora

