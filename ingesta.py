import time
import os
import logging
import psycopg
import requests
from urllib.parse import urlparse, urlunparse

# =======================================================
# VARIABLES GLOBALES
# =======================================================
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

SERVER_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SLEEPTIME=900

# =======================================================
# LOGGING
# =======================================================
# log_level = os.getenv("LOG_LEVEL")
# logging.basicConfig(level=getattr(logging, log_level), format="%(asctime)s [%(levelname)s] %(message)s")
log_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ====================================
# CREAR BASE DE DATOS
# ====================================
# def create_database():
#     conn = None
#     cur = None
#     for _ in range(10):  # intenta 10 veces
#         try:
#             logging.info("Verificando base '%s'...", DB_NAME)
#             conn = psycopg.connect(
#                 host=DB_HOST,
#                 port=DB_PORT,
#                 user=DB_USER,
#                 password=DB_PASS,
#                 dbname="postgres",  # se conecta a postgres para crear la BD
#                 connect_timeout=5
#             )
#             conn.autocommit = True
#             cur = conn.cursor()

#             cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
#             exists = cur.fetchone()
            
#             if not exists:
#                 logging.info("Base no existe. Creándola...")
#                 cur.execute(f"CREATE DATABASE {DB_NAME};")
#                 logging.info(f"Base de datos '{DB_NAME}' creada correctamente.")
#             else:
#                 logging.info("La base ya existe.")
#             cur.close()
#             conn.close()
#             return
            
#         except Exception as e:
#             logging.error(f"Error creando database: {e}")
#             if cur:
#                 cur.close()
#             if conn:
#                 conn.close()
#             time.sleep(3)
#     logging.error("No se pudo crear/verificar la base de datos después de 10 intentos.")
#     exit(1)
# ====================================
# FUNCION ESPERAR CONEXIÓN
# ====================================
def wait_for_connection():
    for _ in range(10):
        try:
            conn = psycopg.connect(DB_URL)
            logging.info("Conexión exitosa a la base de datos objetivo.")
            conn.close()
            return
        except Exception as e:
            logging.warning(f"No se puede conectar todavía: {e}")
            time.sleep(3)
    logging.error("No se pudo conectar después de varios intentos.")
    exit(1)

# =======================================================
# DESCARGAR DATOS DE LA API
# =======================================================
def fetch_data():
    url = (
        "https://valencia.opendatasoft.com/api/records/1.0/search/"
        "?dataset=estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas"
        "&rows=1000"
    )
    try:
        resp = requests.get(url)
        resp.raise_for_status()
        return resp.json().get("records", [])
    except Exception as e:
        logging.error(f"Error al obtener datos de la API: {e}")
        return []
# =======================================================
# CREAR TABLA EN POSTGRES
# =======================================================
def create_table():
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                # cur.execute("DROP TABLE IF EXISTS estaciones CASCADE;")
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
                # Crear índice único para evitar duplicados
                cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_estaciones_objectid_fecha 
                    ON estaciones(objectid, fecha_carg);
                """)
                logging.info("Tabla e índice único creados correctamente.")
    except Exception as e:
        logging.error(f"Error guardando datos: {e}")

# =======================================================
# GUARDAR DATOS EN POSTGRES
# =======================================================
def insert_data(records):
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                inserted_count=0
                for rec in records:
                    fields = rec.get("fields", {})
                    geo = fields.get("geo_point_2d", [])
                    lon = geo[0] if len(geo) > 0 else None
                    lat = geo[1] if len(geo) > 1 else None

                    cur.execute("""
                        INSERT INTO estaciones (
                            objectid, nombre, direccion, tipozona,
                            so2, no2, o3, co, pm10, pm25,
                            tipoemisio, fecha_carg, calidad_am, fiwareid,
                            lon, lat
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)   
                    """, (
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
                    ))
                    if cur.rowcount > 0:
                        inserted_count += 1

        logging.info(f"✅ {inserted_count} registros nuevos insertados de {len(records)} procesados.")
    except Exception as e:
        logging.error(f"Error guardando datos: {e}")

# =======================================================
# MAIN
# =======================================================
if __name__ == "__main__":
    logging.info("Iniciando script...")

    # # 1. Crear DB desde cero si falta
    # create_database()

    # 2. Esperar conexión a nueva DB
    wait_for_connection()

    # 3. Crear Tabla
    create_table()

    # 4. Bucle de actualización
    while True:
        records = fetch_data()
        if records:
            insert_data(records)
        else:
            logging.warning("No se obtuvieron registros de la API.")

        logging.info(f"Esperando {int(SLEEPTIME / 60)} minutos para la siguiente actualización...")
        time.sleep(SLEEPTIME)
