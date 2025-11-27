import time
import logging
import psycopg
import requests
from urllib.parse import urlparse, urlunparse

# =======================================================
# CONFIGURACIÓN DIRECTA (SIN VARIABLES DE ENTORNO)
# =======================================================
DB_USER = "postgres"
DB_PASS = "postgres"
DB_HOST = "db"
DB_PORT = "5432"
DB_NAME = "data_project_1"  

SERVER_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# =======================================================
# LOGGING
# =======================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# ====================================
<<<<<<< HEAD
# CREAR BASE DE DATOS SI NO EXISTE
# ====================================
def create_database_if_not_exists():
    logging.info(f"Verificando base '{DB_NAME}'...")

    try:
        with psycopg.connect(SERVER_URL) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
                exists = cur.fetchone()

                if not exists:
                    logging.info(f"No existe. Creando '{DB_NAME}'...")
                    cur.execute(f"CREATE DATABASE {DB_NAME};")
                    logging.info("Base creada correctamente.")
                else:
                    logging.info("La base ya existe.")
    except Exception as e:
        logging.error(f"Error creando/verificando base: {e}")
        exit(1)

def ensure_database():
    for _ in range(10):  # intenta 10 veces
        try:
            logging.info("Verificando base '%s'...", DB_NAME)
            conn = psycopg.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                dbname="postgres"  # se conecta a postgres para crear la BD
            )
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
            exists = cur.fetchone()

            if not exists:
                logging.info("Base no existe. Creándola...")
                cur.execute(f"CREATE DATABASE {DB_NAME};")
            else:
                logging.info("La base ya existe.")

            cur.close()
            conn.close()
            return
        except Exception as e:
            logging.error("Error creando/verificando base: %s", e)
            time.sleep(3)

    raise Exception("Postgres no respondió después de varios intentos")

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

=======
# CREAR BASE DE DATOS
# ====================================
def create_database():
    for _ in range(10):  # intenta 10 veces
        try:
            logging.info("Verificando base '%s'...", DB_NAME)
            conn = psycopg.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                dbname="postgres"  # se conecta a postgres para crear la BD
            )
            conn.autocommit = True
            cur = conn.cursor()

            cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
            exists = cur.fetchone()
            
            if not exists:
                logging.info("Base no existe. Creándola...")
                cur.execute(f"CREATE DATABASE {DB_NAME};")
            else:
                logging.info("La base ya existe.")
                break
        except Exception as e:
            logging.error(f"Error creando database: {e}")
            time.sleep(3)
    cur.close()
    conn.close()

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

>>>>>>> main
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
<<<<<<< HEAD


# =======================================================
# GUARDAR DATOS EN POSTGRES
=======
# =======================================================
# CREAR TABLA EN POSTGRES
>>>>>>> main
# =======================================================
def create_table():
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
<<<<<<< HEAD
=======
                # cur.execute("DROP TABLE IF EXISTS estaciones CASCADE;")
>>>>>>> main
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS estaciones (
                        id SERIAL PRIMARY KEY,
                        objectid INTEGER NOT NULL,
                        nombre TEXT,
                        direccion TEXT NOT NULL,
                        tipozona TEXT,
                        so2 NUMERIC,
                        no2 NUMERIC,
                        o3 NUMERIC,
                        co NUMERIC,
                        pm10 NUMERIC,
                        pm25 NUMERIC,
                        tipoemisio TEXT,
                        fecha_carg TIMESTAMP NOT NULL,
                        calidad_am TEXT,
                        fiwareid TEXT,
                        lon NUMERIC,
                        lat NUMERIC,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)
<<<<<<< HEAD
                
                # Crear constraint único para evitar duplicados por actualización
                cur.execute("""
                    ALTER TABLE estaciones
                    ADD CONSTRAINT IF NOT EXISTS estaciones_unique
                    UNIQUE (direccion, fecha_carg, calidad_am);
                """)
                
        logging.info("Tabla 'estaciones' creada correctamente (si no existía).")
    except Exception as e:
        logging.error(f"Error creando la tabla: {e}")

=======
    except Exception as e:
        logging.error(f"Error guardando datos: {e}")

# =======================================================
# GUARDAR DATOS EN POSTGRES
# =======================================================
>>>>>>> main
def insert_data(records):
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
<<<<<<< HEAD
                # Asegurar que la tabla tenga un UNIQUE sobre estación + fecha + calidad
                cur.execute("""
                    ALTER TABLE estaciones
                    ADD CONSTRAINT IF NOT EXISTS estaciones_unique
                    UNIQUE (direccion, fecha_carg, calidad_am);
                """)

=======
>>>>>>> main
                for rec in records:
                    fields = rec.get("fields", {})
                    geo = fields.get("geo_point_2d", [])
                    lon = geo[0] if len(geo) > 0 else None
                    lat = geo[1] if len(geo) > 1 else None

<<<<<<< HEAD
                    fecha_carg = fields.get("fecha_carg")

                    # INSERT con ON CONFLICT para evitar duplicados
=======
>>>>>>> main
                    cur.execute("""
                        INSERT INTO estaciones (
                            objectid, nombre, direccion, tipozona,
                            so2, no2, o3, co, pm10, pm25,
                            tipoemisio, fecha_carg, calidad_am, fiwareid,
                            lon, lat
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
<<<<<<< HEAD
                        ON CONFLICT (direccion, fecha_carg, calidad_am) 
                        DO NOTHING;
=======
>>>>>>> main
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
<<<<<<< HEAD
                        fecha_carg,
=======
                        fields.get("fecha_carg"),
>>>>>>> main
                        fields.get("calidad_am"),
                        fields.get("fiwareid"),
                        lon,
                        lat
                    ))
<<<<<<< HEAD

        logging.info(f"{len(records)} registros procesados correctamente (sin duplicados).")
    except Exception as e:
        logging.error(f"Error insertando datos: {e}")       logging.error(f"Error realizando UPSERT en Postgres: {e}")

=======
        logging.info(f"{len(records)} registros guardados correctamente.")
    except Exception as e:
        logging.error(f"Error guardando datos: {e}")

>>>>>>> main
# =======================================================
# EJECUCIÓN PRINCIPAL
# =======================================================
if __name__ == "__main__":
    logging.info("Iniciando script...")
<<<<<<< HEAD
    ensure_database()
    # 1. Crear DB desde cero si falta
    create_database_if_not_exists()
=======

    # 1. Crear DB desde cero si falta
    create_database()
>>>>>>> main

    # 2. Esperar conexión a nueva DB
    wait_for_connection()

<<<<<<< HEAD
    # 3. Bucle de actualización
    while True:
        records = fetch_data()
        if records:
            insert_data(records)  # <-- aquí llamamos a insert_data
        else:
            logging.warning("No se obtuvieron registros de la API.")

        logging.info("Esperando 15 minutos para la siguiente actualización...")
        time.sleep(900)  # 900 segundos = 15 minutos
=======
    # 3. Crear Tabla
    create_table()

    # 4. Bucle de actualización
    while True:
        records = fetch_data()
        if records:
            insert_data(records)
        else:
            logging.warning("No se obtuvieron registros de la API.")
>>>>>>> main

        logging.info("Esperando 1 hora para la siguiente actualización...")
        time.sleep(900)
