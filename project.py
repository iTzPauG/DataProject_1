import requests
import psycopg
import os
import time
from urllib.parse import urlparse

# -------------------------------
# Configuración de conexión
# -------------------------------
print("Conectando a la base de datos")
intentos_conexion = 0
conectado = False
while intentos_conexion < 5 and not conectado:
    try:
        # Conectamos a la base de datos
        conexion = psycopg.connect(os.getenv("DATABASE_URL"))
        if conexion:
            print("Conexión exitosa a la base de datos")
            conectado = True
        cursor = conexion.cursor()
    except Exception as e:
        print("Error al conectar a la base de datos, reintentando...")
        print(e)
        intentos_conexion += 1
        time.sleep(10)
        if intentos_conexion == 5:
            print("No se pudo conectar a la base de datos después de varios intentos.")
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
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("records", [])

# -------------------------------
# Función para guardar datos en Postgres
# -------------------------------
def save_to_postgres(records):
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not set")

    try:
        with psycopg.connect(db_url) as conn:
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
        # commit happens automatically when the context manager exits successfully
    except Exception as e:
        print("Error saving to Postgres:", e)
        raise
# -------------------------------
# Ejecución principal con actualización cada hora
# -------------------------------
if __name__ == "__main__":
    print("hola")
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
