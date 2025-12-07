import time
import os
import logging
import psycopg
import requests
from urllib.parse import urlparse, urlunparse
from pull_db_gsheets import exportar_a_drive

# =======================================================
# VARIABLES GLOBALES
# =======================================================
API_HOST = os.getenv("API_HOST", "api")
API_PORT = os.getenv("API_PORT", "5000")
API_URL = f"http://{API_HOST}:{API_PORT}"

SLEEPTIME=900

# =======================================================
# LOGGING
# =======================================================
log_level = os.getenv("LOG_LEVEL")
logging.basicConfig(level=getattr(logging, log_level), format="%(asctime)s [%(levelname)s] %(message)s")

# ====================================
# FUNCION ESPERAR CONEXIÓN
# ====================================
def wait_for_api():
    for _ in range(10):
        try:
            resp = requests.get(f"{API_URL}/health")
            if resp.status_code == 200:
                logging.info("✅ Conexión exitosa a la API.")
                return
        except Exception as e:
            logging.warning(f"Esperando conexión a API: {e}")
        time.sleep(3)
    logging.error("❌ No se pudo conectar a la API después de varios intentos.")
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
# ENVIAR DATOS A LA API /valencia
# =======================================================
def post_api(records):
    inserted_count = 0
    dup_count = 0
    error_count = 0
    
    for rec in records:
        fields = rec.get("fields", {})
        
        try:
            resp = requests.post(
                f"{API_URL}/valencia",
                json=fields,
                headers={"Content-Type": "application/json"}
            )
            
            if resp.status_code == 201:
                inserted_count += 1
            elif resp.status_code== 200:
                dup_count +=1
            else :
                error_count += 1
                logging.warning(f"{resp.json()}")
                
        except Exception as e:
            error_count += 1
            logging.error(f"Error enviando datos a API: {e}")
    
    logging.info(f"✅ {inserted_count} registros enviados correctamente,{dup_count} duplicados, {error_count} errores.")

# =======================================================
# MAIN
# =======================================================
if __name__ == "__main__":
    logging.info("🚀 Iniciando script de ingesta...")

    # 1. Esperar conexión a la API
    wait_for_api()

    # 2. Bucle de actualización
    while True:
        records = fetch_data()
        if records:
            post_api(records)
            exportar_a_drive('mediciones')
            exportar_a_drive('mart_monthly_promedio')
            exportar_a_drive('mart_hourly')
        else:
            logging.warning("⚠️ No se obtuvieron registros de la API externa.")

        logging.info(f"⏳ Esperando {int(SLEEPTIME / 60)} minutos para la siguiente actualización...")
        time.sleep(SLEEPTIME)
