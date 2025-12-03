import time
import os
import logging
import requests

# =======================================================
# VARIABLES GLOBALES
# =======================================================
API_HOST = os.getenv("API_HOST", "api")
API_PORT = os.getenv("API_PORT", "5000")
API_URL = f"http://{API_HOST}:{API_PORT}"

SLEEPTIME = 900  # 15 minutos

# =======================================================
# LOGGING
# =======================================================
log_level = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(level=getattr(logging, log_level), format="%(asctime)s [%(levelname)s] %(message)s")

# =======================================================
# MAPEO MAGNITUDES MADRID (según documentación)
# =======================================================
MAGNITUDES_MADRID = {
    8: "no2",
    14: "o3",
    10: "pm10",
    9: "pm25"
}

# =======================================================
# CATÁLOGO ESTACIONES MADRID (solo ALTA, código = últimos dígitos)
# =======================================================
ESTACIONES_MADRID = {
    4: {"nombre": "Pza. de España", "lat": 40.4238823, "lon": -3.7122567},
    8: {"nombre": "Escuelas Aguirre", "lat": 40.4215533, "lon": -3.6823158},
    11: {"nombre": "Avda. Ramón y Cajal", "lat": 40.4514734, "lon": -3.6773491},
    16: {"nombre": "Arturo Soria", "lat": 40.4400457, "lon": -3.6392422},
    17: {"nombre": "Villaverde Alto", "lat": 40.3478988, "lon": -3.7133355},
    18: {"nombre": "Farolillo", "lat": 40.3947825, "lon": -3.7318356},
    24: {"nombre": "Casa de Campo", "lat": 40.4193577, "lon": -3.7473445},
    27: {"nombre": "Barajas Pueblo", "lat": 40.4769179, "lon": -3.5800258},
    35: {"nombre": "Pza. del Carmen", "lat": 40.4192091, "lon": -3.7031662},
    36: {"nombre": "Moratalaz", "lat": 40.4079517, "lon": -3.6453104},
    38: {"nombre": "Cuatro Caminos", "lat": 40.4455439, "lon": -3.7071173},
    39: {"nombre": "Barrio del Pilar", "lat": 40.4782322, "lon": -3.7115364},
    40: {"nombre": "Vallecas", "lat": 40.3881478, "lon": -3.6515286},
    47: {"nombre": "Méndez Álvaro", "lat": 40.3980991, "lon": -3.6868138},
    48: {"nombre": "Castellana", "lat": 40.4398904, "lon": -3.6903729},
    49: {"nombre": "Retiro", "lat": 40.4144444, "lon": -3.6824999},
    50: {"nombre": "Pza. Castilla", "lat": 40.4655841, "lon": -3.6887449},
    54: {"nombre": "Ensanche de Vallecas", "lat": 40.3730118, "lon": -3.6121394},
    55: {"nombre": "Urb. Embajada", "lat": 40.4623628, "lon": -3.5805649},
    56: {"nombre": "Plaza Elíptica", "lat": 40.3850336, "lon": -3.7187679},
    57: {"nombre": "Sanchinarro", "lat": 40.4942012, "lon": -3.6605173},
    58: {"nombre": "El Pardo", "lat": 40.5180701, "lon": -3.7746101},
    59: {"nombre": "Parque Juan Carlos I", "lat": 40.4607255, "lon": -3.6163407},
    60: {"nombre": "Tres Olivos", "lat": 40.5005477, "lon": -3.6897308}
}

# ====================================
# FUNCION ESPERAR CONEXIÓN
# ====================================
def wait_for_api():
    for _ in range(10):
        try:
            resp = requests.get(f"{API_URL}/health")
            if resp.status_code == 200:
                logging.info("✅ [Madrid] Conexión exitosa a la API interna.")
                return
        except Exception as e:
            logging.warning(f"[Madrid] Esperando conexión a API interna: {e}")
        time.sleep(3)
    logging.error("❌ [Madrid] No se pudo conectar a la API después de varios intentos.")
    exit(1)

# =======================================================
# OBTENER ÚLTIMA HORA VÁLIDA DE UN REGISTRO
# =======================================================
def get_ultima_hora_valida(registro):
    """Obtiene el valor y hora de la última medición válida (V)."""
    for i in range(24, 0, -1):
        h_key = f"H{str(i).zfill(2)}"
        v_key = f"V{str(i).zfill(2)}"
        
        valor_validez = registro.get(v_key, "")
        if valor_validez == "V":
            try:
                valor = float(registro.get(h_key, 0))
                return valor, i
            except (ValueError, TypeError):
                pass
    return None, None

# =======================================================
# TRANSFORMAR DATOS DE MADRID AL FORMATO ESTÁNDAR
# =======================================================
def transformar_datos(records):
    """
    Agrupa registros por estación y extrae la última medición.
    """
    estaciones = {}
    
    for registro in records:
        # Convertir a entero para comparar con diccionarios
        try:
            estacion_id = int(registro.get("ESTACION", 0))
            magnitud_id = int(registro.get("MAGNITUD", 0))
        except (ValueError, TypeError):
            continue
        
        # Filtrar estaciones y magnitudes conocidas
        if estacion_id not in ESTACIONES_MADRID:
            continue
        if magnitud_id not in MAGNITUDES_MADRID:
            continue
        
        # Obtener última hora válida
        valor, hora = get_ultima_hora_valida(registro)
        if valor is None:
            continue
        
        # Construir fecha
        ano = registro.get("ANO", "")
        mes = str(registro.get("MES", "")).zfill(2)
        dia = str(registro.get("DIA", "")).zfill(2)
        fecha_hora = f"{ano}-{mes}-{dia} {str(hora).zfill(2)}:00:00"
        
        # Crear entrada para estación si no existe
        if estacion_id not in estaciones:
            info = ESTACIONES_MADRID[estacion_id]
            estaciones[estacion_id] = {
                "nombre": info["nombre"],
                "lat": info["lat"],
                "lon": info["lon"],
                "no2": None,
                "o3": None,
                "pm10": None,
                "pm25": None,
                "fecha_carg": fecha_hora
            }
        
        # Asignar valor al contaminante
        nombre_contaminante = MAGNITUDES_MADRID[magnitud_id]
        estaciones[estacion_id][nombre_contaminante] = valor
        
        # Actualizar fecha si es más reciente
        if fecha_hora > estaciones[estacion_id]["fecha_carg"]:
            estaciones[estacion_id]["fecha_carg"] = fecha_hora
    
    # Log para debug
    for est_id, datos in estaciones.items():
        logging.debug(f"[Madrid] Estación {est_id} ({datos['nombre']}): NO2={datos['no2']}, O3={datos['o3']}, PM10={datos['pm10']}, PM25={datos['pm25']}")
    
    return list(estaciones.values())

# =======================================================
# DESCARGAR DATOS DE LA API DE MADRID
# =======================================================
def fetch_data():
    url = "https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json"
    }

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        
        records = resp.json().get("records", [])
        logging.info(f"[Madrid] 📥 Obtenidos {len(records)} registros de la API externa.")
        
        registros_transformados = transformar_datos(records)
        logging.info(f"[Madrid] 🔄 Transformados a {len(registros_transformados)} estaciones.")
        
        return registros_transformados

    except Exception as e:
        logging.error(f"[Madrid] Error al obtener datos: {e}")
        return []

# =======================================================
# ENVIAR DATOS A API INTERNA
# =======================================================
def post_api(records):
    inserted = duplicated = errors = 0
    
    for rec in records:
        try:
            resp = requests.post(f"{API_URL}/madrid", json=rec, headers={"Content-Type": "application/json"})
            if resp.status_code == 201:
                inserted += 1
                logging.info(f"[Madrid] ✅ Insertado: {rec['nombre']} - NO2={rec['no2']}, O3={rec['o3']}, PM10={rec['pm10']}, PM25={rec['pm25']}")
            elif resp.status_code == 200:
                duplicated += 1
            else:
                errors += 1
                logging.warning(f"[Madrid] ⚠️ Error insertando {rec['nombre']}: {resp.text}")
        except Exception as e:
            errors += 1
            logging.error(f"[Madrid] Error: {e}")
    
    logging.info(f"✅ [Madrid] {inserted} insertados, {duplicated} duplicados, {errors} errores.")

# =======================================================
# MAIN
# =======================================================
if __name__ == "__main__":
    logging.info("🐻 Iniciando script de ingesta (Madrid)...")
    wait_for_api()

    while True:
        records = fetch_data()
        if records:
            post_api(records)
        else:
            logging.warning("[Madrid] ⚠️ No se obtuvieron registros.")

        logging.info(f"[Madrid] ⏳ Esperando {int(SLEEPTIME / 60)} minutos...")
        time.sleep(SLEEPTIME)