import requests
import time
import os
import logging
import psycopg
from confluent_kafka import Producer
import json
#configurar Kafka
conf = {
    # inside docker-compose use kafka:29092 (internal advertised listener)
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
}
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/data_project_1")
logging.basicConfig(level=logging.INFO)
producer = Producer(conf)

#TESTEANDO KAFKA - BORRAR DESPUÉS
mensaje_prueba = {
    "nombre": "Sistema",
    "estado": "alerta",
    "alerta_tipo": "Inicio de Producer",
    "alerta_icono": "🔵"
}
producer.produce(
    topic='alertas_poblacion',
    value=json.dumps(mensaje_prueba, ensure_ascii=False).encode('utf-8')
    )
producer.flush()
## FIN DEL TEST DE KAFKA ##

#####################
# CONSULTA BD
#####################
def fetch_from_db():
    """Return list of dict rows from estaciones table that share the latest created_at timestamp."""
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                # fetch latest created_at timestamp
                cur.execute("SELECT MAX(created_at) AS latest_ts FROM estaciones;")
                row = cur.fetchone()
                latest_ts = row["latest_ts"] if row else None
                if not latest_ts:
                    return []

                # return rows that have that latest timestamp
                cur.execute(
                    """
                    SELECT objectid, nombre, no2, lon, lat, fecha_carg, created_at
                    FROM estaciones
                    WHERE created_at = %s
                    ORDER BY objectid;
                    """,
                    (latest_ts,),
                )
                return cur.fetchall()
    except Exception as e:
        logging.error("DB fetch error: %s", e)
        return []

def enviar_alerta_poblacion(mensaje):
    producer.produce(
        topic='alertas_poblacion',
        value=json.dumps(mensaje, ensure_ascii=False).encode('utf-8')
    )
    producer.flush()
    print(f"✅ Alerta enviada al topic 'alertas_confirmadas'")

def enviar_alerta_CECOPI(mensaje):
    producer.produce(
        topic='alertas_CECOPI',
        value=json.dumps(mensaje, ensure_ascii=False).encode('utf-8')
    )
    producer.flush()
    print(f"✅ Alerta enviada al topic 'alertas_confirmadas'")

# Panel de control
API_URL = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records?limit=20"

UMBRAL_NO2 = 50  # µg/m³
INTERVALO_MINUTOS = 1
TIEMPO_REPETICION_ALERTA = 2 * 60 * 60  # 24 horas en segundos

# DICCIONARIO PARA GUARDAR EL ESTADO DE CADA ESTACIÓN
# Estructura: { "NombreEstacion": { "activa": True/False, "ultimo_aviso": tiempo_unix } }
estado_estaciones = {} 

def revisar_calidad_aire():
    ahora = time.time()
    hora_legible = time.strftime('%H:%M:%S')
    print(f"[{hora_legible}] ⏳ Consultando sensores (desde DB)...")
    
    try:
        mensaje = {}
        sensores = fetch_from_db()

        for sensor in sensores:
            # 1. Obtener datos básicos
            nombre = sensor.get('nombre')
            valor = sensor.get('no2')

            # Si no hay valor o nombre, saltamos este sensor
            if valor is None or not nombre:
                print(f"⚠️ Datos incompletos para el sensor {nombre}, se omite.")
                continue

            # Inicializamos el estado de la estación si es la primera vez que la vemos
            if nombre not in estado_estaciones:
                estado_estaciones[nombre] = {"activa": False, "ultimo_aviso": 0}

            estado = estado_estaciones[nombre]
            

            ##### Lógica de la alerta #####
            
            # CASO A: Supera el umbral
            if valor > UMBRAL_NO2:
                # Calculamos cuánto tiempo pasó desde el último aviso
                tiempo_pasado = ahora - estado["ultimo_aviso"]
                
                # Condición: Si NO estaba activa O si ya pasó un día (recordatorio)
                if not estado["activa"] or tiempo_pasado > TIEMPO_REPETICION_ALERTA:
                    print("-" * 40)
                    if estado["activa"]:
                        print(f"⏰ RECORDATORIO DIARIO en {nombre}")
                    else:
                        print(f"🚨 NUEVA ALERTA en {nombre}")
                    
                    print(f"   Nivel NO2: {valor} µg/m³ (Límite: {UMBRAL_NO2})")
                    print("-" * 40)
                    
                    # Actualizamos el estado
                    estado["activa"] = True
                    estado["ultimo_aviso"] = ahora
            
            # CASO B: Ya no supera el umbral (Bajada de nivel)
            else:
                # Solo avisamos si antes ESTABA activa (la situación ha mejorado)
                if estado["activa"]:
                    print(f"✅ NIVEL RESTABLECIDO en {nombre}")
                    print(f"   El nivel ha bajado a {valor} µg/m³.")
                    #Ahora hacemos el mensaje para Kafka
                    mensaje= {
                        "estacion": nombre,
                        "nivel_no2": valor,
                        "estado": "restablecido",
                        "texto": f"El nivel de NO2 en {nombre} ha bajado a {valor} µg/m³."
                    }
                    
                    # Reseteamos la alerta a False
                    estado["activa"] = False
            if mensaje:   # only send populated messages
                enviar_alerta_poblacion(mensaje)
                logging.info(f"Mensaje {mensaje} enviado")
            else: logging.error("Mensaje no enviado")

    except Exception as e:
        print(f"❌ Error conectando la API: {e}")

# Bucle
print(f"*** Monitor Iniciado ***")
print(f"- Umbral: {UMBRAL_NO2} µg/m³")
print(f"- Recordatorio de alerta persistente: Cada 24 horas")

while True:
    revisar_calidad_aire()
    time.sleep(INTERVALO_MINUTOS * 60)