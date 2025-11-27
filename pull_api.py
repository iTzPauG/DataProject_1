import requests
import time
import os
import logging
import psycopg
from confluent_kafka import Producer
import json
from datetime import date, datetime
from decimal import Decimal


### Panel de control

API_URL = "https://valencia.opendatasoft.com/api/explore/v2.1/catalog/datasets/estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas/records?limit=20"

UMBRAL_NO2 = 10 # µg/m³
INTERVALO_MINUTOS = 1
TIEMPO_REPETICION_ALERTA = 60  # 24 horas en segundos


### Configurar el producer de Kafka

conf = {
    # inside docker-compose use kafka:29092 (internal advertised listener)
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
}
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/data_project_1")
logging.basicConfig(level=logging.INFO)
producer = Producer(conf)

### Funciones

def json_serializer(obj): # Cambia las variables datetime y decimal a formatos serializables JSON para ingestarlos en el producer
    """Función auxiliar para convertir objetos no serializables a JSON"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()  # Convierte fecha a texto ISO 8601
    if isinstance(obj, Decimal):
        return float(obj)       # Convierte Decimal a float
    raise TypeError(f"Type {type(obj)} not serializable")


def fetch_from_db(): # Consulta la BD
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

def enviar_alerta_poblacion(mensaje): # Manda la alerta a Kafka (alertas_poblacion)
    producer.produce(
        topic='alertas_poblacion',
        value=json.dumps(mensaje, default=json_serializer, ensure_ascii=False).encode('utf-8')
    )
    producer.flush()
    print(f"✅ Alerta enviada al topic 'alertas_confirmadas'")

def enviar_alerta_CECOPI(mensaje): # Manda la alerta a Kafka (alertas_CECOPI))
    producer.produce(
        topic='alertas_CECOPI',
        value=json.dumps(mensaje, ensure_ascii=False).encode('utf-8')
    )
    producer.flush()
    print(f"✅ Alerta enviada al topic 'alertas_confirmadas'")


### Lógica principal

estado_estaciones = {} # Diccionario para guardar el estado de cada estación. Estructura: { "NombreEstacion": { "activa": True/False, "ultimo_aviso": tiempo_unix } }

def revisar_calidad_aire():
    ahora = time.time()
    hora_legible = time.strftime('%H:%M:%S')
    print(f"[{hora_legible}] ⏳ Consultando sensores (desde DB)...")
    
    try:
        mensaje = {}
        sensores = fetch_from_db()

        print(f"🔍 DEBUG: Se han encontrado {len(sensores)} sensores en la BD.")
        if len(sensores) == 0:
            print("⚠️ LA BASE DE DATOS ESTÁ VACÍA O NO DEVUELVE DATOS.")

        for sensor in sensores:
            mensaje = {}
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
                        mensaje = {
                            "estacion": nombre,
                            "tipo_aviso": "Recordatorio",
                            "nivel_no2": valor,
                            "alerta_activa": True,
                            "texto": f"Recordatorio: El nivel de NO2 en {nombre} sigue alto: {valor} µg/m³.",
                            "fecha_carg": sensor.get('fecha_carg'),
                            "fecha_envío" : time.ctime()
                        }
                        print(f"⏰ RECORDATORIO DIARIO en {nombre} a las {time.ctime()}")
                    else:
                        mensaje = {
                            "estacion": nombre,
                            "tipo_aviso": "Activation",
                            "nivel_no2": valor,
                            "alerta_activa": True,
                            "texto": f"ALERTA: El nivel de NO2 en {nombre} ha subido por encima del límite seguro. Valor actual: {valor} µg/m³.",
                            "fecha_carg": sensor.get('fecha_carg'),
                            "fecha_envío" : time.ctime()
                        }
                        print(f"🚨 NUEVA ALERTA en {nombre}. Fecha de envío:{time.ctime()}")
                    
                    print(f"   Nivel NO2: {valor} µg/m³ (Límite: {UMBRAL_NO2})")
                    print("-" * 40)
                    
                    # Actualizamos el estado
                    estado["activa"] = True
                    estado["ultimo_aviso"] = ahora
            
            # CASO B: Ya no supera el umbral (Bajada de nivel)
            else:
                # Solo avisamos si antes ESTABA activa (la situación ha mejorado)
                if estado["activa"]:
                    mensaje = {
                            "estacion": nombre,
                            "tipo_aviso": "Deactivation",
                            "nivel_no2": valor,
                            "alerta_activa": False,
                            "texto": f"ALERTA: El nivel de NO2 en {nombre} se ha restablecido a niveles seguros. Valor actual: {valor} µg/m³.",
                            "fecha_carg": sensor.get('fecha_carg'),
                            "fecha_envío" : time.ctime()
                    }
                    print(f"✅ NIVEL RESTABLECIDO en {nombre}")
                    print(f"   El nivel ha bajado a {valor} µg/m³.")

                    estado["activa"] = False
            if mensaje:   # only send populated messages
                enviar_alerta_poblacion(mensaje)
                logging.info(f"Mensaje {mensaje} enviado")
            else: logging.error("Mensaje no enviado")

    except Exception as e:
        print(f"❌ Error conectando la Base de datos: {e}")


### Bucle
print(f"*** Monitor Iniciado ***")
print(f"- Umbral: {UMBRAL_NO2} µg/m³")
print(f"- Recordatorio de alerta persistente: Cada 24 horas")

while True:
    revisar_calidad_aire()
    time.sleep(INTERVALO_MINUTOS * 10)