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

def fetch_from_db():
    """Return list of dict rows with the latest record for each station."""
    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (nombre_estacion)
                        id, nombre_estacion, lon, lat, city, no2, o3, pm10, pm25, fecha_carg, created_at
                    FROM mediciones
                    ORDER BY nombre_estacion, fecha_carg DESC;
                    """
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
    print(f"✅ Alerta enviada al topic 'alertas_poblacion'")

def enviar_alerta_CECOPI(mensaje): # Manda la alerta a Kafka (alertas_CECOPI))
    producer.produce(
        topic='alertas_CECOPI',
        value=json.dumps(mensaje, ensure_ascii=False).encode('utf-8')
    )
    producer.flush()
    print(f"✅ Alerta enviada al topic 'alertas_CECOPI'")


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
            nombre = sensor.get('nombre_estacion')
            valor_no2 = sensor.get('no2')
            valor_o3 = sensor.get('o3')
            valor_pm10 = sensor.get('pm10')
            valor_pm25 = sensor.get('pm25')
            city = sensor.get('city')

            # Si no hay nombre, saltamos este sensor
            if not nombre:
                print(f"⚠️ Sensor sin nombre, se omite.")
                continue

            # Si no hay valor de NO2, usamos 0 para la lógica de alertas
            if valor_no2 is None:
                valor_no2 = 0

            # Inicializamos el estado de la estación si es la primera vez que la vemos
            if nombre not in estado_estaciones:
                estado_estaciones[nombre] = {"activa": False, "ultimo_aviso": 0}

            estado = estado_estaciones[nombre]
            
            # Datos base del mensaje (siempre incluidos)
            datos_base = {
                "estacion": nombre,
                "city": city,
                "nivel_no2": valor_no2,
                "nivel_o3": valor_o3,
                "nivel_pm10": valor_pm10,
                "nivel_pm25": valor_pm25,
                "lon": sensor.get('lon'),
                "lat": sensor.get('lat'),
                "fecha_carg": sensor.get('fecha_carg'),
                "fecha_envio": time.ctime()
            }

            ##### Lógica de la alerta #####
            
            # CASO A: Supera el umbral
            if valor_no2 > UMBRAL_NO2:
                # Calculamos cuánto tiempo pasó desde el último aviso
                tiempo_pasado = ahora - estado["ultimo_aviso"]
                
                # Condición: Si NO estaba activa O si ya pasó un día (recordatorio)
                if not estado["activa"] or tiempo_pasado > TIEMPO_REPETICION_ALERTA:
                    print("-" * 40)
                    if estado["activa"]:
                        mensaje = {
                            **datos_base,
                            "tipo_aviso": "Recordatorio",
                            "alerta_activa": True,
                            "texto": f"Recordatorio: El nivel de NO2 en {nombre} ({city}) sigue alto: {valor_no2} µg/m³.",
                        }
                        print(f"⏰ RECORDATORIO DIARIO en {nombre} a las {time.ctime()}")
                    else:
                        mensaje = {
                            **datos_base,
                            "tipo_aviso": "Activation",
                            "alerta_activa": True,
                            "texto": f"ALERTA: El nivel de NO2 en {nombre} ({city}) ha subido por encima del límite seguro. Valor actual: {valor_no2} µg/m³.",
                        }
                        print(f"🚨 NUEVA ALERTA en {nombre}. Fecha de envío:{time.ctime()}")
                    
                    print(f"   NO2: {valor_no2} µg/m³ | O3: {valor_o3} µg/m³ | PM10: {valor_pm10} µg/m³ | PM2.5: {valor_pm25} µg/m³")
                    print("-" * 40)
                    
                    # Actualizamos el estado
                    estado["activa"] = True
                    estado["ultimo_aviso"] = ahora
            
            # CASO B: Ya no supera el umbral (Bajada de nivel)
            else:
                # Solo avisamos si antes ESTABA activa (la situación ha mejorado)
                if estado["activa"]:
                    mensaje = {
                        **datos_base,
                        "tipo_aviso": "Deactivation",
                        "alerta_activa": False,
                        "texto": f"ALERTA: El nivel de NO2 en {nombre} ({city}) se ha restablecido a niveles seguros. Valor actual: {valor_no2} µg/m³.",
                    }
                    print(f"✅ NIVEL RESTABLECIDO en {nombre}")
                    print(f"   NO2: {valor_no2} µg/m³ | O3: {valor_o3} µg/m³ | PM10: {valor_pm10} µg/m³ | PM2.5: {valor_pm25} µg/m³")

                    estado["activa"] = False
                    
            if mensaje:
                enviar_alerta_poblacion(mensaje)
                logging.info(f"Mensaje enviado para {nombre}")
            else:
                logging.debug("Sin cambios de estado para %s", nombre)

    except Exception as e:
        print(f"❌ Error conectando la Base de datos: {e}")


### Bucle
print(f"*** Monitor Iniciado ***")
print(f"- Umbral: {UMBRAL_NO2} µg/m³")
print(f"- Recordatorio de alerta persistente: Cada 24 horas")

while True:
    revisar_calidad_aire()
    time.sleep(INTERVALO_MINUTOS * 10)