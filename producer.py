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

UMBRAL_NO2 = 40 # µg/m³
UMBRAL_O3 = 100 # µg/m³
UMBRAL_PM10 = 50 # µg/m³
UMBRAL_PM25 = 25 # µg/m³

INTERVALO_MINUTOS = 1
TIEMPO_REPETICION_ALERTA = 3600  # 1 hora en segundos


### Configurar el producer de Kafka

conf = {
    'bootstrap.servers': os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
}
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@db:5432/data_project_1")
logging.basicConfig(level=logging.INFO)
producer = Producer(conf)

### Funciones

def json_serializer(obj):
    """Función auxiliar para convertir objetos no serializables a JSON"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
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

def enviar_alerta_poblacion(mensaje):
    producer.produce(
        topic='alertas_poblacion',
        value=json.dumps(mensaje, default=json_serializer, ensure_ascii=False).encode('utf-8')
    )
    producer.flush()

def evaluar_alertas(no2, o3, pm10, pm25):
    """Evalúa qué gases están en alerta y devuelve lista de alertas activas"""
    alertas = []
    if no2 is not None and no2 > UMBRAL_NO2:
        alertas.append(f"NO₂: {no2:.1f} µg/m³")
    if o3 is not None and o3 > UMBRAL_O3:
        alertas.append(f"O₃: {o3:.1f} µg/m³")
    if pm10 is not None and pm10 > UMBRAL_PM10:
        alertas.append(f"PM10: {pm10:.1f} µg/m³")
    if pm25 is not None and pm25 > UMBRAL_PM25:
        alertas.append(f"PM2.5: {pm25:.1f} µg/m³")
    return alertas

def format_value(val):
    """Formatea valor para log: número o N/D"""
    return f"{val:.1f}" if val is not None else "N/D"


### Lógica principal

estado_estaciones = {}

def revisar_calidad_aire():
    ahora = time.time()
    hora_legible = time.strftime('%H:%M:%S')
    print(f"\n[{hora_legible}] ⏳ Consultando sensores...")
    
    try:
        sensores = fetch_from_db()

        print(f"📡 Sensores encontrados: {len(sensores)}")
        if len(sensores) == 0:
            print("⚠️ LA BASE DE DATOS ESTÁ VACÍA.")
            return

        for sensor in sensores:
            nombre = sensor.get('nombre_estacion')
            city = sensor.get('city')
            
            if not nombre:
                continue

            # Obtener valores (mantener None si no hay datos)
            no2 = sensor.get('no2')
            o3 = sensor.get('o3')
            pm10 = sensor.get('pm10')
            pm25 = sensor.get('pm25')

            # Inicializar estado si no existe
            if nombre not in estado_estaciones:
                estado_estaciones[nombre] = {"alerta_activa": False, "ultimo_aviso": 0}

            estado = estado_estaciones[nombre]
            
            # Evaluar si hay alertas
            alertas = evaluar_alertas(no2, o3, pm10, pm25)
            hay_alerta = len(alertas) > 0
            
            # Determinar tipo de mensaje
            tiempo_pasado = ahora - estado["ultimo_aviso"]
            
            if hay_alerta:
                if not estado["alerta_activa"]:
                    tipo_aviso = "Activation"
                    print(f"🚨 NUEVA ALERTA en {nombre}: {', '.join(alertas)}")
                elif tiempo_pasado > TIEMPO_REPETICION_ALERTA:
                    tipo_aviso = "Recordatorio"
                    print(f"⏰ RECORDATORIO en {nombre}: {', '.join(alertas)}")
                else:
                    tipo_aviso = "Update"
                
                estado["alerta_activa"] = True
                estado["ultimo_aviso"] = ahora
                
            else:
                if estado["alerta_activa"]:
                    tipo_aviso = "Deactivation"
                    print(f"✅ ALERTA DESACTIVADA en {nombre}")
                    estado["alerta_activa"] = False
                else:
                    tipo_aviso = "Normal"
            
            # SIEMPRE enviar mensaje con todos los datos (None si no hay)
            mensaje = {
                "estacion": nombre,
                "city": city,
                "nivel_no2": no2,
                "nivel_o3": o3,
                "nivel_pm10": pm10,
                "nivel_pm25": pm25,
                "alerta_activa": hay_alerta,
                "tipo_aviso": tipo_aviso,
                "alertas_detalle": alertas,
                "lon": sensor.get('lon'),
                "lat": sensor.get('lat'),
                "fecha_carg": sensor.get('fecha_carg'),
                "fecha_envio": time.ctime()
            }
            
            enviar_alerta_poblacion(mensaje)
            
            # Log resumido
            estado_icono = "🔴" if hay_alerta else "🟢"
            print(f"   {estado_icono} {nombre}: NO₂={format_value(no2)} | O₃={format_value(o3)} | PM10={format_value(pm10)} | PM2.5={format_value(pm25)}")

    except Exception as e:
        print(f"❌ Error: {e}")


### Bucle
print(f"{'='*50}")
print(f"*** Monitor de Calidad del Aire Iniciado ***")
print(f"{'='*50}")
print(f"Umbrales:")
print(f"  NO₂:   {UMBRAL_NO2} µg/m³")
print(f"  O₃:    {UMBRAL_O3} µg/m³")
print(f"  PM10:  {UMBRAL_PM10} µg/m³")
print(f"  PM2.5: {UMBRAL_PM25} µg/m³")
print(f"Intervalo: {INTERVALO_MINUTOS * 10} segundos")
print(f"{'='*50}\n")

while True:
    revisar_calidad_aire()
    time.sleep(INTERVALO_MINUTOS * 10)