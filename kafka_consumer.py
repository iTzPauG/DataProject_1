import json
import os
from confluent_kafka import Consumer
import time 

ALERTAS_FILE = os.getenv("ALERTAS_FILE", "/shared/alertas.json")

# Buffer en memoria (opcional, para mantener compatibilidad)
alertas_buffer = []

def load_alertas():
    """Cargar alertas existentes desde el archivo JSON."""
    try:
        with open(ALERTAS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_alertas(alertas):
    """Guardar alertas en el archivo JSON compartido."""
    os.makedirs(os.path.dirname(ALERTAS_FILE), exist_ok=True)
    with open(ALERTAS_FILE, "w") as f:
        json.dump(alertas, f, indent=2)

# 1. Configuración básica
conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:29092'),
    'group.id': 'grupo_poblacion',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['alertas_poblacion'])

print(f"Conectado a: {conf['bootstrap.servers']}, esperando mensajes")

# Cargar alertas existentes al iniciar
alertas_buffer = load_alertas()
print(f"✅ Cargadas {len(alertas_buffer)} alertas existentes\n")

try:
    while True:
        # Buscamos mensajes cada 1 segundo
        msg = consumer.poll(5.0)

        if msg is None:
            print(".", end="", flush=True)
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        print(f"\n📩 Mensaje recibido!")
        # Decodificamos el mensaje que llega en bytes
        texto = msg.value().decode('utf-8')

        # Intentamos leerlo como JSON (para tu sistema de alertas)
        try:
            datos = json.loads(texto)
            
            # Extraemos los datos que te interesan
            estacion = datos.get("estacion")
            estado = datos.get("alerta_activa")
            valor = datos.get("nivel_no2")
            fecha_carg = datos.get("fecha_carg", "")

            if estado == True:
                print(f"!!! ALERTA DETECTADA en: {estacion} !!!")
                print(f"Datos completos: {datos}")
                # Agregar al buffer y guardar
                alertas_buffer.append({
                    "nombre": estacion,
                    "nivel_no2": valor,
                    "fecha_carg": fecha_carg
                })
                save_alertas(alertas_buffer)
            else:
                print(f"Mensaje normal de {estacion}: Niveles de NO2: {valor} µg/m³")

        # Si el mensaje NO es JSON (ej. "Producer iniciado"), entra aquí
        except json.JSONDecodeError:
            print(f"Mensaje no-JSON: {texto}")

except KeyboardInterrupt:
    print(f"Mensaje no-JSON: {texto}")
    save_alertas(alertas_buffer)
    consumer.close()
