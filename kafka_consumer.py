import json
import os
from confluent_kafka import Consumer
import threading
import os

ALERTAS_FILE = os.getenv("ALERTAS_FILE", "/shared/alertas.json")
alertas_buffer = []
def load_alertas():
    """Load alerts from shared JSON file on startup."""
    try:
        with open(ALERTAS_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []

def save_alertas(alertas):
    """Persist alerts to shared JSON file."""
    os.makedirs(os.path.dirname(ALERTAS_FILE), exist_ok=True)
    with open(ALERTAS_FILE, "w") as f:
        json.dump(alertas, f, indent=2)

def kafka_listener():
    global alertas_buffer
    # 1. Configuración básica
    conf = {
        'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
        'group.id': 'grupo_poblacion',
        'auto.offset.reset': 'latest'
    }

    consumer = Consumer(conf)
    consumer.subscribe(['alertas_poblacion'])

    print("--- Consumidor Iniciado. Esperando mensajes ---")
    alertas_buffer = load_alertas()
    print(f"Loaded {len(alertas_buffer)} existing alerts from file")
    try:
        while True:
            # Buscamos mensajes cada 1 segundo
            msg = consumer.poll(5.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            # Decodificamos el mensaje que llega en bytes
            texto = msg.value().decode('utf-8')

            # Intentamos leerlo como JSON (para tu sistema de alertas)
            try:
                datos = json.loads(texto)
                
                # Extraemos los datos que te interesan
                estacion = datos.get("estacion")
                estado = datos.get("alerta_activa")
                valor = datos.get("nivel_no2")

                if estado == True:
                    print(f"!!! ALERTA DETECTADA en: {estacion} !!!")
                    print(f"Datos completos: {datos}")
                else:
                    print(f"Mensaje normal de {estacion}: Niveles de NO2: {valor} µg/m³")

                # Si el mensaje NO es JSON (ej. "Producer iniciado"), entra aquí
            except json.JSONDecodeError:
                    print(f"Error al decodificar JSON: {texto}")

    except KeyboardInterrupt:
        print("Consumidor detenido.")
        save_alertas(alertas_buffer)
        consumer.close()

if __name__ == "__main__":
    kafka_listener()