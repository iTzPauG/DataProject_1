import json
from confluent_kafka import Consumer

# 1. Configuración básica
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'grupo_poblacion',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(conf)
consumer.subscribe(['alertas_poblacion'])

print("--- Consumidor Iniciado. Esperando mensajes ---")

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
    consumer.close()