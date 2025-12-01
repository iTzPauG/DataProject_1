from confluent_kafka import Producer, Consumer
import json
import time
#from producer import fetch_from_db

# sensores = fetch_from_db()

# print(sensores)
estaciones = {
    1: "Universidad Politècnica",
    2: "Centro",
    3: "Dr. Lluch",
    4: "Patraix",
    5: "Cabanyal",
    6: "Viveros",
    7: "Olivereta",
    8: "Francia",
    9: "Boulevar Sur",
    10: "Molí del Sol",
    11: "Pista de silla"
}

print("Zonas disponibles para alertas de población:")
for estacion in estaciones:
    print(f"{estacion}. - {estaciones[estacion]}")

selection = input("""Selecciona el número de la zona para la que quieres escuchar las alertas:""")


# Configurar consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'alertas_poblacion',  
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)
consumer.subscribe(['alertas_zona_{estaciones[int(selection)]}'])

print(f"Conectado a alertas para la zona de: {estaciones[int(selection)]}, esperando mensajes...\n")


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        datos = json.loads(msg.value().decode('utf-8'))
        momento = time.strftime("%Y-%m-%d %H:%M:%S")

        print(f"\n📩 Alerta recibida para la zona de {estaciones[int(selection)]}!")           


except KeyboardInterrupt:
    print("Monitoreo de la zona {estaciones[int(selection)]} detenido.")
finally:
    consumer.close()

