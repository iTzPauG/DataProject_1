from confluent_kafka import Producer, Consumer
import json
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
            
            # Extraemos los datos
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

