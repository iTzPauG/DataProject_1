import psycopg
from producer import fetch_from_db

sensores = fetch_from_db()

print(sensores)



# Configurar consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'grupo_exterior',  
    'auto.offset.reset': 'latest'
}
consumer = Consumer(conf)
consumer.subscribe(['eventos_exterior'])






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

