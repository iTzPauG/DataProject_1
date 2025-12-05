import psycopg
import gspread
import os
from decimal import Decimal
from datetime import date, datetime
from dotenv import load_dotenv

load_dotenv()




# variables de conexión
# usamos 'localhost' porque el script corre en windows
host = "localhost"
port = os.getenv("DB_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")

def exportar_a_drive(tab):
    # Config googe sheets
    nombre_hoja = "DatosAire"
    archivo_llave = "credentials.json"
    tab = str(tab)


    print(f"Descargando datos de la DB, tabla {tab}...")
    
    try:
        # conexión con la DB
        with psycopg.connect(host=host, port=port, dbname=db_name, user=db_user, password=db_pass) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {tab}")
                
                # obtener nombres de columnas y filas
                encabezados = [desc[0] for desc in cur.description]
                datos_raw = cur.fetchall()

        print(f"{len(datos_raw)} registros encontrados")

        print("Subiendo a Google Sheets...")
        
        # conexión con google
        gc = gspread.service_account(filename=archivo_llave)
        hoja = gc.open(nombre_hoja)
        pestana = hoja.worksheet(tab)

        #Limpiamos para JSON
        datos_listos = []
        for fila in datos_raw:
            nueva_fila = []
            for celda in fila:
                # si es decimal
                if isinstance(celda, Decimal):
                    nueva_fila.append(float(celda))
                # si es fecha
                elif isinstance(celda, (date, datetime)):
                    nueva_fila.append(str(celda))
                # Else
                else:
                    nueva_fila.append(celda)
            datos_listos.append(nueva_fila)

        # limpiar y escribir
        pestana.clear()
        pestana.append_row(encabezados)
        pestana.append_rows(datos_listos)

        print(f"✅Hoja actualizada correctamente con la tabla {tab}.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    exportar_a_drive('estaciones')