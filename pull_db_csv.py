import psycopg
import csv
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración en local
db_host = "localhost"
db_port = os.getenv("DB_PORT")
db_name = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")

nombre_archivo = "estaciones_export.csv"

def descargar_todo():
    print("Descargando el csv...")

    try:
        # Conexion
        with psycopg.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_pass
        ) as conn:
            
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM estaciones")
                
                # Creacion del archivo
                with open(nombre_archivo, mode='w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow([col[0] for col in cur.description])
                    writer.writerows(cur.fetchall())

        print(f"✅ Archivo '{nombre_archivo}' creado")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    descargar_todo()