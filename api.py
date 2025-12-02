from flask import Flask, jsonify, request
from flask_httpauth import HTTPBasicAuth
from werkzeug.security import generate_password_hash, check_password_hash
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
import psycopg
import os
import logging

app = Flask(__name__)
auth = HTTPBasicAuth()

# =======================================================
# LOGGING
# =======================================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# =======================================================
# DATABASE CONNECTION
# =======================================================
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

SERVER_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/postgres"
DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# =======================================================
# MAPEO
# =======================================================
MAP_CIUDADES = {
    "valencia": {
        "nombre_estacion": "nombre",
        "lon": ["geo_point_2d", "lon"],
        "lat": ["geo_point_2d", "lat"],
        "no2": "no2",
        "o3": "o3",
        "pm10": "pm10",
        "pm25": "pm25",
        "fecha_carg": "fecha_carg"
    }
}

# =======================================================
# CREATE TABLE IF NOT EXISTS
# =======================================================
def create_table_if_not_exists():
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS mediciones (
                        id SERIAL PRIMARY KEY,
                        nombre_estacion TEXT,
                        lon NUMERIC,
                        lat NUMERIC,
                        city TEXT,
                        no2 NUMERIC,
                        o3 NUMERIC,
                        pm10 NUMERIC,
                        pm25 NUMERIC,
                        fecha_carg TIMESTAMP,
                        created_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mediciones_fecha 
                    ON mediciones(fecha_carg);
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mediciones_city 
                    ON mediciones(city);
                """)
                logging.info("✅ Tabla 'mediciones' verificada/creada")
    except Exception as e:
        logging.error(f"Error creando tabla: {e}")

# =======================================================
# EXTRACT VALUE (SUPPORTS NESTED FIELDS)
# =======================================================
def extraer_campo(data, field_path):
    if field_path is None or data is None:
        return None
    
    # If field_path is a list, it's a nested path
    if isinstance(field_path, list):
        value = data
        for key in field_path:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value
    
    # Simple field
    return data.get(field_path)

# =======================================================
# INSERT MEASUREMENT
# =======================================================
def insertar(nombre_estacion, lon, lat, city, no2, o3, pm10, pm25, fecha_carg):
    try:
        with psycopg.connect(DB_URL) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO mediciones (
                        nombre_estacion, lon, lat, city,
                        no2, o3, pm10, pm25, fecha_carg
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    nombre_estacion,
                    lon,
                    lat,
                    city,
                    no2,
                    o3,
                    pm10,
                    pm25,
                    fecha_carg
                ))
                inserted_id = cur.fetchone()[0]
                logging.info(f"✅ Registro insertado con id: {inserted_id} para {city}")
                return inserted_id
    except Exception as e:
        logging.error(f"Error insertando datos: {e}")
        raise e

# =======================================================
# GENERIC CITY HANDLER
# =======================================================
def handle_city_request(city):
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400
        
        mapping = MAP_CIUDADES.get(city.lower())
        if not mapping:
            return jsonify({'error': f'Ciudad {city} no implementada'}), 400
        
        create_table_if_not_exists()
        
        inserted_id = insertar(
            nombre_estacion=extraer_campo(data, mapping.get("nombre_estacion")),
            lon=extraer_campo(data, mapping.get("lon")),
            lat=extraer_campo(data, mapping.get("lat")),
            city=city,
            no2=extraer_campo(data, mapping.get("no2")),
            o3=extraer_campo(data, mapping.get("o3")),
            pm10=extraer_campo(data, mapping.get("pm10")),
            pm25=extraer_campo(data, mapping.get("pm25")),
            fecha_carg=extraer_campo(data, mapping.get("fecha_carg"))
        )
        
        return jsonify({
            'message': 'Insertado con éxito',
            'id': inserted_id,
            'city': city
        }), 201
        
    except Exception as e:
        logging.error(f"Error en /{city}: {e}")
        return jsonify({'error': str(e)}), 500

# =======================================================
# ROUTES
# =======================================================
@app.route('/valencia', methods=['POST'])
def valencia():
    return handle_city_request("valencia")


@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'healthy'}), 200

@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Resource not found.'}), 404

@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({'error': 'Method not allowed.'}), 405

if __name__ == '__main__':
    create_table_if_not_exists()
    app.run(host='0.0.0.0', port=5000, debug=True)