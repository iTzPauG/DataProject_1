# Imagen base
FROM python:3.11-slim

# Instalamos git y dbt para Postgres
RUN apt-get update && \
    apt-get install -y git && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir "dbt-postgres==1.8.2" requests psycopg[binary]

# Directorio de trabajo
WORKDIR /usr/app/dbt

# Copiamos el script y requirements.txt si lo tuvieras
COPY project.py .
COPY requirements.txt .

# Instalamos dependencias adicionales si existen
RUN pip install --no-cache-dir -r requirements.txt || echo "No requirements.txt found"

# Ejecutar el script continuamente
ENTRYPOINT ["python", "project.py"]

