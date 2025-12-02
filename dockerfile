FROM python:3.11-slim

# Crear usuario no root
RUN useradd -m appuser

# Directorio de trabajo
WORKDIR /app

RUN mkdir -p /shared && chown -R appuser:appuser /shared

COPY requirements.txt .
COPY ingesta.py .
COPY producer.py .
COPY kafka_consumer.py .
COPY dashboard_alertas.py .

# Instalar dependencias
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    # Dockerfile, Línea 20
    && pip install --no-cache-dir dbt-postgres

# Cambiar a usuario no root
USER appuser

# Ejecutar script al iniciar el contenedor
CMD ["python", "ingesta.py"]

