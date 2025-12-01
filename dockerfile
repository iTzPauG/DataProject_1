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
    && pip install --no-cache-dir dbt-postgres==1.10.15

# Cambiar a usuario no root
USER appuser

# Ejecutar script al iniciar el contenedor
CMD ["python", "ingesta.py"]

