FROM python:3.11-slim

# Crear usuario no root
RUN useradd -m appuser

# Directorio de trabajo
WORKDIR /app

RUN mkdir -p /shared && chown -R appuser:appuser /shared
# Copiar archivos
COPY requirements.txt .
COPY ingesta.py .
COPY /ingestas/ingesta_valencia.py .
COPY /ingestas/ingesta_madrid.py .
COPY producer.py .
COPY kafka_consumer.py .
COPY dashboard_alertas.py .
COPY api.py .

# Instalar dependencias
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 


# Cambiar a usuario no root
USER appuser