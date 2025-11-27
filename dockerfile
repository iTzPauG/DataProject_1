FROM python:3.11-slim

# Crear usuario no root
RUN useradd -m appuser

# Directorio de trabajo
WORKDIR /app

# Copiar archivos
COPY requirements.txt .
COPY project.py .
COPY pull_api.py .
COPY kafka_consumer.py .
COPY dashboard_alertas.py .

# Instalar dependencias
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 


# Cambiar a usuario no root
USER appuser

# Ejecutar script al iniciar el contenedor
CMD ["python", "project.py"]

