FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Crear usuario no root
RUN useradd -m appuser

# Copiar proyecto dbt al contenedor
WORKDIR /usr/app/dbt
COPY ./dbt /usr/app/dbt

# Copiar el resto del código Python
WORKDIR /app
COPY requirements.txt .
COPY producer.py .
COPY dashboard_consumer.py .
COPY api.py .
COPY pull_db_gsheets.py .
COPY credentials.json .
COPY ingestas/ingesta_madrid.py .
COPY ingestas/ingesta_valencia.py .

# Instalar dependencias Python
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt 

RUN chown -R appuser:appuser /usr/app/dbt

USER appuser
