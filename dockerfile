FROM python:3.11-slim

# Crear usuario no root
RUN useradd -m appuser

# Directorio de trabajo: Todos los archivos del proyecto van aquí
WORKDIR /usr/app/project

# Copiar archivos y dependencias
COPY requirements.txt ./
COPY .env ./
COPY ingesta_valencia.py ./
COPY producer.py ./
COPY kafka_consumer.py ./
COPY dashboard_alertas.py ./
COPY api.py ./
# Copiar la carpeta dbt (modelos, profiles.yml, etc.)
COPY dbt ./dbt

# Instalar Graphviz (si es necesario para el DIAGRAMA) y dependencias Python como ROOT
RUN apt-get update && \
    apt-get install -y graphviz && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Instalar dbt-core y dbt-postgres como ROOT en ubicación GLOBAL
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir dbt-core dbt-postgres  # <--- Aseguramos la instalación de dbt-core

# Directorio de trabajo final (donde se ejecuta el comando)
WORKDIR /usr/app/project/dbt 

# Cambiar permisos y usuario
RUN chown -R appuser:appuser /usr/app/project
USER appuser

# Comando por defecto para el servicio 'dbt'
# dbt ya estará en el PATH global porque se instaló como root.
CMD ["dbt", "run"]

