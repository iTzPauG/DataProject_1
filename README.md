# 🌍 Monitor de Calidad del Aire

Pipeline end-to-end para la **automatización de alertas sobre la calidad del aire** y para la **mejora en la toma de decisiones** de las autoridades.

> ⚠️ Este pipeline es un diseño demostrativo a falta de recursos para implementar las tecnologías de forma completa en producción.

---

## 📋 Requisitos

Antes de comenzar, asegúrate de tener:

- 🐳 **Docker** y **Docker Compose** instalados
- 📄 Archivo `.env` configurado (ver sección siguiente)
- 🔑 Archivo `credentials.json` (proporcionado en la entrega)

---

## ⚙️ Configuración del archivo `.env`

Crea un archivo `.env` en la raíz del proyecto con el siguiente contenido:

```env
# Base de datos
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=data_project_1
DB_HOST=db
DB_PORT=5432
DATABASE_URL=postgresql://postgres:postgres@db:5432/data_project_1

# Kafka
KAFKA_BOOTSTRAP=kafka:29092

# Python
PYTHONUNBUFFERED=1
LOG_LEVEL=INFO
```

> 💡 Modifica `POSTGRES_USER` y `POSTGRES_PASSWORD` según tus preferencias, y actualiza `DATABASE_URL` acorde.

---

## 🚀 Instrucciones de Uso

### 1️⃣ Arranque Completo del Pipeline

Este comando construye las imágenes, lanza la infraestructura (DB, Kafka) y ejecuta automáticamente las ingestas iniciales:

```bash
docker compose up -d
```

### 2️⃣ Verificación de Servicios

Asegúrate de que todos los contenedores estén levantados y funcionando:

```bash
docker compose ps
```

Deberías ver todos los servicios con estado `running` o `healthy`.

---

## 📊 Dashboards

### 🚨 Dashboard de Alertas (Población)

Monitoreo en tiempo real de la calidad del aire para ciudadanos:

🔗 **http://localhost:8050**

| Característica | Descripción |
|----------------|-------------|
| Semáforo visual | Verde (OK) / Rojo (Alerta) |
| Gráfico Radar | Comparativa de 4 contaminantes |
| Gráfico Barras | Niveles vs límites |
| Actualización | Cada 2 segundos |

### 📈 Dashboard Experto (Autoridades)

Análisis detallado para toma de decisiones:

🔗 **[Dashboard en Tableau Public](https://public.tableau.com/app/profile/daniel.adam5716/viz/CalidadAireDP_varias/Alertas)**

---

## 🏗️ Arquitectura

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   APIs      │────▶│  PostgreSQL │────▶│     dbt     │
│  (Ingestas) │     │     (DB)    │     │ (Transform) │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                           ▼
                    ┌─────────────┐     ┌─────────────┐
                    │   Kafka     │────▶│  Dashboard  │
                    │  (Alertas)  │     │   (Dash)    │
                    └─────────────┘     └─────────────┘
```

---

## 🛠️ Comandos Útiles

| Comando | Descripción |
|---------|-------------|
| `docker compose up -d` | Iniciar todos los servicios |
| `docker compose down` | Detener todos los servicios |
| `docker compose logs -f consumer` | Ver logs del dashboard |
| `docker compose logs -f producer` | Ver logs de alertas |
| `docker compose ps` | Estado de los contenedores |

---

## 📁 Estructura del Proyecto

```
DataProject_1/
├── 📄 docker-compose.yml
├── 📄 dockerfile
├── 📄 requirements.txt
├── 📄 .env
├── 📄 credentials.json
├── 🐍 producer.py          # Generador de alertas Kafka
├── 🐍 dashboard_consumer.py # Dashboard Dash
├── 🐍 ingestas.py          # Carga de datos APIs
├── 📁 dbt/                 # Transformaciones dbt
└── 📄 README.md
```

---

## 👥 Autores

Proyecto desarrollado como parte del Data Project 1 por Gemma, Daniel y Pau.

---

