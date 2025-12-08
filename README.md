# 💨 DATA PROJECT I: Monitorización de la Calidad del Aire

**Máster en Big Data & Cloud 2025-2026**

**Tutor:** Pedro Nieto  
**Participantes:** Daniel Adam, Pau Garcia, Gemma Balaguer

## 🌟 1. Introducción

Este proyecto implementa un **pipeline de datos moderno y escalable** para la monitorización en tiempo real de la calidad del aire de las ciudades de **Madrid y Valencia**.

El sistema integra la ingesta de APIs externas, almacenamiento relacional, procesamiento **Batch (dbt)** y **Streaming (Kafka)**, y entrega a múltiples herramientas de Business Intelligence (Plotly/Tableau), todo orquestado y empaquetado mediante **Docker Compose**.

### 🛠️ Stack Tecnológico Clave

| Categoría | Tecnología | Uso Principal |
| :--- | :--- | :--- |
| **Ingesta** | Python (Requests) | Extracción de datos de APIs de Ayuntamientos (Madrid y Valencia). |
| **Orquestación** | Docker Compose | Definición, construcción y orquestación de todos los servicios. |
| **Almacenamiento** | PostgreSQL | Base de datos OLTP/Analítica para datos brutos y transformados. |
| **Transformación** | dbt (Data Build Tool) | Modelado de datos (ELT) con SQL para generar *data marts*. |
| **Streaming** | Apache Kafka | Plataforma de mensajería para desacoplamiento y procesamiento en tiempo real. |
| **Visualización** | Plotly / Tableau | Dashboards de baja latencia (Plotly) y análisis profesional (Tableau). |

---

## 🏗️ 2. Arquitectura del Proyecto

El sistema está diseñado como una arquitectura de **Lambda/Kappa simplificada**, combinando el procesamiento *batch* tradicional con capacidades de *streaming* en tiempo real.



### 2.1. Flujo de Datos

1.  **Ingesta (`/ingestas`):** Los scripts `ingesta_madrid.py` e `ingesta_valencia.py` extraen datos horarios de calidad del aire desde las APIs oficiales.
2.  **API Gateway (`api.py`):** Los datos ingestados se envían a este *gateway* único para control de acceso, validación y estandarización de formatos antes de la persistencia.
3.  **Base de Datos (`data_project_1` - PostgreSQL):** Almacenamiento centralizado de los datos **raw** (brutos).
4.  **Transformaciones (dbt):** Modelado de datos (limpieza, normalización y cálculo de métricas) ejecutado sobre PostgreSQL para generar la capa de **Marts**.
5.  **Streaming (Kafka):**
    * **Producer (`producer.py`):** Lee continuamente nuevos registros de la base de datos y los envía al *cluster* de Kafka.
    * **Consumer (`dashboard_consumer.py`):** Consume los mensajes de Kafka para alimentar un dashboard de alertas en **tiempo real** (Plotly).
6.  **Entrega BI (Reverse ETL):**
    * `pull_db_gsheets.py` exporta los *data marts* finales a Google Sheets.
    * Tableau consume la información desde Google Sheets/Drive para dashboards de **análisis avanzado**.

---

## 🚀 3. Ejecución del Proyecto

El proyecto se despliega completamente mediante un único comando de `docker-compose`.

### 3.1. Requisitos

* **Docker Desktop** (o Docker Engine y Docker Compose) instalado y en ejecución.
* Configuración de credenciales de las APIs (si aplica) en el archivo `.env`.

### 3.2. Pasos de Ejecución (Comando Único)

El siguiente comando construye las imágenes, arranca la infraestructura (PostgreSQL, Kafka, UIs) y ejecuta automáticamente los *pipelines* de ingesta, transformación y *streaming*.

```bash
docker compose up -d