# 💻 DATA PROJECT: MONITORIZACIÓN DE LA CALIDAD DEL AIRE

****

Este proyecto se basa en el diseño e implementación de una **arquitectura moderna de Ingeniería de Datos (Data Engineering)** para la ingesta, almacenamiento, transformación y explotación de datos de la calidad del aire procedentes de los portales de Datos Abiertos de Madrid y Valencia.

## 1. 🎯 Presentación y Objetivos del Proyecto

El Data Project simula un entorno de trabajo real, requiriendo la integración de datos públicos, diseño arquitectónico y toma de decisiones técnicas para abordar la monitorización de la calidad del aire.

### Objetivos Clave

* **Integración de Fuentes:** Integrar múltiples fuentes de datos heterogéneas (APIs de Madrid y Valencia).
* **Transformación y Calidad:** Homogeneizar, limpiar y transformar datos a través de procesos automatizados (dbt).
* **Arquitectura Híbrida:** Garantizar una arquitectura escalable, modular y desacoplada (Kafka) para manejar **Streaming** y **Batch**.
* **Entrega BI:** Publicar los datos procesados para el análisis a través de Tableau BI.
* **Mantenibilidad:** Diseñar un *pipeline* reproducible, mantenible y **versionado** con Git.

***

## 2. ⚙️ Diseño de Arquitectura y Justificación de Piezas

La arquitectura implementa un **sistema híbrido** que maneja el **análisis histórico (Batch BI)** y el **monitoreo de baja latencia (Streaming)**, siguiendo el paradigma **ELT (Extract, Load, Transform)**.

### A. Capa de Ingesta y Fuente de Verdad

| Componente | Justificación Estratégica | Archivos de Evidencia |
| :--- | :--- | :--- |
| **APIs y API Gateway** | **Centraliza la seguridad y gestión de tráfico** (*rate limiting*). Se requieren *scripts* individualizados para fuentes heterogéneas. | `ingestas/ingesta_madrid.py`, `ingestas/ingesta_valencia.py`, `api.py`. |
| **DB PROD (DuckDB)** | Actúa como destino inicial de la carga (la 'L' en ELT) y **Fuente de Verdad Operacional**. **DuckDB** se elige por su **excelente rendimiento en consultas analíticas** y fácil integración con Docker, optimizando el costo. | `dev.duckdb`, `ingesta.py`. |

### B. Flujo de Eventos y Streaming (Tiempo Real) ⚡

| Componente | Justificación Estratégica Extendida | Archivos de Evidencia |
| :--- | :--- | :--- |
| **Bus de Eventos Kafka** | Es la **espina dorsal del *streaming***. Ofrece **desacoplamiento total**, **tolerancia a fallos** y capacidad para manejar **picos de alta concurrencia** (recibe "Nuevos Datos" vía CDC). | `/kafka/docker-compose.yml`, `producer.py`, `kafka_consumer.py`. |
| **Dashboard de Alertas** | Componente dedicado a la **baja latencia**. Muestra **alertas casi instantáneamente**, crucial para el monitoreo operacional. | `dashboard_alertas.py`. |

### C. Transformación Analítica y Modelado (dbt) 🛠️

| Componente | Justificación Estratégica Extendida | Archivos de Evidencia |
| :--- | :--- | :--- |
| **dbt (Data Build Tool)** | Implementa el paradigma **ELT**. Permite **versionar** el código SQL en Git y realizar **pruebas automatizadas** de calidad de datos, garantizando la **confiabilidad** y **auditoría** del dato. | Estructura `/dbt`, pruebas `unique_...sql`. |
| **Estructura en Capas** | Adopción del estándar **Staging → Intermediate → Marts** para crear un **linaje de datos claro** y modular, optimizando la mantenibilidad. | Directorios `/staging`, `/intermediate`, `/marts`. |

## 3. 💾 Modelos de Datos: Diseño del Data Warehouse

El Data Warehouse de consumo se basa estrictamente en un conjunto de **Tablas de Hechos Pre-agregadas** (`mart_hourly` y `mart_monthly_promedio`) diseñadas para la máxima velocidad de consulta en Tableau BI.

### A. Tablas de Hechos (Capa de Consumo Final)

Ambas tablas comparten la dimensión de **Estación** (`city`, `nombre_estacion`).

#### 1. Tabla de Hechos Horaria: `mart_hourly`

Soporta análisis de alta granularidad y la lógica de clasificación y ranking.

| Atributo | Rol Analítico | Definición / Lógica |
| :--- | :--- | :--- |
| **`fecha_hour`** | Dimensión | Granularidad horaria. |
| `no2_avg` a `pm25_avg` | Métrica Base | Promedios de contaminantes por hora. |
| **`indice_contaminacion`** | Métrica Calculada | Índice de contaminación por hora, promedio de los cuatro contaminantes. |
| **`nivel_no2`** | Métrica Clasificada | Clasificación de NO2 según umbrales (ej. 'Muy Alto', 'Alto', 'Moderado', 'Bajo'). |
| **`ranking_pm25`** | Métrica Calculada | Ranking de PM2.5 por hora dentro de cada ciudad, optimizado para el top N en BI. |

#### 2. Tabla de Hechos Mensual: `mart_monthly_promedio`

Soporta análisis de tendencias a largo plazo y estacionalidad.

| Atributo | Rol Analítico | Definición / Lógica |
| :--- | :--- | :--- |
| **`fecha_month`** | Dimensión | Granularidad mensual. |
| `no2_avg` a `pm25_avg` | Métrica Base | Promedios de contaminantes por mes. |
| **`contaminacion_promedio`** | Métrica Calculada | Promedio general de los cuatro contaminantes para el mes. |

## 4. 🔎 Origen de Datos y Flujo de Entrega (BI)

### A. Datasets Explorados y Justificación

### A. Datasets Explorados y Justificación

| Dataset | Origen | Decisión | Justificación | URL |
| :--- | :--- | :--- | :--- | :--- |
| **Calidad del Aire - Tiempo Real** | API Madrid | **INCLUIDO** | Proporciona los datos horarios necesarios para el **monitoreo en tiempo real**. | `https://ciudadesabiertas.madrid.es/dynamicAPI/API/query/calair_tiemporeal.json?pageSize=5000` |
| **Estaciones Contaminación Atmosférica** | API Valencia | **INCLUIDO** | Fuente de datos crítica para el análisis geográfico y la construcción de la **dimensión de la estación**. | `https://valencia.opendatasoft.com/api/records/1.0/search/?dataset=estacions-contaminacio-atmosferiques-estaciones-contaminacion-atmosfericas&rows=1000` |


### B. Estructura del Repositorio Git y Entrega de BI 📊

| Carpeta / Archivo | Rol en la Arquitectura | Descripción / Instrucciones |
| :--- | :--- | :--- |
| **`README.md`** | **Instrucciones Principales** | Contiene el diseño de la arquitectura, la justificación y los pasos de ejecución. |
| **`requirements.txt`** | **Entorno Python** | Lista las librerías necesarias para la reproducibilidad del entorno. |
| **`docker-compose.yml`** | **Infraestructura** | Define y orquesta los contenedores (Kafka, DB, Python/dbt). |
| **`/ingestas`** | Ingesta | Scripts de extracción de las APIs. |
| **`/kafka`** | Infraestructura Streaming | Configuración de los servicios de Kafka y Zookeeper. |
| **`producer.py` / `kafka_consumer.py`** | Streaming | Scripts de envío y recepción de datos por Kafka. |
| **`/dbt/models`** | Transformación (ELT) | Contiene la lógica SQL de modelado (`staging`, `intermediate`, `marts`). |
| **`pull_db_gsheets.py`** | Entrega BI | Script de *Reverse ETL* que extrae los *marts* para Tableau. |