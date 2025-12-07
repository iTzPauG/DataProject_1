# 💻 DATA PROJECT: MONITORIZACIÓN DE LA CALIDAD DEL AIRE

****

Este proyecto se basa en el diseño e implementación de una **arquitectura moderna de Ingeniería de Datos (Data Engineering)** para la ingesta, almacenamiento, transformación y explotación de datos de la calidad del aire procedentes de los portales de Datos Abiertos de Madrid y Valencia.

## 0. EQUIPO:
1. PAU GARCIA

2. DANIEL ADAM

3. GEMMA BALAGUER

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
| **Dashboard de Alertas (Plotly)** | Componente dedicado a la **baja latencia**. Muestra **alertas casi instantáneamente**, crucial para el monitoreo operacional. | `dashboard_alertas.py`. |

### C. Transformación Analítica y Modelado (dbt) 🛠️

| Componente | Justificación Estratégica Extendida | Archivos de Evidencia |
| :--- | :--- | :--- |
| **dbt (Data Build Tool)** | Implementa el paradigma **ELT**. Permite **versionar** el código SQL en Git y realizar **pruebas automatizadas** de calidad de datos, garantizando la **confiabilidad** y **auditoría** del dato. | Estructura `/dbt`, pruebas `unique_...sql`. |
| **Estructura en Capas** | Adopción del estándar **Staging → Intermediate → Marts** para crear un **linaje de datos claro** y modular, optimizando la mantenibilidad. | Directorios `/staging`, `/intermediate`, `/marts`. |

## 3. 💾 VISUALIZACIÓN DE LOS DATOS.

### A. DASHBOARD DE LA CALIDAD DEL AIRE

Los datos de este archivo provienen de la tabla mediciones que se encuentra en la base de datos Data_Project_1. 

**Variables empleadas**:

| Columna | Descripción | Ejemplo de Dato |
| :--- | :--- | :--- |
| **Lat, Lon** | Coordenadas geográficas de la estación (latitud y longitud). | 40.4514734, -3.6773491 |
| **Id** | Identificador único de la estación. | 1813 |
| **Nombre estación** | Nombre descriptivo de la estación de monitoreo. | Avda. Ramón y Cajal |
| **Ciudad** | Ciudad donde se encuentra la estación. | Madrid |
| **is_latest** | Indica si es el dato más reciente (`True` o `False`). | True |
| **No2** | Concentración de **Dióxido de Nitrógeno** $(\text{NO}_2)$ en $\mu g/m^3$. | 9 |
| **O3** | Concentración de **Ozono** $(\text{O}_3)$ en $\mu g/m^3$. | 49 |
| **Pm10, Pm25** | Concentración de **Partículas en Suspensión** (diámetros $\le 10\mu m$ y $\le 2.5\mu m$). | 6, 4 |

**Objetivo de la visualización**

El objetivo principal es conseguir un mapa interactivo que permita a los usuarios: 
1. Identificar geográficamente todas las estaciones de monitoreo. 
2. Evaluar rápidamente el nivel de un contaminante clave mediante la codifcación por color con marcadores. 
3. Consultar los calores exactos de todos los contaminantes y los detalles de la estación al hacer clic sobre el marcador. 

**Mapeo de colores**:

| Rango de contaminante | Color | Significado |
| :--- | :--- | :--- |
| Bajo | Verde | Buena Calidad |
| Medio | Amarillo | Calidad Aceptable | 
| Alto | Rojo | Mala Calidad |



## 4. 🔎 Origen de Datos y Flujo de Entrega (BI)

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

## 5. ⚙️ Ejecución del Proyecto

El proyecto está diseñado para una ejecución automatizada de principio a fin usando Docker Compose.

### A. Requisitos
* Docker Desktop (o Docker Engine y Docker Compose).
* Acceso a las APIs de datos abiertos (se requiere la configuración de credenciales si aplica, en el archivo `.env`).

### B. Pasos de Ejecución (Comando Único)

Desde el directorio raíz del proyecto:

1.  **Arranque Completo del Pipeline (Batch y Streaming):**
    Este comando construye las imágenes, lanza la infraestructura (DB, Kafka) y ejecuta automáticamente las ingestas iniciales, la transformación con dbt, y el flujo de streaming (producer, consumer, dashboard).

    ```bash
    docker-compose up -d --build
    ```

2.  **Verificación de Servicios:**
    Asegúrate de que todos los contenedores estén levantados y sanos.

    ```bash
    docker-compose ps
    ```

3.  **Monitoreo del Dashboard:**
    Accede al dashboard de alertas en tiempo real:

    ```
    Abrir navegador: http://localhost:8050
    ```

### C. Consulta y Administración
| Servicio | URL Local | Descripción |
| :--- | :--- | :--- |
| **Dashboard** | `http://localhost:8050` | Visualización de alertas de baja latencia. |
| **Kafka UI (Kafbat)** | `http://localhost:8080` | Monitoreo del flujo de mensajes. |
| **pgAdmin** | `http://localhost:5050` | Acceso a PostgreSQL (servidor: `db`, puerto: 5432). |

### D. Limpieza
Para detener todos los servicios y eliminar los contenedores (usar `-v` para borrar también los datos persistentes de la base de datos):

```bash
docker-compose down
```
# docker-compose down -v  (Si quieres borrar los datos de Postgres)

