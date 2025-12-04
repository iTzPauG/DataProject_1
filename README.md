
# 📂 Arquitectura Proyecto DataFlow

## 🗺️ Diagrama de Arquitectura
El siguiente diagrama ilustra el flujo de datos completo, reflejando el **flujo API -> DB -> Producer -> Kafka** y los scripts Python asociados.

![Arquitectura Proyecto DataFlow](./arquitectura_readme.png)

---

## 🛠️ Componentes y Scripts Clave

| Componente | Rol en el Flujo | Archivo Python |
| :--- | :--- | :--- |
| **APIs (Fuentes)** | Puntos de partida de los datos de calidad del aire. | (e.g., `ingesta.py`) |
| **API Gateway** | Recibe peticiones y **guarda en la DB**. | `api.py` |
| **DB/PROD (PostgreSQL)** | Almacenamiento persistente de datos brutos. | N/A |
| **dbt Transform** | Modelado y transformación de datos en la DB. | N/A (Scripts SQL/YAML) |
| **Producer** | Proceso que **lee los datos de la DB** y **produce el mensaje de alerta** para Kafka. | `producer.py` |
| **Kafka** | Message Broker. | N/A |
| **Consumer** | Lee de Kafka y prepara el buffer de alertas. | `kafka_consumer.py` |
| **Dashboard PlotLy/Dash** | Visualización interactiva en tiempo real. | `dashboard_alertas.py` |
| **Export CSV / Drive** | Extracción y almacenamiento de datos para reporting. | `pull_db_gsheets.py` |
| **Tableau BI** | Herramienta de Business Intelligence. | N/A |
