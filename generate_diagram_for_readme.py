# generate_diagram_for_readme_final.py
from diagrams import Diagram, Edge
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.queue import Kafka
from diagrams.aws.compute import EC2
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Tableau
from diagrams.generic.network import Firewall
from diagrams.generic.storage import Storage

# Nombre del archivo de salida
diagram_file = "arquitectura_readme.png"

with Diagram(
    "Arquitectura Proyecto DataFlow",
    filename=diagram_file.replace(".png", ""),
    outformat="png",
    show=False,
    direction="LR"
):
    # 1️⃣ APIs de ingesta
    api_val = Firewall("API Valencia")
    api_mad = Firewall("API Madrid")
    api_zar = Firewall("API Zaragoza")

    # 2️⃣ API Gateway
    api_gateway = EC2("API Gateway\n(api.py)")

    # 3️⃣ Base de datos y producer
    db_prod = PostgreSQL("DB/PROD")
    producer = Airflow("Producer\n(producer.py)")
    dbt_transform = Airflow("dbt Transform")

    # 4️⃣ Kafka y consumer
    kafka_broker = Kafka("Kafka")
    kafka_consumer = EC2("Consumer\n(kafka_consumer.py)")

    # 5️⃣ Visualización y exportación
    dashboard = EC2("Dashboard PlotLy/Dash\n(dashboard_alertas.py)")
    drive_csv = Storage("To CSV / Drive")
    gsheets_pull = Airflow("GSheets Pull\n(pull_db_gsheets.py)")
    tableau = Tableau("Tableau BI")

    # --- Flujos de datos ---
    # APIs → API Gateway → DB
    api_val >> Edge(label="Datos JSON") >> api_gateway
    api_mad >> Edge(label="Datos JSON") >> api_gateway
    api_zar >> Edge(label="Datos JSON") >> api_gateway
    api_gateway >> Edge(label="Guarda Datos (Raw)") >> db_prod

    # DB → Producer → Kafka → Consumer → Dashboard
    db_prod >> Edge(label="Lee Nuevos Datos") >> producer
    producer >> Edge(label="Produce Mensaje") >> kafka_broker
    kafka_broker >> Edge(label="Consume Mensajes") >> kafka_consumer
    kafka_consumer >> dashboard

    # DB → DBT
    db_prod - Edge(label="Lee/Escribe") - dbt_transform

    # DB → GSheets Pull → Drive → Tableau
    db_prod >> Edge(label="Pull de Datos") >> gsheets_pull
    gsheets_pull >> drive_csv
    drive_csv >> tableau
