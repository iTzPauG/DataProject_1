# generate_diagram_for_readme.py
from diagrams import Diagram, Edge
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.queue import Kafka
from diagrams.aws.compute import EC2
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.analytics import Tableau
from diagrams.generic.device import Mobile

# Generar la imagen directamente en la carpeta actual
with Diagram(
    "Arquitectura Proyecto DataFlow",
    filename="arquitectura_readme",  # Esto creará arquitectura_readme.png
    outformat="png",
    show=False,                       # No abrir automáticamente
    direction="LR"                    # Flujo izquierda → derecha
):
    # APIs de ingesta
    api_val = Mobile("API Valencia")
    api_mad = Mobile("API Madrid")
    api_zar = Mobile("API Zaragoza")
    
    # API Gateway / Producer
    api_gateway = EC2("API Gateway")
    producer = Airflow("Producer (Kafka/DB)")

    # DB y Kafka
    db_prod = PostgreSQL("DB/PROD")
    kafka_broker = Kafka("Kafka")
    kafka_consumer = EC2("Consumer")

    # Visualización / Reporting
    dashboard = EC2("Dashboard Plotly")
    drive_csv = EC2("Export CSV / Drive")
    tableau = Tableau("Tableau BI")

    # Flujo de datos
    api_val >> Edge(label="Llamada API") >> api_gateway
    api_mad >> Edge(label="Llamada API") >> api_gateway
    api_zar >> Edge(label="Llamada API") >> api_gateway

    api_gateway >> producer
    producer >> db_prod
    producer >> kafka_broker
    kafka_broker >> kafka_consumer
    kafka_consumer >> dashboard
    db_prod >> drive_csv >> tableau
