from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import json
import os

ALERTAS_FILE = os.getenv("ALERTAS_FILE", "/shared/alertas.json")

app = Dash(__name__)

app.layout = html.Div([
    html.H2("⚠️ Alertas de Calidad del Aire (Kafka + Plotly)"),
    
    dcc.Graph(id="grafico-alertas"),
    
    # Refrescar cada 2 segundos
    dcc.Interval(id="interval", interval=2000, n_intervals=0)
])

@app.callback(
    Output("grafico-alertas", "figure"),
    Input("interval", "n_intervals")
)
def actualizar_grafico(n):
    try:
        with open(ALERTAS_FILE, "r") as f:
            alertas_buffer = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        alertas_buffer = []

    if len(alertas_buffer) == 0:
        return go.Figure()

    x = [a["fecha_carg"] for a in alertas_buffer]
    y = [a["nivel_no2"] for a in alertas_buffer]
    nombres = [a["nombre"] for a in alertas_buffer]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=x,
        y=y,
        mode="lines+markers",
        text=nombres,
        hovertemplate="Zona: %{text}<br>Partículas: %{y}<br>Fecha: %{x}"
    ))

    fig.update_layout(
        title="Evolución de partículas detectadas",
        xaxis_title="Tiempo",
        yaxis_title="Partículas"
    )

    return fig

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8050)
